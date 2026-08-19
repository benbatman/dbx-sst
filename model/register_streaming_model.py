"""MLflow pyfunc wrapping NVIDIA NeMo `nvidia/parakeet-unified-en-0.6b` for
buffered-streaming ASR.

The endpoint is STATELESS. "Buffered streaming" (per the model card) means the
CLIENT sends a rolling audio window (left context + newest chunk) on every step;
this server transcribes the whole window and returns text. Cross-request state
(the rolling window) lives in the Databricks App, never here.

Input : DataFrame with a single string column `audio_b64` -- base64 of a mono
        16 kHz 16-bit PCM WAV file containing the rolling window.
Output: DataFrame with a single string column `text`.
"""

import argparse
import base64
import binascii
import io
import logging
import os
import tempfile
import wave
from typing import Any

import mlflow
import pandas as pd
from mlflow.models import ModelSignature
from mlflow.types import ColSpec, DataType, Schema

LOGGER = logging.getLogger(__name__)

MODEL_ID = "nvidia/parakeet-unified-en-0.6b"
SAMPLE_RATE = 16_000
# Cap the decoded WAV size to protect the GPU worker from oversized payloads.
# A ~10 s mono 16 kHz 16-bit window is ~320 KB; 10 MB should be good
MAX_WAV_BYTES = 10 * 1024 * 1024


class ParakeetStreamingPyFunc(mlflow.pyfunc.PythonModel):
    def __init__(self, model_id: str = MODEL_ID) -> None:
        self.model_id = model_id
        self._asr_model: Any | None = None
        self._torch: Any | None = None

    def load_context(self, context: mlflow.pyfunc.PythonModelContext) -> None:
        del context
        try:
            import nemo.collections.asr as nemo_asr
            import torch
            from omegaconf import open_dict

            device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
            self._asr_model = nemo_asr.models.ASRModel.from_pretrained(
                model_name=self.model_id
            )
            # NeMo's RNNT transcription dataloader reads validation_ds with
            # `.get()`, but this checkpoint stores the field as null because it
            # does not ship a validation dataset. Normalize it for inference.
            if self._asr_model.cfg.get("validation_ds") is None:
                with open_dict(self._asr_model.cfg):
                    self._asr_model.cfg.validation_ds = {}
            self._asr_model.to(device)
            self._asr_model.eval()
            self._torch = torch
            LOGGER.info("Loaded %s on %s", self.model_id, device)
        except Exception as exc:  # noqa: BLE001 - surface any load failure clearly
            LOGGER.exception("Unable to load %s", self.model_id)
            raise RuntimeError(f"Unable to load {self.model_id}") from exc

    @staticmethod
    def _decode_to_wav_path(audio_b64: str) -> str:
        """Validate and write the base64 WAV to a temp .wav file, returning its
        path.

        The model card documents file-path input to `transcribe([path])` and
        states no pre-processing is required, so we hand NeMo the file path
        rather than a raw numpy array.
        """
        if not isinstance(audio_b64, str) or not audio_b64:
            raise ValueError("audio_b64 must be a non-empty base64 string")
        try:
            wav_bytes = base64.b64decode(audio_b64, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise ValueError("audio_b64 is not valid base64") from exc
        if not wav_bytes:
            raise ValueError("audio_b64 decoded to empty bytes")
        if len(wav_bytes) > MAX_WAV_BYTES:
            raise ValueError(f"WAV bytes must be <= {MAX_WAV_BYTES} bytes")

        # Sanity-check that the bytes are a readable WAV before touching the GPU.
        try:
            with wave.open(io.BytesIO(wav_bytes), "rb") as wav_file:
                if wav_file.getnframes() == 0:
                    raise ValueError("WAV contains no audio frames")
        except wave.Error as exc:
            raise ValueError("audio_b64 must contain a readable WAV") from exc

        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tmp:
            try:
                tmp.write(wav_bytes)
                tmp.flush()
            finally:
                tmp.close()
            return tmp.name

    def predict(
        self,
        context: mlflow.pyfunc.PythonModelContext,
        model_input: pd.DataFrame,
        params: dict[str, Any] | None = None,
    ) -> pd.DataFrame:
        del context, params
        if self._asr_model is None or self._torch is None:
            raise RuntimeError("Parakeet is not loaded")
        if "audio_b64" not in model_input.columns:
            raise ValueError("Input must contain an 'audio_b64' column")
        # `.empty` is a PROPERTY, not a method.
        if model_input.empty:
            raise ValueError("Input must contain at least one row")

        wav_paths: list[str] = []
        try:
            wav_paths = [
                self._decode_to_wav_path(value) for value in model_input["audio_b64"]
            ]
            try:
                with self._torch.inference_mode():
                    hypotheses = self._asr_model.transcribe(wav_paths)
            except Exception as exc:
                LOGGER.exception("Parakeet transcription failed")
                raise RuntimeError("Parakeet transcription failed") from exc

            texts = [
                hyp.text if hasattr(hyp, "text") else str(hyp) for hyp in hypotheses
            ]
            return pd.DataFrame({"text": texts})
        finally:
            for path in wav_paths:
                try:
                    os.remove(path)
                except OSError:
                    LOGGER.warning("Could not remove temp wav %s", path)


def _silent_wav_b64() -> str:
    """Return base64 of a 1 s silent mono 16 kHz 16-bit WAV (for input_example)."""
    buffer = io.BytesIO()
    with wave.open(buffer, "wb") as wav_file:
        wav_file.setnchannels(1)
        wav_file.setsampwidth(2)
        wav_file.setframerate(SAMPLE_RATE)
        wav_file.writeframes(b"\x00\x00" * SAMPLE_RATE)
    return base64.b64encode(buffer.getvalue()).decode("ascii")


PIP_REQUIREMENTS = [
    "mlflow",
    "nemo_toolkit[asr]==3.0.0",
    "soundfile",
    "torch==2.13.0",
    "numpy==1.26.4",
]


def register_model(registered_model_name: str, experiment_name: str) -> int:
    mlflow.set_tracking_uri("databricks")
    mlflow.set_registry_uri("databricks-uc")
    mlflow.set_experiment(experiment_name=experiment_name)
    signature = ModelSignature(
        inputs=Schema([ColSpec(DataType.string, "audio_b64")]),
        outputs=Schema([ColSpec(DataType.string, "text")]),
    )
    input_example = pd.DataFrame({"audio_b64": [_silent_wav_b64()]})

    with mlflow.start_run():
        model_info = mlflow.pyfunc.log_model(
            name="parakeet-streaming-model",
            python_model=ParakeetStreamingPyFunc(),
            signature=signature,
            input_example=input_example,
            registered_model_name=registered_model_name,
            pip_requirements=PIP_REQUIREMENTS,
            await_registration_for=600,
        )

    if model_info.registered_model_version is None:
        raise RuntimeError("MLflow did not return a registered model version")

    version = int(model_info.registered_model_version)
    LOGGER.info("REGISTERED_MODEL=%s", registered_model_name)
    LOGGER.info("MODEL_VERSION=%s", version)
    return version


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--registered-model-name", required=True)
    parser.add_argument("--experiment-name", required=True)
    args = parser.parse_args()
    register_model(args.registered_model_name, args.experiment_name)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
