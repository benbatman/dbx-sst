// Mic capture -> 16 kHz mono 16-bit PCM chunks.


export const TARGET_SAMPLE_RATE = 16_000;
export const CHUNK_MS = 500;

export interface MicStream {
  stop: () => void;
}

function floatTo16BitPCM(input: Float32Array): ArrayBuffer {
  const out = new DataView(new ArrayBuffer(input.length * 2));
  for (let i = 0; i < input.length; i++) {
    const s = Math.max(-1, Math.min(1, input[i]));
    out.setInt16(i * 2, s < 0 ? s * 0x8000 : s * 0x7fff, true);
  }
  return out.buffer;
}

/** Linear resample a mono Float32 buffer from inRate to outRate. */
function resample(
  input: Float32Array,
  inRate: number,
  outRate: number,
): Float32Array {
  if (inRate === outRate) return input;
  const ratio = inRate / outRate;
  const outLen = Math.floor(input.length / ratio);
  const out = new Float32Array(outLen);
  for (let i = 0; i < outLen; i++) {
    const srcPos = i * ratio;
    const i0 = Math.floor(srcPos);
    const i1 = Math.min(i0 + 1, input.length - 1);
    const frac = srcPos - i0;
    out[i] = input[i0] * (1 - frac) + input[i1] * frac;
  }
  return out;
}

export async function startMic(
  onChunk: (pcm: ArrayBuffer) => void,
): Promise<MicStream> {
  const media = await navigator.mediaDevices.getUserMedia({
    audio: {
      channelCount: 1,
      echoCancellation: true,
      noiseSuppression: true,
    },
  });

  const ctx = new AudioContext();
  await ctx.audioWorklet.addModule("/pcm_worklet_processor.js");
  const source = ctx.createMediaStreamSource(media)
  const node = new AudioWorkletNode(ctx, "pcm-capture", {
    numberOfInputs: 1,
    numberOfOutputs: 1,
    channelCount: 1,
  });

  const inRate = ctx.sampleRate;
  const samplesPerChunk = Math.floor((TARGET_SAMPLE_RATE * CHUNK_MS) / 1000);
  let acc: Float32Array = new Float32Array(0);

  node.port.onmessage = (ev: MessageEvent<Float32Array>) => {
    const resampled = resample(ev.data, inRate, TARGET_SAMPLE_RATE);
    const merged = new Float32Array(acc.length + resampled.length);
    merged.set(acc, 0);
    merged.set(resampled, acc.length);
    acc = merged;

    while (acc.length >= samplesPerChunk) {
      const slice = acc.subarray(0, samplesPerChunk);
      onChunk(floatTo16BitPCM(slice));
      acc = acc.subarray(samplesPerChunk);
    }
  };

  source.connect(node);
  node.connect(ctx.destination);

  return {
    stop: () => {
      node.port.onmessage = null;
      node.disconnect();
      source.disconnect();
      media.getTracks().forEach((t) => t.stop());
      void ctx.close();
    },
  };
}
