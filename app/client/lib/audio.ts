// Mic capture -> 16 kHz mono 16-bit PCM chunks.

const STATE_WRITE_INDEX = 0;
const STATE_READ_INDEX = 1;
const STATE_OVERRUN_SAMPLES = 2;
const STATE_STOPPED = 3;
const STATE_SLOTS = 4;

const RING_SECONDS = 4;
const NETWORK_FRAME_MS = 100;
const WORKER_STOP_TIMEOUT_MS = 2_000;

export interface MicFormat {
  type: "start";
  version: 1;
  format: "f32le";
  sampleRate: number;
  channels: 1;
}

export interface MicCallbacks {
  onFormat: (format: MicFormat) => void;
  onChunk: (packet: ArrayBuffer) => void;
  onError: (message: string) => void;
}

export interface MicStream {
  stop: () => Promise<void>;
}

type AudioWorkerMessage = 
  | {
    type: "audio";
    buffer: ArrayBuffer;
  }
  | {
    type: "error";
    message: string;
  }
  | {
    type: "stopped";
  };

function timeout(ms: number): Promise<void> {
  return new Promise((resolve) => {
    window.setTimeout(resolve, ms);
  });
}

export async function startMic(
  callbacks: MicCallbacks
): Promise<MicStream> {
  if (!window.crossOriginIsolated) {
    throw new Error(
        "SharedArrayBuffer is unavailable. Serve the app with " +
        "COOP=same-origin and COEP=require-corp.",
    );
  }

  const media = await navigator.mediaDevices.getUserMedia({
      audio: {
        channelCount: 1,
        echoCancellation: true,
        noiseSuppression: true,
      },
  });

  const ctx = new AudioContext();

  let source: MediaStreamAudioSourceNode | null = null;
  let node: AudioWorkletNode | null = null;
  let worker: Worker | null;

  try {
    await ctx.resume();
    await ctx.audioWorklet.addModule(
      "/pcm_worklet_processor.js"
    );

    source = ctx.createMediaStreamSource(media);

    const capacity = Math.ceil(
      ctx.sampleRate * RING_SECONDS
    );

    const frameSamples = Math.max(
      1, Math.round((ctx.sampleRate * NETWORK_FRAME_MS) / 1000)
    );

    const audioSAB = new SharedArrayBuffer(
      capacity * Float32Array.BYTES_PER_ELEMENT,
    );

    const stateSAB = new SharedArrayBuffer(
      STATE_SLOTS * Int32Array.BYTES_PER_ELEMENT
    );

    const state = new Int32Array(stateSAB);

    // Explicitly initialize indices and falgs
    Atomics.store(state, STATE_WRITE_INDEX, 0);
    Atomics.store(state, STATE_READ_INDEX, 0);
    Atomics.store(state, STATE_OVERRUN_SAMPLES, 0);
    Atomics.store(state, STATE_STOPPED, 0);

    node = new AudioWorkletNode(
      ctx,
      "pcm-capture",
      {
        numberOfInputs: 1,
        numberOfOutputs: 1,
        outputChannelCount: [1],
        channelCount: 1,
        processorOptions: {
          audioSAB,
          stateSAB, 
          capacity
        },
      },
    );

    worker = new Worker(
      "/audio_transport_worker.js"
    );

    let resolveWorkerStopped: () => void = () => undefined;

    const workerStopped = new Promise<void>(
      (resolve) => {
        resolveWorkerStopped = resolve;
      },
    );

    // Worker keeps resampling, growing-array concat, and float-to-pcm16 conversion
    // off main thread
    // Main thread is only forwarding already-packaged binary frames from WS
    worker.onmessage = (
      event: MessageEvent<AudioWorkerMessage>,
    ) => {
      const message = event.data;

      if (message.type === "audio") {
        try {
          callbacks.onChunk(message.buffer);
        } catch (error) {
          const detail = error instanceof Error
          ? error.message 
          : "Unable to send capture audio";

          callbacks.onError(detail);
          Atomics.store(
            state, STATE_STOPPED,
            1
          );
          Atomics.notify(
            state, STATE_WRITE_INDEX, 1
          );
        }

        return;
      }

      if (message.type === "error") {
        callbacks.onError(message.message);
        return;
      }

      resolveWorkerStopped();
    };

    worker.onerror = (event) => {
      callbacks.onError(
        event.message || "Audio worker failed"
      );
      resolveWorkerStopped();
    };

    // This callback runs before the microphone is connected,
    // guaranteeing that the WebSocket start control frame is
    // queued before any binary audio frames
    callbacks.onFormat({
      type: "start",
      version: 1,
      format: "f32le",
      sampleRate: ctx.sampleRate,
      channels: 1
    });

    worker.postMessage({
      type: "init",
      audioSAB,
      stateSAB,
      capacity,
      frameSamples
    });

    source.connect(node);

    // The procesor emits silence, so this keeps the graph active
    // without playing microphone audio through the speakers
    node.connect(ctx.destination); 

    let stopPromise: Promise<void> | null = null;

    return {
      stop: () => {
        if (stopPromise) {
          return stopPromise
        }

        stopPromise = (async () => {
          // Prevent additional microphone data first 
          for (const track of media.getTracks()) {
            track.stop();
          }
          
          // Tell the worker to drain the remaining ring data
          Atomics.store(
            state, STATE_STOPPED, 1
          );
          Atomics.notify(
            state, 
            STATE_WRITE_INDEX, 
            1
          );

          await Promise.race([
            workerStopped,
            timeout(WORKER_STOP_TIMEOUT_MS)
          ]);

          worker?.terminate();
          node?.disconnect();
          source?.disconnect();

          await ctx.close();
        })();

        return stopPromise;
      },
    };
  } catch (error) {
        worker?.terminate();
        node?.disconnect();
        source?.disconnect();

        for (const track of media.getTracks()) {
          track.stop();
        }

        await ctx.close().catch(() => undefined);
        throw error;
  }
}

// Previous for ref

// export const TARGET_SAMPLE_RATE = 16_000;
// export const CHUNK_MS = 500;

// export interface MicStream {
//   stop: () => void;
// }

// function floatTo16BitPCM(input: Float32Array): ArrayBuffer {
//   const out = new DataView(new ArrayBuffer(input.length * 2));
//   for (let i = 0; i < input.length; i++) {
//     const s = Math.max(-1, Math.min(1, input[i]));
//     out.setInt16(i * 2, s < 0 ? s * 0x8000 : s * 0x7fff, true);
//   }
//   return out.buffer;
// }

// /** Linear resample a mono Float32 buffer from inRate to outRate. */
// function resample(
//   input: Float32Array,
//   inRate: number,
//   outRate: number,
// ): Float32Array {
//   if (inRate === outRate) return input;
//   const ratio = inRate / outRate;
//   const outLen = Math.floor(input.length / ratio);
//   const out = new Float32Array(outLen);
//   for (let i = 0; i < outLen; i++) {
//     const srcPos = i * ratio;
//     const i0 = Math.floor(srcPos);
//     const i1 = Math.min(i0 + 1, input.length - 1);
//     const frac = srcPos - i0;
//     out[i] = input[i0] * (1 - frac) + input[i1] * frac;
//   }
//   return out;
// }

// export async function startMic(
//   onChunk: (pcm: ArrayBuffer) => void,
// ): Promise<MicStream> {
//   const media = await navigator.mediaDevices.getUserMedia({
//     audio: {
//       channelCount: 1,
//       echoCancellation: true,
//       noiseSuppression: true,
//     },
//   });

//   const ctx = new AudioContext();
//   await ctx.audioWorklet.addModule("/pcm_worklet_processor.js");
//   const source = ctx.createMediaStreamSource(media)
//   const capacity = ctx.sampleRate * 2 // Two seconds of source-rate audio

//   const audioSAB = new SharedArrayBuffer(
//     capacity * Float32Array.BYTES_PER_ELEMENT
//   );

//   const stateSAB = new SharedArrayBuffer(
//     4 * Int32Array.BYTES_PER_ELEMENT,
//   );
  
//   const node = new AudioWorkletNode(ctx, "pcm-capture", {
//     numberOfInputs: 1,
//     numberOfOutputs: 1,
//     channelCount: 1,
//     processorOptions: {
//       audioSAB, 
//       stateSAB,
//       capacity,
//     }
//   });

//   const inRate = ctx.sampleRate;
//   const samplesPerChunk = Math.floor((TARGET_SAMPLE_RATE * CHUNK_MS) / 1000);
//   let acc: Float32Array = new Float32Array(0);

//   node.port.onmessage = (ev: MessageEvent<Float32Array>) => {
//     const resampled = resample(ev.data, inRate, TARGET_SAMPLE_RATE);
//     const merged = new Float32Array(acc.length + resampled.length);
//     merged.set(acc, 0);
//     merged.set(resampled, acc.length);
//     acc = merged;

//     while (acc.length >= samplesPerChunk) {
//       const slice = acc.subarray(0, samplesPerChunk);
//       onChunk(floatTo16BitPCM(slice));
//       acc = acc.subarray(samplesPerChunk);
//     }
//   };

//   source.connect(node);
//   node.connect(ctx.destination);

//   return {
//     stop: () => {
//       node.port.onmessage = null;
//       node.disconnect();
//       source.disconnect();
//       media.getTracks().forEach((t) => t.stop());
//       void ctx.close();
//     },
//   };
// }
