const STATE_WRITE_INDEX = 0;
const STATE_READ_INDEX = 1;
const STATE_OVERRUN_SAMPLES = 2;
const STATE_STOPPED = 3;

// Bytes "AUD1" when written little-endian.
const AUDIO_PACKET_MAGIC = 0x31445541;
const AUDIO_PACKET_HEADER_BYTES = 12;

let audio = null;
let state = null;
let capacity = 0;
let frameSamples = 0;
let sequence = 0;
let initialized = false;

self.onmessage = (event) => {
  const message = event.data;

  if (message?.type !== "init" || initialized) {
    return;
  }

  initialized = true;
  audio = new Float32Array(message.audioSAB);
  state = new Int32Array(message.stateSAB);
  capacity = message.capacity;
  frameSamples = message.frameSamples;

  try {
    validateConfiguration();
    pump();
  } catch (error) {
    self.postMessage({
      type: "error",
      message:
        error instanceof Error
          ? error.message
          : "Unknown audio worker error",
    });
    self.postMessage({ type: "stopped" });
    self.close();
  }
};

function validateConfiguration() {
  if (audio.length !== capacity) {
    throw new Error("Audio worker ring capacity mismatch");
  }

  if (!Number.isInteger(frameSamples) || frameSamples <= 0) {
    throw new Error("Audio worker received invalid frameSamples");
  }
}

function availableSamples() {
  const writeIndex = Atomics.load(
    state,
    STATE_WRITE_INDEX,
  );
  const readIndex = Atomics.load(
    state,
    STATE_READ_INDEX,
  );

  return (
    (writeIndex - readIndex + capacity) % capacity
  );
}

function createPacket(sampleCount) {
  const packet = new ArrayBuffer(
    AUDIO_PACKET_HEADER_BYTES +
      sampleCount * Float32Array.BYTES_PER_ELEMENT,
  );

  const header = new DataView(packet);
  header.setUint32(0, AUDIO_PACKET_MAGIC, true);
  header.setUint32(4, sequence, true);
  header.setUint32(8, sampleCount, true);

  const payload = new Float32Array(
    packet,
    AUDIO_PACKET_HEADER_BYTES,
    sampleCount,
  );

  let readIndex = Atomics.load(
    state,
    STATE_READ_INDEX,
  );

  for (let i = 0; i < sampleCount; i++) {
    payload[i] = audio[readIndex];
    readIndex++;

    if (readIndex === capacity) {
      readIndex = 0;
    }
  }

  Atomics.store(
    state,
    STATE_READ_INDEX,
    readIndex,
  );

  sequence = (sequence + 1) >>> 0;
  return packet;
}

function drain(flushPartialFrame) {
  while (true) {
    const available = availableSamples();

    let sampleCount = 0;

    if (available >= frameSamples) {
      sampleCount = frameSamples;
    } else if (flushPartialFrame && available > 0) {
      sampleCount = available;
    }

    if (sampleCount === 0) {
      return;
    }

    const packet = createPacket(sampleCount);

    // The packet is transferred, so the worker does not retain its
    // backing buffer.
    self.postMessage(
      {
        type: "audio",
        buffer: packet,
      },
      [packet],
    );
  }
}

function pump() {
  while (
    Atomics.load(state, STATE_STOPPED) === 0
  ) {
    drain(false);

    const observedWriteIndex = Atomics.load(
      state,
      STATE_WRITE_INDEX,
    );

    if (availableSamples() < frameSamples) {
      // Atomics.wait is allowed in a dedicated worker, not on the
      // browser main thread. The timeout protects against a missed
      // notification and lets us observe the stopped flag.
      Atomics.wait(
        state,
        STATE_WRITE_INDEX,
        observedWriteIndex,
        50,
      );
    }
  }

  // Normal stop must flush the partial final frame.
  drain(true);

  const droppedSamples = Atomics.load(
    state,
    STATE_OVERRUN_SAMPLES,
  );

  if (droppedSamples > 0) {
    self.postMessage({
      type: "error",
      message:
        `Audio ring overrun: ${droppedSamples} ` +
        "source samples were not captured",
    });
  }

  self.postMessage({ type: "stopped" });
  self.close();
}