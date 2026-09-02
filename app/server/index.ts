import {createServer, IncomingMessage, ServerResponse } from "http";
import {readFile} from "fs/promises";
import {existsSync} from "fs";
import { join, normalize, extname } from "path";
import { WebSocketServer, WebSocket, type RawData } from "ws";
import { randomUUID  } from "crypto";

const __dirname = import.meta.dirname
const PORT = Number(
    process.env.DATABRICKS_APP_PORT || process.env.PORT || 8080
);

const SERVING_ENDPOINT = process.env.SERVING_ENDPOINT ?? "";
console.assert(SERVING_ENDPOINT !== "", "SERVING_ENDPOINT should not be empty.")

const SAMPLE_RATE = 16_000
const BYTES_PER_SAMPLE = 2; // 16-bit PCM
const MODEL_CHUNK_SECONDS = 0.56;

const MODEL_CHUNK_BYTES = Math.floor(
  MODEL_CHUNK_SECONDS *
    SAMPLE_RATE *
    BYTES_PER_SAMPLE,
);

const MAX_PENDING_SECONDS = 120;

const MAX_PENDING_BYTES =
  MAX_PENDING_SECONDS *
  SAMPLE_RATE *
  BYTES_PER_SAMPLE;

const MAX_BATCH_WINDOWS = 4;

const MIN_SOURCE_SAMPLE_RATE = 8_000;
const MAX_SOURCE_SAMPLE_RATE = 192_000;

// 5.6s left context by default, prepend to each new chunk
const LEFT_CONTEXT_SECONDS = 5.6;
const LEFT_CONTEXT_BYTES = Math.floor(
    LEFT_CONTEXT_SECONDS * SAMPLE_RATE * BYTES_PER_SAMPLE
);

const CLIENT_DIR = join(__dirname, "..", "client", "out");
const HOST = (process.env.DATABRICKS_HOST ?? "").replace(/\/$/, "")
console.assert(HOST !== "", "Databricks HOST cannot be empty")

let cachedToken: { value: string, expiresAt: number } | null = null;

async function fetchM2mToken(clientId: string, clientSecret: string): Promise<{
    value: string,
    expiresAt: number;
}> {
    const resp = await fetch(`${HOST}/oidc/v1/token`, {
        method: "POST",
        headers: {
            "Content-Type": "application/x-www-form-urlencoded",
            Authorization: `Basic ${Buffer.from(`${clientId}:${clientSecret}`).toString("base64")}`
        },
        body: "grant_type=client_credentials&scope=all-apis"
    })
      if (!resp.ok) {
    const detail = await resp.text().catch(() => "");
    throw new Error(`OAuth token exchange ${resp.status}: ${detail.slice(0, 300)}`);
  }
  const data = (await resp.json()) as { access_token: string; expires_in?: number };
  const ttlMs = (data.expires_in ?? 3600) * 1000;
  // Refresh 60s before actual expiry to avoid races.
  return { value: data.access_token, expiresAt: Date.now() + ttlMs - 60_000 };
}

async function getToken(): Promise<string> {
  const pat = process.env.DATABRICKS_TOKEN;
  if (pat) return pat;

  const clientId = process.env.DATABRICKS_CLIENT_ID;
  const clientSecret = process.env.DATABRICKS_CLIENT_SECRET;
  if (!clientId || !clientSecret) {
    throw new Error(
      "No auth available: set DATABRICKS_TOKEN for local dev, or " +
        "DATABRICKS_CLIENT_ID/DATABRICKS_CLIENT_SECRET (injected in-platform).",
    );
  }

  if (cachedToken && cachedToken.expiresAt > Date.now()) {
    return cachedToken.value;
  }
  cachedToken = await fetchM2mToken(clientId, clientSecret);
  if (!cachedToken) {
    throw new Error("Auth token was not retrieved")
  }
  return cachedToken.value;
}

interface TranscriptState {
    committedWords: string[];
    windowWords: string[];
}

interface WordMatch {
    previousStart: number;
    currentStart: number;
    length: number;
}

const MIN_ANCHOR_WORDS = 3;
const MAX_CURRENT_PREFIX_WORDS = 2;

function splitWords(text: string): string[] {
    return text.trim().split(/\s+/).filter(Boolean);
}


function normalizeWord(word: string): string {
  return word
    .toLowerCase()
    .replace(/^[^\p{L}\p{N}']+|[^\p{L}\p{N}']+$/gu, "");
}

/**
 * Find the longest unchanged sequence shared by the previous and current
 * rolling-window hypotheses.
 */

function findBestMatch(
    previousWords: string[],
    currentWords: string[],
): WordMatch {
    const previous = previousWords.map(normalizeWord);
    const current = currentWords.map(normalizeWord);

    let best: WordMatch = {
        previousStart: 0,
        currentStart: 0,
        length: 0
    };

   for (let previousStart = 0; previousStart < previous.length; previousStart++) {
    for (let currentStart = 0; currentStart < current.length; currentStart++) {
      let length = 0;

      while (
        previousStart + length < previous.length &&
        currentStart + length < current.length &&
        previous[previousStart + length] !== "" &&
        previous[previousStart + length] === current[currentStart + length]
      ) {
        length++;
      }

      const isBetter =
        length > best.length ||
        (length === best.length &&
          currentStart < best.currentStart) ||
        (length === best.length &&
          currentStart === best.currentStart &&
          previousStart > best.previousStart);

      if (isBetter) {
        best = { previousStart, currentStart, length };
      }
    }
  }

  return best;
}

function stitchTranscript(
    state: TranscriptState,
    hypothesis: string,
): string {
    const currentWords = splitWords(hypothesis);

    if (currentWords.length === 0) {
        if (state.windowWords.length > 0) {
            state.committedWords.push(...state.windowWords);
            state.windowWords = [];
        }
        return state.committedWords.join(" ");
    }

    if(state.windowWords.length > 0) {
        const match = findBestMatch(state.windowWords, currentWords);

        if (
            match.length >= MIN_ANCHOR_WORDS && match.currentStart <= MAX_CURRENT_PREFIX_WORDS
        ) {
            const wordsLeavingWindow = state.windowWords.slice(
                0, match.previousStart
            );

            state.committedWords.push(...wordsLeavingWindow);
        }
    }

    state.windowWords = currentWords;

    return [...state.committedWords, ...state.windowWords].join(" ");
}

function encodeWav(pcm: Buffer, sampleRate = SAMPLE_RATE): Buffer {
    const numChannels = 1;
    const bitsPerSample = 16;
    const byteRate = (sampleRate * numChannels * bitsPerSample) / 8;
    const blockAlign = (numChannels * bitsPerSample) / 8;
    const dataSize = pcm.length;

    const header = Buffer.alloc(44);
    header.write("RIFF", 0);
    header.writeUint32LE(36 + dataSize, 4);
    header.write("WAVE", 8);
    header.write("fmt ", 12);
    header.writeUInt32LE(16, 16); // PCM fmt chunk size
    header.writeUInt16LE(1, 20); // audio format = PCM
    header.writeUInt16LE(numChannels, 22);
    header.writeUInt32LE(sampleRate, 24);
    header.writeUInt32LE(byteRate, 28);
    header.writeUInt16LE(blockAlign, 32);
    header.writeUInt16LE(bitsPerSample, 34);
    header.write("data", 36);
    header.writeUInt32LE(dataSize, 40);

    return Buffer.concat([header, pcm]);
}

// Serving Endpoint Call
const SERVING_REQUEST_TIMEOUT_MS = 180_000;
const SERVING_MAX_ATTEMPTS = 3;

function delay(ms: number): Promise<void> {
    return new Promise((resolve) => {
        setTimeout(resolve, ms)
    });
}

function predToText(
    prediction: unknown,
): string {
    if (typeof prediction === "string") {
        return prediction;
    }

    if (
        prediction !== null &&
        typeof prediction === 'object' &&
        "text" in prediction
    ) {
        return String(
            (prediction as {text: unknown}).text ?? "",
        );
    }

    return "";
}

async function transcribeWindows(windows: readonly Buffer[],
    clientRequestId: string,
): Promise<string[]> {
      if (!HOST || !SERVING_ENDPOINT) {
    throw new Error("Missing DATABRICKS_HOST or SERVING_ENDPOINT config");
  }

  if (windows.length === 0) {
    return [];    
}

    const token = await getToken()
    const url = `${HOST}/serving-endpoints/${SERVING_ENDPOINT}/invocations`;
    
    // pyfunc DataFrame contract: dataframe_records with the `audio_b64` column
    const body = JSON.stringify({
        client_request_id: clientRequestId,
        dataframe_split: {
            columns: ['audio_b64'],
            data: windows.map((windowPcm) => [
                encodeWav(windowPcm).toString("base64")
            ]),
        },
    });

    for (
        let attempt = 1;
        attempt <= SERVING_MAX_ATTEMPTS;
        attempt++
    ) {
        const controller = new AbortController();

        const timeoutHandle = setTimeout(() => {
            controller.abort();
        }, SERVING_REQUEST_TIMEOUT_MS);

        let response: Response;

        try {
            response = await fetch(url, {
                method: "POST",
                headers: {
                Authorization: `Bearer ${token}`,
                "Content-Type": "application/json",
                }, 
                body, 
                signal: controller.signal
            });
        } catch (error) {
            clearTimeout(timeoutHandle);

            if (attempt === SERVING_MAX_ATTEMPTS) {
                throw new Error(
                `Serving endpoint request failed: ${
                (error as Error).message
                }`,
                );
            }

            await delay(250 * 2 ** (attempt - 1));
            continue;
        }

        clearTimeout(timeoutHandle);

        if (!response.ok) {
            const detail = await response.text().catch(() => "");

            const retryable = response.status === 429 ||
                                response.status >= 500;

            if (
                retryable &&
                attempt < SERVING_MAX_ATTEMPTS
            ) {
                await delay(
                    250 * 2 ** (attempt - 1),
                );
                continue;
            }

            throw new Error(
                `Serving endpoint ${response.status}: ` +
                detail.slice(0, 500),
            );
        }

        const data: unknown = await response.json()

        const predictions = 
        (
            data as {
                predictions?: unknown;
            }
        ).predictions ?? data 

        if (!Array.isArray(predictions)) {
            throw new Error(
                "Serving endpoint returned no predictions array"
            );
        }

        if (
            predictions.length !== windows.length
        ) {
            throw new Error(
                `Serving endpoint returned ` +
                `${predictions.length} predictions for ` +
                `${windows.length} windows`,
            )
        }

        return predictions.map(
            predToText,
        );
    }

    throw new Error(
        "Serving endpoint retry loop exhausted"
    );
}

// HTTP static server 

const CROSS_ORIGIN_ISOLATION_HEADERS = {
  "Cross-Origin-Opener-Policy": "same-origin",
  "Cross-Origin-Embedder-Policy": "require-corp",
  "Cross-Origin-Resource-Policy": "same-origin",
} as const; // const assertion, doesn't affect runtime at all

const MIME: Record<string, string> = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".svg": "image/svg+xml",
  ".ico": "image/x-icon",
  ".png": "image/png",
  ".txt": "text/plain; charset=utf-8",
};

async function serveStatic(req: IncomingMessage, res: ServerResponse) {
    const urlPath = (req.url ?? "/").split("?")[0];
    let rel = urlPath === "/" ? "/index.html" : urlPath;

    // Prevent path traversal.
    const filePath = normalize(join(CLIENT_DIR, rel));
    if (!filePath.startsWith(normalize(CLIENT_DIR))) {
    res.writeHead(403);
    res.end("forbidden");
    return;
    }

    let target = filePath;
    if (!existsSync(target)) {
    const htmlCandidate = filePath.endsWith(".html")
      ? filePath
      : `${filePath}.html`;
    target = existsSync(htmlCandidate)
      ? htmlCandidate
      : join(CLIENT_DIR, "index.html");
    }

    try {
        const content = await readFile(target);
        res.writeHead(200, {
            ...CROSS_ORIGIN_ISOLATION_HEADERS,
            "Content-Type": MIME[extname(target)] ?? "application/octet-stream",
      });
      res.end(content);
    } catch {
        res.writeHead(404);
        res.end("not found");
    }
}

const server = createServer((req, res) => {
    if ((req.url ?? "").startsWith("/health")) {
        res.writeHead(200);
        res.end("ok");
        return;
    }
    void serveStatic(req, res);
});

class StreamingLinearResampler {
    private readonly ratio: number;
    private sourceSamplesSeen = 0;
    private outputSamplesProduced = 0;
    private previousSample: number | null = null;
    private flushed = false;

    constructor(
        private readonly sourceRate: number,
        private readonly targetRate: number
    ) {
        if (
            !Number.isFinite(sourceRate) ||
            sourceRate <= 0 ||
            !Number.isFinite(targetRate) ||
            targetRate <= 0
        ) {
            throw new Error(
                "Resampler rates must be positive"
            );
        }

        this.ratio = sourceRate / targetRate;
    }

    push(input: Float32Array): Float32Array {
        if (this.flushed) {
            throw new Error(
            "Cannot push audio after resampler flush");
        }

        if (input.length === 0) {
            return new Float32Array(0);
        }

        const chunkStart = this.sourceSamplesSeen;
        const chunkEnd = chunkStart + input.length - 1;

        // The extra slots cover the boundary sample retained from
        // the previous chunk and rounding differences
        const maximumOutput = Math.max(
            4, 
            Math.ceil(
                (input.length * this.targetRate) /
                this.sourceRate,
            ) + 4,
        );

        const output = new Float32Array(
            maximumOutput,
        );

        let outputLength = 0;

        while (true) {
            const sourcePosition = 
            this.outputSamplesProduced * this.ratio;

            const sourceIndex0 = Math.floor(
                sourcePosition,
            );
            const sourceIndex1 = sourceIndex0 + 1;

            // Hold the final source sample until the next input chunk
            // supplies the sample on the other side of interpolation
            if (sourceIndex1 > chunkEnd) {
                break;
            }

            const sample0 = this.sampleAt(
                sourceIndex0, 
                chunkStart, 
                input
            );

            const sample1 = this.sampleAt(
                sourceIndex1, 
                chunkStart, 
                input
            );

            const fraction = sourcePosition - sourceIndex0;

            output[outputLength] = sample0 + (sample1 - sample0) * fraction;

            outputLength++;
            this.outputSamplesProduced++
        }

        this.sourceSamplesSeen += input.length;
        this.previousSample = input[input.length - 1];

        return output.slice(0, outputLength);
    }

    flush() : Float32Array {
        if (this.flushed) {
            return new Float32Array(0);
        }

        this.flushed = true;

        if (
            this.previousSample == null ||
            this.sourceSamplesSeen === 0
        ) {
            return new Float32Array(0);
        }

        const lastSourceIndex = this.sourceSamplesSeen - 1;

        const output: number[] = [];

        while (true) {
            const sourcePosition = this.outputSamplesProduced * this.ratio;

            if (
                sourcePosition > lastSourceIndex + Number.EPSILON
            ) {
                break;
            }

            // At flush time, the only remaining interpolation point
            // is at the final source sample. Clamp to that sample.
            output.push(this.previousSample);
            this.outputSamplesProduced++;
        }

        return Float32Array.from(output)
    }

    private sampleAt(
        sourceIndex: number, 
        chunkStart: number, 
        input: Float32Array,
    ): number {
        if (
            sourceIndex === chunkStart - 1 &&
            this.previousSample !== null
        ) {
            return this.previousSample;
        }

        const localIndex = sourceIndex - chunkStart;

        if (
            localIndex < 0 || localIndex >= input.length
        ) {
            throw new Error(
                "Resampler requested unavailable history"
            );
        }

        return input[localIndex];
    }
}

function floatToPcm16(
    samples: Float32Array
): Buffer {
    const pcm = Buffer.allocUnsafe(
        samples.length * BYTES_PER_SAMPLE
    );

    for (
        let index = 0;
        index < samples.length;
        index++
    ) {
        const input = Number.isFinite(samples[index])
        ? samples[index]
        : 0;

        const clipped = Math.max(
            -1, 
            Math.min(1, input)
        );

        const value = 
        clipped < 0
        ? Math.round(clipped * 0x8000)
        : Math.round(clipped * 0x7fff);

        pcm.writeInt16LE(value, index * BYTES_PER_SAMPLE);
    }

    return pcm;
}

class BufferQueue {
    private chunks: Buffer[] = [];
    private headIndex = 0;
    private headOffset = 0;
    private totalBytes = 0;


    get byteLength(): number {
        return this.totalBytes;
    }

    push(chunk: Buffer): void {
        if (chunk.length === 0) {
            return;
        }

        this.chunks.push(chunk);
        this.totalBytes += chunk.length;
    }

    take(byteCount: number): Buffer {
        if (
            !Number.isInteger(byteCount) ||
            byteCount < 0
        ) {
            throw new Error(
                "BufferQueue byteCount must be non-negative"
            );
        }

        const output = Buffer.allocUnsafe(byteCount);
        let outputOffset = 0;

        while (outputOffset < byteCount) {
            const chunk = this.chunks[this.headIndex];

            const availableInHead = chunk.length - this.headOffset;

            const copyLength = Math.min(
                availableInHead,
                byteCount - outputOffset
            );

            chunk.copy(
                output, 
                outputOffset, 
                this.headOffset, 
                this.headOffset + copyLength
            );

            outputOffset += copyLength;
            this.headOffset += copyLength;

            if (this.headOffset === chunk.length) {
                this.headIndex++;
                this.headOffset = 0;
            }
        }

        this.totalBytes -= byteCount;
        this.compact();

        return output;
    }

    takeAll(): Buffer {
        return this.take(this.totalBytes);
    }

    private compact(): void {
        if (
            this.headIndex >= 64 ||
            this.headIndex * 2 >= this.chunks.length
        ) {
            this.chunks.splice(0, this.headIndex);
            this.headIndex = 0;
        }

        if (this.totalBytes === 0) {
            this.chunks = [];
            this.headIndex = 0;
            this.headOffset = 0;
        }
    }
}

// binary packet decoder

// Special constants to define and validate custom binary audio packet
// Sent from browser worker to server

const AUDIO_PACKET_MAGIC = 0x31445541;
const AUDIO_PACKET_HEADER_BYTES = 12;
const MAX_SOURCE_FRAME_SAMPLES = 200_000;

interface DecodedAudioPacket {
    sequence: number;
    samples: Float32Array;
}

function decodeAudioPacket(
    raw: Buffer,
): DecodedAudioPacket {
    if (
        raw.length < AUDIO_PACKET_HEADER_BYTES
    ) {
        throw new Error(
            "Audio packet is shorter than its header"
        );
    }
    const magic = raw.readUInt32LE(0);

    if (magic !== AUDIO_PACKET_MAGIC) {
        throw new Error(
            "Audio packet has an invalid magic value"
        );
    }

    const sequence = raw.readUInt32LE(4);
    const sampleCount = raw.readUInt32LE(8);

    if (
        sampleCount === 0 ||
        sampleCount > MAX_SOURCE_FRAME_SAMPLES
    ) {
        throw new Error(
            "Invalid audio packet sample count: ${sampleCount}"
        );
    }

    const expectedBytes =
    AUDIO_PACKET_HEADER_BYTES +
    sampleCount * Float32Array.BYTES_PER_ELEMENT;

    if (raw.length !== expectedBytes) {
        throw new Error(
        `Audio packet length mismatch: expected ` +
        `${expectedBytes}, received ${raw.length}`,
        );
    }

    // Decode explicitly as little-endian rather than depending on 
    // the host machine's type-array byte order
    const samples = new Float32Array(sampleCount);

    for (
        let index = 0;
        index < sampleCount;
        index++
    ) {
        samples[index] = raw.readFloatLE(
            AUDIO_PACKET_HEADER_BYTES + index * Float32Array.BYTES_PER_ELEMENT
        );
    }

    return {
        sequence, samples,
    }
}

interface StartControlMessage {
  type: "start";
  version: 1;
  format: "f32le";
  sampleRate: number;
  channels: 1;
}

interface EndControlMessage {
  type: "end";
}

type ControlMessage =
  | StartControlMessage
  | EndControlMessage;

interface PendingAudio {
  id: number;
  pcm: Buffer;
}

function rawDataToBuffer(
  raw: RawData,
): Buffer {
  if (Buffer.isBuffer(raw)) {
    return raw;
  }

  if (Array.isArray(raw)) {
    return Buffer.concat(raw);
  }

  return Buffer.from(raw);
}

function copyTailBytes(
  buffer: Buffer,
  maximumBytes: number,
): Buffer<ArrayBuffer> {
  return Buffer.from(
    tailBytes(buffer, maximumBytes),
  );
}

const wss = new WebSocketServer({
  server,
  path: "/ws",
});

wss.on(
  "connection",
  (ws: WebSocket) => {
    const sessionId = randomUUID();

    let started = false;
    let endRequested = false;
    let finalSent = false;
    let failed = false;
    let socketClosed = false;

    let expectedSequence = 0;
    let nextPendingId = 0;

    let resampler:
      | StreamingLinearResampler
      | null = null;

    const pcmQueue = new BufferQueue();

    // Only successfully inferred audio goes here.
    let leftContext = Buffer.alloc(0);

    // Only never-yet-committed model chunks go here.
    const pending: PendingAudio[] = [];
    let pendingHead = 0;
    let pendingBytes = 0;
    let draining = false;

    const transcriptState: TranscriptState = {
      committedWords: [],
      windowWords: [],
    };

    function sendJson(
      value: object,
    ): void {
      if (
        ws.readyState === WebSocket.OPEN
      ) {
        ws.send(JSON.stringify(value));
      }
    }

    function failSession(
      error: unknown,
    ): void {
      if (failed) {
        return;
      }

      failed = true;

      const message =
        error instanceof Error
          ? error.message
          : "Unknown ASR session error";

      sendJson({
        type: "error",
        message,
      });

      if (
        ws.readyState === WebSocket.OPEN
      ) {
        ws.close(1011, "ASR session failed");
      }
    }

    function enqueueModelAudio(
      pcm: Buffer,
    ): void {
      if (pcm.length === 0) {
        return;
      }

      if (
        pcm.length % BYTES_PER_SAMPLE !==
        0
      ) {
        throw new Error(
          "PCM chunk is not sample-aligned",
        );
      }

      if (
        pendingBytes + pcm.length >
        MAX_PENDING_BYTES
      ) {
        throw new Error(
          `ASR backlog exceeded ` +
            `${MAX_PENDING_SECONDS} seconds`,
        );
      }

      pending.push({
        id: nextPendingId,
        pcm,
      });

      nextPendingId++;
      pendingBytes += pcm.length;

      void drainPending();
    }

    function emitCompleteModelChunks(): void {
      while (
        pcmQueue.byteLength >=
        MODEL_CHUNK_BYTES
      ) {
        enqueueModelAudio(
          pcmQueue.take(MODEL_CHUNK_BYTES),
        );
      }
    }

    function acceptAudioPacket(
      raw: Buffer,
    ): void {
      if (!started || !resampler) {
        throw new Error(
          "Audio received before start control message",
        );
      }

      if (endRequested) {
        throw new Error(
          "Audio received after end control message",
        );
      }

      const packet = decodeAudioPacket(raw);

      if (
        packet.sequence !==
        expectedSequence
      ) {
        throw new Error(
          `Audio sequence mismatch: expected ` +
            `${expectedSequence}, received ` +
            `${packet.sequence}`,
        );
      }

      const resampled = resampler.push(
        packet.samples,
      );

      const pcm = floatToPcm16(resampled);
      pcmQueue.push(pcm);
      emitCompleteModelChunks();

      expectedSequence =
        (expectedSequence + 1) >>> 0;
    }

    function handleStart(
      message: StartControlMessage,
    ): void {
      if (started) {
        throw new Error(
          "Duplicate start control message",
        );
      }

      if (
        message.version !== 1 ||
        message.format !== "f32le" ||
        message.channels !== 1
      ) {
        throw new Error(
          "Unsupported audio stream format",
        );
      }

      if (
        !Number.isInteger(
          message.sampleRate,
        ) ||
        message.sampleRate <
          MIN_SOURCE_SAMPLE_RATE ||
        message.sampleRate >
          MAX_SOURCE_SAMPLE_RATE
      ) {
        throw new Error(
          `Unsupported source sample rate: ` +
            `${message.sampleRate}`,
        );
      }

      resampler =
        new StreamingLinearResampler(
          message.sampleRate,
          SAMPLE_RATE,
        );

      started = true;

      sendJson({
        type: "ready",
      });
    }

    function handleEnd(): void {
      if (!started || !resampler) {
        throw new Error(
          "End received before start",
        );
      }

      if (endRequested) {
        return;
      }

      endRequested = true;

      const finalResampled =
        resampler.flush();

      pcmQueue.push(
        floatToPcm16(finalResampled),
      );

      emitCompleteModelChunks();

      if (pcmQueue.byteLength > 0) {
        enqueueModelAudio(
          pcmQueue.takeAll(),
        );
      }

      void drainPending();
    }

    function parseControlMessage(
      raw: Buffer,
    ): ControlMessage {
      let value: unknown;

      try {
        value = JSON.parse(
          raw.toString("utf8"),
        );
      } catch {
        throw new Error(
          "Malformed WebSocket control message",
        );
      }

      if (
        value === null ||
        typeof value !== "object" ||
        !("type" in value)
      ) {
        throw new Error(
          "Invalid WebSocket control message",
        );
      }

      const type = (
        value as {
          type: unknown;
        }
      ).type;

      if (type === "start") {
        return value as StartControlMessage;
      }

      if (type === "end") {
        return value as EndControlMessage;
      }

      throw new Error(
        `Unknown control message: ${String(type)}`,
      );
    }

    function finishIfReady(): void {
      if (
        !endRequested ||
        finalSent ||
        pendingHead < pending.length ||
        draining ||
        failed
      ) {
        return;
      }

      const finalText = stitchTranscript(
        transcriptState,
        "",
      );

      finalSent = true;

      sendJson({
        type: "final",
        text: finalText,
      });
    }

    function compactPending(): void {
      if (
        pendingHead >= 64 ||
        pendingHead * 2 >=
          pending.length
      ) {
        pending.splice(0, pendingHead);
        pendingHead = 0;
      }

      if (pendingHead === pending.length) {
        pending.length = 0;
        pendingHead = 0;
      }
    }

    async function drainPending(): Promise<void> {
      if (
        draining ||
        failed ||
        socketClosed
      ) {
        return;
      }

      draining = true;

      try {
        while (
          pendingHead < pending.length &&
          !failed &&
          !socketClosed
        ) {
          const batchLength = Math.min(
            MAX_BATCH_WINDOWS,
            pending.length - pendingHead,
          );

          const items = pending.slice(
            pendingHead,
            pendingHead + batchLength,
          );

          const windows: Buffer[] = [];

          // This is candidate context. It is not committed to
          // leftContext until the whole REST request succeeds.
          let candidateContext =
            leftContext;

          for (const item of items) {
            const windowPcm = Buffer.concat(
              [
                candidateContext,
                item.pcm,
              ],
              candidateContext.length +
                item.pcm.length,
            );

            windows.push(windowPcm);

            candidateContext =
              copyTailBytes(
                windowPcm,
                LEFT_CONTEXT_BYTES,
              );
          }

          const firstId = items[0].id;
          const lastId =
            items[items.length - 1].id;

          const requestId =
            `${sessionId}:` +
            `${firstId}-${lastId}`;

          const texts =
            await transcribeWindows(
              windows,
              requestId,
            );

          if (socketClosed || failed) {
            return;
          }

          // The request succeeded. Only now do we commit its
          // acoustic context, transcript state, and queue position.
          leftContext = candidateContext;

          let latestText = "";

          for (const text of texts) {
            latestText = stitchTranscript(
              transcriptState,
              text,
            );
          }

          for (const item of items) {
            pendingBytes -= item.pcm.length;
          }

          pendingHead += items.length;
          compactPending();

          if (latestText) {
            sendJson({
              type: "caption",
              text: latestText,
            });
          }
        }
      } catch (error) {
        failSession(error);
      } finally {
        draining = false;

        if (
          !failed &&
          !socketClosed &&
          pendingHead < pending.length
        ) {
          void drainPending();
          return;
        }

        finishIfReady();
      }
    }

    ws.on(
      "message",
      (
        raw: RawData,
        isBinary: boolean,
      ) => {
        if (failed || socketClosed) {
          return;
        }

        try {
          const buffer =
            rawDataToBuffer(raw);

          if (isBinary) {
            acceptAudioPacket(buffer);
            return;
          }

          const message =
            parseControlMessage(buffer);

          if (message.type === "start") {
            handleStart(message);
          } else {
            handleEnd();
          }
        } catch (error) {
          failSession(error);
        }
      },
    );

    ws.on("close", () => {
      socketClosed = true;
      failed = true;

      pending.length = 0;
      pendingHead = 0;
      pendingBytes = 0;
    });

    ws.on("error", () => {
      socketClosed = true;
      failed = true;
    });
  },
);

function tailBytes(buf: Buffer, maxBytes: number): Buffer {
    if (buf.length <= maxBytes) return buf;
    // keep sample alignment (2 bytes per 16-bit sample)
    const start = buf.length - maxBytes;
    const aligned = start + (start % BYTES_PER_SAMPLE);
    return buf.subarray(aligned);
}

server.listen(PORT, () => {
  console.log(`[server] listening on :${PORT}`);
  console.log(`[server] serving endpoint: ${SERVING_ENDPOINT || "(unset)"}`);
})