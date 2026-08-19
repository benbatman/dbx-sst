import {createServer, IncomingMessage, ServerResponse } from "http";
import {readFile} from "fs/promises";
import {existsSync} from "fs";
import { join, normalize, extname } from "path";
import { WebSocketServer, WebSocket } from "ws";

const __dirname = import.meta.dirname
const PORT = Number(
    process.env.DATABRICKS_APP_PORT || process.env.PORT || 8080
);

const SERVING_ENDPOINT = process.env.SERVING_ENDPOINT ?? "";
console.assert(SERVING_ENDPOINT !== "", "SERVING_ENDPOINT should not be empty.")

const SAMPLE_RATE = 16_000
const BYTES_PER_SAMPLE = 2; // 16-bit PCM

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

async function transcribeWindow(wavBase64: string): Promise<string> {
      if (!HOST || !SERVING_ENDPOINT) {
    throw new Error("Missing DATABRICKS_HOST or SERVING_ENDPOINT config");
  }
    const token = await getToken()
    const url = `${HOST}/serving-endpoints/${SERVING_ENDPOINT}/invocations`;
    // pyfunc DataFrame contract: dataframe_records with the `audio_b64` column
    const body = JSON.stringify({
        dataframe_records: [{audio_b64: wavBase64}]
    });

    const resp = await fetch(url, {
        method: "POST",
        headers: {
            Authorization: `Bearer ${token}`,
            "Content-Type": "application/json"
        }, 
        body
    });

    if (!resp.ok) {
        const detail = await resp.text().catch(() => "");
        throw new Error(`Serving endpoint ${resp.status}, ${detail}`);
    }

    const data: unknown = await resp.json();
    console.log("Returned data: ", JSON.stringify(data))

    const preds = (data as {predictions?: unknown}).predictions ?? data;
    if (Array.isArray(preds) && preds.length > 0) {
        const first = preds[0];
        if (typeof first === 'string') return first; 
        if (typeof first === 'object' && "text" in first) {
            return String((first as {text: unknown}).text ?? "")
        }
    }
    return "";
}

// HTTP static server 

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

// WebSocket relay 

const wss = new WebSocketServer({ server, path: "/ws"});

wss.on("connection", (ws: WebSocket) => {
    // Per-conn rolling left context and full transcript
    let leftContext: Buffer = Buffer.alloc(0);
    let busy = false;

    const transcriptState: TranscriptState = {
        committedWords: [],
        windowWords: []
    };


    ws.on("message", async (raw: Buffer, isBinary: boolean) => {
        if (!isBinary) return; //control/text frames ignored
        const chunk = Buffer.from(raw);
        if (chunk.length === 0) return;

        if (busy) {
            leftContext = tailBytes(
                Buffer.concat([leftContext, chunk]),
                LEFT_CONTEXT_BYTES
            );
            return;
        }

        busy = true;
        const windowPcm = Buffer.concat([leftContext, chunk]);
        leftContext = tailBytes(windowPcm, LEFT_CONTEXT_BYTES);

        try {
            const wav = encodeWav(windowPcm);
            const windowText = await transcribeWindow(wav.toString("base64"));
            const fullText = stitchTranscript(transcriptState, windowText);
            if (ws.readyState === ws.OPEN && fullText) {
                ws.send(JSON.stringify({ type: "caption", "text": fullText}));
            }
        } catch (err) {
            if (ws.readyState === ws.OPEN) {
                ws.send(JSON.stringify({
                    type: "error",
                    message: (err as Error).message,
                }));
            }
        } finally {
            busy = false;
        }
    });
});

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