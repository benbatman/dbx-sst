import { useCallback, useEffect, useRef, useState } from "react";
import { startMic, MicStream } from "../lib/audio";

type WsMessage =
  | { type: "caption"; text: string }
  | { type: "error"; message: string };

export default function Home() {
  const [recording, setRecording] = useState(false);
  const [caption, setCaption] = useState("");
  const [status, setStatus] = useState("idle");
  const [error, setError] = useState<string | null>(null);

  const wsRef = useRef<WebSocket | null>(null);
  const micRef = useRef<MicStream | null>(null);

  const stop = useCallback(() => {
    micRef.current?.stop();
    micRef.current = null;
    wsRef.current?.close();
    wsRef.current = null;
    setRecording(false);
    setStatus("idle");
  }, []);

  const start = useCallback(async () => {
    setError(null);
    setStatus("connecting");
    const proto = window.location.protocol === "https:" ? "wss" : "ws";
    const ws = new WebSocket(`${proto}://${window.location.host}/ws`);
    ws.binaryType = "arraybuffer";
    wsRef.current = ws;

    ws.onopen = async () => {
      setStatus("listening (first caption may lag on cold start)");
      try {
        micRef.current = await startMic((pcm) => {
          if (ws.readyState === WebSocket.OPEN) ws.send(pcm);
        });
        setRecording(true);
      } catch (e) {
        setError(`Mic error: ${(e as Error).message}`);
        stop();
      }
    };

    ws.onmessage = (ev) => {
      try {
        const msg = JSON.parse(ev.data as string) as WsMessage;
        if (msg.type === "caption") {
          if (msg.text) setCaption(msg.text);
        } else if (msg.type === "error") {
          setError(msg.message);
        }
      } catch {
        /* ignore malformed frames */
      }
    };

    ws.onerror = () => setError("WebSocket error");
    ws.onclose = () => setStatus("idle");
  }, [stop]);

  useEffect(() => () => stop(), [stop]);

  return (
    <main
      style={{
        fontFamily: "system-ui, sans-serif",
        maxWidth: 800,
        margin: "0 auto",
        padding: "2rem",
      }}
    >
      <h1>Parakeet Live Captions</h1>
      <div
        style={{
          background: "#fff7d6",
          border: "1px solid #e6c200",
          borderRadius: 8,
          padding: "0.75rem 1rem",
          margin: "1rem 0",
        }}
      >
        <strong>Cold start:</strong> the endpoint scales to zero when idle. The
        first caption after a period of inactivity can take tens of seconds
        while the model weights load. Captions stay blank until it warms up.
      </div>

      <div style={{ margin: "1.5rem 0" }}>
        {!recording ? (
          <button onClick={start} style={btnStyle}>
            Start captioning
          </button>
        ) : (
          <button onClick={stop} style={{ ...btnStyle, background: "#b00020" }}>
            Stop
          </button>
        )}
        <span style={{ marginLeft: "1rem", color: "#888" }}>{status}</span>
      </div>

      {error && (
        <div style={{ color: "#b00020", marginBottom: "1rem" }}>{error}</div>
      )}

      <div
        style={{
          minHeight: 160,
          background: "#111",
          color: "#fff",
          borderRadius: 8,
          padding: "1.25rem",
          fontSize: "1.5rem",
          lineHeight: 1.5,
        }}
      >
        {caption || <span style={{ color: "#666" }}>captions will appear here&hellip;</span>}
      </div>
    </main>
  );
}

const btnStyle: React.CSSProperties = {
  background: "#0b6bcb",
  color: "#fff",
  border: "none",
  borderRadius: 6,
  padding: "0.6rem 1.2rem",
  fontSize: "1rem",
  cursor: "pointer",
};
