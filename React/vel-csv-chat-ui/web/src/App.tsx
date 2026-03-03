import { useMemo, useState } from "react";
import "./App.css";

type Msg = { role: "user" | "assistant"; text: string; ts: number };
type Thread = { id: string; title: string; messages: Msg[]; sessionId?: string };

function uid() {
  return Math.random().toString(36).slice(2);
}

export default function App() {
  const [threads, setThreads] = useState<Thread[]>(() => [
    { id: uid(), title: "New chat", messages: [], sessionId: "" },
  ]);
  const [activeId, setActiveId] = useState(threads[0].id);
  const [input, setInput] = useState("");
  const [busy, setBusy] = useState(false);

  const active = useMemo(
    () => threads.find((t) => t.id === activeId)!,
    [threads, activeId]
  );

  function newThread() {
    const t: Thread = { id: uid(), title: "New chat", messages: [], sessionId: "" };
    setThreads((p) => [t, ...p]);
    setActiveId(t.id);
  }

  async function send() {
    const text = input.trim();
    if (!text || busy) return;

    setBusy(true);
    setInput("");

    const userMsg: Msg = { role: "user", text, ts: Date.now() };
    setThreads((prev) =>
      prev.map((t) =>
        t.id === activeId
          ? {
            ...t,
            title: t.messages.length === 0 ? text.slice(0, 40) : t.title,
            messages: [...t.messages, userMsg],
          }
          : t
      )
    );

    try {
      const payload = {
        user_id: "web-user-1",
        session_id: active.sessionId || "",
        user_message: text,
      };

      const resp = await fetch("/api/query", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      const j = await resp.json();

      // persist returned session_id into the active thread
      if (j.session_id) {
        setThreads((prev) =>
          prev.map((t) => (t.id === activeId ? { ...t, sessionId: j.session_id } : t))
        );
      }

      const assistantText: string =
        j.assistant_text ||
        (j.raw ? JSON.stringify(j.raw, null, 2) : JSON.stringify(j, null, 2));

      const asstMsg: Msg = { role: "assistant", text: assistantText, ts: Date.now() };

      setThreads((prev) =>
        prev.map((t) =>
          t.id === activeId ? { ...t, messages: [...t.messages, asstMsg] } : t
        )
      );
    } catch (e: any) {
      const errMsg: Msg = {
        role: "assistant",
        text: `Error: ${e?.message || String(e)}`,
        ts: Date.now(),
      };
      setThreads((prev) =>
        prev.map((t) =>
          t.id === activeId ? { ...t, messages: [...t.messages, errMsg] } : t
        )
      );
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="layout">
      <aside className="sidebar">
        <div className="brand">Velocity AI</div>

        <button className="btn primary" onClick={newThread}>
          + New chat
        </button>

        <div className="thread-list">
          {threads.map((t) => (
            <button
              key={t.id}
              className={"thread " + (t.id === activeId ? "active" : "")}
              onClick={() => setActiveId(t.id)}
              title={t.title}
            >
              {t.title || "New chat"}
            </button>
          ))}
        </div>
      </aside>

      <main className="main">
        <div className="messages">
          {active.messages.length === 0 ? (
            <div className="empty">Ask about missed topics, invalid transcripts, table freshness…</div>
          ) : (
            active.messages.map((m, i) => (
              <div key={i} className={"msg " + m.role}>
                <div className="bubble">{m.text}</div>
              </div>
            ))
          )}
        </div>

        <div className="composer">
          <input
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => (e.key === "Enter" ? send() : null)}
            placeholder="Ask me anything…"
            disabled={busy}
          />
          <button className="btn" onClick={send} disabled={busy || !input.trim()}>
            {busy ? "…" : "Send"}
          </button>
        </div>
      </main>
    </div>
  );
}