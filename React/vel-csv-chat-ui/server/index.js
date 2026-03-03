import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import { GoogleAuth } from "google-auth-library";
dotenv.config();
const app = express();
app.use(cors());
app.use(express.json({ limit: "2mb" }));
const PORT = process.env.PORT || 8080;
const BQ_AGENT_QUERY_URL = process.env.BQ_AGENT_QUERY_URL;
if (!BQ_AGENT_QUERY_URL) {
    console.warn("WARNING: BQ_AGENT_QUERY_URL is not set");
}
const auth = new GoogleAuth({
    scopes: ["https://www.googleapis.com/auth/cloud-platform"],
});
app.get("/api/health", (_, res) => res.json({ ok: true }));
async function callEngine(client, classMethod, input) {
    const resp = await client.request({
        url: BQ_AGENT_QUERY_URL,
        method: "POST",
        headers: { "Content-Type": "application/json" },
        data: { classMethod, input },
    });
    return resp.data;
}
function extractAssistantText(anyObj) {
    const obj = anyObj?.output ?? anyObj;
    // common locations
    const events = obj?.events ?? obj?.output?.events;
    if (Array.isArray(events)) {
        // scan from end for latest assistant-ish content
        for (let i = events.length - 1; i >= 0; i--) {
            const e = events[i];
            const candidates = [
                e?.content?.text,
                e?.content,
                e?.text,
                e?.message?.content,
                e?.message?.text,
                e?.delta?.text,
            ];
            for (const c of candidates) {
                if (typeof c === "string" && c.trim()) return c;
            }
            // sometimes content is structured
            for (const c of candidates) {
                if (c && typeof c === "object") return JSON.stringify(c, null, 2);
            }
        }
    }
    // sometimes there is direct output text
    if (typeof obj?.text === "string") return obj.text;
    if (typeof obj?.output?.text === "string") return obj.output.text;
    return "";
}
async function createSessionIfNeeded(client, userId, sessionId) {
    if (sessionId) return sessionId;
    // Your earlier screenshots show create_session works.
    const createResp = await callEngine(client, "create_session", { user_id: userId });
    const sid =
        createResp?.output?.id ||
        createResp?.id ||
        createResp?.output?.session_id ||
        createResp?.session_id;
    if (!sid) {
        throw new Error("Failed to create session: " + JSON.stringify(createResp));
    }
    return sid;
}
/**
* Try query methods / param names because different ADK apps wire these differently.
* We attempt:
* - async_stream_query
* - stream_query
* and within each, we try query field names:
* - query
* - user_message
* - message
* - text
*/
async function runQuery(client, userId, sessionId, userMessage) {
    const methodsToTry = ["async_stream_query", "stream_query"];
    const payloadsToTry = [
        { user_id: userId, session_id: sessionId, query: userMessage },
        { user_id: userId, session_id: sessionId, user_message: userMessage },
        { user_id: userId, session_id: sessionId, message: userMessage },
        { user_id: userId, session_id: sessionId, text: userMessage },
    ];
    let lastErr = null;
    for (const m of methodsToTry) {
        for (const input of payloadsToTry) {
            try {
                const resp = await callEngine(client, m, input);
                return { method: m, input, resp };
            } catch (e) {
                lastErr = e;
                // continue trying other combinations
            }
        }
    }
    // surface the last useful error
    const errData = lastErr?.response?.data || lastErr?.message || String(lastErr);
    throw new Error(typeof errData === "string" ? errData : JSON.stringify(errData));
}
app.post("/api/query", async (req, res) => {
    try {
        const client = await auth.getClient();
        const userId = req.body.user_id || "web-user-1";
        let sessionId = req.body.session_id || "test-session";
        const userMessage = req.body.user_message;
        if (!userMessage || !String(userMessage).trim()) {
            return res.status(400).json({ error: "user_message is required" });
        }
        // 1) Run query directly
        const payload = { session_id: sessionId, query: userMessage };
        const queryResp = await callEngine(client, "query", payload);
        
        const assistantText = extractAssistantText(queryResp);
        return res.json({
            session_id: sessionId,
            assistant_text: assistantText || "",
            raw: queryResp, // keep for debugging
            debug_used: { method: "query", input: payload }, 
        });
    } catch (e) {
        const errData = e?.response?.data || e?.message || String(e);
        return res.status(e?.response?.status || 500).json({ error: errData });
    }
});
app.listen(PORT, () => {
    console.log(`Server listening on ${PORT}`);
    console.log(`BQ_AGENT_QUERY_URL: ${BQ_AGENT_QUERY_URL}`);
});