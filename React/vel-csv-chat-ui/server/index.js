import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import { GoogleAuth } from "google-auth-library";
import fetch from "node-fetch";

dotenv.config();
console.log("GOOGLE_APPLICATION_CREDENTIALS:", process.env.GOOGLE_APPLICATION_CREDENTIALS);

const app = express();
app.use(cors());
app.use(express.json({ limit: "2mb" }));

const PORT = process.env.PORT || 8080;

// Put the FULL query URL you showed in browser address bar
// Example:
// https://us-central1-aiplatform.googleapis.com/v1/projects/PROJECT/locations/us-central1/reasoningEngines/ENGINE_ID:query
const BQ_AGENT_QUERY_URL = process.env.BQ_AGENT_QUERY_URL;
const BQ_AGENT_STREAM_URL = process.env.BQ_AGENT_STREAM_URL; // optional (alt=sse endpoint)

const auth = new GoogleAuth({
    scopes: ["https://www.googleapis.com/auth/cloud-platform"],
});

app.get("/api/health", (req, res) => res.json({ ok: true }));

/**
 * Non-streaming query
 * The safest approach: pass-through the request body to the agent query endpoint.
 * Your UI can send the exact payload you want the agent to receive.
 */
app.post("/api/query", async (req, res) => {
    try {
        if (!BQ_AGENT_QUERY_URL) {
            return res.status(500).json({ error: "BQ_AGENT_QUERY_URL not configured" });
        }

        console.log("HIT /api/query");
        const client = await auth.getClient();
        const authHeaders = await client.getRequestHeaders();

        console.log("Auth Keys:", Object.keys(authHeaders));
        console.log("Auth Header Present:", !!(authHeaders.Authorization || authHeaders.authorization));
        console.log("Auth Heade Sample:", (authHeaders.Authorization || authHeaders.authorization || "").slice(0, 20));
        console.log("Auth header present?", !!authHeaders.authorization);

        const upstream = await fetch(BQ_AGENT_QUERY_URL, {
            method: "POST",
            headers: {
                ...authHeaders,
                "Content-Type": "application/json",
            },
            body: JSON.stringify(req.body),
        });

        const text = await upstream.text();
        res.status(upstream.status).type("application/json").send(text);
    } catch (e) {
        console.error(e);
        res.status(500).json({ error: String(e) });
    }
});


/**
 * Streaming proxy (SSE)
 * If your Agent Engine provides a streamQuery URL (?alt=sse), we pipe it through.
 */


app.post("/api/stream", async (req, res) => {
    try {
        if (!BQ_AGENT_STREAM_URL) {
            return res.status(500).json({ error: "BQ_AGENT_STREAM_URL not configured" });
        }
        console.log("HIT /api/strem");
        const client = await auth.getClient();
        const authHeaders = await client.getRequestHeaders();

        console.log("Stream auth header present?", !!authHeaders.authorization);

        const upstream = await fetch(BQ_AGENT_STREAM_URL, {
            method: "POST",
            headers: {
                ...authHeaders,
                "Content-Type": "application/json",
                "Accept": "text/event-stream",
            },
            body: JSON.stringify(req.body),
        });

        res.status(upstream.status);
        res.setHeader("Content-Type", "text/event-stream");
        res.setHeader("Cache-Control", "no-cache");
        res.setHeader("Connection", "keep-alive");

        upstream.body.on("data", (chunk) => res.write(chunk));
        upstream.body.on("end", () => res.end());
        upstream.body.on("error", () => res.end());

    } catch (e) {
        console.error(e);
        res.status(500).json({ error: String(e) });
    }
});


app.listen(PORT, () => console.log(`Server listening on ${PORT}`));