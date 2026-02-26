import { useState } from "react";
import "./App.css";

type Stage =
    | "Transcript Generator"
    | "Transcript Validator"
    | "Signal Extractor"
    | "Shadow Validator";

function App() {
    const [stage, setStage] = useState<Stage>("Transcript Generator");
    const [projectId, setProjectId] = useState("");
    const [dataset, setDataset] = useState("");
    const [sourceVersion, setSourceVersion] = useState("");
    const [targetVersion, setTargetVersion] = useState("");
    const [limit, setLimit] = useState(100);

    const [result, setResult] = useState<any>(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);

    const handleRun = async () => {
        setLoading(true);
        setError(null);
        setResult(null);

        const payload = { stage, projectId, dataset, sourceVersion, targetVersion, limit };

        try {
            const res = await fetch("/api/run", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(payload),
            });

            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            const data = await res.json();
            setResult(data);
        } catch (e: any) {
            setError(e.message ?? "Unknown error");
        } finally {
            setLoading(false);
        }
    };

    return (
        <div className="container">
            <h1>CSV Module Pipeline UI</h1>

            <div className="card">
                <h2>Select Stage</h2>
                <select value={stage} onChange={(e) => setStage(e.target.value as Stage)}>
                    <option>Transcript Generator</option>
                    <option>Transcript Validator</option>
                    <option>Signal Extractor</option>
                    <option>Shadow Validator</option>
                </select>
            </div>

            <div className="card">
                <h2>Configuration</h2>

                <input
                    placeholder="GCP Project ID"
                    value={projectId}
                    onChange={(e) => setProjectId(e.target.value)}
                />

                <input
                    placeholder="BigQuery Dataset"
                    value={dataset}
                    onChange={(e) => setDataset(e.target.value)}
                />

                <input
                    placeholder="Source Table Version"
                    value={sourceVersion}
                    onChange={(e) => setSourceVersion(e.target.value)}
                />

                <input
                    placeholder="Target Table Version"
                    value={targetVersion}
                    onChange={(e) => setTargetVersion(e.target.value)}
                />

                <input
                    type="number"
                    placeholder="Row Limit"
                    value={limit}
                    onChange={(e) => setLimit(Number(e.target.value))}
                />
            </div>

            <button className="run-btn" onClick={handleRun}>
                Run Pipeline
            </button>
            {loading && <p>Running…</p>}
            {error && <p style={{ color: "red" }}>Error: {error}</p>}
            {result && (
                <pre style={{ background: "#111", color: "#0f0", padding: 12, borderRadius: 8 }}>
                    {JSON.stringify(result, null, 2)}
                </pre>
            )}
        </div>
    );
}

export default App;