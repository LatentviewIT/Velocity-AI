from fastapi import FastAPI
from pydantic import BaseModel
import subprocess
import uuid

app = FastAPI()

class RunRequest(BaseModel):
    stage: str
    projectId: str | None = None
    dataset: str | None = None
    sourceVersion: str | None = None
    targetVersion: str | None = None
    limit: int = 100

@app.get("/api/health")
def health():
    return {"ok": True}

@app.post("/api/run")
def run_pipeline(req: RunRequest):
    job_id = str(uuid.uuid4())[:8]

    # Map UI stage -> python file / entrypoint
    stage_map = {
    "Transcript Generator": "Python/synth-data-gen-005_vertex-colab-ent_csv_nirvana.ipynb",
    "Transcript Validator": "Python/transcript-val-001_vertex-colab-ent_csv_nirvana.ipynb",
    "Signal Extractor": "Python/signal-extractor-003_vertex-colab-ent_csv_nirvana.ipynb",
    "Shadow Validator": "Python/signals-validator-001_vertex-colab-ent_csv_nirvana.ipynb",
    }

    target = stage_map.get(req.stage)
    if not target:
        return {"jobId": job_id, "status": "ERROR", "message": f"Unknown stage: {req.stage}"}

    # MVP: don’t run notebooks yet; just show wiring works.
    # Replace this with real runner later (papermill / python module / cloud job).
    return {
    "jobId": job_id,
    "status": "QUEUED",
    "stage": req.stage,
    "target": target,
    "config": req.model_dump()
    }