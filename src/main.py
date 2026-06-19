"""
Ruby-Workflow-Engine: Production microservice: Ruby Workflow Engine
"""
import time
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="Ruby-Workflow-Engine", version="3.0.0")

from typing import Optional

class JobRequest(BaseModel):
    job_id: str
    job_type: str
    params: dict
    priority: Optional[int] = 5

job_queue = []

@app.post("/api/v1/jobs")
def enqueue_job(job: JobRequest):
    job_queue.append(job.dict())
    job_queue.sort(key=lambda x: x["priority"])
    return {"status": "queued", "job_id": job.job_id, "queue_depth": len(job_queue)}

@app.get("/api/v1/jobs")
def list_jobs():
    return {"jobs": job_queue[:10], "total": len(job_queue)}


@app.get("/health")
def health():
    return {"status": "healthy", "service": "Ruby-Workflow-Engine", "timestamp": int(time.time())}
