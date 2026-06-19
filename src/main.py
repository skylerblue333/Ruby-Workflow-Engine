import asyncio
import time
from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel

app = FastAPI(title="Ruby-Workflow-Engine")

class TaskPayload(BaseModel):
    data: dict

async def process_data_background(payload: dict):
    # Simulate heavy processing
    await asyncio.sleep(1)
    print(f"Processed: {payload}")

@app.post("/api/v1/process")
async def process_endpoint(payload: TaskPayload, background_tasks: BackgroundTasks):
    background_tasks.add_task(process_data_background, payload.data)
    return {"status": "accepted", "processing_time_ms": int(time.time() * 1000)}

@app.get("/health")
def health():
    return {"status": "healthy", "version": "3.0.0"}
