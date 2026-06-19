from fastapi import FastAPI
import asyncio
import time

app = FastAPI(title="Ruby-Workflow-Engine API", version="2.0.0")

class Processor:
    def __init__(self):
        self.ready = False
        self.items_processed = 0
        
    async def initialize(self):
        await asyncio.sleep(0.1)
        self.ready = True
        
    def process(self, data: dict) -> dict:
        if not self.ready:
            raise RuntimeError("Not initialized")
        self.items_processed += 1
        return {"status": "success", "processed": True, "domain": "engine", "data": data}

processor = Processor()

@app.on_event("startup")
async def startup():
    await processor.initialize()

@app.get("/health")
def health():
    return {"status": "ok", "ready": processor.ready, "processed": processor.items_processed}

@app.post("/api/v1/process")
def process_data(payload: dict):
    return processor.process(payload)
