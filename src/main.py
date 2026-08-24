"""Sky Workflow: deterministic persisted workflow coordination for SKYCOIN4444."""

from __future__ import annotations

import hmac
import json
import os
import re
import sqlite3
import time
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

from fastapi import Depends, FastAPI, HTTPException, Request
from pydantic import BaseModel, Field, field_validator, model_validator

SERVICE_NAME = "sky-workflow"
MAX_JSON_BYTES = 64 * 1024
NAME_RE = re.compile(r"^[A-Za-z0-9._:-]{1,100}$")


class StepSpec(BaseModel):
    stepId: str = Field(min_length=1, max_length=100)
    maxAttempts: int = Field(default=3, ge=1, le=10)
    retryDelaySeconds: int = Field(default=5, ge=0, le=86_400)
    timeoutSeconds: int = Field(default=300, ge=5, le=86_400)

    @field_validator("stepId")
    @classmethod
    def valid_id(cls, value: str) -> str:
        if not NAME_RE.fullmatch(value):
            raise ValueError("stepId contains unsupported characters")
        return value


class StartWorkflow(BaseModel):
    workflowType: str = Field(min_length=1, max_length=100)
    steps: list[StepSpec] = Field(min_length=1, max_length=50)
    input: dict[str, Any] = Field(default_factory=dict)
    idempotencyKey: str | None = Field(default=None, min_length=1, max_length=128)

    @field_validator("workflowType")
    @classmethod
    def valid_type(cls, value: str) -> str:
        if not NAME_RE.fullmatch(value):
            raise ValueError("workflowType contains unsupported characters")
        return value

    @model_validator(mode="after")
    def unique_steps(self) -> StartWorkflow:
        ids = [step.stepId for step in self.steps]
        if len(ids) != len(set(ids)):
            raise ValueError("stepId values must be unique within a workflow")
        return self


class AdvanceRequest(BaseModel):
    stepId: str = Field(min_length=1, max_length=100)
    outcome: Literal["success", "failure"]
    operationKey: str = Field(min_length=1, max_length=128)
    result: dict[str, Any] = Field(default_factory=dict)
    error: str | None = Field(default=None, max_length=1000)


class WorkflowStore:
    def __init__(self, path: str) -> None:
        self.path = str(Path(path).expanduser().resolve())
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=5000")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute("CREATE TABLE IF NOT EXISTS workflows (id TEXT PRIMARY KEY,idempotency_key TEXT UNIQUE,workflow_type TEXT NOT NULL,definition_json TEXT NOT NULL,input_json TEXT NOT NULL,status TEXT NOT NULL CHECK(status IN ('running','succeeded','failed','cancelled')),current_index INTEGER NOT NULL,current_attempt INTEGER NOT NULL,available_at REAL NOT NULL,deadline_at REAL,output_json TEXT,last_error TEXT,created_at REAL NOT NULL,updated_at REAL NOT NULL)")
            connection.execute("CREATE TABLE IF NOT EXISTS history (sequence INTEGER PRIMARY KEY AUTOINCREMENT,workflow_id TEXT NOT NULL,operation_key TEXT,event_type TEXT NOT NULL,step_id TEXT,payload_json TEXT NOT NULL,created_at REAL NOT NULL,UNIQUE(workflow_id, operation_key),FOREIGN KEY(workflow_id) REFERENCES workflows(id))")
            connection.execute("CREATE INDEX IF NOT EXISTS idx_history_workflow ON history(workflow_id, sequence)")

    @staticmethod
    def _json(value: Any, field: str) -> str:
        try:
            encoded = json.dumps(value, separators=(",", ":"), sort_keys=True, allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{field} must be valid JSON") from exc
        if len(encoded.encode("utf-8")) > MAX_JSON_BYTES:
            raise ValueError(f"{field} exceeds {MAX_JSON_BYTES} bytes")
        return encoded

    def start(self, request: StartWorkflow) -> dict[str, Any]:
        now = time.time()
        workflow_id = uuid4().hex
        definition = self._json([step.model_dump() for step in request.steps], "steps")
        input_json = self._json(request.input, "input")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                if request.idempotencyKey:
                    existing = connection.execute(
                        "SELECT * FROM workflows WHERE idempotency_key = ?",
                        (request.idempotencyKey,),
                    ).fetchone()
                    if existing:
                        connection.commit()
                        return self._decode(existing)
                first = request.steps[0]
                deadline = now + first.timeoutSeconds
                connection.execute(
                    "INSERT INTO workflows VALUES (?,?,?,?,?,'running',0,1,?,?,NULL,NULL,?,?)",
                    (
                        workflow_id,
                        request.idempotencyKey,
                        request.workflowType,
                        definition,
                        input_json,
                        now,
                        deadline,
                        now,
                        now,
                    ),
                )
                connection.execute(
                    "INSERT INTO history(workflow_id,operation_key,event_type,step_id,payload_json,created_at) VALUES(?,?,?,?,?,?)",
                    (workflow_id, f"start:{workflow_id}", "workflow_started", first.stepId, "{}", now),
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        return self.get(workflow_id)

    def get(self, workflow_id: str) -> dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM workflows WHERE id = ?", (workflow_id,)).fetchone()
        if not row:
            raise KeyError(workflow_id)
        return self._decode(row)

    def history(self, workflow_id: str) -> list[dict[str, Any]]:
        self.get(workflow_id)
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT sequence,event_type,step_id,payload_json,created_at FROM history WHERE workflow_id = ? ORDER BY sequence",
                (workflow_id,),
            ).fetchall()
        return [
            {
                "sequence": row["sequence"],
                "eventType": row["event_type"],
                "stepId": row["step_id"],
                "payload": json.loads(row["payload_json"]),
                "createdAt": row["created_at"],
            }
            for row in rows
        ]

    def advance(self, workflow_id: str, request: AdvanceRequest) -> dict[str, Any]:
        now = time.time()
        result_json = self._json(request.result, "result")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute("SELECT * FROM workflows WHERE id = ?", (workflow_id,)).fetchone()
                if not row:
                    raise KeyError(workflow_id)
                if row["status"] != "running":
                    raise RuntimeError("workflow is not running")
                prior = connection.execute(
                    "SELECT 1 FROM history WHERE workflow_id = ? AND operation_key = ?",
                    (workflow_id, request.operationKey),
                ).fetchone()
                if prior:
                    connection.commit()
                    return self._decode(row)

                steps = [StepSpec(**item) for item in json.loads(row["definition_json"])]
                current = steps[row["current_index"]]
                if request.stepId != current.stepId:
                    raise RuntimeError("stepId does not match the active step")
                if now < row["available_at"]:
                    raise RuntimeError("active step is waiting for retry delay")
                if row["deadline_at"] is not None and now > row["deadline_at"]:
                    request = request.model_copy(update={"outcome": "failure", "error": "step timeout exceeded"})

                if request.outcome == "success":
                    next_index = row["current_index"] + 1
                    if next_index >= len(steps):
                        connection.execute(
                            "UPDATE workflows SET status='succeeded',output_json=?,last_error=NULL,updated_at=? WHERE id=?",
                            (result_json, now, workflow_id),
                        )
                        event_type = "workflow_succeeded"
                    else:
                        next_step = steps[next_index]
                        connection.execute(
                            "UPDATE workflows SET current_index=?,current_attempt=1,available_at=?,deadline_at=?,last_error=NULL,updated_at=? WHERE id=?",
                            (next_index, now, now + next_step.timeoutSeconds, now, workflow_id),
                        )
                        event_type = "step_succeeded"
                else:
                    if row["current_attempt"] < current.maxAttempts:
                        next_attempt = row["current_attempt"] + 1
                        available_at = now + current.retryDelaySeconds
                        connection.execute(
                            "UPDATE workflows SET current_attempt=?,available_at=?,deadline_at=?,last_error=?,updated_at=? WHERE id=?",
                            (next_attempt, available_at, available_at + current.timeoutSeconds, request.error, now, workflow_id),
                        )
                        event_type = "step_retry_scheduled"
                    else:
                        connection.execute(
                            "UPDATE workflows SET status='failed',last_error=?,updated_at=? WHERE id=?",
                            (request.error or "step failed", now, workflow_id),
                        )
                        event_type = "workflow_failed"

                payload = self._json({"outcome": request.outcome, "result": request.result, "error": request.error}, "history payload")
                connection.execute(
                    "INSERT INTO history(workflow_id,operation_key,event_type,step_id,payload_json,created_at) VALUES(?,?,?,?,?,?)",
                    (workflow_id, request.operationKey, event_type, request.stepId, payload, now),
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        return self.get(workflow_id)

    def cancel(self, workflow_id: str) -> dict[str, Any]:
        now = time.time()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute("SELECT * FROM workflows WHERE id = ?", (workflow_id,)).fetchone()
                if not row:
                    raise KeyError(workflow_id)
                if row["status"] == "running":
                    connection.execute("UPDATE workflows SET status='cancelled',updated_at=? WHERE id=?", (now, workflow_id))
                    connection.execute(
                        "INSERT OR IGNORE INTO history(workflow_id,operation_key,event_type,step_id,payload_json,created_at) VALUES(?,?,?,?,?,?)",
                        (workflow_id, f"cancel:{workflow_id}", "workflow_cancelled", None, "{}", now),
                    )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        return self.get(workflow_id)

    @staticmethod
    def _decode(row: sqlite3.Row) -> dict[str, Any]:
        steps = json.loads(row["definition_json"])
        active_step = steps[row["current_index"]]["stepId"] if row["status"] == "running" else None
        return {
            "id": row["id"],
            "workflowType": row["workflow_type"],
            "status": row["status"],
            "activeStepId": active_step,
            "currentAttempt": row["current_attempt"],
            "availableAt": row["available_at"],
            "deadlineAt": row["deadline_at"],
            "input": json.loads(row["input_json"]),
            "output": json.loads(row["output_json"]) if row["output_json"] else None,
            "lastError": row["last_error"],
            "createdAt": row["created_at"],
            "updatedAt": row["updated_at"],
        }


DB_PATH = os.getenv("WORKFLOW_DB_PATH", "data/workflows.db")
API_TOKEN = os.getenv("WORKFLOW_API_TOKEN", "")
store = WorkflowStore(DB_PATH)
app = FastAPI(title="Sky Workflow", version="1.0.0")


def authorize(request: Request) -> None:
    if not API_TOKEN:
        return
    supplied = request.headers.get("authorization", "")
    expected = f"Bearer {API_TOKEN}"
    if not hmac.compare_digest(supplied, expected):
        raise HTTPException(status_code=401, detail="unauthorized")


@app.get("/healthz")
def health() -> dict[str, str]:
    return {"status": "healthy", "service": SERVICE_NAME}


@app.get("/readyz")
def ready() -> dict[str, str]:
    store._connect().close()
    return {"status": "ready", "service": SERVICE_NAME}


@app.get("/metrics")
def metrics() -> dict[str, Any]:
    with store._connect() as connection:
        rows = connection.execute("SELECT status,COUNT(*) AS count FROM workflows GROUP BY status").fetchall()
    return {"service": SERVICE_NAME, "workflows": {row["status"]: row["count"] for row in rows}}


@app.post("/api/v1/workflows", dependencies=[Depends(authorize)])
def start_workflow(request: StartWorkflow) -> dict[str, Any]:
    try:
        return store.start(request)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/v1/workflows/{workflow_id}", dependencies=[Depends(authorize)])
def get_workflow(workflow_id: str) -> dict[str, Any]:
    try:
        return store.get(workflow_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="workflow not found") from exc


@app.get("/api/v1/workflows/{workflow_id}/history", dependencies=[Depends(authorize)])
def get_history(workflow_id: str) -> list[dict[str, Any]]:
    try:
        return store.history(workflow_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="workflow not found") from exc


@app.post("/api/v1/workflows/{workflow_id}/advance", dependencies=[Depends(authorize)])
def advance_workflow(workflow_id: str, request: AdvanceRequest) -> dict[str, Any]:
    try:
        return store.advance(workflow_id, request)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="workflow not found") from exc
    except (RuntimeError, ValueError) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/api/v1/workflows/{workflow_id}/cancel", dependencies=[Depends(authorize)])
def cancel_workflow(workflow_id: str) -> dict[str, Any]:
    try:
        return store.cancel(workflow_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="workflow not found") from exc
