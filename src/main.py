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
            raise ValueError(f"{field} must be valid JSON data") from exc
        if len(encoded.encode("utf-8")) > MAX_JSON_BYTES:
            raise ValueError(f"{field} exceeds {MAX_JSON_BYTES} bytes")
        return encoded

    @staticmethod
    def _definition(row: sqlite3.Row) -> list[dict[str, Any]]:
        return json.loads(row["definition_json"])

    @classmethod
    def _view(cls, row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        definition = cls._definition(row)
        index = row["current_index"]
        current = definition[index] if row["status"] == "running" and 0 <= index < len(definition) else None
        return {"id": row["id"], "idempotencyKey": row["idempotency_key"], "workflowType": row["workflow_type"], "steps": definition, "input": json.loads(row["input_json"]), "status": row["status"], "currentStep": current, "currentAttempt": row["current_attempt"], "availableAt": row["available_at"], "deadlineAt": row["deadline_at"], "output": json.loads(row["output_json"]) if row["output_json"] else None, "lastError": row["last_error"], "createdAt": row["created_at"], "updatedAt": row["updated_at"]}

    def start(self, request: StartWorkflow, now: float) -> tuple[dict[str, Any], bool]:
        definition = [step.model_dump() for step in request.steps]
        definition_json = self._json(definition, "steps")
        input_json = self._json(request.input, "input")
        workflow_id = str(uuid4())
        deadline = now + request.steps[0].timeoutSeconds
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if request.idempotencyKey:
                existing = connection.execute("SELECT * FROM workflows WHERE idempotency_key=?", (request.idempotencyKey,)).fetchone()
                if existing:
                    connection.execute("COMMIT")
                    return self._view(existing) or {}, False
            try:
                connection.execute("INSERT INTO workflows (id,idempotency_key,workflow_type,definition_json,input_json,status,current_index,current_attempt,available_at,deadline_at,created_at,updated_at) VALUES (?,?,?,?,?,'running',0,1,?,?,?,?)", (workflow_id, request.idempotencyKey, request.workflowType, definition_json, input_json, now, deadline, now, now))
                self._append_history(connection, workflow_id, None, "workflow_started", request.steps[0].stepId, {"attempt": 1}, now)
                row = connection.execute("SELECT * FROM workflows WHERE id=?", (workflow_id,)).fetchone()
                connection.execute("COMMIT")
                return self._view(row) or {}, True
            except sqlite3.IntegrityError:
                connection.execute("ROLLBACK")
                if not request.idempotencyKey:
                    raise
                row = connection.execute("SELECT * FROM workflows WHERE idempotency_key=?", (request.idempotencyKey,)).fetchone()
                if row is None:
                    raise
                return self._view(row) or {}, False

    def get(self, workflow_id: str, now: float) -> dict[str, Any] | None:
        self.reconcile_timeout(workflow_id, now)
        with self._connect() as connection:
            return self._view(connection.execute("SELECT * FROM workflows WHERE id=?", (workflow_id,)).fetchone())

    def advance(self, workflow_id: str, request: AdvanceRequest, now: float) -> tuple[str, dict[str, Any] | None, bool]:
        result_json = self._json(request.result, "result")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT * FROM workflows WHERE id=?", (workflow_id,)).fetchone()
            if row is None:
                connection.execute("COMMIT")
                return "not_found", None, False
            replay = connection.execute("SELECT 1 FROM history WHERE workflow_id=? AND operation_key=?", (workflow_id, request.operationKey)).fetchone()
            if replay:
                connection.execute("COMMIT")
                return "ok", self._view(row), False
            row = self._reconcile_timeout_locked(connection, row, now)
            if row["status"] != "running":
                connection.execute("COMMIT")
                return "terminal", self._view(row), False
            if now < row["available_at"]:
                connection.execute("COMMIT")
                return "backoff", self._view(row), False
            definition = self._definition(row)
            step = definition[row["current_index"]]
            if request.stepId != step["stepId"]:
                connection.execute("COMMIT")
                return "step_mismatch", self._view(row), False
            if request.outcome == "success":
                self._append_history(connection, workflow_id, request.operationKey, "step_succeeded", request.stepId, request.result, now)
                next_index = row["current_index"] + 1
                if next_index >= len(definition):
                    connection.execute("UPDATE workflows SET status='succeeded', output_json=?, deadline_at=NULL, available_at=?, last_error=NULL, updated_at=? WHERE id=?", (result_json, now, now, workflow_id))
                else:
                    next_step = definition[next_index]
                    connection.execute("UPDATE workflows SET current_index=?, current_attempt=1, available_at=?, deadline_at=?, last_error=NULL, updated_at=? WHERE id=?", (next_index, now, now + next_step["timeoutSeconds"], now, workflow_id))
            else:
                self._apply_failure_locked(connection, row, request.operationKey, "step_failed", request.error or "step failed", now)
            updated = connection.execute("SELECT * FROM workflows WHERE id=?", (workflow_id,)).fetchone()
            connection.execute("COMMIT")
            return "ok", self._view(updated), True

    def cancel(self, workflow_id: str, now: float) -> str:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT * FROM workflows WHERE id=?", (workflow_id,)).fetchone()
            if row is None:
                connection.execute("COMMIT")
                return "not_found"
            if row["status"] != "running":
                connection.execute("COMMIT")
                return "terminal"
            step_id = self._definition(row)[row["current_index"]]["stepId"]
            connection.execute("UPDATE workflows SET status='cancelled', deadline_at=NULL, updated_at=? WHERE id=?", (now, workflow_id))
            self._append_history(connection, workflow_id, None, "workflow_cancelled", step_id, {}, now)
            connection.execute("COMMIT")
            return "ok"

    def reconcile_timeout(self, workflow_id: str, now: float) -> None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT * FROM workflows WHERE id=?", (workflow_id,)).fetchone()
            if row is not None:
                self._reconcile_timeout_locked(connection, row, now)
            connection.execute("COMMIT")

    def _reconcile_timeout_locked(self, connection: sqlite3.Connection, row: sqlite3.Row, now: float) -> sqlite3.Row:
        if row["status"] != "running" or row["deadline_at"] is None or now < row["deadline_at"]:
            return row
        return self._apply_failure_locked(connection, row, None, "step_timed_out", "step timeout", now)

    def _apply_failure_locked(self, connection: sqlite3.Connection, row: sqlite3.Row, operation_key: str | None, event_type: str, error: str, now: float) -> sqlite3.Row:
        step = self._definition(row)[row["current_index"]]
        self._append_history(connection, row["id"], operation_key, event_type, step["stepId"], {"error": error}, now)
        if row["current_attempt"] >= step["maxAttempts"]:
            connection.execute("UPDATE workflows SET status='failed', deadline_at=NULL, last_error=?, updated_at=? WHERE id=?", (error, now, row["id"]))
        else:
            next_attempt = row["current_attempt"] + 1
            available = now + step["retryDelaySeconds"]
            connection.execute("UPDATE workflows SET current_attempt=?, available_at=?, deadline_at=?, last_error=?, updated_at=? WHERE id=?", (next_attempt, available, available + step["timeoutSeconds"], error, now, row["id"]))
        return connection.execute("SELECT * FROM workflows WHERE id=?", (row["id"],)).fetchone()

    def _append_history(self, connection: sqlite3.Connection, workflow_id: str, operation_key: str | None, event_type: str, step_id: str | None, payload: dict[str, Any], now: float) -> None:
        connection.execute("INSERT INTO history(workflow_id,operation_key,event_type,step_id,payload_json,created_at) VALUES(?,?,?,?,?,?)", (workflow_id, operation_key, event_type, step_id, self._json(payload, "history payload"), now))

    def history(self, workflow_id: str) -> list[dict[str, Any]] | None:
        with self._connect() as connection:
            if connection.execute("SELECT 1 FROM workflows WHERE id=?", (workflow_id,)).fetchone() is None:
                return None
            rows = connection.execute("SELECT sequence,operation_key,event_type,step_id,payload_json,created_at FROM history WHERE workflow_id=? ORDER BY sequence", (workflow_id,)).fetchall()
        return [{"sequence": row["sequence"], "operationKey": row["operation_key"], "eventType": row["event_type"], "stepId": row["step_id"], "payload": json.loads(row["payload_json"]), "createdAt": row["created_at"]} for row in rows]

    def metrics(self) -> dict[str, int]:
        with self._connect() as connection:
            rows = connection.execute("SELECT status, COUNT(*) AS count FROM workflows GROUP BY status").fetchall()
        values = {status: 0 for status in ("running", "succeeded", "failed", "cancelled")}
        values.update({row["status"]: row["count"] for row in rows})
        return values

    def ping(self) -> None:
        with self._connect() as connection:
            connection.execute("SELECT 1").fetchone()


DB_PATH = os.getenv("WORKFLOW_DB_PATH", "data/workflows.db")
API_TOKEN = os.getenv("WORKFLOW_API_TOKEN") or None
if API_TOKEN is not None and len(API_TOKEN) < 16:
    raise RuntimeError("WORKFLOW_API_TOKEN must contain at least 16 characters when configured")
store = WorkflowStore(DB_PATH)
app = FastAPI(title="Sky Workflow", version="1.0.0")


def authorize(request: Request) -> None:
    if API_TOKEN is None:
        return
    if not hmac.compare_digest(request.headers.get("authorization", ""), f"Bearer {API_TOKEN}"):
        raise HTTPException(status_code=401, detail="unauthorized")


@app.get("/healthz")
def health() -> dict[str, str]:
    return {"status": "healthy", "service": SERVICE_NAME}


@app.get("/readyz")
def ready() -> dict[str, str]:
    store.ping()
    return {"status": "ready", "service": SERVICE_NAME}


@app.get("/metrics")
def metrics() -> dict[str, int | str]:
    return {"service": SERVICE_NAME, **store.metrics()}


@app.post("/api/v1/workflows", dependencies=[Depends(authorize)], status_code=201)
def start_workflow(request: StartWorkflow) -> dict[str, Any]:
    try:
        workflow, created = store.start(request, time.time())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not created:
        workflow["replayed"] = True
    return workflow


@app.get("/api/v1/workflows/{workflow_id}", dependencies=[Depends(authorize)])
def get_workflow(workflow_id: str) -> dict[str, Any]:
    workflow = store.get(workflow_id, time.time())
    if workflow is None:
        raise HTTPException(status_code=404, detail="workflow not found")
    return workflow


@app.post("/api/v1/workflows/{workflow_id}/advance", dependencies=[Depends(authorize)])
def advance_workflow(workflow_id: str, request: AdvanceRequest) -> dict[str, Any]:
    try:
        status, workflow, applied = store.advance(workflow_id, request, time.time())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if status == "not_found":
        raise HTTPException(status_code=404, detail="workflow not found")
    if status == "terminal":
        raise HTTPException(status_code=409, detail="workflow is terminal")
    if status == "backoff":
        raise HTTPException(status_code=409, detail="current step is waiting for retry backoff")
    if status == "step_mismatch":
        raise HTTPException(status_code=409, detail="step does not match current workflow step")
    assert workflow is not None
    workflow["applied"] = applied
    return workflow


@app.post("/api/v1/workflows/{workflow_id}/cancel", dependencies=[Depends(authorize)])
def cancel_workflow(workflow_id: str) -> dict[str, str]:
    status = store.cancel(workflow_id, time.time())
    if status == "not_found":
        raise HTTPException(status_code=404, detail="workflow not found")
    if status == "terminal":
        raise HTTPException(status_code=409, detail="workflow is terminal")
    return {"status": "cancelled"}


@app.get("/api/v1/workflows/{workflow_id}/history", dependencies=[Depends(authorize)])
def workflow_history(workflow_id: str) -> dict[str, Any]:
    events = store.history(workflow_id)
    if events is None:
        raise HTTPException(status_code=404, detail="workflow not found")
    return {"workflowId": workflow_id, "events": events}
