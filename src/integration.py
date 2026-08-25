"""Stable SKYCOIN4444 integration contracts for Sky Workflow.

These helpers transform an already-authorized workflow view into a bounded event envelope.
They do not publish to a broker, execute work, or make delivery guarantees.
"""

from __future__ import annotations

import re
from typing import Any, Literal, TypedDict

_ID = re.compile(r"^[A-Za-z0-9._:-]{1,100}$")
_ALLOWED_STATUS = {"running", "succeeded", "failed", "cancelled"}


class WorkflowIntegrationEvent(TypedDict):
    schemaVersion: Literal[1]
    workflowId: str
    workflowType: str
    status: Literal["running", "succeeded", "failed", "cancelled"]
    currentStepId: str | None
    currentAttempt: int
    updatedAt: float


def _safe_id(label: str, value: Any) -> str:
    if not isinstance(value, str) or not _ID.fullmatch(value):
        raise ValueError(f"{label} must be a bounded safe identifier")
    return value


def integration_event(view: dict[str, Any]) -> WorkflowIntegrationEvent:
    """Build a deterministic, low-data workflow event from a workflow view."""
    workflow_id = _safe_id("workflow id", view.get("id"))
    workflow_type = _safe_id("workflow type", view.get("workflowType"))
    status = view.get("status")
    if status not in _ALLOWED_STATUS:
        raise ValueError("status is unsupported")

    attempt = view.get("currentAttempt")
    if not isinstance(attempt, int) or isinstance(attempt, bool) or not 0 <= attempt <= 10:
        raise ValueError("currentAttempt must be an integer from 0 to 10")

    updated_at = view.get("updatedAt")
    if not isinstance(updated_at, (int, float)) or isinstance(updated_at, bool) or updated_at < 0:
        raise ValueError("updatedAt must be a non-negative number")

    current = view.get("currentStep")
    current_step_id: str | None = None
    if current is not None:
        if not isinstance(current, dict):
            raise ValueError("currentStep must be an object or null")
        current_step_id = _safe_id("current step id", current.get("stepId"))

    return {
        "schemaVersion": 1,
        "workflowId": workflow_id,
        "workflowType": workflow_type,
        "status": status,
        "currentStepId": current_step_id,
        "currentAttempt": attempt,
        "updatedAt": float(updated_at),
    }
