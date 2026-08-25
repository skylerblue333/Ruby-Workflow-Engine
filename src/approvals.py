"""Deterministic SkyApprovals policy core for bounded approval workflows."""

from dataclasses import dataclass, replace
from typing import Literal
import re

Decision = Literal["approve", "reject"]
Status = Literal["pending", "approved", "rejected", "cancelled"]

_ID = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
_MAX_APPROVERS = 20


class ApprovalError(ValueError):
    """Raised when an approval request or transition is invalid."""


@dataclass(frozen=True)
class ApprovalRequest:
    request_id: str
    subject: str
    required_approvals: int
    eligible_approvers: tuple[str, ...]
    decisions: tuple[tuple[str, Decision], ...] = ()
    status: Status = "pending"


def _identifier(value: str, field: str) -> str:
    normalized = value.strip() if isinstance(value, str) else ""
    if not _ID.fullmatch(normalized):
        raise ApprovalError(f"{field} must be a 1-128 character safe identifier")
    return normalized


def create_request(
    *, request_id: str, subject: str, required_approvals: int, eligible_approvers: list[str] | tuple[str, ...]
) -> ApprovalRequest:
    rid = _identifier(request_id, "request_id")
    sub = _identifier(subject, "subject")
    if not isinstance(required_approvals, int) or isinstance(required_approvals, bool):
        raise ApprovalError("required_approvals must be an integer")
    normalized = tuple(dict.fromkeys(_identifier(item, "approver") for item in eligible_approvers))
    if not normalized or len(normalized) > _MAX_APPROVERS:
        raise ApprovalError(f"eligible_approvers must contain 1-{_MAX_APPROVERS} unique approvers")
    if required_approvals < 1 or required_approvals > len(normalized):
        raise ApprovalError("required_approvals must be within eligible approver count")
    return ApprovalRequest(rid, sub, required_approvals, normalized)


def decide(request: ApprovalRequest, *, approver: str, decision: Decision) -> ApprovalRequest:
    if request.status != "pending":
        raise ApprovalError("terminal approval request cannot be changed")
    actor = _identifier(approver, "approver")
    if actor not in request.eligible_approvers:
        raise ApprovalError("approver is not eligible")
    if decision not in ("approve", "reject"):
        raise ApprovalError("decision must be approve or reject")
    if any(existing == actor for existing, _ in request.decisions):
        raise ApprovalError("approver has already decided")

    decisions = request.decisions + ((actor, decision),)
    if decision == "reject":
        status: Status = "rejected"
    else:
        approvals = sum(1 for _, value in decisions if value == "approve")
        status = "approved" if approvals >= request.required_approvals else "pending"
    return replace(request, decisions=decisions, status=status)


def cancel(request: ApprovalRequest) -> ApprovalRequest:
    if request.status != "pending":
        raise ApprovalError("only pending approval requests can be cancelled")
    return replace(request, status="cancelled")


def workflow_contract(request: ApprovalRequest) -> dict[str, object]:
    """Return a stable integration contract; this does not execute a workflow transition."""
    approvals = sum(1 for _, value in request.decisions if value == "approve")
    return {
        "schema": "sky.approvals.workflow.v1",
        "request_id": request.request_id,
        "subject": request.subject,
        "status": request.status,
        "required_approvals": request.required_approvals,
        "approval_count": approvals,
        "decision_count": len(request.decisions),
    }
