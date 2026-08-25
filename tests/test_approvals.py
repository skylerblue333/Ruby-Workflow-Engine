import pytest

from src.approvals import (
    ApprovalError,
    cancel,
    create_request,
    decide,
    workflow_contract,
)


def test_reaches_quorum_deterministically():
    request = create_request(
        request_id="approval-1",
        subject="project-42",
        required_approvals=2,
        eligible_approvers=["alice", "bob", "carol", "alice"],
    )
    request = decide(request, approver="alice", decision="approve")
    assert request.status == "pending"
    request = decide(request, approver="bob", decision="approve")
    assert request.status == "approved"


def test_rejection_is_terminal_and_duplicate_actor_is_blocked():
    request = create_request(
        request_id="approval-2", subject="task-1", required_approvals=2, eligible_approvers=["alice", "bob"]
    )
    request = decide(request, approver="alice", decision="reject")
    assert request.status == "rejected"
    with pytest.raises(ApprovalError, match="terminal"):
        decide(request, approver="bob", decision="approve")

    pending = create_request(
        request_id="approval-3", subject="task-2", required_approvals=2, eligible_approvers=["alice", "bob"]
    )
    pending = decide(pending, approver="alice", decision="approve")
    with pytest.raises(ApprovalError, match="already decided"):
        decide(pending, approver="alice", decision="approve")


def test_validates_approver_set_threshold_and_identifiers():
    with pytest.raises(ApprovalError):
        create_request(request_id="../bad", subject="task", required_approvals=1, eligible_approvers=["alice"])
    with pytest.raises(ApprovalError):
        create_request(request_id="a", subject="task", required_approvals=2, eligible_approvers=["alice"])
    request = create_request(
        request_id="a", subject="task", required_approvals=1, eligible_approvers=["alice"]
    )
    with pytest.raises(ApprovalError, match="not eligible"):
        decide(request, approver="mallory", decision="approve")


def test_cancel_and_workflow_contract_are_bounded():
    request = create_request(
        request_id="approval-4", subject="workflow-9", required_approvals=2, eligible_approvers=["alice", "bob"]
    )
    request = decide(request, approver="alice", decision="approve")
    contract = workflow_contract(request)
    assert contract == {
        "schema": "sky.approvals.workflow.v1",
        "request_id": "approval-4",
        "subject": "workflow-9",
        "status": "pending",
        "required_approvals": 2,
        "approval_count": 1,
        "decision_count": 1,
    }
    cancelled = cancel(request)
    assert cancelled.status == "cancelled"
    with pytest.raises(ApprovalError, match="only pending"):
        cancel(cancelled)
