import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from src import main


def make_store(tmp_path):
    return main.WorkflowStore(str(tmp_path / "workflow.db"))


def workflow_request(**overrides):
    data = {
        "workflowType": "order.fulfillment",
        "steps": [
            {"stepId": "reserve", "maxAttempts": 2, "retryDelaySeconds": 5, "timeoutSeconds": 10},
            {"stepId": "ship", "maxAttempts": 2, "retryDelaySeconds": 5, "timeoutSeconds": 10},
        ],
        "input": {"orderId": "o-1"},
        "idempotencyKey": "order-o-1",
    }
    data.update(overrides)
    return main.StartWorkflow(**data)


def test_health_and_readiness() -> None:
    client = TestClient(main.app)
    assert client.get("/healthz").json()["status"] == "healthy"
    assert client.get("/readyz").json()["status"] == "ready"


def test_start_is_idempotent(tmp_path) -> None:
    store = make_store(tmp_path)
    first, created = store.start(workflow_request(), 100.0)
    replay, replay_created = store.start(workflow_request(), 101.0)
    assert created is True
    assert replay_created is False
    assert replay["id"] == first["id"]
    assert first["currentStep"]["stepId"] == "reserve"


def test_success_advances_steps_and_finishes(tmp_path) -> None:
    store = make_store(tmp_path)
    workflow, _ = store.start(workflow_request(idempotencyKey=None), 100.0)

    status, after_first, applied = store.advance(
        workflow["id"],
        main.AdvanceRequest(stepId="reserve", outcome="success", operationKey="op-1", result={"reservation": "r-1"}),
        101.0,
    )
    assert status == "ok" and applied is True
    assert after_first["currentStep"]["stepId"] == "ship"

    status, finished, applied = store.advance(
        workflow["id"],
        main.AdvanceRequest(stepId="ship", outcome="success", operationKey="op-2", result={"tracking": "t-1"}),
        102.0,
    )
    assert status == "ok" and applied is True
    assert finished["status"] == "succeeded"
    assert finished["output"] == {"tracking": "t-1"}
    assert [event["eventType"] for event in store.history(workflow["id"])] == [
        "workflow_started",
        "step_succeeded",
        "step_succeeded",
    ]


def test_advance_operation_is_idempotent(tmp_path) -> None:
    store = make_store(tmp_path)
    workflow, _ = store.start(workflow_request(idempotencyKey=None), 100.0)
    request = main.AdvanceRequest(stepId="reserve", outcome="success", operationKey="same-op")
    assert store.advance(workflow["id"], request, 101.0)[2] is True
    replay = store.advance(workflow["id"], request, 102.0)
    assert replay[0] == "ok"
    assert replay[2] is False


def test_failure_retries_then_fails(tmp_path) -> None:
    store = make_store(tmp_path)
    workflow, _ = store.start(workflow_request(idempotencyKey=None), 100.0)

    first = store.advance(
        workflow["id"],
        main.AdvanceRequest(stepId="reserve", outcome="failure", operationKey="fail-1", error="temporary"),
        101.0,
    )[1]
    assert first["status"] == "running"
    assert first["currentAttempt"] == 2
    assert store.advance(
        workflow["id"],
        main.AdvanceRequest(stepId="reserve", outcome="failure", operationKey="too-early", error="still failing"),
        102.0,
    )[0] == "backoff"

    second = store.advance(
        workflow["id"],
        main.AdvanceRequest(stepId="reserve", outcome="failure", operationKey="fail-2", error="permanent"),
        107.0,
    )[1]
    assert second["status"] == "failed"
    assert second["lastError"] == "permanent"


def test_timeout_consumes_attempt_budget(tmp_path) -> None:
    store = make_store(tmp_path)
    workflow, _ = store.start(workflow_request(idempotencyKey=None), 100.0)
    timed = store.get(workflow["id"], 111.0)
    assert timed is not None
    assert timed["currentAttempt"] == 2
    assert timed["lastError"] == "step timeout"
    assert any(event["eventType"] == "step_timed_out" for event in store.history(workflow["id"]))


def test_cancel_is_terminal(tmp_path) -> None:
    store = make_store(tmp_path)
    workflow, _ = store.start(workflow_request(idempotencyKey=None), 100.0)
    assert store.cancel(workflow["id"], 101.0) == "ok"
    assert store.get(workflow["id"], 102.0)["status"] == "cancelled"  # type: ignore[index]
    assert store.cancel(workflow["id"], 103.0) == "terminal"


def test_duplicate_step_ids_are_rejected() -> None:
    with pytest.raises(ValidationError):
        workflow_request(steps=[{"stepId": "same"}, {"stepId": "same"}])


def test_optional_api_token_is_fail_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(main, "API_TOKEN", "0123456789abcdef0123456789abcdef")
    client = TestClient(main.app)
    assert client.get("/api/v1/workflows/missing").status_code == 401
