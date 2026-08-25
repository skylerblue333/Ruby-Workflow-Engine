import pytest

from src.integration import integration_event


def workflow_view(**overrides):
    view = {
        "id": "wf:001",
        "workflowType": "classroom.provision",
        "status": "running",
        "currentStep": {"stepId": "members.sync"},
        "currentAttempt": 2,
        "updatedAt": 42.5,
        "input": {"secret": "must-not-leak"},
        "output": {"large": "must-not-leak"},
    }
    view.update(overrides)
    return view


def test_builds_bounded_low_data_integration_event():
    event = integration_event(workflow_view())
    assert event == {
        "schemaVersion": 1,
        "workflowId": "wf:001",
        "workflowType": "classroom.provision",
        "status": "running",
        "currentStepId": "members.sync",
        "currentAttempt": 2,
        "updatedAt": 42.5,
    }
    assert "input" not in event
    assert "output" not in event


def test_terminal_event_allows_no_current_step():
    event = integration_event(workflow_view(status="succeeded", currentStep=None))
    assert event["status"] == "succeeded"
    assert event["currentStepId"] is None


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("id", "bad id"),
        ("workflowType", ""),
        ("status", "unknown"),
        ("currentAttempt", True),
        ("currentAttempt", 11),
        ("updatedAt", -1),
        ("updatedAt", float("nan")),
        ("updatedAt", float("inf")),
        pytest.param("updatedAt", 10**10_000, id="updatedAt-huge-int"),
    ],
)
def test_rejects_invalid_untrusted_views(field, value):
    with pytest.raises(ValueError):
        integration_event(workflow_view(**{field: value}))


def test_rejects_invalid_timestamp_type():
    with pytest.raises(TypeError):
        integration_event(workflow_view(updatedAt="42"))


def test_rejects_invalid_current_step_shape():
    with pytest.raises(ValueError):
        integration_event(workflow_view(currentStep={"stepId": "bad step"}))
