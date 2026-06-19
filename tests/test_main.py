from fastapi.testclient import TestClient
from src.main import app

client = TestClient(app)

def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"

def test_enqueue_job():
    r = client.post("/api/v1/jobs", json={"job_id": "job-001", "job_type": "email", "params": {"to": "user@example.com"}, "priority": 1})
    assert r.status_code == 200
    assert r.json()["status"] == "queued"

def test_list_jobs():
    r = client.get("/api/v1/jobs")
    assert r.status_code == 200
    assert "jobs" in r.json()

