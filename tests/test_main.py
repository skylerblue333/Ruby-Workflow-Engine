from fastapi.testclient import TestClient
from src.main import app

client = TestClient(app)

def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"

def test_process():
    response = client.post("/api/v1/process", json={"data": {"key": "value"}})
    assert response.status_code == 200
    assert response.json()["status"] == "accepted"
