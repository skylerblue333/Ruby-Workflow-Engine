from fastapi.testclient import TestClient
from src.main import app

def test_health():
    with TestClient(app) as client:
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "ok"
        assert response.json()["ready"] == True

def test_process():
    with TestClient(app) as client:
        response = client.post("/api/v1/process", json={"test": "data"})
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert data["domain"] == "engine"
