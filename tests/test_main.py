from fastapi.testclient import TestClient
from src.main import app

client = TestClient(app)

def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"

def test_anomaly_detected():
    r = client.post("/api/v1/analyze", json={"metric": "cpu", "value": 99.5, "threshold": 80.0})
    assert r.status_code == 200
    assert r.json()["is_anomaly"] == True

def test_no_anomaly():
    r = client.post("/api/v1/analyze", json={"metric": "cpu", "value": 50.0, "threshold": 80.0})
    assert r.status_code == 200
    assert r.json()["is_anomaly"] == False

