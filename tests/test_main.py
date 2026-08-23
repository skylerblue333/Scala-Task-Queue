import time

import pytest
from fastapi.testclient import TestClient

from src import main


def make_store(tmp_path):
    return main.QueueStore(str(tmp_path / "queue.db"))


def test_health_and_readiness() -> None:
    client = TestClient(main.app)
    assert client.get("/healthz").json()["status"] == "healthy"
    assert client.get("/readyz").json()["status"] == "ready"


def test_idempotent_submission_returns_same_task(tmp_path) -> None:
    store = make_store(tmp_path)
    task = main.SubmitTask(taskType="email.send", payload={"to": "user@example.test"}, idempotencyKey="order-123")
    first, created = store.submit(task, 100.0)
    second, replay_created = store.submit(task, 101.0)
    assert created is True
    assert replay_created is False
    assert second["id"] == first["id"]
    assert second["payload"] == {"to": "user@example.test"}


def test_claim_complete_lifecycle(tmp_path) -> None:
    store = make_store(tmp_path)
    record, _ = store.submit(main.SubmitTask(taskType="report.build", payload={"report": 7}), 100.0)
    claimed = store.claim("worker-1", 30, 100.0)
    assert claimed is not None
    assert claimed["id"] == record["id"]
    assert claimed["status"] == "running"
    assert claimed["attempts"] == 1

    assert store.complete(record["id"], "other-worker", {"ok": True}, 101.0) == "conflict"
    assert store.complete(record["id"], "worker-1", {"ok": True}, 101.0) == "ok"
    finished = store.get(record["id"])
    assert finished is not None
    assert finished["status"] == "succeeded"
    assert finished["result"] == {"ok": True}


def test_failure_retries_then_dead_letters(tmp_path) -> None:
    store = make_store(tmp_path)
    record, _ = store.submit(main.SubmitTask(taskType="sync.run", maxAttempts=2), 100.0)

    first = store.claim("worker-a", 10, 100.0)
    assert first is not None
    assert store.fail(record["id"], "worker-a", "temporary", 5, 101.0) == "queued"
    assert store.claim("worker-b", 10, 104.0) is None

    second = store.claim("worker-b", 10, 106.0)
    assert second is not None
    assert second["attempts"] == 2
    assert store.fail(record["id"], "worker-b", "permanent", 0, 107.0) == "dead_letter"
    assert store.get(record["id"])["status"] == "dead_letter"  # type: ignore[index]


def test_expired_lease_can_be_reclaimed_with_attempt_budget(tmp_path) -> None:
    store = make_store(tmp_path)
    record, _ = store.submit(main.SubmitTask(taskType="index.block", maxAttempts=2), 100.0)
    assert store.claim("worker-a", 5, 100.0) is not None
    reclaimed = store.claim("worker-b", 5, 106.0)
    assert reclaimed is not None
    assert reclaimed["id"] == record["id"]
    assert reclaimed["workerId"] == "worker-b"
    assert reclaimed["attempts"] == 2


def test_queued_task_can_be_cancelled(tmp_path) -> None:
    store = make_store(tmp_path)
    record, _ = store.submit(main.SubmitTask(taskType="cleanup.run"), 100.0)
    assert store.cancel(record["id"], 101.0) == "ok"
    assert store.get(record["id"])["status"] == "cancelled"  # type: ignore[index]
    assert store.claim("worker", 10, 102.0) is None


def test_oversized_payload_is_rejected(tmp_path) -> None:
    store = make_store(tmp_path)
    with pytest.raises(ValueError):
        store.submit(main.SubmitTask(taskType="blob.store", payload={"data": "x" * main.MAX_PAYLOAD_BYTES}), time.time())


def test_optional_api_token_is_fail_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(main, "API_TOKEN", "0123456789abcdef0123456789abcdef")
    client = TestClient(main.app)
    assert client.get("/api/v1/tasks/missing").status_code == 401
    response = client.get(
        "/api/v1/tasks/missing",
        headers={"Authorization": "Bearer 0123456789abcdef0123456789abcdef"},
    )
    assert response.status_code == 404
