"""Sky Queue: a durable single-node task queue API for the SKYCOIN4444 ecosystem."""

from __future__ import annotations

import hmac
import json
import os
import re
import sqlite3
import time
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

from fastapi import Depends, FastAPI, HTTPException, Request
from pydantic import BaseModel, Field, field_validator

SERVICE_NAME = "sky-queue"
MAX_PAYLOAD_BYTES = 64 * 1024
TASK_TYPE_RE = re.compile(r"^[A-Za-z0-9._:-]{1,100}$")
WORKER_RE = re.compile(r"^[A-Za-z0-9._:-]{1,100}$")


class SubmitTask(BaseModel):
    taskType: str = Field(min_length=1, max_length=100)
    payload: dict[str, Any] = Field(default_factory=dict)
    idempotencyKey: str | None = Field(default=None, min_length=1, max_length=128)
    maxAttempts: int = Field(default=3, ge=1, le=10)
    delaySeconds: int = Field(default=0, ge=0, le=86_400)

    @field_validator("taskType")
    @classmethod
    def validate_task_type(cls, value: str) -> str:
        if not TASK_TYPE_RE.fullmatch(value):
            raise ValueError("taskType contains unsupported characters")
        return value


class ClaimRequest(BaseModel):
    workerId: str = Field(min_length=1, max_length=100)
    leaseSeconds: int = Field(default=60, ge=5, le=3600)

    @field_validator("workerId")
    @classmethod
    def validate_worker(cls, value: str) -> str:
        if not WORKER_RE.fullmatch(value):
            raise ValueError("workerId contains unsupported characters")
        return value


class CompleteRequest(BaseModel):
    workerId: str = Field(min_length=1, max_length=100)
    result: dict[str, Any] = Field(default_factory=dict)


class FailRequest(BaseModel):
    workerId: str = Field(min_length=1, max_length=100)
    error: str = Field(min_length=1, max_length=1000)
    retryDelaySeconds: int = Field(default=5, ge=0, le=86_400)


class QueueStore:
    def __init__(self, path: str) -> None:
        self.path = str(Path(path).expanduser().resolve())
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=5000")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    id TEXT PRIMARY KEY,
                    idempotency_key TEXT UNIQUE,
                    task_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ('queued','running','succeeded','failed','dead_letter','cancelled')),
                    attempts INTEGER NOT NULL,
                    max_attempts INTEGER NOT NULL,
                    available_at REAL NOT NULL,
                    lease_until REAL,
                    worker_id TEXT,
                    result_json TEXT,
                    last_error TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                )
                """
            )
            connection.execute("CREATE INDEX IF NOT EXISTS idx_tasks_claim ON tasks(status, available_at, created_at)")

    @staticmethod
    def _serialize(value: dict[str, Any], *, field: str) -> str:
        try:
            encoded = json.dumps(value, separators=(",", ":"), sort_keys=True, allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{field} must be valid JSON data") from exc
        if len(encoded.encode("utf-8")) > MAX_PAYLOAD_BYTES:
            raise ValueError(f"{field} exceeds {MAX_PAYLOAD_BYTES} bytes")
        return encoded

    @staticmethod
    def _view(row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        return {
            "id": row["id"],
            "idempotencyKey": row["idempotency_key"],
            "taskType": row["task_type"],
            "payload": json.loads(row["payload_json"]),
            "status": row["status"],
            "attempts": row["attempts"],
            "maxAttempts": row["max_attempts"],
            "availableAt": row["available_at"],
            "leaseUntil": row["lease_until"],
            "workerId": row["worker_id"],
            "result": json.loads(row["result_json"]) if row["result_json"] else None,
            "lastError": row["last_error"],
            "createdAt": row["created_at"],
            "updatedAt": row["updated_at"],
        }

    def submit(self, task: SubmitTask, now: float) -> tuple[dict[str, Any], bool]:
        payload_json = self._serialize(task.payload, field="payload")
        task_id = str(uuid4())
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if task.idempotencyKey:
                existing = connection.execute(
                    "SELECT * FROM tasks WHERE idempotency_key = ?", (task.idempotencyKey,)
                ).fetchone()
                if existing:
                    connection.execute("COMMIT")
                    return self._view(existing) or {}, False
            try:
                connection.execute(
                    """
                    INSERT INTO tasks (
                        id,idempotency_key,task_type,payload_json,status,attempts,max_attempts,
                        available_at,created_at,updated_at
                    ) VALUES (?,?,?,?, 'queued',0,?,?,?,?)
                    """,
                    (
                        task_id,
                        task.idempotencyKey,
                        task.taskType,
                        payload_json,
                        task.maxAttempts,
                        now + task.delaySeconds,
                        now,
                        now,
                    ),
                )
                row = connection.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
                connection.execute("COMMIT")
                return self._view(row) or {}, True
            except sqlite3.IntegrityError:
                connection.execute("ROLLBACK")
                if not task.idempotencyKey:
                    raise
                row = connection.execute(
                    "SELECT * FROM tasks WHERE idempotency_key = ?", (task.idempotencyKey,)
                ).fetchone()
                if row is None:
                    raise
                return self._view(row) or {}, False

    def get(self, task_id: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            return self._view(connection.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone())

    def claim(self, worker_id: str, lease_seconds: int, now: float) -> dict[str, Any] | None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """
                UPDATE tasks
                   SET status='dead_letter', worker_id=NULL, lease_until=NULL,
                       last_error=COALESCE(last_error, 'worker lease expired after final attempt'), updated_at=?
                 WHERE status='running' AND lease_until <= ? AND attempts >= max_attempts
                """,
                (now, now),
            )
            row = connection.execute(
                """
                SELECT * FROM tasks
                 WHERE (status='queued' AND available_at <= ?)
                    OR (status='running' AND lease_until <= ? AND attempts < max_attempts)
                 ORDER BY available_at, created_at
                 LIMIT 1
                """,
                (now, now),
            ).fetchone()
            if row is None:
                connection.execute("COMMIT")
                return None
            connection.execute(
                """
                UPDATE tasks
                   SET status='running', attempts=attempts+1, worker_id=?, lease_until=?, updated_at=?
                 WHERE id=?
                """,
                (worker_id, now + lease_seconds, now, row["id"]),
            )
            updated = connection.execute("SELECT * FROM tasks WHERE id=?", (row["id"],)).fetchone()
            connection.execute("COMMIT")
            return self._view(updated)

    def complete(self, task_id: str, worker_id: str, result: dict[str, Any], now: float) -> Literal["ok", "not_found", "conflict"]:
        result_json = self._serialize(result, field="result")
        with self._connect() as connection:
            row = connection.execute("SELECT status, worker_id FROM tasks WHERE id=?", (task_id,)).fetchone()
            if row is None:
                return "not_found"
            if row["status"] != "running" or row["worker_id"] != worker_id:
                return "conflict"
            connection.execute(
                """
                UPDATE tasks SET status='succeeded', result_json=?, worker_id=NULL, lease_until=NULL, updated_at=?
                 WHERE id=?
                """,
                (result_json, now, task_id),
            )
            return "ok"

    def fail(self, task_id: str, worker_id: str, error: str, retry_delay: int, now: float) -> Literal["queued", "dead_letter", "not_found", "conflict"]:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT status, worker_id, attempts, max_attempts FROM tasks WHERE id=?", (task_id,)
            ).fetchone()
            if row is None:
                connection.execute("COMMIT")
                return "not_found"
            if row["status"] != "running" or row["worker_id"] != worker_id:
                connection.execute("COMMIT")
                return "conflict"
            terminal = row["attempts"] >= row["max_attempts"]
            new_status = "dead_letter" if terminal else "queued"
            connection.execute(
                """
                UPDATE tasks
                   SET status=?, available_at=?, worker_id=NULL, lease_until=NULL, last_error=?, updated_at=?
                 WHERE id=?
                """,
                (new_status, now + retry_delay, error, now, task_id),
            )
            connection.execute("COMMIT")
            return "dead_letter" if terminal else "queued"

    def cancel(self, task_id: str, now: float) -> Literal["ok", "not_found", "conflict"]:
        with self._connect() as connection:
            row = connection.execute("SELECT status FROM tasks WHERE id=?", (task_id,)).fetchone()
            if row is None:
                return "not_found"
            if row["status"] != "queued":
                return "conflict"
            connection.execute("UPDATE tasks SET status='cancelled', updated_at=? WHERE id=?", (now, task_id))
            return "ok"

    def metrics(self) -> dict[str, int]:
        with self._connect() as connection:
            rows = connection.execute("SELECT status, COUNT(*) AS count FROM tasks GROUP BY status").fetchall()
        values = {status: 0 for status in ("queued", "running", "succeeded", "failed", "dead_letter", "cancelled")}
        values.update({row["status"]: row["count"] for row in rows})
        return values

    def ping(self) -> None:
        with self._connect() as connection:
            connection.execute("SELECT 1").fetchone()


DB_PATH = os.getenv("QUEUE_DB_PATH", "data/queue.db")
API_TOKEN = os.getenv("QUEUE_API_TOKEN") or None
if API_TOKEN is not None and len(API_TOKEN) < 16:
    raise RuntimeError("QUEUE_API_TOKEN must contain at least 16 characters when configured")
store = QueueStore(DB_PATH)
app = FastAPI(title="Sky Queue", version="2.0.0")


def require_api_token(request: Request) -> None:
    if API_TOKEN is None:
        return
    expected = f"Bearer {API_TOKEN}"
    supplied = request.headers.get("authorization", "")
    if not hmac.compare_digest(expected, supplied):
        raise HTTPException(status_code=401, detail="unauthorized", headers={"WWW-Authenticate": "Bearer"})


@app.get("/healthz")
def health() -> dict[str, str]:
    return {"status": "healthy", "service": SERVICE_NAME}


@app.get("/readyz")
def ready() -> dict[str, str]:
    try:
        store.ping()
    except sqlite3.Error as exc:
        raise HTTPException(status_code=503, detail="queue storage unavailable") from exc
    return {"status": "ready", "service": SERVICE_NAME}


@app.get("/metrics")
def metrics() -> dict[str, Any]:
    return {"service": SERVICE_NAME, **store.metrics()}


@app.post("/api/v1/tasks", dependencies=[Depends(require_api_token)])
def submit(task: SubmitTask) -> tuple[dict[str, Any], int] | dict[str, Any]:
    try:
        record, created = store.submit(task, time.time())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    record["submission"] = "created" if created else "idempotent_replay"
    return record


@app.get("/api/v1/tasks/{task_id}", dependencies=[Depends(require_api_token)])
def get_task(task_id: str) -> dict[str, Any]:
    record = store.get(task_id)
    if record is None:
        raise HTTPException(status_code=404, detail="task not found")
    return record


@app.post("/api/v1/workers/claim", dependencies=[Depends(require_api_token)])
def claim(input: ClaimRequest) -> dict[str, Any]:
    record = store.claim(input.workerId, input.leaseSeconds, time.time())
    if record is None:
        return {"status": "empty"}
    return {"status": "claimed", "task": record}


@app.post("/api/v1/tasks/{task_id}/complete", dependencies=[Depends(require_api_token)])
def complete(task_id: str, input: CompleteRequest) -> dict[str, str]:
    try:
        outcome = store.complete(task_id, input.workerId, input.result, time.time())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if outcome == "not_found":
        raise HTTPException(status_code=404, detail="task not found")
    if outcome == "conflict":
        raise HTTPException(status_code=409, detail="task is not leased by this worker")
    return {"status": "succeeded"}


@app.post("/api/v1/tasks/{task_id}/fail", dependencies=[Depends(require_api_token)])
def fail(task_id: str, input: FailRequest) -> dict[str, str]:
    outcome = store.fail(task_id, input.workerId, input.error, input.retryDelaySeconds, time.time())
    if outcome == "not_found":
        raise HTTPException(status_code=404, detail="task not found")
    if outcome == "conflict":
        raise HTTPException(status_code=409, detail="task is not leased by this worker")
    return {"status": outcome}


@app.post("/api/v1/tasks/{task_id}/cancel", dependencies=[Depends(require_api_token)])
def cancel(task_id: str) -> dict[str, str]:
    outcome = store.cancel(task_id, time.time())
    if outcome == "not_found":
        raise HTTPException(status_code=404, detail="task not found")
    if outcome == "conflict":
        raise HTTPException(status_code=409, detail="only queued tasks can be cancelled")
    return {"status": "cancelled"}
