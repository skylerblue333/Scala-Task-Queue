# Sky Queue

**SKYCOIN4444 standalone product #8** — a durable single-node background-task queue API implemented in Python/FastAPI with SQLite/WAL.

The repository name is historical. The shipped product is **not Scala** and the documentation intentionally follows the code that actually runs.

## Implemented capability

- durable SQLite task storage with WAL and full synchronous durability mode;
- idempotent task submission via unique idempotency keys;
- validated task types, bounded JSON payloads, configurable delay, and attempt budget;
- lease-based worker claiming with bounded lease duration;
- expired-lease recovery while retry budget remains;
- completion ownership checks so only the active worker can settle a task;
- retry scheduling with explicit delay;
- dead-letter transition when the attempt budget is exhausted;
- cancellation of queued work;
- persistent task status/result/error inspection;
- optional constant-time bearer gate for `/api/v1/*` operations;
- `/healthz`, `/readyz`, and status-count `/metrics` endpoints;
- non-root container with persistent `/data` volume;
- tests for idempotency, claim/complete, retries, dead-lettering, lease recovery, cancellation, payload limits and auth;
- CI compile, Ruff, pytest, dependency audit, Docker build and non-root-image gates.

## Run locally

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
export QUEUE_DB_PATH=./data/queue.db
uvicorn src.main:app --port 8080
```

Submit a task:

```bash
curl -X POST http://localhost:8080/api/v1/tasks \
  -H 'content-type: application/json' \
  -d '{"taskType":"email.send","payload":{"to":"user@example.test"},"idempotencyKey":"order-123"}'
```

Claim work:

```bash
curl -X POST http://localhost:8080/api/v1/workers/claim \
  -H 'content-type: application/json' \
  -d '{"workerId":"worker-1","leaseSeconds":60}'
```

Workers then call `/api/v1/tasks/{id}/complete` or `/api/v1/tasks/{id}/fail` with the same `workerId`.

## Verify

```bash
python -m compileall -q src tests
ruff check src tests
pytest -q
pip-audit -r requirements.txt
docker build -t sky-queue .
```

## Container

```bash
docker build -t sky-queue .
docker run --rm -p 8080:8080 -v sky-queue-data:/data sky-queue
```

Optional API protection:

```bash
-e QUEUE_API_TOKEN='generate-a-long-random-value'
```

## Architecture

```text
Producer
  │ submit + idempotency key
  ▼
Sky Queue
  ├─ SQLite/WAL durable task log
  ├─ available-at scheduling
  ├─ worker lease ownership
  ├─ retry/attempt budget
  └─ dead-letter state
         │
         ▼
      Worker(s)
```

## Deliberate boundaries

Read [`SECURITY.md`](SECURITY.md), [`PRODUCT.md`](PRODUCT.md), and [`MASTER_PLAN.md`](MASTER_PLAN.md).

This release is a **single-node durable queue**, not Kafka, SQS, RabbitMQ, NATS or a distributed exactly-once system. SQLite coordinates concurrent local clients, but HA replication, cross-node consensus, distributed leases, remote broker protocols, worker autoscaling and cloud SLA claims remain future integration work.

## License

See [`LICENSE`](LICENSE).
