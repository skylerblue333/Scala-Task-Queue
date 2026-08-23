# Scala-Task-Queue

## Audit-correct status

Despite the repository name, the current implementation is a **Python/FastAPI metric-anomaly analysis service**, not a Scala task queue. The README previously overstated the implementation as an enterprise-grade Scala queue; this document intentionally describes the code that actually exists.

## Implemented

- FastAPI HTTP service
- `POST /api/v1/analyze` for threshold-based anomaly evaluation
- `GET /health` health endpoint
- Pydantic request validation
- Anomaly/deviation calculation
- High/low severity classification
- Automated tests covering health and anomaly/no-anomaly behavior

The implementation is currently small and focused. It should be treated as a reusable analysis primitive rather than a production distributed task queue.

## Not implemented / not verified

- Scala implementation
- Durable task queue
- Worker pool
- Redis/Kafka/NATS integration
- Persistent storage
- Authentication/authorization
- Distributed execution
- Production deployment
- Load/performance evidence
- End-to-end external-service tests

## SKYCOIN4444 consolidation role

The useful capability belongs conceptually in the **Analytics / Events / Jobs** boundaries. Before creating another independent service, compare this implementation with `skycoin4444-analytics` and `TypeScript-Message-Queue` and preserve the strongest functionality through shared contracts or adapters.

```text
Metric/Event
    ↓
Validation
    ↓
Anomaly Analysis
    ↓
Event / Job System
    ↓
Analytics / Operations
```

## Testing

Existing tests cover the health endpoint and basic anomaly classification. They do not establish production readiness.

## Setup

The repository currently uses Python/FastAPI despite its historical name. Use the repository dependency manifests and CI workflow as the source of truth for environment setup.

## Domains

- https://skycoin4444.com
- https://skycoin4444.net
- https://skycoin4444.shop
- https://skycoin44.token

## Authorship

Developed by Skyler Blue Spillers with human, open-source, automation, and AI-assisted engineering. AI assistance does not imply solely AI-authored work.
