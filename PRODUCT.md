# Sky Queue — Product Definition

Sky Queue is product #8 in the SKYCOIN4444 standalone-product roadmap.

## Product role

A compact durable task broker for single-node/self-hosted deployments. Producers submit bounded JSON work records; workers claim tasks under expiring leases and settle success/failure through explicit APIs.

## Useful product capability

- persistent background work across service restarts;
- idempotent submissions;
- delayed availability;
- retry and dead-letter lifecycle;
- lease ownership and recovery;
- operational status and container packaging.

## Explicit non-claims

Sky Queue is not represented as Kafka, RabbitMQ, SQS, NATS, a distributed stream platform, an exactly-once distributed executor or an HA broker. It does not execute arbitrary job code itself; workers remain separate processes/services.

## Productization gate

Exact-head CI must pass compile, Ruff, pytest, dependency audit, Docker build and non-root declaration checks. Completion is declared only after merge and default-branch verification.

## Future integration

`Py-Async-Worker` becomes a worker/client adapter rather than a duplicate queue server. SKYCOIN4444 applications should integrate through the queue API or a generated client contract.
