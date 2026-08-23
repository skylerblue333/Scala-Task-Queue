# Security Model — Sky Queue

## Implemented controls

- Task types and worker identifiers use restricted character sets and bounded lengths.
- JSON payload and result documents are serialized deterministically and capped at 64 KiB.
- Task submission supports unique idempotency keys to prevent duplicate enqueue during retries.
- Worker completion/failure requires ownership of the active lease.
- Leases expire and can be reclaimed only while attempt budget remains.
- Exhausted work transitions to `dead_letter` rather than being retried forever.
- Optional API bearer protection uses constant-time comparison.
- SQLite is configured with WAL, `synchronous=FULL`, busy timeout and explicit write transactions for claim/idempotency operations.
- The container runs as a non-root UID and stores mutable queue state in `/data`.
- CI performs compile, lint, tests, dependency audit, image build and user verification.

## Trust boundaries

Sky Queue stores task payloads as application data. Producers must not enqueue plaintext secrets unless the deployment's data-at-rest and access-control policy explicitly permits it. This service does not encrypt arbitrary task payloads itself.

The optional bearer token is a coarse service boundary, not tenant-aware authorization. For multi-tenant deployments, put Sky Gateway/Sky Identity or another authorization layer in front of the queue.

## Single-node boundary

The SQLite backend is intentionally a single-node durability design. It is not a distributed consensus log and no cross-node exactly-once guarantee is claimed. Running multiple containers against a network filesystem is not a supported HA design.

## Reporting

Use private security reporting when available. Never post production queue databases, bearer tokens, customer task payloads, or live credentials in public issues.
