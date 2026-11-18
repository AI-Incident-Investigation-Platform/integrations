# Service design

## Decisions

2026-11-17: Connectors and repositories store only public settings in their main rows.
Credentials are AES-256-GCM envelopes with the owner UUID as associated data and a key ID
for rotation. Public response schemas omit the credential field.

2026-11-17: Health checks run outside database transactions, then write status, history and
`integration.health_changed` to one transaction. Due work is claimed with `SKIP LOCKED` and
rescheduled with jitter. Outbox relay publishes events to `integrations.events`.

2026-11-17: Explorer uses a separate bounded HTTP pool, a 16-request semaphore, five-second
timeout, two-megabyte response cap and no redirects. Loki filters are constructed from UUIDs
and JSON-escaped text. Prometheus accepts only named query templates. All queries first
check `project.read`. The selected connector's encrypted bearer credential is decrypted
after the database read and supplied only to its upstream request. External clients ignore
environment proxy settings.

2026-11-17: A Tempo trace detail read validates every accepted resource against the requested
project and environment and requires a service UUID. Cross-service parent spans produce at
most 500 topology edges. The event window is the earliest and latest accepted span start;
each edge retains its latest observed time. Oversized traces are rejected instead of silently
ignoring unvalidated resources.

2026-11-17: A background worker scans connected Tempo sources every five minutes, at most ten
connectors and ten recent traces per connector per pass. It reads a five-minute window and
emits the same edge format as trace detail. A deterministic inbox key per connector and trace
deduplicates output across replicas. It advances a connector ID cursor between passes.

2026-11-17: Connector, repository and health-check lists use keyset pagination with an opaque
cursor (UUID for owners, timestamp for checks) and a 100-item page. Explorer cursors need a
separate upstream pagination policy.

2026-11-17: Loki explorer cursors retain the nanosecond timestamp and count already returned
at that timestamp. The next query overlaps the timestamp because Loki's `end` is exclusive;
the service sorts tied entries locally and skips the prior count. It fetches at most 1000 log
entries per page attempt and rejects a saturated timestamp instead of silently dropping it.
Tempo search cursors retain pending time segments. A segment returning more than 100 traces
is split until it is below the limit; a saturated one-second segment is rejected. Every
candidate is fetched by trace ID and all resource tags must match project and environment,
with at least one span from the requested service. This detail validation is required because
Tempo tag search uses case-insensitive substring matching. Tempo also returns the first N
matches in nondeterministic order, so a configured server cap below the requested 101 or
concurrent ingestion can still make a page incomplete. See
[Loki query_range](https://grafana.com/docs/loki/latest/reference/loki-http-api/) and
[Tempo search](https://grafana.com/docs/tempo/latest/api_docs/).

2026-11-17: `project.deleting` is consumed from `integrations.projects-events`. Inbox deduplication,
project-scoped deletion and the `cleanup.completed` outbox command share one transaction.
Integration and repository credentials are selected by owner UUID and deleted before their
owners. Health history cascades with integrations. Prior project outbox rows are removed
before writing the acknowledgement.

## Dependencies and limits

Postgres stores service data and the transactional outbox. RabbitMQ carries events. Dragonfly
backs ACL caching. Auth provides JWKS and service tokens; projects provides ACL decisions.
The external HTTP transport resolves and validates every address immediately before opening
the TCP connection, then connects to the selected IP while preserving the original hostname
for TLS SNI and certificate verification. The deployment must also enforce egress policy as
defense in depth. Migration and connector/cleanup flows were tested with isolated
PostgreSQL and RabbitMQ containers. Synthetic Loki logs and Tempo traces passed live cursor
tests against isolated containers. No customer API or k3d verification has been performed.
