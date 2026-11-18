# Requirements and status

Source: `backend/tasks.md` INT-1–INT-5 and `backend/services/integrations.md`.

| Requirement | Status | Evidence and remaining work |
|---|---|---|
| INT-1 connectors | Partial | CRUD, encrypted credentials, type list, manual test, scheduled checks with jitter, seven-day check retention and health-change outbox exist. Creation, credential use and health event are covered by a PostgreSQL integration test. No deployment smoke test. The outbound transport validates DNS at TCP connection time and pins the public IP, preserving the hostname for TLS. |
| INT-2 repositories | Partial | CRUD, encrypted tokens and service UUID storage exist. Service IDs are not validated against the projects catalog; GitHub and GitLab tokens are not tested upstream. |
| INT-3 explorer | Partial | Loki, Prometheus and Tempo query paths, dedicated semaphore and HTTP pool, connector bearer credentials, fixed PromQL templates and bounded results exist. Loki uses a timestamp/offset cursor; Tempo uses bounded time segments and validates every returned trace detail against project, environment and service before returning a summary. Synthetic data passed live Loki and Tempo cursor tests in isolated containers. Service IDs are not checked against the projects catalog. Tempo's search API returns the first N matches in nondeterministic order, so a server-side limit below 101 or concurrent ingestion can still cause incomplete pages. Connector, repository and health-check lists use keyset cursors. |
| INT-4 topology | Implemented locally | Trace detail and a five-minute scheduled Tempo scanner extract cross-service parent edges and write bounded `topology.observed` outbox events. The scanner limits each pass to ten connectors and ten traces per connector and deduplicates each integration/trace pair in PostgreSQL. The Tempo search and trace-detail paths were verified with synthetic traces in an isolated container; the complete scanner-to-projects event flow remains unverified. |
| INT-5 MCP credentials | Pending contract | `backend/services/integrations.md` requires an internal service-token API after agreement with Oleg. Audience, scopes, response shape, secret exposure rules and audit semantics are not yet specified. No credential release endpoint is exposed. |
| Project deletion | Implemented locally | The `project.deleting` RabbitMQ consumer removes project rows and credentials in one PostgreSQL transaction and writes an idempotent `cleanup.completed` command. PostgreSQL and RabbitMQ integration tests cover deletion, repeated delivery and acknowledgement. A full cluster deletion run with projects is still pending. |

Public API additions in `backend/services/integrations.md` (integration types, checks and
repositories) are not present in the generated OpenAPI contract under `docs/contracts/`.
The owning contract repository needs an agreed update before clients rely on these paths.

The `catalog.changed` event is not consumed because projects does not expose the catalog
validation contract yet. Existing service IDs are stored without cross-service validation.
