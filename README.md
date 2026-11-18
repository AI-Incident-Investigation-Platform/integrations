# AIIP integrations

This service stores external connector settings and encrypted credentials, runs health checks,
and exposes bounded Loki, Prometheus, and Tempo queries. Each public project operation uses the
`aiip-kit` ACL checker. Credentials are AES-256-GCM encrypted with the owner UUID as
associated data and never returned from public endpoints.

## Local development

Use Python 3.14 and `uv sync`. Copy `.env.example` to `.env` and configure the database,
RabbitMQ, Dragonfly, auth service client and a random 32-byte credential key. Run `make
upgrade`, `make check`, `make test`, then `make run`.

Outbound endpoints must use public HTTPS URLs. The service rejects private IP literals and
DNS answers and never follows redirects. For production, enforce matching egress network
policy as DNS can change between validation and connection.

## Scope still pending

See [requirements](docs/requirements.md), [endpoints](docs/endpoints.md), and
[design](docs/design.md) for implemented behavior and outstanding stage 1 work.
The MCP credential release contract and catalog validation endpoint need agreement with
their owners. Trace detail requires project, environment, and service UUID attributes on
every Tempo resource; observed cross-service parent links produce a bounded topology event
on trace detail reads and scheduled scans.
