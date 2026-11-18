# HTTP endpoints

Public endpoints use bearer user tokens. Project operations call the `aiip-kit` ACL checker;
missing resources return 404, denied access 403, invalid input 422 and unavailable ACL 503.
Responses never include credentials.

| Method and path | Input / output | Permission |
|---|---|---|
| GET `/api/v1/integration-types` | type descriptors | authenticated |
| GET `/api/v1/integrations` | project_id, optional environment_id and cursor → page | project.read |
| POST `/api/v1/integrations` | IntegrationWrite → Integration, 201 | integration.manage |
| GET `/api/v1/integrations/{id}` | Integration | project.read |
| PATCH `/api/v1/integrations/{id}` | IntegrationPatch → Integration | integration.manage |
| DELETE `/api/v1/integrations/{id}` | 204 | integration.manage |
| POST `/api/v1/integrations/{id}/test` | TestResult | integration.manage |
| GET `/api/v1/integrations/{id}/checks` | cursor → latest 100 checks per page | project.read |
| GET `/api/v1/repositories` | project_id, cursor → page | project.read |
| POST `/api/v1/repositories` | RepositoryWrite → Repository, 201 | repository.manage |
| GET `/api/v1/repositories/{id}` | Repository | project.read |
| PATCH `/api/v1/repositories/{id}` | RepositoryPatch → Repository | repository.manage |
| DELETE `/api/v1/repositories/{id}` | 204 | repository.manage |
| POST `/api/v1/telemetry/logs/search` | LogSearchRequest with optional cursor → LogSearchResult | project.read |
| POST `/api/v1/telemetry/metrics/query` | MetricQueryRequest → MetricQueryResult | project.read |
| GET `/api/v1/telemetry/traces` | project_id, environment_id, service_id, from, to, optional cursor → TracesPage | project.read |
| GET `/api/v1/telemetry/traces/{trace_id}` | project_id, environment_id → TraceDetail | project.read |

`GET /health` checks process liveness. `GET /ready` checks Postgres and cache.
The planned `/internal/v1/projects/{id}/integrations` and `/repositories` endpoints are
not exposed until their service-token and credential release contract is agreed.
