"""Bounded telemetry queries. Incoming search text never becomes LogQL or PromQL syntax."""

import asyncio
import base64
import binascii
import contextlib
import hashlib
import json
import logging
import math
from datetime import UTC, datetime, timedelta
from uuid import NAMESPACE_URL, UUID, uuid5

import httpx
from aiip_kit.access.checker import AccessChecker
from aiip_kit.access.model import Principal
from aiip_kit.messaging.message import MessageV1
from aiip_kit.messaging.topology import Destination
from aiip_kit.outbox.writer import OutboxWriter
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.api.schemas import LogSearchRequest, MetricQueryRequest
from src.api.service import NotFound
from src.api.topology import TraceScopeError, observation, parse_trace
from src.core.db.models import Credential, Integration, inbox, outbox
from src.security import CredentialCipher, resolve_public_url

TEMPLATES = {
    "http_5xx_rate": 'sum(rate(http_requests_total{status=~"5..",service_id=~"{services}"}[5m]))',
    "http_p95_latency": (
        "histogram_quantile(0.95,sum by(le)(rate("
        'http_request_duration_seconds_bucket{service_id=~"{services}"}[5m])))'
    ),
    "http_request_rate": 'sum(rate(http_requests_total{service_id=~"{services}"}[5m]))',
    "db_pool_checked_out": 'sum(db_pool_checked_out{service_id=~"{services}"})',
    "db_pool_timeouts": 'sum(rate(db_pool_timeouts_total{service_id=~"{services}"}[5m]))',
    "pod_restarts": 'sum(kube_pod_container_status_restarts_total{service_id=~"{services}"})',
    "service_up": 'max(up{service_id=~"{services}"})',
}
log = logging.getLogger(__name__)


def bounded_window(start: datetime, end: datetime) -> None:
    if start.tzinfo is None or end.tzinfo is None or start >= end:
        raise ValueError("A valid timezone-aware interval is required")
    if end - start > timedelta(hours=24) or end > datetime.now(UTC) + timedelta(minutes=1):
        raise ValueError("Query interval exceeds 24 hours")


def promql(query_id: str, service_ids: list[UUID]) -> str:
    if query_id not in TEMPLATES or not service_ids:
        raise ValueError("A known query and at least one service are required")
    return TEMPLATES[query_id].format(services="|".join(map(str, service_ids)))


def logql(service_ids: list[UUID], text: str, project_id: UUID, environment_id: UUID) -> str:
    if not service_ids:
        raise ValueError("At least one service is required")
    selector = ",".join(
        [
            f'project_id="{project_id}"',
            f'environment_id="{environment_id}"',
            'service_id=~"' + "|".join(map(str, service_ids)) + '"',
        ]
    )
    return "{" + selector + "} |= " + json.dumps(text)


def encode_cursor(kind: str, scope: str, position: object) -> str:
    payload = json.dumps(
        [kind, hashlib.sha256(scope.encode()).hexdigest(), position], separators=(",", ":")
    )
    return base64.urlsafe_b64encode(payload.encode()).decode().rstrip("=")


def decode_cursor(cursor: str, kind: str, scope: str) -> object:
    if len(cursor) > 2000:
        raise ValueError("Invalid cursor")
    try:
        raw = base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4))
        name, digest, position = json.loads(raw)
    except (binascii.Error, ValueError, UnicodeDecodeError, TypeError) as exc:
        raise ValueError("Invalid cursor") from exc
    if name != kind or digest != hashlib.sha256(scope.encode()).hexdigest():
        raise ValueError("Cursor does not match query")
    return position


def normalize_trace_id(value: str) -> str | None:
    if 1 <= len(value) <= 32 and all(ch in "0123456789abcdef" for ch in value):
        return value.zfill(32)
    return None


class Explorer:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        access: AccessChecker,
        client: httpx.AsyncClient,
        cipher: CredentialCipher,
    ) -> None:
        self.sessions = sessions
        self.access = access
        self.client = client
        self.cipher = cipher
        self.semaphore = asyncio.Semaphore(16)
        self.writer = OutboxWriter(outbox)
        self.scan_after: UUID | None = None

    async def run_topology(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await self.scan_topology_once()
            except Exception as exc:
                log.warning("Topology scan failed: %s", type(exc).__name__)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=300)

    async def scan_topology_once(self) -> None:
        async with self.sessions() as session:
            query = select(Integration).where(
                Integration.type == "tempo",
                Integration.status == "connected",
                Integration.environment_id.is_not(None),
            )
            if self.scan_after is not None:
                query = query.where(Integration.id > self.scan_after)
            rows = (await session.scalars(query.order_by(Integration.id).limit(10))).all()
            if not rows:
                self.scan_after = None
                return
            self.scan_after = rows[-1].id
            sources = []
            for row in rows:
                credential = await session.get(Credential, row.id)
                headers = (
                    {
                        "Authorization": "Bearer "
                        + self.cipher.decrypt(row.id, credential.key_id, credential.ciphertext)
                    }
                    if credential
                    else {}
                )
                sources.append(
                    (row.id, row.project_id, row.environment_id, row.config_public["url"], headers)
                )
        now = datetime.now(UTC)
        for integration_id, project_id, environment_id, url, headers in sources:
            assert environment_id is not None
            try:
                endpoint = await resolve_public_url(url)
                search = await self._get(
                    endpoint + "/api/search",
                    {
                        "tags": f"project.id={project_id} environment.id={environment_id}",
                        "start": int((now - timedelta(minutes=5)).timestamp()),
                        "end": int(now.timestamp()),
                        "limit": 10,
                    },
                    headers,
                )
                for trace in search.get("traces", [])[:10]:
                    trace_id = normalize_trace_id(trace.get("traceID", ""))
                    if trace_id is None:
                        continue
                    data = await self._get(endpoint + "/api/traces/" + trace_id, {}, headers)
                    try:
                        spans, edges = parse_trace(data, project_id, environment_id)
                    except ValueError:
                        continue
                    if not edges:
                        continue
                    identity = f"{integration_id}:{trace_id}"
                    async with self.sessions.begin() as session:
                        inserted = await session.execute(
                            insert(inbox)
                            .values(
                                handler="topology.scan",
                                message_id=uuid5(NAMESPACE_URL, identity),
                                payload_hash=hashlib.sha256(identity.encode()).hexdigest(),
                            )
                            .on_conflict_do_nothing()
                            .returning(inbox.c.message_id)
                        )
                        if inserted.first() is None:
                            continue
                        event = MessageV1.event(
                            "topology.observed",
                            "integrations",
                            observation(spans, edges),
                            project_id=project_id,
                            aggregate_type="integration",
                            aggregate_id=integration_id,
                        )
                        await self.writer.add(
                            session, event, Destination.event("integrations", "topology.observed")
                        )
            except (httpx.HTTPError, ValueError, OSError) as exc:
                log.warning("Tempo topology source failed: %s", type(exc).__name__)

    async def _endpoint(
        self, project_id: UUID, environment_id: UUID, kind: str
    ) -> tuple[str, dict[str, str]]:
        async with self.sessions() as session:
            row = await session.scalar(
                select(Integration)
                .where(
                    Integration.project_id == project_id,
                    Integration.environment_id == environment_id,
                    Integration.type == kind,
                    Integration.status == "connected",
                )
                .limit(1)
            )
            if row is None:
                raise ValueError("No connected telemetry source")
            credential = await session.get(Credential, row.id)
            headers = (
                {
                    "Authorization": "Bearer "
                    + self.cipher.decrypt(row.id, credential.key_id, credential.ciphertext)
                }
                if credential
                else {}
            )
            endpoint = row.config_public["url"]
        return await resolve_public_url(endpoint), headers

    async def _get(self, url: str, params: dict, headers: dict[str, str]) -> dict:
        async with self.semaphore:
            response = await self.client.get(
                url, params=params, headers=headers, follow_redirects=False, timeout=5.0
            )
            response.raise_for_status()
            if len(response.content) > 2_000_000:
                raise ValueError("Upstream response is too large")
            return response.json()

    async def logs(self, user: Principal, body: LogSearchRequest) -> dict:
        await self.access.require(user, body.project_id, "project.read")
        bounded_window(body.start, body.end)
        scope = json.dumps(body.model_dump(mode="json", exclude={"cursor"}), sort_keys=True)
        boundary = int(body.end.timestamp() * 1e9)
        offset = 0
        if body.cursor:
            position = decode_cursor(body.cursor, "logs", scope)
            if (
                not isinstance(position, list)
                or len(position) != 2
                or not all(type(value) is int for value in position)
            ):
                raise ValueError("Invalid cursor")
            boundary, offset = position
            if (
                not int(body.start.timestamp() * 1e9) <= boundary < int(body.end.timestamp() * 1e9)
                or not 0 <= offset < 1000
            ):
                raise ValueError("Invalid cursor")
            boundary += 1
        endpoint, headers = await self._endpoint(body.project_id, body.environment_id, "loki")
        data = await self._get(
            endpoint + "/loki/api/v1/query_range",
            {
                "query": logql(body.service_ids, body.query, body.project_id, body.environment_id),
                "start": int(body.start.timestamp() * 1e9),
                "end": boundary,
                "limit": 1000,
                "direction": "backward",
            },
            headers,
        )
        records = []
        streams = data.get("data", {}).get("result", [])
        if len(streams) > 20:
            raise ValueError("Too many log streams")
        raw_count = sum(len(stream.get("values", [])) for stream in streams)
        for stream in streams:
            labels = stream.get("stream", {})
            try:
                service_id = UUID(labels["service_id"])
            except KeyError, ValueError:
                continue
            if (
                service_id not in body.service_ids
                or labels.get("project_id") != str(body.project_id)
                or labels.get("environment_id") != str(body.environment_id)
            ):
                continue
            for stamp, message in stream.get("values", []):
                nanos = int(stamp)
                if int(body.start.timestamp() * 1e9) <= nanos < boundary:
                    records.append((nanos, str(service_id), str(message), labels))
        records.sort(
            key=lambda item: (-item[0], item[1], item[2], json.dumps(item[3], sort_keys=True))
        )
        if body.cursor:
            records = [
                record
                for index, record in enumerate(records)
                if record[0] != boundary - 1 or index >= offset
            ]
        page = records[: body.limit]
        if raw_count >= 1000 and (
            len(records) <= body.limit or (page and page[-1][0] == records[-1][0])
        ):
            raise ValueError("Log timestamp is too dense to page safely")
        items = [
            {
                "id": str(stamp),
                "timestamp": datetime.fromtimestamp(stamp / 1e9, UTC).isoformat(),
                "service_id": service_id,
                "level": labels.get("level", "unknown"),
                "message": message[:10000],
            }
            for stamp, service_id, message, labels in page
        ]
        next_cursor = None
        if len(records) > body.limit and page:
            last_stamp = page[-1][0]
            consumed = sum(record[0] == last_stamp for record in page)
            if body.cursor and last_stamp == boundary - 1:
                consumed += offset
            next_cursor = encode_cursor("logs", scope, [last_stamp, consumed])
        return {"items": items, "next_cursor": next_cursor, "total_estimate": None}

    async def metrics(self, user: Principal, body: MetricQueryRequest) -> dict:
        await self.access.require(user, body.project_id, "project.read")
        bounded_window(body.start, body.end)
        if (body.end - body.start).total_seconds() / body.step_seconds > 2000:
            raise ValueError("Too many metric points")
        endpoint, headers = await self._endpoint(body.project_id, body.environment_id, "prometheus")
        data = await self._get(
            endpoint + "/api/v1/query_range",
            {
                "query": promql(body.query_id, body.service_ids),
                "start": body.start.timestamp(),
                "end": body.end.timestamp(),
                "step": body.step_seconds,
            },
            headers,
        )
        series = []
        for entry in data.get("data", {}).get("result", [])[:50]:
            series.append(
                {"labels": entry.get("metric", {}), "points": entry.get("values", [])[:2001]}
            )
        return {"series": series}

    async def traces(
        self,
        user: Principal,
        project_id: UUID,
        environment_id: UUID,
        service_id: UUID,
        start: datetime,
        end: datetime,
        cursor: str | None = None,
    ) -> dict:
        await self.access.require(user, project_id, "project.read")
        bounded_window(start, end)
        lower = int(start.timestamp())
        upper = math.ceil(end.timestamp())
        if upper <= lower:
            upper = lower + 1
        scope = json.dumps(
            [
                str(project_id),
                str(environment_id),
                str(service_id),
                start.isoformat(),
                end.isoformat(),
            ],
            separators=(",", ":"),
        )
        segments = [[lower, upper]]
        if cursor:
            position = decode_cursor(cursor, "traces", scope)
            if (
                not isinstance(position, list)
                or not 0 < len(position) <= 32
                or any(
                    not isinstance(segment, list)
                    or len(segment) != 2
                    or any(type(value) is not int for value in segment)
                    or not lower <= segment[0] < segment[1] <= upper
                    for segment in position
                )
            ):
                raise ValueError("Invalid cursor")
            segments = position
        endpoint, headers = await self._endpoint(project_id, environment_id, "tempo")
        requests = 0
        while segments and requests < 32:
            segment_start, segment_end = segments.pop()
            data = await self._get(
                endpoint + "/api/search",
                {
                    "tags": (
                        f"project.id={project_id} environment.id={environment_id} "
                        f"service.id={service_id}"
                    ),
                    "start": segment_start,
                    "end": segment_end,
                    "limit": 101,
                },
                headers,
            )
            requests += 1
            found = data.get("traces", [])
            if len(found) > 100:
                if segment_end - segment_start <= 1:
                    raise ValueError("Trace interval is too dense to page safely")
                middle = (segment_start + segment_end) // 2
                segments.extend([[segment_start, middle], [middle, segment_end]])
                continue
            candidates = []
            for trace in found:
                trace_id = normalize_trace_id(trace.get("traceID", ""))
                try:
                    started_nanos = int(trace["startTimeUnixNano"])
                except KeyError, TypeError, ValueError:
                    continue
                if trace_id is not None and segment_start <= started_nanos / 1e9 < segment_end:
                    candidates.append((started_nanos, trace_id))

            async def validated_trace(started_nanos: int, trace_id: str) -> dict | None:
                data = await self._get(endpoint + "/api/traces/" + trace_id, {}, headers)
                try:
                    spans, _ = parse_trace(data, project_id, environment_id)
                except ValueError:
                    return None
                if not any(span["service_id"] == str(service_id) for span in spans):
                    return None
                root = next((span for span in spans if span["parent_span_id"] is None), spans[0])
                return {
                    "trace_id": trace_id,
                    "root_service_id": root["service_id"],
                    "started_at": datetime.fromtimestamp(started_nanos / 1e9, UTC).isoformat(),
                    "duration_ms": max(span["duration_ms"] for span in spans),
                    "span_count": len(spans),
                    "has_error": any(span["status"] == "ERROR" for span in spans),
                }

            validated = await asyncio.gather(
                *(validated_trace(stamp, trace_id) for stamp, trace_id in candidates)
            )
            page = [item for item in validated if item is not None]
            page.sort(key=lambda item: (item["started_at"], item["trace_id"]), reverse=True)
            if page:
                return {
                    "items": page,
                    "next_cursor": encode_cursor("traces", scope, segments) if segments else None,
                }
        return {
            "items": [],
            "next_cursor": encode_cursor("traces", scope, segments) if segments else None,
        }

    async def trace_detail(
        self, user: Principal, trace_id: str, project_id: UUID, environment_id: UUID
    ) -> dict:
        grant = await self.access.require(user, project_id, "project.read")
        if len(trace_id) not in (16, 32) or any(ch not in "0123456789abcdef" for ch in trace_id):
            raise ValueError("Invalid trace id")
        endpoint, headers = await self._endpoint(project_id, environment_id, "tempo")
        data = await self._get(endpoint + "/api/traces/" + trace_id, {}, headers)
        try:
            spans, edges = parse_trace(data, project_id, environment_id)
        except TraceScopeError as exc:
            raise NotFound from exc
        if edges:
            event = MessageV1.event(
                "topology.observed",
                "integrations",
                observation(spans, edges),
                project_id=project_id,
                lifecycle_version=grant.lifecycle_version,
            )
            async with self.sessions.begin() as session:
                await self.writer.add(
                    session, event, Destination.event("integrations", "topology.observed")
                )
        started = min(span["started_at"] for span in spans)
        root = next((span for span in spans if span["parent_span_id"] is None), spans[0])
        return {
            "trace": {
                "trace_id": trace_id,
                "root_service_id": root["service_id"],
                "started_at": started,
                "duration_ms": max(span["duration_ms"] for span in spans),
                "span_count": len(spans),
                "has_error": any(span["status"] == "ERROR" for span in spans),
            },
            "spans": spans,
        }
