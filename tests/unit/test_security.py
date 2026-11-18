import asyncio
from datetime import UTC, datetime, timedelta
from uuid import uuid7

import pytest
from cryptography.exceptions import InvalidTag

from src.api.explorer import bounded_window, logql, promql
from src.api.service import public_config
from src.security import CredentialCipher, PublicNetworkBackend, validate_url


def test_credentials_are_bound_to_owner_and_ciphertext_is_not_plaintext() -> None:
    cipher = CredentialCipher({"v1": bytes(range(32))}, "v1")
    owner = uuid7()
    key_id, encrypted = cipher.encrypt(owner, "private-token")
    assert b"private-token" not in encrypted
    assert cipher.decrypt(owner, key_id, encrypted) == "private-token"
    with pytest.raises(InvalidTag):
        cipher.decrypt(uuid7(), key_id, encrypted)


@pytest.mark.parametrize(
    "url",
    [
        "http://example.com",
        "https://127.0.0.1",
        "https://10.0.0.1",
        "https://169.254.169.254",
        "https://service.local",
        "https://user:pass@example.com",
    ],
)
def test_private_and_unsafe_urls_rejected(url: str) -> None:
    with pytest.raises(ValueError):
        validate_url(url)


def test_public_config_disallows_extra_secret_fields() -> None:
    with pytest.raises(ValueError):
        public_config("loki", {"url": "https://example.com", "token": "secret"})


def test_explorer_escapes_text_and_forbids_raw_promql() -> None:
    project, env, service = uuid7(), uuid7(), uuid7()
    query = logql([service], '"} |= .*', project, env)
    assert query.endswith('|= "\\"} |= .*"')
    with pytest.raises(ValueError):
        promql("sum(up)", [service])


def test_explorer_window_capped_at_24_hours() -> None:
    now = datetime.now(UTC)
    with pytest.raises(ValueError):
        bounded_window(now - timedelta(days=2), now)


@pytest.mark.asyncio
async def test_external_connect_pins_validated_public_address(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, int]] = []

    class Backend:
        async def connect_tcp(self, host: str, port: int, **kwargs: object) -> object:
            calls.append((host, port))
            return object()

    async def public_answer(*args: object, **kwargs: object) -> list[tuple]:
        return [(0, 0, 0, "", ("93.184.215.14", 443))]

    loop = asyncio.get_running_loop()
    monkeypatch.setattr(loop, "getaddrinfo", public_answer)
    backend = PublicNetworkBackend(Backend())  # type: ignore[arg-type]
    await backend.connect_tcp("example.com", 443)
    assert calls == [("93.184.215.14", 443)]

    async def private_answer(*args: object, **kwargs: object) -> list[tuple]:
        return [(0, 0, 0, "", ("127.0.0.1", 443))]

    monkeypatch.setattr(loop, "getaddrinfo", private_answer)
    with pytest.raises(ValueError, match="private"):
        await backend.connect_tcp("example.com", 443)
    assert calls == [("93.184.215.14", 443)]
