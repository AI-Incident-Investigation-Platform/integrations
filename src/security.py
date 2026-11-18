"""Credential envelope and outbound URL policy."""

import asyncio
import base64
import ipaddress
import os
import socket
from urllib.parse import urlsplit
from uuid import UUID

import httpcore
import httpx
from cryptography.hazmat.primitives.ciphers.aead import AESGCM


class PublicNetworkBackend(httpcore.AsyncNetworkBackend):
    def __init__(self, backend: httpcore.AsyncNetworkBackend | None = None) -> None:
        self.backend = backend or httpcore.AnyIOBackend()

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options=None,
    ) -> httpcore.AsyncNetworkStream:
        answers = await asyncio.get_running_loop().getaddrinfo(host, port, type=socket.SOCK_STREAM)
        if not answers:
            raise ValueError("Endpoint has no DNS answer")
        addresses = [ipaddress.ip_address(answer[4][0]) for answer in answers]
        if any(not address.is_global for address in addresses):
            raise ValueError("Endpoint resolves to a private address")
        return await self.backend.connect_tcp(
            str(addresses[0]),
            port,
            timeout=timeout,
            local_address=local_address,
            socket_options=socket_options,
        )

    async def connect_unix_socket(self, path: str, timeout=None, socket_options=None):
        raise ValueError("Unix sockets are not allowed for external endpoints")


class PublicHTTPTransport(httpx.AsyncHTTPTransport):
    """Preserve the HTTPS hostname for TLS while connecting to a validated public IP."""

    def __init__(self, max_connections: int) -> None:
        super().__init__(trust_env=False, limits=httpx.Limits(max_connections=max_connections))
        self._pool = httpcore.AsyncConnectionPool(
            max_connections=max_connections,
            network_backend=PublicNetworkBackend(),
        )


class CredentialCipher:
    def __init__(self, keys: dict[str, bytes], active_key_id: str) -> None:
        if active_key_id not in keys or any(len(key) != 32 for key in keys.values()):
            raise ValueError("AES-256 keys required")
        self.keys = keys
        self.active_key_id = active_key_id

    @classmethod
    def from_environment(cls) -> CredentialCipher:
        key_id = os.environ["CREDENTIAL_KEY_ID"]
        key = base64.b64decode(os.environ["CREDENTIAL_KEY"])
        return cls({key_id: key}, key_id)

    def encrypt(self, owner_id: UUID, secret: str) -> tuple[str, bytes]:
        nonce = os.urandom(12)
        value = AESGCM(self.keys[self.active_key_id]).encrypt(
            nonce, secret.encode(), owner_id.bytes
        )
        return self.active_key_id, nonce + value

    def decrypt(self, owner_id: UUID, key_id: str, ciphertext: bytes) -> str:
        return (
            AESGCM(self.keys[key_id])
            .decrypt(ciphertext[:12], ciphertext[12:], owner_id.bytes)
            .decode()
        )


def validate_url(value: str, *, allow_http: bool = False) -> str:
    parts = urlsplit(value)
    if parts.scheme not in ({"https", "http"} if allow_http else {"https"}):
        raise ValueError("HTTPS URL required")
    if not parts.hostname or parts.username or parts.password or parts.fragment:
        raise ValueError("Invalid endpoint URL")
    if parts.port is not None and parts.port not in {80, 443, 8443}:
        raise ValueError("Endpoint port is not allowed")
    if parts.hostname.endswith(".local") or parts.hostname.endswith(".internal"):
        raise ValueError("Private endpoint is not allowed")
    try:
        address = ipaddress.ip_address(parts.hostname)
    except ValueError:
        pass
    else:
        if not address.is_global:
            raise ValueError("Private endpoint is not allowed")
    return value.rstrip("/")


async def resolve_public_url(value: str) -> str:
    """Reject DNS names resolving to private, loopback or link-local networks."""
    value = validate_url(value)
    host = urlsplit(value).hostname
    assert host is not None
    addresses = await asyncio.get_running_loop().getaddrinfo(host, None, type=socket.SOCK_STREAM)
    if not addresses or any(not ipaddress.ip_address(item[4][0]).is_global for item in addresses):
        raise ValueError("Endpoint resolves to a private address")
    return value
