from uuid import UUID

import jwt
from aiip_kit.access.model import Principal
from aiip_kit.errors import NotAuthenticatedError
from aiip_kit.service_auth.jwks import KeyResolver
from aiip_kit.service_auth.verifier import bearer


class JwtUserAuthenticator:
    def __init__(
        self, keys: KeyResolver, audience: str = "integrations", issuer: str = "auth"
    ) -> None:
        self._keys = keys
        self._audience = audience
        self._issuer = issuer

    async def authenticate(self, authorization: str | None) -> Principal:
        token = bearer(authorization)
        if token is None:
            raise NotAuthenticatedError
        try:
            kid = jwt.get_unverified_header(token).get("kid")
        except jwt.PyJWTError as exc:
            raise NotAuthenticatedError from exc
        if not isinstance(kid, str) or (key := await self._keys.get(kid)) is None:
            raise NotAuthenticatedError
        try:
            claims = jwt.decode(
                token,
                key,
                algorithms=["RS256"],
                issuer=self._issuer,
                audience=self._audience,
                options={"require": ["iss", "aud", "sub", "sid", "iat", "exp"]},
            )
            if claims.get("token_use") == "service":
                raise NotAuthenticatedError
            return Principal(user_id=UUID(claims["sub"]), session_id=UUID(claims["sid"]))
        except (jwt.PyJWTError, ValueError, TypeError, KeyError) as exc:
            raise NotAuthenticatedError from exc
