from aiip_kit.errors import (
    DependencyUnavailableError,
    NotAuthenticatedError,
    PermissionDeniedError,
    ProjectNotFoundError,
)
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from src.api.explorer import Explorer
from src.api.router import Authenticate, build_router
from src.api.service import IntegrationService, NotFound


def create_app(
    service: IntegrationService, explorer: Explorer, authenticate: Authenticate
) -> FastAPI:
    app = FastAPI(title="AIIP integrations")
    app.include_router(build_router(service, explorer, authenticate))

    @app.exception_handler(NotFound)
    @app.exception_handler(ProjectNotFoundError)
    async def missing(request: Request, exc: Exception) -> JSONResponse:
        return JSONResponse({"error": {"code": "not_found"}}, status_code=404)

    @app.exception_handler(NotAuthenticatedError)
    async def unauthorized(request: Request, exc: Exception) -> JSONResponse:
        return JSONResponse({"error": {"code": "unauthenticated"}}, status_code=401)

    @app.exception_handler(PermissionDeniedError)
    async def forbidden(request: Request, exc: Exception) -> JSONResponse:
        return JSONResponse({"error": {"code": "permission_denied"}}, status_code=403)

    @app.exception_handler(DependencyUnavailableError)
    async def unavailable(request: Request, exc: Exception) -> JSONResponse:
        return JSONResponse({"error": {"code": "dependency_unavailable"}}, status_code=503)

    @app.exception_handler(ValueError)
    async def invalid(request: Request, exc: ValueError) -> JSONResponse:
        return JSONResponse(
            {"error": {"code": "invalid_request", "message": str(exc)}}, status_code=422
        )

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    return app
