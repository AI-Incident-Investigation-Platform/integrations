FROM python:3.14-slim
COPY --from=ghcr.io/astral-sh/uv:0.9.0 /uv /uvx /bin/
WORKDIR /app
COPY pyproject.toml ./
RUN uv sync --no-dev
COPY src ./src
COPY alembic ./alembic
COPY alembic.ini ./alembic.ini
USER 1000:1000
EXPOSE 8007
CMD ["uv", "run", "--no-sync", "python", "-m", "src"]
