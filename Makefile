.PHONY: help check lint-fix test test-integration test-all migrate upgrade run

help:
	@echo "check, lint-fix, test, test-integration, test-all, migrate, upgrade, run"

check:
	uv run ruff format --check src tests
	uv run ruff check src tests
	uv run mypy src tests

lint-fix:
	uv run ruff format src tests
	uv run ruff check --fix src tests

test:
	uv run pytest tests/unit

test-integration:
	uv run pytest tests/integration

test-all:
	uv run pytest tests

migrate:
	uv run alembic revision --autogenerate -m "$(MSG)"

upgrade:
	uv run alembic upgrade head

run:
	uv run python -m src
