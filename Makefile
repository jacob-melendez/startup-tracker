# SPEC §11: up, down, migrate, seed, refresh, test, lint — and deliberately nothing else.
# `make up` is the only setup step (SPEC §3). Database targets run inside Compose (Docker only);
# `make test` and `make lint` run on the host and need uv (https://docs.astral.sh/uv/). `make test`
# also needs Docker for testcontainers unless TEST_DATABASE_URL is set (see .env.example).
# There is no `make dev`: running the web app on the host with auto-reload is
# `uv run uvicorn web.app:app --reload` — one command, documented in the README.
# Nor is there a `make stats`, `make merge-review` or `make scheduler`. The two commands are
# `docker compose run --rm app python cli.py stats` and `... python cli.py merge-review`, and the
# scheduler is a service, not a command: `make up` starts it and it fires the cadences in
# config/connectors.yaml from then on (SPEC §7.2). `docker compose logs -f scheduler` reads it.

COMPOSE ?= docker compose
UV      ?= uv

.PHONY: help up down migrate seed refresh test lint

help: ## List targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-10s %s\n", $$1, $$2}'

up: ## Start Postgres 16, the web app and the connector scheduler (UI on http://localhost:8000)
	$(COMPOSE) up -d --wait db app scheduler

down: ## Stop all services (data volume is kept; `docker compose down -v` wipes it)
	$(COMPOSE) down

migrate: ## Apply Alembic migrations to the Compose database
	$(COMPOSE) run --rm app alembic upgrade head

seed: ## Load config/seed_companies.yaml, resolving every domain over the network (SPEC §10)
	$(COMPOSE) run --rm app python cli.py seed

refresh: ## Run connectors now: all of them, or CONNECTOR=<name> for one (needs CONTACT_EMAIL for sec_edgar)
	$(COMPOSE) run --rm app python cli.py refresh $(if $(CONNECTOR),--connector $(CONNECTOR),--all)

test: ## Run pytest (disposable Postgres via testcontainers unless TEST_DATABASE_URL is set in env or .env)
	$(UV) run pytest

lint: ## ruff check + ruff format --check + mypy --strict (scope in pyproject.toml)
	$(UV) run ruff check .
	$(UV) run ruff format --check .
	$(UV) run mypy
