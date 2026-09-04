# SPEC §11: up, down, migrate, seed, refresh, test, lint.
# `make up` is the only setup step (SPEC §3). Database targets run inside Compose (Docker only);
# `make test` and `make lint` run on the host and need uv (https://docs.astral.sh/uv/). `make test`
# also needs Docker for testcontainers unless TEST_DATABASE_URL is set (see .env.example).

COMPOSE ?= docker compose
UV      ?= uv

.PHONY: help up down migrate seed refresh test lint

help: ## List targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-10s %s\n", $$1, $$2}'

up: ## Start Postgres 16 and wait until healthy (the app service joins in Phase 4, when web/app.py exists)
	$(COMPOSE) up -d --wait db

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
