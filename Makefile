# ─────────────────────────────────────────────────────────────────────────────
# Makefile: the single entry point for everything you run by hand.
#
#   make help      list every target with a one-line description
#
# Two families of targets:
#   local  → uv-managed ./.venv (tests, lint, running a service on the host)
#   stack  → docker compose (added layer by layer: source, kafka, spark, airflow)
#
# .env is included, so make and docker compose see the same ports and settings.
# ─────────────────────────────────────────────────────────────────────────────
SHELL := /bin/bash
.DEFAULT_GOAL := help

# If a venv is activated globally (e.g. /opt/venvs/pyDS), uv warns and other
# tools can pick up the wrong interpreter. uv always uses the project's .venv,
# so recipes run without it.
unexport VIRTUAL_ENV

# Optional include: .env does not exist until `make setup` has run once.
-include .env
export

UV      ?= uv
COMPOSE ?= docker compose

.PHONY: help setup dirs test lint fmt

help: ## list targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "} {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

# ── local environment ────────────────────────────────────────────────────────
setup: .env dirs ## create .venv (uv sync), .env (with your UID/GID) and data/ dirs
	$(UV) sync

# .env is generated once from the template, with your UID/GID filled in.
# Edit .env afterwards; it is never overwritten.
.env:
	sed -e "s/^HOST_UID=.*/HOST_UID=$$(id -u)/" \
	    -e "s/^HOST_GID=.*/HOST_GID=$$(id -g)/" .env.example > .env
	@echo "created .env from .env.example"

# Runtime data that containers bind-mount (gitignored).
dirs:
	@mkdir -p data/source data/bridge data/warehouse data/lake data/checkpoints

test: ## run unit tests (pytest, all workspace members)
	$(UV) run pytest

lint: ## ruff lint + format check
	$(UV) run ruff check . && $(UV) run ruff format --check .

fmt: ## ruff autofix + format
	$(UV) run ruff check --fix . && $(UV) run ruff format .
