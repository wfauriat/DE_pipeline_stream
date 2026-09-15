# ─────────────────────────────────────────────────────────────────────────────
# Makefile: the single entry point for everything you run by hand.
#
#   make help      list every target, grouped by section
#
# Two families of targets:
#   local  → the uv-managed ./.venv (tests, lint, running a service on the host)
#   stack  → docker compose (grows layer by layer: source, kafka, spark, airflow)
# The source controls are plain curl calls to the source's admin API. Read
# them to see the HTTP contract; `jq` only formats the answers.
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

# By default BuildKit attaches a timestamped provenance attestation to every
# image. The image ID then changes on every build, even when every layer comes
# from cache, and `make up` would needlessly restart the containers.
# (Run `docker compose up --build` by hand and you will see those restarts.)
export BUILDX_NO_DEFAULT_ATTESTATIONS := 1

UV       ?= uv
COMPOSE  ?= docker compose
API      := http://localhost:$(or $(SOURCE_API_PORT),8000)
CURL     := curl -fsS
JSON     := -H 'content-type: application/json'

.PHONY: help setup dirs test lint fmt up down ps logs reset \
        source-run source-reset clock speed pause resume ff stream stats \
        faults fault fault-rate fault-on fault-off fault-log \
        topics tail dlq bridge-run bridge-reset

help: ## list targets
	@awk 'BEGIN {FS = ":.*?## "} \
	  /^##@/ {printf "\n\033[1m%s\033[0m\n", substr($$0, 5)} \
	  /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-13s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

##@ Local environment
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

##@ Stack (docker compose)
up: .env dirs ## build and start the stack in the background
	$(COMPOSE) up -d --build

down: ## stop the stack (data/ is kept)
	$(COMPOSE) down

ps: ## list containers and their health (one-shot ones too: kafka-init)
	$(COMPOSE) ps -a

logs: ## follow logs: make logs [s=source-api]
	$(COMPOSE) logs -f --tail=100 $(s)

# The pieces of state must stay consistent with each other: the bridge's
# checkpoint is a seq of the source's stream, and Kafka holds what was sent
# up to it. So reset them together.
reset: ## stop everything and wipe ALL state: source world, bridge checkpoint, Kafka topics
	$(COMPOSE) down -v
	rm -f data/source/state.* data/bridge/checkpoint.*

##@ Source: the synthetic bike-share API  (docs: http://localhost:8000/docs)
source-run: .env dirs ## run the source on the host instead of Docker (Ctrl-C stops it)
	SOURCE_PORT=$(or $(SOURCE_API_PORT),8000) $(UV) run python -m bikeshare_sim

source-reset: ## stop the source and wipe its state (a fresh world; see also `make reset`)
	-$(COMPOSE) rm -sf source-api
	rm -f data/source/state.pkl data/source/state.pkl.bak data/source/state.tmp

clock: ## show the simulated clock (sim_time, engine backlog, speed)
	@$(CURL) $(API)/admin/clock | jq .

speed: ## change speed: make speed x=600  (simulated seconds per real second)
	@$(CURL) -X PATCH $(API)/admin/clock $(JSON) -d '{"speed": $(x)}' | jq .

pause: ## freeze simulated time
	@$(CURL) -X PATCH $(API)/admin/clock $(JSON) -d '{"paused": true}' | jq .

resume: ## unfreeze simulated time
	@$(CURL) -X PATCH $(API)/admin/clock $(JSON) -d '{"paused": false}' | jq .

ff: ## fast-forward: make ff h=6 (or m=30). The skipped period arrives as a burst.
	@$(CURL) -X POST $(API)/admin/clock/advance $(JSON) \
	  -d '{"hours": $(or $(h),0), "minutes": $(or $(m),0)}' | jq .

stream: ## watch the raw SSE stream: make stream [types=trip_started,trip_ended]
	curl -sN "$(API)/v1/stream$(if $(types),?types=$(types))"

stats: ## counters: emitted events by type, injected faults, engine internals
	@$(CURL) $(API)/admin/stats | jq .

# jq program for `make faults`: one tab-separated line per fault. It lives in a
# variable because a backslash-newline inside a quoted recipe string would reach
# jq verbatim.
FAULTS_JQ := .[] | [.name, .kind, "enabled=\(.enabled)", "rate=\(.rate)", "injected=\(.injected)", \
             (.active_episodes | map(.station_id // "whole stream") | join(","))] | @tsv

faults: ## list faults: kind, enabled, rate, injected count, active episodes
	@$(CURL) $(API)/admin/faults | jq -r '$(FAULTS_JQ)' | column -t -s $$'\t'

fault: ## force one fault now: make fault f=teleport [station=ST-007]
	@$(CURL) -X POST $(API)/admin/faults/$(f)/trigger $(JSON) \
	  -d '$(if $(station),{"station_id": "$(station)"},{})' | jq .

fault-rate: ## change a fault's rate: make fault-rate f=duplicate r=0.05
	@$(CURL) -X PATCH $(API)/admin/faults/$(f) $(JSON) -d '{"rate": $(r)}' | jq .

fault-on: ## enable a fault: make fault-on f=duplicate
	@$(CURL) -X PATCH $(API)/admin/faults/$(f) $(JSON) -d '{"enabled": true}' | jq .

fault-off: ## disable a fault: make fault-off f=duplicate
	@$(CURL) -X PATCH $(API)/admin/faults/$(f) $(JSON) -d '{"enabled": false}' | jq .

fault-log: ## the last 20 injected faults (ground truth)
	@$(CURL) "$(API)/admin/fault-log?limit=10000" \
	  | jq -c '.[-20:][] | {fault_id, fault_type, injected_at, event_id, details}'

##@ Kafka and the bridge  (Console UI: http://localhost:8081)
# The Kafka CLI tools ship inside the broker image. Run them there, against the INTERNAL listener.
KAFKA_BIN := $(COMPOSE) exec -T kafka /opt/kafka/bin

topics: ## topics with partition count and messages written (end offsets)
	@$(KAFKA_BIN)/kafka-get-offsets.sh --bootstrap-server kafka:9092 --exclude-internal-topics \
	  | awk -F: '{n[$$1] += $$3; p[$$1]++} \
	    END {for (t in n) printf "  %-30s partitions=%-2d messages=%d\n", t, p[t], n[t]}' | sort

tail: ## print messages: make tail t=bikeshare.trip-events.v1 [n=5] [from=beginning]
	@$(KAFKA_BIN)/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic $(t) \
	  --max-messages $(or $(n),5) $(if $(from),--from-beginning) --timeout-ms 20000 \
	  --command-property enable.auto.commit=false \
	  --formatter-property print.partition=true --formatter-property print.key=true \
	  --formatter-property print.headers=true 2>/dev/null || true

dlq: ## the first dead letters (events the bridge rejected): make dlq [n=3]
	@$(MAKE) -s tail t=bikeshare.dlq.v1 n=$(or $(n),3) from=beginning

# The container bridge must be stopped first: both would share data/bridge/checkpoint.json.
bridge-run: .env dirs ## run the bridge on the host (first: docker compose stop bridge)
	SOURCE_URL=$(API) KAFKA_BOOTSTRAP=localhost:$(or $(KAFKA_HOST_PORT),9094) \
	  BRIDGE_STATE_DIR=data/bridge $(UV) run python -m bikeshare_bridge

bridge-reset: ## stop the bridge and forget its checkpoint (next start replays the source buffer)
	-$(COMPOSE) rm -sf bridge
	rm -f data/bridge/checkpoint.json
