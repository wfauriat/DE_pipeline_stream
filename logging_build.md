# Build log: DE_modern

This file hands work over from one session to the next. Update it at every review stop ⏸.

The plan of record is in [`PLAN.md`](PLAN.md). This file tracks progress against that plan, plus every decision or deviation taken along the way.

## Status

| # | Layer | Status | Date | Commit |
|---|---|---|---|---|
| 0 | Records: git init, `PLAN.md`, `logging_build.md` | ✅ done | 2026-09-15 | `4cae360` |
| 1a | Environment: uv workspace, `.env.example`, Makefile skeleton | ✅ done | 2026-09-15 | `9c0606a` |
| 1b | Source: sim clock, world, engine, faults, FastAPI and SSE, tests, Dockerfile, compose | ⏸ waiting for your review | 2026-09-15 | "Layer 1b" commit |
| 2 | Kafka (KRaft), topic init, bridge (SSE → Kafka, DLQ), Redpanda Console | ⬜ todo | | |
| 3 | Spark Structured Streaming analyzer: alerts topic, Parquet metrics | ⬜ todo | | |
| 4 | Airflow 3 (LocalExecutor): DuckDB landing and API extract DAGs, pool, assets | ⬜ todo | | |
| 5 | dbt-duckdb project, `dbt_transform` DAG, serving copy, DQ scorecard | ⬜ todo | | |
| 6 | README guided tour, wiring map, smoke script, optional Streamlit | ⬜ todo | | |

Legend: ⬜ todo · ⏳ in progress · ⏸ waiting for your review · ✅ done

## How to resume (new session)

1. Read this file, then `PLAN.md` §5–§8 (architecture and components).
2. Check the repo state with `git log --oneline` and `git status`.
3. Pick up the first row above that is not ✅. Stop for review at the end of each layer.
4. Constraint: the sibling `../refund-lab-*` repos are a separate hands-on exercise. Never read, modify or reuse them.

## Built so far

### Layer 0: records (2026-09-15)

| File | Purpose |
|---|---|
| `PLAN.md` | Your initial request (verbatim), the follow-up decisions and the approved plan. |
| `logging_build.md` | This file. |
| `.gitignore` | Ignores `data/` (runtime bind mounts), `.venv/`, `.env` and dbt/Airflow artifacts. |

### Layer 1a: environment (2026-09-15)

| File | Purpose |
|---|---|
| `pyproject.toml` | Virtual uv workspace root. Members are `source` and `ingest`. Groups: `dev` and `warehouse` (synced by default), `spark` (opt-in). |
| `source/pyproject.toml` | Package `bikeshare-sim` (flat layout, `uv_build`). |
| `ingest/pyproject.toml` | Package `bikeshare-bridge` (flat layout, `uv_build`). |
| `uv.lock`, `.python-version` | Resolved environment (120 packages), Python 3.12. |
| `.env.example` | Template for `.env`. `make setup` fills in the UID/GID. |
| `Makefile` | `help`, `setup`, `test`, `lint` and `fmt` so far. Runs with `unexport VIRTUAL_ENV`. |

- `make setup` creates `.venv` (648 MB), `.env` (UID/GID 1001) and `data/{source,bridge,warehouse,lake,checkpoints}`.
- Checked: all imports work, the workspace packages are editable installs, and `dbt --version` and `ruff` run.

### Layer 1b: the synthetic source (2026-09-15)

**Read first, in this order:**

1. `config/source.toml`: every setting and fault, commented.
2. `source/bikeshare_sim/simulation.py`: how clock → engine → faults → event log are wired.
3. `api.py`: the HTTP surface and the concurrency model.
4. `engine.py`, `faults.py`, `eventlog.py`, `world.py`, `clock.py`, `schemas.py`: one concern each.

| File | Purpose |
|---|---|
| `config/source.toml` | World (40 stations, 600 bikes), clock (default 60×, 10 s steps), emission cadence, 9 faults with rates and parameters. |
| `source/bikeshare_sim/config.py` | TOML plus env overrides (`BIKESHARE_CONFIG`, `SIM_SEED`, `SIM_SPEED`, `SIM_STATE_DIR`), validated with pydantic. |
| `…/clock.py` | `SimClock`: speed, pause, resume and advance. Every control re-anchors, so time never jumps backwards. |
| `…/world.py` | The city: stations (zones, capacity), bikes, demand curves, destination pulls, weather model. |
| `…/engine.py` | Fixed-step truthful generator: trips (a heap of rides), status snapshots, silent rebalancing and capacity changes. |
| `…/faults.py` | Fault layer: 6 per-event and 3 episode injectors, triggers, and the ground-truth `FaultLog`. Uses its own RNG. |
| `…/eventlog.py` | Outbox: seq numbering, a 50k ring buffer, gap notices, subscriber wake-up, `close()` on shutdown. |
| `…/simulation.py` | Wiring, the asyncio run loop, and persistence (`data/source/state.pkl`, atomic write, config fingerprint). |
| `…/schemas.py` | The published contract: strict pydantic models, plus `EVENT_ADAPTER` for validation. |
| `…/api.py` | FastAPI: `/v1/stream` (hand-written SSE), `/v1/events`, `/v1/stations`, `/v1/bikes`, `/v1/weather`, `/admin/*`, `/health`. |
| `…/__main__.py` | Entry point. A `uvicorn.Server` subclass closes the SSE streams on SIGTERM, so shutdown and the final save stay clean. |
| `source/tests/` | 28 tests: clock, determinism, conservation, contract, rhythms, one signature per fault, API, SSE resume/gap, persistence. `simkit.py` holds the helpers. |
| `source/Dockerfile`, `.dockerignore` | `python:3.12-slim` plus uv 0.12.9. Installs only `bikeshare-sim` from `uv.lock`. Two layers: deps, then code. |
| `docker-compose.yml` | Service `source-api`. Runs as the host UID, mounts config read-only and `data/source`, has a healthcheck and a 20 s stop grace period. |
| `Makefile` | Adds the stack targets (`up`, `down`, `ps`, `logs`) and the source controls (`clock`, `speed`, `pause`, `resume`, `ff`, `stream`, `stats`, `faults`, `fault`, `fault-rate`, `fault-on`, `fault-off`, `fault-log`, `source-run`, `source-reset`). |
| `README.md` | Quickstart for layer 1. The full tour comes in layer 6. |

**How to poke at it:** `make up`, open http://localhost:8000/docs, then:

- `make stream`
- `make ff h=3`, then `make clock` (the backlog catches up in under 1 s)
- `make fault f=frozen_station station=ST-007`, then `make faults` and `make fault-log`

**Measured (full-size world, all faults on):**

- 2 simulated days take 1.2 s of CPU, which gives ~16k events per day: ~11.5k status, ~2.4k starts, ~2.4k ends.
- Rush peaks at 07–08 h and 17–18 h. Median trip is 13 min.
- Image: 536 MB.
- `state.pkl` is ~1.6 MB after half a day and ~16 MB with a full buffer.

**Verified:**

- Locally: live SSE, the type filter, fast-forward catch-up and triggers.
- Restart: resumes the world and seq from `state.pkl`.
- SIGTERM with an SSE client connected: the stream ends cleanly (no ERROR) and the state is saved.
- In Docker: healthy, runs as uid 1001, the host file belongs to you, 87 SSE frames in 4 s through `:8000`, resumes after `docker compose restart`.

## Decisions and deviations from the plan

| Date | Decision | Why |
|---|---|---|
| 2026-09-15 | Local environment is a uv workspace `.venv`. `/opt/venvs/pyDS` is not modified. | Reproducible lockfile, and the shared base environment stays clean. |
| 2026-09-15 | The bridge package is `ingest/bikeshare_bridge/`. The plan said `ingest/bridge/`. | The module name matches the distribution name `bikeshare-bridge`, with no build-backend renaming. |
| 2026-09-15 | `unexport VIRTUAL_ENV` in the Makefile. | Your shell activates `/opt/venvs/pyDS` globally. uv would warn about it, and other tools could pick up the wrong interpreter. |
| 2026-09-15 | `trip_ended` carries `start_station_id` and `duration_s`, like a "completed trip" record. | Teleports are then detectable from `trip_ended` alone (implied speed). The start/end join is still needed for orphans. |
| 2026-09-15 | `impossible_status`: 70% over capacity, 30% negative. A negative value violates the contract (`bikes_available ≥ 0`), **so the bridge will send it to the DLQ**. Over capacity passes the contract, so Spark and dbt must catch it with the station reference data. | This illustrates two kinds of check: contract validation at ingestion, and semantic checks against reference data downstream. PLAN.md §7 lists only Spark and dbt for this fault. |
| 2026-09-15 | Teleport rate raised to 0.01 (other per-event faults: 0.001–0.004). | Only short trips are eligible. At 0.003 there were ~2 teleports per simulated day. |
| 2026-09-15 | Operator rebalancing and capacity expansions are silent. They are visible only through `station_status` and `/v1/stations.updated_at`. | This is how a real vendor behaves, and it gives dbt snapshots (SCD2) something to track. |
| 2026-09-15 | Runtime controls (speed, fault settings) reset to the config at every restart. The world, time, buffer and fault log persist. | One mental model: config = controls, state.pkl = the world. |
| 2026-09-15 | Clean SSE shutdown: `__main__.Server.handle_exit` → `api.close_streams` → `EventLog.close()`. | Otherwise every source restart logs an ERROR with a traceback while the bridge is connected (seen in testing). |
| 2026-09-15 | pytest runs in importlib mode. Test helpers live in `source/tests/simkit.py`, which is on the `pythonpath`, rather than in `conftest` imports. | Several members' tests can share file names without clashing. |

## Pinned versions

Resolved in `uv.lock` on 2026-09-15. Image tags are added when their layer lands.

| Component | Version | Where pinned |
|---|---|---|
| Python (local) | 3.12.3 (system) | `.python-version` |
| fastapi / uvicorn / pydantic | 0.141.1 / 0.53.0 / 2.13.5 | `uv.lock` |
| numpy | 2.5.3 | `uv.lock` |
| httpx / confluent-kafka | 0.28.1 / 2.15.1 (librdkafka 2.15.1) | `uv.lock` |
| duckdb | 1.5.5 (**the Airflow image must use the same version**) | `uv.lock` |
| dbt-core / dbt-duckdb | 1.12.5 / 1.11.0 | `uv.lock` |
| harlequin | 2.14.0 | `uv.lock` |
| pyspark (opt-in `spark` group) | 4.2.0, so the Spark image should be 4.2.x | `uv.lock` |
| pytest / ruff | 9.1.1 / 0.16.7 | `uv.lock` |
| Base image (source) | `python:3.12-slim` + `ghcr.io/astral-sh/uv:0.12.9` | `source/Dockerfile` |

## Host environment (checked 2026-09-15)

- **Tools:** Docker 29.6.2, Compose v5.3.1, uv 0.12.9, Java 21.0.12, GNU Make 4.3, Node 24, git 2.43.
- **Machine:** 20 cores, 15 GiB RAM (~11 GiB available), ~15 GB free disk.
- **Host UID:GID** is 1001:1001, used for container bind mounts.
- Kafka, Spark, Airflow, dbt and duckdb are not installed on the host. They come from Docker images or the uv venv.
- The planned host ports (8000, 8080, 8081, 4040, 9092, 9094) were free. Airflow's metadata Postgres will not publish a host port.

## Known issues / TODO

- **Editor (Pyright) shows "import could not be resolved"** for every `bikeshare_sim` import. Runtime, tests and ruff are fine. Point the editor's interpreter at `.venv/bin/python`. `[tool.pyright]` in `pyproject.toml` sets `venv = ".venv"` for editors that read it.
- **Rush hours redirect many arrivals** ("destination full"). Internally that's ~20–45% of arrivals in the busiest hours. It doesn't affect data correctness. Tune it with `rebalancing_every_h` if the occupancy analytics look too extreme.
- **Shell messages:** in the French locale, a process killed by `timeout` prints "Complété". That's SIGTERM, not an error.
- **Next (layer 2):**
  - `apache/kafka` 4.x KRaft (dual listeners), plus a `kafka-init` topic creator;
  - the bridge in `ingest/bikeshare_bridge/`: SSE client with Last-Event-ID resume, a consumer-side contract copy, confluent-kafka producer, DLQ, JSON logs;
  - Redpanda Console on :8081;
  - bridge tests.
