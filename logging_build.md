# Build log: DE_modern

This file hands work over from one session to the next. Update it at every review stop ⏸.

The plan of record is in [`PLAN.md`](PLAN.md). This file tracks progress against that plan, plus every decision or deviation taken along the way.

## Status

| # | Layer | Status | Date | Commit |
|---|---|---|---|---|
| 0 | Records: git init, `PLAN.md`, `logging_build.md` | ✅ done | 2026-09-15 | `4cae360` |
| 1a | Environment: uv workspace, `.env.example`, Makefile skeleton | ✅ done | 2026-09-15 | `9c0606a` |
| 1b | Source: sim clock, world, engine, faults, FastAPI and SSE, tests, Dockerfile, compose | ✅ done | 2026-09-15 | `efee5aa` |
| 2 | Kafka (KRaft), topic init, bridge (SSE → Kafka, DLQ), Redpanda Console | ⏸ waiting for your review | 2026-09-15 | "Layer 2" commit |
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

### Layer 2: Kafka, the bridge, Console (2026-09-15)

**Read first, in this order:**

1. The `kafka` service in `docker-compose.yml`: KRaft and the two listeners, commented.
2. `kafka/create-topics.sh`: topics, keys, partitions, retention.
3. `ingest/bikeshare_bridge/bridge.py`: the loop and the failure table in its docstring.
4. `contract.py`: routing and the DLQ.
5. `sink.py`: the idempotent producer, and why the checkpoint waits for flush().

| File | Purpose |
|---|---|
| `kafka/create-topics.sh` | The only place topics are declared: `trip-events.v1` (3 partitions, key bike_id), `station-status.v1` (3, key station_id), `dlq.v1` (1 partition, 30 days), `alerts.v1` (3, for layer 3). Retention is 7 days unless stated. |
| `ingest/bikeshare_bridge/config.py` | Env-only settings (`SOURCE_URL`, `KAFKA_BOOTSTRAP`, `BRIDGE_STATE_DIR`, `BRIDGE_START_FROM`, …) and topic names. |
| `…/sse.py` | A hand-written SSE parser (keepalive comments become frames) and `open_stream()` with `Last-Event-ID`. |
| `…/contract.py` | The consumer-side copy of the contract (strict pydantic), and `route()`: valid → topic, key and headers; invalid → DLQ record with `error_type`, `errors` and `raw`. |
| `…/sink.py` | `KafkaSink`: confluent-kafka producer (idempotent, zstd, linger 20 ms), delivery tracking, `flush()` raises `DeliveryFailed`, `check_topics()`. librdkafka logs go through Python logging. |
| `…/checkpoint.py` | `data/bridge/checkpoint.json`, written atomically, only after a successful flush. |
| `…/bridge.py` | Reconnect loop with backoff, frame handling, gap accounting, reset detection, periodic checkpoint and throughput report, clean `close()`. |
| `…/logs.py` | One JSON object per line. |
| `…/__main__.py` | Entry point: SIGTERM → SystemExit → `close()`. Fails fast if Kafka or the topics are missing. |
| `ingest/tests/` | 22 tests: SSE parser, contract routing (every drift variant, negative vs over-capacity), a **consumer-driven contract test against the real source simulation**, and the bridge loop against a scripted source (`bridgekit.FakeSource`, httpx MockTransport) and a fake producer: resume, live start, delivery failure keeps the checkpoint, gap, source reset, clean close. |
| `ingest/Dockerfile` | Same recipe as the source. |
| `docker-compose.yml` | Adds `kafka` (`apache/kafka:4.3.1`, 512 MB heap, named volume `kafka-data`), `kafka-init` (one-shot), `bridge`, `console` (`redpandadata/console:v3.11.0` on :8081). |
| `Makefile` | Adds `topics`, `tail`, `dlq`, `bridge-run`, `bridge-reset` and `reset` (wipes the world, checkpoint and topics together). `ps` now shows one-shots too. Exports `BUILDX_NO_DEFAULT_ATTESTATIONS=1`. |
| `.env.example` | Adds `KAFKA_HOST_PORT=9094` and `CONSOLE_PORT=8081`. Drops `COMPOSE_PROJECT_NAME`, which the compose `name:` already sets. |

**How to poke at it:**

- `make up`, then `make topics` and `make tail t=bikeshare.trip-events.v1`
- `make fault f=schema_drift`, then `make dlq`
- `make logs s=bridge`
- http://localhost:8081

**Verified:**

- **Bring-up:** 5 services in ~25 s. `kafka-init` Exited (0) with 4 topics.
- **First start:** the bridge replayed the source buffer (29,410 events in 30 s, ~976/s), then reached `source_lag: 0`.
- **Consistency probe** (pause the source, compare the sum of end offsets with the source's `last_seq`):
  - after clean restarts of the bridge and source: **exactly equal** (31,536 = 31,536);
  - after `SIGKILL` of the bridge: **18 duplicates**, the events since the last 2 s checkpoint, as designed;
  - after a 3 h fast-forward with the bridge stopped: caught up in under 5 s, no new duplicates;
  - after a source restart (connection refused once, then resume) and a Kafka restart (producer buffered and retried): no loss, no new duplicates.
- **Host bridge** via `localhost:9094` (EXTERNAL listener): works and hands the checkpoint back to the container.
- **DLQ vs ground truth:** 64 events the fault log says break the contract (schema_drift, plus negative impossible_status) = the 64 dead-lettered event_ids. 0 missed, 0 unexpected.

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
| 2026-09-15 | Image tags are pinned in `docker-compose.yml`, next to their service. The plan said `.env`. | One less indirection when reading the compose file. `.env` keeps ports and settings. |
| 2026-09-15 | Kafka messages carry the source's JSON bytes untouched. Metadata goes in headers (`event_type`, `schema_version`, `source_seq`, `ingested_at`). | Exact lineage: what the vendor sent is what consumers read. `ingested_at` is the pipeline's wall clock, the third of the "three clocks". |
| 2026-09-15 | Bridge delivery is at-least-once. The checkpoint is saved only after `flush()` succeeds, every 2 s and on shutdown. A delivery failure rewinds to the checkpoint. | Duplicates are bounded (18 after a SIGKILL) and dropped downstream by `event_id`. Exactly-once would need Kafka transactions plus offsets in the sink, more machinery than a bridge needs. |
| 2026-09-15 | The bridge detects a reset source (checkpoint ahead of the source's `last_seq`) and restarts from its oldest event. | Otherwise it would silently wait for seq numbers that belong to a previous world. `make reset` wipes all state together. |
| 2026-09-15 | `BUILDX_NO_DEFAULT_ATTESTATIONS=1` is exported by the Makefile. `provenance: false` in compose did not help (tested). | Default provenance attestations change the image ID on every build, so `make up` restarted the source and bridge every time. |
| 2026-09-15 | librdkafka logs are routed into Python logging (`"logger"` in the producer config). | Keeps the bridge's output 100% JSON lines. |

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
| Base image (source, bridge) | `python:3.12-slim` + `ghcr.io/astral-sh/uv:0.12.9` | `source/Dockerfile`, `ingest/Dockerfile` |
| Kafka | `apache/kafka:4.3.1` (KRaft, JDK 21), cluster id `MoQ1R7PRSkaSfyAV5NC-sg` | `docker-compose.yml` |
| Redpanda Console | `redpandadata/console:v3.11.0` | `docker-compose.yml` |

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
- **Every `make up` re-runs `kafka-init`.** Compose re-runs a completed one-shot dependency. It is idempotent (`--if-not-exists`) and takes ~10 s.
- **`make tail` joins a throwaway consumer group** (`console-consumer-…`, auto-commit off). It may show up briefly in the Console's group list.
- **The Console's topic list includes `__consumer_offsets`**, Kafka's internal topic. `make topics` hides it.
- **Rebuilding an image recreates its container.** Any change to the root `pyproject.toml` or `uv.lock` rebuilds both Python images, because both copy them. Code-only changes rebuild just the affected image.
- **Next (layer 3):**
  - a Spark 4.2.x Structured Streaming analyzer in its own container, `local[*]`, with the Kafka connector baked into the image;
  - it reads `trip-events` and `station-status`, deduplicates by `event_id` within a watermark, and applies status rules (impossible values, flatline) and trip rules (teleport, orphans via a stream-stream join);
  - it writes alerts to `bikeshare.alerts.v1` and 15-minute station metrics to `data/lake/` (Parquet);
  - checkpoints go to `data/checkpoints/`.
