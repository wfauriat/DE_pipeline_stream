# Build log: DE_modern

This file hands work over from one session to the next. Update it at every review stop ⏸.

The plan of record is in [`PLAN.md`](PLAN.md). This file tracks progress against that plan, plus every decision or deviation taken along the way.

## Status

| # | Layer | Status | Date | Commit |
|---|---|---|---|---|
| 0 | Records: git init, `PLAN.md`, `logging_build.md` | ✅ done | 2026-09-15 | `4cae360` |
| 1a | Environment: uv workspace, `.env.example`, Makefile skeleton | ✅ done | 2026-09-15 | `9c0606a` |
| 1b | Source: sim clock, world, engine, faults, FastAPI and SSE, tests, Dockerfile, compose | ✅ done | 2026-09-15 | `efee5aa` |
| 2 | Kafka (KRaft), topic init, bridge (SSE → Kafka, DLQ), Redpanda Console | ✅ done | 2026-09-15 | `04dede1` |
| 3 | Spark Structured Streaming analyzer: alerts topic, Parquet metrics | ✅ done (review deferred) | 2026-09-16 | `87929c4` |
| 4 | Airflow 3 (LocalExecutor): DuckDB landing and API extract DAGs, pool, assets | ✅ done (review deferred) | 2026-09-16 | `dbfff54` |
| 5 | dbt-duckdb project, `dbt_transform` DAG, serving copy, DQ scorecard | ⏸ waiting for your review | 2026-09-16 | "Layer 5" commit |
| 6 | README guided tour, wiring map, smoke script, optional Streamlit | ⬜ todo | | |

Legend: ⬜ todo · ⏳ in progress · ⏸ waiting for your review · ✅ done

## How to resume (new session)

1. Read this file, then `PLAN.md` §5–§8 (architecture and components).
2. Check the repo state with `git log --oneline` and `git status`.
3. Pick up the first row above that is not ✅. Stop for review at the end of each layer.
4. Constraint: the sibling `../refund-lab-*` repos are a separate hands-on exercise. Never read, modify or reuse them.

## Deferred reviews: replaying a layer from its commit

On 2026-09-16 you accepted layers 3 and 4 without their review stops, planning
to replay them later from their commits. Each commit holds one complete,
runnable layer:

| Layer | Commit | Section in this file |
|---|---|---|
| 3: Spark analyzer | `87929c4` | "Layer 3: Spark analyzer" |
| 4: Airflow and the landing zone | `dbfff54` | "Layer 4: Airflow and the DuckDB landing zone" |

To replay one:

```bash
make down                        # stop the current stack (every profile)
git switch --detach 87929c4      # the layer's commit (dbfff54 for layer 4)
make reset                       # optional: start from an empty world, topics, lake and warehouse
make setup && make up            # sync .venv to that commit's uv.lock, then build and start
# … explore with that layer's "How to poke at it" list …
make down && git switch main     # back to the latest layer
make setup && make up            # re-sync .venv and restart
```

Things to know:

- **Commit or stash first.** `git switch` refuses to leave uncommitted changes behind.
- **Layer 3's commit has no Airflow services.** Its Makefile defaults to `COMPOSE_PROFILES=stream`, but your current `.env` says `stream,batch`. Compose simply finds nothing in the `batch` profile.
- **Your data survives a replay unless you run `make reset`.** Newer data under `data/` (e.g. the DuckDB warehouse) is simply unused by layer 3.
- **Replaying layer 3 on the existing data** means Spark resumes from its checkpoints. For a clean replay, run `make spark-reset` before `make up`.

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

### Layer 3: Spark analyzer (2026-09-15 → 16)

**Read first, in this order:**

1. `streaming/analyzer/main.py`: the four queries and why there are four.
2. `rules.py`: the detection logic, as pure DataFrame functions.
3. `streaming/conf/spark-defaults.conf`: local mode, shuffle partitions, RocksDB state store.
4. `streaming/Dockerfile` and `entrypoint.sh`: connector at build time, arbitrary UID.

| File | Purpose |
|---|---|
| `streaming/stream_analyzer.py` | The spark-submit entry file. It imports `analyzer.main`. |
| `streaming/analyzer/config.py` | Env settings: Kafka, source URL, lake and checkpoint dirs, trigger, watermark (30 min), windows (30 min metrics, 120 min health), status interval, max trip duration (3 h), late threshold (25 min), starting offsets, maxOffsetsPerTrigger. |
| `…/schemas.py` | The event schema (envelope plus a flat payload superset), the station schema, the alert columns. |
| `…/rules.py` | `parse_events`, `over_capacity`, `teleports` (haversine), `late_events`, `station_windows`, `window_alerts`, `trip_pairs` (full outer join), `orphan_alerts`, `make_alerts` (deterministic `alert_id`), `to_kafka_records` (`ignoreNullFields=false`). |
| `…/reference.py` | `/v1/stations` fetched with urllib and refreshed every 60 s. It is used inside `foreachBatch`, because capacities change. |
| `…/progress.py` | `StreamingQueryListener`: one JSON line per non-empty batch (input rows, rate, watermark, state rows, rows dropped late, duplicates dropped). |
| `…/main.py` | Four queries: `reference_rules` (foreachBatch + persist → Kafka), `station_metrics` (parquet file sink, `partitionBy(date)`), `station_health` (2 h windows → Kafka), `trip_pairing` (join → Kafka). Each has its own checkpoint and a 10 s trigger. |
| `streaming/Dockerfile` | `spark:4.2.0-scala2.13-java21-python3-ubuntu` (Docker Official Image, Python 3.10). Resolves `spark-sql-kafka-0-10_2.13:4.2.0` with Ivy at build time and copies only the 4 connector jars. Spark ships the other transitive jars at identical versions (verified). |
| `streaming/entrypoint.sh` | nss_wrapper passwd entry for the host UID, under `tini`. |
| `streaming/conf/spark-defaults.conf`, `log4j2.properties` | Mounted read-only and commented. |
| `streaming/ruff.toml` | Lints this folder for Python 3.10. |
| `streaming/tests/` | 7 rule tests on a local SparkSession: real JSON through `parse_events`, one test per rule, the orphan time bound, deterministic ids. |
| `scripts/peek_alerts.py` | Host Kafka consumer (`localhost:9094`, `assign()`, no commits): counts by type and by distinct alert_id. |
| `scripts/peek_lake.py` | DuckDB reads `data/lake/station_metrics/**/*.parquet` in place. |
| `docker-compose.yml` | Service `spark`, profile `stream`, runs as host UID, mounts conf plus `data/lake` and `data/checkpoints`, Spark UI on :4040. |
| `Makefile` | Adds `alerts`, `lake` and `spark-reset` (wipes checkpoints, lake and the alerts topic). `COMPOSE_PROFILES ?= stream`. `down` and `reset` use `--profile '*'`, and `reset` also wipes checkpoints and the lake. |

**How to poke at it:**

- `make alerts`
- `make lake`
- `make logs s=spark | grep progress`
- http://localhost:4040
- `make fault f=teleport`, then about 20 s later `make alerts`

**Verified:**

- **Crash loop at first start**, caused by Hadoop login for a UID with no passwd entry. `HADOOP_USER_NAME` is not enough. Fixed with nss_wrapper.
- **Catch-up:** the backlog of ~100k events came in 20k-row batches. The four queries then settled at a few hundred rows per 10 s batch.
- **Restart resumes from the checkpoints:** batch ids continue from 50.
- **`spark-reset` replay:** rebuilds the lake and the alerts from Kafka's retention.
- **Resources:** Spark ~1.6 GB; the whole stack ~2.2 GB.
- **Scorecard vs ground truth** (ad-hoc script, final rules, after injecting 8 frozen and 4 silent episodes):

  | alert type | precision | recall |
  |---|---|---|
  | over_capacity | 100% | 100% |
  | teleport | 100% | 98% |
  | late_event (incl. stall backlog) | 100% | 100% |
  | orphan_trip | 66% | 100% |
  | frozen_station | 86% | 40% |
  | silent_station | 100% | 100% |

  - **Orphan false positives:** 16 had their partner dead-lettered (schema drift). 16 could not be checked (the partner had left the source buffer).

### Layer 4: Airflow and the DuckDB landing zone (2026-09-16)

**Read first, in this order:**

1. `orchestration/include/landing/kafka_loader.py`: the exactly-once argument is in its docstring.
2. `orchestration/dags/stream_landing.py`: how a DAG wires connections, variable, pool and asset.
3. The `x-airflow-common` block in `docker-compose.yml`: Airflow 3's pieces and their configuration.
4. `landing/warehouse.py`: the single-writer rule and the `raw` schema.

| File | Purpose |
|---|---|
| `orchestration/include/landing/warehouse.py` | `connect()` retries on the DuckDB write lock and sets the session TimeZone to UTC. `ensure_raw_schema()` is idempotent DDL: `raw.trip_events`, `station_status`, `dlq` and `alerts` (identical shape, PK topic/partition/offset, value kept as JSON text); `_kafka_offsets`; `_ingest_batches` (audit); typed `stations`, `bikes`, `weather`, `fault_log`. |
| `…/landing/kafka_loader.py` | `plan_slices` (start = offset stored in DuckDB, end = end-offset snapshot, retention-gap note), `read_slices` (time budget), `land_topic` (one transaction for rows, offsets and audit, then a Kafka commit as consumer group `duckdb-loader`, purely for the lag view). |
| `…/landing/api_extract.py` | stations and bikes: snapshot, replaced. weather: incremental by time. fault_log: incremental by id, paged. Each extract is one idempotent transaction with an audit row. |
| `orchestration/dags/stream_landing.py` | Every 5 min: `prepare_warehouse` → `land_<table>` × 4 (pool `duckdb`) → `report` (outlet Asset `raw_stream` carrying row counts; skipped when nothing landed, so no asset event). |
| `orchestration/dags/api_extract.py` | Every 15 min: `prepare_warehouse` → stations, bikes, weather, fault_log → `report` (Asset `raw_api`). |
| `orchestration/Dockerfile` | `apache/airflow:3.3.1-python3.12`, plus confluent-kafka 2.15.1 and duckdb 1.5.5 installed with Airflow pinned, plus `/opt/dbt-venv` (dbt-core 1.12.5, dbt-duckdb 1.11.0) for layer 5. |
| `orchestration/tests/` | 12 tests on a real DuckDB file: first run, incremental run, bounded snapshot, **a failed write rolls back rows and position together and never commits to Kafka**, expired offsets, per-run cap, idempotent extracts, weather cursor, fault-log paging, and the lock wait across processes (subprocess holder). |
| `docker-compose.yml` | Profile `batch`: `airflow-db` (postgres:16-alpine, no host port), `airflow-init` (migrate + pool), `airflow-apiserver` (:8080), `airflow-scheduler`, `airflow-dag-processor`. Shared `x-airflow-common`: LocalExecutor, Execution API URL, JWT secret, SimpleAuthManager with all users admin, connections and variable from env, `PYTHONPATH=/opt/airflow/include`, mounts (dags, include, warehouse, lake:ro, logs volume), recovery timings. |
| `Makefile` | Adds `dags`, `runs`, `trigger` (the Airflow CLI through `/entrypoint`), `warehouse` and `sql` (harlequin, read-only). `COMPOSE_PROFILES ?= stream,batch`. `reset` also wipes `data/warehouse`. |
| `scripts/peek_warehouse.py` | Rows per raw table, stored positions vs Kafka end offsets, latest batches, a first JSON query. |
| `.env.example` | `COMPOSE_PROFILES=stream,batch`, `AIRFLOW_PORT`, `AIRFLOW_JWT_SECRET`. |

**How to poke at it:**

- `make up`, then `make dags`, `make runs` and `make warehouse`
- http://localhost:8080: DAG graph, task logs, Assets and pool views
- the Console → consumer group `duckdb-loader` → lag
- `make sql`, then `SELECT value->>'$.event_type', count(*) FROM raw.trip_events GROUP BY 1`

**Verified:**

- **Bring-up:** 11 services; `airflow-init` Exited (0) with "Pool duckdb created".
- **DAGs:** both parsed with no import errors, started unpaused, first runs succeeded (stream_landing ~22 s, api_extract ~12 s).
- **First landing:** 32,083 trip events, 75,785 status, 149 dead letters and 928 alerts, plus 40 stations, 600 bikes, 158 weather hours and 1,173 fault-log rows.
- **Invariant, rows = distinct (partition, offset) = sum of stored positions**, held for all 4 tables after every run.
- **Kafka's committed offsets for `duckdb-loader`** equal the positions stored in DuckDB, so the Console's lag is correct.
- **Asset events** `raw_stream` and `raw_api` were recorded, with row counts in `extra` (REST API).
- **Crash tests:** `SIGKILL` of the scheduler while a `land_*` task was running, 3 times. The killed attempt left nothing (its transaction never committed), the retry landed the slice once, and the invariant held each time. Recovery took 5 min 01 s and 5 min 24 s with the defaults, then 87 s after setting `orphaned_tasks_check_interval=60` (see decisions).
- **Resources:** Airflow ~1.1 GB (scheduler 570 MB, API server 270 MB, DAG processor 190 MB, Postgres 45 MB). The whole stack ~3.8 GB.

### Layer 5: dbt, the scorecard and the serving copy (2026-09-16)

**Read first, in this order:**

1. `transform/dbt_project.yml` and `profiles.yml`: layers, schemas, shared thresholds, the connection.
2. `models/sources.yml`: raw plus the Spark lake as an external source.
3. `models/intermediate/int_status_sequence.sql` and `int_detections.sql`: the exact sequential checks, and every detector in one shape.
4. `models/marts/mart_detection_scorecard.sql`.
5. `orchestration/dags/dbt_transform.py`: asset scheduling, then build → report → publish.
6. `orchestration/include/serving/publish.py`: the atomic serving copy.

| Path | Purpose |
|---|---|
| `transform/dbt_project.yml` | One schema per layer (staging and intermediate are views, marts are tables), plus snapshots and reference. Vars shared with other components: status interval, late threshold, teleport speed, max trip hours, judge horizon. |
| `transform/profiles.yml` | dbt-duckdb, path from `DUCKDB_PATH` (the default works from `transform/`), `TimeZone: UTC`. |
| `transform/models/sources.yml` | `raw.*` with freshness on `loaded_at`, and `lake.station_metrics` via `external_location` (read_parquet, from `LAKE_DIR`). |
| `models/staging/` (9 views) | JSON → typed columns. Dedup by `event_id` with `copies` and `source_copies` (source duplicate vs bridge re-send), `lateness_min`. Dead letters re-parsed from `raw`. Alerts deduped by `alert_id`. Fault log keys and variants. `stg_station_metrics` guards against an empty lake. |
| `snapshots/snap_stations.yml` | SCD2 on `updated_at` (timestamp strategy); current version valid to 9999-12-31. |
| `seeds/detection_checks.csv` | Check → targeted fault → how to match (event, trip, episode, stall). |
| `models/intermediate/` | `int_trips` (full outer join, pairing with a latest-event horizon, haversine speed); `int_status_sequence` (a table: LAG, trips between snapshots via a range join, `stale_snapshot`, `reporting_gap`, `trip_event_ids`); `int_detections` (bridge + Spark + 7 dbt checks). |
| `models/marts/` (7 tables) | `dim_stations` (current SCD2 version), `fct_trips` (incremental on `loaded_at`, delete+insert), `mart_station_usage_hourly` (with weather), `mart_data_quality_daily`, `mart_detection_scorecard` (enforced contract), `mart_pipeline_health` (batches, Kafka→DuckDB latency), `mart_stream_vs_batch` (Spark windows vs batch, final on both sides). |
| `tests/` | Generic `within_range`; singular `status_above_capacity` and `trip_speed_implausible` (warn + store_failures → schema `audit`). |
| `macros/` | `generate_schema_name` (plain schema names), `haversine_km`, `lake_file_count` (empty-lake guard), `duckdb__snapshot_get_time` (TIMESTAMPTZ clock). |
| `*.yml` model files | Docs, unique/not_null/accepted_values/relationships tests, unit test `int_trips_pairs_halves_and_spots_orphans`, contract on the scorecard. |
| `orchestration/dags/dbt_transform.py` | `schedule=(raw_stream \| raw_api)`: `source_freshness` → `dbt_build` (all_done) → `quality_report` (all_done; logs results, freshness and the scorecard) → `publish_serving` (only after a successful build; Asset `serving_marts`). All tasks in pool `duckdb`. |
| `orchestration/include/serving/publish.py` | Copies every `marts` table plus a `published` row into `bikeshare_serving.duckdb.tmp`, then `os.replace`. 2 tests: only marts are published; an open reader keeps its snapshot while a new process sees the new file. |
| `docker-compose.yml` | Adds `DBT_PROFILES_DIR`, `DUCKDB_PATH`, `LAKE_DIR`, `DBT_SEND_ANONYMOUS_USAGE_STATS`, `AIRFLOW_VAR_SERVING_PATH` and the `./transform` mount. |
| `Makefile` | `dbt c=…` (host), `dbt-docs` (:8082), `scorecard`, `quality`, `serving`. `sql` now opens the serving copy. |
| `scripts/peek_serving.py` | Reads the serving copy (never locked). |
| `source/…/engine.py` | **Fix:** status snapshots are stamped with their step's end, the instant their values are true. |
| `source/…/faults.py` | **Fix:** back-to-back stalls no longer drop the first stall's backlog. |
| `source/tests/` | +2 tests. The stall test was checked to FAIL on the old code. |

**How to poke at it:**

- `make scorecard`, `make quality` and `make serving`
- `make sql`, then e.g. `FROM marts.mart_stream_vs_batch`
- `make dbt-docs` for the lineage graph
- http://localhost:8080 → DAG `dbt_transform`, and the Assets view (`raw_stream` → `dbt_transform` → `serving_marts`)

**Verified:**

- Developed against a **copy** of the live warehouse (`ATTACH … READ_ONLY` + `COPY FROM DATABASE`), so the running Airflow never saw a lock.
- Full build on ~210k events: 63 pass, 2 expected warns (205 over-capacity, 88 teleports), 0 errors, in 18 s.
- **End to end from `make reset`:**
  - 11 services up;
  - 3 DAGs parsed;
  - `dbt_transform` ran `asset_triggered` after each landing and succeeded (freshness 2 s, build 4–14 s, report, publish);
  - the serving copy was published with 7 marts.
- A manual `api_extract` → asset event → dbt run → republished serving copy → `make scorecard`.
- **Scorecard on the fresh world** (~5 simulated days at 600×):
  - precision ~100% for duplicate, contract, over_capacity, late_event and teleport;
  - recall: impossible_status 100% combined (bridge 37.5% + Spark/dbt 62.5%), schema drift, late, stall, silent and teleport all 100%, orphans 97.6%, duplicates 99.6%;
  - frozen: dbt `stale_snapshot` 100% recall at 42% precision, 71% counting the teleport-explained ones; Spark 0/3 (short episodes).
- Resources: stack ~4.5 GB RAM; disk 6.3 GB free.

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
| 2026-09-15 | The Spark job lives in `streaming/analyzer/` (a package) plus `streaming/stream_analyzer.py`. The plan said `jobs/stream_analyzer.py` + `rules.py`. | A package gives importable, testable modules, with no generic top-level names like `rules` on the path. |
| 2026-09-15 | Four queries: reference_rules, station_metrics, station_health, trip_pairing. | One sink per query. Stateless rules must see late events that the stateful ones drop. Each query shows one pattern. |
| 2026-09-16 | **Frozen/silent use 2-hour "health" windows. Metrics keep 30-minute windows.** The plan said 15 min. | Measured against the ground truth: 30-min windows gave frozen 8% precision (one departure plus one arrival, or trips after the last snapshot, look frozen); 2 h windows gave 86%. Silent became `< half` of the expected reports after a first-window artifact (the world starts at 05:00, mid-window). |
| 2026-09-15 | Spark raises `late_event` alerts itself (`emitted_at − event_time ≥ 25 min`), in the stateless query. | The plan only said "lateness / watermark drops". A direct check catches both late_event faults and stall backlogs, which the stateful queries drop past the watermark. |
| 2026-09-15 | Alerts carry a deterministic `alert_id` (sha2 of type, entity and event or window) and explicit nulls. | The Kafka sink and foreachBatch are at-least-once: consumers dedup by id. One stable schema. |
| 2026-09-15 | Spark runs as the host UID via `entrypoint.sh` (nss_wrapper) under `tini`. | Lake and checkpoint files stay yours. |
| 2026-09-16 | `pyspark==4.2.0` is in the default dependency groups. The plan said opt-in. | Spark is a core component. The rules are unit-tested locally, and the version is pinned to match the image. |
| 2026-09-15 | Spark is in compose profile `stream`. The Makefile defaults `COMPOSE_PROFILES=stream`. `down` and `reset` use `--profile '*'`. | Lets you run the core without Spark when RAM is tight. Stopping always covers every profile. |
| 2026-09-15 | `spark-reset` also deletes the alerts topic. | Resetting a job's progress without its outputs duplicates its results on replay. |
| 2026-09-16 | The landing code is a plain package, `orchestration/include/landing/`, with no Airflow import. DAGs only schedule it. The plan said `include/kafka_to_duckdb.py` + `duckdb_io.py`. | Unit-testable without Airflow (not installed locally). It is the same package the peek script uses. |
| 2026-09-16 | Raw topic tables keep `value` and `headers` as JSON **text** (VARCHAR), not the JSON type. | A malformed message can never block landing. dbt parses the text. |
| 2026-09-16 | stations and bikes are replaced on every extract (current snapshot). weather and fault_log are append-only incremental. | History of reference data belongs in dbt snapshots (layer 5). Incremental loads rely on primary keys + `ON CONFLICT DO NOTHING`, so they are idempotent. |
| 2026-09-16 | Warehouse connections set `TimeZone = 'UTC'`. | DuckDB returned TIMESTAMPTZ in the host's local zone (+01:00 on this machine). A test on the weather cursor caught it. |
| 2026-09-16 | `report` raises `AirflowSkipException` when nothing landed. | A skipped task emits no asset event, so dbt (layer 5) won't run for nothing. |
| 2026-09-16 | Airflow commands run through the image's `/entrypoint`: `airflow-init`'s command is `bash -c …`, and the Makefile uses `exec … /entrypoint airflow`. | As an arbitrary UID, only the entrypoint puts Airflow's user site-packages on PYTHONPATH. Without it: "No module named airflow" (seen). |
| 2026-09-16 | No triggerer service. The official compose has one. | No deferrable operators here. Saves ~200 MB. |
| 2026-09-16 | `orphaned_tasks_check_interval=60` and `task_instance_heartbeat_timeout=90` (defaults 300 / 300). | Measured: after a scheduler crash, the dead task held the only `duckdb` pool slot for 5 min. The first fix tried (heartbeat timeout alone) did not help, because the orphan check is what recovers a dead scheduler's tasks. Recovery is now 87 s. |
| 2026-09-16 | Your `.env` was regenerated from `.env.example`. | It was identical to the layer 3 template, and `COMPOSE_PROFILES=stream` kept Airflow from starting. |
| 2026-09-16 | **Source fix: status snapshots are stamped at their step's END.** The plan did not anticipate this. | The values were read at the end of the 10 s step but stamped at the scheduled time inside it, so a snapshot counted trips from its own future. dbt's exact frozen check had 13.7% precision; the cause was proven by realigning to step ends (1,002 → 238 detections). "Make the source truthful" beats encoding simulator internals in dbt. |
| 2026-09-16 | **Source fix: back-to-back stalls.** | If a stall ended and another started in the same step, the flushed backlog was dropped (data loss). Found while reading the code during the late-event analysis. |
| 2026-09-16 | The scorecard judges only up to `least(latest event − judge_after_hours, latest extracted fault)`, for precision and recall alike. | The fault log is extracted every 15 min (15 simulated hours at 60×), so it lags the events. Without the bound, late_event looked 82% precise; with it, 100%. |
| 2026-09-16 | "Explained" false positives: orphans whose other half was dead-lettered (schema drift), and stale snapshots that counted a teleported arrival. | Reported next to precision, not hidden: one fault showing up as another is itself a finding. |
| 2026-09-16 | A seed (`detection_checks.csv`) drives the scorecard's matching. | Adding a detector means adding rows, not rewriting SQL. |
| 2026-09-16 | dbt reads Spark's Parquet in place as a source (`external_location`), with a glob guard. | Shows DuckDB and Spark sharing a lake; the build survives an empty lake (no `stream` profile, or before the first window closes). |
| 2026-09-16 | `mart_stream_vs_batch` compares only windows final on BOTH sides. | Spark runs ahead of the 5-min landing: comparing Spark's newest windows produced −3,183 "missing" events. |
| 2026-09-16 | Serving copy = marts only, rebuilt and `os.replace`d after each successful build. The plan said the whole DB. | Smaller, never locked, and it holds only what readers should use. |
| 2026-09-16 | `duckdb__snapshot_get_time` overridden to TIMESTAMPTZ. | dbt-duckdb's naive TIMESTAMP clock warned against the TIMESTAMPTZ `updated_at`. |
| 2026-09-16 | dbt pinned to the Airflow image's versions (`dbt-core==1.12.5`, `dbt-duckdb==1.11.0`) in the `warehouse` group. | The same dbt on the host and in Airflow, over the same file. |
| 2026-09-16 | The end-to-end check ran after `make reset`. | The source fix only applies to new data, and it proves the whole pipeline starts from zero. |

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
| pyspark (`spark` group, default) | 4.2.0 (= the image) | `pyproject.toml`, `uv.lock` |
| Spark image | `spark:4.2.0-scala2.13-java21-python3-ubuntu` (Docker Official Image; JDK 21.0.12, Python 3.10.12) | `streaming/Dockerfile` |
| Spark Kafka connector | `spark-sql-kafka-0-10_2.13:4.2.0` → kafka-clients 3.9.2, commons-pool2 2.13.1 | `streaming/Dockerfile` |
| Airflow | `apache/airflow:3.3.1-python3.12` (Python 3.12.13, pyarrow 25.0.0, httpx 0.28.1) | `orchestration/Dockerfile` |
| Airflow extras | confluent-kafka 2.15.1, duckdb 1.5.5; `/opt/dbt-venv`: dbt-core 1.12.5, dbt-duckdb 1.11.0 | `orchestration/Dockerfile` |
| Airflow metadata DB | `postgres:16-alpine` | `docker-compose.yml` |
| duckdb (host) | 1.5.5, now pinned `==` (was `>=1.4`); pyarrow ≥ 25 added to `warehouse` | `pyproject.toml` |
| dbt (host) | dbt-core 1.12.5, dbt-duckdb 1.11.0 (= the Airflow image) | `pyproject.toml` |
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
- **2026-09-16, disk:** 5.5 GB free (93% used) after the Airflow image (3.2 GB base). Docker images total ~7.4 GB, many layers shared; build cache 2.1 GB, ~0.7 GB of it reclaimable (`docker builder prune`).

## Known issues / TODO

- **Editor (Pyright) shows "import could not be resolved"** for every `bikeshare_sim` import. Runtime, tests and ruff are fine. Point the editor's interpreter at `.venv/bin/python`. `[tool.pyright]` in `pyproject.toml` sets `venv = ".venv"` for editors that read it.
- **Rush hours redirect many arrivals** ("destination full"). Internally that's ~20–45% of arrivals in the busiest hours. It doesn't affect data correctness. Tune it with `rebalancing_every_h` if the occupancy analytics look too extreme.
- **Shell messages:** in the French locale, a process killed by `timeout` prints "Complété". That's SIGTERM, not an error.
- **Every `make up` re-runs `kafka-init`.** Compose re-runs a completed one-shot dependency. It is idempotent (`--if-not-exists`) and takes ~10 s.
- **`make tail` joins a throwaway consumer group** (`console-consumer-…`, auto-commit off). It may show up briefly in the Console's group list.
- **The Console's topic list includes `__consumer_offsets`**, Kafka's internal topic. `make topics` hides it.
- **Rebuilding an image recreates its container.** Any change to the root `pyproject.toml` or `uv.lock` rebuilds both Python images, because both copy them. Code-only changes rebuild just the affected image.
- **Spark: frozen_station recall is ~40%.** Episodes that are short, or at quiet stations and hours, cannot fill a 2 h window with evidence. The exact snapshot-to-snapshot check (LAG plus the trips in between) is planned in dbt (layer 5). Comparing the two is a good use of the scorecard.
- **Spark: orphan_trip precision is ~66%** because of secondary effects (partners dead-lettered by the bridge). Layer 5's scorecard should attribute these to schema_drift rather than count them as plain false positives.
- **Spark: `input_rows` for `trip_pairing` is 2× the Kafka rows.** Both sides of the self-join scan the source. That is inherent and harmless at this volume.
- **Spark drops rows behind the watermark, and they are counted.** Over the final replay the progress lines added up to `dropped_late` = 10 (late events and stall backlog more than 30 simulated minutes behind) and `dropped_duplicates` = 914. Only the stateless `reference_rules` query still sees those late rows, which is why `late_event` is detected there.
- **PySpark pitfall:** `collect()` returns naive datetimes in the Python process's local timezone (seen in the tests on this Paris-time host). The container runs in UTC.
- **Spark's Parquet sink records its commits in `_spark_metadata/`.** DuckDB's glob ignores that log, so a crashed batch's uncommitted file could be counted. That is acceptable here.
- **Changing `spark.sql.shuffle.partitions` or any stateful query's shape** requires `make spark-reset`. The checkpoints remember both.
- **Airflow: reading the warehouse from the host can be refused while a DAG writes.** DuckDB has one writer, and a landing task holds the lock for a few seconds. `make warehouse` waits (retry). harlequin (`make sql`) refuses to start: retry a few seconds later. Layer 5's serving copy removes this.
- **The warehouse file is `ai-user:root`** (Airflow runs as your UID with group 0, the official pattern). You own it and can delete it.
- **Airflow CLI calls take a few seconds each** (`make dags`, `runs`, `trigger`): each one starts a Python process through the entrypoint. For tight polling, use the REST API (`curl localhost:8080/api/v2/...`), which needs no login locally.
- **`stream_landing` lands at most 200k messages per partition per run** (`max_per_partition`) and reads for at most 120 s. After a huge fast-forward, it catches up over several runs (the notes in `raw._ingest_batches.detail` say so).
- **dbt on the host (`make dbt`) needs the warehouse write lock.** It fails if a DAG task writes at that moment. Retry, or pause the DAGs while you iterate. For free exploration, copy the warehouse as in layer 5's development (`ATTACH … (READ_ONLY)` + `COPY FROM DATABASE`).
- **At high simulation speed, the fault log lags** (extracted every 15 real minutes = 6 simulated days at 600×), so the scorecard judges an older horizon. `make trigger d=api_extract` refreshes it, and the asset event then rebuilds the marts.
- **`stale_snapshot` still has unexplained false positives** (~29% of detections in the fresh run). Likely causes: dead-lettered or dropped trip events at the station, and silent rebalancing coinciding with trips. Candidate attributions to add.
- **Spark's `frozen_station` found 0 of 3 frozen episodes in the fresh run** (short episodes). dbt's exact check found 3 of 3. That contrast is the point of having both.
- **dbt rebuilds everything except `fct_trips` on each run** (views and tables). About 4–15 s at this volume. Incremental marts would be the next step at scale.
- **Next (layer 6):**
  - README guided tour (a start-to-finish walkthrough, what to open where);
  - wiring map: every service ↔ host:port ↔ env var ↔ file;
  - the "three clocks" explained once;
  - `scripts/smoke.py` (`make smoke`): an end-to-end check from an empty stack (source healthy → topics filling → Spark alerts → landing → dbt → serving copy → scorecard rows);
  - optional Streamlit dashboard on the serving copy.
