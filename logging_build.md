# Build log: DE_modern

This file hands work over from one session to the next. Update it at every review stop ⏸.

The plan of record is in [`PLAN.md`](PLAN.md). This file tracks progress against that plan, plus every decision or deviation taken along the way.

## Status

| # | Layer | Status | Date | Commit |
|---|---|---|---|---|
| 0 | Records: git init, `PLAN.md`, `logging_build.md` | ✅ done | 2026-09-15 | `4cae360` |
| 1a | Environment: uv workspace, `.env.example`, Makefile skeleton | ✅ done | 2026-09-15 | "Layer 1a" commit |
| 1b | Source: sim clock, world, engine, faults, FastAPI and SSE, tests, Dockerfile, compose | ⏳ in progress | 2026-09-15 | |
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

## Decisions and deviations from the plan

| Date | Decision | Why |
|---|---|---|
| 2026-09-15 | Local environment is a uv workspace `.venv`. `/opt/venvs/pyDS` is not modified. | Reproducible lockfile, and the shared base environment stays clean. |
| 2026-09-15 | The bridge package is `ingest/bikeshare_bridge/`. The plan said `ingest/bridge/`. | The module name matches the distribution name `bikeshare-bridge`, with no build-backend renaming. |
| 2026-09-15 | `unexport VIRTUAL_ENV` in the Makefile. | Your shell activates `/opt/venvs/pyDS` globally. uv would warn about it, and other tools could pick up the wrong interpreter. |

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

## Host environment (checked 2026-09-15)

- **Tools:** Docker 29.6.2, Compose v5.3.1, uv 0.12.9, Java 21.0.12, GNU Make 4.3, Node 24, git 2.43.
- **Machine:** 20 cores, 15 GiB RAM (~11 GiB available), ~15 GB free disk.
- **Host UID:GID** is 1001:1001, used for container bind mounts.
- Kafka, Spark, Airflow, dbt and duckdb are not installed on the host. They come from Docker images or the uv venv.

## Known issues / TODO

- (none yet)
