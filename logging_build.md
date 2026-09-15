# Build log: DE_modern

This file hands work over from one session to the next. Update it at every review stop ⏸.

The plan of record is in [`PLAN.md`](PLAN.md). This file tracks progress against that plan, plus every decision or deviation taken along the way.

## Status

| # | Layer | Status | Date | Commit |
|---|---|---|---|---|
| 0 | Records: git init, `PLAN.md`, `logging_build.md` | ✅ done | 2026-09-15 | first commit |
| 1a | Environment: uv workspace, `.env.example`, Makefile and compose skeletons | ⏳ in progress | 2026-09-15 | |
| 1b | Source: sim clock, world, engine, faults, FastAPI and SSE, tests, Dockerfile | ⬜ todo | | |
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

## Decisions and deviations from the plan

| Date | Decision | Why |
|---|---|---|
| 2026-09-15 | Local environment is a uv workspace `.venv`. `/opt/venvs/pyDS` is not modified. | Reproducible lockfile, and the shared base environment stays clean. |

## Pinned versions

To be filled in as each layer lands (resolved from `uv.lock` or the image tags).

| Component | Version | Where pinned |
|---|---|---|
| Python (local) | 3.12.3 (system) | `.python-version` |

## Host environment (checked 2026-09-15)

- **Tools:** Docker 29.6.2, Compose v5.3.1, uv 0.12.9, Java 21.0.12, GNU Make 4.3, Node 24, git 2.43.
- **Machine:** 20 cores, 15 GiB RAM (~11 GiB available), ~15 GB free disk.
- **Host UID:GID** is 1001:1001, used for container bind mounts.
- Kafka, Spark, Airflow, dbt and duckdb are not installed on the host. They come from Docker images or the uv venv.

## Known issues / TODO

- (none yet)
