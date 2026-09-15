# DE_modern: a streaming data pipeline on a modern stack

A synthetic bike-share operator streams events over SSE. A pipeline built
with Kafka, Spark Structured Streaming, Airflow, DuckDB and dbt consumes that
stream, lands it, transforms it and checks it for faults.

- [`PLAN.md`](PLAN.md): the request, the architecture and the design decisions.
- [`logging_build.md`](logging_build.md): what is built so far and what comes next.

> This README grows with each layer. The full guided tour comes in layer 6.

## Quickstart (layer 1: the source)

```bash
make setup      # .venv (uv), .env with your UID/GID, data/ dirs
make test       # unit tests
make up         # build + start the stack (for now: source-api) → http://localhost:8000/docs
make help       # every target
```

Play with the simulated world:

```bash
make clock                          # simulated time, speed, engine backlog
make stream                         # raw SSE frames (Ctrl-C to stop)
make stream types=trip_started      # only one event type
make speed x=600                    # 10 simulated minutes per real second
make ff h=6                         # fast-forward 6 h; the skipped events arrive as a burst
make faults                         # the fault catalogue, with live counters
make fault f=teleport               # force one fault now
make fault f=silent_station station=ST-007
make fault-log                      # ground truth: what was injected, when, where
make stats                          # emitted events, faults, engine internals
make source-reset                   # wipe the world (fresh start next time)
make down                           # stop the stack (data/ is kept)
```

The source can also run on the host with `make source-run`, using the same
config and the same state file. Don't run it at the same time as the container:
both use port 8000.

## Layer 2: Kafka and the bridge

`make up` now also starts Kafka (a single KRaft node), `kafka-init` (a one-shot
that creates the topics), the bridge (source SSE → Kafka) and Redpanda Console.

```
source-api ──SSE──► bridge ──► bikeshare.trip-events.v1     (key: bike_id)
                      │    ──► bikeshare.station-status.v1  (key: station_id)
                      └──────► bikeshare.dlq.v1             (contract violations, with the reason)
```

```bash
make ps                                   # 5 services; kafka-init shows "Exited (0)"
make topics                               # messages per topic
make tail t=bikeshare.trip-events.v1      # next 5 messages: partition, headers, key, value
make fault f=schema_drift && make dlq     # watch a contract violation land in the DLQ
make logs s=bridge                        # JSON logs; a "throughput" line every 30 s
open http://localhost:8081                # Redpanda Console: browse topics and messages
make reset                                # wipe ALL state (world, checkpoint, topics) together
```

Kafka answers on two addresses. Containers use `kafka:9092`; tools on your
machine use `localhost:9094` (e.g. `make bridge-run`). The kafka service in
`docker-compose.yml` explains why.

If your `.env` predates layer 2, the new variables (`KAFKA_HOST_PORT`,
`CONSOLE_PORT`) fall back to their defaults. Compare it with `.env.example`.

## Layer 3: the Spark analyzer

A Spark 4.2 Structured Streaming job (`streaming/`) runs four queries over
the Kafka topics:

| query | pattern | output |
|---|---|---|
| `reference_rules` | stateless `foreachBatch` + station data refreshed from the source API | `over_capacity`, `teleport`, `late_event` → `bikeshare.alerts.v1` |
| `station_metrics` | watermark + dedup + 30-min windows | Parquet in `data/lake/station_metrics/` (exactly-once file sink) |
| `station_health` | watermark + dedup + 2-hour windows | `frozen_station`, `silent_station` → alerts topic |
| `trip_pairing` | stream-stream full outer join with a time bound | `orphan_trip` → alerts topic |

```bash
make alerts                 # alert counts by type + the latest ones (a host Kafka consumer)
make lake                   # DuckDB reading Spark's Parquet in place, from the host
make logs s=spark           # one JSON "progress" line per micro-batch and query
open http://localhost:4040  # Spark UI → Structured Streaming tab
make spark-reset && make up # replay everything from Kafka (checkpoints, lake, alerts wiped)
COMPOSE_PROFILES= make up   # run the stack without Spark (~1.5 GB less RAM)
```

Measured against the source's ground truth (after injecting extra frozen and silent episodes):

| alert type | precision | recall |
|---|---|---|
| over_capacity | 100% | 100% |
| teleport | 100% | 98% |
| late_event | 100% | 100% |
| orphan_trip | 66% | 100% |
| frozen_station | 86% | 40% |
| silent_station | 100% | 100% |

The orphan "false positives" are not rule bugs. The missing half of those trips
was schema-drifted and dead-lettered by the bridge, so the pipeline really
never saw it: one fault showing up as another.

Frozen recall is limited by episodes too short, or at stations too quiet, to fill
a 2-hour window. The exact check is sequential and comes in layer 5, with dbt.
