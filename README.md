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
