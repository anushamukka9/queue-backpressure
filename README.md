# queue-backpressure

A worker queue with backpressure: a bounded queue that sheds load with
`429 + Retry-After` instead of growing until the process dies, retries with
exponential backoff, a dead-letter queue for jobs that never succeed, an
AIMD adaptive concurrency gate, monotonic fencing tokens, and graceful
shutdown with a drain timeout.

This is the companion code for the tutorial
**[Give Your Worker Queue Backpressure Before It Falls Over](https://anushamukka.com/posts/give-your-worker-queue-backpressure/)**.
The tutorial walks through the core pieces step by step: watch the naive
version fall over, put a ceiling on the queue, shed load politely, and let
the concurrency limit tune itself.

## What is in here

`src/backpressure/` - the reusable primitives:

- `queue.py`:
  - `BoundedWorkQueue` - a bounded asyncio queue. `try_enqueue` never
    blocks; when full, the shed policy decides what happens.
  - `ShedPolicy` - `REJECT` (refuse, the producer gets a signal),
    `DROP_NEWEST` (silently discard the incoming job), `DROP_OLDEST`
    (evict the oldest queued job to make room for the new one).
  - `DeadLetterQueue` - a bounded parking lot for permanently failed jobs,
    with reason, attempt count, and timestamp per entry.
- `backoff.py` - `backoff_delay` (exponential backoff with optional full
  jitter) and `run_with_retries` (retry an async callable up to N attempts,
  then raise the last error).
- `gate.py` - `AdaptiveGate`, an AIMD concurrency limiter. Additive
  increase on fast responses, multiplicative decrease when latency crosses
  your target.
- `fence.py` - `FencingTokenStore`, monotonic fencing tokens. Authority
  comes from the store's own generation counter, never from a clock.

The demo apps:

- `src/naive_app.py` - the worker with no backpressure (the "before").
- `src/bounded_app.py` - the same worker with a bounded queue, `429` and
  `Retry-After` on shed load (the "after").
- `src/resilient_app.py` - the full version: retries with backoff, a
  dead-letter queue, graceful shutdown with a drain timeout, a `/health`
  endpoint that signals producers to slow down, and `/stats` with queue
  depth, per-policy counters, retry counts, and DLQ depth. Built with
  `create_app(...)` so tests can drive it directly.
- `src/flood.py` - a rude client: hammers the server, ignores 429s.
- `src/polite_flood.py` - a polite client: honors `Retry-After` and retries.
- `src/adaptive_demo.py` - the AIMD gate against a simulated contended
  downstream. No server needed.

`examples/` - runnable scripts:

- `examples/shed_policies.py` - what each shed policy does to a burst of
  jobs. No server needed.
- `examples/retry_and_dlq.py` - a flaky job retries with backoff; a
  hopeless one lands in the dead-letter queue. No server needed.
- `examples/adaptive_producer.py` - a producer that polls `/health` and
  slows its send rate when pressure climbs, instead of hammering into
  429s. Needs a server running.
- `examples/graceful_drain.py` - starts `resilient_app`, loads it with
  healthy, flaky, and hopeless jobs, sends SIGTERM, and shows the drain.
  Fully automated.

`tests/` - pytest suite covering the queue, shed policies, backoff, the
DLQ, the gate, the fence, and the apps end to end.

## Prerequisites

- Python 3.10+
- pip

## Run it

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

**Step 1 - watch the naive version fall over:**

```bash
python -m uvicorn naive_app:app --app-dir src --port 8000 &
python src/flood.py --n 1000
```

The queue depth climbs toward 1,000 and takes ~50 seconds to drain.

**Step 2 - the bounded version sheds instead of dying:**

```bash
python -m uvicorn bounded_app:app --app-dir src --port 8000 &
python src/polite_flood.py --n 300
curl -s localhost:8000/stats   # depth stays at or under 100
```

**Step 3 - the resilient version retries, parks, and drains:**

```bash
python -m uvicorn resilient_app:app --app-dir src --port 8000 &
# a job that fails twice then succeeds:
curl -s -X POST localhost:8000/jobs -H 'Content-Type: application/json' \
  -d '{"id": "flaky", "fail_times": 2}'
# a job that never succeeds - watch it land in /dlq:
curl -s -X POST localhost:8000/jobs -H 'Content-Type: application/json' \
  -d '{"id": "hopeless", "fail_times": 99}'
curl -s localhost:8000/stats
curl -s localhost:8000/health   # slow_down flips on above 80% full
curl -s localhost:8000/dlq
```

**Step 4 - the adaptive gate (no server needed):**

```bash
python src/adaptive_demo.py
```

The un-limited run shows multi-second median latency; the AIMD run
converges to a single-digit concurrency with sub-second latency on
identical total work.

**Step 5 - no-server examples:**

```bash
python examples/shed_policies.py
python examples/retry_and_dlq.py
```

**Run the tests:**

```bash
pytest tests/ -q
```

## API reference

### BoundedWorkQueue

```python
from backpressure import BoundedWorkQueue, ShedPolicy

q = BoundedWorkQueue(maxsize=100, retry_after_seconds=2,
                     shed_policy=ShedPolicy.REJECT)
q.try_enqueue(job)   # True if queued; False if shed/dropped when full
q.depth              # current queue depth
q.maxsize            # the ceiling
q.pressure           # fill ratio 0.0 - 1.0; producers can watch this
q.snapshot()         # dict of depth, pressure, accepted/shed/dropped
await q.get()        # worker side
q.task_done()
await q.join()       # blocks until every queued job is task_done()
```

Counters: `accepted` (queued), `shed` (refused, producer was told),
`dropped` (silently discarded by a drop policy). `retry_after_seconds`
is what your endpoint should put in the `Retry-After` header.

Picking a shed policy:

- `REJECT` - the default. The producer gets a refusal (HTTP 429) and
  decides whether to retry later. Use when clients can cooperate.
- `DROP_NEWEST` - silently discard the incoming job. Use for
  fire-and-forget data the producer will not retry anyway (metrics,
  heartbeats, telemetry).
- `DROP_OLDEST` - evict the oldest queued job to make room. Use when the
  freshest data matters most (live dashboards, latest-position updates)
  and losing an old job is acceptable.

### DeadLetterQueue

```python
from backpressure import DeadLetterQueue

dlq = DeadLetterQueue(maxsize=1000)
dlq.park(job, reason="max attempts exceeded", attempts=3, error=str(e))
dlq.list(limit=20)   # newest first
len(dlq)             # current depth
dlq.snapshot()       # depth, maxsize, parked, evicted
```

Bounded, so a flood of failures cannot grow it forever; the oldest
entries are evicted when full and the eviction is counted.

### Backoff

```python
from backpressure import backoff_delay, run_with_retries

backoff_delay(0, base=0.5, cap=30.0)          # 0.5
backoff_delay(1, base=0.5, cap=30.0)          # 1.0
backoff_delay(10, base=0.5, cap=30.0)         # 30.0 (capped)
backoff_delay(2, base=0.5, jitter=True)       # uniform in [0, 2.0]

await run_with_retries(make_coro, max_attempts=3, base=0.5,
                       on_retry=lambda attempt, e: log.warning(...))
```

`make_coro` is a zero-arg callable returning a fresh coroutine each call
(a coroutine object can only be awaited once). Raises the last error
after `max_attempts` failures; that is your cue to park the job in the
DLQ. `attempt` is 0-based: the wait before the first retry doubles from
`base` each time and never exceeds `cap`.

### AdaptiveGate

```python
from backpressure import AdaptiveGate

gate = AdaptiveGate(start=2, max_slots=64, target=0.6)
await gate.acquire()
try:
    ...  # call the downstream
finally:
    await gate.release(elapsed)
```

`target` is the latency (seconds) you consider healthy. Responses faster
than the target widen the gate by one slot; slower responses halve it.
Size `target` against your real p99, not a guess.

### FencingTokenStore

```python
from backpressure import FencingTokenStore, StaleWriterError

store = FencingTokenStore()
store.write("order-42", token=7, value={...})   # raises StaleWriterError if token <= fence
store.read("order-42")
store.fence("order-42")                          # current generation, or None
```

Tokens need not be timestamps. A monotonically increasing number from
ZooKeeper, etcd, or a database sequence works, and unlike a timestamp it
cannot misorder. Retried writes carry the same token and are correctly
recognized as duplicates, not new writes.

## The endpoints (bounded_app / resilient_app)

| Endpoint | Method | Behavior |
|---|---|---|
| `/jobs` | POST | Queue the job. `200` with depth when accepted. `429` + `Retry-After` header when the queue is full. `503` while the server is draining on shutdown (resilient_app only). |
| `/stats` | GET | Queue snapshot: depth, maxsize, pressure, shed policy, accepted/shed/dropped; plus completed, retried, DLQ snapshot, draining flag (resilient_app). |
| `/health` | GET | `status: ok/degraded`, `slow_down: true` above 80% pressure, pressure, depth, maxsize, retry_after_seconds (resilient_app). Producers should check this before pushing hard. |
| `/dlq` | GET | Newest dead letters first, `?limit=` (resilient_app). |

## A note on 429 vs 503

This project sheds load with `429 Too Many Requests` + `Retry-After`.
`503 Service Unavailable` is the other defensible choice; the mechanism
that matters is the same: refuse fast, tell the client when to come
back, and never let the backlog grow unbounded. `503` is used here only
for "I am shutting down, stop sending", which is what it means.

## Production notes

These are demo apps, but the patterns transfer directly:

- **Bound your queues everywhere.** An unbounded queue is a memory leak
  with extra steps. `maxsize` should come from load testing, not vibes:
  enough to absorb a burst, small enough that the oldest job in it is
  still worth doing by the time a worker reaches it.
- **Refuse fast and say when to come back.** `429 + Retry-After` turns a
  flood into delayed success; a silent block or a timeout turns it into
  a retry storm. The polite client in this repo is the reference pattern.
- **Signal pressure before shedding.** `/health` with a `slow_down` flag
  (or a pressure gauge in your metrics) lets producers ease off before
  the server starts refusing. Shedding is the last resort, not the
  first signal.
- **Retry with backoff and jitter, and cap the attempts.** Unbounded
  retries on a failing downstream are a self-inflicted DDoS. Full jitter
  keeps a fleet of clients from retrying in lockstep.
- **Dead-letter, don't drop, permanent failures.** A job that exhausts
  its retries is evidence: keep it with the reason and the attempt
  count, alert on DLQ growth, and replay from the DLQ when the
  downstream recovers.
- **Drain on shutdown.** Stop accepting, finish what you have (bounded by
  a timeout), then exit. Anything still queued after the timeout needs a
  decision: requeue it, DLQ it, or accept the loss. This repo's drain
  logs the remainder so the loss is at least visible.
- **AIMD gates protect shared downstreams.** A fixed concurrency limit
  is a guess about capacity that goes stale; the gate re-learns it from
  latency on every response.
- **Fence writes by generation, not by clock.** When two writers race,
  the one with the newer token wins regardless of what any clock says.

## License

MIT. See [LICENSE](LICENSE).
