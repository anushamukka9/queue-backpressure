# queue-backpressure

A worker queue with backpressure: a bounded queue that sheds load with
`429 + Retry-After` instead of growing until the process dies, plus an
AIMD adaptive concurrency gate and monotonic fencing tokens.

This is the companion code for the tutorial
**[Give Your Worker Queue Backpressure Before It Falls Over](https://anushamukka.com/posts/give-your-worker-queue-backpressure/)**.
The tutorial walks through every piece here step by step: watch the naive
version fall over, put a ceiling on the queue, shed load politely, and let
the concurrency limit tune itself.

## What is in here

- `src/backpressure/` — the reusable primitives:
  - `queue.py`: `BoundedWorkQueue`, a bounded asyncio queue. `try_enqueue`
    returns `False` instead of blocking when full, and counts accepted vs.
    shed jobs.
  - `gate.py`: `AdaptiveGate`, an AIMD concurrency limiter. Additive
    increase on fast responses, multiplicative decrease when latency crosses
    your target.
  - `fence.py`: `FencingTokenStore`, monotonic fencing tokens. Authority
    comes from the store's own generation counter, never from a clock.
- `src/naive_app.py` — the worker with no backpressure (the "before").
- `src/bounded_app.py` — the same worker with a bounded queue, `429` and
  `Retry-After` on shed load (the "after").
- `src/flood.py` — a rude client: hammers the server, ignores 429s.
- `src/polite_flood.py` — a polite client: honors `Retry-After` and retries.
- `src/adaptive_demo.py` — the AIMD gate against a simulated contended
  downstream. No server needed.
- `tests/` — pytest suite covering the queue, the gate, the fence, and
  both apps end to end.

## Prerequisites

- Python 3.10+
- pip

## Run it

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

**Step 1 — watch the naive version fall over:**

```bash
python -m uvicorn naive_app:app --app-dir src --port 8000 &
python src/flood.py --n 1000
```

The queue depth climbs toward 1,000 and takes ~50 seconds to drain.

**Step 2 — the bounded version sheds instead of dying:**

```bash
python -m uvicorn bounded_app:app --app-dir src --port 8000 &
python src/polite_flood.py --n 300
curl -s localhost:8000/stats   # depth stays at or under 100
```

**Step 3 — the adaptive gate (no server needed):**

```bash
python src/adaptive_demo.py
```

The un-limited run shows multi-second median latency; the AIMD run
converges to a single-digit concurrency with sub-second latency on
identical total work.

**Run the tests:**

```bash
pytest tests/ -q
```

## A note on 429 vs 503

This project sheds load with `429 Too Many Requests` + `Retry-After`,
following the tutorial. `503 Service Unavailable` is the other defensible
choice; the mechanism that matters is the same: refuse fast, tell the
client when to come back, and never let the backlog grow unbounded.

## License

MIT. See [LICENSE](LICENSE).
