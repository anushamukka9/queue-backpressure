"""resilient_app: a worker that retries, parks the hopeless, and drains politely.

On top of the bounded queue from ``bounded_app``:

- Jobs that fail are retried with exponential backoff (see ``backoff.py``).
  A job payload carries ``fail_times``: the simulated work fails that many
  times before succeeding, so the retry path is deterministic and easy to
  demo.
- Jobs that exhaust their attempts are parked in a dead-letter queue,
  inspectable at ``/dlq``.
- On shutdown the app stops accepting work (``/jobs`` returns 503), waits
  for in-flight jobs to finish (bounded by ``drain_timeout``), then exits.
- ``/health`` reports queue pressure and tells producers to slow down
  before shedding starts.
- ``/stats`` reports queue depth, per-policy counters, retries, and DLQ
  depth.

Run: python -m uvicorn resilient_app:app --app-dir src --port 8000
"""

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from backpressure import (
    BoundedWorkQueue,
    DeadLetterQueue,
    backoff_delay,
)

log = logging.getLogger("resilient_app")
if not log.handlers:
    # uvicorn only configures its own loggers; without this, our info
    # logs vanish when the app runs under uvicorn.
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    log.addHandler(_handler)
    log.setLevel(logging.INFO)

SLOWDOWN_PRESSURE = 0.8  # above this fill ratio, /health says slow_down


class FlakyError(Exception):
    """The simulated work failed. In real life this is your DB timeout."""


def create_app(
    *,
    queue_maxsize: int = 100,
    max_attempts: int = 4,
    drain_timeout: float = 10.0,
    worker_count: int = 2,
    retry_base: float = 0.2,
    retry_cap: float = 5.0,
) -> FastAPI:
    work = BoundedWorkQueue(maxsize=queue_maxsize, retry_after_seconds=2)
    dlq = DeadLetterQueue(maxsize=500)
    # Mutable state lives in one dict so endpoints, the lifespan, and tests
    # all read the same values.
    state = {"completed": 0, "retried": 0, "draining": False}

    async def do_work(job: dict) -> None:
        """Simulated work: fails ``fail_times`` times, then succeeds."""
        fails_left = job.setdefault("_fails_left", job.get("fail_times", 0))
        if fails_left > 0:
            job["_fails_left"] = fails_left - 1
            raise FlakyError(f"simulated failure for job {job.get('id')!r}")
        await asyncio.sleep(0.02)  # stand-in for real work

    async def handle(job: dict) -> None:
        attempts = 0
        while True:
            attempts += 1
            try:
                await do_work(job)
                state["completed"] += 1
                return
            except FlakyError as e:
                if attempts >= max_attempts:
                    clean = {k: v for k, v in job.items() if not k.startswith("_")}
                    dlq.park(
                        clean,
                        reason="max attempts exceeded",
                        attempts=attempts,
                        error=str(e),
                    )
                    log.warning("job %r parked in DLQ: %s", job.get("id"), e)
                    return
                state["retried"] += 1
                delay = backoff_delay(
                    attempts - 1, base=retry_base, cap=retry_cap, jitter=False
                )
                log.info(
                    "job %r failed (attempt %d/%d), retrying in %.2fs",
                    job.get("id"),
                    attempts,
                    max_attempts,
                    delay,
                )
                await asyncio.sleep(delay)

    async def worker() -> None:
        while True:
            job = await work.get()
            try:
                await handle(job)
            finally:
                work.task_done()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        tasks = [asyncio.create_task(worker()) for _ in range(worker_count)]
        log.info("started %d workers, queue maxsize=%d", worker_count, queue_maxsize)
        yield
        # Shutdown: stop taking work, drain what is left, then leave.
        state["draining"] = True
        log.info("shutting down: draining up to %ds", drain_timeout)
        try:
            await asyncio.wait_for(work.join(), timeout=drain_timeout)
            log.info("drained cleanly")
        except asyncio.TimeoutError:
            left = work.depth
            log.warning("drain timed out with %d jobs still queued", left)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        log.info(
            "shutdown complete: completed=%d retried=%d dlq_depth=%d",
            state["completed"],
            state["retried"],
            len(dlq),
        )

    app = FastAPI(title="resilient worker (retries + DLQ + drain)", lifespan=lifespan)

    @app.post("/jobs")
    async def enqueue(payload: dict):
        if state["draining"]:
            return JSONResponse(
                status_code=503, content={"error": "server is shutting down"}
            )
        if not work.try_enqueue(payload):
            return JSONResponse(
                status_code=429,
                content={"error": "queue full"},
                headers={"Retry-After": str(work.retry_after_seconds)},
            )
        return {"accepted": True, "depth": work.depth}

    @app.get("/stats")
    async def stats():
        snap = work.snapshot()
        snap.update(
            {
                "completed": state["completed"],
                "retried": state["retried"],
                "dlq": dlq.snapshot(),
                "draining": state["draining"],
            }
        )
        return snap

    @app.get("/health")
    async def health():
        pressure = work.pressure
        slow = pressure >= SLOWDOWN_PRESSURE
        return {
            "status": "degraded" if slow else "ok",
            "slow_down": slow,
            "pressure": round(pressure, 3),
            "depth": work.depth,
            "maxsize": work.maxsize,
            "retry_after_seconds": work.retry_after_seconds,
        }

    @app.get("/dlq")
    async def dead_letters(limit: int = 20):
        return {"dead_letters": dlq.list(limit=limit)}

    app.state.work = work
    app.state.dlq = dlq
    app.state.state = state
    return app


app = create_app()
