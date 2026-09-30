"""End-to-end tests for the resilient app.

The lifespan is driven directly (app.router.lifespan_context) so the
worker tasks start and the shutdown path actually runs, deterministically.
"""

import asyncio
import time

from fastapi.testclient import TestClient

from resilient_app import create_app


def small_app(**kwargs):
    defaults = dict(
        queue_maxsize=10,
        max_attempts=3,
        drain_timeout=5.0,
        worker_count=1,
        retry_base=0.01,
        retry_cap=0.05,
    )
    defaults.update(kwargs)
    return create_app(**defaults)


def test_sheds_429_when_full():
    app = small_app()
    client = TestClient(app)  # no lifespan: nobody drains the queue
    for i in range(10):
        assert client.post("/jobs", json={"id": i}).status_code == 200
    r = client.post("/jobs", json={"id": "one-too-many"})
    assert r.status_code == 429
    assert r.headers["retry-after"] == "2"
    assert r.json() == {"error": "queue full"}


def test_health_ok_then_degraded():
    app = small_app()
    client = TestClient(app)
    healthy = client.get("/health").json()
    assert healthy["status"] == "ok"
    assert healthy["slow_down"] is False
    q = app.state.work
    for i in range(9):  # 90% full
        assert q.try_enqueue({"id": i})
    degraded = client.get("/health").json()
    assert degraded["status"] == "degraded"
    assert degraded["slow_down"] is True
    assert degraded["pressure"] == 0.9
    assert degraded["depth"] == 9


def test_returns_503_when_draining():
    app = small_app()
    client = TestClient(app)
    app.state.state["draining"] = True
    r = client.post("/jobs", json={"id": 1})
    assert r.status_code == 503
    assert r.json() == {"error": "server is shutting down"}


async def poll_until(predicate, timeout=5.0):
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        if predicate():
            return True
        await asyncio.sleep(0.05)
    return False


def test_flaky_job_succeeds_after_retries():
    app = small_app()

    async def go():
        async with app.router.lifespan_context(app):
            app.state.work.try_enqueue({"id": "flaky", "fail_times": 2})
            assert await poll_until(lambda: app.state.state["completed"] == 1)
            assert app.state.state["retried"] == 2

    asyncio.run(go())


def test_hopeless_job_lands_in_dlq():
    app = small_app()

    async def go():
        async with app.router.lifespan_context(app):
            app.state.work.try_enqueue({"id": "hopeless", "fail_times": 99})
            dlq = app.state.dlq
            assert await poll_until(lambda: len(dlq) == 1)
            entry = dlq.list()[0]
            assert entry["job"] == {"id": "hopeless", "fail_times": 99}
            assert entry["attempts"] == 3
            assert entry["reason"] == "max attempts exceeded"

    asyncio.run(go())


def test_shutdown_drains_pending_jobs():
    app = small_app()

    async def go():
        async with app.router.lifespan_context(app):
            for i in range(5):
                app.state.work.try_enqueue({"id": i, "fail_times": 0})
            # exit the lifespan without joining: shutdown must drain them

    asyncio.run(go())
    assert app.state.state["completed"] == 5


def test_dlq_endpoint_lists_parked_jobs():
    app = small_app()

    async def go():
        async with app.router.lifespan_context(app):
            app.state.work.try_enqueue({"id": "doomed", "fail_times": 99})
            assert await poll_until(lambda: len(app.state.dlq) == 1)
            client = TestClient(app)
            body = client.get("/dlq").json()
            assert body["dead_letters"][0]["job"]["id"] == "doomed"

    asyncio.run(go())


def test_stats_reports_full_picture():
    app = small_app()
    client = TestClient(app)
    body = client.get("/stats").json()
    assert body["depth"] == 0
    assert body["maxsize"] == 10
    assert body["shed_policy"] == "reject"
    assert body["completed"] == 0
    assert body["retried"] == 0
    assert body["dlq"] == {"depth": 0, "maxsize": 500, "parked": 0, "evicted": 0}
    assert body["draining"] is False
