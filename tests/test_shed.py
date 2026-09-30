"""Shed policies: what happens when a burst hits a full queue."""

import pytest

from backpressure import BoundedWorkQueue, ShedPolicy


def test_default_policy_is_reject():
    q = BoundedWorkQueue(maxsize=1)
    assert q.shed_policy is ShedPolicy.REJECT


def test_reject_counts_shed_and_keeps_queue_contents():
    q = BoundedWorkQueue(maxsize=2, shed_policy=ShedPolicy.REJECT)
    assert q.try_enqueue("a")
    assert q.try_enqueue("b")
    assert q.try_enqueue("c") is False
    assert q.shed == 1
    assert q.dropped == 0
    assert q.depth == 2


def test_drop_oldest_evicts_oldest_and_accepts_new():
    async def go():
        q = BoundedWorkQueue(maxsize=2, shed_policy=ShedPolicy.DROP_OLDEST)
        assert q.try_enqueue("a")
        assert q.try_enqueue("b")
        assert q.try_enqueue("c") is True  # "a" evicted to make room
        assert q.dropped == 1
        assert q.shed == 0
        assert q.accepted == 3
        assert await q.get() == "b"
        assert await q.get() == "c"

    import asyncio

    asyncio.run(go())


def test_drop_newest_silently_discards_incoming():
    q = BoundedWorkQueue(maxsize=1, shed_policy=ShedPolicy.DROP_NEWEST)
    assert q.try_enqueue("a")
    assert q.try_enqueue("b") is False  # no refusal signal, just gone
    assert q.dropped == 1
    assert q.shed == 0
    assert q.depth == 1


def test_invalid_policy_rejected():
    with pytest.raises(ValueError):
        BoundedWorkQueue(maxsize=2, shed_policy="nope")


def test_pressure_fill_ratio():
    q = BoundedWorkQueue(maxsize=4)
    assert q.pressure == 0.0
    q.try_enqueue("a")
    q.try_enqueue("b")
    assert q.pressure == 0.5
    q.try_enqueue("c")
    q.try_enqueue("d")
    assert q.pressure == 1.0


def test_snapshot_has_metrics():
    q = BoundedWorkQueue(maxsize=2, shed_policy=ShedPolicy.DROP_OLDEST)
    q.try_enqueue("a")
    q.try_enqueue("b")
    q.try_enqueue("c")
    snap = q.snapshot()
    assert snap == {
        "depth": 2,
        "maxsize": 2,
        "pressure": 1.0,
        "shed_policy": "drop-oldest",
        "accepted": 3,
        "shed": 0,
        "dropped": 1,
    }
