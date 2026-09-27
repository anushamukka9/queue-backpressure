import asyncio

import pytest

from backpressure import BoundedWorkQueue


def test_accepts_up_to_maxsize():
    q = BoundedWorkQueue(maxsize=3)
    assert [q.try_enqueue(i) for i in range(3)] == [True, True, True]
    assert q.depth == 3
    assert q.accepted == 3


def test_refuses_when_full_and_counts_shed():
    q = BoundedWorkQueue(maxsize=2)
    assert q.try_enqueue("a")
    assert q.try_enqueue("b")
    assert not q.try_enqueue("c")
    assert q.shed == 1
    assert q.accepted == 2
    assert q.depth == 2


def test_drain_frees_capacity():
    async def go():
        q = BoundedWorkQueue(maxsize=1)
        assert q.try_enqueue("a")
        assert not q.try_enqueue("b")
        assert await q.get() == "a"
        q.task_done()
        assert q.try_enqueue("b")

    asyncio.run(go())


def test_invalid_maxsize_rejected():
    with pytest.raises(ValueError):
        BoundedWorkQueue(maxsize=0)


def test_retry_after_configurable():
    assert BoundedWorkQueue(maxsize=5).retry_after_seconds == 2
    assert BoundedWorkQueue(maxsize=5, retry_after_seconds=10).retry_after_seconds == 10
