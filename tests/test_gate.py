import asyncio

import pytest

from backpressure import AdaptiveGate


def run(coro):
    return asyncio.run(coro)


def test_additive_increase_on_fast_responses():
    async def go():
        g = AdaptiveGate(start=2, max_slots=64, target=0.6)
        await g.acquire()
        await g.release(0.1)
        assert g.slots == 3
        await g.acquire()
        await g.release(0.1)
        assert g.slots == 4

    run(go())


def test_multiplicative_decrease_on_slow_responses():
    async def go():
        g = AdaptiveGate(start=8, max_slots=64, target=0.6)
        await g.acquire()
        await g.release(2.0)
        assert g.slots == 4
        await g.acquire()
        await g.release(2.0)
        assert g.slots == 2

    run(go())


def test_slots_stay_within_bounds():
    async def go():
        g = AdaptiveGate(start=1, max_slots=4, target=0.6)
        await g.acquire()
        await g.release(99.0)  # way over target: halve, but floor at 1
        assert g.slots == 1
        for _ in range(10):  # fast: grow, but cap at max_slots
            await g.acquire()
            await g.release(0.01)
        assert g.slots == 4

    run(go())


def test_acquire_blocks_when_at_limit():
    async def go():
        g = AdaptiveGate(start=1, max_slots=4, target=10.0)
        await g.acquire()  # occupies the single slot
        acquired = []

        async def waiter():
            await g.acquire()
            acquired.append(True)
            await g.release(0.01)

        task = asyncio.create_task(waiter())
        await asyncio.sleep(0.05)
        assert acquired == []  # still blocked: no slot free
        await g.release(0.01)  # free the slot; waiter must proceed
        await asyncio.wait_for(task, timeout=5)
        assert acquired == [True]

    run(go())


def test_invalid_start_rejected():
    with pytest.raises(ValueError):
        AdaptiveGate(start=0)
