"""An AIMD adaptive concurrency gate.

Additive increase on fast responses, multiplicative decrease when latency
crosses the target. The gate adapts to what the downstream can actually
handle right now, instead of trusting a fixed concurrency number.
"""

import asyncio


class AdaptiveGate:
    """Limit in-flight work against a contended downstream.

    ``target`` is the latency (seconds) you consider healthy. Responses
    faster than the target widen the gate by one slot; responses slower
    than the target halve it. Size ``target`` against your real p99, not a
    guess: the gate learns whatever lesson the latency teaches it.
    """

    def __init__(self, start: int = 2, max_slots: int = 64, target: float = 0.6) -> None:
        if start < 1:
            raise ValueError("start must be at least 1")
        self.slots = start
        self.max_slots = max_slots
        self.target = target
        self.in_flight = 0
        self._cond = asyncio.Condition()

    async def acquire(self) -> None:
        async with self._cond:
            while self.in_flight >= self.slots:
                await self._cond.wait()
            self.in_flight += 1

    async def release(self, elapsed: float) -> None:
        async with self._cond:
            self.in_flight -= 1
            if elapsed > self.target:
                self.slots = max(1, self.slots // 2)  # multiplicative decrease
            else:
                self.slots = min(self.max_slots, self.slots + 1)  # additive increase
            self._cond.notify_all()
