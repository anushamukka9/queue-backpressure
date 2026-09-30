"""A bounded work queue that says no instead of dying slowly."""

import asyncio
import time
from collections import deque
from enum import Enum


class ShedPolicy(Enum):
    """What to do when the queue is full and a new job arrives.

    REJECT: refuse the job; the producer gets a signal (HTTP 429).
        This is the default, and what a server that answers back uses.
    DROP_NEWEST: silently discard the incoming job, counted in ``dropped``.
        Use when the producer cannot or will not retry (fire-and-forget
        telemetry, metrics, heartbeats).
    DROP_OLDEST: evict the oldest queued job to make room for the new one,
        counted in ``dropped``. Use when the freshest data matters most
        (live dashboards, latest-position updates); the old job is gone
        for good, so only pick this if losing it is acceptable.
    """

    REJECT = "reject"
    DROP_NEWEST = "drop-newest"
    DROP_OLDEST = "drop-oldest"


class BoundedWorkQueue:
    """An asyncio work queue with a ceiling.

    The whole point of backpressure: when the queue is full, the producer
    gets a refusal (shed load) instead of the process accumulating an
    unbounded backlog. ``try_enqueue`` never blocks; what it does when
    full depends on ``shed_policy``.

    Counters are the cheap observability: ``accepted``, ``shed`` (refused,
    producer was told), and ``dropped`` (silently discarded by a
    drop-oldest / drop-newest policy).
    """

    def __init__(
        self,
        maxsize: int,
        retry_after_seconds: int = 2,
        shed_policy: ShedPolicy = ShedPolicy.REJECT,
    ) -> None:
        if maxsize < 1:
            raise ValueError("maxsize must be at least 1")
        if not isinstance(shed_policy, ShedPolicy):
            raise ValueError("shed_policy must be a ShedPolicy")
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=maxsize)
        self.retry_after_seconds = retry_after_seconds
        self.shed_policy = shed_policy
        self.accepted = 0
        self.shed = 0
        self.dropped = 0

    def try_enqueue(self, item: object) -> bool:
        """Try to enqueue without blocking.

        Returns True if the job is now queued. When the queue is full, the
        behavior depends on the shed policy: REJECT returns False and counts
        a shed; DROP_NEWEST returns False and counts a drop; DROP_OLDEST
        evicts the oldest job, queues the new one, and returns True.
        """
        try:
            self._queue.put_nowait(item)
        except asyncio.QueueFull:
            if self.shed_policy is ShedPolicy.DROP_OLDEST:
                try:
                    self._queue.get_nowait()
                except asyncio.QueueEmpty:  # lost the race; treat as shed
                    self.shed += 1
                    return False
                self._queue.task_done()
                self.dropped += 1
                self._queue.put_nowait(item)
                self.accepted += 1
                return True
            if self.shed_policy is ShedPolicy.DROP_NEWEST:
                self.dropped += 1
                return False
            self.shed += 1
            return False
        self.accepted += 1
        return True

    @property
    def depth(self) -> int:
        return self._queue.qsize()

    @property
    def maxsize(self) -> int:
        return self._queue.maxsize

    @property
    def pressure(self) -> float:
        """Fill ratio from 0.0 (empty) to 1.0 (full). Producers can watch
        this and slow down before the queue starts shedding."""
        return self.depth / self.maxsize

    def snapshot(self) -> dict:
        """A JSON-friendly metrics snapshot for a stats endpoint."""
        return {
            "depth": self.depth,
            "maxsize": self.maxsize,
            "pressure": round(self.pressure, 3),
            "shed_policy": self.shed_policy.value,
            "accepted": self.accepted,
            "shed": self.shed,
            "dropped": self.dropped,
        }

    async def get(self) -> object:
        return await self._queue.get()

    def task_done(self) -> None:
        self._queue.task_done()

    async def join(self) -> None:
        await self._queue.join()


class DeadLetterQueue:
    """A bounded parking lot for jobs that failed permanently.

    When a job exhausts its retries, it goes here instead of vanishing:
    the payload, the reason, how many attempts were made, and when it was
    parked are all kept for inspection. Bounded, so a flood of failures
    cannot grow it forever; the oldest entries are evicted when full, and
    the eviction is counted in ``evicted``.
    """

    def __init__(self, maxsize: int = 1000) -> None:
        if maxsize < 1:
            raise ValueError("maxsize must be at least 1")
        self._entries: deque = deque(maxlen=maxsize)
        self.maxsize = maxsize
        self.parked = 0
        self.evicted = 0

    def park(
        self, job: object, reason: str, attempts: int, error: str | None = None
    ) -> None:
        """Park a permanently-failed job. Evicts the oldest entry if full."""
        if len(self._entries) >= self.maxsize:
            self.evicted += 1
        self._entries.append(
            {
                "job": job,
                "reason": reason,
                "attempts": attempts,
                "error": error,
                "parked_at": time.time(),
            }
        )
        self.parked += 1

    def list(self, limit: int | None = None) -> list:
        """Newest entries first."""
        entries = list(reversed(self._entries))
        return entries if limit is None else entries[:limit]

    def __len__(self) -> int:
        return len(self._entries)

    def snapshot(self) -> dict:
        return {
            "depth": len(self._entries),
            "maxsize": self.maxsize,
            "parked": self.parked,
            "evicted": self.evicted,
        }
