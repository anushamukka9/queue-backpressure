"""A bounded work queue that says no instead of dying slowly."""

import asyncio


class BoundedWorkQueue:
    """An asyncio work queue with a ceiling.

    The whole point of backpressure: when the queue is full, the producer
    gets a refusal (shed load) instead of the process accumulating an
    unbounded backlog. ``try_enqueue`` never blocks; it returns False when
    there is no room, and the caller decides what the refusal means
    (HTTP 429 + Retry-After, drop, dead-letter, ...).
    """

    def __init__(self, maxsize: int, retry_after_seconds: int = 2) -> None:
        if maxsize < 1:
            raise ValueError("maxsize must be at least 1")
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=maxsize)
        self.retry_after_seconds = retry_after_seconds
        self.accepted = 0
        self.shed = 0

    def try_enqueue(self, item: object) -> bool:
        """Try to enqueue without blocking. True if accepted, False if shed."""
        try:
            self._queue.put_nowait(item)
        except asyncio.QueueFull:
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

    async def get(self) -> object:
        return await self._queue.get()

    def task_done(self) -> None:
        self._queue.task_done()

    async def join(self) -> None:
        await self._queue.join()
