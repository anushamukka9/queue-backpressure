"""Exponential backoff for retries, with optional jitter.

Backoff is the polite half of a retry loop: after a failure, wait a little
before trying again, and wait longer each time, so a struggling downstream
gets room to recover instead of a retry storm. Jitter spreads synchronized
retries out so a fleet of clients does not hammer in lockstep.
"""

import asyncio
import random
from collections.abc import Awaitable, Callable


def backoff_delay(
    attempt: int, base: float = 0.5, cap: float = 30.0, jitter: bool = True
) -> float:
    """Seconds to wait before the next retry.

    ``attempt`` is 0-based: 0 is the wait before the first retry. The wait
    doubles each attempt (``base * 2**attempt``), never exceeds ``cap``, and
    is reduced to a uniform random value in [0, delay] when ``jitter`` is
    on (full jitter, the AWS-architecture-blog flavor).
    """
    if attempt < 0:
        raise ValueError("attempt must be >= 0")
    if base <= 0:
        raise ValueError("base must be > 0")
    delay = min(cap, base * (2**attempt))
    if jitter:
        delay = random.uniform(0, delay)
    return delay


async def sleep_backoff(attempt: int, **kwargs) -> None:
    """Sleep for ``backoff_delay(attempt, **kwargs)`` seconds."""
    await asyncio.sleep(backoff_delay(attempt, **kwargs))


async def run_with_retries(
    make_coro: Callable[[], Awaitable],
    max_attempts: int = 3,
    base: float = 0.5,
    cap: float = 30.0,
    jitter: bool = True,
    on_retry: Callable[[int, BaseException], None] | None = None,
):
    """Run ``make_coro()`` until it succeeds or attempts run out.

    ``make_coro`` is a zero-arg callable returning a fresh coroutine each
    time, because a coroutine object can only be awaited once. Returns the
    successful result. Raises the last exception after ``max_attempts``
    failures. ``on_retry(attempt, error)`` is called before each sleep
    (attempt is 0-based), for logging or metrics.
    """
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")
    last_error: BaseException | None = None
    for attempt in range(max_attempts):
        try:
            return await make_coro()
        except Exception as e:  # noqa: BLE001 - retry policy is exception-agnostic
            last_error = e
            if attempt < max_attempts - 1:
                if on_retry is not None:
                    on_retry(attempt, e)
                await sleep_backoff(attempt, base=base, cap=cap, jitter=jitter)
    assert last_error is not None
    raise last_error
