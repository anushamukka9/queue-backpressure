"""retry_and_dlq: a flaky job retries with backoff, a hopeless one is parked.

No server needed. Two jobs against run_with_retries and DeadLetterQueue:
one fails twice then succeeds, the other never succeeds and ends up in
the dead-letter queue. Run: python examples/retry_and_dlq.py
"""

import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from backpressure import DeadLetterQueue, run_with_retries  # noqa: E402


class Flaky:
    def __init__(self, failures_before_success: int):
        self.left = failures_before_success

    async def __call__(self):
        if self.left > 0:
            self.left -= 1
            raise RuntimeError("simulated failure")
        return "done"


async def main() -> None:
    t0 = time.monotonic()

    # A job that stumbles twice, then succeeds on attempt 3.
    result = await run_with_retries(
        Flaky(2),
        max_attempts=4,
        base=0.1,
        jitter=False,
        on_retry=lambda attempt, e: print(
            f"  attempt {attempt + 1} failed ({e}); backing off..."
        ),
    )
    print(f"flaky job finished: {result} after {time.monotonic() - t0:.2f}s")

    # A job that never succeeds: after 3 attempts it goes to the DLQ.
    dlq = DeadLetterQueue(maxsize=10)
    job = {"id": "hopeless", "payload": "..."}
    try:
        await run_with_retries(
            Flaky(99), max_attempts=3, base=0.05, jitter=False
        )
    except RuntimeError as e:
        dlq.park(job, reason="max attempts exceeded", attempts=3, error=str(e))
    print(f"hopeless job parked: dlq depth={len(dlq)}")
    for entry in dlq.list():
        print(f"  {entry['job']['id']}: {entry['reason']} ({entry['attempts']} attempts)")


if __name__ == "__main__":
    asyncio.run(main())
