"""adaptive_demo: an AIMD concurrency limit against a contended downstream.

No server needed. The "downstream" is a simulated database with 4
connections; past 4 concurrent queries, you wait in line and latency climbs.
Watch the limiter converge to a sane concurrency while the un-limited run
melts into multi-second latencies on identical total work.

Usage: python adaptive_demo.py [--total N] [--seed S]
"""

import argparse
import asyncio
import random
import time

from backpressure import AdaptiveGate


class Downstream:
    """Pretend this is a database with 4 connections. Past 4 concurrent
    queries, you wait in line and latency climbs."""

    def __init__(self, connections: int = 4):
        self.pool = asyncio.Semaphore(connections)

    async def query(self) -> None:
        async with self.pool:
            await asyncio.sleep(0.2 + random.uniform(0, 0.05))


async def hammer(gate: AdaptiveGate | None, total: int = 400) -> None:
    ds = Downstream()
    latencies: list[float] = []

    async def one(i: int) -> None:
        if gate:
            await gate.acquire()
        t0 = time.monotonic()
        await ds.query()
        elapsed = time.monotonic() - t0
        latencies.append(elapsed)
        if gate:
            await gate.release(elapsed)

    t0 = time.monotonic()
    await asyncio.gather(*(one(i) for i in range(total)))
    wall = time.monotonic() - t0
    latencies.sort()
    p99 = latencies[int(len(latencies) * 0.99)]
    limit = gate.slots if gate else "none (400 in flight)"
    print(
        f"limit={limit}  wall={wall:.1f}s  "
        f"median={latencies[len(latencies) // 2]:.2f}s  p99={p99:.2f}s"
    )


async def main(total: int) -> None:
    print("no limiter:")
    await hammer(None, total)
    print("AIMD limiter:")
    await hammer(AdaptiveGate(), total)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--total", type=int, default=400)
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()
    random.seed(args.seed)
    asyncio.run(main(args.total))
