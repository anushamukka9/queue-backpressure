"""naive_app: the worker with no backpressure. Do not run this in production.

An unbounded asyncio.Queue that never says no. Under a flood, the backlog
grows until the process runs out of memory. This is the "before" picture.
"""

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI

queue: asyncio.Queue = asyncio.Queue()  # no maxsize: this is the whole problem


async def worker() -> None:
    while True:
        job = await queue.get()
        await asyncio.sleep(0.05)  # stand-in for real work: a DB write, an API call
        queue.task_done()


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(worker())
    yield
    task.cancel()


app = FastAPI(title="naive worker (no backpressure)", lifespan=lifespan)


@app.post("/jobs")
async def enqueue(payload: dict):
    queue.put_nowait(payload)  # never says no
    return {"accepted": True, "depth": queue.qsize()}


@app.get("/stats")
async def stats():
    return {"depth": queue.qsize()}
