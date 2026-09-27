"""bounded_app: the same worker, now with a ceiling and a polite refusal.

The queue is bounded. When it is full, the endpoint sheds load with
HTTP 429 and a Retry-After header instead of accepting work it cannot do.
The server survives; the client decides whether to retry later.
"""

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from backpressure import BoundedWorkQueue

work = BoundedWorkQueue(maxsize=100, retry_after_seconds=2)


async def worker() -> None:
    while True:
        job = await work.get()
        await asyncio.sleep(0.05)  # stand-in for real work: a DB write, an API call
        work.task_done()


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(worker())
    yield
    task.cancel()


app = FastAPI(title="bounded worker (backpressure)", lifespan=lifespan)


@app.post("/jobs")
async def enqueue(payload: dict):
    if not work.try_enqueue(payload):
        return JSONResponse(
            status_code=429,
            content={"error": "queue full"},
            headers={"Retry-After": str(work.retry_after_seconds)},
        )
    return {"accepted": True, "depth": work.depth}


@app.get("/stats")
async def stats():
    return {"depth": work.depth, "accepted": work.accepted, "shed": work.shed}
