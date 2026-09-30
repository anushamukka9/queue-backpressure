"""adaptive_producer: a producer that listens to the server's slowdown signal.

A monitor thread polls /health and raises the per-send delay when the
server reports high pressure; worker threads send jobs concurrently at
the current rate. The result: throughput stays high while the server is
healthy, and backs off before the queue starts shedding. Compare with
flood.py, which just hammers.

Start a server first, then run this:
    python -m uvicorn resilient_app:app --app-dir src --port 8000
    python examples/adaptive_producer.py --n 2000 --rate 400 --workers 50
"""

import argparse
import json
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor


def get_json(url: str) -> dict:
    with urllib.request.urlopen(url) as r:
        return json.loads(r.read())


def send(url: str, i: int) -> int:
    """Returns the HTTP status: 200 accepted, 429 shed."""
    body = json.dumps({"id": i, "fail_times": 0}).encode()
    req = urllib.request.Request(
        url, data=body, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req):
            return 200
    except urllib.error.HTTPError as e:
        return e.code


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="http://127.0.0.1:8000")
    parser.add_argument("--n", type=int, default=2000)
    parser.add_argument("--rate", type=float, default=400.0,
                        help="target jobs/sec per worker while the server is healthy")
    parser.add_argument("--workers", type=int, default=50)
    args = parser.parse_args()

    base_delay = 1.0 / args.rate
    state = {"delay": base_delay, "sent": 0, "shed": 0, "slow_checks": 0}
    lock = threading.Lock()
    stop = threading.Event()

    def monitor() -> None:
        """Watch /health; grow the delay under pressure, relax when clear."""
        while not stop.is_set():
            try:
                health = get_json(args.base + "/health")
            except Exception:
                time.sleep(0.2)
                continue
            with lock:
                if health["slow_down"]:
                    state["delay"] = min(2.0, state["delay"] * 1.5 + 0.05)
                    state["slow_checks"] += 1
                else:
                    state["delay"] = max(base_delay, state["delay"] * 0.9)
            time.sleep(0.2)

    def one(i: int) -> None:
        with lock:
            delay = state["delay"]
        time.sleep(delay)
        code = send(args.base + "/jobs", i)
        with lock:
            state["sent"] += 1
            if code == 429:
                state["shed"] += 1
                state["delay"] = min(2.0, state["delay"] * 2.0)

    monitor_thread = threading.Thread(target=monitor, daemon=True)
    monitor_thread.start()
    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        list(pool.map(one, range(args.n)))
    wall = time.monotonic() - t0
    stop.set()
    monitor_thread.join()
    stats = get_json(args.base + "/stats")
    print(
        f"sent {state['sent']} jobs in {wall:.1f}s "
        f"({state['sent'] / wall:.0f}/s): {state['shed']} shed, "
        f"slowed on {state['slow_checks']} health checks, "
        f"server accepted={stats['accepted']} completed={stats.get('completed')}"
    )


if __name__ == "__main__":
    main()
