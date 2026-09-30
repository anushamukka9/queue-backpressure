"""graceful_drain: watch the server stop accepting and finish in-flight work.

Starts resilient_app under uvicorn, posts N jobs (some flaky), sends
SIGTERM, and prints the shutdown log. Expected: /jobs starts returning
503, the workers finish or retry what they can within the drain timeout,
and hopeless jobs land in the DLQ.

Run: python examples/graceful_drain.py [--n 40] [--port 8001]
"""

import argparse
import json
import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "..", "src")


def post(url: str, payload: dict) -> int | None:
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        url, data=body, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req):
            return 200
    except urllib.error.HTTPError as e:
        return e.code
    except urllib.error.URLError:
        return None  # server is gone


def wait_ready(base: str, timeout: float = 20.0) -> None:
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        try:
            with urllib.request.urlopen(base + "/health"):
                return
        except Exception:
            time.sleep(0.2)
    raise RuntimeError("server never came up")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=40)
    parser.add_argument("--port", type=int, default=8001)
    args = parser.parse_args()
    base = f"http://127.0.0.1:{args.port}"

    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "resilient_app:app",
            "--app-dir",
            SRC,
            "--port",
            str(args.port),
            "--log-level",
            "info",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        wait_ready(base)

        # Every job is slow to finish: most fail once and retry with
        # backoff, every 5th never succeeds. This keeps the drain busy
        # for several seconds so the shutdown below is observable.
        for i in range(args.n):
            fail_times = 99 if i % 5 == 4 else 1
            status = post(base + "/jobs", {"id": i, "fail_times": fail_times})
            if status != 200:
                print(f"job {i}: got {status} while loading (queue is tight)")
        print(f"loaded {args.n} jobs; sending SIGTERM")
        proc.send_signal(signal.SIGTERM)

        # During the drain, uvicorn stops listening, so new connections
        # are refused at the TCP level while in-flight work finishes.
        # (The app also answers 503 to any request that does get through
        # during drain - e.g. behind a load balancer that has not noticed
        # the shutdown yet.)
        refused = 0
        saw_503 = False
        while proc.poll() is None:  # still draining
            code = post(base + "/jobs", {"id": "late"})
            if code == 503:
                saw_503 = True
            elif code is None:
                refused += 1
            time.sleep(0.2)
        print(
            f"drain finished: {refused} new connections refused while "
            f"draining, saw_503={saw_503}"
        )
    finally:
        out, _ = proc.communicate(timeout=60)
    print("--- server log ---")
    for line in out.splitlines():
        if any(k in line for k in ("drain", "shutdown", "DLQ", "worker", "completed=")):
            print(line)


if __name__ == "__main__":
    main()
