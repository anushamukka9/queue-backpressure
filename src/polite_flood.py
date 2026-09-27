"""polite_flood.py: a client that takes no for an answer, then asks again later.

Reads 429 + Retry-After and backs off, so shed load turns into delayed
success instead of a retry storm. Pair with the bounded app.

Usage: python polite_flood.py [--url URL] [--n N] [--workers W]
"""

import argparse
import json
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor

PAYLOAD = "x" * 10_000


def send_one(url: str, i: int) -> int:
    """Send one job, honoring Retry-After. Returns the number of retries."""
    body = json.dumps({"id": i, "payload": PAYLOAD}).encode()
    req = urllib.request.Request(
        url, data=body, headers={"Content-Type": "application/json"}
    )
    retries = 0
    for _ in range(12):
        try:
            with urllib.request.urlopen(req):
                return retries
        except urllib.error.HTTPError as e:
            if e.code == 429:
                retries += 1
                time.sleep(float(e.headers.get("Retry-After", "1")))
                continue
            raise
    raise RuntimeError(f"job {i} never got in")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8000/jobs")
    parser.add_argument("--n", type=int, default=300)
    parser.add_argument("--workers", type=int, default=20)
    args = parser.parse_args()

    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        per_job = list(pool.map(lambda i: send_one(args.url, i), range(args.n)))
    wall = time.monotonic() - t0
    print(
        f"all {args.n} jobs accepted in {wall:.0f}s, "
        f"after {sum(per_job)} polite retries"
    )


if __name__ == "__main__":
    main()
