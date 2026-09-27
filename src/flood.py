"""flood.py: a rude client. Sends N jobs as fast as the network allows.

It never reads 429s and never retries. Against the naive app this fills
the unbounded queue; against the bounded app most requests get shed.

Usage: python flood.py [--url URL] [--n N] [--workers W]
"""

import argparse
import json
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

PAYLOAD = "x" * 10_000  # 10 KB per job


def send_one(url: str, i: int) -> int:
    body = json.dumps({"id": i, "payload": PAYLOAD}).encode()
    req = urllib.request.Request(
        url, data=body, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read())["depth"]
    except urllib.error.HTTPError as e:
        return -e.code  # negative marks a shed request


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8000/jobs")
    parser.add_argument("--n", type=int, default=1000)
    parser.add_argument("--workers", type=int, default=50)
    args = parser.parse_args()

    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        depths = list(pool.map(lambda i: send_one(args.url, i), range(args.n)))
    wall = time.monotonic() - t0
    shed = sum(1 for d in depths if d < 0)
    accepted_depths = [d for d in depths if d >= 0]
    peak = max(accepted_depths) if accepted_depths else 0
    print(
        f"sent {args.n} jobs in {wall:.1f}s: "
        f"{args.n - shed} accepted (peak depth {peak}), {shed} shed"
    )


if __name__ == "__main__":
    main()
