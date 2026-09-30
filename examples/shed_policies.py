"""shed_policies: what each shed policy does to a burst of jobs.

No server needed. Fills a 5-deep queue with 10 jobs under each policy and
prints where every job went. Run: python examples/shed_policies.py
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from backpressure import BoundedWorkQueue, ShedPolicy  # noqa: E402


def demo(policy: ShedPolicy) -> None:
    q = BoundedWorkQueue(maxsize=5, shed_policy=policy)
    results = [q.try_enqueue({"id": i}) for i in range(10)]
    print(f"policy={policy.value}")
    print(f"  accepted={results.count(True)} shed={q.shed} dropped={q.dropped}")
    if policy is ShedPolicy.DROP_OLDEST:
        # the queue holds the 5 newest jobs; the 5 oldest were evicted
        print("  oldest job evicted to make room: newest jobs kept")


def main() -> None:
    for policy in ShedPolicy:
        demo(policy)


if __name__ == "__main__":
    main()
