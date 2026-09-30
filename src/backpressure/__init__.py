"""Backpressure primitives.

Bounded queues that refuse work instead of growing forever (with
configurable shed policies), exponential backoff for retries, a
dead-letter queue for permanently failed jobs, an AIMD adaptive
concurrency gate, and monotonic fencing tokens. Companion code for the
tutorial "Give Your Worker Queue Backpressure Before It Falls Over".
"""

from .backoff import backoff_delay, run_with_retries, sleep_backoff
from .fence import FencingTokenStore, StaleWriterError
from .gate import AdaptiveGate
from .queue import BoundedWorkQueue, DeadLetterQueue, ShedPolicy

__all__ = [
    "AdaptiveGate",
    "BoundedWorkQueue",
    "DeadLetterQueue",
    "FencingTokenStore",
    "ShedPolicy",
    "StaleWriterError",
    "backoff_delay",
    "run_with_retries",
    "sleep_backoff",
]
