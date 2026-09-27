"""Backpressure primitives.

Bounded queues that refuse work instead of growing forever, an AIMD
adaptive concurrency gate, and monotonic fencing tokens. Companion code for
the tutorial "Give Your Worker Queue Backpressure Before It Falls Over".
"""

from .fence import FencingTokenStore, StaleWriterError
from .gate import AdaptiveGate
from .queue import BoundedWorkQueue

__all__ = [
    "AdaptiveGate",
    "BoundedWorkQueue",
    "FencingTokenStore",
    "StaleWriterError",
]
