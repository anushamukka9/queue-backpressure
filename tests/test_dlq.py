"""Dead-letter queue: parking permanently failed jobs."""

import pytest

from backpressure import DeadLetterQueue


def test_park_and_list():
    dlq = DeadLetterQueue()
    dlq.park({"id": 1}, reason="max attempts exceeded", attempts=3, error="boom")
    assert len(dlq) == 1
    entry = dlq.list()[0]
    assert entry["job"] == {"id": 1}
    assert entry["reason"] == "max attempts exceeded"
    assert entry["attempts"] == 3
    assert entry["error"] == "boom"
    assert "parked_at" in entry


def test_list_is_newest_first_and_limitable():
    dlq = DeadLetterQueue()
    for i in range(3):
        dlq.park({"id": i}, reason="r", attempts=1)
    ids = [e["job"]["id"] for e in dlq.list()]
    assert ids == [2, 1, 0]
    assert [e["job"]["id"] for e in dlq.list(limit=2)] == [2, 1]


def test_bounded_evicts_oldest_and_counts():
    dlq = DeadLetterQueue(maxsize=2)
    for i in range(4):
        dlq.park({"id": i}, reason="r", attempts=1)
    assert len(dlq) == 2
    assert [e["job"]["id"] for e in dlq.list()] == [3, 2]
    assert dlq.parked == 4
    assert dlq.evicted == 2


def test_snapshot():
    dlq = DeadLetterQueue(maxsize=5)
    dlq.park({"id": 1}, reason="r", attempts=2)
    assert dlq.snapshot() == {
        "depth": 1,
        "maxsize": 5,
        "parked": 1,
        "evicted": 0,
    }


def test_invalid_maxsize_rejected():
    with pytest.raises(ValueError):
        DeadLetterQueue(maxsize=0)
