import pytest

from backpressure import FencingTokenStore, StaleWriterError


def test_write_and_read():
    s = FencingTokenStore()
    s.write("k", 1, "v1")
    assert s.read("k") == "v1"
    assert s.fence("k") == 1


def test_stale_and_equal_tokens_rejected():
    s = FencingTokenStore()
    s.write("k", 5, "v")
    with pytest.raises(StaleWriterError):
        s.write("k", 5, "v2")  # equal token: a retry, not a new write
    with pytest.raises(StaleWriterError):
        s.write("k", 3, "v0")  # older token: stale writer
    assert s.read("k") == "v"  # the fenced value is untouched


def test_newer_token_wins():
    s = FencingTokenStore()
    s.write("k", 1, "old")
    s.write("k", 2, "new")
    assert s.read("k") == "new"
    assert s.fence("k") == 2


def test_keys_are_independent():
    s = FencingTokenStore()
    s.write("a", 10, "x")
    s.write("b", 1, "y")
    assert s.read("a") == "x"
    assert s.read("b") == "y"


def test_unknown_key_has_no_fence():
    s = FencingTokenStore()
    assert s.fence("nope") is None
    assert s.read("nope") is None
