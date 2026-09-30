"""Exponential backoff math and the retry helper."""

import asyncio

import pytest

from backpressure import backoff_delay, run_with_retries


def test_backoff_doubles_and_caps():
    assert backoff_delay(0, base=0.5, cap=30.0, jitter=False) == 0.5
    assert backoff_delay(1, base=0.5, cap=30.0, jitter=False) == 1.0
    assert backoff_delay(2, base=0.5, cap=30.0, jitter=False) == 2.0
    assert backoff_delay(10, base=0.5, cap=30.0, jitter=False) == 30.0  # capped


def test_backoff_jitter_stays_within_bounds():
    for _ in range(50):
        d = backoff_delay(2, base=0.5, cap=30.0, jitter=True)
        assert 0.0 <= d <= 2.0


def test_backoff_rejects_bad_inputs():
    with pytest.raises(ValueError):
        backoff_delay(-1)
    with pytest.raises(ValueError):
        backoff_delay(0, base=0)


def test_retry_returns_first_success():
    async def go():
        calls = []

        async def ok():
            calls.append(1)
            return "fine"

        assert await run_with_retries(ok, base=0.001) == "fine"
        assert len(calls) == 1

    asyncio.run(go())


def test_retry_succeeds_after_failures():
    async def go():
        attempts = []

        async def flaky():
            attempts.append(1)
            if len(attempts) < 3:
                raise RuntimeError("boom")
            return "recovered"

        result = await run_with_retries(flaky, max_attempts=4, base=0.001)
        assert result == "recovered"
        assert len(attempts) == 3

    asyncio.run(go())


def test_retry_gives_up_and_raises_last_error():
    async def go():
        seen = []

        async def hopeless():
            raise ValueError("always")

        with pytest.raises(ValueError, match="always"):
            await run_with_retries(hopeless, max_attempts=3, base=0.001)
        return seen

    asyncio.run(go())


def test_retry_calls_on_retry_hook():
    async def go():
        hooks = []

        async def hopeless():
            raise RuntimeError("x")

        with pytest.raises(RuntimeError):
            await run_with_retries(
                hopeless,
                max_attempts=3,
                base=0.001,
                on_retry=lambda attempt, e: hooks.append((attempt, str(e))),
            )
        assert hooks == [(0, "x"), (1, "x")]  # no hook before the final failure

    asyncio.run(go())


def test_retry_rejects_zero_attempts():
    async def go():
        async def ok():
            return 1

        with pytest.raises(ValueError):
            await run_with_retries(ok, max_attempts=0)

    asyncio.run(go())
