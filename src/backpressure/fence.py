"""Monotonic fencing tokens.

A TTL says "this is valid until time T", which is a statement about a clock
you do not control. A fencing token says "this writer holds generation N;
anything older is stale", which is a statement about your own state machine.
Clocks can disagree all they want; the fence does not care.
"""


class StaleWriterError(Exception):
    """A writer presented a token that is not newer than the fenced generation."""


class FencingTokenStore:
    """In-memory fencing token store.

    Tokens need not be timestamps. A monotonically increasing number from
    ZooKeeper, etcd, or a database sequence works, and unlike a timestamp
    it cannot misorder. In production, back this with your real store; the
    protocol is what matters, not the dict.
    """

    def __init__(self) -> None:
        self._fences: dict[str, int] = {}
        self._values: dict[str, object] = {}

    def write(self, key: str, token: int, value: object) -> None:
        """Write ``value`` under ``key`` if ``token`` is newer than the fence.

        Raises StaleWriterError if the token is equal to or older than the
        current fence. Retried writes carry the same token and are correctly
        recognized as duplicates, not new writes.
        """
        current = self._fences.get(key)
        if current is not None and token <= current:
            raise StaleWriterError(
                f"stale token {token} for {key!r}: fenced at generation {current}"
            )
        self._fences[key] = token
        self._values[key] = value

    def read(self, key: str, default: object = None) -> object:
        return self._values.get(key, default)

    def fence(self, key: str) -> int | None:
        """The current fenced generation for ``key``, or None if never written."""
        return self._fences.get(key)
