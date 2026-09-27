"""End-to-end tests against the demo apps.

The TestClient is used *without* entering its lifespan context manager, so
the background worker never starts and the queue fills deterministically.
"""

from fastapi.testclient import TestClient

import bounded_app
import naive_app


def test_bounded_sheds_with_retry_after():
    client = TestClient(bounded_app.app)
    for i in range(100):
        r = client.post("/jobs", json={"id": i})
        assert r.status_code == 200, r.text
        assert r.json()["accepted"] is True
    r = client.post("/jobs", json={"id": "one-too-many"})
    assert r.status_code == 429
    assert r.headers["retry-after"] == "2"
    assert r.json() == {"error": "queue full"}
    stats = client.get("/stats").json()
    assert stats["depth"] == 100
    assert stats["accepted"] == 100
    assert stats["shed"] == 1


def test_naive_accepts_everything():
    client = TestClient(naive_app.app)
    for i in range(150):
        r = client.post("/jobs", json={"id": i})
        assert r.json()["accepted"] is True
    assert client.get("/stats").json()["depth"] == 150
