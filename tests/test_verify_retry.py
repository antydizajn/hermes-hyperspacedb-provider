"""Verify-retry regression: read-after-write verification tolerates one blip.

Live E2E (2026-08-22, post v2.6.0 restart) measured a ~5% store failure rate
from single-shot verify: the SDK get_points() swallows RpcError and returns
[], so one transient server blip during the immediate post-insert read made
_insert_verified raise MutationVerificationFailed even though the insert had
landed durably (durability=committed). The record then sat in retry_pending
until the next reconcile, and the tool reported an error for a write that
succeeded. This suite pins the fix: verify retries a bounded number of times
with a short delay before declaring failure. Fail-closed semantics are
unchanged: if the point truly never appears, the mutation still fails.
"""

from __future__ import annotations

import pytest


class FlakyGetPointsClient:
    """Fake client whose get_points misses N times before succeeding."""

    def __init__(self, plugin, tmp_path, miss_first_n: int):
        self._plugin = plugin
        self.calls = 0
        self.miss_first_n = miss_first_n
        cfg = {
            "collection": "test_memory",
            "host": "127.0.0.1:50051",
            "api_key": "k",
            "ownership_hmac_key": "ok",
            "state_path": str(tmp_path / "ledger.sqlite3"),
            "auto_store": False,
            "trust_mode": "owned_only",
        }

        class Client:
            def __init__(self, outer: "FlakyGetPointsClient") -> None:
                self.outer = outer

            def health_check(self):
                return "SERVING"

            def get_collection_stats(self, name):
                return {"name": name, "metric": "lorentz", "dimension": 129}

            def vectorize(self, text, metric="", **kwargs):
                return [0.1] * 129

            def insert(self, point_id, **kwargs):
                self.outer.inserted = True
                self.outer.last_meta = dict(kwargs.get("metadata") or {})
                self.outer.last_content = kwargs.get("document")
                return True

            def get_points(self, ids, collection=""):
                # allocate_id probes use the same RPC; only miss during the
                # verify phase (after insert), never during allocation.
                if self.outer.inserted:
                    self.outer.calls += 1
                    if self.outer.calls <= self.outer.miss_first_n:
                        # Simulate SDK swallow: transient error -> empty list.
                        return []
                    point = {
                        "id": ids[0],
                        "vector": [0.1] * 129,
                        "metadata": dict(self.outer.last_meta or {}),
                        "payload": (self.outer.last_content or "").encode("utf-8"),
                    }
                    return [point]
                return []

            def close(self):
                pass

        self.last_meta = None
        self.inserted = False
        self._client = Client(self)
        self.provider = plugin.HyperspaceDBMemoryProvider(
            cfg, client_factory=lambda **kwargs: self._client
        )

    def initialize(self):
        self.provider.initialize("verify-retry-session")

    def shutdown(self):
        self.provider.shutdown()


def test_verify_retries_past_single_blip(plugin, tmp_path, monkeypatch):
    flaky = FlakyGetPointsClient(plugin, tmp_path, miss_first_n=1)
    monkeypatch.setattr("time.sleep", lambda _s: None)
    flaky.initialize()
    out = flaky.provider.handle_tool_call("hyperspace_store", {
        "content": "RETRY_BLIP_V9: one missed get_points must not fail the store",
    })
    assert '"ok": true' in out or '"ok":true' in out
    assert flaky.calls >= 2
    flaky.shutdown()


def test_verify_still_fails_when_point_never_appears(plugin, tmp_path, monkeypatch):
    flaky = FlakyGetPointsClient(plugin, tmp_path, miss_first_n=10_000)
    monkeypatch.setattr("time.sleep", lambda _s: None)
    flaky.initialize()
    out = flaky.provider.handle_tool_call("hyperspace_store", {
        "content": "RETRY_EXHAUST_V10: point never visible must still fail closed",
    })
    assert '"ok": false' in out or '"ok":false' in out
    assert "MUTATION_VERIFICATION_FAILED" in out
    flaky.shutdown()


@pytest.mark.parametrize("miss", [0])
def test_verify_immediate_success_unaffected(plugin, tmp_path, monkeypatch, miss):
    flaky = FlakyGetPointsClient(plugin, tmp_path, miss_first_n=0)
    monkeypatch.setattr("time.sleep", lambda _s: None)
    flaky.initialize()
    out = flaky.provider.handle_tool_call("hyperspace_store", {
        "content": "RETRY_CLEAN_V11: happy path stays single-shot",
    })
    assert '"ok": true' in out or '"ok":true' in out
    assert flaky.calls == 1
    flaky.shutdown()
