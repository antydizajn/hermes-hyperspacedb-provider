"""Self-heal regression: initialize must recover from a prior CONFIGURATION_ERROR.

Live finding (2026-08-22): if the first initialize() ran while the backend was
unreachable or the collection contract could not be verified, _health stayed
CONFIGURATION_ERROR forever. Every later initialize() call returned early
because the contract check raised before it could clear the latch, so writes
stayed disabled even after the backend recovered. Recovery required an agent
restart. This suite pins the new behavior: a later initialize() retries the
probe and self-heals when the backend is healthy again.
"""

import json


def _make_provider(plugin, fake_client, tmp_path, **overrides):
    cfg = {
        "collection": "test_memory",
        "host": "127.0.0.1:50051",
        "api_key": "test-key",
        "ownership_hmac_key": "test-ownership-key",
        "state_path": str(tmp_path / "ledger.sqlite3"),
        "auto_store": False,
        "trust_mode": "annotate_all",
    }
    cfg.update(overrides)
    return plugin.HyperspaceDBMemoryProvider(
        cfg, client_factory=lambda **kwargs: fake_client
    )


def test_initialize_self_heals_after_backend_recovers(plugin, fake_client, tmp_path):
    p = _make_provider(plugin, fake_client, tmp_path)
    # First initialize: backend is down (connection refused -> classified error).
    fake_client.fail = ConnectionError("connection refused")
    p.initialize("s1")
    assert p._health in {"DEGRADED", "CONFIGURATION_ERROR"}
    # Backend comes back.
    fake_client.fail = None
    fake_client.health = "SERVING"
    p.initialize("s2")
    assert p._health == "SERVING"
    assert p.status_snapshot()["collection_contract_verified"] is True
    p.shutdown()


def test_initialize_still_fails_closed_when_backend_still_down(
    plugin, fake_client, tmp_path
):
    p = _make_provider(plugin, fake_client, tmp_path)
    fake_client.fail = ConnectionError("connection refused")
    p.initialize("s1")
    p.initialize("s2")
    assert p._health in {"DEGRADED", "CONFIGURATION_ERROR"}
    assert p._collection_contract_verified is False
    p.shutdown()


def test_initialize_heals_metric_mismatch_after_config_fix(
    plugin, fake_client, tmp_path
):
    p = _make_provider(plugin, fake_client, tmp_path)
    # Collection reports a metric that does not match config.
    stats = dict(fake_client.stats_value or {})
    fake_client.stats_value = {
        "name": "test_memory", "metric": "cosine", "dimension": 129,
    }
    p.initialize("s1")
    assert p._health == "CONFIGURATION_ERROR"
    # Operator fixed the server-side collection; now it matches.
    fake_client.stats_value = None
    p.initialize("s2")
    assert p._health == "SERVING"
    p.shutdown()


def test_worker_starts_even_when_first_init_is_degraded(
    plugin, fake_client, tmp_path
):
    """Regression (2026-08-22): the ordered write worker must start before the
    health probe so mirrored writes during a DEGRADED first session are
    serialized into the ledger (retry_pending) instead of sitting in-process
    until a second initialize(). Restores v2.4.3 behavior."""
    cfg = {
        "collection": "test_memory",
        "host": "127.0.0.1:50051",
        "state_path": str(tmp_path / "ledger.sqlite3"),
        "trust_mode": "annotate_all",
        "auto_store": True,
    }
    p = plugin.HyperspaceDBMemoryProvider(
        cfg, client_factory=lambda **kwargs: fake_client
    )
    fake_client.fail = ConnectionError("connection refused")
    p.initialize("s1")
    assert p._worker is not None and p._worker.is_alive()
    p.shutdown()
