import json
import pytest
from conftest import load_plugin

mod = load_plugin()
BackendAuthError = mod.BackendAuthError
ConfigurationError = mod.ConfigurationError
IdentityLedger = mod.IdentityLedger
LedgerRecord = mod.LedgerRecord
_classify_exception = mod._classify_exception


def test_max_distance_rejects_undistanced_vector_hit(provider, fake_client):
    provider._max_distance = 0.5
    fake_client.search_results = [
        {"id": 1, "payload": b"undistanced content", "metadata": {"source": "user"}},
        {"id": 2, "payload": b"close content", "distance": 0.2, "metadata": {"source": "user"}},
        {"id": 3, "payload": b"far content", "distance": 0.8, "metadata": {"source": "user"}},
    ]
    raw = provider.handle_tool_call("hyperspace_search", {"query": "query", "limit": 5})
    out = json.loads(raw)
    assert out.get("ok") is True
    results = out.get("results", [])
    contents = [r["content"] for r in results]
    assert "close content" in contents
    assert "undistanced content" not in contents
    assert "far content" not in contents


def test_annotate_all_undistanced_vector_denied_prefetch(provider, fake_client):
    provider._trust_mode = "annotate_all"
    provider._max_distance = 0.5
    fake_client.search_results = [
        {"id": 1, "payload": b"undistanced content", "metadata": {"source": "user"}},
        {"id": 2, "payload": b"distanced content", "distance": 0.3, "metadata": {"source": "user"}},
    ]
    results = provider._search_records("query", limit=5)
    lookup = {r["content"]: r for r in results}
    assert "distanced content" in lookup
    assert lookup["distanced content"]["allowed_for_prefetch"] is True
    assert "undistanced content" not in lookup or lookup["undistanced content"]["allowed_for_prefetch"] is False


def test_records_with_status_enforces_profile_scope_isolation(tmp_path):
    db_path = tmp_path / "ledger.sqlite3"
    ledger = IdentityLedger(db_path)
    r1 = LedgerRecord("digest1", 101, "scope_alpha", "memory", "user", "alpha content", "delete_pending", "", "2026-08-17T00:00:00Z")
    r2 = LedgerRecord("digest2", 102, "scope_beta", "memory", "user", "beta content", "delete_pending", "", "2026-08-17T00:00:00Z")
    ledger.upsert(r1)
    ledger.upsert(r2)

    alpha_records = ledger.records_with_status("delete_pending", 10, profile_scope="scope_alpha")
    beta_records = ledger.records_with_status("delete_pending", 10, profile_scope="scope_beta")
    all_records = ledger.records_with_status("delete_pending", 10)

    assert len(alpha_records) == 1
    assert alpha_records[0].digest == "digest1"
    assert len(beta_records) == 1
    assert beta_records[0].digest == "digest2"
    assert len(all_records) == 2


def test_worker_thread_does_not_start_if_collection_contract_fails(plugin, fake_client, tmp_path):
    cfg = {
        "collection": "broken_collection",
        "metric": "lorentz",
        "expected_dimension": 129,
        "state_path": str(tmp_path / "ledger.sqlite3"),
        "auto_store": True,
    }
    p = plugin.HyperspaceDBMemoryProvider(cfg, client_factory=lambda **kwargs: fake_client)
    fake_client.stats_fail = RuntimeError("collection not found")
    p.initialize(session_id="test-session")
    assert p._worker is None or not p._worker.is_alive()
    assert p.status_snapshot()["health"] in {"DEGRADED", "CONFIGURATION_ERROR"}


def test_graph_sanitizer_strips_raw_vectors_and_internal_keys(provider):
    raw_response = {
        "id": 42,
        "nodes": [{"id": 10, "label": "concept"}],
        "vector": [0.1, 0.2, 0.3],
        "embeddings": [[0.1, 0.2]],
        "raw_point_id": 999,
        "_hs_digest": "secret_digest",
        "_hs_owner": "provider_x",
        "depth": 2,
        "distance": 0.15,
    }
    sanitized = provider._sanitize_graph_result(raw_response, collection="test_memory")
    assert "handle" in sanitized
    assert "id" not in sanitized
    assert "vector" not in sanitized
    assert "embeddings" not in sanitized
    assert "raw_point_id" not in sanitized
    assert "_hs_digest" not in sanitized
    assert "_hs_owner" not in sanitized
    assert sanitized["depth"] == 2
    assert sanitized["distance"] == 0.15
    assert sanitized["nodes"][0]["handle"].startswith("hsdbh_")


def test_unauthenticated_error_contains_local_container_guidance():
    class DummyGrpcError(Exception):
        pass

    dummy = DummyGrpcError("StatusCode.UNAUTHENTICATED: invalid bearer token")
    setattr(dummy, "name", "UNAUTHENTICATED")
    err = _classify_exception(dummy)
    assert isinstance(err, BackendAuthError)
    msg = str(err)
    assert "local HyperspaceDB containers" in msg
    assert "api_key" in msg
