import json


def test_search_returns_fresh_ledger_write_when_vector_index_misses(provider, fake_client):
    fake_client.search_results = []
    marker = "ryw_ledger_nonce_7f3a9c"
    provider.on_memory_write("add", "memory", f"fresh fact {marker}")
    assert provider.flush_writes(timeout=5)
    out = json.loads(provider.handle_tool_call("hyperspace_search", {"query": marker, "limit": 5}))
    assert out["ok"] is True
    assert out["state"] == "HIT"
    assert any(marker in item.get("content", "") for item in out["results"])


def test_ledger_fallback_does_not_match_unrelated_query(provider, fake_client):
    fake_client.search_results = []
    provider.on_memory_write("add", "memory", "fresh fact ryw_ledger_nonce_7f3a9c")
    assert provider.flush_writes(timeout=5)
    out = json.loads(provider.handle_tool_call("hyperspace_search", {"query": "zzzz_no_such_token", "limit": 5}))
    assert out["ok"] is True
    assert out["state"] == "NO_HIT"
    assert out["results"] == []


def test_ledger_fallback_result_has_capability_handle(provider, fake_client):
    fake_client.search_results = []
    marker = "ryw_handle_nonce_aa11"
    provider.on_memory_write("add", "memory", f"fresh fact {marker}")
    assert provider.flush_writes(timeout=5)
    out = json.loads(provider.handle_tool_call("hyperspace_search", {"query": marker, "limit": 5}))
    assert out["ok"] is True
    hit = next(item for item in out["results"] if marker in item.get("content", ""))
    handle = hit.get("handle")
    assert isinstance(handle, str) and handle.startswith("hsdbh_")
    node = json.loads(provider.handle_tool_call("hyperspace_graph", {"operation": "node", "handle": handle}))
    assert node.get("ok") is True
