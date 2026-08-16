from __future__ import annotations

import json


def test_ledger_fallback_does_not_evict_vector_hits(provider, fake_client):
    marker = "shared_q_aa91"
    fake_client.search_results = [
        {
            "id": idx,
            "payload": ("vec_%s %s" % (idx, marker)).encode("utf-8"),
            "metadata": {"_content": "vec_%s %s" % (idx, marker), "source": "other"},
            "distance": float(idx),
        }
        for idx in range(1, 6)
    ]
    for idx in range(1, 6):
        provider.on_memory_write("add", "memory", "led_%s %s" % (idx, marker))
    assert provider.flush_writes(timeout=5)
    out = json.loads(provider.handle_tool_call(
        "hyperspace_search", {"query": marker, "limit": 5},
    ))
    assert out["ok"] is True
    contents = [item.get("content", "") for item in out["results"]]
    assert len(contents) == 5
    assert any(item.startswith("vec_") for item in contents)
    assert sum(item.startswith("vec_") for item in contents) == 5


def test_annotate_all_max_distance_excludes_undistanced_ledger_from_prefetch(plugin, fake_client, tmp_path):
    provider = plugin.HyperspaceDBMemoryProvider(
        {
            "collection": "test_memory",
            "host": "127.0.0.1:50051",
            "api_key": "test-key",
            "ownership_hmac_key": "test-ownership-key",
            "state_path": str(tmp_path / "ledger.sqlite3"),
            "auto_store": True,
            "rpc_timeout": 0.25,
            "top_k": 5,
            "trust_mode": "annotate_all",
            "max_distance": 0.1,
        },
        client_factory=lambda **kwargs: fake_client,
    )
    provider.initialize("annotate-all-session")
    try:
        fake_client.search_results = []
        marker = "undistanced_ledger_bb17"
        provider.on_memory_write("add", "memory", "fresh undistanced " + marker)
        assert provider.flush_writes(timeout=5)
        search = json.loads(provider.handle_tool_call(
            "hyperspace_search", {"query": marker, "limit": 5},
        ))
        assert search["ok"] is True
        assert search["state"] == "HIT"
        assert any(marker in item.get("content", "") for item in search["results"])
        prefetched = provider.prefetch(marker)
        assert marker not in prefetched
    finally:
        provider.shutdown()
