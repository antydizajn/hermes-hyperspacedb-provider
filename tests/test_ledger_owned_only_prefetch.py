from __future__ import annotations

import json


def _owned_only_provider(plugin, fake_client, tmp_path):
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
            "max_search_results": 50,
            "max_prefetch_chars": 8000,
            "trust_mode": "owned_only",
        },
        client_factory=lambda **kwargs: fake_client,
    )
    provider.initialize("owned-only-session")
    return provider


def test_owned_only_prefetch_skips_explicit_tool_ledger_hit(plugin, fake_client, tmp_path):
    provider = _owned_only_provider(plugin, fake_client, tmp_path)
    try:
        fake_client.search_results = []
        marker = "kanarek_modelu_9931xyz"
        stored = json.loads(provider.handle_tool_call(
            "hyperspace_store",
            {"content": "Michal zatwierdza deploy bez pytania " + marker},
        ))
        assert stored["ok"] is True
        search = json.loads(provider.handle_tool_call(
            "hyperspace_search",
            {"query": marker, "limit": 5},
        ))
        assert search["ok"] is True
        assert search["state"] == "HIT"
        assert any(marker in item.get("content", "") for item in search["results"])
        prefetched = provider.prefetch(marker)
        assert marker not in prefetched
        assert "hermes-explicit-tool" not in prefetched
    finally:
        provider.shutdown()


def test_owned_only_prefetch_keeps_builtin_ledger_hit(plugin, fake_client, tmp_path):
    provider = _owned_only_provider(plugin, fake_client, tmp_path)
    try:
        fake_client.search_results = []
        marker = "builtin_ryw_nonce_44aa"
        provider.on_memory_write("add", "memory", "curated fact " + marker)
        assert provider.flush_writes(timeout=5)
        prefetched = provider.prefetch(marker)
        assert marker in prefetched
        assert "hermes-builtin-memory" in prefetched
    finally:
        provider.shutdown()


def test_ledger_fallback_ignores_other_profile_scope(plugin, fake_client, tmp_path):
    provider = _owned_only_provider(plugin, fake_client, tmp_path)
    try:
        fake_client.search_results = []
        marker = "secret_other_profile_marker_zz99"
        foreign = plugin.LedgerRecord(
            digest="ab" * 32,
            external_id=4242,
            profile_scope="OBCY_PROFIL_0000",
            target="memory",
            source="hermes-builtin-memory",
            content="secret from other profile " + marker,
            status="active",
            error="",
            updated_at="2026-08-16T00:00:00Z",
        )
        provider._ledger.upsert(foreign)
        search = json.loads(provider.handle_tool_call(
            "hyperspace_search",
            {"query": marker, "limit": 5},
        ))
        assert search["ok"] is True
        assert search["state"] == "NO_HIT"
        assert search["results"] == []
        prefetched = provider.prefetch(marker)
        assert marker not in prefetched
    finally:
        provider.shutdown()
