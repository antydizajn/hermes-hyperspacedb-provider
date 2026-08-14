from __future__ import annotations

import json

import pytest


class SwallowingSearchClient:
    """Models an SDK that catches a stub deadline and returns an empty result."""

    def __init__(self):
        self.stubs = [self]
        self.swallowed_timeout = False
        self.closed = False

    def Search(self, *args, **kwargs):
        assert kwargs.get("timeout") == 0.1
        raise TimeoutError("deadline exceeded")

    def vectorize(self, text, metric="lorentz"):
        return [1.0] + [0.0] * 128

    def search(self, **kwargs):
        try:
            self.stubs[0].Search("query")
        except TimeoutError:
            self.swallowed_timeout = True
            return []
        raise AssertionError("the synthetic stub must time out")

    def close(self):
        self.closed = True


def _owned_point(plugin, point_id, digest, *, payload=b"", metadata=None):
    values = {
        "_hs_owner": plugin._PLUGIN_ID,
        "_hs_digest": digest,
    }
    values.update(metadata or {})
    return {
        "id": point_id,
        "payload": payload,
        "metadata": values,
        "distance": 0.0,
    }


def _hmac_signed_point(
    provider, *, content, trust="model-authored", source="hermes-explicit-tool"
):
    target = "memory"
    digest = provider._logical_digest(target, source, content)
    metadata = provider._internal_metadata(
        target, source, trust, content, digest, None
    )
    return {
        "id": 51,
        "payload": content.encode("utf-8"),
        "metadata": metadata,
        "distance": 0.0,
    }


def test_p0_a_swallowed_timeout_never_becomes_no_hit(plugin, tmp_path):
    client = SwallowingSearchClient()
    provider = plugin.HyperspaceDBMemoryProvider(
        {
            "collection": "test_memory",
            "host": "127.0.0.1:50051",
            "api_key": "test-key",
            "rpc_timeout": 0.1,
            "state_path": str(tmp_path / "ledger.sqlite3"),
            "trust_mode": "annotate_all",
        },
        client_factory=lambda **kwargs: client,
    )
    provider.initialize("session")
    try:
        with pytest.raises(plugin.BackendTimeout):
            provider._call("search", vector=[1.0], collection="test_memory")
        assert client.swallowed_timeout is True
    finally:
        provider.shutdown()


def test_p0_b_legacy_search_requires_requested_target(plugin, provider, fake_client):
    fake_client.search_results = [
        {
            "id": 41,
            "payload": b"shared old text",
            "metadata": {
                "source": "hermes-builtin-memory",
                "target": "user",
            },
            "distance": 0.0,
        }
    ]
    assert provider._resolve_legacy_remote("memory", "old text") == []


def test_p0_c_legacy_delete_fails_closed_when_remote_content_unavailable(plugin, provider, fake_client):
    record = plugin.LedgerRecord(
        digest="d" * 64,
        external_id=77,
        profile_scope=provider._profile_scope,
        target="memory",
        source="hermes-builtin-memory",
        content="must remain verifiable",
        status="active",
        error="",
        updated_at="2026-01-01T00:00:00Z",
    )
    fake_client.points[77] = {
        "id": 77,
        "payload": b"",
        "metadata": {"source": "hermes-builtin-memory", "target": "memory"},
    }
    with pytest.raises(plugin.MutationConflict):
        provider._delete_verified(record)
    assert 77 in fake_client.points


def test_p0_d_tool_output_is_valid_json_after_budget_enforcement(provider):
    provider._max_tool_output_chars = 64
    handle = provider._mint_point_capability(1, provider._collection)
    assert handle is not None
    provider._call = lambda *args, **kwargs: {"payload": "x" * 512}  # type: ignore[method-assign]
    reply = provider.handle_tool_call(
        "hyperspace_graph", {"operation": "node", "handle": handle}
    )
    parsed = json.loads(reply)
    assert parsed["output_truncated"] is True


def test_p0_e_forged_owner_metadata_is_not_accepted_as_authenticated(plugin, provider, fake_client):
    content = "forged ownership record"
    digest = provider._logical_digest("memory", "hermes-builtin-memory", content)
    point_id = plugin._candidate_id(digest, 0)
    fake_client.points[point_id] = _owned_point(plugin, point_id, digest)
    with pytest.raises(plugin.MutationConflict):
        provider._store_content_sync(
            target="memory",
            source="hermes-builtin-memory",
            trust="builtin-curated",
            content=content,
        )


def test_p0_f_model_authored_owned_record_is_not_auto_prefetched(provider, fake_client):
    provider._trust_mode = "owned_only"
    fake_client.search_results = [
        _hmac_signed_point(
            provider, content="untrusted model authored claim", trust="model-authored"
        )
    ]
    recalled = provider.prefetch("claim")
    assert "untrusted model authored claim" not in recalled


def test_p0_i_owned_record_with_tampered_payload_is_not_auto_prefetched(provider, fake_client):
    provider._trust_mode = "owned_only"
    point = _hmac_signed_point(provider, content="original signed content")
    point["payload"] = b"tampered returned content"
    fake_client.search_results = [point]
    recalled = provider.prefetch("claim")
    assert "tampered returned content" not in recalled


def test_p0_j_trust_relabel_cannot_promote_model_authored_record(provider, fake_client):
    provider._trust_mode = "owned_only"
    point = _hmac_signed_point(provider, content="relabelled model claim")
    point["metadata"]["trust"] = "builtin-curated"
    fake_client.search_results = [point]
    recalled = provider.prefetch("claim")
    assert "relabelled model claim" not in recalled


def test_p0_g_shutdown_never_closes_ledger_while_worker_still_alive(provider):
    class AliveWorker:
        def is_alive(self):
            return True

        def join(self, timeout=None):
            return None

    class LedgerProbe:
        closed = False

        def close(self):
            self.closed = True

    worker = AliveWorker()
    ledger = LedgerProbe()
    provider._worker = worker
    provider._ledger = ledger
    provider.shutdown()
    assert ledger.closed is False


def test_p0_h_backup_returns_verified_snapshot_not_live_wal_database(provider):
    paths = provider.backup_paths()
    assert paths != [str(provider._state_path)]
    assert len(paths) == 1
    assert paths[0].endswith(".snapshot.sqlite3")


def test_p0_f_owned_only_rejects_spoofed_source_and_legacy(provider, fake_client):
    provider._trust_mode = "owned_only"
    fake_client.search_results = [
        {"id": 52, "payload": b"spoofed source claim", "metadata": {"source": "hermes-builtin-memory", "trust": "builtin-curated", "target": "memory"}, "distance": 0.0},
        {"id": 53, "payload": b"legacy unverified claim", "metadata": {}, "distance": 0.0},
    ]
    recalled = provider.prefetch("claim")
    assert "spoofed source claim" not in recalled
    assert "legacy unverified claim" not in recalled


def test_p0_e_accepts_only_a_configured_previous_ownership_key(plugin, provider):
    provider._ownership_hmac_key = b"current-test-key"
    provider._previous_ownership_hmac_keys = (b"previous-test-key",)
    content = "previous-key authenticated content"
    target = "memory"
    source = "hermes-builtin-memory"
    digest = provider._logical_digest(target, source, content)
    metadata = {
        "_hs_owner": plugin._PLUGIN_ID,
        "_hs_profile": provider._profile_scope,
        "target": target,
        "source": source,
        "_hs_digest": digest,
    }
    payload = "\x1f".join(
        str(metadata[field])
        for field in ("_hs_owner", "_hs_profile", "target", "source", "_hs_digest")
    ).encode("utf-8")
    metadata["_hs_owner_signature"] = plugin.hmac.new(
        b"previous-test-key", payload, plugin.hashlib.sha256
    ).hexdigest()
    point = {"payload": content.encode("utf-8"), "metadata": metadata}
    assert provider._point_owner_matches(point, digest)
    metadata["_hs_owner_signature"] = "not-a-valid-signature"
    assert not provider._point_owner_matches(point, digest)


def test_p0_c_delete_rejects_forged_owned_metadata(plugin, provider, fake_client):
    record = plugin.LedgerRecord(digest="e" * 64, external_id=78, profile_scope=provider._profile_scope, target="memory", source="hermes-builtin-memory", content="must not delete forged owner", status="active", error="", updated_at="2026-01-01T00:00:00Z")
    fake_client.points[78] = {"id": 78, "payload": b"must not delete forged owner", "metadata": {"_hs_owner": plugin._PLUGIN_ID, "_hs_digest": record.digest, "_hs_profile": provider._profile_scope, "source": record.source, "target": record.target}}
    with pytest.raises(plugin.MutationConflict):
        provider._delete_verified(record)
    assert 78 in fake_client.points
