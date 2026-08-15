import json
import threading


class _EventClient:
    def __init__(self):
        self.closed = threading.Event()
        self.emitted = threading.Event()
        self.release = threading.Event()
        self.subscribe_calls = []

    def health_check(self):
        return "SERVING"

    def get_collection_stats(self, name):
        return {"name": name, "metric": "lorentz", "dimension": 129}

    def list_collections(self):
        return [{"name": "test_memory", "metric": "lorentz", "dimension": 129}]

    def subscribe_to_events(self, types=None, collection=None):
        self.subscribe_calls.append({"types": list(types or []), "collection": collection})
        yield {
            "type": "insert",
            "payload": {
                "id": 123,
                "collection": collection,
                "logical_clock": 7,
                "metadata": {
                    "_hs_owner": "hermes-hyperspacedb",
                    "_hs_profile": "event-test-profile",
                    "source": "hermes-builtin-memory",
                    "target": "memory",
                    "trust": "builtin-curated",
                    "_content": "must never reach the polling tool",
                },
            },
        }
        self.emitted.set()
        self.release.wait(2.0)

    def close(self):
        self.closed.set()
        self.release.set()


def test_event_observation_is_opt_in_and_default_tool_surface_is_unchanged(provider):
    names = [schema["name"] for schema in provider.get_tool_schemas()]
    assert "hyperspace_events" not in names
    assert "hyperspace_reconcile" not in names
    assert "hyperspace_batch" not in names
    response = json.loads(provider.handle_tool_call("hyperspace_events", {"operation": "recent"}))
    assert response["ok"] is False
    assert response["error"]["code"] == "UNKNOWN_TOOL"


def test_enabled_event_observer_returns_only_sanitized_profile_owned_inserts(plugin, tmp_path):
    clients = []

    def factory(**kwargs):
        client = _EventClient()
        clients.append(client)
        return client

    provider = plugin.HyperspaceDBMemoryProvider(
        {
            "collection": "test_memory",
            "host": "127.0.0.1:50051",
            "auto_store": False,
            "event_observation_enabled": True,
            "event_buffer_size": 8,
            "profile_scope": "event-test-profile",
            "state_path": str(tmp_path / "events.sqlite3"),
        },
        client_factory=factory,
    )
    provider.initialize("event-test-session")
    try:
        assert "hyperspace_events" in [schema["name"] for schema in provider.get_tool_schemas()]
        assert len(clients) == 2
        event_client = clients[1]
        assert event_client.emitted.wait(1.0)

        response = json.loads(provider.handle_tool_call("hyperspace_events", {
            "operation": "recent", "limit": 5,
        }))
        assert response["ok"] is True
        assert response["dropped"] == 0
        assert response["filtered"] == 0
        assert response["events"] == [{
            "event_type": "insert",
            "source": "hermes-builtin-memory",
            "target": "memory",
            "trust": "builtin-curated",
        }]
        dumped = json.dumps(response["events"])
        assert "id" not in dumped
        assert "metadata" not in dumped
        assert "content" not in dumped
    finally:
        if len(clients) > 1:
            clients[1].release.set()
        provider.shutdown()
        if len(clients) > 1:
            assert not provider._event_thread.is_alive()
            assert clients[1].closed.is_set()


def test_reconcile_tool_hidden_until_operator_enable(provider):
    out = json.loads(provider.handle_tool_call("hyperspace_reconcile", {"operation": "dry_run"}))
    assert out["ok"] is False
    assert out["error"]["code"] == "UNKNOWN_TOOL"


def test_operator_reconcile_dry_run_does_not_mutate(plugin, provider, fake_client, tmp_path):
    enabled = plugin.HyperspaceDBMemoryProvider(
        {
            "collection": "test_memory",
            "host": "127.0.0.1:50051",
            "api_key": "test-key",
            "ownership_hmac_key": "test-ownership-key",
            "state_path": str(tmp_path / "reconcile.sqlite3"),
            "auto_store": True,
            "operator_reconcile_enabled": True,
            "trust_mode": "annotate_all",
        },
        client_factory=lambda **kwargs: fake_client,
    )
    enabled.initialize("reconcile-session")
    try:
        assert "hyperspace_reconcile" in [s["name"] for s in enabled.get_tool_schemas()]
        enabled.on_memory_write("add", "memory", "recoverable delete fact")
        assert enabled.flush_writes(timeout=2.0)
        record = enabled._ledger.resolve(enabled._profile_scope, "memory", "recoverable delete")[0]
        enabled._ledger.set_status(record.digest, "delete_pending", "simulated lost response")
        dry = json.loads(enabled.handle_tool_call("hyperspace_reconcile", {"operation": "dry_run", "limit": 8}))
        assert dry["ok"] is True
        assert dry["state"] == "DRY_RUN"
        assert dry["would_apply"] == 1
        assert enabled._ledger.get(record.digest).status == "delete_pending"
        assert record.external_id in fake_client.points
        missing = json.loads(enabled.handle_tool_call("hyperspace_reconcile", {"operation": "apply", "limit": 8}))
        assert missing["ok"] is False
        assert missing["error"]["code"] == "INVALID_ARGUMENT"
        applied = json.loads(enabled.handle_tool_call(
            "hyperspace_reconcile",
            {"operation": "apply", "limit": 8, "idempotency_token": "tok-a8-1"},
        ))
        assert applied["ok"] is True
        assert applied["state"] in {"APPLIED", "PARTIAL"}
        assert applied["receipt"]
        assert enabled._ledger.get(record.digest).status == "removed"
        replay = json.loads(enabled.handle_tool_call(
            "hyperspace_reconcile",
            {"operation": "apply", "limit": 8, "idempotency_token": "tok-a8-1"},
        ))
        assert replay["ok"] is True
        assert replay["state"] == "IDEMPOTENT_REPLAY"
    finally:
        enabled.shutdown()


def test_batch_hidden_until_enabled(provider):
    out = json.loads(provider.handle_tool_call("hyperspace_batch", {"operations": []}))
    assert out["ok"] is False
    assert out["error"]["code"] == "UNKNOWN_TOOL"


def test_batch_uses_single_record_invariants(plugin, fake_client, tmp_path):
    enabled = plugin.HyperspaceDBMemoryProvider(
        {
            "collection": "test_memory",
            "host": "127.0.0.1:50051",
            "api_key": "test-key",
            "ownership_hmac_key": "test-ownership-key",
            "state_path": str(tmp_path / "batch.sqlite3"),
            "auto_store": True,
            "batch_mutation_enabled": True,
            "trust_mode": "annotate_all",
        },
        client_factory=lambda **kwargs: fake_client,
    )
    enabled.initialize("batch-session")
    try:
        assert "hyperspace_batch" in [s["name"] for s in enabled.get_tool_schemas()]
        too_big = json.loads(enabled.handle_tool_call("hyperspace_batch", {
            "operations": [{"action": "add", "content": f"x{i}"} for i in range(17)],
        }))
        assert too_big["ok"] is False
        assert too_big["error"]["code"] == "INVALID_ARGUMENT"
        out = json.loads(enabled.handle_tool_call("hyperspace_batch", {
            "operations": [
                {"action": "add", "content": "batch fact alpha"},
                {"action": "add", "content": "batch fact beta"},
                {"action": "remove", "old_text": "batch fact alpha"},
            ],
        }))
        assert out["ok"] is True
        assert out["state"] == "COMPLETE"
        assert len(out["results"]) == 3
        assert all(item["ok"] is True for item in out["results"])
        assert any("batch fact beta" in str(pt.get("metadata", {}).get("_content", "")) or
                   b"batch fact beta" in (pt.get("payload") or b"")
                   for pt in fake_client.points.values())
    finally:
        enabled.shutdown()
