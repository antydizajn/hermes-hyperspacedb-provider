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
        assert "id" not in json.dumps(response["events"])
        assert "metadata" not in json.dumps(response["events"])
        assert "content" not in json.dumps(response["events"])
    finally:
        if len(clients) > 1:
            clients[1].release.set()
        provider.shutdown()
        if len(clients) > 1:
            assert not provider._event_thread.is_alive()
            assert clients[1].closed.is_set()
