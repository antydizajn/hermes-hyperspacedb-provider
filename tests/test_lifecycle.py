import json


def test_client_closed_on_shutdown(provider, fake_client):
    provider.shutdown()
    assert fake_client.closed is True


def test_client_rotates_when_api_key_changes(plugin, tmp_path):
    clients = []
    class C:
        def __init__(self): self.closed = False
        def health_check(self): return "SERVING"
        def get_collection_stats(self, name): return {"name": name}
        def close(self): self.closed = True
    def factory(**kwargs):
        c = C(); clients.append((kwargs, c)); return c
    p = plugin.HyperspaceDBMemoryProvider({
        "collection": "test_memory", "host": "127.0.0.1:50051",
        "api_key": "one", "state_path": str(tmp_path / "ledger.sqlite3")
    }, client_factory=factory)
    p.initialize("s")
    first = p._get_client()
    p._config["api_key"] = "two"
    second = p._get_client()
    assert second is not first
    assert first.closed is False
    p._release_client(first)
    assert first.closed is True
    p._release_client(second)
    p.shutdown()


def test_backup_paths_only_include_provider_state(provider, tmp_path):
    paths = provider.backup_paths()
    assert len(paths) == 1
    assert paths[0].endswith(".snapshot.sqlite3")
    assert "hyperspace-db" not in paths[0].lower()


def test_deadline_stub_proxy_injects_real_timeout(plugin):
    seen = {}
    class Stub:
        def Search(self, request, **kwargs):
            seen.update(kwargs)
            return "ok"
    proxy = plugin._DeadlineStubProxy(Stub(), 1.25)
    assert proxy.Search(object()) == "ok"
    assert seen["timeout"] == 1.25


def test_status_exposes_real_health_and_queue(provider):
    out = json.loads(provider.handle_tool_call("hyperspace_status", {}))
    assert out["ok"] is True
    assert out["health"] == "SERVING"
    assert "pending_writes" in out
    assert out["collection"] == "test_memory"


def test_system_prompt_is_dynamic_and_neutral(provider):
    block = provider.system_prompt_block()
    assert "test_memory" in block
    assert "SERVING" in block
    assert "Gniew" not in block
    assert ("Anti" + "gravity") not in block
