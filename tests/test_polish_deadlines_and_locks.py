from __future__ import annotations

import json
import threading
import time


class _RecStub:
    def __init__(self):
        self.last_timeout = None

    def Insert(self, *args, **kwargs):
        self.last_timeout = kwargs.get("timeout")

    def Search(self, *args, **kwargs):
        self.last_timeout = kwargs.get("timeout")


class _SdkLikeClient:
    """Mimic HyperspaceClient.stub selection + insert that touches the stub."""

    def __init__(self):
        self._thread_local = threading.local()
        self.stubs = [_RecStub()]
        self.seen_timeouts = []
        self.points = {}
        self.health = "SERVING"
        self.closed = False
        self.search_results = None
        self.stats_value = None
        self.count_value = 0
        self.digest_value = {"logical_clock": 0, "state_hash": 0, "count": 0}
        self.cache_stats_value = {
            "l1_size": 0, "l2_index_size": 0, "l1_hit_rate": 0.0, "l2_hit_rate": 0.0,
            "tombstone_count": 0, "pending_rebuild": 0, "estimated_memory_bytes": 0,
        }

    @property
    def stub(self):
        if hasattr(self._thread_local, "stub"):
            return self._thread_local.stub
        selected = self.stubs[0]
        self._thread_local.stub = selected
        return selected

    def health_check(self):
        return self.health

    def insert(self, *args, **kwargs):
        self.stub.Insert()
        inner = self.stubs[0]._stub if hasattr(self.stubs[0], "_stub") else self.stubs[0]
        self.seen_timeouts.append(("insert", inner.last_timeout, threading.get_ident()))
        return True

    def search(self, *args, **kwargs):
        self.stub.Search()
        return list(self.search_results or [])

    def vectorize(self, *args, **kwargs):
        return [1.0] * 129

    def get_points(self, ids, collection=""):
        return []

    def get_collection_stats(self, name):
        return self.stats_value or {}

    def collection_count(self, name):
        return self.count_value

    def close(self):
        self.closed = True


def test_write_rpc_timeout_grows_with_content_and_stays_capped(provider):
    short = provider._write_rpc_timeout("x")
    long = provider._write_rpc_timeout("y" * 12000)
    assert long > short
    assert short >= provider._rpc_timeout
    assert long <= 300.0


def test_write_deadline_survives_interleaved_read_reinstall(plugin, tmp_path):
    client = _SdkLikeClient()
    cfg = {
        "collection": "test_memory",
        "host": "127.0.0.1:50051",
        "api_key": "***",
        "ownership_hmac_key": "test-ownership-key",
        "state_path": str(tmp_path / "ledger.sqlite3"),
        "auto_store": True,
        "rpc_timeout": 4.0,
        "trust_mode": "annotate_all",
    }
    provider = plugin.HyperspaceDBMemoryProvider(cfg, client_factory=lambda **kwargs: client)
    provider.initialize("race-session")
    write_installed = threading.Event()
    read_installed = threading.Event()
    errors = []
    original = plugin._push_rpc_deadline
    holders = {}

    def hooked(timeout):
        result = original(timeout)
        current = threading.current_thread()
        if current is holders.get("writer") and timeout == 25.0:
            write_installed.set()
            assert read_installed.wait(timeout=2)
        elif current is holders.get("reader") and timeout == 4.0:
            assert write_installed.wait(timeout=2)
            read_installed.set()
        return result

    plugin._push_rpc_deadline = hooked

    def writer():
        try:
            provider._call("insert", 1, rpc_timeout=25.0)
        except Exception as exc:
            errors.append(exc)

    def reader():
        try:
            assert write_installed.wait(timeout=2)
            provider._call("search", rpc_timeout=4.0)
        except Exception as exc:
            errors.append(exc)

    holders["writer"] = threading.Thread(target=writer)
    holders["reader"] = threading.Thread(target=reader)
    holders["writer"].start()
    holders["reader"].start()
    holders["writer"].join(timeout=5)
    holders["reader"].join(timeout=5)
    plugin._push_rpc_deadline = original
    provider.shutdown()
    assert errors == []
    insert_timeouts = [item[1] for item in client.seen_timeouts if item[0] == "insert"]
    assert insert_timeouts == [25.0], client.seen_timeouts


def test_handle_op_lock_keeps_graph_backend_calls_exclusive(provider, fake_client):
    fake_client.search_results = [{
        "id": 11,
        "payload": b"result used to mint a graph capability",
        "metadata": {"source": "local-test", "trust": "unknown", "target": "memory", "_content": "alpha"},
        "distance": 0.0,
    }]
    active = 0
    max_active = 0
    guard = threading.Lock()

    def get_node(id, layer=0, collection=""):
        nonlocal active, max_active
        with guard:
            active += 1
            max_active = max(max_active, active)
        time.sleep(0.08)
        with guard:
            active -= 1
        return {"id": id, "layer": layer, "neighbors": [12]}

    fake_client.get_node = get_node
    search = json.loads(provider.handle_tool_call("hyperspace_search", {"query": "alpha", "limit": 1}))
    handle = search["results"][0]["handle"]
    errors = []

    def worker():
        try:
            result = json.loads(provider.handle_tool_call(
                "hyperspace_graph", {"operation": "node", "handle": handle}
            ))
            assert result.get("ok") is True
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)
    assert errors == []
    assert max_active == 1
