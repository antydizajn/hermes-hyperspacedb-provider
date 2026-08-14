from __future__ import annotations

import json
import threading

from conftest import FakeClient


def test_write_rpc_timeout_grows_with_content_and_stays_capped(provider):
    short = provider._write_rpc_timeout("x")
    long = provider._write_rpc_timeout("y" * 12000)
    assert long > short
    assert short >= provider._rpc_timeout
    assert long <= 300.0


def test_store_installs_longer_deadline_than_default_rpc(provider, fake_client, monkeypatch):
    seen = []
    plugin = __import__("sys").modules[provider.__class__.__module__]
    original = plugin._install_deadlines

    def spy(client, timeout):
        seen.append(timeout)
        return original(client, timeout)

    monkeypatch.setattr(plugin, "_install_deadlines", spy)
    payload = "provider polish store " + ("z" * 8000)
    raw = provider.handle_tool_call("hyperspace_store", {"content": payload})
    result = json.loads(raw)
    assert result.get("ok") is True
    write_timeouts = [item for item in seen if item > provider._rpc_timeout]
    assert write_timeouts, seen


def test_parallel_graph_calls_on_same_handle_both_succeed(provider, fake_client):
    fake_client.search_results = [{
        "id": 11,
        "payload": b"result used to mint a graph capability",
        "metadata": {"source": "local-test", "trust": "unknown", "target": "memory", "_content": "alpha"},
        "distance": 0.0,
    }]
    fake_client.get_node = lambda id, layer=0, collection="": {"id": id, "layer": layer, "neighbors": [12, 13]}
    search = json.loads(provider.handle_tool_call("hyperspace_search", {"query": "alpha", "limit": 1}))
    handle = search["results"][0]["handle"]
    results = [None, None]
    errors = [None, None]
    start = threading.Barrier(2)

    def worker(slot):
        try:
            start.wait(timeout=2)
            results[slot] = json.loads(provider.handle_tool_call(
                "hyperspace_graph", {"operation": "node", "handle": handle}
            ))
        except Exception as exc:
            errors[slot] = exc

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)
    assert errors == [None, None]
    assert all(item and item.get("ok") is True for item in results)
