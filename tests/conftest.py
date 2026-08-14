from __future__ import annotations

import sys
from pathlib import Path as _Path

_STUB = _Path(__file__).resolve().parent / "stubs"
if _STUB.is_dir():
    try:
        import hyperspace  # noqa: F401
    except ImportError:
        sys.path.insert(0, str(_STUB))

import importlib.util
import sys
from pathlib import Path

import pytest

PLUGIN_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def isolate_hyperspace_process_env(monkeypatch):
    """Keep unit tests independent of the operator's live Hyperspace env."""
    for name in (
        "HYPERSPACE_API_KEY",
        "HYPERSPACE_USER_ID",
        "HYPERSPACE_OWNERSHIP_HMAC_KEY",
        "HSDB_TEST_OWNERSHIP_HMAC_KEY",
    ):
        monkeypatch.delenv(name, raising=False)


def load_plugin():
    name = "hermes_hyperspacedb_plugin_under_test"
    sys.modules.pop(name, None)
    spec = importlib.util.spec_from_file_location(name, PLUGIN_ROOT / "__init__.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class FakeClient:
    def __init__(self):
        self.points = {}
        self.health = "SERVING"
        self.fail = None
        self.closed = False
        self.last_search = None
        self.calls = []
        self.search_results = None
        self.stats_value = None
        self.count_value = 0
        self.digest_value = {"logical_clock": 0, "state_hash": 0, "count": 0}
        self.cache_stats_value = {
            "l1_size": 0,
            "l2_index_size": 0,
            "l1_hit_rate": 0.0,
            "l2_hit_rate": 0.0,
            "tombstone_count": 0,
            "pending_rebuild": 0,
            "estimated_memory_bytes": 0,
        }

    def _maybe_fail(self):
        if self.fail:
            raise self.fail

    def health_check(self):
        self._maybe_fail()
        self.calls.append(("health_check", {}))
        return self.health

    def get_collection_stats(self, name):
        self._maybe_fail()
        self.calls.append(("get_collection_stats", {"name": name}))
        if self.stats_value is not None:
            return dict(self.stats_value)
        return {
            "name": name,
            "count": len(self.points),
            "indexing_queue": 0,
            "disk_usage_bytes": 0,
            "ram_usage_bytes": 0,
            "active_tasks": 0,
            "metric": "lorentz",
        }

    def count(self, filters=None, collection=""):
        self._maybe_fail()
        self.calls.append(("count", {"collection": collection}))
        return self.count_value

    def get_digest(self, collection=""):
        self._maybe_fail()
        self.calls.append(("get_digest", {"collection": collection}))
        return dict(self.digest_value)

    def get_cache_stats(self, name):
        self._maybe_fail()
        self.calls.append(("get_cache_stats", {"name": name}))
        return dict(self.cache_stats_value)

    def list_collections(self):
        self._maybe_fail()
        return [{"name": "test_memory", "metric": "lorentz", "dimension": 129}]

    def vectorize(self, text, metric="l2"):
        self._maybe_fail()
        self.calls.append(("vectorize", {"text": text, "metric": metric}))
        return [1.0] + [0.0] * 128

    def insert(self, id, vector=None, document=None, payload=None, metadata=None,
               typed_metadata=None, collection="", durability=0):
        self._maybe_fail()
        self.calls.append(("insert", {"id": id, "collection": collection,
                                      "durability": durability}))
        self.points[int(id)] = {
            "id": int(id),
            "payload": payload,
            "metadata": dict(metadata or {}),
            "typed_metadata": dict(typed_metadata or {}),
            "distance": 0.0,
        }
        return True

    def get_points(self, ids, collection=""):
        self._maybe_fail()
        self.calls.append(("get_points", {"ids": list(ids), "collection": collection}))
        return [self.points[i] for i in ids if i in self.points]

    def delete(self, id, collection=""):
        self._maybe_fail()
        self.calls.append(("delete", {"id": id, "collection": collection}))
        existed = int(id) in self.points
        self.points.pop(int(id), None)
        return existed

    def search(self, vector=None, query_text=None, top_k=10, filter=None, filters=None,
               hybrid_query=None, hybrid_alpha=None, bm25=None, mrl_dimension=None,
               use_wasserstein=None, collection="", options=None, use_wave=False,
               restart_factor=None, include_payload=None):
        self._maybe_fail()
        self.last_search = {
            "top_k": top_k,
            "collection": collection,
            "include_payload": include_payload,
            "use_wasserstein": use_wasserstein,
            "use_wave": use_wave,
        }
        if self.search_results is not None:
            return list(self.search_results)[:top_k]
        return list(self.points.values())[:top_k]

    def scroll(self, limit, offset=0, filters=None, collection=""):
        self._maybe_fail()
        values = list(self.points.values())
        return values[offset:offset + limit]

    def get_node(self, id, layer=0, collection=""):
        self._maybe_fail()
        return {"id": id, "layer": layer}

    def get_neighbors(self, id, layer=0, limit=64, offset=0, collection=""):
        self._maybe_fail()
        return [{"id": id + 1}][:limit]

    def traverse(self, start_id, max_depth=2, max_nodes=256, layer=0,
                 traversal_mode=0, filter=None, filters=None, collection=""):
        self._maybe_fail()
        return [{"id": start_id, "depth": 0}][:max_nodes]

    def get_subsumption_tree(self, root_id, max_depth=3, collection=""):
        self._maybe_fail()
        return [{"id": root_id, "depth": 0}]

    def get_concept_parents(self, id, layer=0, limit=32, collection=""):
        self._maybe_fail()
        return [{"id": id + 10}][:limit]

    def find_semantic_clusters(self, layer=0, min_cluster_size=3,
                               max_clusters=32, max_nodes=10000, collection=""):
        self._maybe_fail()
        return [[1, 2, 3]][:max_clusters]

    def close(self):
        self.closed = True


@pytest.fixture
def plugin():
    return load_plugin()


@pytest.fixture
def fake_client():
    return FakeClient()


@pytest.fixture
def provider(plugin, fake_client, tmp_path):
    cfg = {
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
        "trust_mode": "annotate_all",
        "allow_collection_override": False,
    }
    p = plugin.HyperspaceDBMemoryProvider(
        cfg, client_factory=lambda **kwargs: fake_client
    )
    p.initialize("test-session")
    yield p
    p.shutdown()
