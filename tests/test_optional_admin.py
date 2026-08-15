import json


def test_admin_read_only_operations_allowlist_backend_output(provider, fake_client):
    fake_client.count_value = 17
    fake_client.digest_value = {
        "logical_clock": 8,
        "state_hash": 0xDEADBEEF,
        "count": 17,
        "private": "must not escape",
    }
    fake_client.cache_stats_value = {
        "l1_size": 4,
        "l2_index_size": 7,
        "l1_hit_rate": 0.25,
        "l2_hit_rate": 0.5,
        "tombstone_count": 1,
        "pending_rebuild": 2,
        "estimated_memory_bytes": 4096,
        "private": "must not escape",
    }
    fake_client.stats_value = {
        "count": 17,
        "indexing_queue": 3,
        "disk_usage_bytes": 2048,
        "ram_usage_bytes": 1024,
        "active_tasks": 1,
        "schema": {"private": "must not escape"},
        "private": "must not escape",
    }

    outputs = {
        operation: json.loads(provider.handle_tool_call("hyperspace_admin", {"operation": operation}))
        for operation in ("stats", "count", "digest", "cache_stats")
    }

    assert outputs["stats"] == {
        "ok": True,
        "result": {
            "count": 17,
            "indexing_queue": 3,
            "disk_usage_bytes": 2048,
            "ram_usage_bytes": 1024,
            "active_tasks": 1,
        },
    }
    assert outputs["count"] == {"ok": True, "result": {"count": 17}}
    assert outputs["digest"] == {
        "ok": True,
        "result": {"logical_clock": 8, "state_hash": 0xDEADBEEF, "count": 17},
    }
    assert outputs["cache_stats"] == {
        "ok": True,
        "result": {
            "l1_size": 4,
            "l2_index_size": 7,
            "l1_hit_rate": 0.25,
            "l2_hit_rate": 0.5,
            "tombstone_count": 1,
            "pending_rebuild": 2,
            "estimated_memory_bytes": 4096,
        },
    }
    assert all("must not escape" not in json.dumps(output) for output in outputs.values())
    assert fake_client.calls[-4:] == [
        ("get_collection_stats", {"name": "test_memory"}),
        ("count", {"collection": "test_memory"}),
        ("get_digest", {"collection": "test_memory"}),
        ("get_cache_stats", {"name": "test_memory"}),
    ]


def test_admin_rejects_malformed_read_only_responses(provider, fake_client):
    fake_client.count_value = -1
    count = json.loads(provider.handle_tool_call("hyperspace_admin", {"operation": "count"}))
    fake_client.count_value = 1
    fake_client.digest_value = {"logical_clock": "bad", "state_hash": "short", "count": 1}
    digest = json.loads(provider.handle_tool_call("hyperspace_admin", {"operation": "digest"}))
    fake_client.cache_stats_value = {"l1_size": 1, "l2_index_size": 1}
    cache = json.loads(provider.handle_tool_call("hyperspace_admin", {"operation": "cache_stats"}))

    for response in (count, digest, cache):
        assert response["ok"] is False
        assert response["error"]["code"] == "MALFORMED_RESULT"


def test_status_sanitizes_stats_and_fails_closed_on_malformed_stats(provider, fake_client):
    fake_client.stats_value = {
        "count": 3,
        "indexing_queue": 0,
        "disk_usage_bytes": 10,
        "ram_usage_bytes": 11,
        "active_tasks": 1,
        "secret": "must not escape",
    }
    healthy = json.loads(provider.handle_tool_call("hyperspace_status", {}))
    assert healthy["ok"] is True
    assert healthy["stats"] == {
        "count": 3,
        "indexing_queue": 0,
        "disk_usage_bytes": 10,
        "ram_usage_bytes": 11,
        "active_tasks": 1,
    }
    assert "must not escape" not in json.dumps(healthy)

    fake_client.stats_value = {"count": 3}
    malformed = json.loads(provider.handle_tool_call("hyperspace_status", {}))
    assert malformed["ok"] is False
    assert malformed["error"]["code"] == "MALFORMED_RESULT"


def test_admin_schema_excludes_mutating_operations_and_includes_only_read_only_additions(plugin):
    schema = plugin.HSDB_ADMIN_SCHEMA["parameters"]["properties"]["operation"]
    assert schema["enum"] == ["health", "stats", "count", "digest", "cache_stats"]


def test_admin_rejects_destructive_and_unknown_operations(provider, fake_client):
    before = list(fake_client.calls)
    for operation in ("vacuum", "delete_collection", "rebuild_index", "forget", "drop"):
        raw = json.loads(provider.handle_tool_call("hyperspace_admin", {"operation": operation}))
        assert raw.get("ok") is False
        assert raw.get("error", {}).get("code") in {"INVALID_ARGUMENT", "CONFIGURATION_ERROR"}
    after = list(fake_client.calls)
    assert after == before
