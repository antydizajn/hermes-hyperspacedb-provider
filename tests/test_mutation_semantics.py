import json


def active_rows(provider, target=None):
    return provider._ledger.active_records(target=target)


def test_add_namespaces_target_and_verifies_write(provider, fake_client):
    provider.on_memory_write("add", "memory", "same text")
    provider.on_memory_write("add", "user", "same text")
    assert provider.flush_writes(timeout=2.0)
    rows = active_rows(provider)
    assert len(rows) == 2
    assert rows[0]["external_id"] != rows[1]["external_id"]
    assert len(fake_client.points) == 2
    for point in fake_client.points.values():
        assert point["metadata"]["_hs_owner"] == "hermes-hyperspacedb"
        assert len(point["metadata"]["_hs_digest"]) == 64


def test_replace_removes_old_record_without_zombie(provider, fake_client):
    provider.on_memory_write("add", "memory", "the old durable fact")
    assert provider.flush_writes(timeout=2.0)
    old_id = active_rows(provider, "memory")[0]["external_id"]
    provider.on_memory_write("replace", "memory", "the new durable fact", {"old_text": "old durable"})
    assert provider.flush_writes(timeout=2.0)
    rows = active_rows(provider, "memory")
    assert len(rows) == 1
    assert rows[0]["content"] == "the new durable fact"
    assert old_id not in fake_client.points
    assert rows[0]["external_id"] in fake_client.points


def test_remove_deletes_exact_record(provider, fake_client):
    provider.on_memory_write("add", "memory", "unique removable fact")
    assert provider.flush_writes(timeout=2.0)
    record_id = active_rows(provider, "memory")[0]["external_id"]
    provider.on_memory_write("remove", "memory", "", {"old_text": "removable"})
    assert provider.flush_writes(timeout=2.0)
    assert active_rows(provider, "memory") == []
    assert record_id not in fake_client.points


def test_ambiguous_old_text_fails_closed(provider, fake_client):
    provider.on_memory_write("add", "memory", "alpha shared suffix")
    provider.on_memory_write("add", "memory", "beta shared suffix")
    assert provider.flush_writes(timeout=2.0)
    before = set(fake_client.points)
    provider.on_memory_write("remove", "memory", "", {"old_text": "shared suffix"})
    assert provider.flush_writes(timeout=2.0)
    assert set(fake_client.points) == before
    assert provider.status_snapshot()["failed_writes"] >= 1


def test_uint32_collision_never_overwrites_foreign_point(provider, fake_client, plugin):
    digest = provider._logical_digest("memory", "hermes-builtin-memory", "collision fact")
    first = plugin._candidate_id(digest, 0)
    fake_client.points[first] = {
        "id": first,
        "payload": b"foreign",
        "metadata": {"_hs_owner": "another-provider", "_hs_digest": "foreign"},
        "distance": 0.0,
    }
    provider.on_memory_write("add", "memory", "collision fact")
    assert provider.flush_writes(timeout=2.0)
    row = active_rows(provider, "memory")[0]
    assert row["external_id"] != first
    assert fake_client.points[first]["payload"] == b"foreign"


def test_store_tool_is_idempotent_and_returns_capability_identity(provider, fake_client):
    first = json.loads(provider.handle_tool_call("hyperspace_store", {"content": "tool fact"}))
    second = json.loads(provider.handle_tool_call("hyperspace_store", {"content": "tool fact"}))
    assert first["ok"] is True
    assert second["ok"] is True
    assert "record_id" not in first
    assert first["handle"] == second["handle"]
    assert len(fake_client.points) == 1
