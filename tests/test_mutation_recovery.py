import pytest
import threading


def _pending_record(provider, fake_client):
    provider.on_memory_write("add", "memory", "recoverable delete fact")
    assert provider.flush_writes(timeout=2.0)
    record = provider._ledger.resolve(provider._profile_scope, "memory", "recoverable delete")[0]
    provider._ledger.set_status(record.digest, "delete_pending", "simulated lost response")
    return record


def test_reconciliation_deletes_only_authenticated_pending_record(provider, fake_client):
    record = _pending_record(provider, fake_client)
    result = provider.reconcile_delete_pending(limit=1)
    assert result == {"attempted": 1, "removed": 1, "conflicts": 0, "deferred": 0}
    assert record.external_id not in fake_client.points
    assert provider._ledger.get(record.digest).status == "removed"


def test_reconciliation_conflicts_without_authenticated_ownership(provider, fake_client):
    record = _pending_record(provider, fake_client)
    fake_client.points[record.external_id]["metadata"] = {"_hs_owner": "forged"}
    result = provider.reconcile_delete_pending(limit=1)
    assert result == {"attempted": 1, "removed": 0, "conflicts": 1, "deferred": 0}
    assert record.external_id in fake_client.points
    assert provider._ledger.get(record.digest).status == "conflict"


def test_reconciliation_accepts_confirmed_remote_absence(provider, fake_client):
    record = _pending_record(provider, fake_client)
    fake_client.points.pop(record.external_id)
    result = provider.reconcile_delete_pending(limit=1)
    assert result == {"attempted": 1, "removed": 1, "conflicts": 0, "deferred": 0}
    assert provider._ledger.get(record.digest).status == "removed"


def test_reconciliation_is_disabled_without_signing_key(provider, fake_client):
    record = _pending_record(provider, fake_client)
    provider._ownership_hmac_key = b""
    result = provider.reconcile_delete_pending(limit=1)
    assert result == {"attempted": 0, "removed": 0, "conflicts": 0, "deferred": 0}
    assert record.external_id in fake_client.points
    assert provider._ledger.get(record.digest).status == "delete_pending"


def test_insert_timeout_recovers_existing_signed_record_without_reinsert(provider, fake_client):
    original = fake_client.insert

    def insert_then_timeout(*args, **kwargs):
        original(*args, **kwargs)
        raise TimeoutError("response lost after server insert")

    fake_client.insert = insert_then_timeout
    with pytest.raises(Exception) as raised:
        provider._store_content_sync(
            target="memory", source="hermes-builtin-memory", trust="builtin-curated",
            content="recoverable insert fact",
        )
    assert getattr(raised.value, "code", None) == "BACKEND_TIMEOUT"
    records = provider._ledger.records_with_status("retry_pending", 1)
    assert len(records) == 1
    fake_client.insert = original
    before = len(fake_client.calls)
    result = provider.reconcile_pending_inserts(limit=1)
    assert result == {"attempted": 1, "active": 1, "conflicts": 0, "deferred": 0}
    assert provider._ledger.get(records[0].digest).status == "active"
    assert not any(name == "insert" for name, _ in fake_client.calls[before:])


def test_replace_failure_restores_old_record_to_active(plugin, provider, fake_client):
    provider._apply_memory_event("add", "memory", "old replace state", None)
    old = provider._ledger.resolve(provider._profile_scope, "memory", "old replace")[0]
    fake_client.fail = TimeoutError("backend unavailable during replacement")
    with pytest.raises(plugin.BackendTimeout):
        provider._apply_memory_event("replace", "memory", "new replace state", {"old_text": "old replace"})
    fake_client.fail = None
    assert provider._ledger.get(old.digest).status == "active"
    assert old.external_id in fake_client.points


class _RotatingClient:
    def __init__(self, number):
        self.number = number
        self.closed = 0

    def close(self):
        self.closed += 1


def test_rotation_defers_old_client_close_until_inflight_release(provider):
    created = []

    def factory(**kwargs):
        client = _RotatingClient(len(created))
        created.append(client)
        return client

    provider._client_factory = factory
    provider._config["api_key"] = "first-test-key"
    first = provider._get_client()
    provider._config["api_key"] = "second-test-key"
    second = provider._get_client()
    assert first is not second
    assert first.closed == 0
    provider._release_client(second)
    provider._release_client(first)
    assert first.closed == 1
    assert second.closed == 0


def test_shutdown_defers_inflight_client_close(provider):
    created = []

    def factory(**kwargs):
        client = _RotatingClient(len(created))
        created.append(client)
        return client

    provider._client_factory = factory
    client = provider._get_client()
    provider.shutdown()
    assert provider._last_error_code == "SHUTDOWN_INFLIGHT"
    assert client.closed == 0
    provider._release_client(client)
    assert client.closed == 1


class _BlockingClient(_RotatingClient):
    def __init__(self, number):
        super().__init__(number)
        self.started = threading.Event()
        self.release = threading.Event()

    def search(self, **kwargs):
        self.started.set()
        assert self.release.wait(2.0)
        return []


def test_rotation_during_blocked_rpc_defers_close(provider):
    created = []

    def factory(**kwargs):
        client = _BlockingClient(len(created))
        created.append(client)
        return client

    provider._client_factory = factory
    provider._config["api_key"] = "first-test-key"
    outcome = []
    thread = threading.Thread(target=lambda: outcome.append(provider._call("search")), daemon=True)
    thread.start()
    assert created[0].started.wait(1.0)
    provider._config["api_key"] = "second-test-key"
    second = provider._get_client()
    assert second is not created[0]
    assert created[0].closed == 0
    created[0].release.set()
    thread.join(2.0)
    assert not thread.is_alive()
    assert outcome == [[]]
    assert created[0].closed == 1
    provider._release_client(second)


def test_shutdown_during_blocked_rpc_defers_close(provider):
    created = []

    def factory(**kwargs):
        client = _BlockingClient(len(created))
        created.append(client)
        return client

    provider._client_factory = factory
    provider._config["api_key"] = "blocking-shutdown-key"
    outcome = []
    thread = threading.Thread(target=lambda: outcome.append(provider._call("search")), daemon=True)
    thread.start()
    assert created[0].started.wait(1.0)
    provider.shutdown()
    assert provider._last_error_code == "SHUTDOWN_INFLIGHT"
    assert created[0].closed == 0
    created[0].release.set()
    thread.join(2.0)
    assert not thread.is_alive()
    assert outcome == [[]]
    assert created[0].closed == 1


def test_reconciliation_persists_backoff_and_attempt_cap(provider, fake_client):
    record = _pending_record(provider, "retry persistence fact")
    fake_client.fail = TimeoutError("temporary backend failure")
    first = provider.reconcile_delete_pending(limit=1)
    assert first == {"attempted": 1, "removed": 0, "conflicts": 0, "deferred": 1}
    row = provider._ledger._db.execute("SELECT attempts, next_retry_epoch FROM reconciliation_retries WHERE digest=?", (record.digest,)).fetchone()
    assert row[0] == 1
    assert row[1] > 0
    second = provider.reconcile_delete_pending(limit=1)
    assert second == {"attempted": 0, "removed": 0, "conflicts": 0, "deferred": 1}
