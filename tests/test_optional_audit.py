import json


def test_audit_reports_scoped_aggregates_without_memory_content(plugin, provider, fake_client):
    provider._ledger.upsert(
        plugin.LedgerRecord(
            digest="a" * 64,
            external_id=101,
            profile_scope=provider._profile_scope,
            target="memory",
            source="hermes-builtin-memory",
            content="private ledger content must not escape",
            status="active",
            error="private error must not escape",
            updated_at="2026-08-13T09:20:00+00:00",
        )
    )
    provider._ledger.upsert(
        plugin.LedgerRecord(
            digest="b" * 64,
            external_id=102,
            profile_scope=provider._profile_scope,
            target="memory",
            source="hermes-builtin-memory",
            content="another private record",
            status="delete_pending",
            error="",
            updated_at="2026-08-13T09:20:00+00:00",
        )
    )
    provider._ledger.record_failure(
        provider._profile_scope,
        "add",
        "memory",
        "private failed payload",
        "",
        "TEST_FAILURE",
        "private failure detail",
    )
    provider._ledger.upsert(
        plugin.LedgerRecord(
            digest="c" * 64,
            external_id=103,
            profile_scope="other-profile",
            target="memory",
            source="hermes-builtin-memory",
            content="foreign private record",
            status="active",
            error="",
            updated_at="2026-08-13T09:20:00+00:00",
        )
    )
    provider._ledger.record_failure(
        "other-profile",
        "add",
        "memory",
        "foreign failed payload",
        "",
        "FOREIGN_FAILURE",
        "foreign error detail",
    )

    before_calls = list(fake_client.calls)
    response = json.loads(provider.handle_tool_call("hyperspace_audit", {"operation": "summary"}))

    assert response["ok"] is True
    assert fake_client.calls == before_calls
    result = response["result"]
    assert result["records_by_status"] == {"active": 1, "delete_pending": 1}
    assert result["reconciliation_backlog"] == 1
    assert result["failure_count"] == 1
    assert result["failure_codes"] == {"TEST_FAILURE": 1}
    rendered = json.dumps(response)
    for forbidden in (
        "private ledger content must not escape",
        "another private record",
        "private error must not escape",
        "private failed payload",
        "private failure detail",
        "foreign private record",
        "foreign failed payload",
        "foreign error detail",
        "FOREIGN_FAILURE",
        "a" * 64,
        "b" * 64,
        "101",
        "102",
    ):
        assert forbidden not in rendered
