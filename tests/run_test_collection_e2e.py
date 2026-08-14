"""Operator-authorized E2E against a dedicated HyperspaceDB test collection.

Required environment:
- HSDB_E2E_WRITE_APPROVED: literal `approved` acknowledgment for this dedicated target
- HSDB_TEST_OWNERSHIP_HMAC_KEY: non-production ownership HMAC key
- HSDB_E2E_STATE_PATH: explicit temporary local ledger location outside this plugin directory
- HSDB_TEST_COLLECTION: dedicated target collection prefixed `hsdb_e2e_`

The script never deletes a collection and never reads another collection. It
seeds bounded synthetic fixtures, searches, and verifies provider
add/replace/remove only inside the dedicated target. No fixture content or
secret is printed.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import sys
import time
from pathlib import Path

from hyperspace import HyperspaceClient

ROOT = Path(__file__).resolve().parents[1]
E2E_RPC_TIMEOUT_SECONDS = 60.0
E2E_FLUSH_TIMEOUT_SECONDS = 180.0
E2E_FIXTURES = (
    "hyperspacedb e2e synthetic fixture alpha 20260813",
    "hyperspacedb e2e synthetic fixture beta 20260813",
)


def require_external_state_path(value: str) -> str:
    """Return an absolute ledger path outside PLUGIN_ROOT without creating it."""
    raw = str(value).strip()
    if not raw:
        raise SystemExit("HSDB_E2E_STATE_PATH is required; do not write E2E state into this plugin")
    candidate = Path(raw).expanduser()
    if not candidate.is_absolute():
        raise SystemExit("HSDB_E2E_STATE_PATH must be an absolute path outside this plugin")
    resolved = candidate.resolve(strict=False)
    try:
        resolved.relative_to(ROOT.resolve())
    except ValueError:
        return str(resolved)
    raise SystemExit("HSDB_E2E_STATE_PATH must resolve outside this plugin")


def load_provider_module():
    name = "hsdb_plugin_e2e"
    spec = importlib.util.spec_from_file_location(name, ROOT / "__init__.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def candidate_id(material: str, probe: int) -> int:
    digest = hashlib.sha256(f"{material}:{probe}".encode("utf-8", "replace")).digest()
    return int.from_bytes(digest[:4], "big") or 1


def close_client(client) -> None:
    seen = set()
    for channel in list(getattr(client, "channels", []) or []) + [getattr(client, "channel", None)]:
        if channel is None or id(channel) in seen:
            continue
        seen.add(id(channel))
        channel.close()


def seed_target_fixtures(client, target: str) -> tuple[str, int]:
    """Idempotently seed synthetic test records into target only."""
    seeded = 0
    for text in E2E_FIXTURES:
        digest = hashlib.sha256(text.encode("utf-8", "replace")).hexdigest()
        record_id = None
        for probe in range(64):
            proposed = candidate_id(f"fixture:{text}", probe)
            existing = client.get_points([proposed], collection=target)
            if not existing:
                record_id = proposed
                break
            metadata = existing[0].get("metadata") or {}
            if metadata.get("fixture_digest") == digest:
                record_id = proposed
                break
        if record_id is None:
            raise RuntimeError("Synthetic fixture uint32 allocation exhausted")
        existing = client.get_points([record_id], collection=target)
        if existing:
            seeded += 1
            continue
        vector = client.vectorize(text, metric="lorentz")
        ok = client.insert(
            record_id,
            vector=vector,
            document=text,
            payload=text.encode("utf-8", "replace"),
            metadata={
                "source": "operator-authorized-test-fixture",
                "trust": "test-fixture",
                "fixture_digest": digest,
            },
            collection=target,
            durability=3,
        )
        if ok is not True:
            raise RuntimeError("Synthetic fixture insert did not return True")
        seeded += 1
    if seeded != len(E2E_FIXTURES):
        raise RuntimeError("Synthetic fixture seed count mismatch")
    return E2E_FIXTURES[0], seeded


def main() -> None:
    approval = os.environ.get("HSDB_E2E_WRITE_APPROVED", "").strip().lower()
    if approval != "approved":
        raise SystemExit("Set HSDB_E2E_WRITE_APPROVED=approved before any isolated test write")
    ownership_key = os.environ.get("HSDB_TEST_OWNERSHIP_HMAC_KEY", "").strip()
    if not ownership_key:
        raise SystemExit("HSDB_TEST_OWNERSHIP_HMAC_KEY is required for authenticated E2E writes")
    state_path = require_external_state_path(os.environ.get("HSDB_E2E_STATE_PATH", ""))
    target = os.environ.get("HSDB_TEST_COLLECTION", "").strip()
    if not target:
        raise SystemExit("HSDB_TEST_COLLECTION is required")
    if not target.startswith("hsdb_e2e_"):
        raise SystemExit("HSDB_TEST_COLLECTION must use the hsdb_e2e_ prefix")

    host = os.environ.get("HYPERSPACE_HOST", "127.0.0.1:50051")
    key = os.environ.get("HYPERSPACE_API_KEY") or None
    user_id = os.environ.get("HYPERSPACE_USER_ID") or None
    client = HyperspaceClient(host=host, api_key=key, user_id=user_id, pool_size=2)
    summary = {
        "target_created": False,
        "fixtures_seeded": 0,
        "payload_search_verified": False,
        "add_verified": False,
        "replace_verified": False,
        "remove_verified": False,
    }
    try:
        collections = client.list_collections()
        names = {str(item.get("name")) for item in collections if isinstance(item, dict)}
        if target not in names:
            ok = client.create_collection(target, dimension=129, metric="lorentz")
            if ok is not True:
                raise RuntimeError("Test collection creation did not return True")
            summary["target_created"] = True

        first_query, summary["fixtures_seeded"] = seed_target_fixtures(client, target)

        module = load_provider_module()
        provider = module.HyperspaceDBMemoryProvider({
            "host": host,
            "collection": target,
            "metric": "lorentz",
            "expected_dimension": 129,
            "api_key_env": "HYPERSPACE_API_KEY",
            "user_id_env": "HYPERSPACE_USER_ID",
            "ownership_hmac_key_env": "HSDB_TEST_OWNERSHIP_HMAC_KEY",
            "state_path": state_path,
            "profile_scope": "e2e-test-scope",
            "trust_mode": "annotate_all",
            "rpc_timeout": E2E_RPC_TIMEOUT_SECONDS,
            "top_k": 5,
            "auto_store": True,
        })
        provider.initialize("e2e-test-session")
        try:
            search = json.loads(provider.handle_tool_call(
                "hyperspace_search", {"query": first_query, "limit": 5}
            ))
            summary["payload_search_verified"] = bool(
                search.get("ok") and search.get("results")
                and any(item.get("content") for item in search["results"])
            )

            nonce = hashlib.sha256(f"{time.time_ns()}".encode()).hexdigest()[:16]
            old = f"provider e2e old fact {nonce}"
            new = f"provider e2e new fact {nonce}"
            provider.on_memory_write("add", "memory", old)
            if not provider.flush_writes(timeout=E2E_FLUSH_TIMEOUT_SECONDS):
                raise RuntimeError("Add queue did not drain")
            rows = [record for record in provider._ledger.active_records("memory") if nonce in record["content"]]
            old_external_id = rows[0]["external_id"] if len(rows) == 1 else None
            summary["add_verified"] = (
                old_external_id is not None
                and bool(client.get_points([old_external_id], collection=target))
            )

            provider.on_memory_write(
                "replace", "memory", new, {"old_text": f"old fact {nonce}"}
            )
            if not provider.flush_writes(timeout=E2E_FLUSH_TIMEOUT_SECONDS):
                raise RuntimeError("Replace queue did not drain")
            rows = [record for record in provider._ledger.active_records("memory") if nonce in record["content"]]
            replacement_external_id = rows[0]["external_id"] if len(rows) == 1 else None
            summary["replace_verified"] = (
                replacement_external_id is not None
                and rows[0]["content"] == new
                and bool(client.get_points([replacement_external_id], collection=target))
                and old_external_id is not None
                and not client.get_points([old_external_id], collection=target)
            )

            provider.on_memory_write(
                "remove", "memory", "", {"old_text": f"new fact {nonce}"}
            )
            if not provider.flush_writes(timeout=E2E_FLUSH_TIMEOUT_SECONDS):
                raise RuntimeError("Remove queue did not drain")
            rows = [record for record in provider._ledger.active_records("memory") if nonce in record["content"]]
            summary["remove_verified"] = (
                rows == []
                and replacement_external_id is not None
                and not client.get_points([replacement_external_id], collection=target)
            )
            summary["worker_clean"] = provider.status_snapshot()["failed_writes"] == 0
        finally:
            provider.shutdown()

        required = [
            "payload_search_verified", "add_verified", "replace_verified", "remove_verified", "worker_clean",
        ]
        if not all(summary[key] for key in required):
            raise RuntimeError(f"E2E verification failed: {summary}")
        print(json.dumps({"ok": True, **summary}, sort_keys=True))
    finally:
        close_client(client)


if __name__ == "__main__":
    main()
