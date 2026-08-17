"""Mutation lifecycle, cryptographic ownership HMACs, and reconciliation for HyperspaceDB."""

from __future__ import annotations

import hashlib
import hmac
import logging
import time
from typing import Any, Dict, List, Optional, Tuple

if __package__:
    from ._constants import (
        _PLUGIN_ID,
        _PREFETCH_OWNED_SOURCES,
        _SCHEMA_VERSION,
    )
    from ._errors import (
        BackendMalformed,
        CollisionExhausted,
        ConfigurationError,
        MutationConflict,
        MutationVerificationError,
        ProviderError,
    )
    from ._ledger import (
        IdentityLedger,
        LedgerRecord,
    )
    from ._security import (
        _candidate_id,
        _sanitize_user_metadata,
    )
    from ._utils import (
        _bounded_int,
        _extract_content,
        _metadata,
        _utc_now,
    )
else:
    from _constants import (
        _PLUGIN_ID,
        _PREFETCH_OWNED_SOURCES,
        _SCHEMA_VERSION,
    )
    from _errors import (
        BackendMalformed,
        CollisionExhausted,
        ConfigurationError,
        MutationConflict,
        MutationVerificationError,
        ProviderError,
    )
    from _ledger import (
        IdentityLedger,
        LedgerRecord,
    )
    from _security import (
        _candidate_id,
        _sanitize_user_metadata,
    )
    from _utils import (
        _bounded_int,
        _extract_content,
        _metadata,
        _utc_now,
    )

logger = logging.getLogger("hermes.plugins.memory.hyperspacedb.mutations")


def logical_digest(collection: str, profile_scope: str, target: str, source: str, content: str) -> str:
    parts = [
        _PLUGIN_ID, _SCHEMA_VERSION, collection, profile_scope,
        str(target), str(source), str(content),
    ]
    return hashlib.sha256("\0".join(parts).encode("utf-8", "replace")).hexdigest()


def ownership_signature(metadata: Dict[str, Any], signing_key: bytes) -> str:
    if not signing_key:
        raise ConfigurationError("ownership_hmac_key is required for authenticated writes")
    fields = ("_hs_owner", "_hs_profile", "target", "source", "_hs_digest")
    payload = "\x1f".join(str(metadata.get(field, "")) for field in fields).encode("utf-8")
    return hmac.new(signing_key, payload, hashlib.sha256).hexdigest()


def point_owner_matches(
    point: Dict[str, Any],
    digest: str,
    collection: str,
    profile_scope: str,
    ownership_hmac_key: bytes,
    previous_ownership_hmac_keys: Tuple[bytes, ...],
) -> bool:
    meta = _metadata(point)
    if meta.get("_hs_owner") != _PLUGIN_ID or meta.get("_hs_digest") != digest:
        return False
    if meta.get("_hs_profile") != profile_scope:
        return False
    target = str(meta.get("target") or "")
    source = str(meta.get("source") or "")
    content = _extract_content(point)
    if not target or not source or not content:
        return False
    expected_digest = logical_digest(collection, profile_scope, target, source, content)
    if not hmac.compare_digest(str(meta.get("_hs_digest") or ""), expected_digest):
        return False
    supplied = str(meta.get("_hs_owner_signature") or "")
    keys = (ownership_hmac_key, *previous_ownership_hmac_keys)
    for key in keys:
        if not key:
            continue
        expected = ownership_signature(meta, key)
        if supplied and hmac.compare_digest(supplied, expected):
            return True
    return False


def internal_metadata(
    target: str,
    source: str,
    trust: str,
    content: str,
    digest: str,
    collection: str,
    profile_scope: str,
    ownership_hmac_key: bytes,
    user_metadata: Optional[Dict[str, Any]],
) -> Dict[str, str]:
    metadata = _sanitize_user_metadata(user_metadata)
    metadata.update({
        "_hs_owner": _PLUGIN_ID,
        "_hs_digest": digest,
        "_hs_profile": profile_scope,
        "_hs_schema": _SCHEMA_VERSION,
        "_content": content,
        "source": source,
        "trust": trust,
        "target": target,
        "ts": _utc_now(),
    })
    metadata["_hs_owner_signature"] = ownership_signature(metadata, ownership_hmac_key)
    return metadata
