"""Retrieval, hybrid search, ledger fallback merge, and prefetch formatting for HyperspaceDB."""

from __future__ import annotations

import hashlib
from typing import Any, Dict, List, Optional, Set, Tuple

if __package__:
    from ._constants import (
        _PREFETCH_OWNED_SOURCES,
        _DEFAULT_MAX_QUERY,
        _DEFAULT_MAX_SEARCH_RESULTS,
        _DEFAULT_MAX_RESULT_CHARS,
        _DEFAULT_TOP_K,
    )
    from ._errors import (
        BackendMalformed,
        ConfigurationError,
    )
    from ._security import _looks_like_prompt_injection
    from ._utils import (
        _bounded_float,
        _bounded_int,
        _coerce_bool,
        _extract_content,
        _metadata,
        _record_distance,
    )
else:
    from _constants import (
        _PREFETCH_OWNED_SOURCES,
        _DEFAULT_MAX_QUERY,
        _DEFAULT_MAX_SEARCH_RESULTS,
        _DEFAULT_MAX_RESULT_CHARS,
        _DEFAULT_TOP_K,
    )
    from _errors import (
        BackendMalformed,
        ConfigurationError,
    )
    from _security import _looks_like_prompt_injection
    from _utils import (
        _bounded_float,
        _bounded_int,
        _coerce_bool,
        _extract_content,
        _metadata,
        _record_distance,
    )


def format_bounded_record(
    record: Dict[str, Any],
    max_result_chars: int,
    capability_mint_fn: Optional[Any] = None,
    collection: Optional[str] = None,
) -> Dict[str, Any]:
    content = record["content"]
    truncated = len(content) > max_result_chars
    if record.get("quarantined"):
        rendered = "[QUARANTINED: suspected instruction-like memory content]"
    else:
        rendered = content[:max_result_chars]
    result: Dict[str, Any] = {
        "content": rendered,
        "distance": record.get("distance"),
        "source": record.get("source"),
        "trust": record.get("trust"),
        "target": record.get("target"),
        "timestamp": record.get("timestamp"),
        "quarantined": bool(record.get("quarantined")),
        "truncated": truncated,
    }
    if capability_mint_fn is not None and collection is not None:
        handle = capability_mint_fn(record.get("id"), collection)
        if handle:
            result["handle"] = handle
    return result


def search_records(
    provider: Any,
    query: str,
    limit: int,
    *,
    mode: str = "standard",
    collection: Optional[str] = None,
) -> List[Dict[str, Any]]:
    provider._require_collection_contract()
    selected_collection = str(collection or provider._collection).strip()
    if not selected_collection:
        raise ConfigurationError("collection is required")
    clean_query = str(query).strip()
    if not clean_query:
        raise ConfigurationError("query is required")
    clean_query = clean_query[: provider._max_query_chars]
    bounded = _bounded_int(limit, provider._top_k, 1, provider._max_search_results)
    vector = provider._call("vectorize", clean_query, metric=provider._metric)
    if not isinstance(vector, (list, tuple)) or not vector:
        raise BackendMalformed("Vectorize RPC returned no vector")
    kwargs: Dict[str, Any] = {
        "vector": list(vector),
        "top_k": bounded,
        "collection": selected_collection,
        "include_payload": True,
    }
    if mode == "wasserstein":
        kwargs["use_wasserstein"] = True
    elif mode == "wave":
        kwargs["use_wave"] = True
    elif _coerce_bool(provider._config.get("hybrid_search"), True):
        kwargs["hybrid_query"] = clean_query
        kwargs["hybrid_alpha"] = _bounded_float(
            provider._config.get("hybrid_alpha"), 0.7, 0.0, 1.0
        )
    results = provider._call("search", **kwargs)
    if not isinstance(results, list):
        raise BackendMalformed("Search RPC returned a non-list response")
    normalized: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    for raw in results:
        if not isinstance(raw, dict):
            continue
        content = _extract_content(raw).strip()
        if not content:
            continue
        distance = _record_distance(raw)
        if provider._max_distance is not None and (distance is None or distance > provider._max_distance):
            continue
        digest = hashlib.sha256(content.encode("utf-8", "replace")).hexdigest()
        if digest in seen:
            continue
        seen.add(digest)
        meta = _metadata(raw)
        source = str(meta.get("source") or "unknown")[:200]
        trust = str(meta.get("trust") or "unknown")[:100]
        authenticated_owner = provider._point_owner_matches(raw, str(meta.get("_hs_digest") or ""))
        if provider._trust_mode == "owned_only":
            allowed = authenticated_owner and source in _PREFETCH_OWNED_SOURCES
        elif provider._trust_mode == "annotate_all":
            allowed = distance is not None and (provider._max_distance is None or distance <= provider._max_distance)
        else:
            raise ConfigurationError("trust_mode must be owned_only or annotate_all")
        normalized.append({
            "id": raw.get("id"),
            "content": content,
            "distance": distance,
            "source": source,
            "trust": trust,
            "target": str(meta.get("target") or "unknown")[:100],
            "timestamp": str(meta.get("ts") or meta.get("timestamp") or "")[:100],
            "allowed_for_prefetch": allowed,
            "quarantined": _looks_like_prompt_injection(content),
        })
    normalized.sort(key=lambda item: (
        item["distance"] is None,
        item["distance"] if item["distance"] is not None else float("inf"),
        str(item["id"]),
    ))
    extras = ledger_substring_records(provider, clean_query, seen, bounded)
    return (normalized + extras)[:bounded]


def ledger_substring_records(
    provider: Any,
    query: str,
    seen: Set[str],
    limit: int,
) -> List[Dict[str, Any]]:
    """Surface our own fresh writes when the vector index has not caught up."""
    if provider._ledger is None or limit <= 0:
        return []
    needle = str(query or "").strip().casefold()
    if len(needle) < 4:
        return []
    extras: List[Dict[str, Any]] = []
    for row in provider._ledger.active_records(profile_scope=provider._profile_scope):
        content = str(row.get("content") or "").strip()
        if not content or needle not in content.casefold():
            continue
        digest = hashlib.sha256(content.encode("utf-8", "replace")).hexdigest()
        if digest in seen:
            continue
        seen.add(digest)
        source = str(row.get("source") or "ledger")[:200]
        if provider._trust_mode == "owned_only":
            allowed = source in _PREFETCH_OWNED_SOURCES
        elif provider._trust_mode == "annotate_all":
            allowed = False
        else:
            raise ConfigurationError("trust_mode must be owned_only or annotate_all")
        extras.append({
            "id": row.get("external_id"),
            "content": content,
            "distance": None,
            "source": source,
            "trust": "owned" if source in _PREFETCH_OWNED_SOURCES else "model-authored",
            "target": str(row.get("target") or "memory")[:100],
            "timestamp": str(row.get("updated_at") or "")[:100],
            "allowed_for_prefetch": allowed,
            "quarantined": _looks_like_prompt_injection(content),
        })
    return extras
