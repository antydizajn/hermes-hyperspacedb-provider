"""Graph traversal, hierarchy, cluster discovery, and result sanitization for HyperspaceDB."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

if __package__:
    from ._errors import (
        BackendMalformed,
        CapabilityForbidden,
        CollectionForbidden,
        ConfigurationError,
        InvalidArgument,
        ProviderError,
        _json_error,
    )
    from ._security import _looks_like_prompt_injection
    from ._utils import (
        _bounded_int,
        _bounded_tool_json,
        _extract_content,
        _metadata,
    )
else:
    from _errors import (
        BackendMalformed,
        CapabilityForbidden,
        CollectionForbidden,
        ConfigurationError,
        InvalidArgument,
        ProviderError,
        _json_error,
    )
    from _security import _looks_like_prompt_injection
    from _utils import (
        _bounded_int,
        _bounded_tool_json,
        _extract_content,
        _metadata,
    )


def sanitize_graph_result(value: Any, collection: str, capability_mint_fn: Any) -> Any:
    """Replace backend graph slots in an SDK response with scoped capabilities and filter raw internals."""
    if isinstance(value, list):
        return [sanitize_graph_result(item, collection, capability_mint_fn) for item in value]
    if not isinstance(value, dict):
        return value
    blocked_raw_keys = {
        "vector", "vectors", "embedding", "embeddings", "raw_vector",
        "point_id", "raw_point_id", "_hs_digest", "_hs_owner", "_hs_signature"
    }
    sanitized: Dict[str, Any] = {}
    id_key_to_handle_key = {
        "id": "handle",
        "start_id": "start_handle",
        "root_id": "root_handle",
        "node_id": "node_handle",
        "parent_id": "parent_handle",
        "child_id": "child_handle",
    }
    id_list_key_to_handle_key = {
        "ids": "handles",
        "node_ids": "node_handles",
        "parent_ids": "parent_handles",
        "child_ids": "child_handles",
        "neighbors": "neighbor_handles",
    }
    for key, item in value.items():
        normalized_key = str(key)
        if normalized_key in blocked_raw_keys:
            continue
        if normalized_key in id_key_to_handle_key:
            handle = capability_mint_fn(item, collection)
            if handle is not None:
                sanitized[id_key_to_handle_key[normalized_key]] = handle
            continue
        if normalized_key in id_list_key_to_handle_key and isinstance(item, list):
            handles = [capability_mint_fn(raw_id, collection) for raw_id in item]
            sanitized[id_list_key_to_handle_key[normalized_key]] = [
                h for h in handles if h is not None
            ]
            continue
        sanitized[normalized_key] = sanitize_graph_result(item, collection, capability_mint_fn)
    return sanitized
