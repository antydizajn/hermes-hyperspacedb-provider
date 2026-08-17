"""Tool definitions, argument validation, and dispatching for HyperspaceDB."""

from __future__ import annotations

import math
from typing import Any, Callable, Dict, List, Optional, Sequence

if __package__:
    from ._constants import (
        _ADMIN_CACHE_INT_FIELDS,
        _ADMIN_CACHE_RATE_FIELDS,
        _ADMIN_DIGEST_FIELDS,
        _ADMIN_STATS_FIELDS,
        _TOOL_ALLOWED_ARGS,
        HSDB_ADMIN_SCHEMA,
        HSDB_AUDIT_SCHEMA,
        HSDB_BATCH_SCHEMA,
        HSDB_CLUSTERS_SCHEMA,
        HSDB_EVENTS_SCHEMA,
        HSDB_GEOMETRY_SCHEMA,
        HSDB_GRAPH_SCHEMA,
        HSDB_HIERARCHY_SCHEMA,
        HSDB_RECONCILE_SCHEMA,
        HSDB_SEARCH_ADVANCED_SCHEMA,
        HSDB_SEARCH_SCHEMA,
        HSDB_STATUS_SCHEMA,
        HSDB_STORE_SCHEMA,
    )
    from ._errors import (
        BackendMalformed,
        CapabilityForbidden,
        CollectionForbidden,
        ConfigurationError,
        DiagnosticUnavailable,
        InvalidArgument,
        ProviderError,
        _json_error,
    )
    from ._geometry import geometry_norm
    from ._graph import sanitize_graph_result
    from ._retrieval import format_bounded_record, search_records
    from ._security import _looks_like_prompt_injection
    from ._utils import (
        _bounded_int,
        _bounded_tool_json,
        _extract_content,
        _metadata,
    )
else:
    from _constants import (
        _ADMIN_CACHE_INT_FIELDS,
        _ADMIN_CACHE_RATE_FIELDS,
        _ADMIN_DIGEST_FIELDS,
        _ADMIN_STATS_FIELDS,
        _TOOL_ALLOWED_ARGS,
        HSDB_ADMIN_SCHEMA,
        HSDB_AUDIT_SCHEMA,
        HSDB_BATCH_SCHEMA,
        HSDB_CLUSTERS_SCHEMA,
        HSDB_EVENTS_SCHEMA,
        HSDB_GEOMETRY_SCHEMA,
        HSDB_GRAPH_SCHEMA,
        HSDB_HIERARCHY_SCHEMA,
        HSDB_RECONCILE_SCHEMA,
        HSDB_SEARCH_ADVANCED_SCHEMA,
        HSDB_SEARCH_SCHEMA,
        HSDB_STATUS_SCHEMA,
        HSDB_STORE_SCHEMA,
    )
    from _errors import (
        BackendMalformed,
        CapabilityForbidden,
        CollectionForbidden,
        ConfigurationError,
        DiagnosticUnavailable,
        InvalidArgument,
        ProviderError,
        _json_error,
    )
    from _geometry import geometry_norm
    from _graph import sanitize_graph_result
    from _retrieval import format_bounded_record, search_records
    from _security import _looks_like_prompt_injection
    from _utils import (
        _bounded_int,
        _bounded_tool_json,
        _extract_content,
        _metadata,
    )


def sanitize_admin_int_map(raw: Any, fields: Sequence[str]) -> Dict[str, int]:
    if not isinstance(raw, dict):
        raise BackendMalformed("Admin RPC returned a non-object response")
    sanitized: Dict[str, int] = {}
    for field in fields:
        val = raw.get(field)
        if isinstance(val, bool) or not isinstance(val, int) or val < 0:
            raise BackendMalformed(f"Admin RPC returned invalid {field}")
        sanitized[field] = val
    return sanitized


def sanitize_admin_cache_stats(raw: Any) -> Dict[str, Any]:
    sanitized: Dict[str, Any] = sanitize_admin_int_map(raw, _ADMIN_CACHE_INT_FIELDS)
    for field in _ADMIN_CACHE_RATE_FIELDS:
        val = raw.get(field) if isinstance(raw, dict) else None
        if isinstance(val, bool) or not isinstance(val, (int, float)):
            raise BackendMalformed(f"Admin RPC returned invalid {field}")
        parsed = float(val)
        if not math.isfinite(parsed) or not 0.0 <= parsed <= 1.0:
            raise BackendMalformed(f"Admin RPC returned invalid {field}")
        sanitized[field] = parsed
    return sanitized


def get_all_tool_schemas(
    event_observation_enabled: bool = False,
    operator_reconcile_enabled: bool = False,
    batch_mutation_enabled: bool = False,
) -> List[Dict[str, Any]]:
    schemas = [
        HSDB_SEARCH_SCHEMA,
        HSDB_STORE_SCHEMA,
        HSDB_STATUS_SCHEMA,
        HSDB_AUDIT_SCHEMA,
        HSDB_GRAPH_SCHEMA,
        HSDB_HIERARCHY_SCHEMA,
        HSDB_CLUSTERS_SCHEMA,
        HSDB_SEARCH_ADVANCED_SCHEMA,
        HSDB_ADMIN_SCHEMA,
        HSDB_GEOMETRY_SCHEMA,
    ]
    if event_observation_enabled:
        schemas.append(HSDB_EVENTS_SCHEMA)
    if operator_reconcile_enabled:
        schemas.append(HSDB_RECONCILE_SCHEMA)
    if batch_mutation_enabled:
        schemas.append(HSDB_BATCH_SCHEMA)
    return schemas
