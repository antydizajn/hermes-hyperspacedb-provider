"""Public HyperspaceDB MemoryProvider plugin for Hermes Agent.

The provider mirrors curated Hermes memory mutations into one explicitly
configured HyperspaceDB collection and exposes bounded retrieval tools. It is
fail-closed: backend failures are never represented as empty search results,
model metadata cannot forge provider-owned fields, and production mutation
semantics are backed by a local identity ledger.
"""

from __future__ import annotations

import hashlib
import hmac
import os

# Process-global gRPC settings required for macOS fork safety and native DNS resolution
os.environ["GRPC_ENABLE_FORK_SUPPORT"] = "0"
os.environ["GRPC_DNS_RESOLVER"] = "native"

from typing import Any

if __package__:
    from ._config import (
        _hermes_home,
        _is_loopback_endpoint,
        _is_trivial_query,
        _load_plugin_config,
        _profile_scope,
        _profile_scope_for_home,
        _resolve_env_reference,
    )
    from ._constants import (
        _ADMIN_CACHE_INT_FIELDS,
        _ADMIN_CACHE_RATE_FIELDS,
        _ADMIN_DIGEST_FIELDS,
        _ADMIN_STATS_FIELDS,
        _CAPABILITY_MAX_ENTRIES,
        _CAPABILITY_TTL_SECONDS,
        _DEFAULT_COLLISION_PROBES,
        _DEFAULT_HOST,
        _DEFAULT_MAX_CONTENT,
        _DEFAULT_MAX_PREFETCH,
        _DEFAULT_MAX_QUERY,
        _DEFAULT_MAX_RESULT_CHARS,
        _DEFAULT_MAX_SEARCH_RESULTS,
        _DEFAULT_MAX_TOOL_OUTPUT,
        _DEFAULT_QUEUE_SIZE,
        _DEFAULT_RPC_TIMEOUT,
        _DEFAULT_TOP_K,
        _INJECTION_PATTERNS,
        _PLUGIN_ID,
        _PREFETCH_OWNED_SOURCES,
        _RESERVED_META,
        _SCHEMA_VERSION,
        _TOOL_ALLOWED_ARGS,
        _TRIVIAL_QUERIES,
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
        BackendAuthError,
        BackendMalformed,
        BackendTimeout,
        BackendUnavailable,
        CapabilityForbidden,
        CollectionForbidden,
        CollectionNotFound,
        CollisionExhausted,
        ConfigurationError,
        DiagnosticUnavailable,
        InvalidArgument,
        MutationConflict,
        MutationVerificationError,
        ProviderError,
        _classify_exception,
        _json_error,
        _safe_error_message,
    )
    from ._ledger import (
        IdentityLedger,
        LedgerRecord,
    )
    from ._provider import (
        HyperspaceDBMemoryProvider,
    )
    from ._rpc import (
        _DeadlineStubProxy,
        _RpcTelemetry,
        _close_client,
        _current_rpc_deadline,
        _install_deadlines,
        _pop_rpc_deadline,
        _push_rpc_deadline,
    )
    from ._security import (
        _candidate_id,
        _looks_like_prompt_injection,
        _sanitize_user_metadata,
    )
    from ._utils import (
        _bounded_float,
        _bounded_int,
        _bounded_tool_json,
        _coerce_bool,
        _decode_payload,
        _extract_content,
        _json,
        _metadata,
        _record_distance,
        _utc_now,
    )
else:
    from _config import (
        _hermes_home,
        _is_loopback_endpoint,
        _is_trivial_query,
        _load_plugin_config,
        _profile_scope,
        _profile_scope_for_home,
        _resolve_env_reference,
    )
    from _constants import (
        _ADMIN_CACHE_INT_FIELDS,
        _ADMIN_CACHE_RATE_FIELDS,
        _ADMIN_DIGEST_FIELDS,
        _ADMIN_STATS_FIELDS,
        _CAPABILITY_MAX_ENTRIES,
        _CAPABILITY_TTL_SECONDS,
        _DEFAULT_COLLISION_PROBES,
        _DEFAULT_HOST,
        _DEFAULT_MAX_CONTENT,
        _DEFAULT_MAX_PREFETCH,
        _DEFAULT_MAX_QUERY,
        _DEFAULT_MAX_RESULT_CHARS,
        _DEFAULT_MAX_SEARCH_RESULTS,
        _DEFAULT_MAX_TOOL_OUTPUT,
        _DEFAULT_QUEUE_SIZE,
        _DEFAULT_RPC_TIMEOUT,
        _DEFAULT_TOP_K,
        _INJECTION_PATTERNS,
        _PLUGIN_ID,
        _PREFETCH_OWNED_SOURCES,
        _RESERVED_META,
        _SCHEMA_VERSION,
        _TOOL_ALLOWED_ARGS,
        _TRIVIAL_QUERIES,
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
        BackendAuthError,
        BackendMalformed,
        BackendTimeout,
        BackendUnavailable,
        CapabilityForbidden,
        CollectionForbidden,
        CollectionNotFound,
        CollisionExhausted,
        ConfigurationError,
        DiagnosticUnavailable,
        InvalidArgument,
        MutationConflict,
        MutationVerificationError,
        ProviderError,
        _classify_exception,
        _json_error,
        _safe_error_message,
    )
    from _ledger import (
        IdentityLedger,
        LedgerRecord,
    )
    from _provider import (
        HyperspaceDBMemoryProvider,
    )
    from _rpc import (
        _DeadlineStubProxy,
        _RpcTelemetry,
        _close_client,
        _current_rpc_deadline,
        _install_deadlines,
        _pop_rpc_deadline,
        _push_rpc_deadline,
    )
    from _security import (
        _candidate_id,
        _looks_like_prompt_injection,
        _sanitize_user_metadata,
    )
    from _utils import (
        _bounded_float,
        _bounded_int,
        _bounded_tool_json,
        _coerce_bool,
        _decode_payload,
        _extract_content,
        _json,
        _metadata,
        _record_distance,
        _utc_now,
    )

__version__ = "2.5.1"

__all__ = [
    "__version__",
    "register",
    "HyperspaceDBMemoryProvider",
    "IdentityLedger",
    "LedgerRecord",
    "ProviderError",
    "ConfigurationError",
    "BackendTimeout",
    "BackendUnavailable",
    "BackendAuthError",
    "CollectionNotFound",
    "BackendMalformed",
    "CollectionForbidden",
    "CapabilityForbidden",
    "InvalidArgument",
    "DiagnosticUnavailable",
    "CollisionExhausted",
    "MutationConflict",
    "MutationVerificationError",
    "HSDB_SEARCH_SCHEMA",
    "HSDB_STORE_SCHEMA",
    "HSDB_STATUS_SCHEMA",
    "HSDB_AUDIT_SCHEMA",
    "HSDB_GRAPH_SCHEMA",
    "HSDB_HIERARCHY_SCHEMA",
    "HSDB_CLUSTERS_SCHEMA",
    "HSDB_SEARCH_ADVANCED_SCHEMA",
    "HSDB_ADMIN_SCHEMA",
    "HSDB_GEOMETRY_SCHEMA",
    "HSDB_EVENTS_SCHEMA",
    "HSDB_RECONCILE_SCHEMA",
    "HSDB_BATCH_SCHEMA",
    "_utc_now",
    "_coerce_bool",
    "_bounded_int",
    "_bounded_float",
    "_json",
    "_safe_error_message",
    "_bounded_tool_json",
    "_json_error",
    "_decode_payload",
    "_extract_content",
    "_candidate_id",
    "_looks_like_prompt_injection",
    "_resolve_env_reference",
    "_is_trivial_query",
    "_load_plugin_config",
    "_hermes_home",
    "_profile_scope_for_home",
    "_profile_scope",
    "_is_loopback_endpoint",
    "_sanitize_user_metadata",
    "_metadata",
    "_record_distance",
    "_classify_exception",
    "_RpcTelemetry",
    "_current_rpc_deadline",
    "_push_rpc_deadline",
    "_pop_rpc_deadline",
    "_DeadlineStubProxy",
    "_install_deadlines",
    "_close_client",
]


def register(ctx: Any) -> None:
    ctx.register_memory_provider("hyperspacedb", HyperspaceDBMemoryProvider)
