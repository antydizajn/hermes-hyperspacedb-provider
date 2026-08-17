"""Client lifecycle, configuration validation, health checks, and collection contract verification."""

from __future__ import annotations

import hashlib
import json
import logging
import math
import os
from pathlib import Path
import threading
from typing import Any, Callable, Dict, List, Optional, Tuple

if __package__:
    from ._config import (
        _is_loopback_endpoint,
        _resolve_env_reference,
    )
    from ._constants import (
        _DEFAULT_HOST,
        _DEFAULT_RPC_TIMEOUT,
    )
    from ._errors import (
        BackendAuthError,
        BackendMalformed,
        BackendTimeout,
        BackendUnavailable,
        CollectionNotFound,
        ConfigurationError,
        ProviderError,
        _classify_exception,
    )
    from ._rpc import (
        _RpcTelemetry,
        _close_client,
        _install_deadlines,
        _pop_rpc_deadline,
        _push_rpc_deadline,
    )
else:
    from _config import (
        _is_loopback_endpoint,
        _resolve_env_reference,
    )
    from _constants import (
        _DEFAULT_HOST,
        _DEFAULT_RPC_TIMEOUT,
    )
    from _errors import (
        BackendAuthError,
        BackendMalformed,
        BackendTimeout,
        BackendUnavailable,
        CollectionNotFound,
        ConfigurationError,
        ProviderError,
        _classify_exception,
    )
    from _rpc import (
        _RpcTelemetry,
        _close_client,
        _install_deadlines,
        _pop_rpc_deadline,
        _push_rpc_deadline,
    )

logger = logging.getLogger("hermes.plugins.memory.hyperspacedb.lifecycle")


def extract_collection_contract_fields(details: Any) -> Tuple[str, Any]:
    """Extract one unambiguous metric/dimension pair from SDK collection data."""
    if not isinstance(details, dict):
        return "", None
    schema = details.get("schema")
    if isinstance(schema, dict):
        components = schema.get("components")
        if isinstance(components, list):
            if len(components) > 1:
                return "", None
            if len(components) == 1 and isinstance(components[0], dict):
                comp = components[0]
                return str(comp.get("metric") or "").strip().lower(), comp.get("full_dimension", comp.get("dimension"))
    metric = str(details.get("metric") or "").strip().lower()
    dimension = details.get("dimension", details.get("dimensions"))
    return metric, dimension


def infer_contract_from_vector(vector: Any) -> Tuple[str, Any]:
    """Infer Lorentz spacetime invariants and dimension from a stored vector point."""
    if not isinstance(vector, (list, tuple)) or not vector:
        return "", None
    if any(
        isinstance(val, bool)
        or not isinstance(val, (int, float))
        or not math.isfinite(float(val))
        for val in vector
    ):
        return "", None
    dimension = len(vector)
    if dimension != 129 or float(vector[0]) <= 0.0:
        return "", dimension
    lorentz = [float(val) for val in vector]
    spatial_norm_sq = math.fsum(val * val for val in lorentz[1:])
    time_sq = lorentz[0] * lorentz[0]
    residual = abs(time_sq - spatial_norm_sq - 1.0)
    invariant_scale = max(1.0, abs(time_sq), abs(spatial_norm_sq))
    if residual <= 1e-2 * invariant_scale:
        return "lorentz", dimension
    return "", dimension
