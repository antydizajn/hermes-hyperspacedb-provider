"""Lorentz 129D geometry diagnostics and Poincaré transformations for HyperspaceDB."""

from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Sequence

if __package__:
    from ._errors import (
        BackendMalformed,
        ConfigurationError,
        DiagnosticUnavailable,
        InvalidArgument,
        ProviderError,
        _json_error,
    )
    from ._utils import _bounded_tool_json
else:
    from _errors import (
        BackendMalformed,
        ConfigurationError,
        DiagnosticUnavailable,
        InvalidArgument,
        ProviderError,
        _json_error,
    )
    from _utils import _bounded_tool_json


def geometry_norm(vector: Sequence[float]) -> float:
    return math.sqrt(sum(val * val for val in vector))


def lorentz_vector_for_geometry(point: Any) -> List[float]:
    if not isinstance(point, dict):
        raise BackendMalformed("Geometry point was not returned as a dictionary")
    vector = point.get("vector")
    if not isinstance(vector, (list, tuple)) or len(vector) != 129:
        raise BackendMalformed("Geometry point vector must have dimension 129")
    if any(
        isinstance(val, bool)
        or not isinstance(val, (int, float))
        or not math.isfinite(float(val))
        for val in vector
    ):
        raise BackendMalformed("Geometry point vector contains non-finite values")
    lorentz = [float(val) for val in vector]
    if lorentz[0] <= 0.0:
        raise BackendMalformed("Lorentz time component must be strictly positive")
    spatial_norm_sq = math.fsum(val * val for val in lorentz[1:])
    time_sq = lorentz[0] * lorentz[0]
    residual = abs(time_sq - spatial_norm_sq - 1.0)
    invariant_scale = max(1.0, abs(time_sq), abs(spatial_norm_sq))
    if residual > 1e-2 * invariant_scale:
        raise BackendMalformed("Geometry point violates the Lorentz hyperboloid constraint")
    return lorentz
