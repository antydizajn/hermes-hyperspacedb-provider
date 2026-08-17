"""Error classes and error classification for the HyperspaceDB MemoryProvider plugin."""

from __future__ import annotations

import json
import re
from typing import Any


class ProviderError(RuntimeError):
    code = "PROVIDER_ERROR"


class ConfigurationError(ProviderError):
    code = "CONFIGURATION_ERROR"


class BackendTimeout(ProviderError):
    code = "BACKEND_TIMEOUT"


class BackendUnavailable(ProviderError):
    code = "BACKEND_UNAVAILABLE"


class BackendAuthError(ProviderError):
    code = "AUTH_ERROR"


class CollectionNotFound(ProviderError):
    code = "COLLECTION_NOT_FOUND"


class BackendMalformed(ProviderError):
    code = "MALFORMED_RESULT"


class CollectionForbidden(ProviderError):
    code = "COLLECTION_FORBIDDEN"


class CapabilityForbidden(ProviderError):
    code = "CAPABILITY_FORBIDDEN"


class InvalidArgument(ProviderError):
    code = "INVALID_ARGUMENT"


class DiagnosticUnavailable(ProviderError):
    code = "DIAGNOSTIC_UNAVAILABLE"


class CollisionExhausted(ProviderError):
    code = "ID_COLLISION_EXHAUSTED"


class MutationConflict(ProviderError):
    code = "MUTATION_CONFLICT"


class MutationVerificationError(ProviderError):
    code = "MUTATION_VERIFICATION_FAILED"


def _safe_error_message(value: Any) -> str:
    message = str(value)
    patterns = (
        r"(?i)(authorization\s*:\s*bearer\s+)[^\s,;]+",
        r"(?i)((?:api[_-]?key|token|password|secret)\s*[:=]\s*)[^\s,;]+",
    )
    for pattern in patterns:
        message = re.sub(pattern, lambda match: f"{match.group(1)}[REDACTED]", message)
    return message[:1_000]


def _json_error(code: str, message: str) -> str:
    return json.dumps(
        {"ok": False, "error": {"code": code, "message": _safe_error_message(message)}},
        ensure_ascii=True,
        sort_keys=True,
        default=str,
    )


def _classify_exception(exc: BaseException) -> ProviderError:
    if isinstance(exc, ProviderError):
        return exc
    if isinstance(exc, TimeoutError):
        return BackendTimeout("HyperspaceDB RPC deadline exceeded")
    code_name = ""
    try:
        code = exc.code()  # type: ignore[union-attr]
        code_name = getattr(code, "name", str(code)).upper()
    except Exception:
        code_name = ""
    text = str(exc).lower()
    if "DEADLINE_EXCEEDED" in code_name or "timeout" in text or "timed out" in text:
        return BackendTimeout("HyperspaceDB RPC deadline exceeded")
    if "UNAUTHENTICATED" in code_name or "PERMISSION_DENIED" in code_name or "unauth" in text:
        return BackendAuthError(
            "HyperspaceDB rejected credentials (UNAUTHENTICATED/PERMISSION_DENIED). "
            "Note: local HyperspaceDB containers running without authentication expect an empty API key (api_key: \"\" or unset api_key_env); "
            "remote/cloud servers require a valid HYPERSPACE_API_KEY."
        )
    if "NOT_FOUND" in code_name or "not found" in text:
        return CollectionNotFound("Configured HyperspaceDB collection was not found")
    if "UNAVAILABLE" in code_name or "connection" in text or "refused" in text:
        return BackendUnavailable("HyperspaceDB is unavailable")
    return BackendUnavailable(f"HyperspaceDB call failed ({exc.__class__.__name__})")
