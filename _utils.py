"""Utility and serialization helpers for the HyperspaceDB MemoryProvider plugin."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _coerce_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    return default


def _bounded_int(value: Any, default: int, low: int, high: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(low, min(parsed, high))


def _bounded_float(value: Any, default: float, low: float, high: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        parsed = default
    return max(low, min(parsed, high))


def _json(data: Dict[str, Any]) -> str:
    return json.dumps(data, ensure_ascii=True, sort_keys=True, default=str)


def _bounded_tool_json(data: Dict[str, Any], max_chars: int) -> str:
    """Serialize a bounded tool response without cutting a JSON token stream."""
    budget = max(2, int(max_chars))
    rendered = _json(data)
    if len(rendered) <= budget:
        return rendered
    candidates: list[Dict[str, Any]] = []
    if isinstance(data, dict):
        summary: Dict[str, Any] = {"ok": bool(data.get("ok", False)), "output_truncated": True}
        result = data.get("result")
        if isinstance(result, (list, tuple, set, dict)):
            summary["result_count"] = len(result)
        candidates.append(summary)
    candidates.extend([{"output_truncated": True}, {}])
    for candidate in candidates:
        rendered = _json(candidate)
        if len(rendered) <= budget:
            return rendered
    return "{}"


def _decode_payload(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    if isinstance(value, bytearray):
        return bytes(value).decode("utf-8", "replace")
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        for key in ("content", "text", "document", "body"):
            if value.get(key):
                return _decode_payload(value[key])
    return ""


def _extract_content(raw: Dict[str, Any]) -> str:
    """Extract canonical text, preferring the SDK sidecar payload."""
    if not isinstance(raw, dict):
        return ""
    payload = _decode_payload(raw.get("payload"))
    if payload:
        return payload
    for key in ("content", "document", "text", "body"):
        value = _decode_payload(raw.get(key))
        if value:
            return value
    metadata = raw.get("metadata") or {}
    if isinstance(metadata, dict):
        for key in ("_content", "content", "text", "document", "body"):
            value = _decode_payload(metadata.get(key))
            if value:
                return value
    return ""


def _metadata(raw: Dict[str, Any]) -> Dict[str, Any]:
    value = raw.get("metadata") if isinstance(raw, dict) else None
    return value if isinstance(value, dict) else {}


def _record_distance(raw: Dict[str, Any]) -> Optional[float]:
    if not isinstance(raw, dict):
        return None
    val = raw.get("distance")
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError, AttributeError):
        return None
