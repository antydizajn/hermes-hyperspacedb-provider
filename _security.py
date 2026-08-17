"""Security, provenance, and input sanitization helpers for the HyperspaceDB MemoryProvider plugin."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict

try:
    from ._constants import _INJECTION_PATTERNS, _RESERVED_META
except (ImportError, ValueError):
    from _constants import _INJECTION_PATTERNS, _RESERVED_META


def _candidate_id(full_digest: str, probe: int) -> int:
    material = f"{full_digest}:{probe}".encode("ascii", "strict")
    candidate = int.from_bytes(hashlib.sha256(material).digest()[:4], "big")
    return candidate or 1


def _looks_like_prompt_injection(text: str) -> bool:
    sample = text[:20_000]
    return any(pattern.search(sample) for pattern in _INJECTION_PATTERNS)


def _sanitize_user_metadata(value: Any, max_items: int = 32) -> Dict[str, str]:
    if not isinstance(value, dict):
        return {}
    clean: Dict[str, str] = {}
    for key, item in list(value.items())[:max_items]:
        key_text = str(key).strip()[:80]
        if not key_text or key_text in _RESERVED_META or key_text.startswith("_hs_"):
            continue
        try:
            if isinstance(item, (dict, list, tuple)):
                rendered = json.dumps(item, ensure_ascii=True, sort_keys=True, default=str)
            else:
                rendered = str(item)
        except Exception:
            continue
        clean[f"user.{key_text}"] = rendered[:1_000]
    return clean
