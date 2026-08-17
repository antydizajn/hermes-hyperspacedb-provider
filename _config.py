"""Configuration loading and profile scoping for the HyperspaceDB MemoryProvider plugin."""

from __future__ import annotations

import hashlib
import ipaddress
import os
from pathlib import Path
import re
from typing import Any, Dict

try:
    from ._constants import _TRIVIAL_QUERIES
except (ImportError, ValueError):
    from _constants import _TRIVIAL_QUERIES


def _resolve_env_reference(value: Any, expected_env_name: str) -> str:
    """Resolve an exact configured environment-variable reference, or preserve a literal."""
    raw = str(value or "").strip()
    expected = str(expected_env_name or "").strip()
    if raw and raw == expected:
        return os.environ.get(expected, "")
    return raw


def _is_trivial_query(query: str) -> bool:
    normalized = re.sub(r"[^a-z0-9 ]+", "", query.lower()).strip()
    return normalized in _TRIVIAL_QUERIES or len(normalized) < 2


def _load_plugin_config() -> Dict[str, Any]:
    try:
        from hermes_cli.config import load_config

        config = load_config()
        memory = config.get("memory", {}) if isinstance(config, dict) else {}
        value = memory.get("hyperspacedb", {}) if isinstance(memory, dict) else {}
        return dict(value) if isinstance(value, dict) else {}
    except Exception:
        return {}


def _hermes_home() -> Path:
    try:
        from hermes_constants import get_hermes_home

        return Path(get_hermes_home()).expanduser().resolve()
    except Exception:
        configured = os.environ.get("HERMES_HOME")
        if configured:
            return Path(configured).expanduser().resolve()
        return Path.home() / ".hermes"


def _profile_scope_for_home(hermes_home: Path) -> str:
    raw = str(hermes_home.expanduser().resolve()).encode("utf-8", "replace")
    return hashlib.sha256(raw).hexdigest()[:16]


def _profile_scope() -> str:
    return _profile_scope_for_home(_hermes_home())


def _is_loopback_endpoint(host: str) -> bool:
    raw = host.strip()
    if raw.startswith("unix:"):
        return True
    if "://" in raw:
        raw = raw.split("://", 1)[1]
    if raw.startswith("["):
        address = raw[1:].split("]", 1)[0]
    else:
        address = raw.rsplit(":", 1)[0] if ":" in raw else raw
    if address.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(address).is_loopback
    except ValueError:
        return False
