"""Point capability and handle management for HyperspaceDB MemoryProvider."""

from __future__ import annotations

import secrets
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

if __package__:
    from ._constants import _CAPABILITY_MAX_ENTRIES, _CAPABILITY_TTL_SECONDS
    from ._errors import CapabilityForbidden, ConfigurationError
else:
    from _constants import _CAPABILITY_MAX_ENTRIES, _CAPABILITY_TTL_SECONDS
    from _errors import CapabilityForbidden, ConfigurationError


class CapabilityStore:
    """Manages session-scoped, unforgeable point capability handles (hsdbh_*)."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._capabilities: Dict[str, Tuple[int, str, str, str, float]] = {}

    def clear(self) -> None:
        with self._lock:
            self._capabilities.clear()

    def mint(
        self,
        raw_id: Any,
        collection: str,
        profile_scope: str,
        session_id: str,
    ) -> Optional[str]:
        """Mint an in-memory, session-scoped capability for one backend point slot."""
        if isinstance(raw_id, bool) or not isinstance(raw_id, int) or not (1 <= raw_id <= 0xFFFFFFFF):
            return None
        selected_collection = str(collection).strip()
        if not selected_collection:
            return None
        now = time.monotonic()
        with self._lock:
            expired = [
                h for h, val in self._capabilities.items()
                if val[4] <= now
            ]
            for h in expired:
                self._capabilities.pop(h, None)

            for existing_handle, val in self._capabilities.items():
                point_id, scope, sess, cap_coll, expires_at = val
                if (
                    point_id == raw_id
                    and scope == profile_scope
                    and sess == session_id
                    and cap_coll == selected_collection
                    and expires_at > now
                ):
                    return existing_handle

            while len(self._capabilities) >= _CAPABILITY_MAX_ENTRIES:
                oldest = min(self._capabilities, key=lambda h: self._capabilities[h][4])
                self._capabilities.pop(oldest, None)

            handle = "hsdbh_" + secrets.token_urlsafe(24)
            self._capabilities[handle] = (
                raw_id,
                profile_scope,
                session_id,
                selected_collection,
                now + _CAPABILITY_TTL_SECONDS,
            )
            return handle

    def resolve_many(
        self,
        handles: Any,
        collection: str,
        profile_scope: str,
        session_id: str,
    ) -> List[int]:
        """Resolve only capabilities minted by this live provider session."""
        if not isinstance(handles, list) or not (1 <= len(handles) <= 16):
            raise ConfigurationError("handles must contain 1 to 16 unique capability handles")
        if any(not isinstance(h, str) or not h for h in handles):
            raise ConfigurationError("handles must contain 1 to 16 unique capability handles")
        if len(set(handles)) != len(handles):
            raise ConfigurationError("handles must contain 1 to 16 unique capability handles")

        now = time.monotonic()
        point_ids: List[int] = []
        with self._lock:
            for h in handles:
                val = self._capabilities.get(h)
                if val is None:
                    raise CapabilityForbidden("Capability was not issued by this provider session")
                point_id, scope, sess, cap_coll, expires_at = val
                if (
                    expires_at <= now
                    or scope != profile_scope
                    or sess != session_id
                    or cap_coll != collection
                ):
                    self._capabilities.pop(h, None)
                    raise CapabilityForbidden("Capability is expired or out of scope")
                point_ids.append(point_id)
        return point_ids

    def resolve_one(
        self,
        handle: Any,
        collection: str,
        profile_scope: str,
        session_id: str,
    ) -> int:
        if not isinstance(handle, str) or not handle:
            raise ConfigurationError("handle must be a capability handle issued by this provider session")
        return self.resolve_many([handle], collection, profile_scope, session_id)[0]
