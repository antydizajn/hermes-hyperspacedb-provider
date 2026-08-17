"""RPC proxy, telemetry, and deadline management for HyperspaceDB gRPC stubs."""

from __future__ import annotations

import logging
import threading
from typing import Any, Optional

logger = logging.getLogger(__name__)

_rpc_deadline_local = threading.local()


class _RpcTelemetry:
    """Per-client, per-thread record of RPC errors hidden by an SDK wrapper."""

    def __init__(self) -> None:
        self._local = threading.local()

    def reset(self) -> None:
        self._local.errors = []

    def record(self, exc: Exception) -> None:
        errors = getattr(self._local, "errors", None)
        if errors is None:
            errors = []
            self._local.errors = errors
        errors.append(exc)

    def consume(self) -> Optional[Exception]:
        errors = list(getattr(self._local, "errors", []) or [])
        self.reset()
        return errors[0] if errors else None


def _current_rpc_deadline(default: float) -> float:
    value = getattr(_rpc_deadline_local, "timeout", None)
    return float(default if value is None else value)


def _push_rpc_deadline(timeout: float) -> Any:
    previous = getattr(_rpc_deadline_local, "timeout", None)
    _rpc_deadline_local.timeout = float(timeout)
    return previous


def _pop_rpc_deadline(previous: Any) -> None:
    if previous is None:
        if hasattr(_rpc_deadline_local, "timeout"):
            delattr(_rpc_deadline_local, "timeout")
        return
    _rpc_deadline_local.timeout = previous


class _DeadlineStubProxy:
    """Inject per-call deadlines and retain RPC failures swallowed by an SDK wrapper."""

    def __init__(self, stub: Any, timeout: float, telemetry: Optional[_RpcTelemetry] = None):
        self._stub = stub
        self._timeout = timeout
        self._telemetry = telemetry or _RpcTelemetry()

    def __getattr__(self, name: str) -> Any:
        value = getattr(self._stub, name)
        if not callable(value):
            return value

        def call(*args: Any, **kwargs: Any) -> Any:
            kwargs.setdefault("timeout", _current_rpc_deadline(self._timeout))
            try:
                return value(*args, **kwargs)
            except Exception as exc:
                self._telemetry.record(exc)
                raise

        return call


def _install_deadlines(client: Any, timeout: float) -> _RpcTelemetry:
    """Wrap stubs once. Per-call deadline lives in thread-local, not shared stubs."""
    telemetry = getattr(client, "_hermes_hyperspace_rpc_telemetry", None)
    if not isinstance(telemetry, _RpcTelemetry):
        telemetry = _RpcTelemetry()
        setattr(client, "_hermes_hyperspace_rpc_telemetry", telemetry)
    stubs = getattr(client, "stubs", None)
    if isinstance(stubs, list):
        if stubs and all(isinstance(stub, _DeadlineStubProxy) for stub in stubs):
            return telemetry
        client.stubs = [
            _DeadlineStubProxy(
                stub._stub if isinstance(stub, _DeadlineStubProxy) else stub,
                timeout,
                telemetry,
            )
            for stub in stubs
        ]
        thread_local = getattr(client, "_thread_local", None)
        if thread_local is not None and hasattr(thread_local, "stub"):
            try:
                delattr(thread_local, "stub")
            except Exception:
                pass
    return telemetry


def _close_client(client: Any) -> None:
    if client is None:
        return
    close_method = getattr(client, "close", None)
    if callable(close_method):
        try:
            close_method()
        except Exception:
            logger.debug("HyperspaceDB client close() failed", exc_info=True)
    seen = set()
    for channel in list(getattr(client, "channels", []) or []) + [getattr(client, "channel", None)]:
        if channel is None or id(channel) in seen:
            continue
        seen.add(id(channel))
        try:
            channel.close()
        except Exception:
            logger.debug("HyperspaceDB channel close failed", exc_info=True)
