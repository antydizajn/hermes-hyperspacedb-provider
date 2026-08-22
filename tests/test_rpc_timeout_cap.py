"""Regression: rpc_timeout upper bound must honor values above 60s.

Live E2E (2026-08-22) measured vectorize at 16-40s on a large production
collection plus two get_points verification round-trips (3.5-7.1s each).
The previous hard clamp of 60.0s turned a configured rpc_timeout of 120
into silent effective 60, producing BACKEND_TIMEOUT retry_pending records.
"""

from pathlib import Path
import sys
import types

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _load_provider_module(monkeypatch, config):
    """Import _provider with an isolated stub hyperspace package."""
    stub_pkg = types.ModuleType("hyperspace")
    stub_client = type("HyperspaceClient", (), {"__init__": lambda self, *a, **k: None})
    stub_exception = type("HyperspaceException", (Exception,), {})
    stub_pkg.HyperspaceClient = stub_client  # type: ignore[attr-defined]
    stub_pkg.HyperspaceException = stub_exception  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "hyperspace", stub_pkg)
    if "hermes_hyperspacedb_plugin_under_test" in sys.modules:
        del sys.modules["hermes_hyperspacedb_plugin_under_test"]
    for name in ["_provider"]:
        sys.modules.pop(name, None)
    sys.path.insert(0, str(ROOT))
    try:
        import _provider  # noqa: PLC0415

        return _provider.HyperspaceDBMemoryProvider(config)
    finally:
        sys.path.remove(str(ROOT))


def test_rpc_timeout_accepts_120_seconds(monkeypatch):
    provider = _load_provider_module(monkeypatch, {
        "collection": "c",
        "host": "127.0.0.1:50051",
        "rpc_timeout": 120.0,
    })
    assert provider._rpc_timeout == pytest.approx(120.0)


def test_rpc_timeout_default_unchanged(monkeypatch):
    provider = _load_provider_module(monkeypatch, {
        "collection": "c",
        "host": "127.0.0.1:50051",
    })
    assert provider._rpc_timeout == pytest.approx(4.0)


def test_rpc_timeout_still_rejects_absurd_values(monkeypatch):
    provider = _load_provider_module(monkeypatch, {
        "collection": "c",
        "host": "127.0.0.1:50051",
        "rpc_timeout": 10_000.0,
    })
    assert provider._rpc_timeout <= 300.0


def test_write_rpc_timeout_scales_from_raised_base(monkeypatch):
    provider = _load_provider_module(monkeypatch, {
        "collection": "c",
        "host": "127.0.0.1:50051",
        "rpc_timeout": 120.0,
    })
    # Short content: write timeout must be at least the raised base.
    assert provider._write_rpc_timeout("x") >= 120.0
