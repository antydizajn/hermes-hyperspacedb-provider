import os
from pathlib import Path

import pytest


def test_real_hyperspace_sdk_is_not_the_repo_stub():
    if os.environ.get("HSDB_REQUIRE_REAL_SDK") != "1":
        pytest.skip("set HSDB_REQUIRE_REAL_SDK=1 to prove a real hyperspacedb install")
    import hyperspace
    from hyperspace.math import lorentz_to_poincare

    module_path = Path(hyperspace.__file__).resolve()
    assert "tests/stubs" not in module_path.as_posix()
    ball = lorentz_to_poincare([1.0] + [0.0] * 128)
    assert len(ball) == 128
    assert all(abs(value) < 1.0 for value in ball)
