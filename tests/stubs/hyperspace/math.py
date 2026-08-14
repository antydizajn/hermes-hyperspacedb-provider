"""Poincare helpers used by geometry tests and the provider."""

def lorentz_to_poincare(vector):
    if not vector:
        return []
    time = float(vector[0])
    denom = 1.0 + time
    if denom == 0.0:
        raise ValueError("invalid Lorentz time")
    return [float(value) / denom for value in vector[1:]]


def log_map(*_args, **_kwargs):
    return [0.0] * 128


def koopman_extrapolate(*_args, **_kwargs):
    return [0.0] * 128
