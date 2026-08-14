import json
import math


def _lorentz_point(ball_coordinate):
    radius_squared = ball_coordinate * ball_coordinate
    denominator = 1.0 - radius_squared
    return [
        (1.0 + radius_squared) / denominator,
        2.0 * ball_coordinate / denominator,
    ] + [0.0] * 127


def _issued_handles(provider, fake_client, coordinates, *, start_id=41):
    handles = []
    for offset, coordinate in enumerate(coordinates, start=start_id):
        fake_client.points[offset] = {
            "id": offset,
            "vector": _lorentz_point(coordinate),
            "metadata": {},
            "payload": b"",
        }
        handle = provider._mint_point_capability(offset, provider._collection)
        assert handle is not None
        handles.append(handle)
    return handles


def _geometry(provider, **args):
    return json.loads(provider.handle_tool_call("hyperspace_geometry", args))


def test_geometry_schema_and_capability_only_diagnostics(provider, fake_client, plugin):
    handles = _issued_handles(provider, fake_client, [0.05, 0.10, 0.15])
    schema = plugin.HSDB_GEOMETRY_SCHEMA["parameters"]["properties"]
    assert schema["operation"]["enum"] == ["predict_relation", "predict_momentum", "trust_score"]
    assert "ids" not in schema
    assert "collection" not in schema

    relation = _geometry(provider, operation="predict_relation", handles=handles[:2])
    momentum = _geometry(provider, operation="predict_momentum", handles=handles[:2], steps=1.25)
    unavailable_before = list(fake_client.calls)
    unavailable = _geometry(provider, operation="trust_score", handles=handles)
    assert unavailable["ok"] is False
    assert unavailable["error"]["code"] == "DIAGNOSTIC_UNAVAILABLE"
    assert fake_client.calls == unavailable_before

    forged = _geometry(provider, operation="trust_score", handles=["forged", *handles[1:]])
    missing = _geometry(provider, operation="trust_score", handles=[])
    assert forged["error"]["code"] == "CAPABILITY_FORBIDDEN"
    assert missing["error"]["code"] == "INVALID_ARGUMENT"
    assert fake_client.calls == unavailable_before

    assert relation["result"]["dimension"] == 128
    assert math.isfinite(relation["result"]["l2_norm"])
    assert momentum["result"]["dimension"] == 128
    assert math.isfinite(momentum["result"]["l2_norm"])


def test_geometry_rejects_raw_ids_and_invalid_inputs_before_point_read(provider, fake_client):
    handles = _issued_handles(provider, fake_client, [0.05, 0.10, 0.15])
    before = list(fake_client.calls)

    raw_ids = _geometry(provider, operation="predict_relation", ids=[41, 42])
    forged = _geometry(provider, operation="predict_relation", handles=["forged", handles[0]])
    wrong_arity = _geometry(provider, operation="predict_relation", handles=[handles[0]])
    nonfinite_steps = _geometry(provider, operation="predict_momentum", handles=handles[:2], steps=float("nan"))

    for response in (raw_ids, forged, wrong_arity, nonfinite_steps):
        assert response["ok"] is False
    assert raw_ids["error"]["code"] == "INVALID_ARGUMENT"
    assert forged["error"]["code"] == "CAPABILITY_FORBIDDEN"
    assert wrong_arity["error"]["code"] == "INVALID_ARGUMENT"
    assert nonfinite_steps["error"]["code"] == "INVALID_ARGUMENT"
    assert fake_client.calls == before


def test_geometry_fails_closed_on_missing_or_malformed_lorentz_points(provider, fake_client):
    missing_a = provider._mint_point_capability(998, provider._collection)
    missing_b = provider._mint_point_capability(999, provider._collection)
    assert missing_a is not None and missing_b is not None
    missing = _geometry(provider, operation="predict_relation", handles=[missing_a, missing_b])
    assert missing["ok"] is False
    assert missing["error"]["code"] == "MALFORMED_RESULT"

    handles = _issued_handles(provider, fake_client, [0.05, 0.10])
    fake_client.points[42]["vector"] = [float("nan")] * 129
    malformed = _geometry(provider, operation="predict_relation", handles=handles)
    assert malformed["ok"] is False
    assert malformed["error"]["code"] == "MALFORMED_RESULT"


def test_geometry_rejects_boolean_backend_ids_and_boolean_math_outputs(provider, fake_client, monkeypatch):
    handles = _issued_handles(provider, fake_client, [0.05, 0.10])
    fake_client.points[41]["id"] = True
    boolean_id = _geometry(provider, operation="predict_relation", handles=handles)
    assert boolean_id["ok"] is False
    assert boolean_id["error"]["code"] == "MALFORMED_RESULT"

    fake_client.points[41]["id"] = 41
    import hyperspace.math as math_module

    monkeypatch.setattr(math_module, "log_map", lambda *_args: [0.0] * 129)
    wrong_dimension = _geometry(provider, operation="predict_relation", handles=handles)
    assert wrong_dimension["ok"] is False
    assert wrong_dimension["error"]["code"] == "MALFORMED_RESULT"

    monkeypatch.setattr(math_module, "log_map", lambda *_args: [float("nan")] * 128)
    nonfinite_relation = _geometry(provider, operation="predict_relation", handles=handles)
    assert nonfinite_relation["ok"] is False
    assert nonfinite_relation["error"]["code"] == "MALFORMED_RESULT"

    monkeypatch.setattr(math_module, "koopman_extrapolate", lambda *_args: [False] * 128)
    boolean_momentum = _geometry(provider, operation="predict_momentum", handles=handles)
    assert boolean_momentum["ok"] is False
    assert boolean_momentum["error"]["code"] == "MALFORMED_RESULT"


def test_geometry_accepts_valid_near_boundary_lorentz_point(provider, fake_client):
    handles = _issued_handles(provider, fake_client, [0.999999, 0.999998], start_id=141)
    response = _geometry(provider, operation="predict_relation", handles=handles)
    assert response["ok"] is True
    assert response["result"]["dimension"] == 128


def test_geometry_propagates_swallowed_sdk_error(provider, fake_client, plugin):
    handles = _issued_handles(provider, fake_client, [0.05, 0.10])
    telemetry = plugin._RpcTelemetry()
    fake_client._hermes_hyperspace_rpc_telemetry = telemetry
    original = fake_client.get_points

    def swallowed(ids, collection=""):
        telemetry.record(TimeoutError("swallowed geometry timeout"))
        return original(ids, collection=collection)

    fake_client.get_points = swallowed
    response = _geometry(provider, operation="predict_relation", handles=handles)
    assert response["ok"] is False
    assert response["error"]["code"] == "BACKEND_TIMEOUT"


def test_geometry_requires_verified_lorentz_129_and_propagates_timeout(provider, fake_client):
    handles = _issued_handles(provider, fake_client, [0.05, 0.10])
    provider._configured_metric = "cosine"
    metric = _geometry(provider, operation="predict_relation", handles=handles)
    assert metric["ok"] is False
    assert metric["error"]["code"] == "CONFIGURATION_ERROR"

    provider._configured_metric = "lorentz"
    fake_client.fail = TimeoutError("geometry timeout")
    timeout = _geometry(provider, operation="predict_relation", handles=handles)
    assert timeout["ok"] is False
    assert timeout["error"]["code"] == "BACKEND_TIMEOUT"


def test_geometry_converts_math_helper_exceptions_to_redacted_json_error(provider, fake_client, monkeypatch):
    handles = _issued_handles(provider, fake_client, [0.05, 0.10])
    import hyperspace.math as math_module

    def exploding_log_map(*_args, **_kwargs):
        raise ValueError("private helper failure")

    monkeypatch.setattr(math_module, "log_map", exploding_log_map)
    response = _geometry(provider, operation="predict_relation", handles=handles)
    assert response["ok"] is False
    assert response["error"]["code"] == "MALFORMED_RESULT"
    assert "private helper failure" not in response["error"]["message"]
