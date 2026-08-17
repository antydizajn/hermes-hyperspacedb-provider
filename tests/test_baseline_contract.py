from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = "\n".join(p.read_text(encoding="utf-8") for p in sorted(ROOT.glob("*.py")))


def test_module_starts_with_valid_python_not_grpc_noise():
    assert SOURCE.lstrip().startswith('"""')


def test_public_code_has_no_private_stack_hardcodes():
    forbidden = [
        "gniewka" + "_omniscient",
        "Gniew" + "islawa",
        "Anti" + "gravity",
        "~/" + "AI/",
        "/" + "Users/",
    ]
    for token in forbidden:
        assert token not in SOURCE


def test_remove_is_not_ignored():
    assert 'action not in {"add", "replace"}' not in SOURCE


def test_payload_is_supported():
    assert 'raw.get("payload")' in SOURCE


def test_decorative_deadline_is_gone():
    assert "deadline = time.monotonic()" not in SOURCE


def test_private_checkout_is_not_a_backup_path():
    assert "EXTERNAL/hyperspace-db" not in SOURCE
