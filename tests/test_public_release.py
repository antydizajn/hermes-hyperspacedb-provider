import importlib.util
from pathlib import Path
import subprocess
import re
import sys
import types

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
TEXT_EXTENSIONS = {".py", ".md", ".yaml", ".yml", ".toml", ".txt"}
# Workflow-ledger names excluded from any release (mirrors .gitattributes export-ignore).
_EXPORT_IGNORED_NAMES = {
    "PLAN.md", "PLAN-LUNA.md", "HANDOFF.md", "HANDOFF-TERRA-2.md",
    "AUDIT.md", "AUDIT-CLEAN-20260813.md", "LUNA-AUDIT-REPORT.md",
    "PROMPT_ITERATIONS.md",
}
_NON_SHIPPED_DIRS = {"state", "_deferred_events", "__pycache__", ".ci", ".git"}


def _git_repository_root() -> "Path | None":
    """Return the enclosing Git repository only when it actually tracks this plugin.

    A `git rev-parse --show-toplevel` returns the nearest ancestor repository even
    when the plugin is merely nested inside an unrelated repo (e.g. an extracted
    release artifact under a larger tree). The layout-aware checks only apply when
    the plugin root itself is tracked there, so we confirm with `git ls-files
    --error-unmatch` against `ROOT/__init__.py`. Returns None when there is no
    such repository.
    """
    try:
        output = subprocess.check_output(
            ["git", "rev-parse", "--show-toplevel"], cwd=ROOT, text=True, stderr=subprocess.DEVNULL
        )
    except subprocess.CalledProcessError:
        return None
    repo = Path(output.strip()).resolve()
    probe = subprocess.run(
        ["git", "ls-files", "--error-unmatch", "--", "memory/hyperspacedb/__init__.py"],
        cwd=repo, check=False, capture_output=True,
    )
    # Accept either the exact plugin path or a generic probe; the meaningful
    # signal is that "memory/hyperspacedb/__init__.py" is tracked at repo root.
    if probe.returncode != 0:
        # Some layouts track the plugin at plugins/memory/hyperspacedb; probe both.
        probe_alt = subprocess.run(
            ["git", "ls-files", "--error-unmatch", "--",
             "plugins/memory/hyperspacedb/__init__.py"],
            cwd=repo, check=False, capture_output=True,
        )
        if probe_alt.returncode != 0:
            return None
    return repo


def _repository_root() -> Path:
    repository = _git_repository_root()
    if repository is None:
        pytest.skip("plugin root is not tracked in an enclosing Git repository")
    return repository


def _plugin_prefix(repository: Path) -> str:
    try:
        return ROOT.relative_to(repository).as_posix()
    except ValueError as exc:
        raise AssertionError("plugin root must be inside its Git repository") from exc


def _shipped_text_paths(all_paths, text_extensions=TEXT_EXTENSIONS):
    """Yield ROOT files that would actually ship in a release, regardless of git.

    Walks the real directory tree, skipping runtime state, deferred specs, and
    bytecode caches. Names in the export-ignore ledger set are excluded so the
    OPSEC scans never read private operational documents.
    """
    for candidate in sorted(all_paths):
        relative_parts = candidate.relative_to(ROOT).parts
        if any(part in _NON_SHIPPED_DIRS for part in relative_parts):
            continue
        if candidate.name in _EXPORT_IGNORED_NAMES:
            continue
        if candidate.suffix in text_extensions and candidate.is_file():
            yield candidate


def _is_export_ignored(path: Path) -> bool:
    if path.name in _EXPORT_IGNORED_NAMES:
        return True
    repository = _git_repository_root()
    if repository is None:
        return False
    relative = path.resolve().relative_to(repository).as_posix()
    output = subprocess.check_output(
        ["git", "check-attr", "export-ignore", "--", relative],
        cwd=repository,
        text=True,
    )
    return output.rstrip().endswith(": set")


def shipped_text():
    # Real on-disk release files (git-independent) so the OPSEC scans always
    # run against the actual shipped surface, even in a standalone artifact.
    all_paths = sorted(p for p in ROOT.rglob("*"))
    for path in _shipped_text_paths(all_paths):
        yield path, path.read_text(encoding="utf-8")


def test_no_private_identifiers_or_absolute_user_paths():
    forbidden = [
        "paulina" + "janowska", "anty" + "dizajn", "gniewka" + "_omniscient",
        "Gniew" + "islawa", "ANTI" + "GRAVITY", "/" + "Users/", "~/" + "AI/",
    ]
    failures = []
    for path, text in shipped_text():
        for token in forbidden:
            if token.lower() in text.lower():
                failures.append(f"{path.relative_to(ROOT)}: {token}")
    assert failures == []


def test_no_secret_assignments():
    pattern = re.compile(r"(?i)(api[_-]?key|token|secret|password)\s*[:=]\s*['\"][^'\"]{8,}")
    failures = []
    for path, text in shipped_text():
        if pattern.search(text):
            failures.append(str(path.relative_to(ROOT)))
    assert failures == []


def test_manifest_declares_sdk_dependency():
    text = (ROOT / "plugin.yaml").read_text(encoding="utf-8")
    assert "hyperspacedb" in text
    assert "pip_dependencies" in text


def test_readme_discloses_no_automatic_mutation_replay():
    text = (ROOT / "README.md").read_text(encoding="utf-8").lower()
    assert "does not replay failed `add` or `replace`" in text
    assert "does not claim automatic eventual consistency" in text


def test_runtime_artifacts_are_ignored_without_deleting_them():
    ignored = (ROOT / ".gitignore").read_text(encoding="utf-8")
    assert "/state/" in ignored
    assert "*.sqlite3" in ignored
    assert "__pycache__/" in ignored
    probe = subprocess.run(
        ["git", "check-ignore", "-q", "state/example/ledger.sqlite3"],
        cwd=ROOT,
        check=False,
    )
    assert probe.returncode == 0


def test_plugin_release_has_license_metadata_and_excludes_workflow_ledgers():
    manifest = yaml.safe_load((ROOT / "plugin.yaml").read_text(encoding="utf-8"))
    assert manifest["license"] == "MIT"
    license_path = ROOT / "LICENSE"
    assert license_path.is_file()
    assert "MIT License" in license_path.read_text(encoding="utf-8")
    for name in (
        "PLAN.md",
        "PLAN-LUNA.md",
        "HANDOFF.md",
        "HANDOFF-TERRA-2.md",
        "AUDIT.md",
        "LUNA-AUDIT-REPORT.md",
        "PROMPT_ITERATIONS.md",
    ):
        assert _is_export_ignored(ROOT / name)


def test_readme_discloses_plaintext_ledger_and_permission_boundary():
    text = (ROOT / "README.md").read_text(encoding="utf-8").lower()
    assert "plaintext memory content" in text
    assert "mode `0700`" in text
    assert "mode `0600`" in text
    assert "not encryption at rest" in text


def test_readme_has_required_authorship_block():
    text = (ROOT / "README.md").read_text(encoding="utf-8")
    assert "Created by Paulina Janowska and Gniewisława AI." in text
    assert "Proudly witchcrafted in Poznań, Poland" in text
    assert "unreasonable allergy to confident bullshit" in text
    assert "\u2014" not in text and "\u2013" not in text


def test_readme_matches_collection_contract_configuration():
    text = (ROOT / "README.md").read_text(encoding="utf-8")
    assert "`expected_dimension`" in text
    assert "`trusted_sources`" not in text


def test_provider_has_no_dead_trusted_sources_policy():
    source = (ROOT / "__init__.py").read_text(encoding="utf-8")
    assert "trusted_sources" not in source


def test_readme_documents_hmac_environment_boundary():
    text = (ROOT / "README.md").read_text(encoding="utf-8")
    assert "ownership_hmac_key_env" in text
    assert "do not put the key in a public configuration file" in text


def test_readme_matches_current_ten_tool_and_capability_contract():
    text = (ROOT / "README.md").read_text(encoding="utf-8")
    normalized = " ".join(text.split())
    assert "exposes eight bounded memory tools" not in normalized
    assert "exactly ten bounded tools" in text
    assert "hyperspace_audit" in text
    assert "hyperspace_geometry" in text
    for operation in ("predict_relation", "predict_momentum", "trust_score"):
        assert f"`{operation}`" in text
    assert "DIAGNOSTIC_UNAVAILABLE" in text
    assert "constant 0.5" in text
    assert "capability handle" in text
    assert "raw backend point IDs" in text
    assert "geometric diagnostics, not evidence that a memory is" in text
    assert "factually true or safe" in text


def test_readme_documents_read_only_admin_allowlist():
    text = (ROOT / "README.md").read_text(encoding="utf-8")
    for operation in ("health", "stats", "count", "digest", "cache_stats"):
        assert f"`{operation}`" in text
    assert "allowlisted" in text
    assert "cache mutation" in text


def test_manifest_version_and_dependency_contract_match_readme():
    manifest = yaml.safe_load((ROOT / "plugin.yaml").read_text(encoding="utf-8"))
    assert re.fullmatch(r"\d+\.\d+\.\d+", str(manifest["version"]))
    dependency = manifest["pip_dependencies"][0]
    assert dependency.startswith("hyperspacedb>=") and ",<" in dependency
    text = (ROOT / "README.md").read_text(encoding="utf-8")
    assert f"Version {manifest['version']}" in text
    assert dependency in text


def test_e2e_runner_requires_explicit_approval_and_test_hmac_before_client_creation():
    text = (ROOT / "tests" / "run_test_collection_e2e.py").read_text(encoding="utf-8")
    assert "HSDB_E2E_WRITE_APPROVED" in text
    assert "HSDB_TEST_OWNERSHIP_HMAC_KEY" in text
    assert "HSDB_E2E_STATE_PATH" in text
    assert "hsdb_e2e_" in text
    assert text.index('approval != "approved"') < text.index("client = HyperspaceClient")
    assert text.index('state_path = require_external_state_path') < text.index("client = HyperspaceClient")


def test_e2e_runner_uses_self_seeded_target_only_and_never_reads_source_collection():
    text = (ROOT / "tests" / "run_test_collection_e2e.py").read_text(encoding="utf-8")
    assert "HSDB_TEST_SOURCE_COLLECTION" not in text
    assert "fixture source" not in text.lower()
    assert "def seed_target_fixtures" in text
    assert 'collection=target' in text
    assert "client.scroll(" not in text


def test_e2e_runner_uses_explicit_embedding_aware_deadlines(monkeypatch):
    runner = _load_e2e_runner(monkeypatch)
    assert runner.E2E_RPC_TIMEOUT_SECONDS >= 60.0
    assert runner.E2E_FLUSH_TIMEOUT_SECONDS >= 180.0


def test_e2e_self_seed_is_idempotent_and_target_scoped(monkeypatch):
    runner = _load_e2e_runner(monkeypatch)

    class TargetOnlyClient:
        def __init__(self):
            self.points = {}
            self.calls = []

        def get_points(self, ids, *, collection):
            self.calls.append(("get_points", collection))
            return [self.points[item] for item in ids if item in self.points]

        def vectorize(self, text, *, metric):
            self.calls.append(("vectorize", metric))
            return [1.0] + [0.0] * 128

        def insert(self, point_id, *, vector, document, payload, metadata, collection, durability):
            self.calls.append(("insert", collection))
            self.points[point_id] = {"id": point_id, "metadata": dict(metadata)}
            return True

    client = TargetOnlyClient()
    target = "hsdb_e2e_unit_target"
    query, first_count = runner.seed_target_fixtures(client, target)
    _, second_count = runner.seed_target_fixtures(client, target)

    assert query == runner.E2E_FIXTURES[0]
    assert first_count == len(runner.E2E_FIXTURES)
    assert second_count == len(runner.E2E_FIXTURES)
    collection_calls = [
        collection for operation, collection in client.calls
        if operation in {"get_points", "insert"}
    ]
    assert collection_calls
    assert all(collection == target for collection in collection_calls)


def test_e2e_runner_verifies_remote_lifecycle_and_zero_worker_failures():
    text = (ROOT / "tests" / "run_test_collection_e2e.py").read_text(encoding="utf-8")
    assert 'old_external_id = rows[0]["external_id"]' in text
    assert 'replacement_external_id = rows[0]["external_id"]' in text
    assert 'not client.get_points([old_external_id], collection=target)' in text
    assert 'not client.get_points([replacement_external_id], collection=target)' in text
    assert 'provider.status_snapshot()["failed_writes"] == 0' in text


def _load_e2e_runner(monkeypatch):
    fake_hyperspace = types.ModuleType("hyperspace")
    fake_hyperspace.HyperspaceClient = object
    module_name = "hyperspace_e2e_runner_under_test"
    monkeypatch.setitem(sys.modules, "hyperspace", fake_hyperspace)
    monkeypatch.delitem(sys.modules, module_name, raising=False)
    runner_path = ROOT / "tests" / "run_test_collection_e2e.py"
    spec = importlib.util.spec_from_file_location(module_name, runner_path)
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, module_name, module)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("state_path", ["", "relative/ledger.sqlite3"])
def test_e2e_state_path_guard_rejects_empty_or_relative_path(monkeypatch, state_path):
    runner = _load_e2e_runner(monkeypatch)
    with pytest.raises(SystemExit):
        runner.require_external_state_path(state_path)


def test_e2e_state_path_guard_rejects_path_under_plugin_root(monkeypatch):
    runner = _load_e2e_runner(monkeypatch)
    with pytest.raises(SystemExit):
        runner.require_external_state_path(str(ROOT / "state" / "e2e.sqlite3"))


def test_e2e_state_path_guard_accepts_absolute_external_path(monkeypatch):
    runner = _load_e2e_runner(monkeypatch)
    external = ROOT.parent / "e2e-runtime" / "ledger.sqlite3"
    assert Path(runner.require_external_state_path(str(external))) == external.resolve()


def test_tracked_release_manifest_excludes_runtime_artifacts():
    repository = _repository_root()
    prefix = _plugin_prefix(repository)
    output = subprocess.check_output(
        ["git", "ls-files", "--", prefix], cwd=repository, text=True
    )
    tracked = [line for line in output.splitlines() if line]
    expected = f"{prefix}/" if prefix != "." else ""
    assert f"{expected}__init__.py" in tracked
    assert f"{expected}README.md" in tracked
    assert f"{expected}plugin.yaml" in tracked
    assert f"{expected}LICENSE" in tracked
    assert not any("/state/" in path for path in tracked)
    assert not any(path.endswith((".sqlite3", ".pyc")) for path in tracked)


def test_e2e_runner_never_defaults_its_ledger_into_plugin_state():
    text = (ROOT / "tests" / "run_test_collection_e2e.py").read_text(encoding="utf-8")
    assert '"state_path": state_path' in text
    assert 'ROOT / "state"' not in text


def test_release_ships_github_actions_ci():
    workflow = ROOT / ".github" / "workflows" / "ci.yml"
    if not workflow.is_file():
        pytest.skip("CI workflow lives in the standalone public repo")
    text = workflow.read_text(encoding="utf-8")
    assert "NousResearch/hermes-agent" in text
    assert "56a41715dc3b8bf6f50a740ff9416c4036ef4259" in text
    assert "pytest" in text
    assert "$HERMES_HOME/plugins/hyperspacedb" in text or "plugins/hyperspacedb" in text
