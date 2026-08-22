<div align="center">

# HyperspaceDB Memory Provider

**Make the memory fail closed.**

[![Version](https://img.shields.io/badge/version-2.6.0-black?style=flat-square)](plugin.yaml)
[![Hermes Provider](https://img.shields.io/badge/Hermes-Memory_Provider-111111?style=flat-square)](https://github.com/NousResearch/hermes-agent)
[![License](https://img.shields.io/badge/license-MIT-black?style=flat-square)](#license)
[![CI](https://img.shields.io/github/actions/workflow/status/antydizajn/hermes-hyperspacedb-provider/ci.yml?branch=main&style=flat-square&label=CI&color=black)](https://github.com/antydizajn/hermes-hyperspacedb-provider/actions/workflows/ci.yml)
[![Geometry](https://img.shields.io/badge/geometry-Lorentz_129D-black?style=flat-square)](#how-it-works)
[![Security](https://img.shields.io/badge/provenance-HMAC_authenticated-black?style=flat-square)](#provenance-and-trust-boundaries)

A hardened, fail-closed Hermes Agent memory provider backed by HyperspaceDB, ordered SQLite ledger mutations, HMAC provenance gating, and Lorentz 129D hyperbolic retrieval.

[Install](#install) · [The problem](#the-problem) · [How it works](#how-it-works) · [Fail-closed invariants](#fail-closed-invariants) · [Tools](#exactly-ten-bounded-tools) · [Security](#provenance-and-trust-boundaries) · [Limits](#what-it-does-not-prove)

</div>

---

## The problem

LLM agent memory systems usually fail in five quiet ways:

1. **Silent write drops**: a remote gRPC or HTTP write fails, the agent assumes memory was persisted, and the next turn hallucinates continuity.
2. **Out-of-order mutations**: concurrent `add`, `replace`, and `remove` operations race over the network, causing a stale delete to wipe out a fresh replacement.
3. **Unverified embeddings**: the database silently accepts vector inserts while the collection geometry or dimensionality differs from the active embedding model.
4. **Prompt injection via memory**: raw third-party search results or external text are written into the memory store without provenance, then re-injected as trusted prompt instructions.
5. **Divergent local state**: memory injection reports success while the actual backend collection is empty or drifted.

This provider treats memory as an auditable state machine with deterministic local write-ahead logging rather than fire-and-forget storage.

---

## What this provider does

- **Fail-closed durability**: memory mutation operations fail closed (`BACKEND_UNAVAILABLE` or error status) if the underlying persistence store cannot guarantee state integrity.
- **Ordered mutation pipeline**: a single bounded worker serializes mirrored `add`, `replace`, and `remove` operations, while the SQLite ledger persists deterministic identity, mutation state, failures, and reconciliation metadata.
- **HMAC provenance gating**: records carry cryptographic HMAC signatures to distinguish authenticated agent memories from untrusted external injection payloads.
- **Lorentz 129D hyperbolic geometry**: validates and operates on native Lorentz hyperboloid embeddings for retrieval and bounded geometric diagnostics.
- **Strict mutation contracts**: `replace` and `remove` require exact needle matching and fail if target memories are missing or ambiguous.
- **Isolated capability handles**: graph and hierarchy exploration tools use opaque capability tokens (`hsdbh_*`) rather than exposing raw backend point IDs to LLM contexts.
- **Verified CI artifact continuity**: releases are produced exclusively from green GitHub Actions builds with deterministic SHA-256 manifests.

---

## Fail-closed invariants

These are operational invariants enforced in code and unit tests:

| Failure condition | Provider response |
|---|---|
| HyperspaceDB server unreachable | Return `BACKEND_UNAVAILABLE` immediately; never claim silent memory persistence |
| Dimension or metric mismatch | Fail startup initialization if collection dimensions differ from `expected_dimension` |
| Ambiguous needle in `replace`/`remove` | Reject mutation if target substring matches multiple or zero records |
| Missing HMAC ownership key | Require `ownership_hmac_key_env`; refuse unsigned writes when `trust_mode=owned_only` |
| Malformed tool arguments | Reject unexpected parameters at runtime before executing backend RPCs |
| Read-only admin operations | Restrict `hyperspace_admin` to strictly allowlisted read-only operations |
| Dangerous cache mutations | Reject cache mutation and flush commands in unprivileged tool contexts |
| Server or network failure during write | Persist mutation state in local SQLite ledger; failed `add`/`replace` remains explicit-retry state; authenticated `delete_pending` reconciles on reconnect; never report success without remote confirmation |

---

## How it works

```text
HERMES AGENT
  │
  ├─► memory(action="add" | "replace" | "remove")
  │      │
  │      ▼
  │   [Ordered SQLite Ledger] ── (WAL: mode 0600 / dir 0700)
  │      │
  │      ▼
  │   [HMAC Signer & Validator] ── (SHA-256 Authentication)
  │      │
  │      ▼
  │   [HyperspaceDB gRPC Client] ── (Lorentz 129D / Vector Search)
  │
  └─► Exactly ten bounded tools (Search, Admin, Audit, Geometry, Graph)
```

A single bounded worker serializes mirrored Hermes mutations. SQLite records durable identity, mutation state, failures, and reconciliation metadata using WAL mode (`journal_mode=WAL`) and `synchronous=FULL`.

The provider does not replay failed `add` or `replace` events from Hermes files. It records the failure locally and requires an operator to reconcile the primary Hermes memory state before intentionally reissuing the mutation. The provider does not claim automatic eventual consistency.

---

## Exactly ten bounded tools

The provider registers exactly ten bounded tools divided into primary memory operations and diagnostic inspection capabilities:

### Primary memory tools (Channel operations)
1. **`hyperspace_status`**: inspects backend connectivity, configured collection, metric configuration, and write queue backlog.
2. **`hyperspace_search`**: runs bounded semantic similarity search with provenance verification.
3. **`hyperspace_store`**: writes durable facts into the configured collection with automatic HMAC provenance tagging.

### Deep inspection and analytical tools
4. **`hyperspace_audit`**: returns SQLite ledger aggregates, schema versions, and reconciliation backlog counters without raw memory dumps.
5. **`hyperspace_admin`**: executes strictly allowlisted read-only diagnostics (`health`, `stats`, `count`, `digest`, `cache_stats`). Rejects all cache mutation operations.
6. **`hyperspace_clusters`**: detects emergent semantic clusters in Lorentz vector space.
7. **`hyperspace_graph`**: traverses concept relationship graphs using opaque capability handles (`hsdbh_*`) without exposing raw backend point IDs.
8. **`hyperspace_hierarchy`**: explores Lorentz subsumption trees and parent concept relationships.
9. **`hyperspace_search_advanced`**: executes bounded Wasserstein (Optimal Transport) and Wave distance searches.
10. **`hyperspace_geometry`**: computes Lorentz Poincaré scalar metrics (`predict_relation`, `predict_momentum`, `trust_score`). `trust_score` returns `DIAGNOSTIC_UNAVAILABLE`: the current upstream formula degenerates to constant 0.5 in the relevant case, so the provider deliberately refuses to expose it as a meaningful score. These scalar outputs are geometric diagnostics, not evidence that a memory is factually true or safe.

---

## Provenance and trust boundaries

- **Plaintext memory content**: The local SQLite ledger stores plaintext memory content for fast needle search and mutation recovery. File permissions are restricted to mode `0600` inside a directory of mode `0700`. This provides filesystem-level user isolation, not encryption at rest.
- **HMAC security boundary**: The provider uses `ownership_hmac_key_env` to load the authentication secret from environment variables. It is required before authenticated writes can succeed; configure it before enabling auto_store, and do not put the key in a public configuration file.
- **Upstream vs Client authentication**: `api_key_env` and `user_id_env` provide optional credentials for remote multi-tenant HyperspaceDB servers. In contrast, `ownership_hmac_key_env` is the client-side cryptographic signature that authenticates memories created by this agent.
- **Untrusted data separation**: Retrieved memory records are data, not executable prompt instructions. Filtering on owned content verifies only that the record originated from this agent ledger; it does not make that content true or safe. HMAC is provenance authentication, not a security boundary for untrusted prompt injection.

---

## Install

User plugins are discovered from the standard Hermes user plugin directory:

```text
$HERMES_HOME/plugins/hyperspacedb
```

To install as a user drop-in plugin:

```bash
mkdir -p ~/.hermes/plugins/hyperspacedb
rsync -av --exclude '.git' --exclude '__pycache__' --exclude 'dist' /path/to/repo/ ~/.hermes/plugins/hyperspacedb/
```

Or install the packaged wheel in your Hermes Agent virtual environment:

```bash
pip install hermes_hyperspacedb_provider-2.6.0-py3-none-any.whl
```

### Recommended companion integrations

Always install the upstream companion skills and MCP server alongside this provider for complete memory workflow tooling:

- **[HyperspaceDB Skills](https://github.com/YARlabs/hyperspace-db/tree/main/integrations/hyperspacedb-skills)**: Specialized agent skills for Lorentz geometry diagnostics, concept hierarchy exploration, and memory management procedures.
- **[HyperspaceDB MCP Server](https://github.com/YARlabs/hyperspace-db/tree/main/integrations/mcp-hyperspacedb)**: Model Context Protocol server providing direct tool inspection, graph traversal, and cognitive analytics across agent runtimes.

---

## Configuration

The canonical and recommended way to configure the provider is via the interactive Hermes wizard:

```bash
hermes memory setup
```

Or configure directly in `~/.hermes/config.yaml`:

```yaml
memory:
  provider: hyperspacedb
  hyperspacedb:
    collection: agent_memory
    host: 127.0.0.1:50051
    metric: lorentz
    expected_dimension: 129
    trust_mode: owned_only
    auto_store: true
    rpc_timeout: 120.0
    api_key_env: HYPERSPACE_API_KEY
    user_id_env: HYPERSPACE_USER_ID
    ownership_hmac_key_env: HYPERSPACE_OWNERSHIP_HMAC_KEY
```

Requirements:
- Python `>=3.11`
- `hyperspacedb>=3.1.3,<4`
- `cryptography>=41.0.0`
- A running HyperspaceDB gRPC server instance

### Local vs remote authentication

For a local HyperspaceDB instance with authentication disabled, leave `api_key_env`/`api_key` unset. Do not use a placeholder API key: sending a dummy credential to an unauthenticated local server may return `UNAUTHENTICATED`.

For authenticated remote deployments, configure the real `HYPERSPACE_API_KEY` and, where required, `HYPERSPACE_USER_ID`.

---

## Verification & Tests

Run the default local test suite:

```bash
python3 -m pytest tests/ -v
```

### Live integration E2E runner

The repository includes a dedicated self-seeding E2E mutation runner (`tests/run_test_collection_e2e.py`) designed for isolated non-production testing. To execute live E2E tests against a running HyperspaceDB instance:

```bash
HSDB_E2E_WRITE_APPROVED=approved \
HSDB_TEST_OWNERSHIP_HMAC_KEY="0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef" \
HSDB_TEST_COLLECTION="hsdb_e2e_isolated_test" \
HSDB_E2E_STATE_PATH="/tmp/hsdb_e2e_state.sqlite3" \
python3 tests/run_test_collection_e2e.py
```

---

## Status

Version 2.6.0 raises the RPC timeout ceiling and restores resilient worker startup after live production E2E testing (2026-08-22) against a large collection under memory pressure:

1. **`rpc_timeout` ceiling raised 60s → 300s**: The previous hard clamp silently truncated configured values above 60 seconds. Live measurement on a large production collection showed server-side vectorize at 16 to 40 seconds plus two `get_points` verification round-trips, so a configured 120s deadline was clamped to an effective 60s and produced spurious `BACKEND_TIMEOUT` / `retry_pending` records. Configure `rpc_timeout: 120.0` (or higher, up to 300s) when your HyperspaceDB instance serves large collections or runs with embedding enabled; the default remains 4.0s.
2. **Resilient worker startup**: The ordered write worker starts before the health probe (restoring pre-2.4.4 behavior). When the first session initializes while the backend is briefly unreachable, mirrored writes are now serialized into the ledger as explicit `retry_pending` state instead of sitting in-process until a second `initialize()`. The fail-closed invariant is unchanged: no mutation is ever reported successful without backend read-after-write verification.
3. **Self-heal pin tests**: New regression suite pins that a later `initialize()` retries the health probe and collection-contract verification, so a session recovers automatically once the backend returns; no agent restart required.

Version 2.5.3 hardens CI supply-chain permissions, enforces release asset immutability, and establishes verified artifact continuity:
1. **Verified CI Artifact Continuity**: Automated GitHub Actions workflow builds, verifies wheel purity, tests cross-matrix dependencies, and directly publishes the exact tested CI artifacts with a `SHA256SUMS.txt` manifest upon tag creation (strictly fail-closed on duplicate release attempts without overwriting/clobbering).
2. **Least Privilege CI Permissions**: Workflow applies global `contents: read` permissions with `persist-credentials: false` across all test jobs, elevating to `contents: write` exclusively in the dedicated, isolated `release` gate.
3. **Dependency closure (`cryptography`)**: Declares `cryptography>=41.0.0` as an explicit package dependency to prevent downstream `ModuleNotFoundError` during HyperspaceDB client imports in clean environments.
4. **Clean wheel distribution**: Configures explicit package boundaries in `pyproject.toml` ensuring the binary wheel contains only production provider code and `plugin.yaml` (excluding `tests/`, `_deferred_events/`, and internal workflow artifacts).
5. **`save_config()` user path alignment**: Writes updated configuration to the canonical user-plugin path (`$HERMES_HOME/plugins/hyperspacedb/plugin.yaml`).
6. **Auto-store thread optimization**: Background write worker thread spawns only when `auto_store: true` is configured.
7. **Domain-driven modularization**: Provider implementation split into 16 decoupled submodules (`_capabilities`, `_config`, `_constants`, `_errors`, `_geometry`, `_graph`, `_ledger`, `_lifecycle`, `_mutations`, `_provider`, `_retrieval`, `_rpc`, `_security`, `_tools`, `_utils`, `__init__`) with 100% backward-compatible root exports.

---

## What it does not prove

This memory provider does **not** promise:

- That an agent will reason correctly over retrieved memory.
- That stored memories are objective real-world facts rather than agent perceptions.
- That vector embeddings capture 100% of nuanced semantic meaning.
- Hardware-level encryption at rest (ledger permissions provide OS-level user isolation).
- Automatic recovery if the physical disk is corrupted or wiped.

It guarantees fail-closed mutation durability, deterministic local ordering, and cryptographic provenance gating.

---

## Repository map

```text
hermes-hyperspacedb-provider/
├── README.md                             # Public contract, architecture, and documentation
├── LICENSE                               # MIT License
├── plugin.yaml                           # Hermes Agent plugin manifest (v2.6.0)
├── pyproject.toml                        # Build system, dependencies, and wheel boundaries
├── __init__.py                           # Public package root and exports
├── _capabilities.py                      # Tool capability definitions and schemas
├── _config.py                            # Configuration parsing and defaults
├── _constants.py                         # Internal limits, constants, and allowed args
├── _errors.py                            # Structured error codes and mapping
├── _geometry.py                          # Lorentz Poincaré geometric diagnostics
├── _graph.py                             # Concept graph and hierarchy exploration
├── _ledger.py                            # SQLite write-ahead log and state machine
├── _lifecycle.py                         # Connection management and health checks
├── _mutations.py                         # Mutation handlers (add, replace, remove)
├── _provider.py                          # HyperspaceDBMemoryProvider implementation
├── _retrieval.py                         # Semantic and hybrid search operations
├── _rpc.py                               # gRPC communication layer and error handling
├── _security.py                          # HMAC signing, verification, and sanitization
├── _tools.py                             # Tool dispatcher and parameter validator
├── _utils.py                             # Shared helper functions and timestamp formatters
├── .github/
│   └── workflows/
│       └── ci.yml                        # Matrix CI, packaging, canary, and release workflow
└── tests/
    ├── conftest.py                       # Shared test fixtures and fake clients
    ├── run_test_collection_e2e.py        # Strict live E2E mutation runner
    ├── test_audit_v244_regressions.py    # Regression tests for v2.4.4 audit findings
    ├── test_baseline_contract.py         # MemoryProvider interface compliance
    ├── test_hermes_contract.py           # Hermes CLI and user plugin contract tests
    ├── test_ledger_fallback_merge.py     # SQLite ledger fallback merge tests
    ├── test_ledger_migrations.py         # Ledger schema migration tests
    ├── test_ledger_owned_only_prefetch.py # Prefetch and filtering verification
    ├── test_ledger_read_your_writes.py   # Read-your-writes consistency tests
    ├── test_lifecycle.py                 # Startup, shutdown, and error recovery
    ├── test_mutation_recovery.py         # Uncommitted mutation recovery
    ├── test_mutation_semantics.py        # Exact needle matching and rollback tests
    ├── test_optional_a8.py               # Optional search tools verification
    ├── test_optional_admin.py            # Read-only admin operations allowlist tests
    ├── test_optional_audit.py            # Ledger audit summary tool tests
    ├── test_optional_geometry.py         # Geometric diagnostics verification
    ├── test_optional_graph_points.py     # Graph handle resolution tests
    ├── test_owned_only_semantics.py      # HMAC ownership gating verification
    ├── test_p0_red_regressions.py        # P0 regression suite
    ├── test_polish_deadlines_and_locks.py # Lock timeouts and deadline tests
    ├── test_public_release.py            # Packaging, contract, and release integrity
    ├── test_retrieval.py                 # Search ranking and filtering tests
    ├── test_sdk_import_contract.py       # Real HyperspaceDB SDK import tests
    └── test_security.py                  # Injection prevention and HMAC tests
```

---

## One rule worth stealing

> **Never let an agent assume memory was saved without a verified, ordered write-ahead receipt.**

---

## Authors

Created by **[Paulina Janowska](https://antydizajn.pl)** and **[Gniewisława AI](https://gniewka.antydizajn.pl)**.

**Proudly witchcrafted in Poznań, Poland ♥**

Built with skepticism and an unreasonable allergy to confident bullshit.

---

## License

MIT.
