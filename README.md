# HyperspaceDB Memory Provider for Hermes Agent

A public Hermes Agent `MemoryProvider` backed by an existing HyperspaceDB
collection. The plugin mirrors curated built-in memory mutations, exposes ten
bounded memory tools, and keeps a local identity ledger so `add`, `replace`, and
`remove` do not silently diverge.

## Status

Version 2.4.0 also returns a just-written ledger record when vector search misses the same substring. also scales write RPC deadlines with payload length (4s + 1s/400 chars, cap 300s) and serializes graph, hierarchy, and geometry so parallel tool calls in one turn cannot race the capability table. Version 2.4.0 hardens the collection contract: Lorentz inference requires the
hyperboloid invariant, session switch purges capabilities, and setup marks HMAC
and API keys as secrets. A deployment is not E2E verified until its operator
runs an authorized add/replace/remove probe against a dedicated test collection.

## Requirements

- Hermes Agent with the public `MemoryProvider` plugin interface.
- `hyperspacedb>=3.1.3,<4` in the Hermes Python environment.
- A reachable HyperspaceDB gRPC server.
- An existing collection whose metric matches the configured `metric`.

The plugin never creates or deletes collections automatically.

## Install

User-installed providers are scanned from the flat profile plugins directory:

```text
$HERMES_HOME/plugins/hyperspacedb
```

Bundled/dev checkouts stay at `hermes-agent/plugins/memory/hyperspacedb`. Hermes does not scan `$HERMES_HOME/plugins/memory/hyperspacedb` for user installs.

Then configure the provider and start a new Hermes session. Do not put API keys
in this repository.

## Configuration

```yaml
memory:
  provider: hyperspacedb
  hyperspacedb:
    host: 127.0.0.1:50051
    collection: hermes_memory
    metric: lorentz
    top_k: 5
    rpc_timeout: 4.0
    auto_store: true
    trust_mode: owned_only
    api_key_env: HYPERSPACE_API_KEY
    user_id_env: HYPERSPACE_USER_ID
    ownership_hmac_key_env: HYPERSPACE_OWNERSHIP_HMAC_KEY
```

`collection` is required. There is deliberately no private or deployment-
specific collection default.

### Important options

- `host`: gRPC endpoint. Plaintext remote endpoints are rejected by default.
- `collection`: one existing physical collection used by this provider.
- `metric`: vectorization metric; `lorentz` is the default.
- `expected_dimension`: optional exact collection dimension. Leave at `0` only when the backend cannot report a stable dimension; otherwise a mismatch fails closed.
- `ownership_hmac_key_env`: environment variable for the secret used to authenticate provider-owned records. It is required before authenticated writes can succeed; configure it before enabling `auto_store`, and do not put the key in a public configuration file.
- `rpc_timeout`: clamped to 0.1-60.0 seconds. The default remains 4.0; raise it when live embedding RPCs exceed that.
- `state_path`: optional SQLite identity ledger path. The default is derived
  from the active Hermes home, not from a hardcoded user path.
- `auto_store`: mirrors curated built-in memory writes.
- `trust_mode`: `owned_only` or `annotate_all` for automatic prefetch.
  `owned_only` automatically injects only HMAC-authenticated records whose
  signed source is `hermes-builtin-memory`. Explicit tool stores and every other
  producer remain available through explicit search but are never auto-injected.
  `annotate_all` permits mixed-producer automatic prefetch only when a calibrated
  `max_distance` is explicitly configured; otherwise automatic prefetch is
  fail-closed while explicit search remains available.
- `max_distance`: metric- and corpus-calibrated rejection threshold. It is
  required for `annotate_all` automatic prefetch; this plugin deliberately does
  not invent a universal default cutoff.
- `allow_collection_override`: off by default. Even when enabled, a collection
  must also appear in `allowed_collections`.
- `allow_insecure_remote`: off by default. Enable only when gRPC is already
  protected by a trusted encrypted transport.

Secrets are resolved from the process environment first. An `.env` file is read
only when its path is explicitly configured with `env_file`.

## Mutation semantics

The provider treats the built-in Hermes memory files as the source of mutation
events and HyperspaceDB as a verified mirror:

1. A logical SHA-256 identity includes provider schema, collection, profile
   scope, target, source, and full content.
2. HyperspaceDB still requires a `uint32` point ID. Every candidate is checked
   before use. Foreign ownership causes deterministic probing, never blind
   overwrite.
3. Writes are read back and verified by owner and full digest.
4. `replace` writes and verifies the new record, then deletes and verifies the
   exact old record. Failed deletion becomes `delete_pending`, not success.
5. `remove` resolves `old_text` to exactly one ledger record. Zero or multiple
   matches fail closed.
6. One bounded worker preserves mutation order.

Cross-system atomic transactions are impossible with the current Hermes and
HyperspaceDB contracts. The ledger makes divergence visible and reconcilable;
it does not pretend to provide distributed ACID semantics.

### Failed mutation boundary

The provider does not replay failed `add` or `replace` events from Hermes files.
It records the failure locally and requires an operator to reconcile the primary
Hermes memory state before intentionally reissuing the mutation. The only
automatic reconciliation is bounded `delete_pending` recovery: it runs only
when the remote point has authenticated provider ownership, honors persisted
attempt/backoff limits, and never rebuilds content from a substring match. The
plugin therefore does not claim automatic eventual consistency after a crash.

## Retrieval and trust

Automatic prefetch is dangerous because Hermes currently injects the provider's
returned string as authoritative reference context. This plugin therefore:

- retrieves standard sidecar payloads with `include_payload=True`;
- preserves opaque capability handles, distance, source, trust, target, and timestamp without exposing raw backend point IDs;
- marks every item as memory data, never instructions;
- quarantines common instruction-injection patterns from automatic prefetch;
- bounds query, count, content, graph, cluster, and output sizes;
- distinguishes `NO_HIT` from timeout, authentication, availability,
  collection, and malformed-response failures.

`owned_only` is the recommended public default. It automatically injects only
HMAC-authenticated records whose signed source is `hermes-builtin-memory`.
HMAC authenticates provenance. It does not make that content true or safe.
A signed built-in memory write can still carry paraphrased instructions.
Regex quarantine is defense-in-depth, not a security boundary.
Explicit tool stores and other producers remain explicit-search data. The provider
also recomputes the logical digest from the returned payload before accepting
provider ownership. `annotate_all` is for mixed-producer migration and expands
the trust surface; automatic prefetch is fail-closed until a metric- and
corpus-calibrated `max_distance` is configured. Explicit search remains
available in that state and returns provenance plus distance.

## Local ledger confidentiality

The identity ledger is a local SQLite file containing plaintext memory content needed for deterministic replace and delete operations. The provider creates its ledger directory with mode `0700` and its SQLite file with mode `0600`; these POSIX permissions reduce local-account exposure but are not encryption at rest. Deployments requiring encrypted storage must provide host or volume encryption. The plugin does not claim to encrypt the ledger and does not place API keys in it.

## Tools

The plugin exposes exactly ten bounded tools by default. Opt-in only: event_observation_enabled, operator_reconcile_enabled, batch_mutation_enabled:

- `hyperspace_search`
- `hyperspace_store`
- `hyperspace_status`
- `hyperspace_audit`
- `hyperspace_graph`
- `hyperspace_hierarchy`
- `hyperspace_clusters`
- `hyperspace_search_advanced`
- `hyperspace_admin`
- `hyperspace_geometry`

Search and store responses issue opaque, short-lived capability handles. Graph and
hierarchy tools accept only handles minted by the same live provider profile,
session, and collection; raw backend point IDs are neither accepted nor returned.
Cluster output is limited to cluster cardinalities, not member identifiers.

The geometry tool accepts only live capability handles and checks a verified
Lorentz 129D collection before fetching points. It converts validated Lorentz
hyperboloid points through the SDK Lorentz-to-Poincare bridge, then returns only
bounded scalar summaries for `predict_relation` and `predict_momentum`.
`trust_score` is intentionally `DIAGNOSTIC_UNAVAILABLE`: the current upstream
formula is degenerate (it returns a constant 0.5 for trajectories ending at its
own attractor). These are geometric diagnostics, not evidence that a memory is
factually true or safe;
raw vectors, point IDs, and predicted point content are never returned.

Write RPCs scale their deadline with payload length (4s + 1s/400 chars, cap 300s). Graph, hierarchy, and geometry handlers share one in-process lock so parallel tool calls in one turn cannot race the capability table.

The admin tool is read-only. Its allowed operations are `health`, `stats`,
`count`, `digest`, and `cache_stats`. Every numeric response is field-by-field
allowlisted and malformed backend maps fail closed; raw backend maps, schema
objects, cache entries, and bucket lists are not returned. Collection creation,
deletion, rebuild, vacuum, snapshot, reconsolidation, cache mutation, and cache
configuration operations are intentionally absent.

## Backup and restore

`hermes backup` receives an integrity-checked SQLite identity-ledger snapshot
from `backup_paths()`, not the live WAL database. It does not archive an
arbitrary server checkout and does not claim that copying a live database
directory is a consistent snapshot.

Back up the HyperspaceDB collection with the database's own verified snapshot or
cold-backup procedure. Restore the database and ledger as one documented
recovery operation, then run reconciliation checks before trusting removals or
replacements.

## Migration from 0.1.x

The previous implementation used a content-derived `uint32` without ownership
verification and did not mirror `remove`. Existing owned legacy records can be
resolved lazily by source, target, exact substring, and content. Ambiguous
legacy matches are rejected. Keep the old database snapshot until migration and
an authorized mutation E2E test are complete.

## Test

Run from this directory with the active Hermes source on `PYTHONPATH`:

```bash
python -m pytest -q
```

The default suite uses a fake client and must not write to production. The
read-only integration suite requires explicit environment variables. Mutation
E2E requires a dedicated test collection and separate operator authorization.

For the strict mutation runner, set all of these only for a non-production test:
`HSDB_E2E_WRITE_APPROVED=approved`, `HSDB_TEST_OWNERSHIP_HMAC_KEY`,
`HSDB_TEST_SOURCE_COLLECTION`, `HSDB_TEST_COLLECTION` (prefixed `hsdb_e2e_`),
and `HSDB_E2E_STATE_PATH` outside this plugin directory. Then run
`python tests/run_test_collection_e2e.py`. The runner leaves the isolated
collection intact for operator inspection and never uses this plugin's `state/`.

## Honest limitations

- HyperspaceDB's `uint32` ID API cannot make cross-process allocation races
  impossible without a server-side conditional insert.
- Hermes exposes no per-record external-memory trust type; this plugin can gate
  and label context but cannot change the core wrapper contract.
- Distributed atomicity between local Markdown memory and the remote vector
  database does not exist.
- A universal semantic distance cutoff would be dishonest; calibrate
  `max_distance` on the target metric and corpus.


## Authors

Created by Paulina Janowska and Gniewisława AI.

Proudly witchcrafted in Poznań, Poland ♥

Built with skepticism and an unreasonable allergy to confident bullshit.
