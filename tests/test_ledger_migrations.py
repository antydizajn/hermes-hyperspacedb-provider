import sqlite3

import pytest


def test_ledger_migration_sets_version_and_rejects_future(plugin, tmp_path):
    path = tmp_path / "ledger.sqlite3"
    ledger = plugin.IdentityLedger(path)
    ledger.close()
    db = sqlite3.connect(path)
    assert db.execute("PRAGMA user_version").fetchone()[0] == 3
    db.execute("PRAGMA user_version=4")
    db.commit()
    db.close()
    with pytest.raises(plugin.ConfigurationError):
        plugin.IdentityLedger(path)


def test_ledger_rejects_corrupt_database(plugin, tmp_path):
    path = tmp_path / "corrupt.sqlite3"
    path.write_bytes(b"not a sqlite database")
    with pytest.raises(plugin.ConfigurationError):
        plugin.IdentityLedger(path)


def test_ledger_state_and_snapshot_permissions(plugin, tmp_path):
    path = tmp_path / "state" / "ledger.sqlite3"
    ledger = plugin.IdentityLedger(path)
    snapshot = ledger.snapshot_to(path.with_suffix(".snapshot.sqlite3"))
    assert path.stat().st_mode & 0o777 == 0o600
    assert path.parent.stat().st_mode & 0o777 == 0o700
    assert snapshot.stat().st_mode & 0o777 == 0o600
    ledger.close()


def test_ledger_rejects_symlink_state_path(plugin, tmp_path):
    target = tmp_path / "target.sqlite3"
    target.write_text("")
    link = tmp_path / "ledger.sqlite3"
    link.symlink_to(target)
    with pytest.raises(plugin.ConfigurationError):
        plugin.IdentityLedger(link)


def test_ledger_upgrades_v1_to_v3_retry_and_profile_failure_schema(plugin, tmp_path):
    path = tmp_path / "ledger.sqlite3"
    db = sqlite3.connect(path)
    db.execute("CREATE TABLE records (digest TEXT PRIMARY KEY, external_id INTEGER NOT NULL UNIQUE, profile_scope TEXT NOT NULL, target TEXT NOT NULL, source TEXT NOT NULL, content TEXT NOT NULL, status TEXT NOT NULL, error TEXT NOT NULL, updated_at TEXT NOT NULL)")
    db.execute("CREATE TABLE mutation_failures (id INTEGER PRIMARY KEY AUTOINCREMENT, action TEXT NOT NULL, target TEXT NOT NULL, content TEXT NOT NULL, old_text TEXT NOT NULL, error_code TEXT NOT NULL, error TEXT NOT NULL, created_at TEXT NOT NULL)")
    db.execute("PRAGMA user_version=1")
    db.commit()
    db.close()
    ledger = plugin.IdentityLedger(path)
    assert ledger._db.execute("PRAGMA user_version").fetchone()[0] == 3
    assert ledger._db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='reconciliation_retries'").fetchone()[0] == "reconciliation_retries"
    columns = {row[1] for row in ledger._db.execute("PRAGMA table_info(mutation_failures)")}
    assert "profile_scope" in columns
    ledger.close()
