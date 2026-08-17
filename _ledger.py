"""SQLite Identity Ledger for managing durable state, IDs, and mutations in HyperspaceDB."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional, Sequence

try:
    from ._errors import ConfigurationError
    from ._utils import _utc_now
except (ImportError, ValueError):
    from _errors import ConfigurationError
    from _utils import _utc_now


@dataclass(frozen=True)
class LedgerRecord:
    digest: str
    external_id: int
    profile_scope: str
    target: str
    source: str
    content: str
    status: str
    error: str
    updated_at: str

    def as_dict(self) -> Dict[str, Any]:
        return {
            "digest": self.digest,
            "external_id": self.external_id,
            "profile_scope": self.profile_scope,
            "target": self.target,
            "source": self.source,
            "content": self.content,
            "status": self.status,
            "error": self.error,
            "updated_at": self.updated_at,
        }


class IdentityLedger:
    """Durable mapping between logical Hermes memories and uint32 HSDB IDs."""

    def __init__(self, path: Path):
        self.path = path
        if self.path.is_symlink() or self.path.parent.is_symlink():
            raise ConfigurationError("Ledger state path must not traverse a symlink")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        os.chmod(self.path.parent, 0o700)
        if self.path.exists():
            os.chmod(self.path, 0o600)
        self._lock = threading.RLock()
        try:
            self._db = sqlite3.connect(str(path), check_same_thread=False, timeout=5.0)
            os.chmod(self.path, 0o600)
            with self._lock:
                self._db.execute("PRAGMA journal_mode=WAL")
                self._db.execute("PRAGMA synchronous=FULL")
                version = int(self._db.execute("PRAGMA user_version").fetchone()[0])
                if version > 3:
                    raise ConfigurationError("Ledger schema is newer than this plugin")
                if version < 1:
                    self._db.execute("BEGIN IMMEDIATE")
                    try:
                        self._db.execute(
                            "CREATE TABLE IF NOT EXISTS records ("
                            "digest TEXT PRIMARY KEY, external_id INTEGER NOT NULL UNIQUE, "
                            "profile_scope TEXT NOT NULL, target TEXT NOT NULL, source TEXT NOT NULL, "
                            "content TEXT NOT NULL, status TEXT NOT NULL, error TEXT NOT NULL, "
                            "updated_at TEXT NOT NULL)"
                        )
                        self._db.execute(
                            "CREATE INDEX IF NOT EXISTS idx_records_target_status "
                            "ON records(profile_scope, target, status)"
                        )
                        self._db.execute(
                            "CREATE TABLE IF NOT EXISTS mutation_failures ("
                            "id INTEGER PRIMARY KEY AUTOINCREMENT, action TEXT NOT NULL, "
                            "target TEXT NOT NULL, content TEXT NOT NULL, old_text TEXT NOT NULL, "
                            "error_code TEXT NOT NULL, error TEXT NOT NULL, created_at TEXT NOT NULL)"
                        )
                        self._db.execute("PRAGMA user_version=1")
                        self._db.commit()
                    except Exception:
                        self._db.rollback()
                        raise
                if version < 2:
                    self._db.execute("BEGIN IMMEDIATE")
                    try:
                        self._db.execute(
                            "CREATE TABLE IF NOT EXISTS reconciliation_retries "
                            "(digest TEXT PRIMARY KEY, attempts INTEGER NOT NULL, "
                            "next_retry_epoch REAL NOT NULL, updated_at TEXT NOT NULL, "
                            "FOREIGN KEY(digest) REFERENCES records(digest))"
                        )
                        self._db.execute(
                            "CREATE TABLE IF NOT EXISTS operator_receipts "
                            "(token TEXT PRIMARY KEY, operation TEXT NOT NULL, "
                            "payload TEXT NOT NULL, created_at TEXT NOT NULL)"
                        )
                        self._db.execute("PRAGMA user_version=2")
                        self._db.commit()
                    except Exception:
                        self._db.rollback()
                        raise
                if version < 3:
                    self._db.execute("BEGIN IMMEDIATE")
                    try:
                        columns = {
                            str(row[1])
                            for row in self._db.execute("PRAGMA table_info(mutation_failures)")
                        }
                        if "profile_scope" not in columns:
                            self._db.execute(
                                "ALTER TABLE mutation_failures ADD COLUMN profile_scope TEXT NOT NULL DEFAULT ''"
                            )
                        self._db.execute(
                            "CREATE INDEX IF NOT EXISTS idx_failures_profile_code "
                            "ON mutation_failures(profile_scope, error_code)"
                        )
                        self._db.execute("PRAGMA user_version=3")
                        self._db.commit()
                    except Exception:
                        self._db.rollback()
                        raise
                self._db.execute(
                    "CREATE TABLE IF NOT EXISTS operator_receipts "
                    "(token TEXT PRIMARY KEY, operation TEXT NOT NULL, "
                    "payload TEXT NOT NULL, created_at TEXT NOT NULL)"
                )
                self._db.commit()
        except sqlite3.DatabaseError as error:
            raise ConfigurationError("Identity ledger is unreadable or corrupt") from error

    @staticmethod
    def _row(row: Sequence[Any]) -> LedgerRecord:
        return LedgerRecord(*row)

    def upsert(self, record: LedgerRecord) -> None:
        with self._lock:
            self._db.execute(
                """
                INSERT INTO records
                    (digest, external_id, profile_scope, target, source, content,
                     status, error, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(digest) DO UPDATE SET
                    external_id=excluded.external_id,
                    profile_scope=excluded.profile_scope,
                    target=excluded.target,
                    source=excluded.source,
                    content=excluded.content,
                    status=excluded.status,
                    error=excluded.error,
                    updated_at=excluded.updated_at
                """,
                (
                    record.digest,
                    record.external_id,
                    record.profile_scope,
                    record.target,
                    record.source,
                    record.content,
                    record.status,
                    record.error,
                    record.updated_at,
                ),
            )
            self._db.commit()

    def get(self, digest: str) -> Optional[LedgerRecord]:
        with self._lock:
            row = self._db.execute(
                "SELECT digest, external_id, profile_scope, target, source, "
                "content, status, error, updated_at FROM records WHERE digest=?",
                (digest,),
            ).fetchone()
        return self._row(row) if row else None

    def resolve(self, profile_scope: str, target: str, old_text: str) -> List[LedgerRecord]:
        with self._lock:
            rows = self._db.execute(
                "SELECT digest, external_id, profile_scope, target, source, "
                "content, status, error, updated_at FROM records "
                "WHERE profile_scope=? AND target=? AND status IN ('active','delete_pending')",
                (profile_scope, target),
            ).fetchall()
        return [self._row(row) for row in rows if old_text in row[5]]

    def set_status(self, digest: str, status: str, error: str = "") -> None:
        with self._lock:
            self._db.execute(
                "UPDATE records SET status=?, error=?, updated_at=? WHERE digest=?",
                (status, error[:1_000], _utc_now(), digest),
            )
            self._db.commit()

    def record_failure(
        self,
        profile_scope: str,
        action: str,
        target: str,
        content: str,
        old_text: str,
        error_code: str,
        error: str,
    ) -> None:
        with self._lock:
            self._db.execute(
                "INSERT INTO mutation_failures "
                "(profile_scope,action,target,content,old_text,error_code,error,created_at) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (
                    profile_scope,
                    action,
                    target,
                    content,
                    old_text,
                    error_code,
                    error[:1_000],
                    _utc_now(),
                ),
            )
            self._db.commit()

    def audit_summary(self, profile_scope: str) -> Dict[str, Any]:
        """Return profile-scoped aggregates only; never return record content or IDs."""
        with self._lock:
            status_rows = self._db.execute(
                "SELECT status, COUNT(*) FROM records WHERE profile_scope=? "
                "GROUP BY status ORDER BY status",
                (profile_scope,),
            ).fetchall()
            failure_rows = self._db.execute(
                "SELECT error_code, COUNT(*) FROM mutation_failures WHERE profile_scope=? "
                "GROUP BY error_code ORDER BY error_code",
                (profile_scope,),
            ).fetchall()
            schema_version = int(self._db.execute("PRAGMA user_version").fetchone()[0])
        records_by_status = {str(status): int(count) for status, count in status_rows}
        failure_codes = {str(code): int(count) for code, count in failure_rows}
        return {
            "records_by_status": records_by_status,
            "reconciliation_backlog": int(records_by_status.get("delete_pending", 0)),
            "failure_count": sum(failure_codes.values()),
            "failure_codes": failure_codes,
            "ledger_schema_version": schema_version,
        }

    def active_records(
        self,
        target: Optional[str] = None,
        profile_scope: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        sql = (
            "SELECT digest, external_id, profile_scope, target, source, content, "
            "status, error, updated_at FROM records WHERE status='active'"
        )
        clauses: List[str] = []
        params: List[Any] = []
        if target is not None:
            clauses.append("target=?")
            params.append(target)
        if profile_scope is not None:
            clauses.append("profile_scope=?")
            params.append(profile_scope)
        if clauses:
            sql += " AND " + " AND ".join(clauses)
        sql += " ORDER BY target, content"
        with self._lock:
            rows = self._db.execute(sql, params).fetchall()
        return [self._row(row).as_dict() for row in rows]

    def records_with_status(
        self, status: str, limit: int, profile_scope: Optional[str] = None
    ) -> List[LedgerRecord]:
        bounded = max(1, min(int(limit), 128))
        with self._lock:
            if profile_scope is not None:
                rows = self._db.execute(
                    "SELECT digest, external_id, profile_scope, target, source, "
                    "content, status, error, updated_at FROM records WHERE status=? AND profile_scope=? "
                    "ORDER BY updated_at, external_id LIMIT ?",
                    (status, str(profile_scope), bounded),
                ).fetchall()
            else:
                rows = self._db.execute(
                    "SELECT digest, external_id, profile_scope, target, source, "
                    "content, status, error, updated_at FROM records WHERE status=? "
                    "ORDER BY updated_at, external_id LIMIT ?",
                    (status, bounded),
                ).fetchall()
        return [self._row(row) for row in rows]

    def reconciliation_due(self, digest: str, max_attempts: int, now: Optional[float] = None) -> bool:
        moment = time.time() if now is None else float(now)
        with self._lock:
            row = self._db.execute(
                "SELECT attempts, next_retry_epoch FROM reconciliation_retries WHERE digest=?",
                (digest,),
            ).fetchone()
        return row is None or (int(row[0]) < max(1, int(max_attempts)) and float(row[1]) <= moment)

    def note_reconciliation_retry(self, digest: str, base_delay: float, max_attempts: int) -> int:
        with self._lock:
            row = self._db.execute(
                "SELECT attempts FROM reconciliation_retries WHERE digest=?", (digest,)
            ).fetchone()
            attempts = min((int(row[0]) if row else 0) + 1, max(1, int(max_attempts)))
            delay = min(float(base_delay) * (2 ** min(attempts - 1, 10)), 3600.0)
            self._db.execute(
                "INSERT INTO reconciliation_retries (digest, attempts, next_retry_epoch, updated_at) "
                "VALUES (?,?,?,?) ON CONFLICT(digest) DO UPDATE SET "
                "attempts=excluded.attempts, next_retry_epoch=excluded.next_retry_epoch, "
                "updated_at=excluded.updated_at",
                (digest, attempts, time.time() + delay, _utc_now()),
            )
            self._db.commit()
        return attempts

    def clear_reconciliation_retry(self, digest: str) -> None:
        with self._lock:
            self._db.execute("DELETE FROM reconciliation_retries WHERE digest=?", (digest,))
            self._db.commit()

    def get_operator_receipt(self, token: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self._db.execute(
                "SELECT token, operation, payload, created_at FROM operator_receipts WHERE token=?",
                (token,),
            ).fetchone()
        if row is None:
            return None
        try:
            payload = json.loads(row[2])
        except Exception:
            payload = {}
        return {"token": row[0], "operation": row[1], "payload": payload, "created_at": row[3]}

    def put_operator_receipt(self, token: str, operation: str, payload: Dict[str, Any]) -> None:
        with self._lock:
            self._db.execute(
                "INSERT OR IGNORE INTO operator_receipts (token, operation, payload, created_at) "
                "VALUES (?,?,?,?)",
                (token, operation, json.dumps(payload, sort_keys=True), _utc_now()),
            )
            self._db.commit()

    def failure_count(self) -> int:
        with self._lock:
            row = self._db.execute("SELECT COUNT(*) FROM mutation_failures").fetchone()
        return int(row[0]) if row else 0

    def snapshot_to(self, destination: Path) -> Path:
        """Create an atomic SQLite-consistent copy; never copy a live WAL triplet."""
        if destination.is_symlink() or destination.parent.is_symlink():
            raise ConfigurationError("Ledger snapshot path must not traverse a symlink")
        destination.parent.mkdir(parents=True, exist_ok=True)
        os.chmod(destination.parent, 0o700)
        temporary = destination.with_name(destination.name + ".tmp")
        with self._lock:
            self._db.commit()
            target = sqlite3.connect(str(temporary))
            try:
                self._db.backup(target)
                target.commit()
            finally:
                target.close()
        os.chmod(temporary, 0o600)
        temporary.replace(destination)
        os.chmod(destination, 0o600)
        return destination

    def close(self) -> None:
        with self._lock:
            self._db.close()
