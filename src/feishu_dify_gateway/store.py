from __future__ import annotations

import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class DeliveryLedgerEntry:
    event_id: str
    source: str
    status: str
    transport_accepted: bool
    delivery_confirmed: bool
    attempts: int
    last_error_code: str
    created_at: int
    updated_at: int
    transport_accepted_at: int | None
    delivery_confirmed_at: int | None
    next_retry_at: int | None
    terminal_at: int | None
    synthetic: bool

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> DeliveryLedgerEntry:
        return cls(
            event_id=str(row["event_id"]),
            source=str(row["source"]),
            status=str(row["status"]),
            transport_accepted=bool(row["transport_accepted"]),
            delivery_confirmed=bool(row["delivery_confirmed"]),
            attempts=int(row["attempts"]),
            last_error_code=str(row["last_error_code"]),
            created_at=int(row["created_at"]),
            updated_at=int(row["updated_at"]),
            transport_accepted_at=(
                None if row["transport_accepted_at"] is None else int(row["transport_accepted_at"])
            ),
            delivery_confirmed_at=(
                None if row["delivery_confirmed_at"] is None else int(row["delivery_confirmed_at"])
            ),
            next_retry_at=None if row["next_retry_at"] is None else int(row["next_retry_at"]),
            terminal_at=None if row["terminal_at"] is None else int(row["terminal_at"]),
            synthetic=bool(row["synthetic"]),
        )


@dataclass(frozen=True, slots=True)
class AdministrativeCommunicationLedgerEntry:
    event_id: str
    body_digest: str
    recipient_digest: str
    status: str
    transport_accepted: bool
    delivery_confirmed: bool
    attempts: int
    provider_message_ref: str | None
    last_error_code: str
    created_at: int
    updated_at: int
    transport_accepted_at: int | None
    delivery_confirmed_at: int | None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> AdministrativeCommunicationLedgerEntry:
        return cls(
            event_id=str(row["event_id"]),
            body_digest=str(row["body_digest"]),
            recipient_digest=str(row["recipient_digest"]),
            status=str(row["status"]),
            transport_accepted=bool(row["transport_accepted"]),
            delivery_confirmed=bool(row["delivery_confirmed"]),
            attempts=int(row["attempts"]),
            provider_message_ref=(
                None if row["provider_message_ref"] is None else str(row["provider_message_ref"])
            ),
            last_error_code=str(row["last_error_code"]),
            created_at=int(row["created_at"]),
            updated_at=int(row["updated_at"]),
            transport_accepted_at=(
                None if row["transport_accepted_at"] is None else int(row["transport_accepted_at"])
            ),
            delivery_confirmed_at=(
                None if row["delivery_confirmed_at"] is None else int(row["delivery_confirmed_at"])
            ),
        )


class StateStore:
    """SQLite state containing metadata only; message bodies are never persisted."""

    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._connection.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        with self._lock:
            self._connection.executescript(
                """
                PRAGMA journal_mode=WAL;
                PRAGMA synchronous=FULL;
                CREATE TABLE IF NOT EXISTS processed_events (
                    event_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    created_at INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS conversations (
                    user_hash TEXT PRIMARY KEY,
                    conversation_id TEXT NOT NULL,
                    updated_at INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS notification_delivery_ledger (
                    event_id TEXT PRIMARY KEY,
                    source TEXT NOT NULL,
                    status TEXT NOT NULL,
                    transport_accepted INTEGER NOT NULL DEFAULT 0,
                    delivery_confirmed INTEGER NOT NULL DEFAULT 0,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_error_code TEXT NOT NULL DEFAULT '',
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    transport_accepted_at INTEGER,
                    delivery_confirmed_at INTEGER,
                    next_retry_at INTEGER,
                    terminal_at INTEGER,
                    synthetic INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS administrative_communication_ledger (
                    event_id TEXT PRIMARY KEY,
                    body_digest TEXT NOT NULL,
                    recipient_digest TEXT NOT NULL,
                    status TEXT NOT NULL,
                    transport_accepted INTEGER NOT NULL DEFAULT 0,
                    delivery_confirmed INTEGER NOT NULL DEFAULT 0,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    provider_message_ref TEXT,
                    last_error_code TEXT NOT NULL DEFAULT '',
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    transport_accepted_at INTEGER,
                    delivery_confirmed_at INTEGER
                );
                """
            )

    def is_processed(self, event_id: str) -> bool:
        with self._lock:
            row = self._connection.execute(
                "SELECT 1 FROM processed_events WHERE event_id = ?", (event_id,)
            ).fetchone()
        return row is not None

    def claim_event(self, event_id: str) -> bool:
        with self._lock:
            cursor = self._connection.execute(
                "INSERT OR IGNORE INTO processed_events(event_id, status, created_at) "
                "VALUES (?, 'processing', ?)",
                (event_id, int(time.time())),
            )
        return cursor.rowcount == 1

    def mark_processed(self, event_id: str, status: str = "delivered") -> None:
        with self._lock:
            self._connection.execute(
                "INSERT INTO processed_events(event_id, status, created_at) VALUES (?, ?, ?) "
                "ON CONFLICT(event_id) DO UPDATE SET status=excluded.status",
                (event_id, status, int(time.time())),
            )

    def release_event(self, event_id: str) -> None:
        with self._lock:
            self._connection.execute(
                "DELETE FROM processed_events WHERE event_id = ? AND status = 'processing'",
                (event_id,),
            )

    def begin_delivery(self, event_id: str, source: str, now: int | None = None) -> None:
        timestamp = int(time.time()) if now is None else now
        with self._lock:
            self._connection.execute(
                """
                INSERT INTO notification_delivery_ledger(
                    event_id, source, status, transport_accepted, attempts,
                    created_at, updated_at, transport_accepted_at
                ) VALUES (?, ?, 'delivering', 1, 1, ?, ?, ?)
                ON CONFLICT(event_id) DO UPDATE SET
                    source=excluded.source,
                    status='delivering',
                    transport_accepted=1,
                    attempts=notification_delivery_ledger.attempts + 1,
                    last_error_code='',
                    updated_at=excluded.updated_at,
                    transport_accepted_at=COALESCE(
                        notification_delivery_ledger.transport_accepted_at,
                        excluded.transport_accepted_at
                    ),
                    next_retry_at=NULL,
                    terminal_at=NULL
                """,
                (event_id, source, timestamp, timestamp, timestamp),
            )

    def mark_delivery_confirmed(self, event_id: str, now: int | None = None) -> None:
        timestamp = int(time.time()) if now is None else now
        with self._lock:
            self._connection.execute(
                """
                UPDATE notification_delivery_ledger
                SET status='delivery_confirmed', delivery_confirmed=1,
                    updated_at=?, delivery_confirmed_at=?, next_retry_at=NULL,
                    terminal_at=?, last_error_code=''
                WHERE event_id=?
                """,
                (timestamp, timestamp, timestamp, event_id),
            )

    def mark_delivery_retrying(
        self, event_id: str, error_code: str, next_retry_at: int, now: int | None = None
    ) -> None:
        timestamp = int(time.time()) if now is None else now
        with self._lock:
            self._connection.execute(
                """
                UPDATE notification_delivery_ledger
                SET status='retrying', updated_at=?, last_error_code=?, next_retry_at=?,
                    terminal_at=NULL
                WHERE event_id=?
                """,
                (timestamp, error_code, next_retry_at, event_id),
            )

    def mark_delivery_permanent_failed(
        self, event_id: str, error_code: str, now: int | None = None
    ) -> None:
        timestamp = int(time.time()) if now is None else now
        with self._lock:
            self._connection.execute(
                """
                UPDATE notification_delivery_ledger
                SET status='permanent_failed', updated_at=?, last_error_code=?,
                    next_retry_at=NULL, terminal_at=?
                WHERE event_id=?
                """,
                (timestamp, error_code, timestamp, event_id),
            )

    def prepare_synthetic_delivery(
        self, event_id: str, source: str = "test", now: int | None = None
    ) -> DeliveryLedgerEntry:
        timestamp = int(time.time()) if now is None else now
        with self._lock:
            self._connection.execute(
                """
                INSERT OR IGNORE INTO notification_delivery_ledger(
                    event_id, source, status, created_at, updated_at, synthetic
                ) VALUES (?, ?, 'prepared', ?, ?, 1)
                """,
                (event_id, source, timestamp, timestamp),
            )
            row = self._connection.execute(
                "SELECT * FROM notification_delivery_ledger WHERE event_id = ?", (event_id,)
            ).fetchone()
        if row is None:
            raise RuntimeError("Synthetic delivery ledger entry was not created")
        return DeliveryLedgerEntry.from_row(row)

    def delivery_for(self, event_id: str) -> DeliveryLedgerEntry | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM notification_delivery_ledger WHERE event_id = ?", (event_id,)
            ).fetchone()
        return None if row is None else DeliveryLedgerEntry.from_row(row)

    def begin_administrative_communication(
        self,
        event_id: str,
        body_digest: str,
        recipient_digest: str,
        now: int | None = None,
    ) -> bool:
        timestamp = int(time.time()) if now is None else now
        with self._lock:
            cursor = self._connection.execute(
                """
                INSERT OR IGNORE INTO administrative_communication_ledger(
                    event_id, body_digest, recipient_digest, status,
                    attempts, created_at, updated_at
                ) VALUES (?, ?, ?, 'prepared', 0, ?, ?)
                """,
                (event_id, body_digest, recipient_digest, timestamp, timestamp),
            )
        return cursor.rowcount == 1

    def administrative_communication_for(
        self, event_id: str
    ) -> AdministrativeCommunicationLedgerEntry | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM administrative_communication_ledger WHERE event_id = ?",
                (event_id,),
            ).fetchone()
        return None if row is None else AdministrativeCommunicationLedgerEntry.from_row(row)

    def mark_administrative_communication_transport_accepted(
        self,
        event_id: str,
        provider_message_ref: str | None,
        now: int | None = None,
    ) -> None:
        timestamp = int(time.time()) if now is None else now
        with self._lock:
            self._connection.execute(
                """
                UPDATE administrative_communication_ledger
                SET status='transport_accepted', transport_accepted=1,
                    attempts=attempts + 1, provider_message_ref=?,
                    updated_at=?, transport_accepted_at=?, last_error_code=''
                WHERE event_id=?
                """,
                (provider_message_ref, timestamp, timestamp, event_id),
            )

    def mark_administrative_communication_unknown(
        self, event_id: str, error_code: str, now: int | None = None
    ) -> None:
        timestamp = int(time.time()) if now is None else now
        with self._lock:
            self._connection.execute(
                """
                UPDATE administrative_communication_ledger
                SET status='outcome_unknown', attempts=attempts + 1,
                    updated_at=?, last_error_code=?
                WHERE event_id=?
                """,
                (timestamp, error_code, event_id),
            )

    def mark_administrative_communication_failed(
        self, event_id: str, error_code: str, now: int | None = None
    ) -> None:
        timestamp = int(time.time()) if now is None else now
        with self._lock:
            self._connection.execute(
                """
                UPDATE administrative_communication_ledger
                SET status='permanent_failed', attempts=attempts + 1,
                    updated_at=?, last_error_code=?
                WHERE event_id=?
                """,
                (timestamp, error_code, event_id),
            )

    def conversation_for(self, user_hash: str) -> str:
        with self._lock:
            row = self._connection.execute(
                "SELECT conversation_id FROM conversations WHERE user_hash = ?", (user_hash,)
            ).fetchone()
        return "" if row is None else str(row["conversation_id"])

    def set_conversation(self, user_hash: str, conversation_id: str) -> None:
        with self._lock:
            self._connection.execute(
                "INSERT INTO conversations(user_hash, conversation_id, updated_at) "
                "VALUES (?, ?, ?) ON CONFLICT(user_hash) DO UPDATE SET "
                "conversation_id=excluded.conversation_id, updated_at=excluded.updated_at",
                (user_hash, conversation_id, int(time.time())),
            )

    def clear_conversation(self, user_hash: str) -> None:
        with self._lock:
            self._connection.execute("DELETE FROM conversations WHERE user_hash = ?", (user_hash,))

    def prune(self, retention_seconds: int) -> int:
        cutoff = int(time.time()) - retention_seconds
        with self._lock:
            cursor = self._connection.execute(
                "DELETE FROM processed_events WHERE created_at < ?", (cutoff,)
            )
            ledger_cursor = self._connection.execute(
                """
                DELETE FROM notification_delivery_ledger
                WHERE updated_at < ?
                  AND status IN ('prepared', 'delivery_confirmed', 'permanent_failed')
                """,
                (cutoff,),
            )
            communication_cursor = self._connection.execute(
                """
                DELETE FROM administrative_communication_ledger
                WHERE updated_at < ? AND status IN
                    ('delivery_confirmed', 'permanent_failed')
                """,
                (cutoff,),
            )
        return cursor.rowcount + ledger_cursor.rowcount + communication_cursor.rowcount

    def close(self) -> None:
        with self._lock:
            self._connection.close()
