from __future__ import annotations

import sqlite3
import threading
from datetime import UTC, datetime
from pathlib import Path


class AutodevStore:
    """Small durable adapter state store for the independent Autodev profile."""

    def __init__(self, path: Path) -> None:
        if not path.is_absolute():
            raise ValueError("Autodev state database path must be absolute")
        self._path = path
        self._lock = threading.RLock()
        path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS inbound_events (
                    event_id TEXT PRIMARY KEY,
                    event_kind TEXT NOT NULL,
                    state TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    completed_at TEXT
                );
                CREATE TABLE IF NOT EXISTS card_actions (
                    action_id TEXT PRIMARY KEY,
                    state TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS requests (
                    request_id TEXT PRIMARY KEY,
                    chat_id TEXT NOT NULL,
                    source_message_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    content_sha256 TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS chat_bindings (
                    chat_id TEXT PRIMARY KEY,
                    request_id TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS pending_interventions (
                    chat_id TEXT PRIMARY KEY,
                    request_id TEXT NOT NULL,
                    intervention_id TEXT NOT NULL,
                    notification_message_id TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS deliveries (
                    event_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    sent_at TEXT
                );
                CREATE TABLE IF NOT EXISTS metadata (
                    name TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                """
            )

    def claim_inbound(
        self,
        event_id: str,
        event_kind: str,
        *,
        stale_after_seconds: int = 300,
    ) -> bool:
        now = _now()
        with self._write() as connection:
            row = connection.execute(
                "SELECT state, started_at FROM inbound_events WHERE event_id = ?",
                (event_id,),
            ).fetchone()
            if row is None:
                connection.execute(
                    "INSERT INTO inbound_events(event_id, event_kind, state, started_at) "
                    "VALUES (?, ?, 'processing', ?)",
                    (event_id, event_kind, now),
                )
                return True
            if row[0] == "completed":
                return False
            try:
                started = datetime.fromisoformat(str(row[1]))
            except ValueError:
                started = datetime.min.replace(tzinfo=UTC)
            age = (datetime.now(UTC) - started).total_seconds()
            if age < stale_after_seconds:
                return False
            connection.execute(
                "UPDATE inbound_events SET event_kind = ?, state = 'processing', "
                "started_at = ?, completed_at = NULL WHERE event_id = ?",
                (event_kind, now, event_id),
            )
            return True

    def complete_inbound(self, event_id: str) -> None:
        with self._write() as connection:
            connection.execute(
                "UPDATE inbound_events SET state = 'completed', completed_at = ? "
                "WHERE event_id = ?",
                (_now(), event_id),
            )

    def release_inbound(self, event_id: str) -> None:
        with self._write() as connection:
            connection.execute("DELETE FROM inbound_events WHERE event_id = ?", (event_id,))

    def claim_card_action(self, action_id: str) -> bool:
        with self._write() as connection:
            try:
                connection.execute(
                    "INSERT INTO card_actions(action_id, state, created_at) VALUES (?, 'done', ?)",
                    (action_id, _now()),
                )
            except sqlite3.IntegrityError:
                return False
            return True

    def release_card_action(self, action_id: str) -> None:
        with self._write() as connection:
            connection.execute("DELETE FROM card_actions WHERE action_id = ?", (action_id,))

    def record_request(
        self,
        *,
        request_id: str,
        chat_id: str,
        source_message_id: str,
        title: str,
        content_sha256: str,
    ) -> None:
        with self._write() as connection:
            row = connection.execute(
                "SELECT content_sha256 FROM requests WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if row is not None:
                if row[0] != content_sha256:
                    raise ValueError("provider request identity was reused with different content")
                return
            connection.execute(
                "INSERT INTO requests(request_id, chat_id, source_message_id, title, "
                "content_sha256, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                (request_id, chat_id, source_message_id, title, content_sha256, _now()),
            )
            connection.execute(
                "INSERT INTO chat_bindings(chat_id, request_id) VALUES (?, ?) "
                "ON CONFLICT(chat_id) DO UPDATE SET request_id = excluded.request_id",
                (chat_id, request_id),
            )

    def request_for_chat(self, chat_id: str) -> str | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT request_id FROM chat_bindings WHERE chat_id = ?", (chat_id,)
            ).fetchone()
        return None if row is None else str(row[0])

    def chat_for_request(self, request_id: str) -> str | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT chat_id FROM requests WHERE request_id = ?", (request_id,)
            ).fetchone()
        return None if row is None else str(row[0])

    def set_pending_intervention(
        self,
        *,
        chat_id: str,
        request_id: str,
        intervention_id: str,
        notification_message_id: str,
    ) -> None:
        with self._write() as connection:
            connection.execute(
                "INSERT INTO pending_interventions(chat_id, request_id, intervention_id, "
                "notification_message_id) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(chat_id) DO UPDATE SET request_id = excluded.request_id, "
                "intervention_id = excluded.intervention_id, "
                "notification_message_id = excluded.notification_message_id",
                (chat_id, request_id, intervention_id, notification_message_id),
            )

    def pending_intervention(self, chat_id: str) -> tuple[str, str, str] | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT request_id, intervention_id, notification_message_id "
                "FROM pending_interventions WHERE chat_id = ?",
                (chat_id,),
            ).fetchone()
        return None if row is None else (str(row[0]), str(row[1]), str(row[2]))

    def clear_pending_intervention(self, chat_id: str, intervention_id: str) -> None:
        with self._write() as connection:
            connection.execute(
                "DELETE FROM pending_interventions WHERE chat_id = ? AND intervention_id = ?",
                (chat_id, intervention_id),
            )

    def operator_cursor(self) -> int:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT value FROM metadata WHERE name = 'operator_cursor'"
            ).fetchone()
        return 0 if row is None else int(row[0])

    def set_operator_cursor(self, sequence: int) -> None:
        with self._write() as connection:
            connection.execute(
                "INSERT INTO metadata(name, value) VALUES ('operator_cursor', ?) "
                "ON CONFLICT(name) DO UPDATE SET value = excluded.value",
                (str(sequence),),
            )

    def delivery_status(self, event_id: str) -> str | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT status FROM deliveries WHERE event_id = ?", (event_id,)
            ).fetchone()
        return None if row is None else str(row[0])

    def record_delivery_attempt(self, event_id: str, error: str | None = None) -> None:
        with self._write() as connection:
            connection.execute(
                "INSERT INTO deliveries(event_id, status, attempts, last_error) "
                "VALUES (?, 'pending', 1, ?) "
                "ON CONFLICT(event_id) DO UPDATE SET status = 'pending', "
                "attempts = attempts + 1, "
                "last_error = excluded.last_error",
                (event_id, error[:256] if error else None),
            )

    def mark_delivery_sent(self, event_id: str) -> None:
        with self._write() as connection:
            connection.execute(
                "INSERT INTO deliveries(event_id, status, attempts, sent_at) "
                "VALUES (?, 'sent', 0, ?) "
                "ON CONFLICT(event_id) DO UPDATE SET status = 'sent', sent_at = excluded.sent_at",
                (event_id, _now()),
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._path, timeout=5, check_same_thread=False)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = NORMAL")
        connection.execute("PRAGMA busy_timeout = 5000")
        return connection

    def _write(self) -> _WriteConnection:
        self._lock.acquire()
        connection = self._connect()
        connection.execute("BEGIN IMMEDIATE")
        return _WriteConnection(connection, self._lock)


class _WriteConnection:
    def __init__(self, connection: sqlite3.Connection, lock: threading.RLock) -> None:
        self._connection = connection
        self._lock = lock

    def __enter__(self) -> sqlite3.Connection:
        return self._connection

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        try:
            if exc_type is None:
                self._connection.commit()
            else:
                self._connection.rollback()
        finally:
            self._connection.close()
            self._lock.release()


def _now() -> str:
    return datetime.now(UTC).isoformat()
