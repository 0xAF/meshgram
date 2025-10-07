from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Iterable, Optional


DB_PATH = os.path.join("data", "bbs.db")


def utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S%z")


@dataclass
class PmRow:
    id: int
    sender_node_id: str
    sender_short: Optional[str]
    sender_long: Optional[str]
    sender_source: str
    recipient_node_id: Optional[str]
    recipient_short: Optional[str]
    recipient_long: Optional[str]
    candidates_json: Optional[str]
    text: str
    status: str
    created_at: str
    notified_at: Optional[str]
    delivered_at: Optional[str]
    read_at: Optional[str]
    archived_at: Optional[str]
    expired_at: Optional[str]
    last_attempt_at: Optional[str]
    attempt_count: int
    first_seen_enabled: Optional[int]


class BbsStore:
    """SQLite-backed store for BBS private messages and simple sessions."""

    def __init__(self, path: str = DB_PATH) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self._db = sqlite3.connect(path)
        self._db.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        cur = self._db.cursor()
        cur.executescript(
            """
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;

            CREATE TABLE IF NOT EXISTS pm_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sender_node_id TEXT NOT NULL,
                sender_short TEXT,
                sender_long TEXT,
                sender_source TEXT NOT NULL CHECK(sender_source IN ('mesh','telegram')),
                recipient_node_id TEXT,
                recipient_short TEXT,
                recipient_long TEXT,
                candidates_json TEXT,
                text TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN (
                    'queued','notified','read','expired','failed','archived','deleted'
                )),
                created_at TEXT NOT NULL,
                notified_at TEXT,
                delivered_at TEXT,
                read_at TEXT,
                archived_at TEXT,
                expired_at TEXT,
                last_attempt_at TEXT,
                attempt_count INTEGER DEFAULT 0
            );

            CREATE INDEX IF NOT EXISTS idx_pm_by_recipient ON pm_messages(recipient_node_id, status, created_at);
            CREATE INDEX IF NOT EXISTS idx_pm_by_sender ON pm_messages(sender_node_id, status, created_at);

            CREATE TABLE IF NOT EXISTS sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_node_id TEXT NOT NULL,
                kind TEXT NOT NULL CHECK(kind IN ('inbox','outbox')),
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS session_items (
                session_id INTEGER NOT NULL,
                idx INTEGER NOT NULL,
                pm_id INTEGER NOT NULL,
                PRIMARY KEY(session_id, idx),
                FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
            );
            """
        )
        # Best-effort migration: add first_seen_enabled and delivered_at columns if missing
        try:
            cur.execute("ALTER TABLE pm_messages ADD COLUMN first_seen_enabled INTEGER DEFAULT 0")
        except Exception:
            pass
        try:
            cur.execute("ALTER TABLE pm_messages ADD COLUMN delivered_at TEXT")
        except Exception:
            pass
        self._db.commit()

    # --- PM CRUD ---

    def insert_pm(
        self,
        *,
        sender_node_id: str,
        sender_short: Optional[str],
        sender_long: Optional[str],
        sender_source: str,
        recipient_node_id: Optional[str],
        recipient_short: Optional[str],
        recipient_long: Optional[str],
        candidates_json: Optional[str],
        text: str,
        first_seen_enabled: int = 0,
    ) -> int:
        cur = self._db.cursor()
        cur.execute(
            """
            INSERT INTO pm_messages (
                sender_node_id, sender_short, sender_long, sender_source,
                recipient_node_id, recipient_short, recipient_long, candidates_json,
                text, status, created_at, first_seen_enabled
            ) VALUES (?,?,?,?,?,?,?,?,?, 'queued', ?, ?)
            """,
            (
                sender_node_id, sender_short, sender_long, sender_source,
                recipient_node_id, recipient_short, recipient_long, candidates_json,
                text, utcnow(), first_seen_enabled,
            ),
        )
        self._db.commit()
        lr = cur.lastrowid
        return int(lr) if lr is not None else 0

    def list_inbox(self, recipient_node_id: str, include_archived: bool = False) -> list[PmRow]:
        status_filter = ("archived','deleted" if include_archived else "archived','deleted")
        # even when not archived, we exclude deleted always
        cur = self._db.cursor()
        cur.execute(
            """
            SELECT * FROM pm_messages
            WHERE recipient_node_id = ? AND status NOT IN ('deleted' {arch})
            ORDER BY created_at DESC, id DESC
            """.replace("{arch}", ", 'archived'" if not include_archived else ""),
            (recipient_node_id,),
        )
        return [PmRow(**dict(r)) for r in cur.fetchall()]

    def list_outbox(self, sender_node_id: str, include_archived: bool = False) -> list[PmRow]:
        cur = self._db.cursor()
        cur.execute(
            """
            SELECT * FROM pm_messages
            WHERE sender_node_id = ? AND status NOT IN ('deleted' {arch})
            ORDER BY created_at DESC, id DESC
            """.replace("{arch}", ", 'archived'" if not include_archived else ""),
            (sender_node_id,),
        )
        return [PmRow(**dict(r)) for r in cur.fetchall()]

    def get_pm(self, pm_id: int) -> Optional[PmRow]:
        cur = self._db.cursor()
        cur.execute("SELECT * FROM pm_messages WHERE id = ?", (pm_id,))
        row = cur.fetchone()
        return PmRow(**dict(row)) if row else None

    def mark_read(self, pm_id: int) -> None:
        cur = self._db.cursor()
        cur.execute(
            "UPDATE pm_messages SET status = CASE WHEN status <> 'archived' THEN 'read' ELSE status END, read_at = ? WHERE id = ?",
            (utcnow(), pm_id),
        )
        self._db.commit()

    def mark_notified(self, pm_id: int) -> None:
        cur = self._db.cursor()
        # Only stamp notified_at; do not change status so 'sent' can be reserved for delivered_at
        cur.execute(
            "UPDATE pm_messages SET notified_at = ? WHERE id = ?",
            (utcnow(), pm_id),
        )
        self._db.commit()

    def archive(self, pm_id: int) -> None:
        cur = self._db.cursor()
        cur.execute(
            "UPDATE pm_messages SET status = 'archived', archived_at = ? WHERE id = ?",
            (utcnow(), pm_id),
        )
        self._db.commit()

    def delete_hard(self, pm_id: int) -> None:
        cur = self._db.cursor()
        cur.execute("DELETE FROM pm_messages WHERE id = ?", (pm_id,))
        self._db.commit()

    def expire_old(self, older_than_days: int) -> int:
        cur = self._db.cursor()
        cur.execute(
            "UPDATE pm_messages SET status = 'expired', expired_at = ? WHERE status IN ('queued','notified','read') AND created_at < ?",
            (utcnow(), (datetime.now(timezone.utc) - timedelta(days=older_than_days)).strftime("%Y-%m-%d %H:%M:%S%z")),
        )
        self._db.commit()
        return cur.rowcount

    def update_recipient(self, pm_id: int, *, node_id: str, short: Optional[str], long: Optional[str]) -> None:
        """Finalize recipient for a queued PM (keeps status as-is).

        Also clears candidates_json since a concrete target is chosen.
        """
        cur = self._db.cursor()
        cur.execute(
            "UPDATE pm_messages SET recipient_node_id = ?, recipient_short = ?, recipient_long = ?, candidates_json = NULL WHERE id = ?",
            (node_id, short, long, pm_id),
        )
        self._db.commit()

    # --- Sessions ---

    def create_session(self, user_node_id: str, kind: str, ttl_seconds: int, pm_ids: list[int]) -> int:
        now = datetime.now(timezone.utc)
        cur = self._db.cursor()
        cur.execute(
            "INSERT INTO sessions (user_node_id, kind, created_at, expires_at) VALUES (?,?,?,?)",
            (
                user_node_id,
                kind,
                now.strftime("%Y-%m-%d %H:%M:%S%z"),
                (now + timedelta(seconds=ttl_seconds)).strftime("%Y-%m-%d %H:%M:%S%z"),
            ),
        )
        lr = cur.lastrowid
        session_id = int(lr) if lr is not None else 0
        cur.executemany(
            "INSERT INTO session_items (session_id, idx, pm_id) VALUES (?,?,?)",
            [(session_id, idx + 1, pm_id) for idx, pm_id in enumerate(pm_ids)],
        )
        self._db.commit()
        return session_id

    def resolve_session_index(self, user_node_id: str, kind: str, idx: int) -> Optional[int]:
        cur = self._db.cursor()
        cur.execute(
            """
            SELECT si.pm_id FROM sessions s
            JOIN session_items si ON si.session_id = s.id
            WHERE s.user_node_id = ? AND s.kind = ? AND s.expires_at > ? AND si.idx = ?
            ORDER BY s.id DESC LIMIT 1
            """,
            (user_node_id, kind, utcnow(), idx),
        )
        row = cur.fetchone()
        return int(row[0]) if row else None

    # --- Quota ---

    def count_queued_for_sender(self, sender_node_id: str) -> int:
        cur = self._db.cursor()
        cur.execute(
            "SELECT COUNT(1) FROM pm_messages WHERE sender_node_id = ? AND status IN ('queued','notified')",
            (sender_node_id,),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0

    def find_queued_first_seen_for_node(self, candidate_node_id: str) -> list[PmRow]:
        """Return queued PMs that target 'first-seen' where this node id is a candidate.

        We identify such PMs by recipient_node_id IS NULL and status = 'queued', and
        candidates_json containing the target node id.
        """
        cur = self._db.cursor()
        # Simple LIKE search for JSON substring; avoids JSON1 dependency
        with_bang = candidate_node_id if candidate_node_id.startswith('!') else f'!{candidate_node_id}'
        no_bang = candidate_node_id[1:] if candidate_node_id.startswith('!') else candidate_node_id
        # JSON produced by Python typically has a space after colon: "node_id": "!abc".
        # Match both with and without spaces, and do case-insensitive match.
        needle1 = f'"node_id":"{with_bang}"'.lower()
        needle1b = f'"node_id": "{with_bang}"'.lower()
        needle2 = f'"node_id":"{no_bang}"'.lower()
        needle2b = f'"node_id": "{no_bang}"'.lower()
        cur.execute(
            """
            SELECT * FROM pm_messages
                        WHERE recipient_node_id IS NULL
                            AND status = 'queued'
                            AND first_seen_enabled = 1
              AND candidates_json IS NOT NULL
              AND (
                    instr(LOWER(candidates_json), ?) > 0 OR instr(LOWER(candidates_json), ?) > 0
                 OR instr(LOWER(candidates_json), ?) > 0 OR instr(LOWER(candidates_json), ?) > 0
              )
            ORDER BY created_at ASC, id ASC
            """,
            (needle1, needle1b, needle2, needle2b),
        )
        return [PmRow(**dict(r)) for r in cur.fetchall()]

    def set_first_seen_enabled(self, pm_id: int, enabled: bool) -> None:
        cur = self._db.cursor()
        cur.execute(
            "UPDATE pm_messages SET first_seen_enabled = ? WHERE id = ?",
            (1 if enabled else 0, pm_id),
        )
        self._db.commit()

    # --- Convenience helpers ---

    def unread_count_for_recipient(self, recipient_node_id: str) -> int:
        cur = self._db.cursor()
        cur.execute(
            "SELECT COUNT(1) FROM pm_messages WHERE recipient_node_id = ? AND status IN ('queued','notified')",
            (recipient_node_id,),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0

    def latest_notified_at(self, recipient_node_id: str) -> Optional[str]:
        cur = self._db.cursor()
        cur.execute(
            "SELECT MAX(notified_at) FROM pm_messages WHERE recipient_node_id = ?",
            (recipient_node_id,),
        )
        row = cur.fetchone()
        return row[0] if row and row[0] else None

    def mark_all_notified(self, recipient_node_id: str) -> int:
        cur = self._db.cursor()
        # Only stamp notified_at for queued messages; keep status unchanged
        cur.execute(
            "UPDATE pm_messages SET notified_at = ? WHERE recipient_node_id = ? AND status = 'queued'",
            (utcnow(), recipient_node_id),
        )
        self._db.commit()
        return cur.rowcount

    def mark_delivered_for_recipient(self, recipient_node_id: str) -> int:
        """Mark all undelivered messages for this recipient as delivered now.

        We consider messages with delivered_at IS NULL and a concrete recipient.
        Status is not changed here; listing logic can treat delivered_at as 'sent'.
        """
        cur = self._db.cursor()
        cur.execute(
            """
            UPDATE pm_messages
            SET delivered_at = ?
            WHERE recipient_node_id = ?
              AND delivered_at IS NULL
              AND status IN ('queued','notified','read')
            """,
            (utcnow(), recipient_node_id),
        )
        self._db.commit()
        return cur.rowcount

    def archive_all_read_for_sender(self, sender_node_id: str) -> int:
        cur = self._db.cursor()
        cur.execute(
            "UPDATE pm_messages SET status = 'archived', archived_at = ? WHERE sender_node_id = ? AND status = 'read'",
            (utcnow(), sender_node_id),
        )
        self._db.commit()
        return cur.rowcount

    def close(self) -> None:
        try:
            self._db.close()
        except Exception:
            pass
