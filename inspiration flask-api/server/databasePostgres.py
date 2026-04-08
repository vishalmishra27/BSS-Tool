"""
PostgreSQL database layer for Flask chat metadata — used when COSMOS=OFF.

Drop-in replacement for databaseCosmos.py Database class.
Uses the same sessions table created by migrations/init_postgres.sql.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional

import psycopg2
import psycopg2.extras
from psycopg2 import pool

logger = logging.getLogger("server.databasePostgres")

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.environ.get("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.environ.get("POSTGRES_DB", "controlxai")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")

INTERNAL_PARTITION_KEY = "INTERNAL"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Database:
    """
    PostgreSQL-backed store for Flask chat session metadata.

    Same public interface as databaseCosmos.Database so that all callers
    (app.py, routes) work unchanged.
    """

    def __init__(self):
        logger.info(
            "Database(Postgres): connecting to host=%s port=%s db=%s",
            POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB,
        )
        self._pool = pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=5,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )

    def initialize(self) -> None:
        """Verify connectivity by running a lightweight query."""
        conn = self._pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            logger.info("Database(Postgres): sessions table ready")
        except Exception as exc:
            logger.error("Database(Postgres): connectivity check failed: %s", exc)
            raise
        finally:
            self._pool.putconn(conn)

    def _get_conn(self):
        return self._pool.getconn()

    def _put_conn(self, conn):
        self._pool.putconn(conn)

    def _public_view(self, row: Dict) -> Dict:
        pid = row.get("project_id")
        return {
            "id": row["id"],
            "title": row.get("title", "New Chat"),
            "created_at": row["created_at"].isoformat() if row.get("created_at") else "",
            "updated_at": row["updated_at"].isoformat() if row.get("updated_at") else "",
            "is_archived": bool(row.get("is_archived", False)),
            "projectId": None if pid == INTERNAL_PARTITION_KEY else pid,
        }

    def create_chat(self, chat_id: str, project_id: Optional[str] = None) -> Dict:
        now = _now()
        pk = project_id or INTERNAL_PARTITION_KEY
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO sessions (id, project_id, title, created_at, updated_at, is_archived, flask_source)
                       VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                    (chat_id, pk, "New Chat", now, now, False, "flask"),
                )
            conn.commit()
        finally:
            self._put_conn(conn)
        logger.info("Database(Postgres).create_chat: id=%s project_id=%s", chat_id, pk)
        return {
            "id": chat_id, "title": "New Chat", "created_at": now, "updated_at": now,
            "is_archived": False, "projectId": None if pk == INTERNAL_PARTITION_KEY else pk,
        }

    def list_chats(self, include_archived: bool = False, project_id: Optional[str] = None) -> List[Dict]:
        pk = project_id or INTERNAL_PARTITION_KEY
        conn = self._get_conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                if include_archived:
                    cur.execute(
                        "SELECT * FROM sessions WHERE project_id = %s AND flask_source = 'flask' ORDER BY updated_at DESC", (pk,))
                else:
                    cur.execute(
                        "SELECT * FROM sessions WHERE project_id = %s AND flask_source = 'flask' AND is_archived = FALSE ORDER BY updated_at DESC", (pk,))
                rows = cur.fetchall()
        finally:
            self._put_conn(conn)
        return [self._public_view(r) for r in rows]

    def get_chat(self, chat_id: str) -> Optional[Dict]:
        conn = self._get_conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM sessions WHERE id = %s", (chat_id,))
                row = cur.fetchone()
        finally:
            self._put_conn(conn)
        return self._public_view(row) if row else None

    def update_chat_title(self, chat_id: str, title: str) -> Optional[Dict]:
        now = _now()
        conn = self._get_conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("UPDATE sessions SET title = %s, updated_at = %s WHERE id = %s RETURNING *", (title, now, chat_id))
                row = cur.fetchone()
            conn.commit()
        finally:
            self._put_conn(conn)
        if not row:
            return None
        return self._public_view(row)

    def touch_chat(self, chat_id: str) -> None:
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("UPDATE sessions SET updated_at = %s WHERE id = %s", (_now(), chat_id))
            conn.commit()
        finally:
            self._put_conn(conn)

    def archive_chat(self, chat_id: str) -> bool:
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("UPDATE sessions SET is_archived = TRUE, updated_at = %s WHERE id = %s", (_now(), chat_id))
                updated = cur.rowcount
            conn.commit()
        finally:
            self._put_conn(conn)
        return updated > 0

    def close(self) -> None:
        if self._pool:
            self._pool.closeall()
            logger.info("Database(Postgres).close: connection pool closed")
