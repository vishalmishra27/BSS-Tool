"""
PostgreSQL persistence layer — local fallback when COSMOS=OFF.

Mirrors the CosmosStore interface (same method signatures) so that all
consumers using ``get_cosmos_store()`` work transparently.

Only 6 of the 8 Cosmos containers are replicated:
  1. Sessions       → sessions table
  2. Messages       → messages table
  3. Agent-memory   → agent_memory table
  4. Users          → users table
  5. Projects       → projects table
  6. Documents      → documents table

Embeddings and Context-cache are **skipped** (methods return None / []).
RAG is disabled entirely when this store is active.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extras
from psycopg2 import pool

logger = logging.getLogger("server.postgres_store")

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.environ.get("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.environ.get("POSTGRES_DB", "controlxai")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class PostgresStore:
    """Thread-safe PostgreSQL store — drop-in replacement for CosmosStore."""

    def __init__(self) -> None:
        self._available = False
        self._pool: Optional[pool.ThreadedConnectionPool] = None
        try:
            self._pool = pool.ThreadedConnectionPool(
                minconn=1, maxconn=10,
                host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB,
                user=POSTGRES_USER, password=POSTGRES_PASSWORD,
            )
            self._run_migration()
            self._available = True
            logger.info("PostgresStore initialised — host=%s db=%s", POSTGRES_HOST, POSTGRES_DB)
        except Exception as exc:
            logger.error("PostgresStore init failed: %s", exc)
            self._available = False

    def _run_migration(self) -> None:
        sql_path = Path(__file__).parent / "migrations" / "init_postgres.sql"
        if not sql_path.exists():
            logger.warning("PostgresStore: migration not found at %s", sql_path)
            return
        sql = sql_path.read_text(encoding="utf-8")
        conn = self._pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()
            logger.info("PostgresStore: migration executed")
        finally:
            self._pool.putconn(conn)

    def _get_conn(self):
        return self._pool.getconn()

    def _put_conn(self, conn):
        self._pool.putconn(conn)

    @property
    def available(self) -> bool:
        return self._available

    @property
    def sessions_container(self):
        return None

    # ═══════════════════════════════════════════════════════════════════════════
    # MESSAGES
    # ═══════════════════════════════════════════════════════════════════════════

    def save_message(self, session_id: str, role: str, content: str,
                     message_id: Optional[str] = None, tool_name: Optional[str] = None,
                     tool_call_id: Optional[str] = None, tool_args: Optional[List[Dict]] = None,
                     metadata: Optional[Dict] = None) -> Optional[Dict]:
        if not self._available:
            return None
        try:
            doc_id = message_id or str(uuid.uuid4())
            now = _now()
            conn = self._get_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """INSERT INTO messages (id, session_id, role, content, tool_name, tool_call_id, tool_args, metadata, created_at)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                           ON CONFLICT (id) DO UPDATE SET content=EXCLUDED.content, tool_name=EXCLUDED.tool_name,
                           tool_call_id=EXCLUDED.tool_call_id, tool_args=EXCLUDED.tool_args, metadata=EXCLUDED.metadata""",
                        (doc_id, session_id, role, content, tool_name, tool_call_id,
                         json.dumps(tool_args) if tool_args else None,
                         json.dumps(metadata) if metadata else None, now))
                conn.commit()
            finally:
                self._put_conn(conn)
            return {"id": doc_id, "sessionId": session_id, "role": role, "content": content,
                    "toolName": tool_name, "toolCallId": tool_call_id, "toolArgs": tool_args,
                    "metadata": metadata, "createdAt": now}
        except Exception as exc:
            logger.warning("PostgresStore.save_message failed (non-fatal): %s", exc)
            return None

    def get_messages(self, session_id: str, limit: int = 100) -> List[Dict]:
        if not self._available:
            return []
        try:
            conn = self._get_conn()
            try:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute("SELECT * FROM messages WHERE session_id = %s ORDER BY created_at ASC LIMIT %s",
                                (session_id, limit))
                    rows = cur.fetchall()
            finally:
                self._put_conn(conn)
            return [{"id": r["id"], "sessionId": r["session_id"], "role": r["role"], "content": r["content"],
                     "toolName": r["tool_name"], "toolCallId": r["tool_call_id"], "toolArgs": r["tool_args"],
                     "metadata": r["metadata"],
                     "createdAt": r["created_at"].isoformat() if r["created_at"] else None} for r in rows]
        except Exception as exc:
            logger.warning("PostgresStore.get_messages failed (non-fatal): %s", exc)
            return []

    def delete_messages(self, session_id: str) -> int:
        if not self._available:
            return 0
        try:
            conn = self._get_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM messages WHERE session_id = %s", (session_id,))
                    deleted = cur.rowcount
                conn.commit()
            finally:
                self._put_conn(conn)
            return deleted
        except Exception as exc:
            logger.warning("PostgresStore.delete_messages failed (non-fatal): %s", exc)
            return 0

    # ═══════════════════════════════════════════════════════════════════════════
    # AGENT-MEMORY
    # ═══════════════════════════════════════════════════════════════════════════

    def save_tool_result(self, session_id: str, tool_key: str, result_json: str) -> None:
        if not self._available:
            return
        try:
            item_id = f"{session_id}_{tool_key}"
            conn = self._get_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """INSERT INTO agent_memory (id, session_id, key, value, updated_at)
                           VALUES (%s,%s,%s,%s,%s)
                           ON CONFLICT (id) DO UPDATE SET value=EXCLUDED.value, updated_at=EXCLUDED.updated_at""",
                        (item_id, session_id, tool_key, result_json, _now()))
                conn.commit()
            finally:
                self._put_conn(conn)
        except Exception as exc:
            logger.warning("PostgresStore.save_tool_result failed (non-fatal): %s", exc)

    def restore_agent_memory(self, session_id: str) -> Dict[str, str]:
        if not self._available:
            return {}
        try:
            conn = self._get_conn()
            try:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute("SELECT key, value FROM agent_memory WHERE session_id = %s", (session_id,))
                    rows = cur.fetchall()
            finally:
                self._put_conn(conn)
            cache = {row["key"]: row["value"] for row in rows}
            if cache:
                logger.info("PostgresStore.restore_agent_memory: restored %d key(s) for session=%s", len(cache), session_id)
            return cache
        except Exception as exc:
            logger.warning("PostgresStore.restore_agent_memory failed (non-fatal): %s", exc)
            return {}

    def delete_agent_memory(self, session_id: str) -> int:
        if not self._available:
            return 0
        try:
            conn = self._get_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM agent_memory WHERE session_id = %s", (session_id,))
                    deleted = cur.rowcount
                conn.commit()
            finally:
                self._put_conn(conn)
            return deleted
        except Exception as exc:
            logger.warning("PostgresStore.delete_agent_memory failed (non-fatal): %s", exc)
            return 0

    # ═══════════════════════════════════════════════════════════════════════════
    # EMBEDDINGS — DISABLED
    # ═══════════════════════════════════════════════════════════════════════════

    def save_embedding(self, project_id: str, text: str, embedding: List[float],
                       source: Optional[str] = None, metadata: Optional[Dict] = None,
                       embedding_id: Optional[str] = None) -> Optional[Dict]:
        return None

    def vector_search(self, project_id: str, query_vector: List[float],
                      top_k: int = 10, metadata_type: Optional[str] = None) -> List[Dict]:
        return []

    def get_embeddings(self, project_id: str, limit: int = 100) -> List[Dict]:
        return []

    def delete_embedding(self, project_id: str, embedding_id: str) -> bool:
        return False

    # ═══════════════════════════════════════════════════════════════════════════
    # CONTEXT-CACHE — DISABLED
    # ═══════════════════════════════════════════════════════════════════════════

    def save_context_cache(self, session_id: str, key: str, value: str,
                           embedding: Optional[List[float]] = None) -> None:
        pass

    def get_context_cache(self, session_id: str, key: str) -> Optional[str]:
        return None

    def context_cache_vector_search(self, session_id: str, query_vector: List[float],
                                    top_k: int = 5) -> List[Dict]:
        return []

    # ═══════════════════════════════════════════════════════════════════════════
    # USERS
    # ═══════════════════════════════════════════════════════════════════════════

    def create_user(self, user_doc: Dict) -> Optional[Dict]:
        if not self._available:
            return None
        try:
            doc = {**user_doc}
            user_id = doc.get("id") or str(uuid.uuid4())
            now = _now()
            conn = self._get_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """INSERT INTO users (id, email, name, password_hash, password_salt, role, created_at, updated_at, extra)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                           ON CONFLICT (id) DO UPDATE SET email=EXCLUDED.email, name=EXCLUDED.name,
                           password_hash=EXCLUDED.password_hash, password_salt=EXCLUDED.password_salt,
                           role=EXCLUDED.role, updated_at=EXCLUDED.updated_at, extra=EXCLUDED.extra""",
                        (user_id, doc.get("email"), doc.get("name"), doc.get("passwordHash"),
                         doc.get("passwordSalt"), doc.get("role", "user"), doc.get("createdAt", now), now,
                         json.dumps({k: v for k, v in doc.items()
                                     if k not in ("id", "email", "name", "passwordHash", "passwordSalt", "role", "createdAt", "updatedAt")})))
                conn.commit()
            finally:
                self._put_conn(conn)
            doc["id"] = user_id
            doc.setdefault("createdAt", now)
            doc["updatedAt"] = now
            return doc
        except Exception as exc:
            logger.warning("PostgresStore.create_user failed (non-fatal): %s", exc)
            return None

    def _row_to_user(self, row: Dict) -> Dict:
        doc = {"id": row["id"], "email": row["email"], "name": row["name"],
               "passwordHash": row["password_hash"], "passwordSalt": row["password_salt"],
               "role": row["role"],
               "createdAt": row["created_at"].isoformat() if row["created_at"] else None,
               "updatedAt": row["updated_at"].isoformat() if row["updated_at"] else None}
        extra = row.get("extra")
        if extra and isinstance(extra, dict):
            doc.update(extra)
        return doc

    def get_user(self, user_id: str) -> Optional[Dict]:
        if not self._available:
            return None
        try:
            conn = self._get_conn()
            try:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute("SELECT * FROM users WHERE id = %s", (user_id,))
                    row = cur.fetchone()
            finally:
                self._put_conn(conn)
            return self._row_to_user(row) if row else None
        except Exception as exc:
            logger.debug("PostgresStore.get_user: id=%s error=%s", user_id, exc)
            return None

    def get_user_by_email(self, email: str) -> Optional[Dict]:
        if not self._available:
            return None
        try:
            conn = self._get_conn()
            try:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute("SELECT * FROM users WHERE email = %s", (email,))
                    row = cur.fetchone()
            finally:
                self._put_conn(conn)
            return self._row_to_user(row) if row else None
        except Exception as exc:
            logger.warning("PostgresStore.get_user_by_email failed (non-fatal): %s", exc)
            return None

    def list_users(self, limit: int = 100) -> List[Dict]:
        if not self._available:
            return []
        try:
            conn = self._get_conn()
            try:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute("SELECT * FROM users ORDER BY created_at DESC LIMIT %s", (limit,))
                    rows = cur.fetchall()
            finally:
                self._put_conn(conn)
            return [self._row_to_user(r) for r in rows]
        except Exception as exc:
            logger.warning("PostgresStore.list_users failed (non-fatal): %s", exc)
            return []

    def update_user(self, user_id: str, updates: Dict) -> Optional[Dict]:
        if not self._available:
            return None
        try:
            existing = self.get_user(user_id)
            if not existing:
                return None
            existing.update(updates)
            existing["updatedAt"] = _now()
            return self.create_user(existing)
        except Exception as exc:
            logger.warning("PostgresStore.update_user failed (non-fatal): %s", exc)
            return None

    def delete_user(self, user_id: str) -> bool:
        if not self._available:
            return False
        try:
            conn = self._get_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM users WHERE id = %s", (user_id,))
                    deleted = cur.rowcount
                conn.commit()
            finally:
                self._put_conn(conn)
            return deleted > 0
        except Exception as exc:
            logger.warning("PostgresStore.delete_user failed (non-fatal): %s", exc)
            return False

    # ═══════════════════════════════════════════════════════════════════════════
    # PROJECTS
    # ═══════════════════════════════════════════════════════════════════════════

    def create_project(self, project_doc: Dict) -> Optional[Dict]:
        if not self._available:
            return None
        try:
            doc = {**project_doc}
            project_id = doc.get("id") or str(uuid.uuid4())
            created_by = doc.get("createdBy")
            if not created_by:
                logger.warning("PostgresStore.create_project: 'createdBy' is required")
                return None
            now = _now()
            conn = self._get_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """INSERT INTO projects (id, created_by, name, description, status, created_at, updated_at, extra)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                           ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name, description=EXCLUDED.description,
                           status=EXCLUDED.status, updated_at=EXCLUDED.updated_at, extra=EXCLUDED.extra""",
                        (project_id, created_by, doc.get("name"), doc.get("description"),
                         doc.get("status", "active"), doc.get("createdAt", now), now,
                         json.dumps({k: v for k, v in doc.items()
                                     if k not in ("id", "createdBy", "name", "description", "status", "createdAt", "updatedAt")})))
                conn.commit()
            finally:
                self._put_conn(conn)
            doc["id"] = project_id
            doc.setdefault("status", "active")
            doc.setdefault("createdAt", now)
            doc["updatedAt"] = now
            return doc
        except Exception as exc:
            logger.warning("PostgresStore.create_project failed (non-fatal): %s", exc)
            return None

    def _row_to_project(self, row: Dict) -> Dict:
        doc = {"id": row["id"], "createdBy": row["created_by"], "name": row["name"],
               "description": row["description"], "status": row["status"],
               "createdAt": row["created_at"].isoformat() if row["created_at"] else None,
               "updatedAt": row["updated_at"].isoformat() if row["updated_at"] else None}
        extra = row.get("extra")
        if extra and isinstance(extra, dict):
            doc.update(extra)
        return doc

    def get_project(self, project_id: str, created_by: str) -> Optional[Dict]:
        if not self._available:
            return None
        try:
            conn = self._get_conn()
            try:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute("SELECT * FROM projects WHERE id = %s AND created_by = %s", (project_id, created_by))
                    row = cur.fetchone()
            finally:
                self._put_conn(conn)
            return self._row_to_project(row) if row else None
        except Exception as exc:
            return None

    def list_projects(self, created_by: str, include_archived: bool = False) -> List[Dict]:
        if not self._available:
            return []
        try:
            conn = self._get_conn()
            try:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    if include_archived:
                        cur.execute("SELECT * FROM projects WHERE created_by = %s ORDER BY updated_at DESC", (created_by,))
                    else:
                        cur.execute("SELECT * FROM projects WHERE created_by = %s AND status != 'archived' ORDER BY updated_at DESC", (created_by,))
                    rows = cur.fetchall()
            finally:
                self._put_conn(conn)
            return [self._row_to_project(r) for r in rows]
        except Exception as exc:
            logger.warning("PostgresStore.list_projects failed (non-fatal): %s", exc)
            return []

    def update_project(self, project_id: str, created_by: str, updates: Dict) -> Optional[Dict]:
        if not self._available:
            return None
        try:
            existing = self.get_project(project_id, created_by)
            if not existing:
                return None
            existing.update(updates)
            existing["updatedAt"] = _now()
            return self.create_project(existing)
        except Exception as exc:
            logger.warning("PostgresStore.update_project failed (non-fatal): %s", exc)
            return None

    def delete_project(self, project_id: str, created_by: str) -> bool:
        if not self._available:
            return False
        try:
            conn = self._get_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM projects WHERE id = %s AND created_by = %s", (project_id, created_by))
                    deleted = cur.rowcount
                conn.commit()
            finally:
                self._put_conn(conn)
            return deleted > 0
        except Exception as exc:
            logger.warning("PostgresStore.delete_project failed (non-fatal): %s", exc)
            return False

    # ═══════════════════════════════════════════════════════════════════════════
    # DOCUMENTS
    # ═══════════════════════════════════════════════════════════════════════════

    _DOC_KNOWN_FIELDS = {
        "id", "projectId", "name", "filename", "blobPath", "blobUrl",
        "type", "size", "uploadedBy", "uploadedAt", "fileCount", "totalSize",
        "metadata", "createdAt", "updatedAt",
    }

    def create_document(self, document_doc: Dict) -> Optional[Dict]:
        if not self._available:
            return None
        try:
            doc = {**document_doc}
            doc_id = doc.get("id") or str(uuid.uuid4())
            project_id = doc.get("projectId")
            if not project_id:
                logger.warning("PostgresStore.create_document: 'projectId' is required")
                return None
            now = _now()
            name = doc.get("name") or doc.get("filename")
            conn = self._get_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """INSERT INTO documents
                           (id, project_id, name, blob_path, blob_url, type, size,
                            uploaded_by, uploaded_at, file_count, total_size,
                            metadata, created_at, updated_at, extra)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                           ON CONFLICT (id) DO UPDATE SET
                            name=EXCLUDED.name, blob_path=EXCLUDED.blob_path, blob_url=EXCLUDED.blob_url,
                            type=EXCLUDED.type, size=EXCLUDED.size, uploaded_by=EXCLUDED.uploaded_by,
                            uploaded_at=EXCLUDED.uploaded_at, file_count=EXCLUDED.file_count,
                            total_size=EXCLUDED.total_size, metadata=EXCLUDED.metadata,
                            updated_at=EXCLUDED.updated_at, extra=EXCLUDED.extra""",
                        (doc_id, project_id, name, doc.get("blobPath"), doc.get("blobUrl"),
                         doc.get("type"), doc.get("size"), doc.get("uploadedBy"), doc.get("uploadedAt"),
                         doc.get("fileCount"), doc.get("totalSize"),
                         json.dumps(doc.get("metadata")) if doc.get("metadata") else None,
                         doc.get("createdAt", now), now,
                         json.dumps({k: v for k, v in doc.items() if k not in self._DOC_KNOWN_FIELDS})))
                conn.commit()
            finally:
                self._put_conn(conn)
            doc["id"] = doc_id
            doc.setdefault("createdAt", now)
            doc["updatedAt"] = now
            return doc
        except Exception as exc:
            logger.warning("PostgresStore.create_document failed (non-fatal): %s", exc)
            return None

    def _row_to_document(self, row: Dict) -> Dict:
        doc = {"id": row["id"], "projectId": row["project_id"], "name": row["name"],
               "blobPath": row["blob_path"], "blobUrl": row["blob_url"],
               "type": row["type"], "size": row["size"],
               "uploadedBy": row["uploaded_by"],
               "uploadedAt": row["uploaded_at"].isoformat() if row.get("uploaded_at") else None,
               "fileCount": row.get("file_count"), "totalSize": row.get("total_size"),
               "metadata": row.get("metadata"),
               "createdAt": row["created_at"].isoformat() if row["created_at"] else None,
               "updatedAt": row["updated_at"].isoformat() if row["updated_at"] else None}
        extra = row.get("extra")
        if extra and isinstance(extra, dict):
            doc.update(extra)
        return doc

    def get_document(self, document_id: str, project_id: str) -> Optional[Dict]:
        if not self._available:
            return None
        try:
            conn = self._get_conn()
            try:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute("SELECT * FROM documents WHERE id = %s AND project_id = %s", (document_id, project_id))
                    row = cur.fetchone()
            finally:
                self._put_conn(conn)
            return self._row_to_document(row) if row else None
        except Exception as exc:
            return None

    def list_documents(self, project_id: str, limit: int = 100) -> List[Dict]:
        if not self._available:
            return []
        try:
            conn = self._get_conn()
            try:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute("SELECT * FROM documents WHERE project_id = %s ORDER BY created_at DESC LIMIT %s",
                                (project_id, limit))
                    rows = cur.fetchall()
            finally:
                self._put_conn(conn)
            return [self._row_to_document(r) for r in rows]
        except Exception as exc:
            logger.warning("PostgresStore.list_documents failed (non-fatal): %s", exc)
            return []

    def update_document(self, document_id: str, project_id: str, updates: Dict) -> Optional[Dict]:
        if not self._available:
            return None
        try:
            existing = self.get_document(document_id, project_id)
            if not existing:
                return None
            existing.update(updates)
            existing["updatedAt"] = _now()
            return self.create_document(existing)
        except Exception as exc:
            logger.warning("PostgresStore.update_document failed (non-fatal): %s", exc)
            return None

    def delete_document(self, document_id: str, project_id: str) -> bool:
        if not self._available:
            return False
        try:
            conn = self._get_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM documents WHERE id = %s AND project_id = %s", (document_id, project_id))
                    deleted = cur.rowcount
                conn.commit()
            finally:
                self._put_conn(conn)
            return deleted > 0
        except Exception as exc:
            logger.warning("PostgresStore.delete_document failed (non-fatal): %s", exc)
            return False
