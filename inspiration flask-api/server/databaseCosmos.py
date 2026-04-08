"""
Cosmos DB database layer for Flask chat metadata.

Replaces the previous SQLite implementation.

Chat records are stored in the Cosmos DB "Sessions" container using a fixed
internal partition key value ("INTERNAL") so that Flask-managed sessions
(which are created before a projectId is known) still resolve to a consistent
partition.  Express-managed sessions in the same container use real projectId
values as their partition key.

The LangGraph checkpoint SQLite database (checkpoints.db) is kept as-is —
there is no built-in Cosmos checkpointer in LangGraph.
"""

from __future__ import annotations

import os
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

logger = logging.getLogger("server.database")

# ── Cosmos DB SDK ─────────────────────────────────────────────────────────────
from azure.cosmos import CosmosClient, exceptions as CosmosExceptions

COSMOS_ENDPOINT = os.environ.get("COSMOS_ENDPOINT", "")
COSMOS_KEY = os.environ.get("COSMOS_KEY", "")
COSMOS_DATABASE = os.environ.get("COSMOS_DATABASE", "")
SESSIONS_CONTAINER = "Sessions"

# Flask-internal sessions have no real projectId yet, so we use this sentinel.
INTERNAL_PARTITION_KEY = "INTERNAL"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Database:
    """
    Cosmos DB-backed store for Flask chat session metadata.

    Each chat document in the Sessions container has the shape:
        {
          "id":          <uuid>,          # Cosmos document id AND Flask chat id
          "projectId":   "INTERNAL",      # fixed partition key for Flask-internal records
          "title":       "New Chat",
          "created_at":  <ISO timestamp>,
          "updated_at":  <ISO timestamp>,
          "is_archived": false,
          "flaskSource": "flask"          # distinguishes from Express-created records
        }
    """

    def __init__(self):
        if not COSMOS_ENDPOINT or not COSMOS_KEY or not COSMOS_DATABASE:
            raise RuntimeError(
                "Cosmos DB environment variables missing: "
                "COSMOS_ENDPOINT, COSMOS_KEY, COSMOS_DATABASE must be set."
            )
        logger.info("Database: connecting to Cosmos DB endpoint=%s database=%s", COSMOS_ENDPOINT[:30], COSMOS_DATABASE)
        self._client = CosmosClient(COSMOS_ENDPOINT, credential=COSMOS_KEY, connection_verify=False)
        self._db = self._client.get_database_client(COSMOS_DATABASE)
        self._container = self._db.get_container_client(SESSIONS_CONTAINER)

    def initialize(self) -> None:
        """
        Verify connectivity.  Containers are pre-created in Azure Portal.
        This method performs a lightweight read to confirm access.
        """
        try:
            props = self._container.read()
            logger.info(
                "Cosmos Sessions container ready: %s",
                props.get("id", SESSIONS_CONTAINER),
            )
        except Exception as exc:
            logger.error("Failed to connect to Cosmos Sessions container: %s", exc)
            raise

    # ── Internal helpers ────────────────────────────────────────────────────

    def _create_doc(self, chat_id: str, project_id: Optional[str] = None) -> Dict:
        """Build a new chat document."""
        now = _now()
        return {
            "id": chat_id,
            "projectId": project_id or INTERNAL_PARTITION_KEY,
            "title": "New Chat",
            "created_at": now,
            "updated_at": now,
            "is_archived": False,
            "flaskSource": "flask",   # NOTE: never use _prefix fields — Cosmos reserves those
        }

    def _read_item(self, chat_id: str, project_id: Optional[str] = None) -> Optional[Dict]:
        """Point-read a chat document by id. Tries project_id first, then INTERNAL."""
        partitions = []
        if project_id:
            partitions.append(project_id)
        if INTERNAL_PARTITION_KEY not in partitions:
            partitions.append(INTERNAL_PARTITION_KEY)
        for pk in partitions:
            try:
                item = self._container.read_item(
                    item=chat_id, partition_key=pk
                )
                return item
            except CosmosExceptions.CosmosResourceNotFoundError:
                continue
        logger.debug("Database._read_item: chat_id=%s not found", chat_id)
        return None

    def _public_view(self, doc: Dict) -> Dict:
        """Return the fields that callers expect (matches the old SQLite row shape)."""
        pid = doc.get("projectId")
        return {
            "id": doc["id"],
            "title": doc.get("title", "New Chat"),
            "created_at": doc.get("created_at", ""),
            "updated_at": doc.get("updated_at", ""),
            "is_archived": bool(doc.get("is_archived", False)),
            "projectId": None if pid == INTERNAL_PARTITION_KEY else pid,
        }

    # ── Chat CRUD ────────────────────────────────────────────────────────────

    def create_chat(self, chat_id: str, project_id: Optional[str] = None) -> Dict:
        doc = self._create_doc(chat_id, project_id=project_id)
        self._container.create_item(body=doc)
        logger.info("Database.create_chat: created chat_id=%s project_id=%s", chat_id, doc["projectId"])
        return self._public_view(doc)

    def list_chats(self, include_archived: bool = False, project_id: Optional[str] = None) -> List[Dict]:
        """
        List Flask-internal chats ordered by updated_at DESC.
        Only returns records with flaskSource = "flask" (ignores Express sessions).
        If project_id is provided, uses it as the partition key instead of INTERNAL.
        """
        pk = project_id or INTERNAL_PARTITION_KEY
        if include_archived:
            query = (
                "SELECT * FROM c WHERE c.projectId = @pk AND c.flaskSource = 'flask' "
                "ORDER BY c.updated_at DESC"
            )
        else:
            query = (
                "SELECT * FROM c WHERE c.projectId = @pk AND c.flaskSource = 'flask' "
                "AND c.is_archived = false ORDER BY c.updated_at DESC"
            )
        items = list(
            self._container.query_items(
                query=query,
                parameters=[{"name": "@pk", "value": pk}],
                partition_key=pk,
            )
        )
        logger.info("Database.list_chats: returned %d chat(s) include_archived=%s project_id=%s", len(items), include_archived, pk)
        return [self._public_view(i) for i in items]

    def get_chat(self, chat_id: str) -> Optional[Dict]:
        doc = self._read_item(chat_id)
        if doc:
            logger.debug("Database.get_chat: found chat_id=%s title=%r", chat_id, doc.get("title"))
        else:
            logger.debug("Database.get_chat: chat_id=%s not found", chat_id)
        return self._public_view(doc) if doc else None

    def update_chat_title(self, chat_id: str, title: str) -> Optional[Dict]:
        doc = self._read_item(chat_id)
        if not doc:
            logger.warning("Database.update_chat_title: chat_id=%s not found", chat_id)
            return None
        doc["title"] = title
        doc["updated_at"] = _now()
        self._container.upsert_item(body=doc)
        logger.info("Database.update_chat_title: chat_id=%s → title=%r", chat_id, title)
        return self._public_view(doc)

    def touch_chat(self, chat_id: str) -> None:
        """Update the updated_at timestamp without changing any other field."""
        doc = self._read_item(chat_id)
        if not doc:
            logger.warning("Database.touch_chat: chat_id=%s not found", chat_id)
            return
        doc["updated_at"] = _now()
        self._container.upsert_item(body=doc)
        logger.debug("Database.touch_chat: chat_id=%s updated_at refreshed", chat_id)

    def archive_chat(self, chat_id: str) -> bool:
        doc = self._read_item(chat_id)
        if not doc:
            logger.warning("Database.archive_chat: chat_id=%s not found", chat_id)
            return False
        doc["is_archived"] = True
        doc["updated_at"] = _now()
        self._container.upsert_item(body=doc)
        logger.info("Database.archive_chat: archived chat_id=%s", chat_id)
        return True

    def close(self) -> None:
        """No persistent connections to close for the Cosmos SDK."""
        logger.info("Database.close called (no-op for Cosmos SDK)")
