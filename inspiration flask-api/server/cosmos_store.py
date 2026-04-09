"""
Cosmos DB persistence layer for the Control Testing Agent.

Manages all 8 Cosmos DB containers in a single database:

  1. Sessions       (partition key /projectId)   — chat session metadata
  2. Messages       (partition key /sessionId)   — chat message history
  3. Agent-memory   (partition key /sessionId)   — durable tool results
  4. Embeddings     (partition key /projectId)    — vector embeddings (1536-dim float32)
  5. Context-cache  (partition key /sessionId)    — TTL=3600s short-lived cache
                                                    with vector indexing (1536-dim float32)
  6. Users          (partition key /id)           — user profiles
  7. Projects       (partition key /createdBy)    — project records
  8. Documents      (partition key /projectId)    — document metadata (files in Blob Storage)

Usage:
  store = CosmosStore()          # singleton, initialise once at startup

  # After a tool_end event:
  store.save_tool_result(session_id, tool_key, result_json)

  # On page-refresh (session-results endpoint):
  cache = store.load_tool_results_cache(session_id)   # from Context-cache first

  # On new AgentState creation (restore after restart):
  cache = store.restore_agent_memory(session_id)      # from Agent-memory
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger("server.cosmos_store")

COSMOS_ENDPOINT = os.environ.get("COSMOS_ENDPOINT", "")
COSMOS_KEY = os.environ.get("COSMOS_KEY", "")
COSMOS_DATABASE = os.environ.get("COSMOS_DATABASE", "")

# ── Container names ───────────────────────────────────────────────────────────
SESSIONS_CONTAINER = "Sessions"
MESSAGES_CONTAINER = "Messages"
AGENT_MEMORY_CONTAINER = "Agent-memory"
EMBEDDINGS_CONTAINER = "Embeddings"
CONTEXT_CACHE_CONTAINER = "Context-cache"
USERS_CONTAINER = "Users"
PROJECTS_CONTAINER = "Projects"
DOCUMENTS_CONTAINER = "Documents"

# ── TTL for Context-cache ─────────────────────────────────────────────────────
CONTEXT_CACHE_TTL = 3600  # seconds — must also be enabled on the container in Azure Portal

# ── Vector index configuration (1536-dim float32) ─────────────────────────────
EMBEDDING_DIMENSIONS = 1536
EMBEDDING_DATA_TYPE = "float32"

# Vector embedding policy applied to Embeddings and Context-cache containers
VECTOR_EMBEDDING_POLICY = {
    "vectorEmbeddings": [
        {
            "path": "/embedding",
            "dataType": EMBEDDING_DATA_TYPE,
            "dimensions": EMBEDDING_DIMENSIONS,
            "distanceFunction": "cosine",
        }
    ]
}

# Indexing policy with vector index on /embedding path
VECTOR_INDEXING_POLICY = {
    "indexingMode": "consistent",
    "automatic": True,
    "includedPaths": [{"path": "/*"}],
    "excludedPaths": [{"path": '/"_etag"/?'}],
    "vectorIndexes": [
        {
            "path": "/embedding",
            "type": "quantizedFlat",
        }
    ],
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class CosmosStore:
    """
    Thread-safe Cosmos DB store managing all 8 containers.

    All methods are best-effort: failures are logged but never raised,
    so the agent continues to work even if Cosmos is unreachable.
    """

    def __init__(self) -> None:
        self._available = False

        # Pre-set all container references to None for safe attribute access
        self._sessions = None
        self._messages = None
        self._agent_memory = None
        self._embeddings = None
        self._context_cache = None
        self._users = None
        self._projects = None
        self._documents = None

        if not all([COSMOS_ENDPOINT, COSMOS_KEY, COSMOS_DATABASE]):
            logger.warning(
                "CosmosStore: COSMOS_ENDPOINT / COSMOS_KEY / COSMOS_DATABASE not set — "
                "Cosmos DB persistence disabled."
            )
            return

        try:
            from azure.cosmos import CosmosClient, PartitionKey

            # connection_verify=False disables SSL cert verification
            # needed in corporate proxy/firewall environments
            client = CosmosClient(
                COSMOS_ENDPOINT,
                credential=COSMOS_KEY,
                connection_verify=False,
            )
            db = client.get_database_client(COSMOS_DATABASE)

            # ── Get or create all 8 containers ────────────────────────────

            # 1. Sessions (partition key /projectId)
            self._sessions = self._ensure_container(
                db, SESSIONS_CONTAINER, "/projectId"
            )

            # 2. Messages (partition key /sessionId)
            self._messages = self._ensure_container(
                db, MESSAGES_CONTAINER, "/sessionId"
            )

            # 3. Agent-memory (partition key /sessionId)
            self._agent_memory = self._ensure_container(
                db, AGENT_MEMORY_CONTAINER, "/sessionId"
            )

            # 4. Embeddings (partition key /projectId) — VECTOR INDEX
            self._embeddings = self._ensure_container(
                db,
                EMBEDDINGS_CONTAINER,
                "/projectId",
                vector_embedding_policy=VECTOR_EMBEDDING_POLICY,
                indexing_policy=VECTOR_INDEXING_POLICY,
            )

            # 5. Context-cache (partition key /sessionId) — TTL + VECTOR INDEX
            self._context_cache = self._ensure_container(
                db,
                CONTEXT_CACHE_CONTAINER,
                "/sessionId",
                default_ttl=CONTEXT_CACHE_TTL,
                vector_embedding_policy=VECTOR_EMBEDDING_POLICY,
                indexing_policy=VECTOR_INDEXING_POLICY,
            )

            # 6. Users (partition key /id)
            self._users = self._ensure_container(
                db, USERS_CONTAINER, "/id"
            )

            # 7. Projects (partition key /createdBy)
            self._projects = self._ensure_container(
                db, PROJECTS_CONTAINER, "/createdBy"
            )

            # 8. Documents (partition key /projectId)
            self._documents = self._ensure_container(
                db, DOCUMENTS_CONTAINER, "/projectId"
            )

            self._available = True
            logger.info(
                "CosmosStore initialised (all 8 containers) endpoint=%s database=%s",
                COSMOS_ENDPOINT[:30],
                COSMOS_DATABASE,
            )
        except Exception as exc:
            logger.error("CosmosStore init failed: %s", exc)
            self._available = False

    @staticmethod
    def _ensure_container(
        db,
        container_name: str,
        partition_key_path: str,
        default_ttl: Optional[int] = None,
        vector_embedding_policy: Optional[Dict] = None,
        indexing_policy: Optional[Dict] = None,
    ):
        """
        Get an existing container or create it if it doesn't exist.

        For containers with vector indexing or TTL, creation kwargs are passed
        only when creating a new container.
        """
        try:
            container = db.get_container_client(container_name)
            container.read()
            logger.debug("CosmosStore: container '%s' exists", container_name)
            return container
        except Exception:
            try:
                from azure.cosmos import PartitionKey

                create_kwargs: Dict[str, Any] = {
                    "id": container_name,
                    "partition_key": PartitionKey(path=partition_key_path),
                }
                if default_ttl is not None:
                    create_kwargs["default_ttl"] = default_ttl
                if vector_embedding_policy is not None:
                    create_kwargs["vector_embedding_policy"] = vector_embedding_policy
                if indexing_policy is not None:
                    create_kwargs["indexing_policy"] = indexing_policy

                container = db.create_container(**create_kwargs)
                logger.info(
                    "CosmosStore: created container '%s' (pk=%s ttl=%s vector=%s)",
                    container_name,
                    partition_key_path,
                    default_ttl,
                    vector_embedding_policy is not None,
                )
                return container
            except Exception as create_exc:
                logger.warning(
                    "CosmosStore: could not create container '%s': %s — attempting get",
                    container_name,
                    create_exc,
                )
                try:
                    return db.get_container_client(container_name)
                except Exception:
                    logger.error(
                        "CosmosStore: container '%s' unavailable", container_name
                    )
                    return None

    @property
    def available(self) -> bool:
        return self._available

    # ═══════════════════════════════════════════════════════════════════════════
    # 1. SESSIONS — managed by database.py (Database class)
    # ═══════════════════════════════════════════════════════════════════════════

    @property
    def sessions_container(self):
        """Expose Sessions container for Database class."""
        return self._sessions

    # ═══════════════════════════════════════════════════════════════════════════
    # 2. MESSAGES (partition key /sessionId)
    # ═══════════════════════════════════════════════════════════════════════════

    def save_message(
        self,
        session_id: str,
        role: str,
        content: str,
        message_id: Optional[str] = None,
        tool_name: Optional[str] = None,
        tool_call_id: Optional[str] = None,
        tool_args: Optional[List[Dict]] = None,
        metadata: Optional[Dict] = None,
    ) -> Optional[Dict]:
        """Persist a chat message to the Messages container."""
        if not self._available or not self._messages:
            return None
        try:
            doc = {
                "id": message_id or str(uuid.uuid4()),
                "sessionId": session_id,
                "role": role,
                "content": content,
                "toolName": tool_name,
                "toolCallId": tool_call_id,
                "toolArgs": tool_args,
                "metadata": metadata,
                "createdAt": _now(),
            }
            self._messages.upsert_item(body=doc)
            logger.debug(
                "CosmosStore.save_message: session=%s role=%s id=%s",
                session_id, role, doc["id"],
            )
            return doc
        except Exception as exc:
            logger.warning(
                "CosmosStore.save_message failed (non-fatal): session=%s error=%s",
                session_id, exc,
            )
            return None

    def get_messages(self, session_id: str, limit: int = 100) -> List[Dict]:
        """Retrieve messages for a session, ordered by createdAt ASC."""
        if not self._available or not self._messages:
            return []
        try:
            items = list(
                self._messages.query_items(
                    query=(
                        "SELECT * FROM c WHERE c.sessionId = @sid "
                        "ORDER BY c.createdAt ASC OFFSET 0 LIMIT @limit"
                    ),
                    parameters=[
                        {"name": "@sid", "value": session_id},
                        {"name": "@limit", "value": limit},
                    ],
                    partition_key=session_id,
                )
            )
            logger.debug(
                "CosmosStore.get_messages: session=%s count=%d", session_id, len(items)
            )
            return items
        except Exception as exc:
            logger.warning(
                "CosmosStore.get_messages failed (non-fatal): session=%s error=%s",
                session_id, exc,
            )
            return []

    def delete_messages(self, session_id: str) -> int:
        """Delete all messages for a session."""
        if not self._available or not self._messages:
            return 0
        deleted = 0
        try:
            items = list(
                self._messages.query_items(
                    query="SELECT c.id FROM c WHERE c.sessionId = @sid",
                    parameters=[{"name": "@sid", "value": session_id}],
                    partition_key=session_id,
                )
            )
            for item in items:
                try:
                    self._messages.delete_item(item=item["id"], partition_key=session_id)
                    deleted += 1
                except Exception as del_exc:
                    logger.warning("CosmosStore.delete_messages: could not delete id=%s: %s", item["id"], del_exc)
            logger.info("CosmosStore.delete_messages: deleted %d message(s) for session=%s", deleted, session_id)
        except Exception as exc:
            logger.warning("CosmosStore.delete_messages failed (non-fatal): session=%s error=%s", session_id, exc)
        return deleted

    # ═══════════════════════════════════════════════════════════════════════════
    # 3. AGENT-MEMORY (partition key /sessionId) — durable tool results
    # ═══════════════════════════════════════════════════════════════════════════

    def save_tool_result(self, session_id: str, tool_key: str, result_json: str) -> None:
        """Persist one tool result to Agent-memory. Called after every tool_end SSE event."""
        if not self._available or not self._agent_memory:
            return
        try:
            item_id = f"{session_id}_{tool_key}"
            item = {
                "id": item_id,
                "sessionId": session_id,
                "key": tool_key,
                "value": result_json,
                "updatedAt": _now(),
            }
            self._agent_memory.upsert_item(body=item)
            logger.debug(
                "CosmosStore.save_tool_result: session=%s key=%s bytes=%d",
                session_id, tool_key, len(result_json),
            )
        except Exception as exc:
            logger.warning("CosmosStore.save_tool_result failed (non-fatal): session=%s key=%s error=%s", session_id, tool_key, exc)

    def restore_agent_memory(self, session_id: str) -> Dict[str, str]:
        """Load all persisted tool results for a session from Agent-memory."""
        if not self._available or not self._agent_memory:
            return {}
        try:
            items = list(
                self._agent_memory.query_items(
                    query="SELECT * FROM c WHERE c.sessionId = @sid",
                    parameters=[{"name": "@sid", "value": session_id}],
                    partition_key=session_id,
                )
            )
            cache = {item["key"]: item["value"] for item in items if "key" in item and "value" in item}
            if cache:
                logger.info(
                    "CosmosStore.restore_agent_memory: restored %d key(s) for session=%s keys=%s",
                    len(cache), session_id, list(cache.keys()),
                )
            else:
                logger.debug("CosmosStore.restore_agent_memory: no persisted keys found for session=%s", session_id)
            return cache
        except Exception as exc:
            logger.warning("CosmosStore.restore_agent_memory failed (non-fatal): session=%s error=%s", session_id, exc)
            return {}

    def delete_agent_memory(self, session_id: str) -> int:
        """Delete all Agent-memory entries for a session."""
        if not self._available or not self._agent_memory:
            return 0
        deleted = 0
        try:
            items = list(
                self._agent_memory.query_items(
                    query="SELECT c.id FROM c WHERE c.sessionId = @sid",
                    parameters=[{"name": "@sid", "value": session_id}],
                    partition_key=session_id,
                )
            )
            for item in items:
                try:
                    self._agent_memory.delete_item(item=item["id"], partition_key=session_id)
                    deleted += 1
                except Exception as del_exc:
                    logger.warning("CosmosStore.delete_agent_memory: could not delete id=%s: %s", item["id"], del_exc)
            logger.info("CosmosStore.delete_agent_memory: deleted %d item(s) for session=%s", deleted, session_id)
        except Exception as exc:
            logger.warning("CosmosStore.delete_agent_memory failed (non-fatal): session=%s error=%s", session_id, exc)
        return deleted

    # ═══════════════════════════════════════════════════════════════════════════
    # 4. EMBEDDINGS (partition key /projectId) — vector embeddings
    # ═══════════════════════════════════════════════════════════════════════════

    def save_embedding(
        self,
        project_id: str,
        text: str,
        embedding: List[float],
        source: Optional[str] = None,
        metadata: Optional[Dict] = None,
        embedding_id: Optional[str] = None,
    ) -> Optional[Dict]:
        """Store a vector embedding (1536-dim float32) in the Embeddings container."""
        if not self._available or not self._embeddings:
            return None
        try:
            if len(embedding) != EMBEDDING_DIMENSIONS:
                logger.warning(
                    "CosmosStore.save_embedding: expected %d dimensions, got %d",
                    EMBEDDING_DIMENSIONS, len(embedding),
                )
            doc = {
                "id": embedding_id or str(uuid.uuid4()),
                "projectId": project_id,
                "text": text,
                "embedding": embedding,
                "source": source,
                "metadata": metadata or {},
                "createdAt": _now(),
            }
            self._embeddings.upsert_item(body=doc)
            logger.debug(
                "CosmosStore.save_embedding: project=%s id=%s dims=%d",
                project_id, doc["id"], len(embedding),
            )
            return doc
        except Exception as exc:
            logger.warning("CosmosStore.save_embedding failed (non-fatal): project=%s error=%s", project_id, exc)
            return None

    def vector_search(
        self,
        project_id: str,
        query_vector: List[float],
        top_k: int = 10,
        metadata_type: Optional[str] = None,
    ) -> List[Dict]:
        """Perform cosine similarity vector search in the Embeddings container.

        Parameters
        ----------
        metadata_type : str, optional
            If provided, adds a WHERE filter on c.metadata.type so that only
            embeddings of the given type (e.g. 'rag_chunk') are returned.
        """
        if not self._available or not self._embeddings:
            return []
        try:
            if metadata_type:
                query = (
                    "SELECT TOP @topK c.id, c.projectId, c.text, c.source, c.metadata, c.createdAt, "
                    "VectorDistance(c.embedding, @queryVector) AS similarityScore "
                    "FROM c WHERE c.projectId = @projectId "
                    "AND c.metadata.type = @metadataType "
                    "ORDER BY VectorDistance(c.embedding, @queryVector)"
                )
            else:
                query = (
                    "SELECT TOP @topK c.id, c.projectId, c.text, c.source, c.metadata, c.createdAt, "
                    "VectorDistance(c.embedding, @queryVector) AS similarityScore "
                    "FROM c WHERE c.projectId = @projectId "
                    "ORDER BY VectorDistance(c.embedding, @queryVector)"
                )
            params = [
                {"name": "@topK", "value": top_k},
                {"name": "@queryVector", "value": query_vector},
                {"name": "@projectId", "value": project_id},
            ]
            if metadata_type:
                params.append({"name": "@metadataType", "value": metadata_type})
            items = list(
                self._embeddings.query_items(
                    query=query,
                    parameters=params,
                    partition_key=project_id,
                )
            )
            logger.debug("CosmosStore.vector_search: project=%s results=%d", project_id, len(items))
            return items
        except Exception as exc:
            logger.warning("CosmosStore.vector_search failed (non-fatal): project=%s error=%s", project_id, exc)
            return []

    def get_embeddings(self, project_id: str, limit: int = 100) -> List[Dict]:
        """Retrieve embeddings for a project (without vectors for efficiency)."""
        if not self._available or not self._embeddings:
            return []
        try:
            items = list(
                self._embeddings.query_items(
                    query=(
                        "SELECT c.id, c.projectId, c.text, c.source, c.metadata, c.createdAt "
                        "FROM c WHERE c.projectId = @pid "
                        "ORDER BY c.createdAt DESC OFFSET 0 LIMIT @limit"
                    ),
                    parameters=[
                        {"name": "@pid", "value": project_id},
                        {"name": "@limit", "value": limit},
                    ],
                    partition_key=project_id,
                )
            )
            return items
        except Exception as exc:
            logger.warning("CosmosStore.get_embeddings failed (non-fatal): project=%s error=%s", project_id, exc)
            return []

    def delete_embedding(self, project_id: str, embedding_id: str) -> bool:
        """Delete a single embedding by id."""
        if not self._available or not self._embeddings:
            return False
        try:
            self._embeddings.delete_item(item=embedding_id, partition_key=project_id)
            logger.info("CosmosStore.delete_embedding: id=%s project=%s", embedding_id, project_id)
            return True
        except Exception as exc:
            logger.warning("CosmosStore.delete_embedding failed (non-fatal): id=%s error=%s", embedding_id, exc)
            return False

    # ═══════════════════════════════════════════════════════════════════════════
    # 5. CONTEXT-CACHE (partition key /sessionId, TTL=3600s, vector index)
    # ═══════════════════════════════════════════════════════════════════════════

    def save_context_cache(
        self,
        session_id: str,
        key: str,
        value: str,
        embedding: Optional[List[float]] = None,
    ) -> None:
        """
        Write a value to Context-cache. Item auto-expires after CONTEXT_CACHE_TTL seconds.
        Optionally accepts an embedding vector for vector similarity search.
        """
        if not self._available or not self._context_cache:
            return
        try:
            item_id = f"{session_id}_{key}"
            item: Dict[str, Any] = {
                "id": item_id,
                "sessionId": session_id,
                "key": key,
                "value": value,
                "ttl": CONTEXT_CACHE_TTL,
                "updatedAt": _now(),
            }
            if embedding is not None:
                item["embedding"] = embedding
            self._context_cache.upsert_item(body=item)
            logger.debug(
                "CosmosStore.save_context_cache: session=%s key=%s ttl=%ds bytes=%d vector=%s",
                session_id, key, CONTEXT_CACHE_TTL, len(value), embedding is not None,
            )
        except Exception as exc:
            logger.warning("CosmosStore.save_context_cache failed (non-fatal): session=%s key=%s error=%s", session_id, key, exc)

    def get_context_cache(self, session_id: str, key: str) -> Optional[str]:
        """Read from Context-cache. Returns None if expired or missing."""
        if not self._available or not self._context_cache:
            return None
        try:
            item_id = f"{session_id}_{key}"
            item = self._context_cache.read_item(item=item_id, partition_key=session_id)
            value = item.get("value")
            logger.debug("CosmosStore.get_context_cache: session=%s key=%s found=%s", session_id, key, value is not None)
            return value
        except Exception:
            logger.debug("CosmosStore.get_context_cache: session=%s key=%s — not found or expired", session_id, key)
            return None

    def context_cache_vector_search(
        self,
        session_id: str,
        query_vector: List[float],
        top_k: int = 5,
    ) -> List[Dict]:
        """Perform vector similarity search on cached context items."""
        if not self._available or not self._context_cache:
            return []
        try:
            query = (
                "SELECT TOP @topK c.id, c.sessionId, c.key, c.value, c.updatedAt, "
                "VectorDistance(c.embedding, @queryVector) AS similarityScore "
                "FROM c WHERE c.sessionId = @sid "
                "ORDER BY VectorDistance(c.embedding, @queryVector)"
            )
            items = list(
                self._context_cache.query_items(
                    query=query,
                    parameters=[
                        {"name": "@topK", "value": top_k},
                        {"name": "@queryVector", "value": query_vector},
                        {"name": "@sid", "value": session_id},
                    ],
                    partition_key=session_id,
                )
            )
            logger.debug("CosmosStore.context_cache_vector_search: session=%s results=%d", session_id, len(items))
            return items
        except Exception as exc:
            logger.warning("CosmosStore.context_cache_vector_search failed (non-fatal): session=%s error=%s", session_id, exc)
            return []

    # ═══════════════════════════════════════════════════════════════════════════
    # 6. USERS (partition key /id)
    # ═══════════════════════════════════════════════════════════════════════════

    def create_user(self, user_doc: Dict) -> Optional[Dict]:
        """Create or update a user document."""
        if not self._available or not self._users:
            return None
        try:
            doc = {**user_doc}
            if "id" not in doc:
                doc["id"] = str(uuid.uuid4())
            doc.setdefault("createdAt", _now())
            doc["updatedAt"] = _now()
            self._users.upsert_item(body=doc)
            logger.info("CosmosStore.create_user: id=%s", doc["id"])
            return doc
        except Exception as exc:
            logger.warning("CosmosStore.create_user failed (non-fatal): error=%s", exc)
            return None

    def get_user(self, user_id: str) -> Optional[Dict]:
        """Read a user by id (partition key = id)."""
        if not self._available or not self._users:
            return None
        try:
            return self._users.read_item(item=user_id, partition_key=user_id)
        except Exception:
            logger.debug("CosmosStore.get_user: id=%s not found", user_id)
            return None

    def get_user_by_email(self, email: str) -> Optional[Dict]:
        """Find a user by email (cross-partition query)."""
        if not self._available or not self._users:
            return None
        try:
            items = list(
                self._users.query_items(
                    query="SELECT * FROM c WHERE c.email = @email",
                    parameters=[{"name": "@email", "value": email}],
                    enable_cross_partition_query=True,
                )
            )
            return items[0] if items else None
        except Exception as exc:
            logger.warning("CosmosStore.get_user_by_email failed (non-fatal): email=%s error=%s", email, exc)
            return None

    def list_users(self, limit: int = 100) -> List[Dict]:
        """List all users (cross-partition query)."""
        if not self._available or not self._users:
            return []
        try:
            return list(
                self._users.query_items(
                    query="SELECT * FROM c ORDER BY c.createdAt DESC OFFSET 0 LIMIT @limit",
                    parameters=[{"name": "@limit", "value": limit}],
                    enable_cross_partition_query=True,
                )
            )
        except Exception as exc:
            logger.warning("CosmosStore.list_users failed (non-fatal): error=%s", exc)
            return []

    def update_user(self, user_id: str, updates: Dict) -> Optional[Dict]:
        """Update specific fields on a user document."""
        if not self._available or not self._users:
            return None
        try:
            existing = self._users.read_item(item=user_id, partition_key=user_id)
            existing.update(updates)
            existing["updatedAt"] = _now()
            self._users.upsert_item(body=existing)
            logger.info("CosmosStore.update_user: id=%s", user_id)
            return existing
        except Exception as exc:
            logger.warning("CosmosStore.update_user failed (non-fatal): id=%s error=%s", user_id, exc)
            return None

    def delete_user(self, user_id: str) -> bool:
        """Delete a user by id."""
        if not self._available or not self._users:
            return False
        try:
            self._users.delete_item(item=user_id, partition_key=user_id)
            logger.info("CosmosStore.delete_user: id=%s", user_id)
            return True
        except Exception as exc:
            logger.warning("CosmosStore.delete_user failed (non-fatal): id=%s error=%s", user_id, exc)
            return False

    # ═══════════════════════════════════════════════════════════════════════════
    # 7. PROJECTS (partition key /createdBy)
    # ═══════════════════════════════════════════════════════════════════════════

    def create_project(self, project_doc: Dict) -> Optional[Dict]:
        """Create or update a project document."""
        if not self._available or not self._projects:
            return None
        try:
            doc = {**project_doc}
            if "id" not in doc:
                doc["id"] = str(uuid.uuid4())
            if "createdBy" not in doc:
                logger.warning("CosmosStore.create_project: 'createdBy' is required (partition key)")
                return None
            doc.setdefault("status", "active")
            doc.setdefault("createdAt", _now())
            doc["updatedAt"] = _now()
            self._projects.upsert_item(body=doc)
            logger.info("CosmosStore.create_project: id=%s createdBy=%s", doc["id"], doc["createdBy"])
            return doc
        except Exception as exc:
            logger.warning("CosmosStore.create_project failed (non-fatal): error=%s", exc)
            return None

    def get_project(self, project_id: str, created_by: str) -> Optional[Dict]:
        """Read a project by id (requires createdBy as partition key)."""
        if not self._available or not self._projects:
            return None
        try:
            return self._projects.read_item(item=project_id, partition_key=created_by)
        except Exception:
            logger.debug("CosmosStore.get_project: id=%s createdBy=%s not found", project_id, created_by)
            return None

    def list_projects(self, created_by: str, include_archived: bool = False) -> List[Dict]:
        """List projects created by a specific user."""
        if not self._available or not self._projects:
            return []
        try:
            if include_archived:
                query = "SELECT * FROM c WHERE c.createdBy = @uid ORDER BY c.updatedAt DESC"
            else:
                query = "SELECT * FROM c WHERE c.createdBy = @uid AND c.status != 'archived' ORDER BY c.updatedAt DESC"
            return list(
                self._projects.query_items(
                    query=query,
                    parameters=[{"name": "@uid", "value": created_by}],
                    partition_key=created_by,
                )
            )
        except Exception as exc:
            logger.warning("CosmosStore.list_projects failed (non-fatal): createdBy=%s error=%s", created_by, exc)
            return []

    def update_project(self, project_id: str, created_by: str, updates: Dict) -> Optional[Dict]:
        """Update specific fields on a project document."""
        if not self._available or not self._projects:
            return None
        try:
            existing = self._projects.read_item(item=project_id, partition_key=created_by)
            existing.update(updates)
            existing["updatedAt"] = _now()
            self._projects.upsert_item(body=existing)
            logger.info("CosmosStore.update_project: id=%s", project_id)
            return existing
        except Exception as exc:
            logger.warning("CosmosStore.update_project failed (non-fatal): id=%s error=%s", project_id, exc)
            return None

    def delete_project(self, project_id: str, created_by: str) -> bool:
        """Delete a project by id."""
        if not self._available or not self._projects:
            return False
        try:
            self._projects.delete_item(item=project_id, partition_key=created_by)
            logger.info("CosmosStore.delete_project: id=%s", project_id)
            return True
        except Exception as exc:
            logger.warning("CosmosStore.delete_project failed (non-fatal): id=%s error=%s", project_id, exc)
            return False

    # ═══════════════════════════════════════════════════════════════════════════
    # 8. DOCUMENTS (partition key /projectId)
    #    Metadata only — actual file content lives in Azure Blob Storage.
    # ═══════════════════════════════════════════════════════════════════════════

    def create_document(self, document_doc: Dict) -> Optional[Dict]:
        """Create or update a document metadata record."""
        if not self._available or not self._documents:
            return None
        try:
            doc = {**document_doc}
            if "id" not in doc:
                doc["id"] = str(uuid.uuid4())
            if "projectId" not in doc:
                logger.warning("CosmosStore.create_document: 'projectId' is required (partition key)")
                return None
            doc.setdefault("createdAt", _now())
            doc["updatedAt"] = _now()
            self._documents.upsert_item(body=doc)
            logger.info("CosmosStore.create_document: id=%s project=%s name=%s", doc["id"], doc["projectId"], doc.get("name", ""))
            return doc
        except Exception as exc:
            logger.warning("CosmosStore.create_document failed (non-fatal): error=%s", exc)
            return None

    def get_document(self, document_id: str, project_id: str) -> Optional[Dict]:
        """Read a document metadata record by id."""
        if not self._available or not self._documents:
            return None
        try:
            return self._documents.read_item(item=document_id, partition_key=project_id)
        except Exception:
            logger.debug("CosmosStore.get_document: id=%s project=%s not found", document_id, project_id)
            return None

    def list_documents(self, project_id: str, limit: int = 100) -> List[Dict]:
        """List all documents for a project."""
        if not self._available or not self._documents:
            return []
        try:
            return list(
                self._documents.query_items(
                    query=(
                        "SELECT * FROM c WHERE c.projectId = @pid "
                        "ORDER BY c.createdAt DESC OFFSET 0 LIMIT @limit"
                    ),
                    parameters=[
                        {"name": "@pid", "value": project_id},
                        {"name": "@limit", "value": limit},
                    ],
                    partition_key=project_id,
                )
            )
        except Exception as exc:
            logger.warning("CosmosStore.list_documents failed (non-fatal): project=%s error=%s", project_id, exc)
            return []

    def update_document(self, document_id: str, project_id: str, updates: Dict) -> Optional[Dict]:
        """Update specific fields on a document metadata record."""
        if not self._available or not self._documents:
            return None
        try:
            existing = self._documents.read_item(item=document_id, partition_key=project_id)
            existing.update(updates)
            existing["updatedAt"] = _now()
            self._documents.upsert_item(body=existing)
            logger.info("CosmosStore.update_document: id=%s", document_id)
            return existing
        except Exception as exc:
            logger.warning("CosmosStore.update_document failed (non-fatal): id=%s error=%s", document_id, exc)
            return None

    def delete_document(self, document_id: str, project_id: str) -> bool:
        """Delete a document metadata record (does NOT delete the blob)."""
        if not self._available or not self._documents:
            return False
        try:
            self._documents.delete_item(item=document_id, partition_key=project_id)
            logger.info("CosmosStore.delete_document: id=%s project=%s", document_id, project_id)
            return True
        except Exception as exc:
            logger.warning("CosmosStore.delete_document failed (non-fatal): id=%s error=%s", document_id, exc)
            return False


# ── Module-level singleton ────────────────────────────────────────────────────
_store = None
_COSMOS_MODE = os.environ.get("COSMOS", "ON").strip().upper()


def get_cosmos_store():
    """Return the module-level singleton store.
    COSMOS=ON  → CosmosStore | COSMOS=OFF → PostgresStore
    """
    global _store
    if _store is None:
        if _COSMOS_MODE == "OFF":
            from .postgres_store import PostgresStore
            logger.info("COSMOS=OFF — initialising PostgresStore (local PostgreSQL fallback)")
            _store = PostgresStore()
        else:
            logger.info("COSMOS=ON — initialising CosmosStore (Azure Cosmos DB)")
            _store = CosmosStore()
    return _store


def is_cosmos_enabled() -> bool:
    """Return True if the app is configured to use Cosmos DB."""
    return _COSMOS_MODE != "OFF"
