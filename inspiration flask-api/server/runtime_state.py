"""
Runtime state manager — manages per-thread AgentState instances.

AgentState holds DataFrames and caches that cannot be serialised to SQLite.
Each LangGraph thread_id gets its own AgentState in memory.

On creation, tool_results_cache is restored from Agent-memory (Cosmos DB)
so the state survives Flask restarts. When evicted, state is flushed and
the in-memory copy released.
"""

import logging
import threading
import time
from typing import Dict

from agent.types import AgentState
from .config import get_server_config

logger = logging.getLogger("server.runtime_state")


class RuntimeStateManager:
    """Manages per-thread AgentState (DataFrames, caches) in memory."""

    def __init__(self):
        cfg = get_server_config()
        self._max_states = cfg.max_active_states
        self._idle_timeout = cfg.idle_timeout_seconds
        self._states: Dict[str, AgentState] = {}
        self._last_active: Dict[str, float] = {}
        self._lock = threading.Lock()
        logger.info(
            "RuntimeStateManager initialised: max_states=%d idle_timeout=%ds",
            self._max_states, self._idle_timeout,
        )

    def get_or_create(self, thread_id: str) -> AgentState:
        """
        Get existing AgentState or create a fresh one.

        If creating new, restore tool_results_cache from Agent-memory (Cosmos)
        so results survive a Flask restart.
        """
        with self._lock:
            if thread_id not in self._states:
                self._evict_if_needed()
                state = AgentState()
                # Restore durable tool results from Cosmos Agent-memory
                try:
                    from .cosmos_store import get_cosmos_store
                    store = get_cosmos_store()
                    persisted = store.restore_agent_memory(thread_id)
                    if persisted:
                        state.tool_results_cache.update(persisted)
                        logger.info(
                            "RuntimeStateManager.get_or_create: restored %d cached tool result(s) for thread=%s keys=%s",
                            len(persisted), thread_id, list(persisted.keys()),
                        )

                        # Restore RAG state if previously persisted
                        rag_json = persisted.get("rag_state")
                        if rag_json:
                            import json
                            try:
                                rag = json.loads(rag_json) if isinstance(rag_json, str) else rag_json
                                state.rag_project_id = rag.get("rag_project_id")
                                state.rag_document_name = rag.get("rag_document_name")
                                state.rag_chunk_count = rag.get("rag_chunk_count")
                                logger.info(
                                    "RuntimeStateManager.get_or_create: restored RAG state for thread=%s "
                                    "project=%s doc=%s chunks=%s",
                                    thread_id, state.rag_project_id,
                                    state.rag_document_name, state.rag_chunk_count,
                                )
                            except (json.JSONDecodeError, TypeError) as je:
                                logger.warning(
                                    "RuntimeStateManager.get_or_create: could not parse rag_state for thread=%s: %s",
                                    thread_id, je,
                                )
                    else:
                        logger.debug(
                            "RuntimeStateManager.get_or_create: no persisted cache found for thread=%s",
                            thread_id,
                        )
                except Exception as exc:
                    logger.warning(
                        "RuntimeStateManager.get_or_create: could not restore Agent-memory for thread=%s: %s",
                        thread_id, exc,
                    )
                # Default to the seeded handbook project if no RAG state was restored
                if not state.rag_project_id:
                    state.rag_project_id = "icofar-handbook"
                    state.rag_document_name = "handbook-internal-controls-over-financial-reporting.pdf"

                # Attach session/thread ID so tools can persist per-session data
                state._session_id = thread_id
                self._states[thread_id] = state
                logger.info(
                    "RuntimeStateManager.get_or_create: created new AgentState for thread=%s (active=%d)",
                    thread_id, len(self._states),
                )
            else:
                logger.debug("RuntimeStateManager.get_or_create: reusing existing AgentState for thread=%s", thread_id)
            self._last_active[thread_id] = time.time()
            return self._states[thread_id]

    def evict(self, thread_id: str) -> None:
        """Remove a specific thread's state from memory."""
        with self._lock:
            if thread_id in self._states:
                self._states.pop(thread_id, None)
                self._last_active.pop(thread_id, None)
                logger.info("RuntimeStateManager.evict: evicted AgentState for thread=%s (active=%d)", thread_id, len(self._states))
            else:
                logger.debug("RuntimeStateManager.evict: thread=%s was not in memory (no-op)", thread_id)

    def evict_idle(self) -> int:
        """Remove states idle longer than timeout. Returns count evicted."""
        now = time.time()
        evicted = 0
        with self._lock:
            to_evict = [
                tid
                for tid, last in self._last_active.items()
                if now - last > self._idle_timeout
            ]
            for tid in to_evict:
                self._states.pop(tid, None)
                self._last_active.pop(tid, None)
                evicted += 1
        if evicted:
            logger.info(
                "RuntimeStateManager.evict_idle: evicted %d idle AgentState(s) (active=%d)",
                evicted, len(self._states),
            )
        return evicted

    @property
    def active_count(self) -> int:
        return len(self._states)

    def _evict_if_needed(self) -> None:
        """Evict the oldest idle state if at capacity."""
        if len(self._states) < self._max_states:
            return
        if not self._last_active:
            return
        oldest_tid = min(self._last_active, key=self._last_active.get)
        self._states.pop(oldest_tid, None)
        self._last_active.pop(oldest_tid, None)
        logger.info(
            "RuntimeStateManager._evict_if_needed: evicted LRU AgentState for thread=%s (at capacity=%d)",
            oldest_tid, self._max_states,
        )
