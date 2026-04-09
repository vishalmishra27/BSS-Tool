"""
Playbook Store - RAG-based workflow example retrieval.

Loads curated YAML playbook files, embeds their trigger descriptions using
OpenAI text-embedding-ada-002, and retrieves the most relevant playbooks
for a given user query via cosine similarity.

Injected into the system prompt so the LLM has concrete examples of how
to execute common multi-step operations.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

import numpy as np
import yaml
from openai import AzureOpenAI

logger = logging.getLogger("agent.playbook_store")

EMBEDDING_MODEL = "text-embedding-ada-002"
DEFAULT_TOP_K = 3
DEFAULT_THRESHOLD = 0.5


@dataclass
class Playbook:
    trigger: str
    title: str
    steps: List[str] = field(default_factory=list)
    notes: str = ""
    tags: list = field(default_factory=list)


class PlaybookStore:
    """Loads, embeds, and retrieves playbook workflow examples."""

    def __init__(
        self,
        playbooks_dir: Path,
        openai_api_key: str,
        azure_endpoint: str,
        azure_api_version: str = "2024-12-01-preview",
        embedding_model: str = "",
    ):
        self._dir = Path(playbooks_dir)
        self._client = AzureOpenAI(
            api_key=openai_api_key,
            azure_endpoint=azure_endpoint,
            api_version=azure_api_version,
        )
        self._embedding_model = embedding_model or EMBEDDING_MODEL
        self._playbooks: List[Playbook] = []
        self._embeddings: Optional[np.ndarray] = None
        self._ready = False

    @property
    def ready(self) -> bool:
        return self._ready

    def load(self):
        """Load all .yaml playbooks and embed their triggers."""
        if not self._dir.exists():
            logger.warning("Playbooks directory not found: %s", self._dir)
            return

        files = sorted(self._dir.glob("*.yaml"))
        if not files:
            logger.warning("No .yaml files found in %s", self._dir)
            return

        for f in files:
            try:
                doc = yaml.safe_load(f.read_text(encoding="utf-8"))
                trigger = doc.get("trigger")
                if not trigger:
                    logger.warning("Skipping %s - missing 'trigger' field", f.name)
                    continue
                self._playbooks.append(
                    Playbook(
                        trigger=trigger,
                        title=doc.get("title", f.stem),
                        steps=doc.get("steps", []),
                        notes=doc.get("notes", ""),
                        tags=doc.get("tags", []),
                    )
                )
            except Exception:
                logger.exception("Failed to parse playbook %s", f.name)

        if not self._playbooks:
            logger.warning("No valid playbooks loaded")
            return

        logger.info("Loaded %d playbooks from %s", len(self._playbooks), self._dir)

        try:
            resp = self._client.embeddings.create(
                model=self._embedding_model,
                input=[p.trigger for p in self._playbooks],
            )
            self._embeddings = np.array(
                [d.embedding for d in resp.data], dtype=np.float32
            )
            self._ready = True
            logger.info(
                "Embedded %d playbook triggers (dim=%d)",
                *self._embeddings.shape,
            )
        except Exception:
            logger.exception("Failed to embed playbook triggers - retrieval disabled")

    def retrieve(
        self, query: str, top_k: int = DEFAULT_TOP_K, threshold: float = DEFAULT_THRESHOLD
    ) -> List[Playbook]:
        """Return top-K playbooks whose trigger is similar to the query."""
        if not self._ready or self._embeddings is None:
            return []

        if not query.strip():
            return []

        try:
            resp = self._client.embeddings.create(
                model=self._embedding_model,
                input=query,
            )
            q_vec = np.array(resp.data[0].embedding, dtype=np.float32)
        except Exception:
            logger.exception("Failed to embed query - skipping playbook retrieval")
            return []

        # Cosine similarity
        norms = np.linalg.norm(self._embeddings, axis=1) * np.linalg.norm(q_vec)
        norms = np.where(norms == 0, 1, norms)
        scores = self._embeddings @ q_vec / norms

        ranked = scores.argsort()[::-1]
        results: List[Playbook] = []
        for idx in ranked[:top_k]:
            if float(scores[idx]) >= threshold:
                results.append(self._playbooks[idx])
                logger.debug(
                    "Matched playbook '%s' (score=%.3f)",
                    self._playbooks[idx].title,
                    scores[idx],
                )

        logger.info("Retrieved %d playbooks for query: %.80s...", len(results), query)
        return results

    def format_for_prompt(self, playbooks: List[Playbook]) -> str:
        """Format matched playbooks as a system prompt section."""
        if not playbooks:
            return ""

        parts = [
            "RELEVANT WORKFLOW EXAMPLES (reference only - do NOT auto-execute):",
            "IMPORTANT: These examples show HOW to use tools IF the user asks for "
            "these operations. Do NOT run these steps unless the user EXPLICITLY "
            "requested them. Only use the specific tool/parameter patterns relevant "
            "to what the user actually asked.",
        ]

        for pb in playbooks:
            section = f"\n### {pb.title}"
            section += "\nSteps:"
            for i, step in enumerate(pb.steps, 1):
                section += f"\n  {i}. {step}"
            if pb.notes:
                section += "\nNotes:"
                section += f"\n  - {pb.notes}"
            parts.append(section)

        return "\n".join(parts)


_store: Optional[PlaybookStore] = None


def init_playbook_store(
    openai_api_key: str,
    azure_endpoint: str,
    azure_api_version: str,
    embedding_model: str,
):
    """Initialize the global playbook store (called once at startup)."""
    global _store
    playbooks_dir = Path(__file__).resolve().parent.parent / "playbooks"
    _store = PlaybookStore(
        playbooks_dir=playbooks_dir,
        openai_api_key=openai_api_key,
        azure_endpoint=azure_endpoint,
        azure_api_version=azure_api_version,
        embedding_model=embedding_model,
    )
    _store.load()


def get_playbook_store() -> Optional[PlaybookStore]:
    """Get the global playbook store instance."""
    return _store
