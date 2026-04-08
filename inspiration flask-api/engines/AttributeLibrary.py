"""
Attribute Library — Global RAG-based training library for control attributes.

Stores user-approved control attributes and retrieves the most similar ones
during schema generation to improve attribute quality over time.

Toggle:  engines/config.py → ENABLE_ATTRIBUTE_LIBRARY (default False)
Storage: Cosmos DB Embeddings container, partition "attribute-library-global"

Usage:
    from engines.AttributeLibrary import get_library

    lib = get_library()

    # Store approved attributes
    lib.store(
        control_description="Vendor master creation based on agreement and KYC",
        attributes=[{"id": "1", "name": "KYC Verification", ...}],
        control_id="CA1",
    )

    # Retrieve similar during schema generation
    similar = lib.retrieve("Vendor creation by Senior Accounts Executive", top_k=3)
    prompt_text = lib.format_for_prompt(similar)
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger("engines.AttributeLibrary")

# Fixed project ID — global library, not tied to any user project
LIBRARY_PROJECT_ID = "attribute-library-global"
LIBRARY_METADATA_TYPE = "attribute_library"


# ---------------------------------------------------------------------------
# Lazy helpers (avoid import-time failures when Cosmos is unavailable)
# ---------------------------------------------------------------------------

def _embed_single(text: str) -> List[float]:
    """Embed a single text string using Azure OpenAI."""
    try:
        from engines.config import (
            AZURE_OPENAI_API_KEY,
            AZURE_OPENAI_API_VERSION,
            AZURE_OPENAI_EMBEDDING_DEPLOYMENT,
            AZURE_OPENAI_ENDPOINT,
        )
        from openai import AzureOpenAI

        client = AzureOpenAI(
            api_key=AZURE_OPENAI_API_KEY,
            azure_endpoint=AZURE_OPENAI_ENDPOINT,
            api_version=AZURE_OPENAI_API_VERSION,
        )
        resp = client.embeddings.create(
            model=AZURE_OPENAI_EMBEDDING_DEPLOYMENT,
            input=[text[:32000]],
        )
        return resp.data[0].embedding
    except Exception as exc:
        logger.error("AttributeLibrary embedding failed: %s", exc)
        return [0.0] * 1536


def _get_cosmos_store():
    """Get the CosmosStore singleton (lazy import)."""
    try:
        from server.cosmos_store import get_cosmos_store
        return get_cosmos_store()
    except Exception as exc:
        logger.warning("AttributeLibrary: cannot reach Cosmos — %s", exc)
        return None


# ---------------------------------------------------------------------------
# Library class
# ---------------------------------------------------------------------------

class AttributeLibrary:
    """Global RAG library of user-approved control attributes."""

    def __init__(self):
        from engines.config import ENABLE_ATTRIBUTE_LIBRARY
        self._enabled = ENABLE_ATTRIBUTE_LIBRARY

    @property
    def enabled(self) -> bool:
        return self._enabled

    # ----- Store ----------------------------------------------------------

    def store(
        self,
        control_description: str,
        attributes: List[Dict],
        worksteps: Optional[List[str]] = None,
        control_id: Optional[str] = None,
        control_type: Optional[str] = None,
        nature: Optional[str] = None,
        process: Optional[str] = None,
        subprocess: Optional[str] = None,
    ) -> Optional[str]:
        """Embed and store approved attributes.  Returns doc ID or None."""
        if not self._enabled:
            logger.info("AttributeLibrary disabled — skipping store")
            return None

        store = _get_cosmos_store()
        if not store or not getattr(store, "available", False):
            logger.warning("AttributeLibrary: Cosmos DB not available")
            return None

        # Build embedding text: control description + attribute names
        attr_names = ", ".join(a.get("name", "") for a in attributes) if attributes else ""
        embed_text = f"{control_description} | Attributes: {attr_names}" if attr_names else control_description

        embedding = _embed_single(embed_text)

        metadata = {
            "type": LIBRARY_METADATA_TYPE,
            "control_id": control_id or "",
            "control_type": (control_type or "").strip().lower(),
            "nature": (nature or "").strip().lower(),
            "process": (process or "").strip(),
            "subprocess": (subprocess or "").strip(),
            "attributes": json.dumps(attributes, ensure_ascii=False),
            "worksteps": json.dumps(worksteps or [], ensure_ascii=False),
            "stored_at": datetime.now(timezone.utc).isoformat(),
        }

        doc = store.save_embedding(
            project_id=LIBRARY_PROJECT_ID,
            text=control_description,
            embedding=embedding,
            source=f"attribute_library:{control_id or 'unknown'}",
            metadata=metadata,
        )
        if doc:
            logger.info(
                "AttributeLibrary: stored %s (%d attributes)",
                control_id or "unknown", len(attributes),
            )
            return doc["id"]
        return None

    # ----- Retrieve -------------------------------------------------------

    def retrieve(self, control_description: str, top_k: int = 3) -> List[Dict]:
        """Return the most similar approved controls from the library."""
        if not self._enabled:
            return []

        store = _get_cosmos_store()
        if not store or not getattr(store, "available", False):
            return []

        query_embedding = _embed_single(control_description)

        results = store.vector_search(
            project_id=LIBRARY_PROJECT_ID,
            query_vector=query_embedding,
            top_k=top_k,
            metadata_type=LIBRARY_METADATA_TYPE,
        )

        formatted = []
        for item in results:
            meta = item.get("metadata", {})
            try:
                attributes = json.loads(meta.get("attributes", "[]"))
            except (json.JSONDecodeError, TypeError):
                attributes = []
            try:
                worksteps = json.loads(meta.get("worksteps", "[]"))
            except (json.JSONDecodeError, TypeError):
                worksteps = []

            # Skip entries with >4 attributes (pre-fix legacy data)
            if len(attributes) > 4:
                logger.info(
                    "AttributeLibrary: skipping entry %s with %d attributes (>4)",
                    meta.get("control_id", "?"), len(attributes),
                )
                continue

            formatted.append({
                "control_description": item.get("text", ""),
                "attributes": attributes,
                "worksteps": worksteps,
                "control_type": meta.get("control_type", ""),
                "nature": meta.get("nature", ""),
                "process": meta.get("process", ""),
                "similarity_score": item.get("similarityScore", 0),
                "id": item.get("id"),
            })

        return formatted

    # ----- Format for prompt injection ------------------------------------

    def format_for_prompt(self, results: List[Dict]) -> str:
        """Format retrieved library entries as few-shot examples for the LLM."""
        if not results:
            return ""

        parts = [
            "\n## APPROVED EXAMPLES FROM TRAINING LIBRARY "
            "(user-approved attributes from previous audits — follow these closely)\n"
        ]

        for i, r in enumerate(results, 1):
            attrs = r.get("attributes", [])
            worksteps = r.get("worksteps", [])

            attrs_json = json.dumps(attrs, ensure_ascii=False, indent=8)
            worksteps_json = json.dumps(worksteps, ensure_ascii=False, indent=8)

            parts.append(
                f'Library Example {i} — Previously approved by auditor\n'
                f'Control: "{r["control_description"]}"\n'
                f'{{\n'
                f'    "worksteps": {worksteps_json},\n'
                f'    "attributes": {attrs_json}\n'
                f'}}\n'
            )

        return "\n".join(parts)

    # ----- Admin ----------------------------------------------------------

    def list_all(self, limit: int = 100) -> List[Dict]:
        """List all entries (without embeddings)."""
        if not self._enabled:
            return []

        store = _get_cosmos_store()
        if not store or not getattr(store, "available", False):
            return []

        embeddings = store.get_embeddings(LIBRARY_PROJECT_ID, limit=limit)
        results = []
        for emb in embeddings:
            meta = emb.get("metadata", {})
            if meta.get("type") != LIBRARY_METADATA_TYPE:
                continue
            try:
                attributes = json.loads(meta.get("attributes", "[]"))
            except (json.JSONDecodeError, TypeError):
                attributes = []
            results.append({
                "id": emb.get("id"),
                "control_description": emb.get("text", ""),
                "control_id": meta.get("control_id", ""),
                "control_type": meta.get("control_type", ""),
                "nature": meta.get("nature", ""),
                "process": meta.get("process", ""),
                "attribute_count": len(attributes),
                "stored_at": meta.get("stored_at", ""),
            })
        return results

    def delete(self, doc_id: str) -> bool:
        """Remove an entry from the library."""
        if not self._enabled:
            return False
        store = _get_cosmos_store()
        if not store or not getattr(store, "available", False):
            return False
        return store.delete_embedding(LIBRARY_PROJECT_ID, doc_id)


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------

_library: Optional[AttributeLibrary] = None


def get_library() -> AttributeLibrary:
    """Get or create the singleton AttributeLibrary."""
    global _library
    if _library is None:
        _library = AttributeLibrary()
    return _library
