"""
Embeddings endpoints for vector search.

Embeddings are stored in the Cosmos DB "Embeddings" container (partition key /projectId)
with a vector index on /embedding (1536-dim float32, cosine distance).

Context-cache also supports vector search for cached context items.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request

from ..models import CreateEmbeddingRequest, VectorSearchRequest

router = APIRouter(prefix="/api", tags=["embeddings"])
logger = logging.getLogger("server.routes.embeddings")


def _cosmos(request: Request):
    from ..cosmos_store import get_cosmos_store
    return get_cosmos_store()


def _check_cosmos():
    from ..cosmos_store import is_cosmos_enabled
    if not is_cosmos_enabled():
        raise HTTPException(status_code=503, detail="Embeddings unavailable (COSMOS=OFF). Requires Azure Cosmos DB.")


@router.post("/embeddings")
async def create_embedding(body: CreateEmbeddingRequest, request: Request):
    """Store a vector embedding for a project."""
    _check_cosmos()
    store = _cosmos(request)
    doc = store.save_embedding(
        project_id=body.projectId,
        text=body.text,
        embedding=body.embedding,
        source=body.source,
        metadata=body.metadata,
    )
    if not doc:
        raise HTTPException(status_code=500, detail="Failed to store embedding")
    # Don't return the full vector in the response
    doc.pop("embedding", None)
    return doc


@router.post("/embeddings/search")
async def vector_search(body: VectorSearchRequest, request: Request):
    """Perform vector similarity search on project embeddings."""
    _check_cosmos()
    store = _cosmos(request)
    results = store.vector_search(
        project_id=body.projectId,
        query_vector=body.queryVector,
        top_k=body.topK,
    )
    return {"results": results}


@router.get("/embeddings")
async def list_embeddings(
    request: Request,
    projectId: str = "",
):
    """List embeddings for a project (without vectors)."""
    _check_cosmos()
    if not projectId:
        raise HTTPException(status_code=400, detail="projectId query parameter required")
    store = _cosmos(request)
    embeddings = store.get_embeddings(projectId)
    return {"embeddings": embeddings}


@router.delete("/embeddings/{embedding_id}")
async def delete_embedding(
    embedding_id: str,
    request: Request,
    projectId: str = "",
):
    """Delete an embedding by ID."""
    _check_cosmos()
    if not projectId:
        raise HTTPException(status_code=400, detail="projectId query parameter required")
    store = _cosmos(request)
    ok = store.delete_embedding(projectId, embedding_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Embedding not found")
    return {"ok": True}
