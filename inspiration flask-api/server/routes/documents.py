"""
Document metadata endpoints.

Document metadata is stored in the Cosmos DB "Documents" container (partition key /projectId).
Actual file content lives in Azure Blob Storage — this container only tracks metadata.
"""

from __future__ import annotations

import logging
import uuid

from fastapi import APIRouter, HTTPException, Query, Request

from ..models import (
    CreateDocumentRequest,
    UpdateDocumentRequest,
    DocumentResponse,
)

router = APIRouter(prefix="/api", tags=["documents"])
logger = logging.getLogger("server.routes.documents")


def _cosmos(request: Request):
    from ..cosmos_store import get_cosmos_store
    return get_cosmos_store()


def _to_response(doc: dict) -> DocumentResponse:
    return DocumentResponse(
        id=doc["id"],
        projectId=doc.get("projectId", ""),
        name=doc.get("name", ""),
        blobPath=doc.get("blobPath", ""),
        blobUrl=doc.get("blobUrl"),
        type=doc.get("type"),
        size=doc.get("size"),
        uploadedBy=doc.get("uploadedBy"),
        metadata=doc.get("metadata"),
        createdAt=doc.get("createdAt"),
        updatedAt=doc.get("updatedAt"),
    )


@router.post("/documents", response_model=DocumentResponse)
async def create_document(body: CreateDocumentRequest, request: Request):
    """Create a document metadata record. File should already be in Blob Storage."""
    store = _cosmos(request)
    doc = store.create_document({
        "id": str(uuid.uuid4()),
        "projectId": body.projectId,
        "name": body.name,
        "blobPath": body.blobPath,
        "blobUrl": body.blobUrl,
        "type": body.type,
        "size": body.size,
        "uploadedBy": body.uploadedBy,
        "metadata": body.metadata or {},
    })
    if not doc:
        raise HTTPException(status_code=500, detail="Failed to create document record")
    return _to_response(doc)


@router.get("/documents")
async def list_documents(
    request: Request,
    projectId: str = Query(..., description="Project ID to list documents for"),
):
    """List all documents for a project."""
    store = _cosmos(request)
    documents = store.list_documents(projectId)
    return {"documents": [_to_response(d) for d in documents]}


@router.get("/documents/{document_id}", response_model=DocumentResponse)
async def get_document(
    document_id: str,
    request: Request,
    projectId: str = Query(..., description="Project ID the document belongs to"),
):
    """Get a document metadata record by ID."""
    store = _cosmos(request)
    doc = store.get_document(document_id, projectId)
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")
    return _to_response(doc)


@router.patch("/documents/{document_id}", response_model=DocumentResponse)
async def update_document(
    document_id: str,
    body: UpdateDocumentRequest,
    request: Request,
    projectId: str = Query(..., description="Project ID the document belongs to"),
):
    """Update a document metadata record."""
    store = _cosmos(request)
    updates = body.model_dump(exclude_none=True)
    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")
    doc = store.update_document(document_id, projectId, updates)
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")
    return _to_response(doc)


@router.delete("/documents/{document_id}")
async def delete_document(
    document_id: str,
    request: Request,
    projectId: str = Query(..., description="Project ID the document belongs to"),
    delete_blob: bool = Query(False, description="Also delete the file from Blob Storage"),
):
    """Delete a document metadata record. Optionally delete the blob too."""
    store = _cosmos(request)

    doc = store.get_document(document_id, projectId)
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")

    ok = store.delete_document(document_id, projectId)
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to delete document record")

    blob_deleted = False
    if delete_blob and doc.get("blobPath"):
        try:
            from ..blob_store import get_blob_store
            blob_store = get_blob_store()
            blob_deleted = blob_store.delete_blob(doc["blobPath"])
        except Exception as exc:
            logger.warning("Failed to delete blob for document %s: %s", document_id, exc)

    return {"ok": True, "blob_deleted": blob_deleted}
