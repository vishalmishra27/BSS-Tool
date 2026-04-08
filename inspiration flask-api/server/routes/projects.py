"""
Project management endpoints.

Projects are stored in the Cosmos DB "Projects" container (partition key /createdBy).
"""

from __future__ import annotations

import logging
import uuid

from fastapi import APIRouter, HTTPException, Query, Request

from ..models import (
    CreateProjectRequest,
    UpdateProjectRequest,
    ProjectResponse,
)

router = APIRouter(prefix="/api", tags=["projects"])
logger = logging.getLogger("server.routes.projects")


def _cosmos(request: Request):
    from ..cosmos_store import get_cosmos_store
    return get_cosmos_store()


def _to_response(doc: dict) -> ProjectResponse:
    return ProjectResponse(
        id=doc["id"],
        createdBy=doc.get("createdBy", ""),
        name=doc.get("name", ""),
        description=doc.get("description"),
        status=doc.get("status", "active"),
        metadata=doc.get("metadata"),
        createdAt=doc.get("createdAt"),
        updatedAt=doc.get("updatedAt"),
    )


@router.post("/projects", response_model=ProjectResponse)
async def create_project(body: CreateProjectRequest, request: Request):
    """Create a new project."""
    store = _cosmos(request)
    doc = store.create_project({
        "id": str(uuid.uuid4()),
        "createdBy": body.createdBy,
        "name": body.name,
        "description": body.description,
        "status": body.status,
        "metadata": body.metadata or {},
    })
    if not doc:
        raise HTTPException(status_code=500, detail="Failed to create project")
    return _to_response(doc)


@router.get("/projects")
async def list_projects(
    request: Request,
    createdBy: str = Query(..., description="User ID who created the projects"),
    include_archived: bool = Query(False, description="Include archived projects"),
):
    """List projects for a user."""
    store = _cosmos(request)
    projects = store.list_projects(createdBy, include_archived=include_archived)
    return {"projects": [_to_response(p) for p in projects]}


@router.get("/projects/{project_id}", response_model=ProjectResponse)
async def get_project(
    project_id: str,
    request: Request,
    createdBy: str = Query(..., description="User ID who created the project"),
):
    """Get a project by ID."""
    store = _cosmos(request)
    doc = store.get_project(project_id, createdBy)
    if not doc:
        raise HTTPException(status_code=404, detail="Project not found")
    return _to_response(doc)


@router.patch("/projects/{project_id}", response_model=ProjectResponse)
async def update_project(
    project_id: str,
    body: UpdateProjectRequest,
    request: Request,
    createdBy: str = Query(..., description="User ID who created the project"),
):
    """Update a project's fields."""
    store = _cosmos(request)
    updates = body.model_dump(exclude_none=True)
    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")
    doc = store.update_project(project_id, createdBy, updates)
    if not doc:
        raise HTTPException(status_code=404, detail="Project not found")
    return _to_response(doc)


@router.delete("/projects/{project_id}")
async def delete_project(
    project_id: str,
    request: Request,
    createdBy: str = Query(..., description="User ID who created the project"),
):
    """Delete a project."""
    store = _cosmos(request)
    ok = store.delete_project(project_id, createdBy)
    if not ok:
        raise HTTPException(status_code=404, detail="Project not found")
    return {"ok": True}
