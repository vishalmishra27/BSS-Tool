"""
User management endpoints.

Users are stored in the Cosmos DB "Users" container (partition key /id).
"""

from __future__ import annotations

import logging
import uuid

from fastapi import APIRouter, HTTPException, Request

from ..models import (
    CreateUserRequest,
    UpdateUserRequest,
    UserResponse,
)

router = APIRouter(prefix="/api", tags=["users"])
logger = logging.getLogger("server.routes.users")


def _cosmos(request: Request):
    from ..cosmos_store import get_cosmos_store
    return get_cosmos_store()


def _to_response(doc: dict) -> UserResponse:
    return UserResponse(
        id=doc["id"],
        email=doc.get("email", ""),
        name=doc.get("name", ""),
        role=doc.get("role", "auditor"),
        metadata=doc.get("metadata"),
        createdAt=doc.get("createdAt"),
        updatedAt=doc.get("updatedAt"),
    )


@router.post("/users", response_model=UserResponse)
async def create_user(body: CreateUserRequest, request: Request):
    """Create a new user."""
    store = _cosmos(request)

    existing = store.get_user_by_email(body.email)
    if existing:
        raise HTTPException(status_code=409, detail="User with this email already exists")

    doc = store.create_user({
        "id": str(uuid.uuid4()),
        "email": body.email,
        "name": body.name,
        "role": body.role,
        "metadata": body.metadata or {},
    })
    if not doc:
        raise HTTPException(status_code=500, detail="Failed to create user")
    return _to_response(doc)


@router.get("/users")
async def list_users(request: Request):
    """List all users."""
    store = _cosmos(request)
    users = store.list_users()
    return {"users": [_to_response(u) for u in users]}


@router.get("/users/{user_id}", response_model=UserResponse)
async def get_user(user_id: str, request: Request):
    """Get a user by ID."""
    store = _cosmos(request)
    doc = store.get_user(user_id)
    if not doc:
        raise HTTPException(status_code=404, detail="User not found")
    return _to_response(doc)


@router.patch("/users/{user_id}", response_model=UserResponse)
async def update_user(user_id: str, body: UpdateUserRequest, request: Request):
    """Update a user's fields."""
    store = _cosmos(request)
    updates = body.model_dump(exclude_none=True)
    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")
    doc = store.update_user(user_id, updates)
    if not doc:
        raise HTTPException(status_code=404, detail="User not found")
    return _to_response(doc)


@router.delete("/users/{user_id}")
async def delete_user(user_id: str, request: Request):
    """Delete a user."""
    store = _cosmos(request)
    ok = store.delete_user(user_id)
    if not ok:
        raise HTTPException(status_code=404, detail="User not found")
    return {"ok": True}
