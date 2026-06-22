"""
Authentication endpoints: register, login, me.

Passwords are hashed with PBKDF2-SHA256. Tokens are JWTs signed with HS256.
Users are stored in the Cosmos DB "Users" container.
"""

from __future__ import annotations

import hashlib
import logging
import os
import secrets
import uuid
from datetime import datetime, timezone, timedelta

import jwt
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from ..cosmos_store import get_cosmos_store

router = APIRouter(prefix="/api/auth", tags=["auth"])
logger = logging.getLogger("server.routes.auth")

JWT_SECRET = os.getenv("JWT_SECRET", secrets.token_hex(32))
JWT_ALGORITHM = "HS256"
JWT_EXPIRY_HOURS = 72


# --------------- helpers ---------------

def _hash_password(password: str, salt: str | None = None) -> tuple[str, str]:
    """Return (hash_hex, salt_hex) using PBKDF2-SHA256."""
    if salt is None:
        salt = secrets.token_hex(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), iterations=260_000)
    return dk.hex(), salt


def _verify_password(password: str, stored_hash: str, salt: str) -> bool:
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), iterations=260_000)
    return secrets.compare_digest(dk.hex(), stored_hash)


def _create_token(user_id: str, email: str, role: str) -> str:
    payload = {
        "sub": user_id,
        "email": email,
        "role": role,
        "exp": datetime.now(timezone.utc) + timedelta(hours=JWT_EXPIRY_HOURS),
        "iat": datetime.now(timezone.utc),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def _decode_token(token: str) -> dict:
    return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])


def _user_response(doc: dict) -> dict:
    return {
        "id": doc["id"],
        "email": doc.get("email", ""),
        "name": doc.get("name", ""),
        "role": doc.get("role", "auditor"),
    }


# --------------- request models ---------------

class RegisterRequest(BaseModel):
    email: str
    password: str
    name: str
    role: str = "auditor"


class LoginRequest(BaseModel):
    email: str
    password: str


# --------------- routes ---------------

@router.post("/register")
async def register(body: RegisterRequest):
    """Register a new user and return a JWT."""
    store = get_cosmos_store()

    existing = store.get_user_by_email(body.email)
    if existing:
        raise HTTPException(status_code=409, detail="A user with this email already exists")

    pw_hash, pw_salt = _hash_password(body.password)

    doc = store.create_user({
        "id": str(uuid.uuid4()),
        "email": body.email,
        "name": body.name,
        "role": body.role,
        "passwordHash": pw_hash,
        "passwordSalt": pw_salt,
        "metadata": {},
    })
    if not doc:
        raise HTTPException(status_code=500, detail="Failed to create user")

    token = _create_token(doc["id"], doc["email"], doc.get("role", "auditor"))
    return {
        "success": True,
        "data": {
            "token": token,
            "user": _user_response(doc),
        },
    }


@router.post("/login")
async def login(body: LoginRequest):
    """Authenticate and return a JWT."""
    store = get_cosmos_store()

    user = store.get_user_by_email(body.email)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid email or password")

    stored_hash = user.get("passwordHash", "")
    stored_salt = user.get("passwordSalt", "")

    if not stored_hash or not _verify_password(body.password, stored_hash, stored_salt):
        raise HTTPException(status_code=401, detail="Invalid email or password")

    token = _create_token(user["id"], user["email"], user.get("role", "auditor"))
    return {
        "success": True,
        "data": {
            "token": token,
            "user": _user_response(user),
        },
    }


@router.get("/me")
async def me(request: Request):
    """Return the current user from the JWT in the Authorization header."""
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid token")

    token = auth_header[7:]
    try:
        payload = _decode_token(token)
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

    store = get_cosmos_store()
    user = store.get_user(payload["sub"])
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    return {"success": True, "data": _user_response(user)}
