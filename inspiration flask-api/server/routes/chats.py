"""
Chat CRUD endpoints.
"""

from __future__ import annotations

import json
import logging
import uuid
from typing import List, Dict

from fastapi import APIRouter, HTTPException, Query, Request

from ..models import (
    ChatListResponse,
    ChatResponse,
    RenameChatRequest,
    StarterActionsResponse,
    StarterAction,
)

router = APIRouter(prefix="/api", tags=["chats"])
logger = logging.getLogger("server.routes.chats")


@router.get("/chats", response_model=ChatListResponse)
async def list_chats(request: Request, projectId: str = Query(None)):
    """List all active chats, sorted by most recent."""
    db = request.app.state.db
    chats = db.list_chats(project_id=projectId)
    return ChatListResponse(
        chats=[
            ChatResponse(
                id=c["id"],
                title=c["title"],
                created_at=c["created_at"],
                updated_at=c["updated_at"],
                is_archived=bool(c.get("is_archived", 0)),
                projectId=c.get("projectId"),
            )
            for c in chats
        ]
    )


@router.post("/chats", response_model=ChatResponse)
async def create_chat(request: Request, projectId: str = Query(None)):
    """Create a new chat session."""
    db = request.app.state.db
    chat_id = str(uuid.uuid4())
    chat = db.create_chat(chat_id, project_id=projectId)
    return ChatResponse(
        id=chat["id"],
        title=chat["title"],
        created_at=chat["created_at"],
        updated_at=chat["updated_at"],
        projectId=chat.get("projectId"),
    )


@router.patch("/chats/{chat_id}", response_model=ChatResponse)
async def rename_chat(chat_id: str, body: RenameChatRequest, request: Request):
    """Rename a chat."""
    db = request.app.state.db
    chat = db.update_chat_title(chat_id, body.title)
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")
    return ChatResponse(
        id=chat["id"],
        title=chat["title"],
        created_at=chat["created_at"],
        updated_at=chat["updated_at"],
    )


@router.delete("/chats/{chat_id}")
async def delete_chat(chat_id: str, request: Request):
    """Soft-delete a chat and evict its runtime state."""
    db = request.app.state.db
    state_manager = request.app.state.state_manager
    ok = db.archive_chat(chat_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Chat not found")
    state_manager.evict(chat_id)
    return {"ok": True}


@router.get("/chats/{chat_id}/starter-actions", response_model=StarterActionsResponse)
async def starter_actions(chat_id: str, request: Request):
    """Generate AI-suggested starter actions for a new chat."""
    db = request.app.state.db
    chat = db.get_chat(chat_id)
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")

    actions = _generate_starter_actions()
    return StarterActionsResponse(actions=[StarterAction(**a) for a in actions])


@router.get("/chats/{chat_id}/artifacts")
async def list_artifacts(chat_id: str, request: Request):
    """Return exported artifact paths tracked in runtime state for this chat."""
    import os
    from ..blob_store import BlobStore

    db = request.app.state.db
    state_manager = request.app.state.state_manager
    chat = db.get_chat(chat_id)
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")

    state = state_manager.get_or_create(chat_id)
    raw_artifacts = list(dict.fromkeys([str(a) for a in (state.artifacts or []) if a]))

    # Convert local filesystem paths to blob paths.
    # Tools that upload to blob already store blob paths (artifacts/{session_key}/...).
    # For legacy local paths, try the session_key from output_dir first, then chat_id.
    artifacts = []
    state_output_dir = getattr(state, "output_dir", None)
    session_key = os.path.basename(state_output_dir) if state_output_dir else chat_id
    for path in raw_artifacts:
        if BlobStore.is_blob_path(path):
            artifacts.append(path)
        elif os.path.isabs(path):
            filename = os.path.basename(path)
            blob_path = f"artifacts/{session_key}/{filename}"
            artifacts.append(blob_path)
        else:
            artifacts.append(path)

    logger.info("artifacts chat=%s count=%d", chat_id, len(artifacts))
    return {"artifacts": artifacts}


def _fallback_actions() -> List[Dict[str, str]]:
    return [
        {
            "label": "Summarize My RCM",
            "prompt": "I uploaded an RCM file. Summarize rows, columns, key processes, and missing required fields.",
        },
        {
            "label": "Run Gap Analysis",
            "prompt": "Run AI gap analysis for my RCM and show high-priority suggestions first.",
        },
        {
            "label": "Run TOD + TOE",
            "prompt": "Run Test of Design and Test of Effectiveness and summarize the failed controls first.",
        },
    ]


def _generate_starter_actions() -> List[Dict[str, str]]:
    """
    Ask the model to suggest concise starter actions for the user.
    Falls back to deterministic defaults if generation fails.
    """
    try:
        from agent.config import get_config
        from agent.llm import LLMClient
        from agent.tools import discover_tools

        tool_names = sorted(discover_tools().keys())
        cfg = get_config()
        llm = LLMClient(
            api_key=cfg.openai_api_key,
            model=cfg.openai_model,
            azure_endpoint=cfg.azure_openai_endpoint,
            azure_api_version=cfg.azure_openai_api_version,
        )
        response_text = llm.complete(
            system=(
                "You create starter prompts for a control testing assistant UI. "
                "Return strict JSON with this shape only: "
                '{"actions":[{"label":"short button label","prompt":"user message to send"}]}. '
                "Rules: 3 actions only, label max 28 chars, prompt max 170 chars, "
                "practical next steps for first-time user, no markdown."
            ),
            user=(
                "Available tools:\n"
                + "\n".join(f"- {name}" for name in tool_names)
                + "\n\nGenerate 3 high-value starter actions."
            ),
            max_tokens=260,
            response_format={"type": "json_object"},
        )
        parsed = json.loads(response_text)
        raw_actions = parsed.get("actions", [])
        clean_actions: List[Dict[str, str]] = []
        for item in raw_actions:
            if not isinstance(item, dict):
                continue
            label = str(item.get("label", "")).strip()[:28]
            prompt = str(item.get("prompt", "")).strip()[:170]
            if label and prompt:
                clean_actions.append({"label": label, "prompt": prompt})
        if len(clean_actions) >= 3:
            return clean_actions[:3]
    except Exception:
        logger.exception("Failed to generate AI starter actions; using fallback")

    return _fallback_actions()
