"""
Message endpoints — send messages and stream responses via SSE.

Uses LangGraph's astream_events() for native token-level streaming.
"""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any, Dict
import ast

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from langchain_core.messages import (
    AIMessage,
    HumanMessage,
    ToolMessage,
)

from ..models import MessagesResponse, MessageResponse, SendMessageRequest

logger = logging.getLogger("server.routes.messages")

router = APIRouter(prefix="/api", tags=["messages"])


def _serialize_message(msg, idx: int = 0) -> Dict[str, Any]:
    """Convert a LangChain message to a JSON-serializable dict."""
    result: Dict[str, Any] = {
        "id": getattr(msg, "id", str(idx)),
        "role": "assistant" if isinstance(msg, AIMessage) else
                "user" if isinstance(msg, HumanMessage) else
                "tool" if isinstance(msg, ToolMessage) else "system",
        "content": None,
        "tool_name": None,
        "tool_args": None,
        "tool_call_id": None,
    }

    if isinstance(msg, ToolMessage):
        result["content"] = _content_to_text(msg.content)
        result["tool_name"] = getattr(msg, "name", None)
        result["tool_call_id"] = getattr(msg, "tool_call_id", None)
    elif isinstance(msg, AIMessage):
        result["content"] = _content_to_text(msg.content)
        if hasattr(msg, "tool_calls") and msg.tool_calls:
            result["tool_args"] = [
                {"id": tc.get("id", ""), "name": tc.get("name", ""), "args": tc.get("args", {})}
                for tc in msg.tool_calls
            ]
    else:
        content = getattr(msg, "content", None)
        result["content"] = _content_to_text(content if content is not None else msg)

    return result


def _content_to_text(content: Any) -> str | None:
    """Normalize LangChain content payloads to plain text for API/UI use."""
    if content is None:
        return None
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                text = item.get("text")
                if text:
                    parts.append(str(text))
        joined = "".join(parts)
        if joined.strip():          # has non-whitespace content?
            return joined             # preserve original whitespace
    return json.dumps(content, default=str)


@router.get("/chats/{chat_id}/messages", response_model=MessagesResponse)
async def get_messages(chat_id: str, request: Request):
    """Get all messages for a chat.

    Strategy:
      1. Try LangGraph checkpoint (in-memory SQLite) — fastest, has full graph state.
      2. Fall back to Cosmos DB Messages container — durable, survives restarts.
    """
    graph = request.app.state.graph
    db = request.app.state.db

    if db is not None:
        chat = db.get_chat(chat_id)
        if not chat:
            raise HTTPException(status_code=404, detail="Chat not found")

    # 1. Try LangGraph checkpoint first
    messages = []
    facts: list = []
    try:
        state = await graph.aget_state({"configurable": {"thread_id": chat_id}})
        messages = state.values.get("messages", []) if state.values else []
        facts = state.values.get("facts", []) if state.values else []
    except Exception:
        pass

    if messages:
        return MessagesResponse(
            messages=[
                MessageResponse(**_serialize_message(msg, idx))
                for idx, msg in enumerate(messages)
            ],
            facts=facts,
        )

    # 2. Fall back to Cosmos DB Messages container
    try:
        from ..cosmos_store import get_cosmos_store
        cosmos_msgs = get_cosmos_store().get_messages(chat_id)
        if cosmos_msgs:
            logger.info(
                "get_messages: restored %d message(s) from Cosmos for chat=%s",
                len(cosmos_msgs), chat_id,
            )
            return MessagesResponse(
                messages=[
                    MessageResponse(
                        id=m.get("id", str(idx)),
                        role="user" if m.get("role") == "user" else "assistant",
                        content=m.get("content"),
                        tool_name=m.get("toolName"),
                        tool_args=m.get("toolArgs"),
                        tool_call_id=m.get("toolCallId"),
                        created_at=m.get("createdAt"),
                    )
                    for idx, m in enumerate(cosmos_msgs)
                ],
                facts=[],
            )
    except Exception as exc:
        logger.warning("get_messages: Cosmos fallback failed (non-fatal): %s", exc)

    return MessagesResponse(messages=[], facts=[])


def _find_suffix_prefix_overlap(a: str, b: str) -> int:
    """Return the length of the longest suffix of *a* that equals a prefix of *b*.

    Used to detect when Azure OpenAI sends a token whose beginning overlaps
    with text we've already accumulated — merging at the overlap point avoids
    duplication.
    """
    max_overlap = min(len(a), len(b))
    # Start from the longest possible overlap and work down for accuracy.
    # Limit to 500 chars to keep this O(n) bounded.
    for length in range(min(max_overlap, 500), 0, -1):
        if a.endswith(b[:length]):
            return length
    return 0


@router.post("/chats/{chat_id}/messages")
async def send_message(chat_id: str, body: SendMessageRequest, request: Request):
    """
    Send a user message and stream the agent's response via SSE.

    Uses LangGraph's astream_events(version="v2") for:
    - Token-level streaming (on_chat_model_stream)
    - Tool execution events (on_tool_start, on_tool_end)
    """
    graph = request.app.state.graph
    db = request.app.state.db
    state_manager = request.app.state.state_manager

    if db is None:
        raise HTTPException(status_code=503, detail="Database not available")

    chat = db.get_chat(chat_id)
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")

    runtime_state = state_manager.get_or_create(chat_id)

    # Inject uploaded document metadata into agent state so the system prompt
    # knows about previously uploaded files (survives logout/login cycles).
    if body.documents:
        runtime_state.uploaded_documents = body.documents
        logger.info(
            "send_message: injected %d document(s) into runtime state for chat=%s",
            len(body.documents), chat_id,
        )

    config = {
        "configurable": {
            "thread_id": chat_id,
            "runtime_state": runtime_state,
        },
        "recursion_limit": 100,
    }

    inputs = {"messages": [HumanMessage(content=body.content)]}

    # Persist user message to Cosmos Messages
    try:
        from ..cosmos_store import get_cosmos_store
        get_cosmos_store().save_message(
            chat_id,
            role="user",
            content=body.content,
            message_id=str(uuid.uuid4()),
        )
    except Exception as _persist_exc:
        logger.warning("Failed to persist user message to Cosmos (non-fatal): %s", _persist_exc)

    async def event_generator():
        try:
            current_tool = None
            assistant_text_chunks = []
            # Track accumulated LLM output within one agent turn.
            # Some LLM backends (Azure OpenAI, proxies) emit on_chat_model_stream
            # chunks that contain the FULL accumulated text so far rather than a
            # true incremental delta.  We use robust overlap detection to handle
            # both modes seamlessly and always emit the full accumulated text.
            _agent_turn_text = ""

            async for event in graph.astream_events(
                inputs,
                config=config,
                version="v2",
            ):
                kind = event.get("event", "")

                # Token-level streaming from LLM
                if kind == "on_chat_model_stream":
                    # Only stream tokens produced by the agent node itself.
                    # Tools like run_sox_scoping_engine use internal LLM calls
                    # that also emit on_chat_model_stream events — filtering them
                    # out prevents their output from leaking into the chat stream.
                    if event.get("metadata", {}).get("langgraph_node") != "agent":
                        continue
                    chunk = event.get("data", {}).get("chunk")
                    token = None
                    if chunk and hasattr(chunk, "content"):
                        token = _content_to_text(chunk.content)
                    if token:
                        # Determine whether this token is a snapshot (full
                        # accumulated text) or a true delta (new chars only).
                        #
                        # Azure OpenAI / LangChain can emit tokens in three
                        # modes that we must handle:
                        #   1. True deltas (small new chars)
                        #   2. Full snapshots (entire text so far)
                        #   3. Mixed / inconsistent chunks
                        #
                        # Strategy: use suffix-overlap detection as the final
                        # fallback instead of blindly appending. If a token
                        # shares a significant overlap with the tail of the
                        # accumulated text, merge at the overlap point instead
                        # of duplicating.
                        prev_len = len(_agent_turn_text)
                        mode = "init"
                        if not _agent_turn_text:
                            _agent_turn_text = token
                            mode = "init"
                        elif token.startswith(_agent_turn_text):
                            # Snapshot that extends what we have
                            _agent_turn_text = token
                            mode = "snapshot-extends"
                        elif _agent_turn_text.startswith(token):
                            # Shorter snapshot — ignore
                            mode = "snapshot-shorter(skip)"
                        elif _agent_turn_text in token:
                            # Snapshot that contains our text (possibly with
                            # extra prefix/suffix)
                            _agent_turn_text = token
                            mode = "snapshot-contains"
                        else:
                            # Check for suffix-prefix overlap: the end of
                            # _agent_turn_text may match the start of token.
                            # This catches snapshots with slight differences
                            # that would otherwise be appended as deltas,
                            # causing duplication.
                            overlap = _find_suffix_prefix_overlap(
                                _agent_turn_text, token
                            )
                            if overlap >= min(20, len(token) // 2, prev_len // 2) and overlap > 0:
                                # Merge at overlap point
                                _agent_turn_text += token[overlap:]
                                mode = "delta-overlap"
                            elif len(token) > prev_len * 0.8 and len(token) > 50:
                                # Token is almost as long or longer than what
                                # we've accumulated — likely a snapshot with
                                # formatting differences.  Replace.
                                _agent_turn_text = token
                                mode = "snapshot-long"
                            elif len(token) <= 100:
                                # Short token — safe to treat as a true delta
                                _agent_turn_text += token
                                mode = "delta"
                            else:
                                # Long token that doesn't match any snapshot
                                # pattern — replace to avoid duplication.
                                _agent_turn_text = token
                                mode = "snapshot-fallback"

                        logger.debug(
                            "STREAM token mode=%s token_len=%d accum_len=%d token_preview=%.80r",
                            mode, len(token), len(_agent_turn_text),
                            token[:80],
                        )

                        assistant_text_chunks.append(token)
                        yield f"event: token\ndata: {json.dumps({'token': _agent_turn_text})}\n\n"

                # Tool execution start
                elif kind == "on_tool_start":
                    # Reset accumulated-text tracker for the next agent turn
                    logger.info(
                        "tool_start: %s | agent_turn_text_len=%d chars so far",
                        event.get("name", "unknown"), len(_agent_turn_text),
                    )
                    _agent_turn_text = ""
                    tool_name = event.get("name", "unknown")
                    tool_input = event.get("data", {}).get("input", {})
                    current_tool = tool_name

                    # Truncate large args for SSE
                    safe_input = {}
                    for k, v in (tool_input if isinstance(tool_input, dict) else {}).items():
                        s = str(v)
                        safe_input[k] = s[:300] + "..." if len(s) > 300 else s

                    # Tell the frontend which progress endpoint to poll during
                    # this tool's execution, and at what interval (ms).
                    _PROGRESS_TOOLS = {
                        "run_sox_scoping_engine": {
                            "endpoint": f"/api/chats/{chat_id}/scoping-progress",
                            "poll_interval_ms": 1500,
                        },
                        "run_test_of_design": {
                            "endpoint": f"/api/chats/{chat_id}/tool-progress",
                            "poll_interval_ms": 2000,
                        },
                        "run_test_of_effectiveness": {
                            "endpoint": f"/api/chats/{chat_id}/tool-progress",
                            "poll_interval_ms": 2000,
                        },
                        "run_quality_comparison": {
                            "endpoint": f"/api/chats/{chat_id}/tool-progress",
                            "poll_interval_ms": 2000,
                        },
                    }
                    progress_hint = _PROGRESS_TOOLS.get(tool_name)

                    payload = {"tool": tool_name, "args": safe_input}
                    if progress_hint:
                        payload["progress"] = progress_hint

                    yield f"event: tool_start\ndata: {json.dumps(payload)}\n\n"

                # Tool execution end
                elif kind == "on_tool_end":
                    tool_name = event.get("name", current_tool or "unknown")
                    output = event.get("data", {}).get("output", "")

                    # Parse the output to get success/error status.
                    # LangGraph astream_events v2 may deliver a LangChain ToolMessage
                    # object rather than a raw string — extract .content in that case.
                    logger.debug(
                        "tool_end raw output type=%s tool=%s",
                        type(output).__name__, tool_name,
                    )
                    if hasattr(output, "content"):
                        raw_content = output.content
                        if isinstance(raw_content, str):
                            output_str = raw_content
                        elif isinstance(raw_content, list):
                            parts = [
                                item.get("text", "") if isinstance(item, dict) else str(item)
                                for item in raw_content
                            ]
                            output_str = "".join(parts)
                        else:
                            output_str = json.dumps(raw_content, default=str)
                    elif isinstance(output, (dict, list)):
                        output_str = json.dumps(output, default=str)
                    else:
                        output_str = str(output)
                    artifacts = []
                    try:
                        parsed = json.loads(output_str) if isinstance(output_str, str) else {}
                    except (json.JSONDecodeError, TypeError):
                        parsed = {}
                        # Some tool adapters stringify Python dicts with single quotes.
                        try:
                            literal = ast.literal_eval(output_str)
                            if isinstance(literal, dict):
                                parsed = literal
                        except Exception:
                            parsed = {}
                    try:
                        nested_content = parsed.get("content")
                        if isinstance(nested_content, str):
                            nested_parsed = json.loads(nested_content)
                            if isinstance(nested_parsed, dict):
                                parsed = nested_parsed
                    except Exception:
                        pass
                    try:
                        if isinstance(parsed.get("data"), dict):
                            parsed = parsed["data"]
                    except Exception:
                        pass
                    try:
                        success = "error" not in parsed
                        summary = parsed.get("summary", "")
                        notes = parsed.get("_agent_notes", [])
                        for key in ("output_excel", "excel_output", "path", "file_path", "output_path"):
                            value = parsed.get(key)
                            if isinstance(value, str) and value:
                                artifacts.append(value)
                        parsed_artifacts = parsed.get("artifacts")
                        if isinstance(parsed_artifacts, list):
                            artifacts.extend([a for a in parsed_artifacts if isinstance(a, str)])
                    except Exception:
                        success = True
                        summary = output_str[:200]
                        notes = []
                        artifacts = []

                    # de-duplicate while preserving order
                    if artifacts:
                        artifacts = list(dict.fromkeys(artifacts))

                    result_json = json.dumps(parsed, default=str) if parsed else output_str
                    is_slim = parsed.get("_slim", False) if isinstance(parsed, dict) else False
                    # Extract phase for multi-phase tools (document_list, evidence_validation, etc.)
                    tool_phase = None
                    if isinstance(parsed, dict):
                        tool_phase = parsed.get("phase") or parsed.get("status")

                    payload = {
                        "tool": tool_name,
                        "success": success,
                        "summary": summary,
                        "notes": notes,
                        "result": result_json,
                        "artifacts": artifacts,
                    }
                    if tool_phase:
                        payload["phase"] = tool_phase
                    logger.info(
                        "SSE tool_end chat=%s tool=%s slim=%s result_chars=%d",
                        chat_id, tool_name, is_slim, len(result_json),
                    )
                    logger.info(
                        "tool_end chat=%s tool=%s success=%s artifacts=%s",
                        chat_id,
                        tool_name,
                        success,
                        artifacts,
                    )
                    # ── Persist tool result to Cosmos (non-blocking best-effort) ──
                    # Agent-memory: durable, survives Flask restarts
                    # Context-cache: TTL=3600s, for fast page-refresh restore
                    try:
                        from ..cosmos_store import get_cosmos_store
                        _store = get_cosmos_store()
                        # For slim results, the tool adapter already persisted the
                        # FULL result to both in-memory cache and Cosmos. Do NOT
                        # overwrite with the slim version here — that would replace
                        # full data with header-only stats and cause the frontend
                        # to show an infinite loading spinner.
                        if not is_slim:
                            _store.save_tool_result(chat_id, tool_name, result_json)
                            _store.save_context_cache(chat_id, tool_name, result_json)
                        # Also persist phase-specific keys for multi-phase tools
                        # so session-results can restore all phases after restart.
                        if tool_name == "run_sox_scoping_engine" and tool_phase:
                            phase_key = f"{tool_name}__{tool_phase}"
                            if not is_slim:
                                _store.save_tool_result(chat_id, phase_key, result_json)
                                _store.save_context_cache(chat_id, phase_key, result_json)
                        # Only update in-memory cache if the tool adapter hasn't
                        # already stored a full (non-slim) result. The SSE handler
                        # receives the slim version for _SLIM_RETURN_TOOLS, so
                        # overwriting would replace full data with slim data.
                        if not is_slim:
                            runtime_state.tool_results_cache[tool_name] = result_json
                    except Exception as _cosmos_exc:
                        logger.warning(
                            "Cosmos persist failed for tool_end (non-fatal): %s", _cosmos_exc
                        )

                    yield f"event: tool_end\ndata: {json.dumps(payload)}\n\n"
                    current_tool = None

            # Persist assistant response to Cosmos Messages
            if _agent_turn_text:
                try:
                    from ..cosmos_store import get_cosmos_store
                    get_cosmos_store().save_message(
                        chat_id,
                        role="assistant",
                        content=_agent_turn_text,
                        message_id=str(uuid.uuid4()),
                    )
                except Exception as _persist_exc:
                    logger.warning("Failed to persist assistant message to Cosmos (non-fatal): %s", _persist_exc)

            # Update chat timestamp
            db.touch_chat(chat_id)

            # Auto-name chat if title is still "New Chat"
            if chat.get("title") == "New Chat":
                _auto_name_chat(chat_id, body.content, db)

            yield f"event: done\ndata: {{}}\n\n"

        except Exception as exc:
            logger.exception("Error in SSE stream for chat %s", chat_id)
            yield f"event: error\ndata: {json.dumps({'error': str(exc)})}\n\n"
            yield f"event: done\ndata: {{}}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/chats/{chat_id}/scoping-progress")
async def get_scoping_progress(chat_id: str, request: Request):
    """Return live scoping engine progress for frontend polling."""
    state_manager = request.app.state.state_manager
    runtime_state = state_manager.get_or_create(chat_id)
    progress = runtime_state.scoping_progress
    if not progress:
        return {"phase": None}
    # Strip internal keys (prefixed with _) before sending to frontend
    return {k: v for k, v in progress.items() if not k.startswith("_")}


@router.get("/chats/{chat_id}/tool-progress")
async def get_tool_progress(chat_id: str, request: Request):
    """Return live TOD/TOE tool progress for frontend polling."""
    state_manager = request.app.state.state_manager
    runtime_state = state_manager.get_or_create(chat_id)
    progress = runtime_state.tool_progress
    if not progress:
        return {"tool": None}
    return {k: v for k, v in progress.items() if not k.startswith("_")}


@router.get("/chats/{chat_id}/workflow-state")
async def get_workflow_state(chat_id: str, request: Request):
    """Return persisted workflow progress state for sidebar hydration on page refresh."""
    db = request.app.state.db
    if db is None:
        raise HTTPException(status_code=503, detail="Database not available")

    chat = db.get_chat(chat_id)
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")

    # Try Cosmos Agent-memory first
    try:
        from ..cosmos_store import get_cosmos_store
        store = get_cosmos_store()
        persisted = store.restore_agent_memory(chat_id)
        raw = persisted.get("__workflow_state__")
        if raw:
            state = json.loads(raw) if isinstance(raw, str) else raw
            logger.info("workflow-state: restored from Cosmos for chat=%s", chat_id)
            return {"success": True, "data": state}
    except Exception as exc:
        logger.warning("workflow-state: Cosmos restore failed (non-fatal): %s", exc)

    return {"success": True, "data": None}


@router.patch("/chats/{chat_id}/workflow-state")
async def update_workflow_state(chat_id: str, request: Request):
    """Persist workflow progress state so it survives page refresh."""
    db = request.app.state.db
    if db is None:
        raise HTTPException(status_code=503, detail="Database not available")

    chat = db.get_chat(chat_id)
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")

    body = await request.json()
    state = body.get("state")
    if not state:
        raise HTTPException(status_code=400, detail="No state provided")

    state_json = json.dumps(state, default=str)

    # Persist to Cosmos Agent-memory (durable, survives Flask restarts)
    try:
        from ..cosmos_store import get_cosmos_store
        store = get_cosmos_store()
        store.save_tool_result(chat_id, "__workflow_state__", state_json)
        logger.info("workflow-state: persisted for chat=%s (%d bytes)", chat_id, len(state_json))
    except Exception as exc:
        logger.warning("workflow-state: Cosmos persist failed (non-fatal): %s", exc)

    return {"success": True, "data": state}


@router.get("/chats/{chat_id}/session-results")
async def get_session_results(chat_id: str, request: Request):
    """
    Return cached tool result JSON for page-refresh restore.

    RuntimeStateManager keeps in-memory AgentState per thread_id.
    If evicted (idle timeout), returns empty results — frontend falls back gracefully.
    """
    state_manager = request.app.state.state_manager
    db = request.app.state.db

    chat = db.get_chat(chat_id)
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")

    runtime_state = state_manager.get_or_create(chat_id)
    cache = dict(runtime_state.tool_results_cache)

    # If in-memory cache is empty (e.g. after a Flask restart), fall back to
    # Cosmos Agent-memory for durable restore.
    if not cache:
        try:
            from ..cosmos_store import get_cosmos_store
            store = get_cosmos_store()
            restored = store.restore_agent_memory(chat_id)
            if restored:
                cache = restored
                runtime_state.tool_results_cache.update(restored)
                logger.info(
                    "session-results restored %d keys from Agent-memory for chat=%s",
                    len(restored), chat_id,
                )
        except Exception as exc:
            logger.warning("session-results Cosmos restore failed (non-fatal): %s", exc)

    cache_keys = list(cache.keys())
    cache_sizes = {k: len(v) for k, v in cache.items()}
    logger.info(
        "session-results chat=%s keys=%s sizes=%s",
        chat_id, cache_keys, cache_sizes,
    )
    return {"results": cache}


@router.post("/chats/{chat_id}/toe-approval")
async def submit_toe_approval(chat_id: str, request: Request):
    """Receive edited TOE attribute schemas from the frontend approval modal."""
    state_manager = request.app.state.state_manager
    db = request.app.state.db

    chat = db.get_chat(chat_id)
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")

    runtime_state = state_manager.get_or_create(chat_id)

    body = await request.json()
    edited_schemas = body.get("schemas", {})
    if not edited_schemas:
        raise HTTPException(status_code=400, detail="No schemas provided")

    # Update the pending schemas with user edits
    runtime_state.pending_toe_schemas = edited_schemas
    logger.info("TOE approval: updated %d schemas for chat %s", len(edited_schemas), chat_id)

    return {"status": "ok", "schemas_updated": len(edited_schemas)}


@router.post("/chats/{chat_id}/update-sample-count")
async def update_sample_count(chat_id: str, request: Request):
    """Update the sample count for a single control directly (bypassing chat).

    Calls ModifySamplingOutputTool internally so the RCM dataframe and
    cached sampling results are updated atomically.
    """
    state_manager = request.app.state.state_manager
    db = request.app.state.db

    chat = db.get_chat(chat_id)
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")

    runtime_state = state_manager.get_or_create(chat_id)

    body = await request.json()
    control_id = body.get("control_id", "").strip()
    sample_count = body.get("sample_count")

    if not control_id:
        raise HTTPException(status_code=400, detail="control_id is required")
    if sample_count is None:
        raise HTTPException(status_code=400, detail="sample_count is required")

    try:
        new_count = int(float(str(sample_count)))
        if new_count < 0:
            raise ValueError
    except (ValueError, TypeError):
        raise HTTPException(status_code=400, detail=f"Invalid sample_count: {sample_count!r}")

    from agent.tools.modify_sampling_output import ModifySamplingOutputTool
    tool = ModifySamplingOutputTool()

    precond = tool.preconditions(runtime_state)
    if precond:
        raise HTTPException(status_code=422, detail=precond)

    result = tool.execute(
        {"control_id": control_id, "sample_count": str(new_count)},
        runtime_state,
    )

    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)

    # Persist updated sampling results back to the tool_results_cache
    # so the frontend can restore them on page refresh.
    try:
        import json as _json
        from agent.utils import sanitize_for_json
        updated_cache = {
            "engine_type": "KPMG Standard",
            "controls_processed": len(runtime_state.sampling_results or []),
            "total_samples_required": result.data.get("total_samples"),
            "per_control_results": runtime_state.sampling_results or [],
        }
        runtime_state.tool_results_cache["run_sampling_engine"] = _json.dumps(
            sanitize_for_json(updated_cache)
        )
    except Exception as _e:
        logger.warning("update-sample-count: failed to persist cache: %s", _e)

    logger.info(
        "update-sample-count: chat=%s control=%s old=%s new=%s",
        chat_id, control_id, result.data.get("old_sample_count"), new_count,
    )
    return result.data


@router.post("/chats/{chat_id}/tod-approval")
async def submit_tod_approval(chat_id: str, request: Request):
    """Receive edited TOD attribute schemas from the frontend approval modal."""
    state_manager = request.app.state.state_manager
    db = request.app.state.db

    chat = db.get_chat(chat_id)
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")

    runtime_state = state_manager.get_or_create(chat_id)

    body = await request.json()
    edited_schemas = body.get("schemas", {})
    if not edited_schemas:
        raise HTTPException(status_code=400, detail="No schemas provided")

    # Update the pending schemas with user edits
    runtime_state.pending_tod_schemas = edited_schemas
    logger.info("TOD approval: updated %d schemas for chat %s", len(edited_schemas), chat_id)

    return {"status": "ok", "schemas_updated": len(edited_schemas)}


def _auto_name_chat(chat_id: str, first_message: str, db):
    """Generate a short title from the first user message."""
    try:
        from agent.config import get_config
        from agent.llm import LLMClient

        cfg = get_config()
        llm = LLMClient(
            api_key=cfg.openai_api_key,
            model=cfg.openai_model,
            azure_endpoint=cfg.azure_openai_endpoint,
            azure_api_version=cfg.azure_openai_api_version,
        )
        title = llm.complete(
            system=(
                "Generate a short title (3-6 words) for a chat that starts with "
                "this message. The chat is about audit compliance and control testing work. "
                "Return ONLY the title, no quotes, no punctuation at the end."
            ),
            user=first_message,
            max_tokens=20,
        )
        title = title.strip().strip('"').strip("'")[:50]
    except Exception:
        title = first_message[:40] + ("..." if len(first_message) > 40 else "")

    db.update_chat_title(chat_id, title)
