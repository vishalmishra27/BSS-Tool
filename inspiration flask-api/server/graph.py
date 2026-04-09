"""
LangGraph agent definition — custom StateGraph for control testing.

Uses a custom StateGraph (not prebuilt create_react_agent) because we need:
- Dynamic system prompt built from runtime AgentState (DataFrames, caches)
- Custom message windowing (last 40 messages + semantic facts)
- Reflector notes injected via the tool adapter
"""

from __future__ import annotations

import json
import logging
from typing import Annotated, Any, Dict, List, Optional, Sequence, Tuple

from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)
from langchain_core.runnables import RunnableConfig
from langchain_openai import AzureChatOpenAI, ChatOpenAI
from langgraph.graph import StateGraph, END
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode
from typing_extensions import TypedDict

from agent.config import get_config
from agent.core.executor import _check_tod_toe_mismatch
from agent.core.playbook_store import get_playbook_store, init_playbook_store
from agent.core.loop import _strip_llm_suggestions
from agent.core.suggestions import get_suggestions
from agent.prompts.system import build_system_prompt
from agent.types import AgentState, ToolCategory

from .config import get_server_config
from .tool_adapter import adapt_tools

logger = logging.getLogger("server.graph")


# ---------------------------------------------------------------------------
# State definition
# ---------------------------------------------------------------------------

class GraphState(TypedDict):
    """LangGraph state — messages + semantic facts for compaction."""
    messages: Annotated[list, add_messages]
    facts: List[str]
    iteration_count: int


# ---------------------------------------------------------------------------
# Module-level singletons (initialized by create_graph)
# ---------------------------------------------------------------------------

_model: Optional[ChatOpenAI] = None
_tools: Optional[list] = None
_tool_summary: Optional[str] = None


def _get_model() -> ChatOpenAI:
    global _model
    if _model is None:
        cfg = get_config()
        if cfg.azure_openai_endpoint:
            _model = AzureChatOpenAI(
                api_key=cfg.openai_api_key,
                azure_endpoint=cfg.azure_openai_endpoint,
                api_version=cfg.azure_openai_api_version,
                azure_deployment=cfg.openai_model,
                max_completion_tokens=2048,
                streaming=True,
            )
        else:
            _model = ChatOpenAI(
                api_key=cfg.openai_api_key,
                model=cfg.openai_model,
                max_completion_tokens=2048,
                streaming=True,
            )
    return _model


def _get_tools():
    global _tools, _tool_summary
    if _tools is None:
        # Build capability summary from the adapted tools themselves
        _tools = adapt_tools()
        # Build summary from adapted tool names and descriptions
        # Group by inferring category from tool name patterns
        lines = []
        for t in _tools:
            lines.append(f"  - {t.name}: {t.description}")
        _tool_summary = "\n".join(lines)
    return _tools, _tool_summary


# ---------------------------------------------------------------------------
# Graph nodes
# ---------------------------------------------------------------------------

def call_model(state: GraphState, config: RunnableConfig) -> Dict[str, Any]:
    """
    Build dynamic system prompt, window messages, call LLM with tools.
    """
    server_cfg = get_server_config()

    # Get runtime state (AgentState with DataFrames) from config
    runtime_state: AgentState = config.get("configurable", {}).get(
        "runtime_state", AgentState()
    )

    tools, tool_summary = _get_tools()
    model = _get_model()

    # 0. Detect new user turn for iteration reset
    all_messages = state.get("messages", [])
    is_new_turn = all_messages and isinstance(all_messages[-1], HumanMessage)

    # Clear scoping_awaiting_input on new user turn so the scoping engine
    # can advance to the next phase.  This mirrors the logic in loop.py
    # process_message() which clears the flag when a new user message arrives.
    if is_new_turn and runtime_state.scoping_awaiting_input:
        runtime_state.scoping_awaiting_input = False
        logger.info("scoping_awaiting_input cleared — new user message received (graph)")

    # 1. Build dynamic system prompt from runtime state
    system_prompt = build_system_prompt(state=runtime_state, tool_summary=tool_summary)

    # 1b. Retrieve relevant playbooks based on the latest user message
    user_query = _extract_latest_user_query(all_messages)
    if user_query:
        store = get_playbook_store()
        if store and store.ready:
            matched = store.retrieve(user_query, top_k=3)
            if matched:
                system_prompt += "\n\n" + store.format_for_prompt(matched)

    # 2. Assemble LLM messages
    llm_messages: list[BaseMessage] = [SystemMessage(content=system_prompt)]

    # Inject semantic facts (survive message windowing)
    facts = state.get("facts", [])
    if facts:
        facts_text = "SESSION FACTS (key results from earlier in this conversation):\n" + \
            "\n".join(f"  - {f}" for f in facts)
        llm_messages.append(HumanMessage(content=facts_text))
        llm_messages.append(AIMessage(content="Acknowledged — I have these facts in context."))

    # 3. Window messages to last N
    max_ctx = server_cfg.max_context_messages
    if len(all_messages) > max_ctx:
        recent = all_messages[-max_ctx:]
    else:
        recent = list(all_messages)

    # Ensure message ordering is valid for OpenAI tool-call protocol.
    recent = _sanitize_messages_for_openai(recent)

    llm_messages.extend(recent)

    # 4. Call LLM with tools bound
    model_with_tools = model.bind_tools([t for t in tools])

    try:
        response = model_with_tools.invoke(llm_messages)
    except Exception as exc:
        logger.error("LLM API call failed: %s", exc)
        error_msg = f"I'm having trouble connecting to the AI service: {exc}"
        return {"messages": [AIMessage(content=error_msg)]}

    # TOD / TOE mismatch guard — block before tool execution
    if getattr(response, "tool_calls", None) and user_query:
        for tc in response.tool_calls:
            tc_name = tc.get("name", "") if isinstance(tc, dict) else getattr(tc, "name", "")
            mismatch_err = _check_tod_toe_mismatch(tc_name, user_query)
            if mismatch_err:
                logger.warning("TOD/TOE mismatch blocked in graph: %s", mismatch_err)
                response = AIMessage(content=mismatch_err, id=response.id)
                break

    # Append or replace suggestions with deterministic ones.
    # The LLM may include its own "What you can do next" but miss critical
    # workflow steps (e.g. sampling after TOD). Deterministic suggestions
    # are authoritative and always override the LLM's version.
    if not getattr(response, "tool_calls", None):
        content = response.content or ""
        last_tool = None
        for msg in reversed(all_messages):
            if isinstance(msg, ToolMessage):
                tool_content = msg.content or ""
                if '"error"' not in tool_content:
                    last_tool = getattr(msg, "name", None)
                    break
        suggestions_block = get_suggestions(runtime_state, last_tool)
        if suggestions_block:
            # Strip any LLM-generated suggestions/upload prompts so ours
            # take precedence (prevents duplicate display).
            content = _strip_llm_suggestions(content)
            response = AIMessage(
                content=content + suggestions_block,
                id=response.id,
            )
            logger.info("Set deterministic suggestions (last_tool=%s)", last_tool)

    # Increment iteration count (reset to 1 on new user turn)
    if is_new_turn:
        current_count = 1
    else:
        current_count = state.get("iteration_count", 0) + 1
    return {"messages": [response], "iteration_count": current_count}


def extract_facts(state: GraphState, config: RunnableConfig) -> Dict[str, Any]:
    """
    After tools execute, extract semantic facts from tool results.
    Mirrors agent/core/memory.py _auto_extract_facts().

    Facts are capped at MAX_FACTS to prevent unbounded growth.
    Loading a new RCM clears all prior facts (they're stale).
    """
    MAX_FACTS = 15

    messages = state.get("messages", [])
    existing_facts = list(state.get("facts", []))
    new_facts: list[str] = []
    rcm_reloaded = False

    # Look at the most recent ToolMessages
    for msg in reversed(messages):
        if not isinstance(msg, ToolMessage):
            break
        try:
            result = json.loads(msg.content) if isinstance(msg.content, str) else {}
        except (json.JSONDecodeError, TypeError):
            continue

        tool_name = msg.name or ""

        if tool_name == "load_rcm" and result.get("rows"):
            new_facts.append(
                f"RCM loaded: {result['rows']} rows, {len(result.get('columns', []))} columns"
            )
            # New RCM loaded — old facts about prior RCM are stale
            rcm_reloaded = True
        elif tool_name == "run_ai_suggestions" and result.get("suggestion_count"):
            new_facts.append(f"AI suggestions: {result['suggestion_count']} generated")
        elif tool_name == "merge_suggestions" and result.get("merged_count"):
            new_facts.append(
                f"Merged {result['merged_count']} suggestions → {result.get('new_total', '?')} rows"
            )
        elif tool_name == "run_control_assessment" and result.get("controls_evaluated"):
            new_facts.append(f"Control assessment: {result['controls_evaluated']} controls evaluated")
        elif tool_name == "run_deduplication" and result.get("pair_count") is not None:
            new_facts.append(f"Deduplication: {result['pair_count']} duplicate pairs found")
        elif tool_name == "run_test_of_design" and result.get("controls_evaluated"):
            new_facts.append(
                f"TOD: {result.get('passed', 0)} PASS, {result.get('failed', 0)} FAIL "
                f"out of {result['controls_evaluated']}"
            )
        elif tool_name == "run_test_of_effectiveness" and result.get("controls_evaluated"):
            new_facts.append(
                f"TOE: {result.get('effective', 0)} Effective, "
                f"{result.get('not_effective', 0)} Not Effective"
            )
        elif tool_name == "run_sampling_engine" and result.get("controls_processed"):
            new_facts.append(
                f"Sampling ({result.get('engine_type', 'KPMG')}): "
                f"{result['controls_processed']} controls, "
                f"{result.get('total_samples_required', 0)} total samples"
            )
        elif tool_name == "preview_tod_attributes" and result.get("controls_count"):
            new_facts.append(
                f"TOD attributes previewed: {result['controls_count']} controls, "
                f"{result.get('total_attributes', 0)} attributes — awaiting approval"
            )
        elif tool_name == "preview_toe_attributes" and result.get("controls_count"):
            new_facts.append(
                f"TOE attributes previewed: {result['controls_count']} controls, "
                f"{result.get('total_attributes', 0)} attributes — awaiting approval"
            )
        elif tool_name == "run_sox_scoping_engine" and result.get("output_excel"):
            new_facts.append(
                f"Control scoping complete: {result.get('in_scope_accounts', 0)} in-scope, "
                f"{result.get('out_of_scope_accounts', 0)} out-of-scope "
                f"(output: {result.get('output_excel')})"
            )
        elif tool_name == "index_handbook" and result.get("document_name"):
            new_facts.append(
                f"Handbook indexed: '{result['document_name']}' "
                f"({result.get('total_pages', '?')} pages, {result.get('total_chunks', '?')} chunks)"
            )
        elif tool_name == "remove_failed_tod_controls" and result.get("removed_count") is not None:
            new_facts.append(
                f"Removed {result['removed_count']} failed controls, "
                f"{result.get('remaining_count', '?')} remaining"
            )

    if new_facts:
        # If a new RCM was loaded, discard all prior facts
        if rcm_reloaded:
            combined = new_facts
        else:
            combined = existing_facts + new_facts
        # Cap at MAX_FACTS — keep the most recent ones
        if len(combined) > MAX_FACTS:
            combined = combined[-MAX_FACTS:]
        return {"facts": combined}
    return {}


def route_after_agent(state: GraphState) -> str:
    """Route to tools if the last message has tool calls, else END.

    Includes a safety guard: if iteration_count exceeds the configured
    max_iterations, force-stop the loop to prevent infinite recursion.
    """
    server_cfg = get_server_config()
    iteration_count = state.get("iteration_count", 0)

    if iteration_count >= server_cfg.max_iterations:
        logger.warning(
            "Max iterations reached (%d/%d) — forcing END",
            iteration_count, server_cfg.max_iterations,
        )
        return END

    if iteration_count > server_cfg.max_iterations - 5:
        logger.warning(
            "Approaching iteration limit: %d/%d",
            iteration_count, server_cfg.max_iterations,
        )

    messages = state.get("messages", [])
    if not messages:
        return END

    last = messages[-1]
    if hasattr(last, "tool_calls") and last.tool_calls:
        return "tools"
    return END


def _sanitize_messages_for_openai(messages: list) -> list:
    """
    Make a message sequence safe for OpenAI tool-call validation:
    - Drop orphan ToolMessages with no pending tool_call_id
    - Inject synthetic ToolMessages for unresolved tool calls
    - Ensure tool responses happen before subsequent non-tool messages
    """
    sanitized: list = []
    pending_tool_calls: Dict[str, str] = {}

    def flush_pending() -> None:
        if not pending_tool_calls:
            return
        for tc_id, tc_name in list(pending_tool_calls.items()):
            sanitized.append(
                ToolMessage(
                    content='{"error": "Tool execution skipped — iteration limit reached."}',
                    tool_call_id=tc_id,
                    name=tc_name or "unknown",
                )
            )
            logger.debug("Injected synthetic ToolMessage for missing call %s", tc_id)
        pending_tool_calls.clear()

    def extract_tool_ids(msg: AIMessage) -> List[Tuple[str, str]]:
        pairs: List[Tuple[str, str]] = []
        for tc in getattr(msg, "tool_calls", []) or []:
            tc_id = tc.get("id") or tc.get("tool_call_id", "")
            tc_name = tc.get("name", "unknown")
            if tc_id:
                pairs.append((tc_id, tc_name))
        return pairs

    for msg in messages:
        if isinstance(msg, ToolMessage):
            tc_id = getattr(msg, "tool_call_id", None)
            if tc_id and tc_id in pending_tool_calls:
                sanitized.append(msg)
                pending_tool_calls.pop(tc_id, None)
            else:
                logger.debug("Dropping orphan ToolMessage (tool_call_id=%s)", tc_id)
            continue

        if pending_tool_calls:
            flush_pending()

        sanitized.append(msg)

        if isinstance(msg, AIMessage):
            for tc_id, tc_name in extract_tool_ids(msg):
                pending_tool_calls[tc_id] = tc_name

    if pending_tool_calls:
        flush_pending()

    return sanitized


def _extract_latest_user_query(messages) -> Optional[str]:
    """Walk messages backward to find the most recent user query."""
    for msg in reversed(messages):
        if isinstance(msg, HumanMessage) and msg.content:
            content = msg.content.strip()
            # Skip injected facts/plan blocks
            if content.startswith("SESSION FACTS") or content.startswith("YOUR WORKING PLAN"):
                continue
            return content
    return None


# ---------------------------------------------------------------------------
# Graph construction
# ---------------------------------------------------------------------------

def create_graph(checkpointer):
    """
    Build and compile the LangGraph StateGraph.

    Returns a compiled graph ready for .invoke() / .astream_events().
    """
    tools, _ = _get_tools()

    # Initialize playbook store for RAG-based workflow examples
    cfg = get_config()
    try:
        init_playbook_store(
            openai_api_key=cfg.openai_api_key,
            azure_endpoint=cfg.azure_openai_endpoint,
            azure_api_version=cfg.azure_openai_api_version,
            embedding_model=cfg.azure_openai_embedding_deployment,
        )
        logger.info("Playbook store initialized")
    except Exception:
        logger.exception("Playbook store initialization failed — continuing without playbooks")

    # ToolNode handles executing all LangChain tools
    tool_node = ToolNode(tools)

    builder = StateGraph(GraphState)

    # Nodes
    builder.add_node("agent", call_model)
    builder.add_node("tools", tool_node)
    builder.add_node("extract_facts", extract_facts)

    # Edges
    builder.set_entry_point("agent")
    builder.add_conditional_edges(
        "agent",
        route_after_agent,
        {"tools": "tools", END: END},
    )
    builder.add_edge("tools", "extract_facts")
    builder.add_edge("extract_facts", "agent")

    graph = builder.compile(checkpointer=checkpointer)
    logger.info("LangGraph agent compiled with %d tools", len(tools))
    return graph
