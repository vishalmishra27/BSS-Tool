"""
Tool adapter — wraps existing agent/tools as LangChain StructuredTools.

Each tool from agent/tools/ is converted to a LangChain StructuredTool
that can be used with LangGraph. The tool logic stays identical — only
the wrapper changes.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from langchain_core.runnables import RunnableConfig
from langchain_core.tools import StructuredTool
from pydantic import Field, create_model

from agent.tools import discover_tools
from agent.types import AgentState, ToolResult
from agent.utils import sanitize_for_json
from agent.core.reflector import Reflector
from .config import get_server_config

logger = logging.getLogger("server.tool_adapter")

_reflector = Reflector()

# Tools whose full result data is too large for the LLM context.
# Full data is cached in tool_results_cache for the frontend;
# only a slim summary is returned to the LLM to save tokens.
_SLIM_RETURN_TOOLS = {
    "load_rcm",
    "run_test_of_design",
    "run_test_of_effectiveness",
    "run_sox_scoping_engine",
    "run_sampling_engine",
    "index_handbook",
    "ask_handbook",
}


def _make_slim_result(tool_name: str, full_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create a token-efficient summary of a tool result for the LLM.

    The full result is already cached in tool_results_cache and served to
    the frontend via the session-results endpoint.  The LLM only needs
    enough context to decide the next step — not every row of data.
    """

    if tool_name == "load_rcm":
        # Strip bulky rcm_rows from LLM response — full data cached for frontend
        slim = {k: v for k, v in full_data.items() if k != "rcm_rows"}
        slim["_slim"] = True
        slim["_note"] = (
            "Full RCM row data is cached and displayed in the frontend dashboard. "
            "Use the summary fields above (rows, processes, control_ids, etc.)."
        )
        return slim

    if tool_name == "run_test_of_design":
        # Phase 1 (document_list) or Phase 2 (evidence_validation) — pass through as-is
        if full_data.get("phase") in ("document_list", "evidence_validation"):
            return full_data

        # Phase 3 (full test) — results + control_summary are the large per-control lists
        no_evidence = full_data.get("no_evidence", 0)
        control_summary_count = len(full_data.get("control_summary", []))
        slim = {
            "controls_evaluated": full_data.get("controls_evaluated"),
            "passed": full_data.get("passed"),
            "failed": full_data.get("failed"),
            "no_evidence": no_evidence,
            "controls_with_evidence": full_data.get("controls_with_evidence"),
            "control_summary_count": control_summary_count,
            "output_excel": full_data.get("output_excel"),
            "output_blob_path": full_data.get("output_blob_path"),
            "document_list": full_data.get("document_list"),
            "_agent_notes": full_data.get("_agent_notes", []),
            "_slim": True,
            "_note": (
                "Full per-control results are cached and displayed in the frontend "
                "data pane automatically. Do NOT ask the user to re-run TOD. "
                "Summarize the pass/fail counts above and mention the Required Documents "
                "Excel if document_list is present. Then ask what to do next."
            ),
        }
        logger.info(
            "SLIM TOD: full_size=%d chars → slim_size=%d chars",
            len(json.dumps(full_data, default=str)),
            len(json.dumps(slim, default=str)),
        )
        return slim

    if tool_name == "run_test_of_effectiveness":
        # Phase 1 (document_list) or Phase 2 (evidence_validation) — pass through as-is
        if full_data.get("phase") in ("document_list", "evidence_validation"):
            return full_data

        # Phase 3 (full test) — toe_report is a text string; summary is a LIST of per-control dicts (large!)
        report = full_data.get("toe_report", "")
        if isinstance(report, str) and len(report) > 800:
            report = report[:800] + "… (truncated)"
        slim = {
            "controls_evaluated": full_data.get("controls_evaluated"),
            "effective": full_data.get("effective"),
            "effective_with_exceptions": full_data.get("effective_with_exceptions"),
            "not_effective": full_data.get("not_effective"),
            "tod_failed": full_data.get("tod_failed", 0),
            "no_evidence": full_data.get("no_evidence", 0),
            "output_excel": full_data.get("output_excel"),
            "output_blob_path": full_data.get("output_blob_path"),
            "toe_report": report,
            "document_list": full_data.get("document_list"),
            "_agent_notes": full_data.get("_agent_notes", []),
            "_slim": True,
            "_note": (
                "Full per-control TOE results and workpapers are cached and displayed "
                "in the frontend data pane automatically. Do NOT ask the user to re-run TOE. "
                "Summarize the effectiveness counts above and mention the Required Documents "
                "Excel if document_list is present. Then ask what to do next."
            ),
        }
        logger.info(
            "SLIM TOE: full_size=%d chars → slim_size=%d chars",
            len(json.dumps(full_data, default=str)),
            len(json.dumps(slim, default=str)),
        )
        return slim

    if tool_name == "run_sampling_engine":
        slim = {
            "engine_type": full_data.get("engine_type"),
            "controls_processed": full_data.get("controls_processed"),
            "total_samples_required": full_data.get("total_samples_required"),
            "sampling_table_used": full_data.get("sampling_table_used"),
            # Omit per_control_results (large list) and unmatched_frequencies
            "_agent_notes": full_data.get("_agent_notes", []),
            "_slim": True,
            "_note": (
                "Full per-control sampling results are cached and displayed in the frontend "
                "data pane automatically. Do NOT re-run sampling. "
                "Summarize total samples required and ask what to do next."
            ),
        }
        # Include unmatched count (not the full list) so LLM can mention it
        unmatched = full_data.get("unmatched_frequencies")
        if isinstance(unmatched, list) and unmatched:
            slim["unmatched_frequency_count"] = len(unmatched)
        logger.info(
            "SLIM SAMPLING: full_size=%d chars → slim_size=%d chars",
            len(json.dumps(full_data, default=str)),
            len(json.dumps(slim, default=str)),
        )
        return slim

    if tool_name == "run_sox_scoping_engine":
        status = full_data.get("status", "")
        slim: Dict[str, Any] = {"_slim": True, "status": status}

        # Always forward small scalar keys the LLM needs for next-step decisions
        _SCALAR_KEYS = {
            "next_action", "output_excel", "output_path", "phase_steps",
            "benchmark_options", "benchmark_reference_values",
            "awaiting_input", "message", "blocked",
            "accounts_ingested", "benchmark", "materiality_pct",
            "accounts_excel", "results_excel", "quantitative_excel",
            "qualitative_excel", "scoping_excel",
            "in_scope_count", "out_of_scope_count", "out_scope_count",
            "sop_paths_used", "sop_paths_invalid",
            "process_count", "fs_accounts_total",
            "in_scope_accounts", "out_of_scope_accounts",
            "override_applied",
        }
        for k in _SCALAR_KEYS:
            if k in full_data:
                val = full_data[k]
                # Only forward scalars / small values — skip large lists
                if isinstance(val, list) and len(val) > 15:
                    slim[k] = len(val)  # forward count instead of full list
                    logger.debug("SLIM SCOPING: replaced list key %s (%d items) with count", k, len(val))
                else:
                    slim[k] = val

        # Forward _agent_notes
        if "_agent_notes" in full_data:
            slim["_agent_notes"] = full_data["_agent_notes"]

        # ── Phase-specific slim data ──
        if status == "accounts_fetched":
            # Include first 5 accounts as a sample so the LLM can mention them
            accounts = full_data.get("accounts", [])
            if accounts:
                slim["accounts_sample"] = accounts[:5]
                slim["total_accounts"] = len(accounts)

        elif status == "quantitative_done":
            qa = full_data.get("quantitative_analysis")
            if isinstance(qa, dict):
                slim["quantitative_summary"] = {
                    "materiality_threshold": qa.get("materiality_threshold"),
                    "in_count": qa.get("in_count"),
                    "out_count": qa.get("out_count"),
                }

        elif status == "qualitative_done":
            # Carry forward quant summary
            qa = full_data.get("quantitative_analysis")
            if isinstance(qa, dict):
                slim["quantitative_summary"] = {
                    "materiality_threshold": qa.get("materiality_threshold"),
                    "in_count": qa.get("in_count"),
                    "out_count": qa.get("out_count"),
                }
            qual = full_data.get("qualitative_analysis")
            if isinstance(qual, dict):
                slim["qualitative_summary"] = {
                    "high_risk_count": qual.get("high_risk_count"),
                    "moderate_risk_count": qual.get("moderate_risk_count"),
                    "low_risk_count": qual.get("low_risk_count"),
                    "additions_count": qual.get("additions_count"),
                    "removals_count": qual.get("removals_count"),
                }

        elif status == "scoped_done":
            # Include the pre-formatted markdown table so the LLM can display
            # it directly (the next_action instructs it to use this field).
            md_table = full_data.get("in_scope_accounts_table")
            if md_table and isinstance(md_table, str):
                # Truncate to first 30 rows if very large to save tokens
                lines = md_table.split("\n")
                if len(lines) > 32:  # 2 header lines + 30 data rows
                    slim["in_scope_accounts_table"] = "\n".join(lines[:32]) + \
                        f"\n| ... | ({full_data.get('in_scope_count', '?')} total — see full Excel) | | | |"
                else:
                    slim["in_scope_accounts_table"] = md_table
            # Also include sample for fallback
            inscope = full_data.get("in_scope_accounts", [])
            if isinstance(inscope, list) and inscope:
                slim["in_scope_sample"] = [
                    {"name": a.get("name"), "rule_description": a.get("rule_description")}
                    for a in inscope[:10]
                ]
                slim["in_scope_count"] = len(inscope)
            slim["out_of_scope_count"] = full_data.get("out_of_scope_count")
            slim["llm_override_count"] = full_data.get("llm_override_count")
            slim["llm_overrides"] = full_data.get("llm_overrides")

        elif status in ("success", "complete"):
            # RCM generated — omit full workbook data (biggest token saver)
            slim["rcm_generated"] = True
            rcm_wb = full_data.get("rcm_workbook")
            if isinstance(rcm_wb, dict):
                slim["rcm_sheets"] = rcm_wb.get("sheets", [])
                slim["rcm_total_rows"] = sum(
                    len(v) for v in (rcm_wb.get("data") or {}).values()
                    if isinstance(v, list)
                )

        slim["_note"] = (
            "Full results are cached and displayed in the frontend data pane automatically. "
            "Present the summary to the user and follow the next_action instructions."
        )
        logger.info(
            "SLIM SCOPING [%s]: full_size=%d chars → slim_size=%d chars",
            status,
            len(json.dumps(full_data, default=str)),
            len(json.dumps(slim, default=str)),
        )
        return slim

    if tool_name == "index_handbook":
        slim = {
            "document_name": full_data.get("document_name"),
            "total_pages": full_data.get("total_pages"),
            "total_chunks": full_data.get("total_chunks"),
            "already_indexed": full_data.get("already_indexed"),
            "duration_seconds": full_data.get("duration_seconds"),
            "_slim": True,
            "_note": "Handbook indexed successfully. You can now answer questions from it using ask_handbook.",
        }
        return slim

    if tool_name == "ask_handbook":
        # Keep answer but trim sources to save tokens
        sources = full_data.get("sources", [])
        slim_sources = sources[:3] if len(sources) > 3 else sources
        slim = {
            "answer": full_data.get("answer"),
            "sources": slim_sources,
            "chunks_retrieved": full_data.get("chunks_retrieved"),
            "document_name": full_data.get("document_name"),
            "duration_seconds": full_data.get("duration_seconds"),
            "_slim": True,
        }
        return slim

    # Fallback: return as-is (shouldn't reach here for _SLIM_RETURN_TOOLS)
    return full_data


def _map_type(type_str: str):
    """Map JSON schema type strings to Python types for Pydantic."""
    mapping = {
        "string": str,
        "integer": int,
        "number": float,
        "boolean": bool,
        "array": list,
        "object": dict,
    }
    return mapping.get(type_str, str)


_tool_registry: Dict[str, Any] = {}


def adapt_tools() -> List[StructuredTool]:
    """
    Discover all agent tools and wrap them as LangChain StructuredTools.

    Each tool's execute() is called with args and the AgentState from
    config["configurable"]["runtime_state"].
    """
    global _tool_registry
    registry = discover_tools()
    _tool_registry = registry
    lc_tools = []

    for name, tool in registry.items():
        # Build Pydantic args model dynamically from tool.parameters
        fields: Dict[str, Any] = {}
        for param in tool.parameters:
            field_type = _map_type(param.type)
            desc = param.description or name

            if param.required:
                if param.enum:
                    # For enum params, still use the base type
                    fields[param.name] = (
                        field_type,
                        Field(description=desc + f" (options: {', '.join(param.enum)})"),
                    )
                else:
                    fields[param.name] = (field_type, Field(description=desc))
            else:
                default = param.default
                if param.enum:
                    fields[param.name] = (
                        Optional[field_type],
                        Field(default=default, description=desc + f" (options: {', '.join(param.enum)})"),
                    )
                else:
                    fields[param.name] = (
                        Optional[field_type],
                        Field(default=default, description=desc),
                    )

        # Create the Pydantic model
        if fields:
            ArgsModel = create_model(f"{name}_Args", **fields)
        else:
            ArgsModel = create_model(f"{name}_Args")

        # Create the wrapper function (closure captures 'tool')
        def _make_wrapper(t):
            def wrapper(config: RunnableConfig = None, **kwargs) -> str:
                # Get runtime state from config
                runtime_state = None
                if config and "configurable" in config:
                    runtime_state = config["configurable"].get("runtime_state")
                if runtime_state is None:
                    runtime_state = AgentState()

                # Tool call count safety limit
                server_cfg = get_server_config()
                if runtime_state.tool_call_count >= server_cfg.max_tool_calls:
                    logger.warning(
                        "Tool call limit reached (%d/%d) — rejecting %s",
                        runtime_state.tool_call_count, server_cfg.max_tool_calls, t.name,
                    )
                    return json.dumps({
                        "error": f"Tool call limit reached ({server_cfg.max_tool_calls}). "
                                 "Please summarize your progress and ask the user for next steps."
                    }, default=str)

                # Precondition check
                precond = t.preconditions(runtime_state)
                if precond:
                    return json.dumps({"error": f"Precondition: {precond}"}, default=str)

                # ── Guard: redirect read_file on Excel to load_rcm ──
                # The LLM sometimes picks read_file for RCM Excel files,
                # which skips header detection and column mapping.
                # Automatically redirect to load_rcm for proper parsing.
                if t.name == "read_file":
                    file_path = kwargs.get("file_path", "")
                    if isinstance(file_path, str) and file_path.lower().endswith((".xlsx", ".xls")):
                        logger.info(
                            "GUARD: redirecting read_file(%s) → load_rcm for proper RCM parsing",
                            file_path,
                        )
                        load_rcm_tool = _tool_registry.get("load_rcm")
                        if load_rcm_tool is not None:
                            try:
                                result: ToolResult = load_rcm_tool.execute(
                                    {"file_path": file_path}, runtime_state,
                                )
                                result_data = sanitize_for_json(result.data)
                                result_data["_redirected"] = (
                                    "Automatically used load_rcm instead of read_file "
                                    "for proper header detection and column mapping."
                                )
                                runtime_state.tool_call_count += 1
                                runtime_state.tool_results_cache["load_rcm"] = json.dumps(
                                    result_data, default=str,
                                )
                                return json.dumps(result_data, default=str)
                            except Exception as exc:
                                logger.warning("load_rcm redirect failed: %s — falling back to read_file", exc)

                # Execute
                try:
                    result: ToolResult = t.execute(kwargs, runtime_state)
                except Exception as exc:
                    logger.exception("Tool %s failed", t.name)
                    return json.dumps({"error": f"Tool execution failed: {exc}"}, default=str)

                if result.success:
                    result_data = sanitize_for_json(result.data)
                else:
                    # Preserve structured payloads from tools even on failure.
                    # Some tools intentionally return guidance in result.data
                    # (e.g., awaiting_input step-gates) alongside an error.
                    result_data = sanitize_for_json(result.data or {})
                    result_data["error"] = result.error or "Unknown error"

                # Reflector notes
                notes = _reflector.analyze(t.name, result_data, runtime_state)
                if notes:
                    result_data["_agent_notes"] = notes

                runtime_state.tool_call_count += 1

                # Cache serialized result for session-results endpoint (page-refresh restore)
                _CACHEABLE_TOOLS = {
                    "load_rcm", "read_file",
                    "run_ai_suggestions",
                    "run_sox_scoping_engine",
                    "run_test_of_design",
                    "run_test_of_effectiveness",
                    "run_sampling_engine",
                    "run_quality_comparison",
                    "run_deduplication",
                    "run_control_assessment",
                }
                if t.name in _CACHEABLE_TOOLS and result.success:
                    serialized = json.dumps(result_data, default=str)
                    logger.info(
                        "CACHE %s: storing full result (%d chars) in tool_results_cache for frontend",
                        t.name, len(serialized),
                    )
                    # For the scoping engine, cache each phase under a phase-specific key
                    # so page-reload can reconstruct the full picture by merging all phases.
                    if t.name == "run_sox_scoping_engine":
                        phase_status = result_data.get("status", "")
                        if phase_status:
                            cache_key = f"run_sox_scoping_engine__{phase_status}"
                            runtime_state.tool_results_cache[cache_key] = serialized
                            logger.info("CACHE scoping phase key: %s", cache_key)
                            # Also persist FULL result to Cosmos for durability
                            # (SSE handler only sees slim data; this ensures
                            # session-results can serve full data after restart).
                            _thread_id = (
                                config.get("configurable", {}).get("thread_id")
                                if config else None
                            )
                            if _thread_id:
                                try:
                                    from .cosmos_store import get_cosmos_store
                                    _cosmos = get_cosmos_store()
                                    _cosmos.save_tool_result(
                                        _thread_id, cache_key, serialized,
                                    )
                                    _cosmos.save_tool_result(
                                        _thread_id, t.name, serialized,
                                    )
                                except Exception as _exc:
                                    logger.debug(
                                        "Cosmos full-result persist (non-fatal): %s", _exc,
                                    )
                    # For slim-return tools (TOD, TOE, sampling, etc.), persist
                    # FULL result to Cosmos so session-results can serve full data
                    # after a Flask restart (the SSE handler only sees slim data).
                    if t.name in _SLIM_RETURN_TOOLS and t.name != "run_sox_scoping_engine":
                        _thread_id = (
                            config.get("configurable", {}).get("thread_id")
                            if config else None
                        )
                        if _thread_id:
                            try:
                                from .cosmos_store import get_cosmos_store
                                _cosmos = get_cosmos_store()
                                _cosmos.save_tool_result(
                                    _thread_id, t.name, serialized,
                                )
                                logger.info(
                                    "Cosmos full-result persist for %s (%d chars)",
                                    t.name, len(serialized),
                                )
                            except Exception as _exc:
                                logger.debug(
                                    "Cosmos full-result persist (non-fatal) for %s: %s",
                                    t.name, _exc,
                                )
                    # Always keep the last-phase key as well for backward compatibility
                    runtime_state.tool_results_cache[t.name] = serialized

                # Track artifacts
                if result.artifacts:
                    runtime_state.artifacts.extend(result.artifacts)

                # ── Slim return for token-heavy tools ──
                # The full result is already cached above for the frontend
                # (session-results endpoint).  Return only summary-level data
                # to the LLM so it doesn't consume excessive tokens trying to
                # process hundreds of control rows / sample details.
                if t.name in _SLIM_RETURN_TOOLS and result.success:
                    slim = _make_slim_result(t.name, result_data)
                    logger.info(
                        "Returning slim result to LLM for %s (full data cached for frontend)",
                        t.name,
                    )
                    return json.dumps(slim, default=str)

                return json.dumps(result_data, default=str)

            return wrapper

        lc_tool = StructuredTool(
            name=name,
            description=tool.description,
            func=_make_wrapper(tool),
            args_schema=ArgsModel,
        )
        lc_tools.append(lc_tool)
        logger.info("Adapted tool: %s (%d params)", name, len(tool.parameters))

    logger.info("Total adapted tools: %d", len(lc_tools))
    return lc_tools
