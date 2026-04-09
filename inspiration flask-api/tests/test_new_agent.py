"""
Comprehensive test & dry-run of the autonomous SOX audit agent.

Tests:
    1. All imports resolve
    2. All expected tools are discovered and registered
  3. OpenAI function-calling schemas are valid
  4. AgentState, Memory, Executor, Reflector, AgentLoop wire correctly
  5. System prompt builds dynamically from state
  6. Tool preconditions work (guard before execution)
  7. Simulated tool executions (load_rcm, inspect, modify, etc.)
  8. Reflector anomaly detection fires correctly
  9. Memory auto-extracts semantic facts
  10. Plan scratchpad round-trip (update_plan → shows in context)
  11. End-to-end factory wiring via create_agent()
"""

from __future__ import annotations

import io
import json
import os
import sys
import time
import traceback
from datetime import datetime

# ── Capture all output ────────────────────────────────────────────────────────

LOG_LINES: list[str] = []


def log(msg: str = "") -> None:
    LOG_LINES.append(msg)
    print(msg)


def section(title: str) -> None:
    log(f"\n{'='*80}")
    log(f"  {title}")
    log(f"{'='*80}\n")


def subsection(title: str) -> None:
    log(f"\n--- {title} {'─'*(70 - len(title))}\n")


def pass_check(name: str) -> None:
    log(f"  [PASS] {name}")


def fail_check(name: str, error: str) -> None:
    log(f"  [FAIL] {name}: {error}")


# ══════════════════════════════════════════════════════════════════════════════
# BEGIN TESTS
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    start = time.time()

    section("AUTONOMOUS SOX AUDIT AGENT — COMPREHENSIVE TEST & DRY RUN")
    log(f"Timestamp : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log(f"Python    : {sys.version}")
    log(f"Working dir: {os.getcwd()}")

    passed = 0
    failed = 0
    total = 0

    def check(name: str, fn):
        nonlocal passed, failed, total
        total += 1
        try:
            fn()
            pass_check(name)
            passed += 1
        except Exception as e:
            fail_check(name, str(e))
            log(f"           {traceback.format_exc().splitlines()[-1]}")
            failed += 1

    # ══════════════════════════════════════════════════════════════════════
    # 1. IMPORT TESTS
    # ══════════════════════════════════════════════════════════════════════

    section("1. IMPORT VERIFICATION")

    from agent.types import AgentState, ToolResult, ToolParameter, ToolCategory
    check("import agent.types", lambda: None)

    from agent.config import AgentConfig, get_config
    check("import agent.config", lambda: None)

    from agent.utils import (
        sanitize_for_json, normalize_rcm_columns, parse_indices,
        display_table, COLUMN_NORMALIZE_MAP, RCM_REQUIRED_COLUMNS,
        SUPPORTED_INDUSTRIES,
    )
    check("import agent.utils", lambda: None)

    from agent.llm import LLMClient
    check("import agent.llm", lambda: None)

    from agent.tools.base import Tool
    check("import agent.tools.base", lambda: None)

    from agent.tools import discover_tools
    check("import agent.tools (discover)", lambda: None)

    from agent.core.memory import Memory
    check("import agent.core.memory", lambda: None)

    from agent.core.executor import Executor
    check("import agent.core.executor", lambda: None)

    from agent.core.reflector import Reflector
    check("import agent.core.reflector", lambda: None)

    from agent.prompts.system import build_system_prompt
    check("import agent.prompts.system", lambda: None)

    from agent.core.loop import AgentLoop, DefaultCallbacks
    check("import agent.core.loop", lambda: None)

    from agent import create_agent
    check("import agent (factory)", lambda: None)

    # ══════════════════════════════════════════════════════════════════════
    # 2. TOOL DISCOVERY
    # ══════════════════════════════════════════════════════════════════════

    section("2. TOOL DISCOVERY & REGISTRY")

    tools = discover_tools()
    log(f"  Discovered {len(tools)} tools:\n")

    expected_tools = [
        "load_rcm", "inspect_dataframe", "modify_rcm", "merge_suggestions",
        "remove_duplicates", "run_ai_suggestions", "run_control_assessment",
        "run_deduplication", "run_test_of_design", "run_test_of_effectiveness",
        "preview_toe_attributes", "run_sampling_engine", "run_sox_scoping_engine",
        "list_directory", "read_file", "save_excel", "update_plan",
        "execute_python", "web_search",
    ]

    for name in sorted(tools):
        tool = tools[name]
        cat = tool.category.value
        params = [p.name for p in tool.parameters]
        log(f"    {name:35s} [{cat:10s}]  params={params}")

    check(f"All {len(expected_tools)} tools discovered", lambda: (
        assert_(len(tools) == len(expected_tools),
                f"Expected {len(expected_tools)}, got {len(tools)}")
    ))

    for t in expected_tools:
        check(f"Tool '{t}' registered", lambda t=t: (
            assert_(t in tools, f"'{t}' not found in registry")
        ))

    # ══════════════════════════════════════════════════════════════════════
    # 3. OPENAI FUNCTION-CALLING SCHEMAS
    # ══════════════════════════════════════════════════════════════════════

    section("3. OPENAI FUNCTION-CALLING SCHEMAS")

    all_schemas = []
    for name, tool in sorted(tools.items()):
        schema = tool.to_openai_schema()
        all_schemas.append(schema)

        check(f"Schema '{name}' has type=function", lambda s=schema: (
            assert_(s["type"] == "function", f"type={s['type']}")
        ))
        check(f"Schema '{name}' has function.name", lambda s=schema, n=name: (
            assert_(s["function"]["name"] == n,
                    f"name={s['function']['name']}")
        ))
        check(f"Schema '{name}' has parameters.type=object", lambda s=schema: (
            assert_(s["function"]["parameters"]["type"] == "object", "no object type")
        ))

    log(f"\n  Total schemas generated: {len(all_schemas)}")
    log(f"  All schemas JSON-serialisable: ", )
    try:
        json.dumps(all_schemas)
        log("  YES")
        passed += 1
        total += 1
    except Exception as e:
        log(f"  NO — {e}")
        failed += 1
        total += 1

    # ══════════════════════════════════════════════════════════════════════
    # 4. TYPES & STATE
    # ══════════════════════════════════════════════════════════════════════

    section("4. TYPES & STATE")

    state = AgentState()
    check("AgentState() creates with defaults", lambda: (
        assert_(state.rcm_df is None, "rcm_df not None"),
        assert_(state.output_dir is None, "output_dir not None"),
        assert_(state.tool_call_count == 0, "tool_call_count not 0"),
        assert_(state.plan_scratchpad is None, "plan_scratchpad not None"),
    ))

    check("AgentState.reset() clears all fields", lambda: _test_reset(state))

    tr = ToolResult(success=True, data={"foo": 1}, summary="Test")
    check("ToolResult construction", lambda: (
        assert_(tr.success is True, "not success"),
        assert_(tr.data == {"foo": 1}, "data mismatch"),
        assert_(tr.summary == "Test", "summary mismatch"),
    ))

    tp = ToolParameter("name", "string", "desc", required=True, enum=["a", "b"])
    check("ToolParameter with enum", lambda: (
        assert_(tp.enum == ["a", "b"], "enum mismatch"),
    ))

    for cat in ToolCategory:
        check(f"ToolCategory.{cat.name} exists", lambda: None)

    # ══════════════════════════════════════════════════════════════════════
    # 5. CONFIG
    # ══════════════════════════════════════════════════════════════════════

    section("5. CONFIG")

    cfg = get_config()
    log(f"  openai_model     = {cfg.openai_model}")
    log(f"  max_rounds       = {cfg.max_rounds}")
    log(f"  max_retries      = {cfg.max_retries}")
    log(f"  retry_delay      = {cfg.retry_delay}")
    log(f"  max_context_msgs = {cfg.max_context_messages}")
    log(f"  api_key          = {cfg.openai_api_key[:12]}...{cfg.openai_api_key[-8:]}")

    check("Config is frozen (immutable)", lambda: _test_frozen_config(cfg))
    check("Config singleton returns same object", lambda: (
        assert_(get_config() is cfg, "not same object")
    ))

    # ══════════════════════════════════════════════════════════════════════
    # 6. UTILS
    # ══════════════════════════════════════════════════════════════════════

    section("6. UTILITY FUNCTIONS")

    import pandas as pd
    import numpy as np

    # sanitize_for_json
    check("sanitize_for_json: NaN → None", lambda: (
        assert_(sanitize_for_json(float("nan")) is None, "NaN not None")
    ))
    check("sanitize_for_json: np.int64 → int", lambda: (
        assert_(isinstance(sanitize_for_json(np.int64(42)), int), "not int")
    ))
    check("sanitize_for_json: nested dict/list", lambda: (
        assert_(
            sanitize_for_json({"a": [np.float64("nan"), np.int32(7)]})
            == {"a": [None, 7]},
            "nested sanitize failed"
        )
    ))

    # normalize_rcm_columns
    df_test = pd.DataFrame({"risk id": [1], "control_id": [2], "Process": [3]})
    df_norm = normalize_rcm_columns(df_test.copy())
    check("normalize_rcm_columns: 'risk id' → 'Risk Id'", lambda: (
        assert_("Risk Id" in df_norm.columns, f"columns={list(df_norm.columns)}")
    ))
    check("normalize_rcm_columns: 'control_id' → 'Control Id'", lambda: (
        assert_("Control Id" in df_norm.columns, f"columns={list(df_norm.columns)}")
    ))
    check("normalize_rcm_columns: 'Process' unchanged", lambda: (
        assert_("Process" in df_norm.columns, f"columns={list(df_norm.columns)}")
    ))

    # parse_indices
    check("parse_indices: '1,3,5' → [1,3,5]", lambda: (
        assert_(parse_indices("1,3,5", 10) == [1, 3, 5], "mismatch")
    ))
    check("parse_indices: '1-5' → [1,2,3,4,5]", lambda: (
        assert_(parse_indices("1-5", 10) == [1, 2, 3, 4, 5], "mismatch")
    ))
    check("parse_indices: 'all' with max=3 → [1,2,3]", lambda: (
        assert_(parse_indices("all", 3) == [1, 2, 3], "mismatch")
    ))
    check("parse_indices: 'none' → []", lambda: (
        assert_(parse_indices("none", 10) == [], "mismatch")
    ))

    # display_table
    rows = [{"a": "hello", "b": 42}, {"a": "world", "b": 99}]
    table = display_table(rows, ["a", "b"])
    check("display_table: produces ASCII table", lambda: (
        assert_("hello" in table and "world" in table, "missing data")
    ))

    # ══════════════════════════════════════════════════════════════════════
    # 7. MEMORY
    # ══════════════════════════════════════════════════════════════════════

    section("7. MEMORY SYSTEM")

    mem = Memory()

    mem.add_user_message("Load my RCM")
    check("Memory: user message added", lambda: (
        assert_(len(mem._episodic) == 1, f"len={len(mem._episodic)}")
    ))

    mem.add_assistant_message("I'll load it now.", tool_calls=None)
    check("Memory: assistant message added", lambda: (
        assert_(len(mem._episodic) == 2, f"len={len(mem._episodic)}")
    ))

    # Simulate tool result with auto-fact extraction
    mem.add_tool_result("call_001", "load_rcm", {
        "success": True, "rows": 28, "columns": ["Process", "Control Id", "Risk Id"],
    })
    check("Memory: tool result added", lambda: (
        assert_(len(mem._episodic) == 3, f"len={len(mem._episodic)}")
    ))
    check("Memory: auto-extracted fact for load_rcm", lambda: (
        assert_(any("RCM loaded" in f for f in mem._facts),
                f"facts={mem._facts}")
    ))

    # Add more facts
    mem.add_tool_result("call_002", "run_ai_suggestions", {
        "success": True, "suggestion_count": 12,
    })
    check("Memory: auto-extracted fact for ai_suggestions", lambda: (
        assert_(any("AI suggestions" in f for f in mem._facts),
                f"facts={mem._facts}")
    ))

    mem.add_tool_result("call_003", "run_test_of_design", {
        "success": True, "controls_evaluated": 18, "passed": 16, "failed": 2,
    })
    check("Memory: auto-extracted fact for TOD", lambda: (
        assert_(any("TOD" in f for f in mem._facts), f"facts={mem._facts}")
    ))

    log(f"\n  Semantic facts ({len(mem._facts)}):")
    for f in mem._facts:
        log(f"    • {f}")

    # Context building
    context = mem.build_context(plan_scratchpad="## My Plan\n- [x] Load RCM\n- [ ] Run AI")
    check("Memory: build_context includes facts", lambda: (
        assert_(any("SESSION FACTS" in m.get("content", "") for m in context),
                "no facts in context")
    ))
    check("Memory: build_context includes plan", lambda: (
        assert_(any("WORKING PLAN" in m.get("content", "") for m in context),
                "no plan in context")
    ))
    log(f"\n  Context messages: {len(context)}")
    for m in context:
        role = m.get("role", "?")
        content = str(m.get("content", ""))[:80]
        log(f"    [{role:10s}] {content}...")

    # ══════════════════════════════════════════════════════════════════════
    # 8. REFLECTOR (Advisory notes, NOT flow control)
    # ══════════════════════════════════════════════════════════════════════

    section("8. REFLECTOR — ADVISORY NOTES")

    reflector = Reflector()
    state_for_reflect = AgentState()

    # Simulate: TOD with 100% pass rate
    subsection("TOD: 100% pass rate anomaly")
    tod_result = {"controls_evaluated": 15, "passed": 15, "failed": 0}
    notes = reflector.analyze("run_test_of_design", tod_result, state_for_reflect)
    log(f"  Result: {json.dumps(tod_result)}")
    log(f"  Notes:  {notes}")
    check("Reflector: catches 100% TOD pass rate", lambda: (
        assert_(len(notes) > 0, "no notes"),
        assert_("ANOMALY" in notes[0], f"note={notes[0]}")
    ))

    # Simulate: TOE all effective
    subsection("TOE: all effective anomaly")
    toe_result = {"controls_evaluated": 8, "effective": 8,
                  "effective_with_exceptions": 0, "not_effective": 0}
    notes2 = reflector.analyze("run_test_of_effectiveness", toe_result, state_for_reflect)
    log(f"  Result: {json.dumps(toe_result)}")
    log(f"  Notes:  {notes2}")
    check("Reflector: catches all-effective TOE", lambda: (
        assert_(len(notes2) > 0, "no notes"),
        assert_("ANOMALY" in notes2[0], f"note={notes2[0]}")
    ))

    # Simulate: zero dedup pairs in large RCM
    subsection("Dedup: zero pairs in 50-row RCM")
    state_for_reflect.rcm_df = pd.DataFrame({"x": range(50)})
    dedup_result = {"pair_count": 0}
    notes3 = reflector.analyze("run_deduplication", dedup_result, state_for_reflect)
    log(f"  Result: {json.dumps(dedup_result)}")
    log(f"  Notes:  {notes3}")
    check("Reflector: flags zero dedup in large RCM", lambda: (
        assert_(len(notes3) > 0, "no notes"),
        assert_("Zero duplicates" in notes3[0], f"note={notes3[0]}")
    ))

    # Simulate: error in tool
    subsection("Tool error handling")
    err_result = {"error": "File not found: /bad/path.xlsx"}
    notes4 = reflector.analyze("load_rcm", err_result, state_for_reflect)
    log(f"  Result: {json.dumps(err_result)}")
    log(f"  Notes:  {notes4}")
    check("Reflector: advises on tool error", lambda: (
        assert_(len(notes4) > 0, "no notes"),
        assert_("TOOL ERROR" in notes4[0], f"note={notes4[0]}")
    ))

    # Normal result → no notes
    subsection("Normal result (no anomaly)")
    ok_result = {"controls_evaluated": 10, "passed": 8, "failed": 2}
    notes5 = reflector.analyze("run_test_of_design", ok_result, state_for_reflect)
    log(f"  Result: {json.dumps(ok_result)}")
    log(f"  Notes:  {notes5}")
    check("Reflector: no spurious notes for normal result", lambda: (
        assert_(len(notes5) == 0, f"unexpected notes: {notes5}")
    ))

    # ══════════════════════════════════════════════════════════════════════
    # 9. SYSTEM PROMPT
    # ══════════════════════════════════════════════════════════════════════

    section("9. DYNAMIC SYSTEM PROMPT")

    # Empty state
    state_empty = AgentState()
    prompt_empty = build_system_prompt(state_empty, "TOOLS: [test_tool]")
    log(f"  Empty state prompt ({len(prompt_empty)} chars):\n")
    log(prompt_empty[:600] + "\n  ...(truncated)")
    check("Prompt: contains 'not loaded yet' for empty state", lambda: (
        assert_("not loaded yet" in prompt_empty, "missing 'not loaded yet'")
    ))
    check("Prompt: contains 'No working plan'", lambda: (
        assert_("No working plan" in prompt_empty, "missing 'No working plan'")
    ))
    check("Prompt: does NOT hardcode step order", lambda: (
        assert_("Step 0" not in prompt_empty, "hardcoded Step 0"),
        assert_("Step 1" not in prompt_empty, "hardcoded Step 1"),
        assert_("STEP 0" not in prompt_empty, "hardcoded STEP 0"),
    ))

    # Loaded state with plan
    state_loaded = AgentState()
    state_loaded.rcm_df = pd.DataFrame({"Process": ["P2P", "O2C", "P2P"], "Risk Id": [1, 2, 3]})
    state_loaded.output_dir = "/tmp/sox_agent_test"
    state_loaded.tool_call_count = 7
    state_loaded.plan_scratchpad = "## Audit Plan\n- [x] Load RCM\n- [ ] Run dedup"
    state_loaded.suggestions_cache = [{"id": 1}, {"id": 2}]

    prompt_loaded = build_system_prompt(state_loaded, "TOOLS: [16 tools]")
    log(f"\n\n  Loaded state prompt ({len(prompt_loaded)} chars):\n")
    log(prompt_loaded[:800] + "\n  ...(truncated)")

    check("Prompt: shows row count", lambda: (
        assert_("3 rows" in prompt_loaded, "missing row count")
    ))
    check("Prompt: shows process count", lambda: (
        assert_("2 unique" in prompt_loaded, "missing process count")
    ))
    check("Prompt: shows plan scratchpad", lambda: (
        assert_("Audit Plan" in prompt_loaded, "missing plan")
    ))
    check("Prompt: shows suggestions cache", lambda: (
        assert_("suggestions cached" in prompt_loaded.lower() or
                "AI suggestions cached" in prompt_loaded, "missing suggestions")
    ))

    # ══════════════════════════════════════════════════════════════════════
    # 10. TOOL PRECONDITIONS
    # ══════════════════════════════════════════════════════════════════════

    section("10. TOOL PRECONDITIONS (Guards)")

    state_no_rcm = AgentState()

    tools_needing_rcm = [
        "inspect_dataframe", "modify_rcm", "merge_suggestions",
        "remove_duplicates", "run_ai_suggestions", "run_control_assessment",
        "run_deduplication", "run_test_of_design", "run_test_of_effectiveness",
        "save_excel",
    ]

    for tname in tools_needing_rcm:
        tool = tools[tname]
        err = tool.preconditions(state_no_rcm)
        check(f"Precondition '{tname}' blocks without RCM", lambda e=err: (
            assert_(e is not None, "precondition returned None (should block)")
        ))
        log(f"           → \"{err}\"")

    tools_no_precond = ["load_rcm", "list_directory", "read_file",
                        "execute_python", "web_search", "update_plan"]
    for tname in tools_no_precond:
        tool = tools[tname]
        err = tool.preconditions(state_no_rcm)
        check(f"No precondition for '{tname}'", lambda e=err: (
            assert_(e is None, f"unexpected precondition: {e}")
        ))

    # Merge needs suggestions_cache too
    state_with_rcm = AgentState()
    state_with_rcm.rcm_df = pd.DataFrame({"x": [1]})
    merge_err = tools["merge_suggestions"].preconditions(state_with_rcm)
    check("merge_suggestions blocks without suggestions_cache", lambda: (
        assert_(merge_err is not None, "should block")
    ))
    log(f"           → \"{merge_err}\"")

    # ══════════════════════════════════════════════════════════════════════
    # 11. SIMULATED TOOL EXECUTIONS
    # ══════════════════════════════════════════════════════════════════════

    section("11. SIMULATED TOOL EXECUTIONS")

    sim_state = AgentState()

    # --- update_plan ---
    subsection("update_plan")
    plan_result = tools["update_plan"].execute(
        {"plan_text": "## Plan\n- [ ] Load RCM\n- [ ] Run dedup"},
        sim_state,
    )
    log(f"  Result: success={plan_result.success}, summary='{plan_result.summary}'")
    log(f"  State.plan_scratchpad:\n{sim_state.plan_scratchpad}")
    check("update_plan sets scratchpad", lambda: (
        assert_(sim_state.plan_scratchpad is not None, "scratchpad is None"),
        assert_("Load RCM" in sim_state.plan_scratchpad, "content missing"),
    ))

    # --- list_directory ---
    subsection("list_directory")
    ld_result = tools["list_directory"].execute(
        {"path": os.getcwd(), "extension": ".py"},
        sim_state,
    )
    log(f"  Result: success={ld_result.success}")
    log(f"  Items found: {ld_result.data.get('count', 0)}")
    if ld_result.data.get("items"):
        for item in ld_result.data["items"][:5]:
            log(f"    {item['name']:40s} {item.get('size_kb', '?')} KB")
    check("list_directory lists .py files", lambda: (
        assert_(ld_result.success, f"error: {ld_result.error}"),
        assert_(ld_result.data["count"] > 0, "no files found"),
    ))

    # --- read_file ---
    subsection("read_file")
    rf_result = tools["read_file"].execute(
        {"file_path": os.path.join(os.getcwd(), "agent", "types.py")},
        sim_state,
    )
    log(f"  Result: success={rf_result.success}, type={rf_result.data.get('file_type')}")
    log(f"  Total lines: {rf_result.data.get('total_lines', '?')}")
    log(f"  Content preview: {rf_result.data.get('content', '')[:120]}...")
    check("read_file reads types.py", lambda: (
        assert_(rf_result.success, f"error: {rf_result.error}"),
        assert_("AgentState" in rf_result.data.get("content", ""), "content missing"),
    ))

    # --- execute_python ---
    subsection("execute_python")
    py_result = tools["execute_python"].execute(
        {"code": "import pandas as pd\nresult = {'answer': 42, 'series': pd.Series([1,2,3]).tolist()}"},
        sim_state,
    )
    log(f"  Result: success={py_result.success}")
    log(f"  result value: {py_result.data.get('result')}")
    log(f"  stdout: {py_result.data.get('stdout')}")
    check("execute_python runs code and returns result", lambda: (
        assert_(py_result.success, f"error: {py_result.error}"),
        assert_(py_result.data["result"]["answer"] == 42, "wrong answer"),
    ))

    # --- execute_python: df reassignment ---
    subsection("execute_python: DataFrame reassignment")
    sim_state.rcm_df = pd.DataFrame({"A": [1, 2, 3]})
    py_result2 = tools["execute_python"].execute(
        {"code": "df = df.copy()\ndf['B'] = [10, 20, 30]\nresult = df.shape"},
        sim_state,
    )
    log(f"  Result: success={py_result2.success}")
    log(f"  New df_shape: {py_result2.data.get('df_shape')}")
    check("execute_python updates state.rcm_df when df reassigned", lambda: (
        assert_(py_result2.success, f"error: {py_result2.error}"),
        assert_(list(sim_state.rcm_df.columns) == ["A", "B"], "columns wrong"),
    ))

    # --- modify_rcm: add_column ---
    subsection("modify_rcm: add_column")
    sim_state.rcm_df = pd.DataFrame({"Process": ["P2P", "O2C"], "Risk Id": [1, 2]})
    mod_result = tools["modify_rcm"].execute(
        {"action": "add_column", "column_name": "Status", "value": "Open"},
        sim_state,
    )
    log(f"  Result: success={mod_result.success}, summary='{mod_result.summary}'")
    log(f"  Columns: {list(sim_state.rcm_df.columns)}")
    check("modify_rcm: add_column works", lambda: (
        assert_(mod_result.success, f"error: {mod_result.error}"),
        assert_("Status" in sim_state.rcm_df.columns, "Status column missing"),
        assert_((sim_state.rcm_df["Status"] == "Open").all(), "values wrong"),
    ))

    # --- modify_rcm: rename_column ---
    subsection("modify_rcm: rename_column")
    ren_result = tools["modify_rcm"].execute(
        {"action": "rename_column", "column_name": "Status", "new_name": "Review_Status"},
        sim_state,
    )
    log(f"  Result: success={ren_result.success}, summary='{ren_result.summary}'")
    check("modify_rcm: rename_column works", lambda: (
        assert_(ren_result.success, f"error: {ren_result.error}"),
        assert_("Review_Status" in sim_state.rcm_df.columns, "rename failed"),
    ))

    # --- modify_rcm: update_values (conditional) ---
    subsection("modify_rcm: conditional update_values")
    upd_result = tools["modify_rcm"].execute(
        {"action": "update_values", "column_name": "Review_Status",
         "value": "Reviewed", "condition_column": "Process", "condition_value": "P2P"},
        sim_state,
    )
    log(f"  Result: success={upd_result.success}, rows_updated={upd_result.data.get('rows_updated')}")
    check("modify_rcm: conditional update works", lambda: (
        assert_(upd_result.success, f"error: {upd_result.error}"),
        assert_(upd_result.data["rows_updated"] == 1, "wrong count"),
    ))

    # ══════════════════════════════════════════════════════════════════════
    # 12. EXECUTOR (with precondition + retry)
    # ══════════════════════════════════════════════════════════════════════

    section("12. EXECUTOR")

    executor = Executor(tools)

    # Execute against empty state (should fail precondition)
    exec_result = executor.execute("inspect_dataframe", {"mode": "info"}, AgentState())
    log(f"  inspect_dataframe with no RCM: success={exec_result.success}, error='{exec_result.error}'")
    check("Executor: precondition failure", lambda: (
        assert_(not exec_result.success, "should fail"),
        assert_("Precondition" in exec_result.error, "wrong error"),
    ))

    # Execute unknown tool
    exec_result2 = executor.execute("nonexistent_tool", {}, sim_state)
    log(f"  nonexistent_tool: success={exec_result2.success}, error='{exec_result2.error}'")
    check("Executor: unknown tool", lambda: (
        assert_(not exec_result2.success, "should fail"),
        assert_("Unknown tool" in exec_result2.error, "wrong error"),
    ))

    # Execute valid tool
    sim_state.rcm_df = pd.DataFrame({"Process": ["P2P"], "Risk Id": ["R-001"]})
    exec_result3 = executor.execute("inspect_dataframe", {"mode": "info"}, sim_state)
    log(f"  inspect_dataframe (info): success={exec_result3.success}")
    log(f"    shape = {exec_result3.data.get('shape')}")
    log(f"    duration = {exec_result3.duration_seconds:.3f}s")
    check("Executor: valid tool execution", lambda: (
        assert_(exec_result3.success, f"error: {exec_result3.error}"),
        assert_(exec_result3.data["shape"]["rows"] == 1, "wrong row count"),
    ))

    # ══════════════════════════════════════════════════════════════════════
    # 13. FACTORY: create_agent()
    # ══════════════════════════════════════════════════════════════════════

    section("13. FACTORY: create_agent()")

    loop, fresh_state = create_agent()
    log(f"  AgentLoop created: {type(loop).__name__}")
    log(f"  Registry size: {len(loop._registry)} tools")
    log(f"  State type: {type(fresh_state).__name__}")
    log(f"  Memory type: {type(loop._memory).__name__}")
    log(f"  Executor type: {type(loop._executor).__name__}")
    log(f"  Reflector type: {type(loop._reflector).__name__}")
    log(f"  LLM type: {type(loop._llm).__name__}")

    check("Factory: creates AgentLoop", lambda: (
        assert_(isinstance(loop, AgentLoop), "wrong type")
    ))
    check("Factory: state is fresh", lambda: (
        assert_(fresh_state.rcm_df is None, "rcm not None"),
        assert_(fresh_state.tool_call_count == 0, "tool_call_count not 0"),
    ))
    check(f"Factory: registry has {len(expected_tools)} tools", lambda: (
        assert_(len(loop._registry) == len(expected_tools), f"got {len(loop._registry)}")
    ))

    # ══════════════════════════════════════════════════════════════════════
    # 14. CAPABILITY SUMMARY (what goes in system prompt)
    # ══════════════════════════════════════════════════════════════════════

    section("14. CAPABILITY SUMMARY (shown to LLM)")

    cap_summary = loop._capability_summary()
    log(cap_summary)
    check("Capability summary grouped by category", lambda: (
        assert_("[ANALYSIS]" in cap_summary, "missing ANALYSIS"),
        assert_("[DATA]" in cap_summary, "missing DATA"),
        assert_("[FILESYSTEM]" in cap_summary, "missing FILESYSTEM"),
        assert_("[UTILITY]" in cap_summary, "missing UTILITY"),
    ))

    # ══════════════════════════════════════════════════════════════════════
    # 15. COMPARISON: OLD vs NEW AGENT
    # ══════════════════════════════════════════════════════════════════════

    section("15. OLD (sox_agent.py) vs NEW (agent/) COMPARISON")

    comparisons = [
        ("Architecture",
         "Monolithic 1813-line file",
         "20 files across 4 packages (~3500 lines)"),
        ("Tool count",
         "10 tools (later 15)",
         "16 tools, auto-discovered"),
        ("Tool registration",
         "Manual TOOL_DEFINITIONS list (hardcoded JSON)",
         "Auto-discovery via pkgutil + schema auto-generated from Tool properties"),
        ("Workflow",
         "Hardcoded 6-step pipeline in system prompt",
         "LLM decides every turn — no step iteration"),
        ("Planning",
         "System prompt IS the plan (fixed sequence)",
         "Plan is a TOOL (update_plan) — LLM scratchpad, never enforced by code"),
        ("Reflection",
         "None",
         "Reflector injects advisory _agent_notes into tool results"),
        ("Memory",
         "Single flat conversation[] list",
         "3-tier: Episodic (windowed) + Semantic facts + Plan scratchpad"),
        ("Error handling",
         "try/except per tool, no retry",
         "Executor with configurable retry + exponential backoff"),
        ("State management",
         "Global dict: agent_state = {}",
         "Typed AgentState dataclass with .reset()"),
        ("Config",
         "Hardcoded API keys at module level",
         "Frozen AgentConfig dataclass + env var fallback"),
        ("Extensibility",
         "Edit TOOL_DEFINITIONS + add handler function",
         "Drop a .py file in tools/ — auto-discovered on restart"),
        ("Who decides next action?",
         "Python code (implicit pipeline + system prompt order)",
         "The LLM — every single turn"),
        ("Can skip/reorder steps?",
         "No",
         "Yes, LLM decides freely"),
        ("Can do something unplanned?",
         "No",
         "Yes, LLM has full autonomy"),
        ("System prompt",
         "Prescribes 'Step 0 → Step 1 → ... → Step 5'",
         "Describes capabilities + behavioral guidelines, NO step order"),
        ("Anomaly detection",
         "None — agent blindly accepts 100% pass rates",
         "Reflector flags: 100% TOD, all-effective TOE, 0 dedup pairs, low CA match"),
    ]

    log(f"  {'Aspect':35s} {'OLD (sox_agent.py)':45s} {'NEW (agent/)':45s}")
    log(f"  {'-'*35} {'-'*45} {'-'*45}")
    for aspect, old, new in comparisons:
        log(f"  {aspect:35s} {old:45s} {new:45s}")

    # ══════════════════════════════════════════════════════════════════════
    # 16. UNIQUE CAPABILITIES OF THE NEW AGENT
    # ══════════════════════════════════════════════════════════════════════

    section("16. UNIQUE CAPABILITIES OF THE NEW AUTONOMOUS AGENT")

    capabilities = [
        ("TRUE AUTONOMY",
         "The LLM decides what to do at every turn. No hardcoded workflow.\n"
         "  Ask 'just check for duplicates' → it loads RCM, runs dedup, shows results. DONE.\n"
         "  Ask 'full audit' → it creates a plan, works through it, asks for input as needed.\n"
         "  The same agent handles both. No mode switching. No pipeline enforcement."),

        ("DYNAMIC PLANNING (update_plan tool)",
         "The agent writes its own plan as a markdown scratchpad.\n"
         "  The plan is shown in every LLM context but never enforced by code.\n"
         "  Agent can follow it, revise it, or completely deviate.\n"
         "  Mid-course changes: 'actually skip the dedup' → agent updates plan and moves on."),

        ("ANOMALY DETECTION (Reflector)",
         "Advisory notes are injected into tool results via _agent_notes.\n"
         "  Catches: 100% TOD pass rates, zero TOE deviations, zero dedup pairs in large RCMs,\n"
         "           very few or too many AI suggestions, <50% control assessment match.\n"
         "  The LLM sees these notes and decides what to do — flag to user, re-examine, continue."),

        ("3-TIER MEMORY",
         "Episodic: Full conversation messages, windowed to last 40.\n"
         "  Semantic: Auto-extracted key facts that survive context truncation.\n"
         "    Example: 'RCM loaded: 28 rows, 15 columns' persists even after window slides.\n"
         "  Plan scratchpad: The agent's working plan, always visible in context."),

        ("TOOL AUTO-DISCOVERY",
         "Drop a .py file in agent/tools/ with a Tool subclass → it's registered on restart.\n"
         "  No manual JSON schema editing. No handler mapping. Schema auto-generated.\n"
         "  Example: add custom_analysis.py with class CustomTool(Tool) → available to LLM."),

        ("EXECUTE_PYTHON SANDBOX",
         "The LLM can write and execute arbitrary Python code.\n"
         "  Pre-loaded: df (current RCM), pd, np, os, json, math, datetime.\n"
         "  If the code reassigns 'df', the RCM is automatically updated.\n"
         "  Assign to 'result' to return structured data to the LLM."),

        ("WEB SEARCH",
         "DuckDuckGo Instant Answer API + LLM knowledge fallback.\n"
         "  The agent can look up SOX regulations, PCAOB standards, COSO framework on the fly.\n"
         "  No pre-loaded knowledge required — real-time information retrieval."),

        ("INSPECT_DATAFRAME (8 modes)",
         "info: shape, dtypes, nulls, memory usage\n"
         "  head/tail: first/last N rows\n"
         "  describe: full statistics\n"
         "  columns: all columns with dtype, non-null count, unique count\n"
         "  value_counts: unique values for any column\n"
         "  sample: random N rows\n"
         "  query: filter with pandas query syntax (e.g. 'Process == \"P2P\"')"),

        ("CONDITIONAL UPDATES (modify_rcm)",
         "update_values supports condition_column + condition_value.\n"
         "  Example: 'Set risk_level to Critical where Process is P2P'\n"
         "  Also: add_column, rename_column — full structural RCM editing."),

        ("TYPED, MODULAR ARCHITECTURE",
         "20 files across 4 packages. Every component is independently testable.\n"
         "  AgentState dataclass (not a global dict), frozen AgentConfig,\n"
         "  abstract Tool base class, typed ToolResult, ToolParameter.\n"
         "  Logging throughout. Clean error propagation."),

        ("RETRY WITH BACKOFF",
         "Executor retries transient errors (ConnectionError, TimeoutError)\n"
         "  with configurable max_retries and exponential backoff.\n"
         "  Permanent errors (ValueError, TypeError, KeyError, FileNotFoundError) fail fast."),

        ("COLORED TERMINAL REPL",
         "ANSI-colored output: cyan for agent speech, green for tool calls,\n"
         "  red for errors. /new to reset, /state to inspect session, /quit to exit.\n"
         "  Clean separation of UI (callbacks) from agent logic."),
    ]

    for i, (name, desc) in enumerate(capabilities, 1):
        log(f"\n  {i:2d}. {name}")
        log(f"      {desc}")

    # ══════════════════════════════════════════════════════════════════════
    # FINAL SUMMARY
    # ══════════════════════════════════════════════════════════════════════

    elapsed = time.time() - start
    section("FINAL SUMMARY")
    log(f"  Total checks: {total}")
    log(f"  Passed:       {passed}")
    log(f"  Failed:       {failed}")
    log(f"  Duration:     {elapsed:.2f}s")
    log(f"  Result:       {'ALL TESTS PASSED' if failed == 0 else f'{failed} TESTS FAILED'}")
    log("")


# ── Helpers ───────────────────────────────────────────────────────────────────

def assert_(condition: bool, msg: str = "") -> None:
    if not condition:
        raise AssertionError(msg)


def _test_reset(state: AgentState) -> None:
    state.tool_call_count = 99
    state.plan_scratchpad = "test"
    state.reset()
    assert_(state.tool_call_count == 0, "tool_call_count not reset")
    assert_(state.plan_scratchpad is None, "plan not reset")


def _test_frozen_config(cfg) -> None:
    try:
        cfg.openai_model = "different"
        raise AssertionError("Should be frozen")
    except AttributeError:
        pass  # Expected — frozen dataclass


if __name__ == "__main__":
    main()

    # Write results to file
    output_path = os.path.join(os.getcwd(), "agent_test_results.txt")
    with open(output_path, "w") as f:
        f.write("\n".join(LOG_LINES))
    print(f"\n  Results saved to: {output_path}")
