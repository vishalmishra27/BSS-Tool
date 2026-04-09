"""
Adaptive Control Frequency Inference tool.

When the RCM has controls with missing, empty, or unmappable frequency values,
this tool infers the likely control frequency from the control description using:
  1. A configurable keyword-to-frequency mapping table (rule-based, fast).
  2. LLM-based inference for descriptions that don't match any keyword (slower, smarter).

All inferred frequencies are mapped to the KPMG sampling engine's canonical terms:
  Annual, Quarterly, Monthly, Weekly, Daily, Recurring (multiple times per day).

The tool exports an editable Excel so the user can review/modify inferred values
before they are written to the RCM.  The user can also modify individual frequencies
via chat using the ``modify_control_frequency`` tool or upload a modified Excel
via ``upload_frequency_overrides``.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional

import pandas as pd

from .base import Tool
from ..types import AgentState, ToolCategory, ToolParameter, ToolResult
from ..utils import sanitize_for_json

logger = logging.getLogger("agent.tools.infer_control_frequency")

# ── Canonical KPMG frequency terms (the only values the sampling engine accepts) ──
KPMG_FREQUENCIES = [
    "Annual",
    "Quarterly (including period end, i.e. +1)",
    "Monthly",
    "Weekly",
    "Daily",
    "Recurring (multiple times per day)",
]

# ── Keyword-to-Frequency Mapping Table ──────────────────────────────────────
# Based on the configurable table discussed in the tool testing exercise.
# Each entry: (list_of_keywords, business_frequency, kpmg_mapped_frequency)
# Keywords are matched case-insensitively against the control description.

KEYWORD_FREQUENCY_MAP: List[Dict[str, Any]] = [
    {
        "keywords": ["policy approval", "policy review", "policy update",
                      "annual review", "annual assessment", "annual certification",
                      "annual audit", "board approval", "charter review"],
        "business_frequency": "Annual",
        "kpmg_frequency": "Annual",
    },
    {
        "keywords": ["user access review", "user recertification",
                      "access recertification", "quarterly review",
                      "quarterly assessment", "quarterly reconciliation",
                      "quarter-end", "quarter end", "period-end review",
                      "period end review"],
        "business_frequency": "Quarterly",
        "kpmg_frequency": "Quarterly (including period end, i.e. +1)",
    },
    {
        "keywords": ["monthly review", "monthly reconciliation",
                      "month-end", "month end", "monthly report",
                      "monthly assessment", "monthly closing",
                      "bank reconciliation"],
        "business_frequency": "Monthly",
        "kpmg_frequency": "Monthly",
    },
    {
        "keywords": ["weekly review", "weekly report", "weekly check",
                      "weekly reconciliation", "weekly meeting"],
        "business_frequency": "Weekly",
        "kpmg_frequency": "Weekly",
    },
    {
        "keywords": ["daily backup", "nightly job", "daily reconciliation",
                      "daily review", "daily check", "daily report",
                      "daily monitoring", "end of day", "end-of-day",
                      "daily batch", "daily log review"],
        "business_frequency": "Daily",
        "kpmg_frequency": "Daily",
    },
    {
        "keywords": ["change approval", "cab approval", "change request",
                      "change management", "change advisory board"],
        "business_frequency": "Ad-hoc / Event-driven",
        "kpmg_frequency": "Recurring (multiple times per day)",
    },
    {
        "keywords": ["incident response", "emergency change",
                      "incident management", "security incident",
                      "break-glass", "break glass", "emergency access"],
        "business_frequency": "Ad-hoc",
        "kpmg_frequency": "Recurring (multiple times per day)",
    },
    {
        "keywords": ["password expiry", "account lockout", "password policy",
                      "password rotation", "account disable",
                      "automated lockout", "system-enforced",
                      "auto-disable", "password complexity"],
        "business_frequency": "Continuous / System-enforced",
        "kpmg_frequency": "Recurring (multiple times per day)",
    },
    {
        "keywords": ["go-live approval", "uat sign-off", "uat signoff",
                      "go live approval", "project sign-off",
                      "deployment approval", "release approval",
                      "production release"],
        "business_frequency": "Ad-hoc / Project-based",
        "kpmg_frequency": "Recurring (multiple times per day)",
    },
    {
        "keywords": ["real-time", "real time", "continuous monitoring",
                      "automated alert", "system alert", "automated detection",
                      "automated monitoring", "automated validation",
                      "system validation", "automated check",
                      "per transaction", "each transaction",
                      "transaction level", "transaction-level"],
        "business_frequency": "Continuous / Real-time",
        "kpmg_frequency": "Recurring (multiple times per day)",
    },
    {
        "keywords": ["three-way match", "3-way match", "three way match",
                      "invoice matching", "purchase order matching",
                      "payment approval", "payment processing",
                      "invoice approval", "vendor payment",
                      "expense approval", "journal entry",
                      "journal approval"],
        "business_frequency": "Per transaction",
        "kpmg_frequency": "Recurring (multiple times per day)",
    },
    {
        "keywords": ["segregation of duties", "sod", "access provisioning",
                      "role assignment", "access request", "new hire",
                      "user provisioning", "role-based access",
                      "role based access"],
        "business_frequency": "Event-driven",
        "kpmg_frequency": "Recurring (multiple times per day)",
    },
]


def _match_keyword(description: str) -> Optional[Dict[str, str]]:
    """Try to match a control description against the keyword table.

    Returns {"business_frequency": ..., "kpmg_frequency": ...} or None.
    """
    desc_lower = description.lower()
    for entry in KEYWORD_FREQUENCY_MAP:
        for keyword in entry["keywords"]:
            if keyword in desc_lower:
                return {
                    "business_frequency": entry["business_frequency"],
                    "kpmg_frequency": entry["kpmg_frequency"],
                    "matched_keyword": keyword,
                }
    return None


def _llm_infer_frequencies(
    controls: List[Dict[str, str]],
) -> Dict[str, Dict[str, str]]:
    """Use the LLM to infer frequencies for controls that didn't match keywords.

    Args:
        controls: List of {"control_id": ..., "control_description": ...}

    Returns:
        {control_id: {"business_frequency": ..., "kpmg_frequency": ...}}
    """
    if not controls:
        return {}

    from engines.config import (
        AZURE_OPENAI_API_KEY, AZURE_OPENAI_ENDPOINT,
        AZURE_OPENAI_DEPLOYMENT, AZURE_OPENAI_API_VERSION,
    )
    from openai import AzureOpenAI

    client = AzureOpenAI(
        api_key=AZURE_OPENAI_API_KEY,
        azure_endpoint=AZURE_OPENAI_ENDPOINT,
        api_version=AZURE_OPENAI_API_VERSION,
    )

    # Build a batch prompt with all controls
    controls_text = "\n".join(
        f"- Control ID: {c['control_id']}\n  Description: {c['control_description']}"
        for c in controls
    )

    system_prompt = """You are an audit expert specializing in control frequency classification.

Your task: Given a control description, determine how often the control is performed.

You MUST map each control to exactly ONE of these KPMG sampling frequency categories:
1. "Annual" — performed once a year (e.g., annual policy reviews, yearly certifications)
2. "Quarterly (including period end, i.e. +1)" — performed every quarter (e.g., quarterly reviews, period-end reconciliations)
3. "Monthly" — performed every month (e.g., monthly reconciliations, month-end closes)
4. "Weekly" — performed every week (e.g., weekly reports, weekly checks)
5. "Daily" — performed every day (e.g., daily backups, daily log reviews, nightly jobs)
6. "Recurring (multiple times per day)" — performed multiple times per day or per-transaction, including:
   - Per-transaction controls (invoice matching, payment approvals, journal entries)
   - Event-driven controls (change approvals, incident response, access provisioning)
   - Continuous/system-enforced controls (password policies, automated alerts, real-time monitoring)
   - Ad-hoc controls (emergency changes, go-live approvals)

Also provide a short "business_frequency" label that describes the frequency in business terms
(e.g., "Per transaction", "Ad-hoc / Event-driven", "Continuous / System-enforced", "Annual").

Guidelines:
- If the description mentions specific timing (daily, weekly, monthly, quarterly, annual), use that.
- If the control is triggered by an event/transaction rather than a schedule, use "Recurring (multiple times per day)".
- If the control is automated/system-enforced, use "Recurring (multiple times per day)".
- If uncertain, use a conservative (higher frequency) estimate.
- Be concise in your reasoning.

Return your answer as a JSON array with one object per control:
[
  {
    "control_id": "<id>",
    "kpmg_frequency": "<one of the 6 categories above, EXACTLY as written>",
    "business_frequency": "<short business label>",
    "reasoning": "<one sentence explaining why>"
  }
]

Return ONLY valid JSON. No markdown, no code fences, no extra text."""

    user_prompt = f"Classify the frequency for each of these controls:\n\n{controls_text}"

    try:
        response = client.chat.completions.create(
            model=AZURE_OPENAI_DEPLOYMENT,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            max_completion_tokens=4096,
            response_format={"type": "json_object"},
        )

        raw = response.choices[0].message.content or "[]"
        # Handle both array and object responses
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            # LLM may wrap in {"results": [...]} or {"controls": [...]}
            for key in ("results", "controls", "data", "classifications"):
                if key in parsed and isinstance(parsed[key], list):
                    parsed = parsed[key]
                    break
            else:
                # Single control returned as object
                if "control_id" in parsed:
                    parsed = [parsed]
                else:
                    parsed = list(parsed.values())[0] if parsed else []

        result = {}
        for item in parsed:
            cid = str(item.get("control_id", "")).strip()
            kpmg = item.get("kpmg_frequency", "").strip()
            biz = item.get("business_frequency", "").strip()

            # Validate kpmg_frequency is one of the canonical terms
            if kpmg not in KPMG_FREQUENCIES:
                # Try fuzzy matching
                kpmg_lower = kpmg.lower()
                matched = False
                for canonical in KPMG_FREQUENCIES:
                    if canonical.lower().startswith(kpmg_lower[:8]):
                        kpmg = canonical
                        matched = True
                        break
                if not matched:
                    # Default to Recurring (conservative)
                    kpmg = "Recurring (multiple times per day)"

            if cid:
                result[cid] = {
                    "business_frequency": biz or kpmg,
                    "kpmg_frequency": kpmg,
                    "reasoning": item.get("reasoning", ""),
                }

        return result

    except Exception as e:
        logger.error("LLM frequency inference failed: %s", e)
        # Return empty — will be handled as unresolved
        return {}


def _upload_artifact(local_path: str, state: Optional[AgentState] = None) -> str:
    """Upload to blob storage if available, return blob path or local path."""
    try:
        from server.blob_store import get_blob_store
        store = get_blob_store()
        if not store.available:
            return local_path
        filename = os.path.basename(local_path)
        session_key = "default"
        if state and state.output_dir:
            session_key = os.path.basename(state.output_dir)
        blob_path = f"artifacts/{session_key}/{filename}"
        result = store.upload_file(local_path, blob_path)
        return result or local_path
    except Exception:
        return local_path


class InferControlFrequencyTool(Tool):
    """Infer missing control frequencies from control descriptions."""

    @property
    def name(self) -> str:
        return "infer_control_frequency"

    @property
    def description(self) -> str:
        return (
            "Infer missing or empty control frequency values from control descriptions. "
            "Uses a keyword-to-frequency mapping table first, then falls back to LLM-based "
            "inference. Maps all values to the KPMG sampling engine's canonical frequency terms. "
            "Exports an editable Excel for user review. The user can modify individual "
            "frequencies via chat (modify_control_frequency) or upload a modified Excel "
            "(upload_frequency_overrides) before approving. "
            "Call with apply=true to write approved frequencies to the RCM."
        )

    @property
    def category(self) -> ToolCategory:
        return ToolCategory.ANALYSIS

    @property
    def parameters(self) -> List[ToolParameter]:
        return [
            ToolParameter(
                name="apply",
                type="string",
                description=(
                    "Set to 'true' to write the pending inferred frequencies to the RCM. "
                    "Only use after the user has reviewed and approved the inferences. "
                    "Default: 'false' (infer and show for review)."
                ),
                required=False,
                default="false",
            ),
        ]

    def preconditions(self, state: AgentState) -> Optional[str]:
        if state.rcm_df is None:
            return "No RCM loaded. Use load_rcm first."
        return None

    def execute(self, args: Dict[str, Any], state: AgentState) -> ToolResult:
        apply_mode = str(args.get("apply", "false")).strip().lower() == "true"

        # ── APPLY MODE: Write approved inferences to the RCM ──────────
        if apply_mode:
            return self._apply_inferences(state)

        # ── INFER MODE: Detect missing frequencies and infer ──────────
        return self._infer_frequencies(state)

    def _infer_frequencies(self, state: AgentState) -> ToolResult:
        """Detect controls with missing/empty frequencies and infer them."""
        df = state.rcm_df

        # Find the frequency column
        freq_col = None
        for col in df.columns:
            if col.lower().replace("_", " ").strip() in (
                "control frequency", "controlfrequency", "frequency",
                "control_frequency",
            ):
                freq_col = col
                break

        # Find the control description column
        desc_col = None
        for col in df.columns:
            if col.lower().replace("_", " ").strip() in (
                "control description", "controldescription",
                "control_description",
            ):
                desc_col = col
                break

        if not desc_col:
            return ToolResult(
                success=False, data={},
                error="RCM does not have a 'Control Description' column — cannot infer frequencies.",
            )

        # Find the control ID column
        cid_col = None
        for col in df.columns:
            if col.lower().replace("_", " ").strip() in (
                "control id", "controlid", "control_id",
            ):
                cid_col = col
                break

        if not cid_col:
            return ToolResult(
                success=False, data={},
                error="RCM does not have a 'Control Id' column.",
            )

        # Identify controls with missing/empty frequency
        from .sampling_engine import FREQUENCY_ALIASES

        controls_needing_inference = []
        controls_with_frequency = []

        for idx, row in df.iterrows():
            control_id = str(row.get(cid_col, "")).strip()
            description = str(row.get(desc_col, "")).strip()
            raw_freq = str(row.get(freq_col, "")).strip() if freq_col else ""

            if not control_id or control_id.lower() in ("nan", "none", "null", ""):
                continue

            # Check if frequency is missing/empty/unmappable
            freq_lower = raw_freq.lower().strip()
            is_empty = (
                not raw_freq
                or freq_lower in ("nan", "none", "null", "n/a", "na", "", "-", "tbd",
                                   "to be determined", "not defined", "not specified")
            )

            # Also check if it's unmappable by the sampling engine
            is_unmappable = False
            if not is_empty and freq_lower not in FREQUENCY_ALIASES:
                # Check starts-with match
                matched = False
                for alias in sorted(FREQUENCY_ALIASES.keys(), key=lambda x: -len(x)):
                    if freq_lower.startswith(alias):
                        matched = True
                        break
                if not matched:
                    is_unmappable = True

            if is_empty or is_unmappable:
                controls_needing_inference.append({
                    "index": idx,
                    "control_id": control_id,
                    "control_description": description,
                    "current_frequency": raw_freq if not is_empty else "(empty)",
                })
            else:
                controls_with_frequency.append(control_id)

        if not controls_needing_inference:
            return ToolResult(
                success=True,
                data={
                    "message": "All controls already have valid frequency values.",
                    "total_controls": len(df),
                    "controls_with_frequency": len(controls_with_frequency),
                },
                summary="All controls already have valid frequency values — no inference needed.",
            )

        # ── Step 1: Keyword-based matching ────────────────────────────
        keyword_matched = {}
        llm_needed = []

        for ctrl in controls_needing_inference:
            match = _match_keyword(ctrl["control_description"])
            if match:
                keyword_matched[ctrl["control_id"]] = {
                    "control_id": ctrl["control_id"],
                    "control_description": ctrl["control_description"],
                    "current_frequency": ctrl["current_frequency"],
                    "inferred_frequency": match["kpmg_frequency"],
                    "business_frequency": match["business_frequency"],
                    "source": "Keyword",
                    "matched_keyword": match["matched_keyword"],
                    "confidence": "High",
                    "index": ctrl["index"],
                }
            else:
                llm_needed.append(ctrl)

        # ── Step 2: LLM-based inference for remaining ─────────────────
        llm_inferred = {}
        if llm_needed:
            llm_results = _llm_infer_frequencies(
                [{"control_id": c["control_id"],
                  "control_description": c["control_description"]}
                 for c in llm_needed]
            )

            for ctrl in llm_needed:
                cid = ctrl["control_id"]
                if cid in llm_results:
                    inf = llm_results[cid]
                    llm_inferred[cid] = {
                        "control_id": cid,
                        "control_description": ctrl["control_description"],
                        "current_frequency": ctrl["current_frequency"],
                        "inferred_frequency": inf["kpmg_frequency"],
                        "business_frequency": inf["business_frequency"],
                        "source": "LLM",
                        "reasoning": inf.get("reasoning", ""),
                        "confidence": "Medium",
                        "index": ctrl["index"],
                    }
                else:
                    # LLM didn't return a result — mark as unresolved
                    llm_inferred[cid] = {
                        "control_id": cid,
                        "control_description": ctrl["control_description"],
                        "current_frequency": ctrl["current_frequency"],
                        "inferred_frequency": "Recurring (multiple times per day)",
                        "business_frequency": "Unknown (defaulted to conservative)",
                        "source": "Default",
                        "confidence": "Low",
                        "index": ctrl["index"],
                    }

        # Combine all inferences
        all_inferences = {**keyword_matched, **llm_inferred}

        # Cache in state for approval flow
        state.pending_frequency_inferences = all_inferences

        # ── Export editable Excel ─────────────────────────────────────
        excel_path = None
        blob_path = None
        if state.output_dir:
            excel_path = os.path.join(
                state.output_dir, "inferred_frequencies.xlsx"
            )
            rows_for_excel = []
            for cid in sorted(all_inferences.keys()):
                inf = all_inferences[cid]
                rows_for_excel.append({
                    "Control ID": inf["control_id"],
                    "Control Description": inf["control_description"],
                    "Current Frequency": inf["current_frequency"],
                    "Inferred Frequency (KPMG)": inf["inferred_frequency"],
                    "Business Frequency": inf["business_frequency"],
                    "Source": inf["source"],
                    "Confidence": inf["confidence"],
                })

            try:
                export_df = pd.DataFrame(rows_for_excel)
                export_df.to_excel(excel_path, index=False, engine="openpyxl")
                logger.info("Exported inferred frequencies to %s", excel_path)
                state.frequency_inference_excel_path = excel_path
                blob_path = _upload_artifact(excel_path, state)
            except Exception as e:
                logger.warning("Failed to export frequencies Excel: %s", e)
                excel_path = None

        # ── Build display results ─────────────────────────────────────
        display_results = []
        for cid in sorted(all_inferences.keys()):
            inf = all_inferences[cid]
            display_results.append({
                "control_id": inf["control_id"],
                "control_description": (
                    inf["control_description"][:120] + "..."
                    if len(inf["control_description"]) > 120
                    else inf["control_description"]
                ),
                "current_frequency": inf["current_frequency"],
                "inferred_frequency": inf["inferred_frequency"],
                "business_frequency": inf["business_frequency"],
                "source": inf["source"],
                "confidence": inf["confidence"],
            })

        keyword_count = sum(1 for v in all_inferences.values() if v["source"] == "Keyword")
        llm_count = sum(1 for v in all_inferences.values() if v["source"] == "LLM")
        default_count = sum(1 for v in all_inferences.values() if v["source"] == "Default")

        return ToolResult(
            success=True,
            data=sanitize_for_json({
                "total_controls": len(df),
                "controls_with_frequency": len(controls_with_frequency),
                "controls_inferred": len(all_inferences),
                "keyword_matched": keyword_count,
                "llm_inferred": llm_count,
                "defaulted": default_count,
                "inferred_frequencies": display_results,
                "excel_path": excel_path,
                "excel_blob_path": blob_path,
                "requires_approval": True,
                "valid_frequencies": KPMG_FREQUENCIES,
            }),
            summary=(
                f"Inferred frequencies for {len(all_inferences)} controls: "
                f"{keyword_count} via keywords, {llm_count} via LLM, "
                f"{default_count} defaulted. Review and approve to apply."
            ),
            artifacts=[excel_path] if excel_path else [],
        )

    def _apply_inferences(self, state: AgentState) -> ToolResult:
        """Write approved inferred frequencies to the RCM."""
        if not state.pending_frequency_inferences:
            return ToolResult(
                success=False, data={},
                error=(
                    "No pending frequency inferences to apply. "
                    "Run infer_control_frequency first (without apply=true)."
                ),
            )

        df = state.rcm_df
        inferences = state.pending_frequency_inferences

        # Find the frequency column
        freq_col = None
        for col in df.columns:
            if col.lower().replace("_", " ").strip() in (
                "control frequency", "controlfrequency", "frequency",
                "control_frequency",
            ):
                freq_col = col
                break

        # If no frequency column exists, create one
        if not freq_col:
            freq_col = "Control Frequency"
            df[freq_col] = ""
            logger.info("Created 'Control Frequency' column in RCM")

        # Find control ID column
        cid_col = None
        for col in df.columns:
            if col.lower().replace("_", " ").strip() in (
                "control id", "controlid", "control_id",
            ):
                cid_col = col
                break

        # Apply inferred frequencies
        applied_count = 0
        for idx, row in df.iterrows():
            cid = str(row.get(cid_col, "")).strip()
            if cid in inferences:
                df.at[idx, freq_col] = inferences[cid]["inferred_frequency"]
                applied_count += 1

        state.rcm_df = df

        # Re-export normalised RCM
        if state.output_dir:
            normalised_path = os.path.join(state.output_dir, "normalised_rcm.xlsx")
            try:
                df.to_excel(normalised_path, index=False, engine="openpyxl")
                logger.info("Updated normalised_rcm.xlsx with inferred frequencies")
            except Exception as e:
                logger.warning("Failed to update normalised_rcm.xlsx: %s", e)

        # Clear pending inferences
        state.pending_frequency_inferences = None
        state.frequency_inference_excel_path = None

        return ToolResult(
            success=True,
            data={
                "applied_count": applied_count,
                "message": (
                    f"Applied inferred frequencies to {applied_count} controls. "
                    "The RCM 'Control Frequency' column has been updated. "
                    "You can now proceed to run the sampling engine."
                ),
            },
            summary=f"Applied inferred frequencies to {applied_count} controls in the RCM.",
        )
