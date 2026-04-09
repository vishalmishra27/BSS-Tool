"""
Risk Level Inference tool.

When the RCM has controls with missing Risk Probability, Risk Impact, or
Risk Level values, this tool computes or infers the risk level using:

  1. Weighted scoring (P x I) when both Probability and Impact are present.
  2. Keyword + LLM inference for missing Probability/Impact values.
  3. Direct LLM inference of Risk Level when all three columns are missing.

Scoring logic (non-linear, weighted):
  Low = 1, Medium = 3, High = 6
  Risk Score = Probability Score x Impact Score
  Score  1-5  -> Low
  Score  6-17 -> Medium
  Score 18-35 -> High
  Score 36    -> Critical  (only High x High)

The hardcoded risk matrix:
  P:Low  x I:Low    = Low       (1)
  P:Low  x I:Med    = Low       (3)
  P:Low  x I:High   = Medium    (6)
  P:Med  x I:Low    = Low       (3)
  P:Med  x I:Med    = Medium    (9)
  P:Med  x I:High   = High     (18)
  P:High x I:Low    = Medium    (6)
  P:High x I:Med    = High     (18)
  P:High x I:High   = Critical (36)

Critical is NOT a user-selectable input; it is a system-computed escalation
triggered exclusively when both Probability and Impact are High.

During sampling, Critical automatically maps to High.

All inferred values are flagged as "Inferred - Please Confirm" so auditors
can distinguish computed vs. inferred ratings. Overrides are logged with
timestamp and user ID.
"""

from __future__ import annotations

import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd

from .base import Tool
from ..types import AgentState, ToolCategory, ToolParameter, ToolResult
from ..utils import sanitize_for_json

logger = logging.getLogger("agent.tools.infer_risk_level")

# ── Default weighted score values ──────────────────────────────────────────────
DEFAULT_SCORE_MAP = {"low": 1, "medium": 3, "high": 6}

# ── Default band thresholds ────────────────────────────────────────────────────
# Each tuple is (upper_bound_inclusive, label).  Scores above the last band
# are automatically "Critical".
DEFAULT_BANDS = [(5, "Low"), (17, "Medium"), (35, "High")]

# Valid user-facing input values (Critical is NOT a user input)
VALID_INPUTS = ["Low", "Medium", "High"]

# Valid risk level outputs (includes system-computed Critical)
VALID_OUTPUTS = ["Low", "Medium", "High", "Critical"]


def _score_to_level(score: int, bands=None) -> str:
    """Map a numeric risk score to a risk level string using band thresholds."""
    for threshold, label in (bands or DEFAULT_BANDS):
        if score <= threshold:
            return label
    return "Critical"


def build_risk_matrix(score_map=None, bands=None) -> dict:
    """Compute the full 3×3 risk matrix from weights and band thresholds.

    The matrix is NOT an independent hardcoded artefact — it is the direct
    numeric output of  Score = P_weight × I_weight  mapped through the band
    thresholds.  If the user changes weights or bands, the matrix auto-updates.

    Returns: {(prob_lower, impact_lower): "Low"|"Medium"|"High"|"Critical"}
    """
    sm = score_map or DEFAULT_SCORE_MAP
    b = bands or DEFAULT_BANDS
    matrix = {}
    for p in ("low", "medium", "high"):
        for i in ("low", "medium", "high"):
            score = sm[p] * sm[i]
            matrix[(p, i)] = _score_to_level(score, b)
    return matrix


# Default matrix — derived from default weights + bands at import time.
RISK_MATRIX = build_risk_matrix()


def compute_risk_score(probability: str, impact: str,
                       score_map=None, bands=None) -> tuple[int, str]:
    """Compute risk score and rating from probability and impact strings.

    Uses the provided (or default) score_map and bands.  The risk matrix is
    computed on the fly from these values — never from a separate hardcoded
    dict — so any user-customisation of weights/bands is automatically
    reflected.

    Returns (score, risk_level).
    """
    p = probability.strip().lower()
    i = impact.strip().lower()

    sm = score_map or DEFAULT_SCORE_MAP
    p_score = sm.get(p, 0)
    i_score = sm.get(i, 0)

    if p_score == 0 or i_score == 0:
        return 0, ""

    score = p_score * i_score
    level = _score_to_level(score, bands)

    return score, level


def _normalize_risk_input(raw) -> str:
    """Normalize a raw risk input value to Low/Medium/High."""
    if raw is None:
        return ""
    r = str(raw).strip().lower()
    mapping = {
        "low": "Low", "l": "Low", "minor": "Low", "minimal": "Low",
        "medium": "Medium", "med": "Medium", "moderate": "Medium", "m": "Medium",
        "high": "High", "h": "High", "major": "High", "significant": "High",
        "very high": "High", "critical": "High",
    }
    return mapping.get(r, "")


# ── Keyword-based risk inference ─────────────────────────────────────────────
KEYWORD_RISK_MAP: List[Dict[str, Any]] = [
    {
        "keywords": ["financial reporting", "financial statement", "material misstatement",
                      "regulatory compliance", "regulatory requirement", "legal compliance",
                      "fraud", "fraud risk", "revenue recognition", "asset misappropriation"],
        "probability": "High",
        "impact": "High",
        "risk_level": "Critical",
    },
    {
        "keywords": ["access control", "privileged access", "system access",
                      "segregation of duties", "sod violation", "unauthorized access",
                      "data breach", "cybersecurity", "information security"],
        "probability": "Medium",
        "impact": "High",
        "risk_level": "High",
    },
    {
        "keywords": ["reconciliation", "bank reconciliation", "account reconciliation",
                      "journal entry", "manual journal", "period-end close",
                      "month-end", "quarter-end"],
        "probability": "Medium",
        "impact": "Medium",
        "risk_level": "Medium",
    },
    {
        "keywords": ["change management", "change control", "system change",
                      "configuration change", "patch management"],
        "probability": "Medium",
        "impact": "Medium",
        "risk_level": "Medium",
    },
    {
        "keywords": ["backup", "disaster recovery", "business continuity",
                      "data retention", "archival"],
        "probability": "Low",
        "impact": "High",
        "risk_level": "Medium",
    },
    {
        "keywords": ["training", "awareness", "policy acknowledgment",
                      "documentation", "record keeping"],
        "probability": "Low",
        "impact": "Low",
        "risk_level": "Low",
    },
]


def _match_risk_keyword(description: str) -> Optional[Dict[str, str]]:
    """Try to match a control/risk description against keyword table."""
    desc_lower = description.lower()
    for entry in KEYWORD_RISK_MAP:
        for keyword in entry["keywords"]:
            if keyword in desc_lower:
                return {
                    "probability": entry["probability"],
                    "impact": entry["impact"],
                    "risk_level": entry["risk_level"],
                    "matched_keyword": keyword,
                }
    return None


def _llm_infer_risk(
    controls: List[Dict[str, str]],
    infer_mode: str = "prob_impact",
) -> Dict[str, Dict[str, str]]:
    """Use LLM to infer missing risk probability/impact or risk level.

    Makes one API call per control for reliability — no batch ID-matching
    issues, clear per-control logging, and if one fails only that one
    gets the Default fallback.

    Args:
        controls: List of {"control_id", "description", "existing_prob", "existing_impact"}
        infer_mode: "prob_impact" to infer P and/or I, "risk_level" to infer level directly

    Returns:
        {control_id: {"probability": ..., "impact": ..., "risk_level": ..., "reasoning": ...}}
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

    if infer_mode == "risk_level":
        system_prompt = (
            "You are an audit expert. Given a control description, determine "
            "the Risk Level, Probability, and Impact.\n\n"
            "Risk Levels: Low, Medium, High, Critical (Critical ONLY when both "
            "probability and impact are clearly very high).\n"
            "Probability & Impact: Low, Medium, or High.\n\n"
            "Be conservative — when uncertain, rate higher.\n\n"
            "Return a JSON object (NOT an array):\n"
            '{"probability": "<Low|Medium|High>", "impact": "<Low|Medium|High>", '
            '"risk_level": "<Low|Medium|High|Critical>", '
            '"reasoning": "<one sentence>"}\n\n'
            "Return ONLY valid JSON. No markdown, no code fences."
        )
    else:
        system_prompt = (
            "You are an audit expert. Given a control description, determine "
            "the Risk Probability and Risk Impact.\n\n"
            "Probability: How likely is it that this control will fail?\n"
            "  Low = unlikely, Medium = possible, High = likely.\n"
            "Impact: How severe is the consequence if it does fail?\n"
            "  Low = minor, Medium = moderate, High = severe.\n\n"
            "If one value is already provided, use it as context to infer the other. "
            "Be conservative — when uncertain, rate higher.\n\n"
            "Return a JSON object (NOT an array):\n"
            '{"probability": "<Low|Medium|High>", "impact": "<Low|Medium|High>", '
            '"reasoning": "<one sentence>"}\n\n'
            "Return ONLY valid JSON. No markdown, no code fences."
        )

    def _infer_single(ctrl: dict) -> tuple[str, Optional[dict]]:
        """Call the LLM for a single control. Returns (control_id, entry_or_None)."""
        cid = ctrl["control_id"]
        user_prompt = f"Control: {ctrl['description']}"
        if ctrl.get("existing_prob"):
            user_prompt += f"\nExisting Probability: {ctrl['existing_prob']}"
        if ctrl.get("existing_impact"):
            user_prompt += f"\nExisting Impact: {ctrl['existing_impact']}"

        try:
            response = client.chat.completions.create(
                model=AZURE_OPENAI_DEPLOYMENT,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                max_completion_tokens=256,
                response_format={"type": "json_object"},
            )

            raw = response.choices[0].message.content or "{}"
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                parsed = parsed[0] if parsed else {}
            if not isinstance(parsed, dict):
                raise ValueError(f"Unexpected response type: {type(parsed)}")

            prob = _normalize_risk_input(parsed.get("probability"))
            impact = _normalize_risk_input(parsed.get("impact"))
            risk_level = str(parsed.get("risk_level") or "").strip()

            if risk_level and risk_level not in VALID_OUTPUTS:
                for vo in VALID_OUTPUTS:
                    if vo.lower() == risk_level.lower():
                        risk_level = vo
                        break
                else:
                    risk_level = ""

            entry = {
                "probability": prob or "Medium",
                "impact": impact or "Medium",
                "reasoning": parsed.get("reasoning") or "",
            }
            if risk_level:
                entry["risk_level"] = risk_level

            logger.debug("LLM risk inference OK for %s: P=%s, I=%s, R=%s",
                         cid, entry["probability"], entry["impact"],
                         risk_level or "(computed)")
            return cid, entry

        except Exception as e:
            logger.warning("LLM risk inference failed for %s: %s", cid, e)
            return cid, None

    # Run all controls in parallel
    result = {}
    succeeded = 0
    failed = 0
    max_workers = min(5, len(controls))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_infer_single, ctrl): ctrl for ctrl in controls}
        for future in as_completed(futures):
            cid, entry = future.result()
            if entry is not None:
                result[cid] = entry
                succeeded += 1
            else:
                failed += 1

    logger.info("LLM risk inference complete: %d succeeded, %d failed out of %d",
                succeeded, failed, len(controls))
    return result


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


class InferRiskLevelTool(Tool):
    """Infer missing risk levels from control descriptions using weighted scoring + LLM."""

    @property
    def name(self) -> str:
        return "infer_risk_level"

    @property
    def description(self) -> str:
        return (
            "Infer missing Risk Level values from Risk Probability, Risk Impact, "
            "and/or control descriptions. Uses a weighted non-linear scoring model: "
            "Low=1, Medium=3, High=6; Score = Probability x Impact; "
            "bands: 1-5=Low, 6-17=Medium, 18-35=High, 36=Critical. "
            "When Probability/Impact values are missing, infers them from the control "
            "description using keywords + LLM. When all three are missing, infers "
            "Risk Level directly. Exports an editable Excel for review. "
            "Call with apply=true to write approved values to the RCM. "
            "IMPORTANT: Before calling this tool, ASK the user whether to use the "
            "default weighted scoring (Low=1, Medium=3, High=6) or custom weights/bands. "
            "Do NOT default without asking."
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
                    "Set to 'true' to write the pending inferred risk levels to the RCM. "
                    "Only use after the user has reviewed and approved the inferences. "
                    "Default: 'false' (infer and show for review)."
                ),
                required=False,
                default="false",
            ),
            ToolParameter(
                name="methodology",
                type="string",
                description=(
                    "Risk scoring methodology. 'weighted' for the default non-linear "
                    "scoring (Low=1, Medium=3, High=6; Score=PxI), or 'custom' if the "
                    "user wants custom weights/bands. ASK the user before calling — "
                    "do not assume 'weighted'."
                ),
                required=False,
                enum=["weighted", "custom"],
            ),
            ToolParameter(
                name="custom_weights",
                type="string",
                description=(
                    "Custom score weights. Format: 'Low=2, Medium=5, High=10'. "
                    "Only used if methodology is 'custom'."
                ),
                required=False,
            ),
            ToolParameter(
                name="custom_bands",
                type="string",
                description=(
                    "Custom band thresholds. Format: '1-6=Low, 7-20=Medium, 21-40=High'. "
                    "Scores above the last upper bound map to Critical. "
                    "Only used if methodology is 'custom'."
                ),
                required=False,
            ),
        ]

    def preconditions(self, state: AgentState) -> Optional[str]:
        if state.rcm_df is None:
            return "No RCM loaded. Use load_rcm first."
        return None

    def execute(self, args: Dict[str, Any], state: AgentState) -> ToolResult:
        apply_mode = str(args.get("apply", "false")).strip().lower() == "true"

        if apply_mode:
            return self._apply_inferences(state)

        # Parse methodology params and store in state if custom
        methodology = (args.get("methodology") or "").strip().lower()
        custom_weights_raw = (args.get("custom_weights") or "").strip()
        custom_bands_raw = (args.get("custom_bands") or "").strip()

        if methodology == "custom":
            import re
            if custom_weights_raw:
                pairs = re.findall(r'(low|medium|high)\s*[=:]\s*(\d+)',
                                   custom_weights_raw, re.IGNORECASE)
                if len(pairs) >= 3:
                    state.custom_risk_score_map = {k.lower(): int(v) for k, v in pairs}
                else:
                    return ToolResult(
                        success=False, data={},
                        error=f"Could not parse weights: '{custom_weights_raw}'. "
                              "Expected: 'Low=2, Medium=5, High=10'.",
                    )
            if custom_bands_raw:
                matches = re.findall(
                    r'(\d+)\s*[-–]\s*(\d+)\s*[=:]\s*(Low|Medium|High)',
                    custom_bands_raw, re.IGNORECASE,
                )
                if len(matches) >= 2:
                    state.custom_risk_bands = [
                        (int(upper), label.capitalize())
                        for _, upper, label in sorted(matches, key=lambda m: int(m[1]))
                    ]
                else:
                    return ToolResult(
                        success=False, data={},
                        error=f"Could not parse bands: '{custom_bands_raw}'. "
                              "Expected: '1-6=Low, 7-20=Medium, 21-40=High'.",
                    )

        return self._infer_risk_levels(state)

    def _infer_risk_levels(self, state: AgentState) -> ToolResult:
        """Detect controls with missing risk data and infer them."""
        df = state.rcm_df

        # Pick up user-customised weights/bands (if set via modify_risk_logic)
        _sm = getattr(state, "custom_risk_score_map", None) or None
        _bands = getattr(state, "custom_risk_bands", None) or None

        # Find relevant columns
        prob_col = "risk_probability" if "risk_probability" in df.columns else None
        impact_col = "risk_impact" if "risk_impact" in df.columns else None
        level_col = "risk_level" if "risk_level" in df.columns else None

        desc_col = None
        for col in df.columns:
            if col.lower().replace("_", " ").strip() in (
                "control description", "controldescription", "control_description",
            ):
                desc_col = col
                break

        # Also try risk description as fallback for context
        risk_desc_col = None
        for col in df.columns:
            if col.lower().replace("_", " ").strip() in (
                "risk description", "riskdescription", "risk_description",
            ):
                risk_desc_col = col
                break

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

        if not desc_col and not risk_desc_col:
            return ToolResult(
                success=False, data={},
                error="RCM does not have a 'Control Description' or 'Risk Description' column — cannot infer risk levels.",
            )

        # Categorize controls
        computed_from_pi = {}    # Both P and I present → compute score
        needs_inference = []     # Either P or I (or both) missing
        already_has_level = []   # Already has valid risk_level
        seen_control_ids = set() # Track processed IDs (one control can span multiple risk rows)

        computation_log = []

        for idx, row in df.iterrows():
            control_id = str(row.get(cid_col, "")).strip()
            if not control_id or control_id.lower() in ("nan", "none", "null", ""):
                continue

            # Skip duplicate rows for the same control (risk level is per-control)
            if control_id in seen_control_ids:
                continue
            seen_control_ids.add(control_id)

            raw_prob = str(row.get(prob_col, "")).strip() if prob_col else ""
            raw_impact = str(row.get(impact_col, "")).strip() if impact_col else ""
            raw_level = str(row.get(level_col, "")).strip() if level_col else ""

            norm_prob = _normalize_risk_input(raw_prob)
            norm_impact = _normalize_risk_input(raw_impact)

            is_prob_missing = not norm_prob or raw_prob.lower() in (
                "nan", "none", "null", "n/a", "na", "", "-", "tbd",
            )
            is_impact_missing = not norm_impact or raw_impact.lower() in (
                "nan", "none", "null", "n/a", "na", "", "-", "tbd",
            )
            is_level_missing = not raw_level or raw_level.lower() in (
                "nan", "none", "null", "n/a", "na", "", "-", "tbd",
            )

            description = str(row.get(desc_col, "")).strip() if desc_col else ""
            risk_desc = str(row.get(risk_desc_col, "")).strip() if risk_desc_col else ""
            combined_desc = f"{description} {risk_desc}".strip()

            if not is_prob_missing and not is_impact_missing:
                # Both present — compute directly
                score, level = compute_risk_score(norm_prob, norm_impact, _sm, _bands)
                if score > 0:
                    computed_from_pi[control_id] = {
                        "control_id": control_id,
                        "description": combined_desc[:150],
                        "probability": norm_prob,
                        "impact": norm_impact,
                        "score": score,
                        "risk_level": level,
                        "source": "Computed",
                        "confidence": "High",
                        "flag": "",
                        "index": idx,
                    }
                    computation_log.append({
                        "control_id": control_id,
                        "probability": norm_prob,
                        "impact": norm_impact,
                        "score": score,
                        "risk_level": level,
                        "source": "Computed",
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    })
                    continue

            if not is_level_missing and is_prob_missing and is_impact_missing:
                # Has risk_level but no P/I — already good, skip
                already_has_level.append(control_id)
                continue

            # Needs inference
            needs_inference.append({
                "index": idx,
                "control_id": control_id,
                "description": combined_desc,
                "existing_prob": norm_prob if not is_prob_missing else "",
                "existing_impact": norm_impact if not is_impact_missing else "",
                "has_level": not is_level_missing,
                "current_level": raw_level if not is_level_missing else "",
                "all_missing": is_prob_missing and is_impact_missing and is_level_missing,
            })

        if not computed_from_pi and not needs_inference:
            return ToolResult(
                success=True,
                data={
                    "message": "All controls already have valid risk level values.",
                    "total_controls": len(df),
                    "controls_with_level": len(already_has_level),
                },
                summary="All controls already have valid risk level values — no inference needed.",
            )

        # ── Step 1: Keyword matching for controls needing inference ───
        keyword_matched = {}
        llm_needed_pi = []       # Need P and/or I inferred
        llm_needed_direct = []   # Need risk_level directly

        for ctrl in needs_inference:
            if ctrl["all_missing"]:
                # All three missing — try keyword first, then direct LLM
                match = _match_risk_keyword(ctrl["description"])
                if match:
                    score, level = compute_risk_score(match["probability"], match["impact"], _sm, _bands)
                    keyword_matched[ctrl["control_id"]] = {
                        "control_id": ctrl["control_id"],
                        "description": ctrl["description"][:150],
                        "probability": match["probability"],
                        "impact": match["impact"],
                        "score": score,
                        "risk_level": level,
                        "source": "Keyword",
                        "matched_keyword": match["matched_keyword"],
                        "confidence": "Medium",
                        "flag": "Inferred - Please Confirm",
                        "index": ctrl["index"],
                    }
                    computation_log.append({
                        "control_id": ctrl["control_id"],
                        "probability": match["probability"],
                        "impact": match["impact"],
                        "score": score,
                        "risk_level": level,
                        "source": "Keyword",
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    })
                else:
                    llm_needed_direct.append(ctrl)
            else:
                # Has one of P/I — try keyword, then LLM to infer the missing one
                match = _match_risk_keyword(ctrl["description"])
                if match:
                    prob = ctrl["existing_prob"] or match["probability"]
                    impact = ctrl["existing_impact"] or match["impact"]
                    score, level = compute_risk_score(prob, impact, _sm, _bands)
                    keyword_matched[ctrl["control_id"]] = {
                        "control_id": ctrl["control_id"],
                        "description": ctrl["description"][:150],
                        "probability": prob,
                        "impact": impact,
                        "score": score,
                        "risk_level": level,
                        "source": "Keyword",
                        "matched_keyword": match["matched_keyword"],
                        "confidence": "Medium",
                        "flag": "Inferred - Please Confirm",
                        "index": ctrl["index"],
                    }
                    computation_log.append({
                        "control_id": ctrl["control_id"],
                        "probability": prob,
                        "impact": impact,
                        "score": score,
                        "risk_level": level,
                        "source": "Keyword",
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    })
                else:
                    llm_needed_pi.append(ctrl)

        # ── Step 2: LLM inference for remaining ──────────────────────
        llm_inferred = {}

        # Infer P/I for controls that have one missing
        if llm_needed_pi:
            llm_results = _llm_infer_risk(
                [{"control_id": c["control_id"],
                  "description": c["description"],
                  "existing_prob": c["existing_prob"],
                  "existing_impact": c["existing_impact"]}
                 for c in llm_needed_pi],
                infer_mode="prob_impact",
            )
            for ctrl in llm_needed_pi:
                cid = ctrl["control_id"]
                if cid in llm_results:
                    inf = llm_results[cid]
                    prob = ctrl["existing_prob"] or inf["probability"]
                    impact = ctrl["existing_impact"] or inf["impact"]
                    score, level = compute_risk_score(prob, impact, _sm, _bands)
                    llm_inferred[cid] = {
                        "control_id": cid,
                        "description": ctrl["description"][:150],
                        "probability": prob,
                        "impact": impact,
                        "score": score,
                        "risk_level": level,
                        "source": "LLM",
                        "reasoning": inf.get("reasoning", ""),
                        "confidence": "Medium",
                        "flag": "Inferred - Please Confirm",
                        "index": ctrl["index"],
                    }
                else:
                    # LLM failed to return a result for this control —
                    # fall back to Medium for missing values.
                    prob = ctrl["existing_prob"] or "Medium"
                    impact = ctrl["existing_impact"] or "Medium"
                    score, level = compute_risk_score(prob, impact, _sm, _bands)
                    missing_fields = []
                    if not ctrl["existing_prob"]:
                        missing_fields.append("Probability")
                    if not ctrl["existing_impact"]:
                        missing_fields.append("Impact")
                    llm_inferred[cid] = {
                        "control_id": cid,
                        "description": ctrl["description"][:150],
                        "probability": prob,
                        "impact": impact,
                        "score": score,
                        "risk_level": level,
                        "source": "Default (LLM unavailable)",
                        "confidence": "Low",
                        "flag": "Inferred - Please Confirm",
                        "index": ctrl["index"],
                        "reasoning": (
                            f"{', '.join(missing_fields)} {'was' if len(missing_fields) == 1 else 'were'} "
                            f"missing and could not be inferred by the model. "
                            f"Defaulted to Medium as a conservative estimate. "
                            f"Please review and correct if needed."
                        ),
                    }
                computation_log.append({
                    "control_id": cid,
                    "probability": llm_inferred[cid]["probability"],
                    "impact": llm_inferred[cid]["impact"],
                    "score": llm_inferred[cid]["score"],
                    "risk_level": llm_inferred[cid]["risk_level"],
                    "source": llm_inferred[cid]["source"],
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                })

        # Infer risk level directly for controls with all three missing
        if llm_needed_direct:
            llm_results = _llm_infer_risk(
                [{"control_id": c["control_id"],
                  "description": c["description"],
                  "existing_prob": "",
                  "existing_impact": ""}
                 for c in llm_needed_direct],
                infer_mode="risk_level",
            )
            for ctrl in llm_needed_direct:
                cid = ctrl["control_id"]
                if cid in llm_results:
                    inf = llm_results[cid]
                    prob = inf.get("probability", "Medium")
                    impact = inf.get("impact", "Medium")
                    risk_level = inf.get("risk_level", "")
                    score, computed_level = compute_risk_score(prob, impact, _sm, _bands)
                    # Prefer the directly inferred risk_level if provided
                    final_level = risk_level if risk_level else computed_level
                    llm_inferred[cid] = {
                        "control_id": cid,
                        "description": ctrl["description"][:150],
                        "probability": prob,
                        "impact": impact,
                        "score": score,
                        "risk_level": final_level,
                        "source": "LLM (direct)",
                        "reasoning": inf.get("reasoning", ""),
                        "confidence": "Medium",
                        "flag": "Inferred - Please Confirm",
                        "index": ctrl["index"],
                    }
                else:
                    llm_inferred[cid] = {
                        "control_id": cid,
                        "description": ctrl["description"][:150],
                        "probability": "Medium",
                        "impact": "Medium",
                        "score": 9,
                        "risk_level": "Medium",
                        "source": "Default (LLM unavailable)",
                        "confidence": "Low",
                        "flag": "Inferred - Please Confirm",
                        "index": ctrl["index"],
                        "reasoning": (
                            "Probability, Impact, and Risk Level were all missing "
                            "and could not be inferred by the model. "
                            "Defaulted to Medium/Medium (score 9 = Medium) as a "
                            "conservative estimate. Please review and correct."
                        ),
                    }
                computation_log.append({
                    "control_id": cid,
                    "probability": llm_inferred[cid]["probability"],
                    "impact": llm_inferred[cid]["impact"],
                    "score": llm_inferred[cid]["score"],
                    "risk_level": llm_inferred[cid]["risk_level"],
                    "source": llm_inferred[cid]["source"],
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                })

        # Combine all results
        all_inferences = {**computed_from_pi, **keyword_matched, **llm_inferred}

        # Cache in state
        state.pending_risk_level_inferences = all_inferences
        state.risk_computation_log = computation_log

        # ── Export editable Excel ─────────────────────────────────────
        excel_path = None
        blob_path = None
        if state.output_dir:
            excel_path = os.path.join(state.output_dir, "inferred_risk_levels.xlsx")
            rows_for_excel = []
            for cid in sorted(all_inferences.keys()):
                inf = all_inferences[cid]
                rows_for_excel.append({
                    "Control ID": inf["control_id"],
                    "Description": inf["description"],
                    "Probability": inf["probability"],
                    "Impact": inf["impact"],
                    "Score": inf["score"],
                    "Risk Level": inf["risk_level"],
                    "Source": inf["source"],
                    "Confidence": inf["confidence"],
                    "Flag": inf.get("flag", "") or "-",
                    "Matched Keyword": inf.get("matched_keyword", "") or "-",
                    "Reasoning": inf.get("reasoning", "") or "-",
                })

            try:
                export_df = pd.DataFrame(rows_for_excel)
                export_df.to_excel(excel_path, index=False, engine="openpyxl")
                logger.info("Exported inferred risk levels to %s", excel_path)
                state.risk_level_inference_excel_path = excel_path
                blob_path = _upload_artifact(excel_path, state)
            except Exception as e:
                logger.warning("Failed to export risk levels Excel: %s", e)
                excel_path = None

        # ── Build display results ─────────────────────────────────────
        display_results = []
        for cid in sorted(all_inferences.keys()):
            inf = all_inferences[cid]
            display_results.append({
                "control_id": inf["control_id"],
                "description": (
                    inf["description"][:120] + "..."
                    if len(inf["description"]) > 120
                    else inf["description"]
                ),
                "probability": inf["probability"],
                "impact": inf["impact"],
                "score": inf["score"],
                "risk_level": inf["risk_level"],
                "source": inf["source"],
                "confidence": inf["confidence"],
                "flag": inf.get("flag", ""),
            })

        computed_count = sum(1 for v in all_inferences.values() if v["source"] == "Computed")
        keyword_count = sum(1 for v in all_inferences.values() if v["source"] == "Keyword")
        llm_count = sum(1 for v in all_inferences.values() if v["source"] in ("LLM", "LLM (direct)"))
        default_count = sum(1 for v in all_inferences.values() if v["source"] == "Default")
        inferred_count = keyword_count + llm_count + default_count

        return ToolResult(
            success=True,
            data=sanitize_for_json({
                "total_controls": len(df),
                "controls_already_valid": len(already_has_level),
                "controls_computed": computed_count,
                "controls_inferred": inferred_count,
                "keyword_matched": keyword_count,
                "llm_inferred": llm_count,
                "defaulted": default_count,
                "risk_assessments": display_results,
                "excel_path": excel_path,
                "excel_blob_path": blob_path,
                "requires_approval": True if inferred_count > 0 else False,
                "scoring_model": {
                    "weights": {k.capitalize(): v for k, v in (_sm or DEFAULT_SCORE_MAP).items()},
                    "formula": "Risk Score = Probability Score x Impact Score",
                    "bands": {
                        f"1-{b[0]}": b[1]
                        for b in (_bands or DEFAULT_BANDS)
                    } | {f"{(_bands or DEFAULT_BANDS)[-1][0] + 1}+": "Critical"},
                    "risk_matrix": {
                        f"{p.capitalize()} x {i.capitalize()}": lvl
                        for (p, i), lvl in build_risk_matrix(_sm, _bands).items()
                    },
                    "customised": bool(_sm or _bands),
                },
            }),
            summary=(
                f"Risk level assessment for {len(all_inferences)} controls: "
                f"{computed_count} computed from P x I, "
                f"{keyword_count} via keywords, {llm_count} via LLM, "
                f"{default_count} defaulted."
                + (f" {inferred_count} flagged as 'Inferred - Please Confirm'."
                   if inferred_count > 0 else "")
                + " Review and approve to apply."
            ),
            artifacts=[excel_path] if excel_path else [],
        )

    def _apply_inferences(self, state: AgentState) -> ToolResult:
        """Write approved inferred risk levels to the RCM."""
        if not state.pending_risk_level_inferences:
            return ToolResult(
                success=False, data={},
                error=(
                    "No pending risk level inferences to apply. "
                    "Run infer_risk_level first (without apply=true)."
                ),
            )

        df = state.rcm_df
        inferences = state.pending_risk_level_inferences

        # Ensure columns exist
        if "risk_level" not in df.columns:
            df["risk_level"] = ""
        if "risk_probability" not in df.columns:
            df["risk_probability"] = ""
        if "risk_impact" not in df.columns:
            df["risk_impact"] = ""

        # Find control ID column
        cid_col = None
        for col in df.columns:
            if col.lower().replace("_", " ").strip() in (
                "control id", "controlid", "control_id",
            ):
                cid_col = col
                break

        applied_count = 0
        for idx, row in df.iterrows():
            cid = str(row.get(cid_col, "")).strip()
            if cid in inferences:
                inf = inferences[cid]
                df.at[idx, "risk_level"] = inf["risk_level"]
                if inf.get("probability"):
                    df.at[idx, "risk_probability"] = inf["probability"]
                if inf.get("impact"):
                    df.at[idx, "risk_impact"] = inf["impact"]
                applied_count += 1

        state.rcm_df = df

        # Re-export normalised RCM
        if state.output_dir:
            normalised_path = os.path.join(state.output_dir, "normalised_rcm.xlsx")
            try:
                df.to_excel(normalised_path, index=False, engine="openpyxl")
                logger.info("Updated normalised_rcm.xlsx with inferred risk levels")
            except Exception as e:
                logger.warning("Failed to update normalised_rcm.xlsx: %s", e)

        # Export computation log
        if state.output_dir and state.risk_computation_log:
            log_path = os.path.join(state.output_dir, "risk_computation_log.xlsx")
            try:
                log_df = pd.DataFrame(state.risk_computation_log)
                log_df.to_excel(log_path, index=False, engine="openpyxl")
                logger.info("Exported risk computation log to %s", log_path)
                _upload_artifact(log_path, state)
            except Exception as e:
                logger.warning("Failed to export risk computation log: %s", e)

        # Clear pending inferences
        state.pending_risk_level_inferences = None
        state.risk_level_inference_excel_path = None

        return ToolResult(
            success=True,
            data={
                "applied_count": applied_count,
                "message": (
                    f"Applied risk levels to {applied_count} controls. "
                    "The RCM 'risk_level', 'risk_probability', and 'risk_impact' columns have been updated. "
                    "A computation log has been exported for audit trail."
                ),
            },
            summary=f"Applied inferred risk levels to {applied_count} controls in the RCM.",
        )
