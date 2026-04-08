"""
Quick test script for the RCM column normalizer.

Usage:
    python test_normalizer.py <path_to_rcm.xlsx>
    python test_normalizer.py <path_to_rcm.csv>
    python test_normalizer.py   # uses RCM_PATH below, or built-in dummy
"""

import sys
import os

# Ensure agent package is importable (navigate up from tests/ to flask-api/)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from agent.tools.column_normalizer import normalize_rcm_dataframe

# ─── CONFIGURE YOUR RCM PATH HERE ───────────────────────────────────
# Set this to your RCM file path so you can just run `python test_normalizer.py`
# Leave empty ("") to use the built-in dummy RCM.
RCM_PATH = ""
# Examples:
# RCM_PATH = "/Users/rishi/Downloads/8M/flask-api/data/my_rcm.xlsx"
# RCM_PATH = "./data/RCM_Procure_to_Pay.xlsx"
# ─────────────────────────────────────────────────────────────────────


def main():
    # CLI arg takes priority, then RCM_PATH, then built-in dummy
    if len(sys.argv) > 1:
        path = sys.argv[1]
    elif RCM_PATH.strip():
        path = RCM_PATH.strip()
    else:
        path = ""

    if path:
        if not os.path.exists(path):
            print(f"File not found: {path}")
            sys.exit(1)
        print(f"Loading: {path}\n")
        if path.endswith(".csv"):
            df = pd.read_csv(path, dtype=str)
        else:
            df = pd.read_excel(path, dtype=str)
    else:
        print("No file provided — using built-in dummy RCM with non-standard columns")
        print("Set RCM_PATH at the top of this script to skip this.\n")
        df = pd.DataFrame({
            "Business Process":     ["Procure to Pay", "Revenue"],
            "Activity":             ["Invoice Processing", "Billing"],
            "Ctrl ID":              ["C-P2P-001", "C-REV-001"],
            "Risk #":               ["R-P2P-001", "R-REV-001"],
            "Risk Name":            ["Unauthorized payments", "Revenue misstatement"],
            "Risk Desc":            ["Payments without approval", "Incorrect revenue recognition"],
            "Control Narrative":    ["3-way match before payment", "System enforced credit check"],
            "Performed By":         ["AP Manager", "Billing Lead"],
            "Significance":         ["Key", "Key"],
            "P/D":                  ["Preventive", "Detective"],
            "Automation":           ["Manual", "IT Automated"],
            "How Often":            ["Per occurrence", "Daily"],
            "ERP":                  ["SAP", "Oracle"],
            "Risk Rating":          ["High", "Medium"],
            "Assertion":            ["Completeness", "Occurrence"],
            "Sample Size":          ["25", "40"],
            "My Custom Notes":      ["some note", "another note"],
            "Review Date":          ["2025-01-15", "2025-02-20"],
        })

    print(f"Input: {len(df)} rows, {len(df.columns)} columns")
    print(f"Input columns: {list(df.columns)}\n")
    print("=" * 70)

    df_out, col_map, passthrough, missing = normalize_rcm_dataframe(df)

    # ── Column mapping confirmation ──
    print("\n  COLUMN MAPPING RESULTS")
    print("=" * 70)

    if col_map:
        print(f"\n  Mapped ({len(col_map)}):")
        for orig, mapped in sorted(col_map.items(), key=lambda x: x[1]):
            tag = "  [exact]" if orig.strip().lower() in (mapped.lower(), mapped.replace("/", " / ").lower()) else "  [LLM]  "
            print(f"    {tag}  {orig!r:35s}  -->  {mapped!r}")
    else:
        print("\n  No columns needed mapping (all already standard).")

    if passthrough:
        print(f"\n  Passthrough — kept as-is ({len(passthrough)}):")
        for col in passthrough:
            print(f"             {col!r}")

    if missing:
        print(f"\n  MISSING — required but not found ({len(missing)}):")
        for col in missing:
            print(f"          {col!r}")
    else:
        print(f"\n  All required columns resolved.")

    # ── Show output DataFrame ──
    print("\n" + "=" * 70)
    print(f"\n  Output: {len(df_out)} rows, {len(df_out.columns)} columns")
    print(f"  Output columns: {list(df_out.columns)}\n")
    print(df_out.to_string(index=False))

    # ── Optionally save ──
    if path:
        out_path = os.path.splitext(path)[0] + "_normalized.xlsx"
        df_out.to_excel(out_path, index=False, engine="openpyxl")
        print(f"\n  Saved normalized RCM to: {out_path}")


if __name__ == "__main__":
    main()
