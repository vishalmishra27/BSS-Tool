"""
Standalone test runner for Column Normalizer, TOD Engine, and TOE Engine.

Usage:
    python test_engines.py              # runs all 3 tests
    python test_engines.py 1            # normalizer only
    python test_engines.py 2            # TOD only
    python test_engines.py 3            # TOE only
    python test_engines.py 1 2          # normalizer + TOD
    python test_engines.py 2 3          # TOD + TOE (schemas from TOD reused in TOE)

Configure paths and credentials in the CONFIGURATION section below.
"""

import sys
import os
import time

# Ensure imports work (navigate up from tests/ to flask-api/)
_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _project_root)
engines_dir = os.path.join(_project_root, "engines")
if engines_dir not in sys.path:
    sys.path.insert(0, engines_dir)


# ╔══════════════════════════════════════════════════════════════════════╗
# ║                        CONFIGURATION                                ║
# ║  Change these values to point at your files and API credentials     ║
# ╚══════════════════════════════════════════════════════════════════════╝

# -- Paths --
RCM_PATH = ""                  # e.g. "/Users/rishi/data/RCM.xlsx"
TOD_EVIDENCE_FOLDER = ""       # e.g. "/Users/rishi/data/evidence_tod/"
TOE_EVIDENCE_FOLDER = ""       # e.g. "/Users/rishi/data/evidence_toe/"
OUTPUT_DIR = "./test_output"   # where result files are written

# -- Azure OpenAI credentials --
AZURE_ENDPOINT = ""            # e.g. "https://my-resource.openai.azure.com"
AZURE_API_KEY = ""             # your API key
AZURE_DEPLOYMENT = ""          # e.g. "gpt-4o-mini"
AZURE_API_VERSION = "2024-12-01-preview"

# -- Or set via environment variables (these override the above if non-empty) --
# export AZURE_OPENAI_ENDPOINT=...
# export AZURE_OPENAI_API_KEY=...
# export AZURE_OPENAI_DEPLOYMENT=...

# -- Engine settings --
MAX_WORKERS = 5                # parallel API calls for TOD/TOE

# ╔══════════════════════════════════════════════════════════════════════╗
# ║                      END CONFIGURATION                              ║
# ╚══════════════════════════════════════════════════════════════════════╝


def _resolve(env_var, local_val):
    """Return env var if set, else local config value."""
    return os.getenv(env_var, "").strip() or local_val


def _get_api_config():
    endpoint = _resolve("AZURE_OPENAI_ENDPOINT", AZURE_ENDPOINT)
    api_key = _resolve("AZURE_OPENAI_API_KEY", AZURE_API_KEY) or _resolve("OPENAI_API_KEY", "")
    deployment = _resolve("AZURE_OPENAI_DEPLOYMENT", AZURE_DEPLOYMENT) or _resolve("OPENAI_MODEL", "gpt-4o-mini")
    api_version = _resolve("AZURE_OPENAI_API_VERSION", AZURE_API_VERSION)
    return endpoint, api_key, deployment, api_version


def _load_rcm(path):
    import pandas as pd
    if not path or not os.path.exists(path):
        print(f"  [ERROR] RCM file not found: {path!r}")
        print(f"          Set RCM_PATH at the top of this script.")
        sys.exit(1)
    print(f"  Loading RCM: {path}")
    # Read with header detection + marker normalisation, but WITHOUT column
    # normalisation — the test_normalizer function will run that step itself.
    from engines.rcm_reader import smart_read_file
    try:
        df, header_row, score = smart_read_file(path, normalize_columns=False, return_details=True)
        print(f"  Header detected at row {header_row} (score={score})")
        return df
    except Exception as e:
        print(f"  [WARN] Smart reader failed ({e}), falling back to plain read")
        if path.endswith(".csv"):
            return pd.read_csv(path, dtype=str)
        return pd.read_excel(path, dtype=str)


# ── Test 1: Column Normalizer ─────────────────────────────────────────

def test_normalizer():
    import pandas as pd
    from agent.tools.column_normalizer import (
        normalize_rcm_dataframe, REQUIRED_COLUMNS, OPTIONAL_COLUMNS,
    )

    print("=" * 70)
    print("  TEST 1: COLUMN NORMALIZER")
    print("=" * 70)

    if RCM_PATH and os.path.exists(RCM_PATH):
        df = _load_rcm(RCM_PATH)
    else:
        print("  No RCM_PATH set — using built-in dummy with non-standard columns\n")
        df = pd.DataFrame({
            "Business Process":     ["Procure to Pay", "Revenue"],
            "Sub-Process No":       ["5.1", "6.2"],
            "Sub Process Description": ["Invoice Processing", "Billing"],
            "Ctrl ID":              ["C-P2P-001", "C-REV-001"],
            "Risk #":               ["R-P2P-001", "R-REV-001"],
            "Risk Name":            ["Unauthorized payments", "Revenue misstatement"],
            "Risk Desc":            ["Payments without approval", "Incorrect revenue recognition"],
            "Control Narrative":    ["3-way match before payment", "System enforced credit check"],
            "Performed By":         ["AP Manager", "Billing Lead"],
            "P/D":                  ["Preventive", "Detective"],
            "Automation":           ["Manual", "IT Automated"],
            "How Often":            ["Per occurrence", "Daily"],
            "ERP":                  ["SAP", "Oracle"],
            "Risk Rating":          ["High", "Medium"],
            "Assertion":            ["Completeness", "Occurrence"],
            "Sample Size":          ["25", "40"],
            "Objective":            ["Ensure proper payment", "Ensure accurate billing"],
        })

    print(f"\n  Input: {len(df)} rows, {len(df.columns)} columns")
    print(f"  Input columns: {list(df.columns)}\n")

    start = time.time()
    df_out, col_map, passthrough, missing = normalize_rcm_dataframe(df)
    elapsed = time.time() - start

    # -- Column mapping results --
    print("-" * 70)
    print("  MAPPING RESULTS")
    print("-" * 70)

    if col_map:
        print(f"\n  Mapped ({len(col_map)}):")
        for orig, mapped in sorted(col_map.items(), key=lambda x: x[1]):
            print(f"    {orig!r:35s}  -->  {mapped!r}")

    if passthrough:
        print(f"\n  Passthrough ({len(passthrough)}):")
        for col in passthrough:
            print(f"    {col!r}")

    if missing:
        print(f"\n  MISSING required ({len(missing)}):")
        for col in missing:
            print(f"    {col!r}")
    else:
        print(f"\n  All required columns resolved.")

    print(f"\n  Required columns: {list(REQUIRED_COLUMNS.values())}")
    print(f"  Optional columns: {list(OPTIONAL_COLUMNS.values())}")
    print(f"\n  Output: {len(df_out)} rows, {len(df_out.columns)} columns")
    print(f"  Output columns: {list(df_out.columns)}")
    print(f"  Time: {elapsed:.2f}s")

    # Save normalized RCM to output folder
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    norm_output = os.path.join(OUTPUT_DIR, "Normalized_RCM.xlsx")
    df_out.to_excel(norm_output, index=False, engine="openpyxl")
    print(f"  Output: {norm_output}")
    print()

    return df_out


# ── Test 2: TOD Engine ────────────────────────────────────────────────

def test_tod(rcm_df=None):
    import pandas as pd
    import TOD_Engine

    print("=" * 70)
    print("  TEST 2: TOD ENGINE (Test of Design)")
    print("=" * 70)

    endpoint, api_key, deployment, api_version = _get_api_config()
    if not api_key:
        print("  [ERROR] No API key configured.")
        print("          Set AZURE_API_KEY at the top of this script or AZURE_OPENAI_API_KEY env var.")
        sys.exit(1)

    evidence_folder = TOD_EVIDENCE_FOLDER
    if not evidence_folder or not os.path.exists(evidence_folder):
        print(f"  [ERROR] TOD evidence folder not found: {evidence_folder!r}")
        print(f"          Set TOD_EVIDENCE_FOLDER at the top of this script.")
        sys.exit(1)

    # Prepare RCM file
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    if rcm_df is not None:
        rcm_path = os.path.join(OUTPUT_DIR, "_temp_rcm_for_tod_test.xlsx")
        rcm_df.to_excel(rcm_path, index=False, engine="openpyxl")
        print(f"  Using normalized RCM from Test 1 ({len(rcm_df)} rows)")
    else:
        rcm_path = RCM_PATH
        if not rcm_path or not os.path.exists(rcm_path):
            print(f"  [ERROR] RCM file not found: {rcm_path!r}")
            sys.exit(1)

    print(f"  RCM: {rcm_path}")
    print(f"  Evidence: {evidence_folder}")
    print(f"  Model: {deployment}")
    print(f"  Endpoint: {endpoint or '(direct OpenAI)'}")
    print()

    # Load evidence
    tod_bank = TOD_Engine.load_tod_evidence_folder(evidence_folder)
    print(f"  Evidence loaded for {len(tod_bank)} controls: {list(tod_bank.keys())}")

    for cid, samples in tod_bank.items():
        for s in samples:
            desc_len = len(s.description or "")
            print(f"    {cid}: {s.sample_id} — {desc_len:,} chars")
    print()

    # Create tester
    tester = TOD_Engine.RCMControlTester(
        rcm_path=rcm_path,
        openai_api_key=api_key,
        openai_model=deployment,
        azure_endpoint=endpoint,
        azure_api_key=api_key,
        azure_deployment=deployment,
        azure_api_version=api_version,
    )

    # Run TOD
    start = time.time()
    results, schemas = tester.test_all_tod(tod_bank, max_workers=MAX_WORKERS)
    elapsed = time.time() - start

    # Export
    tod_output = os.path.join(OUTPUT_DIR, "TOD_Results.xlsx")
    tester.export_tod_workpaper(results, tod_output, tod_bank=tod_bank)

    # Summary
    passed = sum(1 for r in results if r.result == "PASS")
    failed = sum(1 for r in results if r.result == "FAIL")

    print()
    print("-" * 70)
    print("  TOD RESULTS")
    print("-" * 70)
    for r in results:
        print(f"    {r.control_id:20s}  {r.result:6s}  Design: {r.design_adequate:4s}  "
              f"Confidence: {r.confidence:6s}  Deficiency: {r.deficiency_type}")
    print()
    print(f"  Total: {len(results)} controls — {passed} PASS, {failed} FAIL")
    print(f"  Schemas generated: {len(schemas)}")
    print(f"  Output: {tod_output}")
    print(f"  Time: {elapsed:.1f}s")
    print()

    return results, schemas


# ── Test 3: TOE Engine ────────────────────────────────────────────────

def test_toe(rcm_df=None, pre_schemas=None):
    import pandas as pd
    import TOE_Engine

    print("=" * 70)
    print("  TEST 3: TOE ENGINE (Test of Operating Effectiveness)")
    print("=" * 70)

    endpoint, api_key, deployment, api_version = _get_api_config()
    if not api_key:
        print("  [ERROR] No API key configured.")
        print("          Set AZURE_API_KEY at the top of this script or AZURE_OPENAI_API_KEY env var.")
        sys.exit(1)

    evidence_folder = TOE_EVIDENCE_FOLDER
    if not evidence_folder or not os.path.exists(evidence_folder):
        print(f"  [ERROR] TOE evidence folder not found: {evidence_folder!r}")
        print(f"          Set TOE_EVIDENCE_FOLDER at the top of this script.")
        sys.exit(1)

    # Prepare RCM file
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    if rcm_df is not None:
        rcm_path = os.path.join(OUTPUT_DIR, "_temp_rcm_for_toe_test.xlsx")
        rcm_df.to_excel(rcm_path, index=False, engine="openpyxl")
        print(f"  Using normalized RCM from Test 1 ({len(rcm_df)} rows)")
    else:
        rcm_path = RCM_PATH
        if not rcm_path or not os.path.exists(rcm_path):
            print(f"  [ERROR] RCM file not found: {rcm_path!r}")
            sys.exit(1)

    if pre_schemas:
        print(f"  Reusing {len(pre_schemas)} schemas from TOD (skipping schema generation)")
    else:
        print(f"  No pre-built schemas — TOE will generate fresh")

    print(f"  RCM: {rcm_path}")
    print(f"  Evidence: {evidence_folder}")
    print(f"  Model: {deployment}")
    print(f"  Endpoint: {endpoint or '(direct OpenAI)'}")
    print()

    # Load evidence
    toe_bank = TOE_Engine.load_toe_evidence_folder(evidence_folder)
    print(f"  Evidence loaded for {len(toe_bank)} controls: {list(toe_bank.keys())}")

    for cid, samples in toe_bank.items():
        print(f"    {cid}: {len(samples)} samples")
        for s in samples:
            desc_len = len(s.description or "")
            print(f"      {s.sample_id} — {desc_len:,} chars")
    print()

    # Create tester
    tester = TOE_Engine.RCMControlTester(
        rcm_path=rcm_path,
        openai_api_key=api_key,
        openai_model=deployment,
        azure_endpoint=endpoint,
        azure_api_key=api_key,
        azure_deployment=deployment,
        azure_api_version=api_version,
    )

    # Run TOE
    start = time.time()
    results = tester.test_all_toe(toe_bank, max_workers=MAX_WORKERS, pre_schemas=pre_schemas)
    elapsed = time.time() - start

    # Export
    toe_output = os.path.join(OUTPUT_DIR, "TOE_Workpaper.xlsx")
    tester.export_toe_workpaper(results, toe_output, toe_bank=toe_bank)

    # Summary
    effective = sum(1 for r in results if r.operating_effectiveness == "Effective")
    exceptions = sum(1 for r in results if r.operating_effectiveness == "Effective with Exceptions")
    not_effective = sum(1 for r in results if r.operating_effectiveness == "Not Effective")

    print()
    print("-" * 70)
    print("  TOE RESULTS")
    print("-" * 70)
    for r in results:
        print(f"    {r.control_id:20s}  {r.operating_effectiveness:30s}  "
              f"Samples: {r.passed_samples}/{r.total_samples} pass  "
              f"Deviation: {r.deviation_rate:.1%}  Deficiency: {r.deficiency_type}")
    print()
    print(f"  Total: {len(results)} controls — {effective} Effective, "
          f"{exceptions} Exceptions, {not_effective} Not Effective")
    print(f"  Output: {toe_output}")
    print(f"  Time: {elapsed:.1f}s")
    print()

    return results


# ── Main ──────────────────────────────────────────────────────────────

def main():
    # Parse which tests to run
    if len(sys.argv) > 1:
        tests = set()
        for arg in sys.argv[1:]:
            if arg in ("1", "2", "3"):
                tests.add(int(arg))
            else:
                print(f"Unknown argument: {arg}")
                print("Usage: python test_engines.py [1] [2] [3]")
                print("  1 = Column Normalizer")
                print("  2 = TOD Engine")
                print("  3 = TOE Engine")
                sys.exit(1)
    else:
        tests = {1, 2, 3}

    print()
    print("╔══════════════════════════════════════════════════════════════════════╗")
    print("║              SOX Engine Test Runner                                 ║")
    print(f"║              Running tests: {sorted(tests)}{'':40s}║")
    print("╚══════════════════════════════════════════════════════════════════════╝")
    print()

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    rcm_df = None
    schemas = None

    # Test 1: Normalizer
    if 1 in tests:
        rcm_df = test_normalizer()

    # Test 2: TOD
    if 2 in tests:
        _, schemas = test_tod(rcm_df=rcm_df)

    # Test 3: TOE (reuses schemas from TOD if both ran)
    if 3 in tests:
        test_toe(rcm_df=rcm_df, pre_schemas=schemas)

    print("=" * 70)
    print("  ALL DONE")
    print(f"  Output files in: {os.path.abspath(OUTPUT_DIR)}")
    print("=" * 70)


if __name__ == "__main__":
    main()
