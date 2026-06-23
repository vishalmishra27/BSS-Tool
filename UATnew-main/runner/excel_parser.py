"""Parse and validate uploaded UAT test-case Excel files."""
import pandas as pd

REQUIRED_COLUMNS = [
    "test_case_id",
    "step_id",
    "action",
    "selector",
    "input_value",
    "expected_result",
]

SUPPORTED_ACTIONS = {
    "navigate",
    "click",
    "type",
    "assert_text",
    "assert_visible",
    "wait",
    "select_dropdown",
    "hover",
}


class ExcelParseError(Exception):
    pass


def parse_excel(file_path: str) -> list:
    """Read the Excel file and return a list of step dicts.

    Raises ExcelParseError on invalid schema or unsupported actions.
    """
    try:
        df = pd.read_excel(file_path, engine="openpyxl")
    except Exception as e:
        raise ExcelParseError(f"Unable to read Excel file: {e}")

    # Normalize column names
    df.columns = [str(c).strip().lower() for c in df.columns]

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ExcelParseError(f"Missing required columns: {', '.join(missing)}")

    # Replace NaN with None for optional fields
    df = df.where(pd.notnull(df), None)

    steps = []
    for idx, row in df.iterrows():
        action = str(row["action"]).strip().lower() if row["action"] else ""
        if not action:
            raise ExcelParseError(f"Row {idx + 2}: action is required")
        if action not in SUPPORTED_ACTIONS:
            raise ExcelParseError(
                f"Row {idx + 2}: unsupported action '{action}'. "
                f"Supported: {', '.join(sorted(SUPPORTED_ACTIONS))}"
            )
        if not row["test_case_id"]:
            raise ExcelParseError(f"Row {idx + 2}: test_case_id is required")
        if not row["step_id"]:
            raise ExcelParseError(f"Row {idx + 2}: step_id is required")

        steps.append(
            {
                "test_case_id": str(row["test_case_id"]).strip(),
                "step_id": str(row["step_id"]).strip(),
                "action": action,
                "selector": (
                    str(row["selector"]).strip() if row["selector"] else None
                ),
                "input_value": (
                    str(row["input_value"]) if row["input_value"] is not None else None
                ),
                "expected_result": (
                    str(row["expected_result"]).strip()
                    if row["expected_result"]
                    else None
                ),
            }
        )

    if not steps:
        raise ExcelParseError("Excel file contains no test steps")

    return steps
