"""Generate a sample test-cases Excel file for UAT Automation.

Run:  python sample/generate_sample.py
Produces: sample/sample_test_cases.xlsx
"""
import os
import pandas as pd

rows = [
    # TC001 — basic search flow on example.com
    {
        "test_case_id": "TC001",
        "step_id": "S1",
        "action": "navigate",
        "selector": "",
        "input_value": "https://example.com",
        "expected_result": "Page loads",
    },
    {
        "test_case_id": "TC001",
        "step_id": "S2",
        "action": "assert_visible",
        "selector": "h1",
        "input_value": "",
        "expected_result": "H1 is visible",
    },
    {
        "test_case_id": "TC001",
        "step_id": "S3",
        "action": "assert_text",
        "selector": "h1",
        "input_value": "",
        "expected_result": "Example Domain",
    },
    # TC002 — navigation + hover + wait
    {
        "test_case_id": "TC002",
        "step_id": "S1",
        "action": "navigate",
        "selector": "",
        "input_value": "https://example.com",
        "expected_result": "Page loads",
    },
    {
        "test_case_id": "TC002",
        "step_id": "S2",
        "action": "hover",
        "selector": "a",
        "input_value": "",
        "expected_result": "Link hovered",
    },
    {
        "test_case_id": "TC002",
        "step_id": "S3",
        "action": "wait",
        "selector": "",
        "input_value": "2",
        "expected_result": "Wait 2 seconds",
    },
    {
        "test_case_id": "TC002",
        "step_id": "S4",
        "action": "click",
        "selector": "a",
        "input_value": "",
        "expected_result": "Link clicked",
    },
]

df = pd.DataFrame(
    rows,
    columns=[
        "test_case_id",
        "step_id",
        "action",
        "selector",
        "input_value",
        "expected_result",
    ],
)

out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sample_test_cases.xlsx")
df.to_excel(out_path, index=False, engine="openpyxl")
print(f"Sample Excel written to: {out_path}")
