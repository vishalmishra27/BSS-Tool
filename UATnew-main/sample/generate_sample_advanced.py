"""Generate an advanced sample test-cases Excel file.

Targets https://the-internet.herokuapp.com — a public playground site
designed for UI automation practice. Exercises every supported action:
navigate, click, type, assert_text, assert_visible, wait, select_dropdown, hover.

Run:  python sample/generate_sample_advanced.py
Produces: sample/sample_test_cases_advanced.xlsx
"""
import os
import pandas as pd

BASE = "https://the-internet.herokuapp.com"

rows = [
    # ---------- TC001: Successful form login ----------
    {"test_case_id": "TC001", "step_id": "S1", "action": "navigate",
     "selector": "", "input_value": f"{BASE}/login",
     "expected_result": "Login page loads"},
    {"test_case_id": "TC001", "step_id": "S2", "action": "assert_visible",
     "selector": "h2", "input_value": "",
     "expected_result": "Header visible"},
    {"test_case_id": "TC001", "step_id": "S3", "action": "assert_text",
     "selector": "h2", "input_value": "",
     "expected_result": "Login Page"},
    {"test_case_id": "TC001", "step_id": "S4", "action": "type",
     "selector": "#username", "input_value": "tomsmith",
     "expected_result": "Username entered"},
    {"test_case_id": "TC001", "step_id": "S5", "action": "type",
     "selector": "#password", "input_value": "SuperSecretPassword!",
     "expected_result": "Password entered"},
    {"test_case_id": "TC001", "step_id": "S6", "action": "click",
     "selector": "button[type='submit']", "input_value": "",
     "expected_result": "Submit clicked"},
    {"test_case_id": "TC001", "step_id": "S7", "action": "assert_visible",
     "selector": "#flash", "input_value": "",
     "expected_result": "Flash message shown"},
    {"test_case_id": "TC001", "step_id": "S8", "action": "assert_text",
     "selector": "#flash", "input_value": "",
     "expected_result": "You logged into a secure area"},
    {"test_case_id": "TC001", "step_id": "S9", "action": "click",
     "selector": "a.button", "input_value": "",
     "expected_result": "Logout clicked"},

    # ---------- TC002: Failed login (negative test) ----------
    {"test_case_id": "TC002", "step_id": "S1", "action": "navigate",
     "selector": "", "input_value": f"{BASE}/login",
     "expected_result": "Login page loads"},
    {"test_case_id": "TC002", "step_id": "S2", "action": "type",
     "selector": "#username", "input_value": "wronguser",
     "expected_result": "Bad username entered"},
    {"test_case_id": "TC002", "step_id": "S3", "action": "type",
     "selector": "#password", "input_value": "wrongpass",
     "expected_result": "Bad password entered"},
    {"test_case_id": "TC002", "step_id": "S4", "action": "click",
     "selector": "button[type='submit']", "input_value": "",
     "expected_result": "Submit clicked"},
    {"test_case_id": "TC002", "step_id": "S5", "action": "assert_text",
     "selector": "#flash", "input_value": "",
     "expected_result": "Your username is invalid"},

    # ---------- TC003: Dropdown selection ----------
    {"test_case_id": "TC003", "step_id": "S1", "action": "navigate",
     "selector": "", "input_value": f"{BASE}/dropdown",
     "expected_result": "Dropdown page loads"},
    {"test_case_id": "TC003", "step_id": "S2", "action": "assert_visible",
     "selector": "#dropdown", "input_value": "",
     "expected_result": "Dropdown present"},
    {"test_case_id": "TC003", "step_id": "S3", "action": "select_dropdown",
     "selector": "#dropdown", "input_value": "2",
     "expected_result": "Option 2 selected"},
    {"test_case_id": "TC003", "step_id": "S4", "action": "wait",
     "selector": "", "input_value": "1",
     "expected_result": "Brief pause"},

    # ---------- TC004: Hover reveals caption ----------
    {"test_case_id": "TC004", "step_id": "S1", "action": "navigate",
     "selector": "", "input_value": f"{BASE}/hovers",
     "expected_result": "Hovers page loads"},
    {"test_case_id": "TC004", "step_id": "S2", "action": "hover",
     "selector": ".figure:nth-of-type(3)", "input_value": "",
     "expected_result": "First figure hovered"},
    {"test_case_id": "TC004", "step_id": "S3", "action": "assert_visible",
     "selector": ".figure:nth-of-type(3) .figcaption", "input_value": "",
     "expected_result": "Caption visible on hover"},
    {"test_case_id": "TC004", "step_id": "S4", "action": "assert_text",
     "selector": ".figure:nth-of-type(3) .figcaption h5", "input_value": "",
     "expected_result": "name: user1"},

    # ---------- TC005: Dynamic content / add-remove elements ----------
    {"test_case_id": "TC005", "step_id": "S1", "action": "navigate",
     "selector": "", "input_value": f"{BASE}/add_remove_elements/",
     "expected_result": "Add/Remove page loads"},
    {"test_case_id": "TC005", "step_id": "S2", "action": "click",
     "selector": "button[onclick='addElement()']", "input_value": "",
     "expected_result": "Add button clicked"},
    {"test_case_id": "TC005", "step_id": "S3", "action": "assert_visible",
     "selector": ".added-manually", "input_value": "",
     "expected_result": "New element appeared"},
    {"test_case_id": "TC005", "step_id": "S4", "action": "click",
     "selector": ".added-manually", "input_value": "",
     "expected_result": "Delete element"},

    # ---------- TC006: Checkboxes ----------
    {"test_case_id": "TC006", "step_id": "S1", "action": "navigate",
     "selector": "", "input_value": f"{BASE}/checkboxes",
     "expected_result": "Checkboxes page loads"},
    {"test_case_id": "TC006", "step_id": "S2", "action": "assert_visible",
     "selector": "#checkboxes", "input_value": "",
     "expected_result": "Checkbox form visible"},
    {"test_case_id": "TC006", "step_id": "S3", "action": "click",
     "selector": "#checkboxes input[type='checkbox']:nth-of-type(1)",
     "input_value": "", "expected_result": "Toggled checkbox 1"},

    # ---------- TC007: Intentional failure (selector does not exist) ----------
    {"test_case_id": "TC007", "step_id": "S1", "action": "navigate",
     "selector": "", "input_value": f"{BASE}/",
     "expected_result": "Home loads"},
    {"test_case_id": "TC007", "step_id": "S2", "action": "assert_visible",
     "selector": "#element-that-does-not-exist", "input_value": "",
     "expected_result": "Should fail — proves non-aborting runs"},
    {"test_case_id": "TC007", "step_id": "S3", "action": "assert_text",
     "selector": "h1", "input_value": "",
     "expected_result": "Welcome to the-internet"},
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

out_path = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "sample_test_cases_advanced.xlsx",
)
df.to_excel(out_path, index=False, engine="openpyxl")
print(f"Advanced sample Excel written to: {out_path}")
print(f"  {len(rows)} steps across 7 test cases")
