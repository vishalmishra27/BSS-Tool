"""Core Playwright execution logic for UAT test runs."""
import os
import time
from datetime import datetime
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

from models import db

SCREENSHOT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "screenshots"
)
os.makedirs(SCREENSHOT_DIR, exist_ok=True)

DEFAULT_TIMEOUT_MS = 10_000  # 10s per step
LOGIN_URL_HINTS = ("login", "signin", "sign-in", "auth", "sso")


def _screenshot_name(test_run_id: int, test_case_id: str, step_id: str) -> str:
    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
    safe_tc = "".join(c if c.isalnum() else "_" for c in test_case_id)
    safe_step = "".join(c if c.isalnum() else "_" for c in step_id)
    return f"run{test_run_id}_{safe_tc}_{safe_step}_{ts}.png"


def _capture_screenshot(page, test_run_id, test_case_id, step_id) -> str:
    """Capture a screenshot and return the filename (not full path)."""
    filename = _screenshot_name(test_run_id, test_case_id, step_id)
    path = os.path.join(SCREENSHOT_DIR, filename)
    try:
        page.screenshot(path=path, full_page=False)
    except Exception:
        # If the page is in a bad state, write an empty placeholder
        try:
            with open(path, "wb") as f:
                f.write(b"")
        except Exception:
            return None
    return filename


def _execute_step(page, step: dict):
    """Execute a single step. Raises an exception on failure."""
    action = step["action"]
    selector = step.get("selector")
    input_value = step.get("input_value")
    expected_result = step.get("expected_result")

    page.set_default_timeout(DEFAULT_TIMEOUT_MS)

    if action == "navigate":
        if not input_value and not selector:
            raise ValueError("navigate requires a URL in input_value (or selector)")
        url = input_value or selector
        page.goto(url, timeout=DEFAULT_TIMEOUT_MS * 3, wait_until="domcontentloaded")
        # Detect login redirect
        current = (page.url or "").lower()
        if any(hint in current for hint in LOGIN_URL_HINTS) and not any(
            hint in (url or "").lower() for hint in LOGIN_URL_HINTS
        ):
            raise RuntimeError(
                f"Login redirect detected: navigated to '{page.url}'. "
                f"Session may be expired or unauthenticated."
            )

    elif action == "click":
        if not selector:
            raise ValueError("click requires a selector")
        page.click(selector)

    elif action == "type":
        if not selector:
            raise ValueError("type requires a selector")
        page.fill(selector, input_value or "")

    elif action == "assert_text":
        if not selector:
            raise ValueError("assert_text requires a selector")
        expected = expected_result or input_value or ""
        actual = page.locator(selector).first.inner_text(timeout=DEFAULT_TIMEOUT_MS)
        if expected.strip() not in (actual or "").strip():
            raise AssertionError(
                f"assert_text failed: expected '{expected}' in '{actual}'"
            )

    elif action == "assert_visible":
        if not selector:
            raise ValueError("assert_visible requires a selector")
        loc = page.locator(selector).first
        loc.wait_for(state="visible", timeout=DEFAULT_TIMEOUT_MS)

    elif action == "wait":
        # input_value is ms or seconds; treat <100 as seconds, else ms
        try:
            val = float(input_value) if input_value else 1.0
        except (TypeError, ValueError):
            val = 1.0
        ms = val * 1000 if val < 100 else val
        page.wait_for_timeout(ms)

    elif action == "select_dropdown":
        if not selector:
            raise ValueError("select_dropdown requires a selector")
        page.select_option(selector, input_value or "")

    elif action == "hover":
        if not selector:
            raise ValueError("hover requires a selector")
        page.hover(selector)

    else:
        raise ValueError(f"Unsupported action: {action}")


def run_test_cases(test_run_id: int, headless: bool = False):
    """Execute all steps for a given test_run_id sequentially.

    Each step's result (pass/fail) is recorded in the database along with
    a screenshot captured after the step, regardless of outcome.
    """
    steps = db.get_test_steps(test_run_id)
    if not steps:
        db.update_run_status(test_run_id, "completed")
        return

    db.update_run_status(test_run_id, "running")

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless)
            context = browser.new_context()
            # Auto-dismiss unexpected dialogs / alerts
            page = context.new_page()
            page.on("dialog", lambda d: d.dismiss())

            for step in steps:
                status = "passed"
                error_message = None

                try:
                    _execute_step(page, step)
                except PWTimeoutError as e:
                    status = "failed"
                    error_message = f"Timeout: {str(e).splitlines()[0] if str(e) else 'step timed out'}"
                except AssertionError as e:
                    status = "failed"
                    error_message = f"Assertion failed: {e}"
                except Exception as e:
                    status = "failed"
                    error_message = f"{type(e).__name__}: {e}"

                # Capture screenshot regardless of outcome
                screenshot_file = _capture_screenshot(
                    page, test_run_id, step["test_case_id"], step["step_id"]
                )

                db.insert_result(
                    test_run_id=test_run_id,
                    test_case_id=step["test_case_id"],
                    step_id=step["step_id"],
                    status=status,
                    error_message=error_message,
                    screenshot_path=screenshot_file,
                )

            try:
                context.close()
                browser.close()
            except Exception:
                pass

        db.update_run_status(test_run_id, "completed")

    except Exception as e:
        db.insert_result(
            test_run_id=test_run_id,
            test_case_id="__runner__",
            step_id="__runner__",
            status="failed",
            error_message=f"Runner crashed: {type(e).__name__}: {e}",
            screenshot_path=None,
        )
        db.update_run_status(test_run_id, "failed")
