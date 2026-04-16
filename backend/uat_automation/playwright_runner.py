"""Core Playwright execution logic for UAT test runs.

The `playwright` package is imported lazily inside run_test_cases() so that
the rest of the uat_automation blueprint (upload, status, results, screenshot)
loads and works even when playwright is not yet installed.  Only POST /run
will return an error in that case.
"""
import os
import logging
from datetime import datetime

from uat_automation import db

logger = logging.getLogger(__name__)

SCREENSHOT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "uat_screenshots"
)
os.makedirs(SCREENSHOT_DIR, exist_ok=True)

DEFAULT_TIMEOUT_MS = 10_000   # 10 s per step
LOGIN_URL_HINTS = ("login", "signin", "sign-in", "auth", "sso")


def _screenshot_name(test_run_id: int, test_case_id: str, step_id: str) -> str:
    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
    safe_tc   = "".join(c if c.isalnum() else "_" for c in test_case_id)
    safe_step = "".join(c if c.isalnum() else "_" for c in step_id)
    return f"run{test_run_id}_{safe_tc}_{safe_step}_{ts}.png"


def _capture_screenshot(page, test_run_id, test_case_id, step_id) -> str:
    """Capture a screenshot and return the filename (not the full path)."""
    filename = _screenshot_name(test_run_id, test_case_id, step_id)
    path = os.path.join(SCREENSHOT_DIR, filename)
    try:
        page.screenshot(path=path, full_page=False)
    except Exception:
        # Page may be in a bad state — write an empty placeholder so the
        # filename still points to something on disk.
        try:
            with open(path, "wb") as f:
                f.write(b"")
        except Exception:
            return None
    return filename


def _execute_step(page, step: dict):
    """Execute a single test step via Playwright.  Raises on failure."""
    # Lazy import — playwright must already be on the path by the time this
    # function is called (i.e. after run_test_cases() has imported it).
    from playwright.sync_api import TimeoutError as PWTimeoutError  # noqa: F401

    action         = step["action"]
    selector       = step.get("selector")
    input_value    = step.get("input_value")
    expected_result = step.get("expected_result")

    page.set_default_timeout(DEFAULT_TIMEOUT_MS)

    if action == "navigate":
        if not input_value and not selector:
            raise ValueError("navigate requires a URL in input_value (or selector)")
        url = input_value or selector
        page.goto(url, timeout=DEFAULT_TIMEOUT_MS * 3, wait_until="domcontentloaded")
        # Detect login redirect — surface auth issues early
        current = (page.url or "").lower()
        if any(hint in current for hint in LOGIN_URL_HINTS) and not any(
            hint in (url or "").lower() for hint in LOGIN_URL_HINTS
        ):
            raise RuntimeError(
                f"Login redirect detected: navigated to '{page.url}'. "
                "Session may be expired or unauthenticated."
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
        actual   = page.locator(selector).first.inner_text(timeout=DEFAULT_TIMEOUT_MS)
        if expected.strip() not in (actual or "").strip():
            raise AssertionError(
                f"assert_text failed: expected '{expected}' in '{actual}'"
            )

    elif action == "assert_visible":
        if not selector:
            raise ValueError("assert_visible requires a selector")
        page.locator(selector).first.wait_for(state="visible", timeout=DEFAULT_TIMEOUT_MS)

    elif action == "wait":
        # input_value treated as seconds if < 100, else milliseconds
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
        raise ValueError(f"Unsupported action: '{action}'")


def run_test_cases(test_run_id: int, headless: bool = False):
    """Execute all steps for *test_run_id* sequentially.

    * Every step is executed regardless of whether a previous step failed
      (non-aborting run — one bad assertion does not kill the whole suite).
    * A screenshot is captured after every step (pass **and** fail) so there
      is a complete visual trail of the run.
    * headless defaults to False (headed mode) because many enterprise / BSS
      portals detect and block headless browsers.  Pass headless=True for
      CI / server environments that have no display.
    """
    # Lazy import so the module loads even without playwright installed.
    # If playwright is missing this raises ImportError here, which is caught
    # by the caller (the background thread starter in endpoints.py) and
    # recorded as a failed run rather than crashing the Flask process.
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError
    except ImportError as exc:
        logger.error("playwright is not installed — cannot execute test run: %s", exc)
        db.insert_result(
            test_run_id=test_run_id,
            test_case_id="__runner__",
            step_id="__runner__",
            status="failed",
            error_message=(
                "playwright is not installed on this server.  "
                "Run: pip install playwright && playwright install chromium"
            ),
        )
        db.update_run_status(test_run_id, "failed")
        return

    steps = db.get_test_steps(test_run_id)
    if not steps:
        db.update_run_status(test_run_id, "completed")
        return

    db.update_run_status(test_run_id, "running")

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless)
            context = browser.new_context()
            page    = context.new_page()
            # Auto-dismiss unexpected alert / confirm / prompt dialogs so they
            # don't freeze the runner waiting for user interaction.
            page.on("dialog", lambda d: d.dismiss())

            for step in steps:
                step_status    = "passed"
                error_message  = None

                try:
                    _execute_step(page, step)
                except PWTimeoutError as e:
                    step_status   = "failed"
                    error_message = f"Timeout: {str(e).splitlines()[0] if str(e) else 'step timed out'}"
                except AssertionError as e:
                    step_status   = "failed"
                    error_message = f"Assertion failed: {e}"
                except Exception as e:
                    step_status   = "failed"
                    error_message = f"{type(e).__name__}: {e}"

                # Screenshot captured after every step — pass or fail
                screenshot_file = _capture_screenshot(
                    page, test_run_id, step["test_case_id"], step["step_id"]
                )

                db.insert_result(
                    test_run_id=test_run_id,
                    test_case_id=step["test_case_id"],
                    step_id=step["step_id"],
                    status=step_status,
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
        logger.error("UAT runner crashed for test_run_id=%s: %s", test_run_id, e)
        db.insert_result(
            test_run_id=test_run_id,
            test_case_id="__runner__",
            step_id="__runner__",
            status="failed",
            error_message=f"Runner crashed: {type(e).__name__}: {e}",
            screenshot_path=None,
        )
        db.update_run_status(test_run_id, "failed")
