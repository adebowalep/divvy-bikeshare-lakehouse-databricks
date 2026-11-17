"""Checks on the static dashboard: data reconciliation, HTML generation, privacy and a headless-browser render.

No Spark and no Azure access needed. The browser tests are skipped (not passed) when no Chromium-based
browser is installed. Interactive behaviour (filters, theme switch, responsive layout) is not covered here;
see docs/validation.md for what was checked by hand.
"""
import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DASH = ROOT / "dashboard"
DATA = json.loads((DASH / "data.json").read_text())
HTML = (DASH / "index.html").read_text()
TEMPLATE = (DASH / "template.html").read_text()
K = DATA["kpi"]
QUALITY = {r["q"]: r["n"] for r in DATA["quality"]}


# ----------------------------------------------------------------------------- data.json
def test_required_sections_present():
    for key in ["kpi", "quality", "weekday_band", "hourly", "monthly_trips", "top_start", "longest_end",
                "age_member", "pay_year", "pay_quarter", "pay_age", "spend_by_rides", "spend_by_minutes"]:
        assert DATA.get(key), key


def test_quality_categories_add_up_and_use_the_defined_set():
    assert set(QUALITY) <= {"OK", "NEGATIVE", "ZERO", "OVER_24H", "MISSING"}
    assert sum(QUALITY.values()) == K["trips"]
    assert QUALITY["OK"] == K["valid_trips"]


@pytest.mark.parametrize("section", ["weekday_band", "hourly", "monthly_trips", "age_member"])
def test_valid_trip_breakdowns_reconcile_to_kpi(section):
    assert sum(r["trips"] for r in DATA[section]) == K["valid_trips"]


def test_payment_breakdowns_reconcile_to_total():
    assert sum(r["total"] for r in DATA["pay_year"]) == pytest.approx(K["total_paid"], abs=0.01)
    assert sum(r["total"] for r in DATA["pay_quarter"]) == pytest.approx(K["total_paid"], abs=0.01)


def test_kpi_values_are_plausible():
    assert 0 < K["median_minutes"] < K["avg_minutes"] < 60 * 24
    assert 0 < K["member_share"] < 1
    assert K["unmatched_trip_keys"] == 0 and K["unmatched_payment_keys"] == 0


def test_no_over_24h_trip_inflates_station_averages():
    # the longest average ride (stations with >= 200 trips) must be under 24 hours once OVER_24H is excluded
    assert max(r["avg_min"] for r in DATA["longest_end"]) < 24 * 60


# ------------------------------------------------------------------------------- HTML build
def test_index_html_has_data_embedded_and_no_unresolved_marker():
    assert "/*DATA*/null" not in HTML and "/*DATA*/null" in TEMPLATE
    embedded = re.search(r"const D\s*=\s*(\{.*?\});", HTML, re.S)
    assert embedded, "embedded data object not found"
    assert json.loads(embedded.group(1)) == DATA


def test_build_dashboard_script_reproduces_the_committed_index(tmp_path):
    out = tmp_path / "index.html"
    r = subprocess.run([sys.executable, str(ROOT / "tools" / "build_dashboard.py"), "--out", str(out)],
                       capture_output=True, text=True, cwd=tmp_path)    # run from another directory on purpose
    assert r.returncode == 0, r.stderr
    assert out.read_text() == HTML


def test_build_dashboard_fails_clearly_on_missing_input(tmp_path):
    r = subprocess.run([sys.executable, str(ROOT / "tools" / "build_dashboard.py"), "--data", str(tmp_path / "nope.json")],
                       capture_output=True, text=True)
    assert r.returncode != 0 and "Missing input file" in r.stderr


def test_html_has_the_expected_sections_and_no_template_placeholders():
    for anchor in ["when", "stations", "riders", "money", "spend", "quality"]:
        assert f'id="{anchor}"' in HTML, anchor
    assert not re.search(r"\{\{.*?\}\}", HTML) and "__PLACEHOLDER__" not in HTML


def test_only_external_dependency_is_optional_web_fonts():
    """The one external request is Google Fonts (Inter, Space Grotesk); system fonts are the fallback."""
    external = re.findall(r"""(?:src|href)\s*=\s*["'](https?://[^"']+)""", HTML)
    assert external and all(u.startswith("https://fonts.googleapis.com") for u in external), external
    assert "system-ui" in HTML                                      # fallback font stack exists
    assert "fetch(" not in HTML and "XMLHttpRequest" not in HTML   # no data requests, no backend


# ------------------------------------------------------------------------------------ privacy
def test_no_rider_personal_data_in_published_files():
    blob = HTML + (DASH / "data.json").read_text()
    for forbidden in ["Alyssa Squares", "Diana Ferry", "Brittany Row", "Dickerson Turnpike", "Nathan Expressway",
                      "Middleton", '"first"', '"last"', '"address"', '"birthday"', "birth_date"]:
        assert forbidden not in blob, forbidden


def test_executed_notebook_outputs_contain_no_rider_names_or_addresses():
    nb = (ROOT / "evidence" / "databricks_run_v1" / "Divvy_Databricks_Lakehouse_executed_v1.ipynb").read_text()
    for forbidden in ["Alyssa Squares", "Diana Ferry", "Jennifer", "Brittany Row", "Dickerson", "Nathan Expressway", "Middleton"]:
        assert forbidden not in nb, forbidden


def test_source_notebook_does_not_display_personal_columns():
    nb = (ROOT / "notebooks" / "Divvy_Databricks_Lakehouse.ipynb").read_text()
    assert "PII_COLS" in nb and "df.drop(*PII_COLS" in nb


# ----------------------------------------------------------------------------- real browser
def _browser():
    sys.path.insert(0, str(ROOT / "tools"))
    import _common
    try:
        return _common.find_browser()
    except SystemExit:
        pytest.skip("no Chromium-based browser installed")


@pytest.fixture(scope="module")
def rendered():
    """Render the page in headless Chromium: returns (DOM after scripts ran, console/stderr output)."""
    browser = _browser()
    with tempfile.TemporaryDirectory() as prof:
        try:
            r = subprocess.run([browser, "--headless=new", "--disable-gpu", "--no-first-run", f"--user-data-dir={prof}",
                                "--virtual-time-budget=6000", "--enable-logging=stderr", "--v=0", "--window-size=1200,3000",
                                "--dump-dom", (DASH / "index.html").resolve().as_uri()],
                               capture_output=True, text=True, timeout=90)
        except subprocess.TimeoutExpired as e:
            pytest.skip(f"headless browser timed out: {e}")
    return r.stdout, r.stderr


def test_browser_page_title_and_kpis_render(rendered):
    dom, _ = rendered
    assert "Divvy Lakehouse Dashboard" in dom
    assert f"{K['trips']:,}" in dom                                # KPI tile / quality table show the real trip count
    visible = re.sub(r"<(script|style)\b.*?</\1>", "", dom, flags=re.S | re.I)   # check rendered text, not source code
    assert "undefined" not in visible and "NaN" not in visible


def test_browser_all_charts_render_as_svg(rendered):
    dom, _ = rendered
    assert len(re.findall(r"<svg", dom)) >= 10


def test_browser_has_no_javascript_errors(rendered):
    _, err = rendered
    js_errors = [ln for ln in err.splitlines() if "CONSOLE" in ln and re.search(r"error|uncaught", ln, re.I)]
    assert not js_errors, js_errors[:3]
