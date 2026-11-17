"""Injects dashboard/data.json into dashboard/template.html -> dashboard/index.html (self-contained).

Usage: python tools/build_dashboard.py [--data PATH] [--template PATH] [--out PATH]
Defaults are resolved from the repository root, so it can be run from any directory."""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import ROOT, require_file

MARKER = "/*DATA*/null"
REQUIRED_KEYS = ["kpi", "quality", "weekday_band", "hourly", "monthly_trips", "top_start", "longest_end",
                 "age_member", "pay_year", "pay_quarter", "pay_age", "spend_by_rides", "spend_by_minutes"]

ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
ap.add_argument("--data", type=Path, default=ROOT / "dashboard" / "data.json")
ap.add_argument("--template", type=Path, default=ROOT / "dashboard" / "template.html")
ap.add_argument("--out", type=Path, default=ROOT / "dashboard" / "index.html")
a = ap.parse_args()

data = json.loads(require_file(a.data, "Create it with tools/export_dashboard_data.py").read_text())
missing = [k for k in REQUIRED_KEYS if k not in data]
if missing:
    sys.exit(f"{a.data} is missing required sections: {missing}")
template = require_file(a.template).read_text()
if MARKER not in template:
    sys.exit(f"{a.template} has no {MARKER!r} placeholder (already built?)")
html = template.replace(MARKER, json.dumps(data, separators=(",", ":")))
if MARKER in html:
    sys.exit("Placeholder still present after substitution")
a.out.write_text(html)
print(f"wrote {a.out} ({len(html):,} bytes)")
