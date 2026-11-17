"""Renders groups of cells from the EXPORTED (executed) notebook into clean PNGs.
These are renders of the outputs stored in the .ipynb, NOT screenshots of the Databricks UI.
Nothing is edited: code and outputs are exactly those stored in the notebook.

Usage: python tools/render_evidence.py NOTEBOOK OUTDIR [--only NAME_PREFIX ...] [--browser PATH]"""
import argparse, os, copy, subprocess, sys, tempfile
from pathlib import Path
import nbformat
from nbconvert import HTMLExporter
sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import find_browser, require_file
ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
ap.add_argument("notebook", type=Path); ap.add_argument("outdir", type=Path)
ap.add_argument("--only", nargs="*", default=None, help="render only groups whose name starts with one of these prefixes")
ap.add_argument("--browser", default=None)
args = ap.parse_args()
nb_path, out = str(require_file(args.notebook)), str(args.outdir)
PROF = tempfile.mkdtemp(prefix="edgeprof_"); os.makedirs(out, exist_ok=True)
nb = nbformat.read(nb_path, as_version=4)
EDGE = find_browser(args.browser)
# (filename, cell indexes, title, window height)
GROUPS = [
 ("15_config_and_file_check", [1,2,3], 900),
 ("16_section1_inspection", [5,6,7], 2600),
 ("17_section2_bronze_delta_files", [9,10,11], 1000),
 ("18_section3_bronze_tables", [12,13,14,15], 1300),
 ("19_section6_gold_tables", [20,21,22], 3000),
 ("20_section7_validation", [23,24,25], 3200),
 ("21_analytics_weekday_station", [26,28,29,30,31], 2400),
 ("22_analytics_age_payments", [32,33,34,35,36,37], 2600),
 ("23_analytics_extra_credit", [38,39,40], 1500),
 ("24_summary", [41,42,43], 1100),
]
CSS = "<style>body{background:#fff!important;font-family:-apple-system,Helvetica,Arial,sans-serif}.jp-Notebook{padding:24px 40px!important;max-width:1500px}.jp-InputPrompt,.jp-OutputPrompt{display:none!important}.jp-Cell{margin-bottom:14px}</style>"
for name, idx, h in GROUPS:
    if args.only and not any(name.startswith(p) for p in args.only): continue
    sub = nbformat.v4.new_notebook(); sub.metadata = nb.metadata
    sub.cells = [copy.deepcopy(nb.cells[i]) for i in idx]
    for c in sub.cells: c.get("metadata", {}).clear()
    html, _ = HTMLExporter(template_name="lab").from_notebook_node(sub)
    import re
    html = re.sub(r"<script[^>]*src=[^>]*></script>", "", html)
    html = html.replace("</head>", CSS + "</head>")
    p = os.path.join(out, name + ".html"); open(p, "w").write(html)
    png = os.path.join(out, name + ".png")
    try:
      subprocess.run([EDGE, "--headless=new", "--disable-gpu", "--no-first-run", f"--user-data-dir={PROF}", "--virtual-time-budget=4000", "--hide-scrollbars", "--force-device-scale-factor=2",
                    f"--window-size={2400 if "validation" in name else 1500},{h*2 if "validation" not in name else h}", f"--screenshot={png}", "file://" + os.path.abspath(p)],
                   check=True, capture_output=True, timeout=40)
    except subprocess.TimeoutExpired:
      pass  # headless Edge sometimes lingers after writing the PNG
    from PIL import Image, ImageChops
    im = Image.open(png).convert("RGB")
    bbox = ImageChops.difference(im, Image.new("RGB", im.size, (255, 255, 255))).getbbox()
    if bbox:
        pad = 40
        im.crop((0, 0, im.width, min(im.height, bbox[3] + pad))).save(png)
    print("wrote", png)
