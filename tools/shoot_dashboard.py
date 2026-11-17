"""Headless screenshot of the dashboard -> PNG, cropped to content.

Usage: python tools/shoot_dashboard.py [--html PATH] [--png PATH] [--scheme light|dark] [--browser PATH]
Defaults: dashboard/index.html -> docs/images/dashboard-overview.png"""
import argparse
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import ROOT, find_browser, require_file
from PIL import Image, ImageChops

ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
ap.add_argument("--html", type=Path, default=ROOT / "dashboard" / "index.html")
ap.add_argument("--png", type=Path, default=ROOT / "docs" / "images" / "dashboard-overview.png")
ap.add_argument("--scheme", choices=["light", "dark"], default="light")
ap.add_argument("--width", type=int, default=1200)
ap.add_argument("--height", type=int, default=4800, help="window height; must be taller than the page")
ap.add_argument("--max-height", type=int, default=0, help="keep only the top N pixels of the capture (for README previews)")
ap.add_argument("--browser", default=None, help="path to a Chromium-based browser (default: auto-detect)")
a = ap.parse_args()

html = require_file(a.html).resolve()
a.png.parent.mkdir(parents=True, exist_ok=True)
prof = tempfile.mkdtemp(prefix="divvy_shot_")
try:
    subprocess.run([find_browser(a.browser), "--headless=new", "--disable-gpu", "--no-first-run", f"--user-data-dir={prof}",
                    "--hide-scrollbars", "--virtual-time-budget=6000", "--force-device-scale-factor=1.5",
                    f"--window-size={a.width},{a.height}",
                    f"--blink-settings=preferredColorScheme={0 if a.scheme == 'dark' else 1}",
                    f"--screenshot={a.png}", html.as_uri()], capture_output=True, timeout=60)
except subprocess.TimeoutExpired:
    pass                                   # headless browsers sometimes linger after writing the PNG
finally:
    shutil.rmtree(prof, ignore_errors=True)
if not a.png.exists():
    sys.exit("Screenshot was not produced")
im = Image.open(a.png).convert("RGB")
bg = im.getpixel((5, im.height - 5))
bbox = ImageChops.difference(im, Image.new("RGB", im.size, bg)).getbbox()
if bbox[3] >= im.height - 5:
    sys.exit("Page is taller than the window: pass a larger --height")
im.crop((0, 0, im.width, min(im.height, bbox[3] + 40, a.max_height or 10**9))).save(a.png)
print("wrote", a.png, Image.open(a.png).size)
