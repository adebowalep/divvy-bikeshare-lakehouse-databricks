"""Headless screenshot of dashboard/index.html -> PNG, cropped to content. Usage: shoot_dashboard.py <html> <png> [scheme: light|dark]"""
import sys, subprocess, os
from PIL import Image, ImageChops
html, png = sys.argv[1:3]; scheme = sys.argv[3] if len(sys.argv) > 3 else "light"
EDGE = "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge"
prof = os.path.join(os.path.dirname(png) or ".", ".edgeprof_tmp")
try:
    subprocess.run([EDGE, "--headless=new", "--disable-gpu", "--no-first-run", f"--user-data-dir={prof}", "--hide-scrollbars",
                    "--virtual-time-budget=6000", "--force-device-scale-factor=1.5", "--window-size=1200,3600",
                    f"--force-dark-mode" if scheme == "dark" else "--disable-features=WebContentsForceDark",
                    f"--screenshot={png}", "file://" + os.path.abspath(html)], capture_output=True, timeout=45)
except subprocess.TimeoutExpired:
    pass
im = Image.open(png).convert("RGB")
bg = im.getpixel((5, im.height - 5))
bbox = ImageChops.difference(im, Image.new("RGB", im.size, bg)).getbbox()
im.crop((0, 0, im.width, min(im.height, bbox[3] + 40))).save(png)
print("wrote", png, Image.open(png).size)
import shutil; shutil.rmtree(prof, ignore_errors=True)
