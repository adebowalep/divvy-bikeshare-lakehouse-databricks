"""Small helpers shared by the scripts in tools/: repository root and headless-browser discovery."""
import os
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

_BROWSER_CANDIDATES = [
    # macOS
    "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/Applications/Chromium.app/Contents/MacOS/Chromium",
    # Windows
    r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
]
_BROWSER_NAMES = ["microsoft-edge", "msedge", "google-chrome", "google-chrome-stable", "chromium", "chromium-browser", "chrome"]


def find_browser(explicit=None):
    """Return the path of a Chromium-based browser able to run headless.

    Order: the explicit argument, the DIVVY_BROWSER environment variable, well-known install paths,
    then executables on PATH. Exits with a clear message when none is found."""
    for cand in (explicit, os.environ.get("DIVVY_BROWSER")):
        if cand:
            if Path(cand).exists() or shutil.which(cand):
                return cand
            sys.exit(f"Browser not found: {cand}")
    for cand in _BROWSER_CANDIDATES:
        if Path(cand).exists():
            return cand
    for name in _BROWSER_NAMES:
        found = shutil.which(name)
        if found:
            return found
    sys.exit("No Chromium-based browser found. Install Edge/Chrome/Chromium, or pass --browser PATH "
             "(or set DIVVY_BROWSER).")


def require_file(path, hint=""):
    path = Path(path)
    if not path.is_file():
        sys.exit(f"Missing input file: {path}" + (f"\n{hint}" if hint else ""))
    return path
