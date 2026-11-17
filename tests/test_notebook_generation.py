"""The notebook generator: reproducible output, embedded code identical to the tested module, and a guard that
refuses to overwrite a notebook that holds saved (execution-evidence) outputs."""
import json
import subprocess
import sys
from pathlib import Path

import pytest

pytest.importorskip("nbformat")
ROOT = Path(__file__).resolve().parents[1]
GEN = ROOT / "tools" / "build_notebook.py"
SOURCE_NB = ROOT / "notebooks" / "Divvy_Databricks_Lakehouse.ipynb"
EXECUTED_NB = ROOT / "evidence" / "databricks_run_v1" / "Divvy_Databricks_Lakehouse_executed_v1.ipynb"


def code_cells(path):
    return ["".join(c["source"]) for c in json.loads(path.read_text())["cells"] if c["cell_type"] == "code"]


def run(*args):
    return subprocess.run([sys.executable, str(GEN), *args], capture_output=True, text=True)


def test_generator_reproduces_the_committed_source_notebook(tmp_path):
    out = tmp_path / "nb.ipynb"
    assert run("--out", str(out)).returncode == 0
    assert code_cells(out) == code_cells(SOURCE_NB)


def test_source_notebook_has_no_saved_outputs():
    nb = json.loads(SOURCE_NB.read_text())
    assert not any(c.get("outputs") for c in nb["cells"] if c["cell_type"] == "code")


def test_notebook_embeds_the_tested_functions_verbatim():
    module = (ROOT / "src" / "divvy_transforms.py").read_text()
    cells = "\n".join(code_cells(SOURCE_NB))
    for fn in ["def age_years", "def duration_quality", "def build_fact_trip", "def build_fact_payment",
               "def build_dim_rider", "def build_dim_date", "def table_fingerprint"]:
        assert fn in module and fn in cells, fn
    body = module.split("# --- region: facts", 1)[1].split("# --- end region: facts", 1)[0]
    assert body.split("\n", 1)[1].strip() in cells                 # facts region copied unchanged


def test_generator_refuses_to_overwrite_executed_notebook(tmp_path):
    copy = tmp_path / "executed.ipynb"
    copy.write_text(EXECUTED_NB.read_text())
    before = copy.read_text()
    r = run("--out", str(copy))
    assert r.returncode != 0 and "Refusing to overwrite" in (r.stderr + r.stdout)
    assert copy.read_text() == before


def test_executed_notebook_still_has_its_azure_outputs():
    nb = json.loads(EXECUTED_NB.read_text())
    code = [c for c in nb["cells"] if c["cell_type"] == "code"]
    assert len(code) == 23 and all(c.get("execution_count") is not None for c in code)
    text = EXECUTED_NB.read_text()
    assert "ALL VALIDATION CHECKS PASSED" in text and "4584921" in text
