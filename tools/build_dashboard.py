"""Injects dashboard/data.json into dashboard/template.html -> dashboard/index.html (self-contained)."""
import json, sys
d = json.load(open("dashboard/data.json"))
t = open("dashboard/template.html").read()
assert "/*DATA*/null" in t
open("dashboard/index.html", "w").write(t.replace("/*DATA*/null", json.dumps(d, separators=(",", ":"))))
print("wrote dashboard/index.html", len(t), "bytes template")
