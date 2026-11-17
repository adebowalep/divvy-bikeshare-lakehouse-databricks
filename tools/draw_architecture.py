"""Generates docs/architecture.drawio (editable) and docs/images/architecture.svg from one description,
so the two files cannot drift apart. Usage: python tools/draw_architecture.py

The diagram shows the pipeline that exists in notebooks/Divvy_Databricks_Lakehouse.ipynb: Bronze -> Gold, no Silver
layer. The dashboard export is an offline step on a local Spark session, not a query against the workspace."""
import sys
from html import escape
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import ROOT

W, H = 1500, 810
BW, BH = 220, 92

# id: (x, y, title, line2, line3, kind)   kind: src | azure | out | offline
NODES = {
    "csv":   (40,  120, "Four Divvy CSV files", "riders, payments, stations, trips", "no header row; ~4.6M trips", "src"),
    "vol":   (310, 120, "Unity Catalog volume", "/Volumes/.../raw", "CSV files are never modified", "azure"),
    "ing":   (580, 120, "PySpark ingestion", "explicit string schema + try_cast", "parse failures counted", "azure"),
    "bfile": (850, 120, "Bronze Delta files", "volume /bronze_delta/<entity>", "overwrite, row counts checked", "azure"),
    "btab":  (1120, 120, "Bronze Delta tables", "divvy_bronze.* (managed)", "CREATE + INSERT OVERWRITE", "azure"),
    "xf":    (1120, 305, "PySpark transformations", "dedupe, surrogate keys, joins", "age, duration_quality, date keys", "azure"),
    "gold":  (850, 305, "Gold Delta star schema", "divvy_gold: 3 dims + 2 facts", "saveAsTable, mode overwrite", "azure"),
    "sql":   (580, 250, "Spark SQL analytics", "ride time, stations, payments", "spend vs riding", "azure"),
    "val":   (580, 360, "Data validation", "assertions + diagnostics", "counts, keys, amounts, rebuild", "azure"),
    "exp":   (580, 545, "Local rebuild + export", "same code on Spark + Delta, laptop", "summary tables only", "offline"),
    "dash":  (310, 545, "Static dashboard", "dashboard/index.html", "data embedded, no backend", "offline"),
    "pages": (40,  545, "GitHub Pages", "free static hosting", "status: see README", "offline"),
}
EDGES = [("csv", "vol"), ("vol", "ing"), ("ing", "bfile"), ("bfile", "btab"), ("btab", "xf"), ("xf", "gold"),
         ("gold", "sql"), ("gold", "val"), ("gold", "exp", True, "same code, rebuilt locally"), ("exp", "dash"), ("dash", "pages")]
STYLE = {  # fill, stroke
    "src": ("#F1F5F9", "#64748B"), "azure": ("#E6F0FB", "#1F6FB5"),
    "offline": ("#FFF4E0", "#C77700"), "out": ("#E8F6EE", "#2E8B57")}
GROUPS = [  # label, x, y, w, h, stroke, dashed
    ("Azure Databricks workspace  -  Unity Catalog", 285, 70, 1180, 415, "#1F6FB5", False),
    ("Offline on a local machine (not connected to the workspace)", 15, 505, 810, 170, "#C77700", True),
]


EDGES = [e if len(e) == 4 else (e[0], e[1], False, "") for e in EDGES]


def anchor(a, b):
    ax, ay = NODES[a][0], NODES[a][1]
    bx, by = NODES[b][0], NODES[b][1]
    if abs(ay - by) < 5:                        # same row
        if bx > ax:
            return (ax + BW, ay + BH / 2, bx, by + BH / 2)
        return (ax, ay + BH / 2, bx + BW, by + BH / 2)
    if abs(ax - bx) < 5:                        # same column
        return (ax + BW / 2, ay + BH, bx + BW / 2, by) if by > ay else (ax + BW / 2, ay, bx + BW / 2, by + BH)
    if by > ay + BH:                            # gold -> offline export (leaves the workspace downwards)
        return (ax + BW / 2, ay + BH, bx + BW, by + BH / 2)
    return (ax, ay + BH / 2, bx + BW, by + BH / 2)   # gold -> targets on its left


def svg():
    o = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" font-family="Helvetica, Arial, sans-serif">',
         '<title>Divvy lakehouse architecture</title>',
         '<desc>Four CSV files in a Unity Catalog volume are ingested with PySpark into Bronze Delta files and managed Bronze Delta tables, transformed into a Gold Delta star schema, then analysed with Spark SQL and validated. A local export of Gold summary tables feeds a static dashboard on GitHub Pages.</desc>',
         '<defs><marker id="a" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#475569"/></marker></defs>',
         f'<rect width="{W}" height="{H}" fill="#FFFFFF"/>',
         f'<text x="40" y="44" font-size="26" font-weight="700" fill="#0F172A">Divvy Bike-Share lakehouse: Bronze to Gold</text>']
    for label, x, y, w, h, st, dash in GROUPS:
        o.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="14" fill="none" stroke="{st}" stroke-width="2"'
                 + (' stroke-dasharray="8 6"' if dash else "") + "/>")
        o.append(f'<text x="{x + 16}" y="{y + 26}" font-size="15" font-weight="700" fill="{st}">{escape(label)}</text>')
    for a, b, dashed, label in EDGES:
        x1, y1, x2, y2 = anchor(a, b)
        o.append(f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="#475569" stroke-width="2.2" marker-end="url(#a)"'
                 + (' stroke-dasharray="7 5"' if dashed else "") + "/>")
        if label:
            o.append(f'<text x="{(x1 + x2) / 2 + 14}" y="{(y1 + y2) / 2}" font-size="13" font-style="italic" fill="#475569">{escape(label)}</text>')
    for k, (x, y, t, l2, l3, kind) in NODES.items():
        f, s = STYLE[kind]
        o.append(f'<rect x="{x}" y="{y}" width="{BW}" height="{BH}" rx="10" fill="{f}" stroke="{s}" stroke-width="2"/>')
        o.append(f'<text x="{x + BW / 2}" y="{y + 30}" font-size="16" font-weight="700" text-anchor="middle" fill="#0F172A">{escape(t)}</text>')
        o.append(f'<text x="{x + BW / 2}" y="{y + 54}" font-size="13" text-anchor="middle" fill="#334155">{escape(l2)}</text>')
        o.append(f'<text x="{x + BW / 2}" y="{y + 74}" font-size="12" text-anchor="middle" fill="#64748B">{escape(l3)}</text>')
    o.append(f'<text x="40" y="725" font-size="14" fill="#334155">There is no separate Silver layer: typing and quality checks happen during Bronze ingestion, and the dimensional transformations produce Gold directly.</text>')
    o.append(f'<text x="40" y="749" font-size="14" fill="#334155">The dashboard is a static snapshot exported from the Gold tables; it does not query Azure Databricks.</text>')
    o.append(f'<g font-size="13" fill="#334155"><rect x="40" y="770" width="18" height="18" rx="4" fill="{STYLE["src"][0]}" stroke="{STYLE["src"][1]}" stroke-width="2"/><text x="66" y="784">Source data</text>'
             f'<rect x="190" y="770" width="18" height="18" rx="4" fill="{STYLE["azure"][0]}" stroke="{STYLE["azure"][1]}" stroke-width="2"/><text x="216" y="784">Runs in Azure Databricks</text>'
             f'<rect x="420" y="770" width="18" height="18" rx="4" fill="{STYLE["offline"][0]}" stroke="{STYLE["offline"][1]}" stroke-width="2"/><text x="446" y="784">Offline / static publishing</text></g>')
    o.append("</svg>")
    return "\n".join(o)


def drawio():
    cells, i = ['<mxCell id="0"/>', '<mxCell id="1" parent="0"/>'], 2
    for label, x, y, w, h, st, dash in GROUPS:
        cells.append(f'<mxCell id="g{i}" value="{escape(label)}" style="rounded=1;whiteSpace=wrap;html=1;fillColor=none;strokeColor={st};strokeWidth=2;'
                     f'{"dashed=1;" if dash else ""}verticalAlign=top;align=left;spacingLeft=14;fontStyle=1;fontColor={st};" vertex="1" parent="1">'
                     f'<mxGeometry x="{x}" y="{y}" width="{w}" height="{h}" as="geometry"/></mxCell>')
        i += 1
    for k, (x, y, t, l2, l3, kind) in NODES.items():
        f, s = STYLE[kind]
        val = f"&lt;b&gt;{escape(t)}&lt;/b&gt;&lt;br&gt;{escape(l2)}&lt;br&gt;&lt;font color=&quot;#64748B&quot;&gt;{escape(l3)}&lt;/font&gt;"
        cells.append(f'<mxCell id="{k}" value="{val}" style="rounded=1;whiteSpace=wrap;html=1;fillColor={f};strokeColor={s};strokeWidth=2;fontColor=#0F172A;" vertex="1" parent="1">'
                     f'<mxGeometry x="{x}" y="{y}" width="{BW}" height="{BH}" as="geometry"/></mxCell>')
    for n, (a, b, dashed, label) in enumerate(EDGES):
        cells.append(f'<mxCell id="e{n}" value="{escape(label)}" style="endArrow=block;html=1;strokeWidth=2;strokeColor=#475569;{"dashed=1;" if dashed else ""}" edge="1" parent="1" source="{a}" target="{b}">'
                     f'<mxGeometry relative="1" as="geometry"/></mxCell>')
    return ('<mxfile host="app.diagrams.net"><diagram name="Architecture" id="arch"><mxGraphModel dx="1500" dy="800" grid="1" gridSize="10" '
            f'page="1" pageWidth="{W}" pageHeight="{H}"><root>' + "".join(cells) + "</root></mxGraphModel></diagram></mxfile>")


if __name__ == "__main__":
    (ROOT / "docs" / "images").mkdir(parents=True, exist_ok=True)
    (ROOT / "docs" / "images" / "architecture.svg").write_text(svg())
    (ROOT / "docs" / "architecture.drawio").write_text(drawio())
    print("wrote docs/images/architecture.svg and docs/architecture.drawio")
