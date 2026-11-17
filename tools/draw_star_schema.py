"""Draws the star-schema diagram (vector PDF) with reportlab.
Run: python tools/draw_star_schema.py [out.pdf]   (default: docs/Divvy_Star_Schema.pdf)"""
import sys
from reportlab.pdfgen import canvas
from reportlab.lib.colors import HexColor, white, black

from pathlib import Path
OUT = sys.argv[1] if len(sys.argv) > 1 else str(Path(__file__).resolve().parents[1] / "docs" / "Divvy_Star_Schema.pdf")
W, H = 2060, 1100
ROW, HEAD = 26, 40
FACT, DIM = HexColor("#1f4e8c"), HexColor("#2e7d5b")
PK, FK = HexColor("#fff4cc"), HexColor("#e6efff")

# (name, type, key) key in {"PK","FK",""}
TABLES = {
 "dim_station": dict(x=40, ytop=690, w=320, kind="dim", rows=[
   ("station_key","INT","PK"),("station_id","STRING",""),("station_name","STRING",""),
   ("latitude","DOUBLE",""),("longitude","DOUBLE","")]),
 "fact_trip": dict(x=600, ytop=900, w=400, kind="fact", rows=[
   ("trip_id","STRING","PK"),("rider_key","INT","FK"),("start_date_key","INT","FK"),
   ("end_date_key","INT","FK"),("start_station_key","INT","FK"),("end_station_key","INT","FK"),
   ("rideable_type","STRING",""),("started_at","TIMESTAMP",""),("ended_at","TIMESTAMP",""),
   ("start_hour","INT",""),("time_band","STRING",""),
   ("ride_duration_seconds","BIGINT",""),("ride_duration_minutes","DOUBLE",""),
   ("duration_quality","STRING",""),("rider_age_at_trip","INT","")]),
 "dim_date": dict(x=1300, ytop=1040, w=330, kind="dim", rows=[
   ("date_key","INT (yyyyMMdd)","PK"),("date","DATE",""),("day_of_week_num","INT (1=Mon)",""),
   ("day_of_week_name","STRING",""),("day_of_month","INT",""),("month","INT",""),
   ("month_name","STRING",""),("quarter","INT",""),("year","INT",""),
   ("year_month","STRING",""),("is_weekend","BOOLEAN","")]),
 "dim_rider": dict(x=1300, ytop=600, w=330, kind="dim", rows=[
   ("rider_key","INT","PK"),("rider_id","INT",""),("birth_date","DATE",""),
   ("account_start_date","DATE",""),("account_end_date","DATE",""),
   ("is_member","BOOLEAN (current snapshot)",""),("age_at_account_start","INT","")]),
 "fact_payment": dict(x=1720, ytop=760, w=330, kind="fact", rows=[
   ("payment_id","INT","PK"),("rider_key","INT","FK"),("payment_date_key","INT","FK"),
   ("payment_date","DATE",""),("amount","DECIMAL(10,2)","")]),
}

c = canvas.Canvas(OUT, pagesize=(W, H))
c.setTitle("Divvy Bike-Share Star Schema")

def box_h(t): return HEAD + ROW*len(t["rows"])

def draw(name, t):
    col = FACT if t["kind"] == "fact" else DIM
    x, yt, w = t["x"], t["ytop"], t["w"]
    h = box_h(t); yb = yt - h
    c.setFillColor(white); c.setStrokeColor(col); c.setLineWidth(2)
    c.rect(x, yb, w, h, fill=1, stroke=1)
    c.setFillColor(col); c.rect(x, yt-HEAD, w, HEAD, fill=1, stroke=0)
    c.setFillColor(white); c.setFont("Helvetica-Bold", 19)
    c.drawCentredString(x+w/2, yt-HEAD+13, name)
    t["rowy"] = {}
    for i, (n, ty, k) in enumerate(t["rows"]):
        ry = yt - HEAD - ROW*(i+1)
        if k:
            c.setFillColor(PK if k == "PK" else FK); c.rect(x+1, ry, w-2, ROW, fill=1, stroke=0)
        c.setFillColor(black); c.setFont("Helvetica-Bold" if k else "Helvetica", 14)
        c.drawString(x+10, ry+8, f"{k}  " if k else "")
        c.setFont("Helvetica-Bold" if k else "Helvetica", 14); c.drawString(x+42, ry+8, n)
        c.setFont("Helvetica", 12); c.setFillColor(HexColor("#555555")); c.drawRightString(x+w-8, ry+8, ty)
        c.setStrokeColor(HexColor("#cccccc")); c.setLineWidth(0.5); c.line(x, ry, x+w, ry)
        t["rowy"][n] = ry + ROW/2
    t["bottom"], t["right"] = yb, x+w

for n, t in TABLES.items(): draw(n, t)

def link(fact, fk, dim, pk, label, style="solid", dy=0, lx=0.5, off2=0):
    f, d = TABLES[fact], TABLES[dim]
    # connect facing edges
    if d["x"] > f["right"]: x1, x2 = f["right"], d["x"]
    elif d["right"] < f["x"]: x1, x2 = f["x"], d["right"]
    else: x1, x2 = f["x"], d["x"]
    y1 = f["rowy"][fk]; y2 = d["rowy"][pk] + off2
    c.setStrokeColor(HexColor("#333333")); c.setLineWidth(1.6)
    if style == "dash": c.setDash(6, 4)
    c.line(x1, y1, x2, y2); c.setDash()
    # cardinality: many (crow's foot) at fact end, one (double bar) at dim end
    sgn = 1 if x2 > x1 else -1
    c.line(x1, y1, x1+sgn*14, y1+9); c.line(x1, y1, x1+sgn*14, y1-9); c.line(x1+sgn*14, y1+9, x1+sgn*14, y1-9)
    for off in (10, 16): c.line(x2-sgn*off, y2+8, x2-sgn*off, y2-8)
    mx = x1 + (x2-x1)*lx; my = y1 + (y2-y1)*lx + dy
    c.setFont("Helvetica-Oblique", 12); c.setFillColor(HexColor("#222222"))
    tw = c.stringWidth(label, "Helvetica-Oblique", 12)
    c.setFillColor(white); c.rect(mx-tw/2-3, my-3, tw+6, 16, fill=1, stroke=0)
    c.setFillColor(HexColor("#222222")); c.drawCentredString(mx, my+1, label)

link("fact_trip","start_station_key","dim_station","station_key","start station (role)", lx=0.28, off2=6)
link("fact_trip","end_station_key","dim_station","station_key","end station (role)", style="dash", lx=0.72, off2=-6)
link("fact_trip","rider_key","dim_rider","rider_key","rider", lx=0.5)
link("fact_trip","start_date_key","dim_date","date_key","start date (role)", lx=0.30, off2=6)
link("fact_trip","end_date_key","dim_date","date_key","end date (role)", style="dash", lx=0.72, off2=-6)
link("fact_payment","rider_key","dim_rider","rider_key","rider", lx=0.5)
link("fact_payment","payment_date_key","dim_date","date_key","payment date", lx=0.5)

# title + notes
c.setFillColor(black); c.setFont("Helvetica-Bold", 30)
c.drawString(40, H-50, "Divvy Bike-Share: Gold Star Schema (Azure Databricks / Delta Lake)")
c.setFont("Helvetica", 15); c.setFillColor(HexColor("#444444"))
c.drawString(40, H-78, "Two fact tables share dim_rider and dim_date. dim_station is role-played twice (start / end) by fact_trip.")
c.setFont("Helvetica", 13)
c.drawString(40, H-100, "Legend:  PK = primary key (yellow)   FK = foreign key (blue)   crow's foot = many (fact side)   double bar = one (dimension side)   dashed = second role of the same dimension")

grains = [
 ("Grain", True),
 ("fact_trip: one row per trip (trip_id).  Measures: ride_duration_seconds / ride_duration_minutes,", False),
 ("   rider_age_at_trip (completed years at trip start). duration_quality: OK (0 < s <= 24h) / NEGATIVE / ZERO / OVER_24H / MISSING.", False),
 ("fact_payment: one row per payment (payment_id).  Measure: amount.", False),
 ("dim_rider: one row per rider.   dim_station: one row per station.   dim_date: one row per calendar date.", False),
 ("Notes", True),
 ("* Surrogate keys (rider_key, station_key) = dense rank of the business ID; date_key = yyyyMMdd.", False),
 ("* Station / rider keys are NULL in the fact if the business ID has no dimension row (left joins; no rows dropped).", False),
 ("* is_member is the current account flag in the source, not a historical membership status.", False),
 ("* Payments are not linked to individual trips in the source; the two facts relate only via rider and date.", False),
 ("* Trip and payment dates cover different periods; dim_date spans the min..max of both.", False),
]
y = 330
for txt, head in grains:
    c.setFillColor(black); c.setFont("Helvetica-Bold" if head else "Helvetica", 17 if head else 14)
    c.drawString(1090 if False else 40, y, txt); y -= 24 if not head else 28
c.showPage(); c.save()
print("wrote", OUT)
