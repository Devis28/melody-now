# -*- coding: utf-8 -*-
import json, os
from bs4 import BeautifulSoup
from melody_core import fetch_html, TZ, parse_date_label, fmt_date, estimate_listeners
from datetime import datetime, time as dtime

OUT_PATH = os.environ.get("OUT_PATH", "data/playlist.json")

def load_json(path: str):
    if not os.path.exists(path): return []
    try:
        with open(path, "r", encoding="utf-8") as f: return json.load(f)
    except Exception:
        return []

def save_json(path: str, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def key(x): return (x["date"], x["time"], x["artist"], x["title"])

def scrape_page():
    html = fetch_html()
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("div.row.data, div.row_data")
    out = []
    for row in rows:
        d_el = row.select_one(".datum")
        t_el = row.select_one(".cas")
        a_el = row.select_one(".interpret")
        s_el = row.select_one(".titul")
        if not all([d_el, t_el, a_el, s_el]): continue
        d = parse_date_label(d_el.get_text())
        hh, mm = [int(x) for x in t_el.get_text().strip().split(":")]
        tm = dtime(hour=hh, minute=mm)
        dt = datetime.combine(d, tm, TZ)
        out.append({
            "title": s_el.get_text(strip=True),
            "artist": a_el.get_text(strip=True),
            "date": fmt_date(d),            # dd.mm.yyyy
            "time": tm.strftime("%H:%M"),
            "listeners": estimate_listeners(dt)
        })
    return out

if __name__ == "__main__":
    items = scrape_page()
    if not items: raise SystemExit("No rows.")
    existing = load_json(OUT_PATH)
    seen = {key(x) for x in existing}
    to_add = [x for x in items if key(x) not in seen]
    merged = to_add + existing
    save_json(OUT_PATH, merged[:50000])  # limit histórie
    print(f"added {len(to_add)} items, total {len(merged)}")
