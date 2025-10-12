# -*- coding: utf-8 -*-
"""
Scraper playlistu: vytiahne položky zo stránky a uloží ich do data/playlist.json.
Každý záznam obsahuje aj 'station' (názov rádia).
"""

from __future__ import annotations
import os, json, time, sys
from datetime import datetime, time as dtime

from bs4 import BeautifulSoup

from melody_core import (
    TZ, fetch_html, parse_date_label, fmt_date,
    estimate_listeners, parse_station_name
)

OUT_PATH = os.environ.get("OUT_PATH", "data/playlist.json")
LIMIT    = int(os.environ.get("PLAYLIST_LIMIT", "50000"))

# ----------------------------------------------------------------------------
# I/O
# ----------------------------------------------------------------------------
def load_json(path: str):
    if not os.path.exists(path): return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []

def save_json(path: str, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def key(x: dict) -> tuple:
    # unikátnosť podľa (date,time,artist,title)
    return (x.get("date"), x.get("time"), x.get("artist"), x.get("title"))

# ----------------------------------------------------------------------------
# HTML fetch s retry/backoff
# ----------------------------------------------------------------------------
def fetch_html_with_retry(tries: int = 4) -> str | None:
    last = None
    for i in range(tries):
        try:
            return fetch_html()
        except Exception as e:
            last = e
            sleep = 4 * (2 ** i)
            print(f"[warn] fetch_html failed try {i+1}/{tries}: {type(e).__name__}: {e}; sleep {sleep}s")
            time.sleep(sleep)
    print(f"[error] giving up fetch_html: {last}")
    return None

# ----------------------------------------------------------------------------
# Scrape
# ----------------------------------------------------------------------------
def scrape_page() -> list[dict]:
    html = fetch_html_with_retry()
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("div.row.data, div.row_data")
    if not rows:
        return []

    station = parse_station_name(html) or "Rádio Melody"

    out: list[dict] = []
    for row in rows:
        d_el = row.select_one(".datum, .play_datum, .pl_datum")
        t_el = row.select_one(".cas, .play_cas, .pl_cas")
        a_el = row.select_one(".interpret, .play_interpret, .pl_interpret")
        s_el = row.select_one(".titul, .play_titul, .pl_titul")

        if not all([d_el, t_el, a_el, s_el]):
            continue

        d = parse_date_label(d_el.get_text())
        hhmm = t_el.get_text().strip()
        try:
            hh, mm = [int(x) for x in hhmm.split(":")[:2]]
        except Exception:
            continue

        tm = dtime(hour=hh, minute=mm)
        dt = datetime.combine(d, tm, TZ)

        out.append({
            "station":    station,
            "title":      s_el.get_text(strip=True),
            "artist":     a_el.get_text(strip=True),
            "date":       fmt_date(d),
            "time":       tm.strftime("%H:%M"),
            "listeners":  estimate_listeners(dt, seed_key=f"{fmt_date(d)} {tm:%H:%M}"),
        })

    return out

# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
if __name__ == "__main__":
    existing = load_json(OUT_PATH)

    try:
        items = scrape_page()
    except Exception as e:
        print(f"[error] scrape failed hard: {type(e).__name__}: {e}")
        items = []

    if not items:
        print(f"added 0 items, total {len(existing)}")
        sys.exit(0)

    seen = {key(x) for x in existing}
    to_add = [x for x in items if key(x) not in seen]

    merged = to_add + existing
    if LIMIT > 0:
        merged = merged[:LIMIT]

    save_json(OUT_PATH, merged)
    print(f"added {len(to_add)} items, total {len(merged)}")
