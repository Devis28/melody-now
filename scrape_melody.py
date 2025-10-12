# -*- coding: utf-8 -*-
import json, os, sys, time
from collections import OrderedDict
from bs4 import BeautifulSoup
from datetime import datetime, time as dtime

from melody_core import (
    fetch_html, TZ, parse_date_label, fmt_date, estimate_listeners
)

OUT_PATH = os.environ.get("OUT_PATH", "data/playlist.json")
LIMIT = int(os.environ.get("PLAYLIST_LIMIT", "50000"))
STATION_FALLBACK = os.environ.get("STATION_NAME", "Rádio Melody")


def load_json(path):
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []


def save_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def key(x):
    # kľúč nechávame bez "station" (je konštantná pre dané rádio)
    return (x["date"], x["time"], x["artist"], x["title"])


def _extract_station_name(soup: BeautifulSoup) -> str:
    # 1) <h1 class="radio_nazov">Rádio Melody</h1>
    h1 = soup.select_one("h1.radio_nazov")
    if h1:
        name = h1.get_text(strip=True)
        if name:
            return name
    # 2) fallback cez alt na logu (ak by raz chýbal h1)
    img = soup.select_one("img[alt]")
    if img:
        alt = (img.get("alt") or "").strip()
        if alt:
            return alt
    # 3) posledná poistka – env / default
    return STATION_FALLBACK


def scrape_page():
    html = fetch_html_with_retry()
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    station_name = _extract_station_name(soup)

    rows = soup.select("div.row.data, div.row_data")
    out = []

    for row in rows:
        d_el = row.select_one(".datum")
        t_el = row.select_one(".cas")
        a_el = row.select_one(".interpret")
        s_el = row.select_one(".titul")
        if not all([d_el, t_el, a_el, s_el]):
            continue

        d = parse_date_label(d_el.get_text())
        hh, mm = [int(x) for x in t_el.get_text().strip().split(":")]
        tm = dtime(hour=hh, minute=mm)
        dt = datetime.combine(d, tm, TZ)

        # Zostav položku v poradí s 'station' ako prvým kľúčom
        item = OrderedDict()
        item["station"]   = station_name
        item["title"]     = s_el.get_text(strip=True)
        item["artist"]    = a_el.get_text(strip=True)
        item["date"]      = fmt_date(d)          # dd.mm.yyyy
        item["time"]      = tm.strftime("%H:%M")
        item["listeners"] = estimate_listeners(
            dt, seed_key=f"{fmt_date(d)} {tm:%H:%M}"
        )

        out.append(item)

    return out


def fetch_html_with_retry():
    last = None
    for i in range(4):  # 4 pokusy s rastúcim spánkom
        try:
            return fetch_html()
        except Exception as e:
            last = e
            sleep = 4 * (2 ** i)
            print(
                f"[warn] fetch_html failed try {i+1}/4: "
                f"{type(e).__name__}: {e}; sleep {sleep}s"
            )
            time.sleep(sleep)
    print(f"[error] giving up fetch_html: {last}")
    return None


if __name__ == "__main__":
    existing = load_json(OUT_PATH)
    try:
        items = scrape_page()
    except Exception as e:
        # extra poistka – nikdy nepadni s non-zero
        print(f"[error] scrape failed hard: {type(e).__name__}: {e}")
        items = []

    if not items:
        # nič nenačítané – nenútime fail jobu
        print(f"added 0 items, total {len(existing)}")
        sys.exit(0)

    seen = {key(x) for x in existing}
    to_add = [x for x in items if key(x) not in seen]
    merged = to_add + existing
    if LIMIT > 0:
        merged = merged[:LIMIT]

    save_json(OUT_PATH, merged)
    print(f"added {len(to_add)} items, total {len(merged)}")
