# -*- coding: utf-8 -*-
import json, os
from bs4 import BeautifulSoup
from datetime import datetime, time as dtime

from melody_core import (
    fetch_html, TZ, parse_date_label, fmt_date, estimate_listeners
)

OUT_PATH = os.environ.get("OUT_PATH", "data/playlist.json")
PLAYLIST_LIMIT = int(os.environ.get("PLAYLIST_LIMIT", "50000"))

def load_json(path: str):
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []

def save_json(path: str, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def key(x):  # unikátny kľúč záznamu
    return (x["date"], x["time"], x["artist"], x["title"])

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
        if not all([d_el, t_el, a_el, s_el]):
            continue

        d = parse_date_label(d_el.get_text())
        hh, mm = [int(x) for x in t_el.get_text().strip().split(":")]
        tm = dtime(hour=hh, minute=mm)
        dt = datetime.combine(d, tm, TZ)

        title = s_el.get_text(strip=True)
        artist = a_el.get_text(strip=True)
        time_str = tm.strftime("%H:%M")

        # deterministický seed pre scraper (stabilné pri re-scrape)
        seed = f"scrape|{fmt_date(d)}|{time_str}|{artist}|{title}"

        out.append({
            "title": title,
            "artist": artist,
            "date": fmt_date(d),     # dd.mm.yyyy
            "time": time_str,        # HH:MM
            # Posielame seed_key (stabilnejšie); ak by sa zabudol, estimate_listeners fallbackne.
            "listeners": int(estimate_listeners(dt, seed_key=seed)),
        })
    return out

if __name__ == "__main__":
    items = scrape_page()
    if not items:
        raise SystemExit("No rows.")

    existing = load_json(OUT_PATH)
    seen = {key(x) for x in existing}
    to_add = [x for x in items if key(x) not in seen]
    merged = to_add + existing
    if PLAYLIST_LIMIT:
        merged = merged[:PLAYLIST_LIMIT]
    save_json(OUT_PATH, merged)

    # výstupy pre GitHub Actions
    added = len(to_add)
    total = len(merged)
    dropped_oldest = max(0, len(to_add) + len(existing) - total)

    gh_out = os.environ.get("GITHUB_OUTPUT")
    if gh_out:
        with open(gh_out, "a", encoding="utf-8") as fh:
            fh.write(f"added={added}\n")
            fh.write(f"total={total}\n")
            fh.write(f"dropped_oldest={dropped_oldest}\n")

    print(f"added {added} items, total {total}, dropped_oldest {dropped_oldest}")
