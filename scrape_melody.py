# -*- coding: utf-8 -*-
import json, os, sys
from bs4 import BeautifulSoup
from melody_core import fetch_html, TZ, parse_date_label, fmt_date, estimate_listeners
from datetime import datetime, time as dtime

OUT_PATH = os.environ.get("OUT_PATH", "data/playlist.json")
LIMIT = int(os.environ.get("PLAYLIST_LIMIT", "50000"))

# ---------- helpers for GH Actions ----------
def gha_summary(title: str, added: int, total: int, dropped: int):
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not path: return
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"### {title}\n\n")
        f.write(f"- Pridané záznamy: **{added}**\n")
        f.write(f"- Celkom po uložení: **{total}**\n")
        if dropped:
            f.write(f"- Odstránené najstaršie (kvôli limitu {LIMIT}): **{dropped}**\n")
        f.write("\n")

def gha_outputs(added: int, total: int, dropped: int):
    path = os.environ.get("GITHUB_OUTPUT")
    if not path: return
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"added={added}\n")
        f.write(f"total={total}\n")
        f.write(f"dropped_oldest={dropped}\n")

# -------------------------------------------

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
    # uprav selektory podľa aktuálnej stránky, toto je pôvodná verzia:
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
    existing = load_json(OUT_PATH)

    if not items:
        # Nezlyhaj job – len zaloguj
        total_now = len(existing)
        print("::warning title=Scrape Melody::Stránka nevrátila žiadne riadky (No rows).")
        print(f"added 0 items, total {total_now}")
        gha_summary("Scrape Melody – výsledky", added=0, total=total_now, dropped=0)
        gha_outputs(added=0, total=total_now, dropped=0)
        sys.exit(0)

    seen = {key(x) for x in existing}
    to_add = [x for x in items if key(x) not in seen]

    merged = to_add + existing
    # ak by sme prekročili limit, staré odsekneme (na konci)
    dropped = max(0, len(merged) - LIMIT)
    if dropped > 0:
        merged = merged[:LIMIT]

    save_json(OUT_PATH, merged)

    added_cnt = len(to_add)
    total_cnt = len(merged)

    # klasický výpis do logu
    print(f"added {added_cnt} items, total {total_cnt} (dropped_oldest {dropped})")
    # pekná bublina v Actions UI
    print(f"::notice title=Scrape Melody::Pridané {added_cnt}, spolu {total_cnt}, zmazaných najstarších {dropped}")

    # Summary + outputs pre ďalšie kroky
    gha_summary("Scrape Melody – výsledky", added=added_cnt, total=total_cnt, dropped=dropped)
    gha_outputs(added=added_cnt, total=total_cnt, dropped=dropped)
