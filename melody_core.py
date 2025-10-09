# -*- coding: utf-8 -*-
import re
import random
from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

TZ = ZoneInfo("Europe/Bratislava")
URL = "https://www.radia.sk/radia/melody/playlist"
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/129.0 Safari/537.36"
)

def _fetch_with_requests():
    headers = {"User-Agent": UA, "Accept-Language": "sk,en;q=0.9"}
    r = requests.get(URL, headers=headers, timeout=20)
    r.raise_for_status()
    return r.text

def fetch_html():
    """Skúsi requests; ak by server kládol odpor, a je dostupný cloudscraper, použije ho."""
    try:
        return _fetch_with_requests()
    except Exception:
        try:
            import cloudscraper  # optional
            s = cloudscraper.create_scraper()
            return s.get(URL, headers={"User-Agent": UA}).text
        except Exception as e:
            raise e

def parse_date_label(lbl: str) -> date:
    t = lbl.strip().lower()
    today = datetime.now(TZ).date()
    if t.startswith("dnes"):
        return today
    if t.startswith("včera") or t.startswith("vcera"):
        return today - timedelta(days=1)
    m = re.search(r"(\d{2}\.\d{2}\.\d{4})", t)
    if m:
        return datetime.strptime(m.group(1), "%d.%m.%Y").date()
    return today

def fmt_date(d: date) -> str:
    return d.strftime("%d.%m.%Y")

def _time_weight(h: int) -> float:
    if 6 <= h < 9:   return 2.4   # ranná špička
    if 9 <= h < 12:  return 1.6
    if 12 <= h < 14: return 1.8   # obed
    if 14 <= h < 17: return 2.1
    if 17 <= h < 19: return 2.6   # popoludňajšia špička
    if 19 <= h < 22:return 1.7
    if 22 <= h or h < 6: return 0.5
    return 1.0

def _dow_weight(dow: int) -> float:
    return 0.85 if dow in (5, 6) else 1.0  # víkend mierne nižší

def estimate_listeners(dt: datetime) -> int:
    BASE = 9000
    w = _time_weight(dt.hour) * _dow_weight(dt.weekday())
    noise = random.uniform(0.85, 1.15)
    val = int(max(200, BASE * w * noise))
    return min(val, 38000)

def parse_first_row(html: str) -> dict | None:
    soup = BeautifulSoup(html, "html.parser")
    # najnovší riadok je prvý
    row = soup.select_one("div.row.data, div.row_data")
    if not row:
        return None

    d_el = row.select_one(".datum")
    t_el = row.select_one(".cas")
    a_el = row.select_one(".interpret")
    s_el = row.select_one(".titul")
    if not all([d_el, t_el, a_el, s_el]):
        return None

    d = parse_date_label(d_el.get_text())
    hh, mm = [int(x) for x in t_el.get_text().strip().split(":")]
    tm = time(hour=hh, minute=mm)
    dt = datetime.combine(d, tm, TZ)

    return {
        "title": s_el.get_text(strip=True),
        "artist": a_el.get_text(strip=True),
        "date": fmt_date(d),
        "time": tm.strftime("%H:%M"),
        "listeners": estimate_listeners(dt),
    }

def get_now_playing() -> dict:
    html = fetch_html()
    row = parse_first_row(html)
    if not row:
        return {"error": "Nepodarilo sa získať aktuálnu skladbu."}
    return row
