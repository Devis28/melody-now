# -*- coding: utf-8 -*-
import re
import hashlib
from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo
import math
import os

import requests
from bs4 import BeautifulSoup

# ----------------- Konštanty a nastavenia -----------------------------------
TZ = ZoneInfo("Europe/Bratislava")
URL = "https://www.radia.sk/radia/melody/playlist"
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/129.0 Safari/537.36"
)

# Počúvanosť – rovnaký model ako v backfill_listeners.py
WEEKDAY_PEAK   = float(os.environ.get("WEEKDAY_PEAK", 3260))   # špička pracovného dňa
WEEKEND_PEAK   = float(os.environ.get("WEEKEND_PEAK", 1820))   # špička víkendu

# Noc – minimum a tvar (cirkulárny Gauss s wrap-around okolo polnoci)
NIGHT_MIN      = float(os.environ.get("NIGHT_MIN", 120))       # minimum v hlbokej noci
NIGHT_CENTER   = float(os.environ.get("NIGHT_CENTER", 2.5))    # hodina minima (~02:30)
NIGHT_WIDTH    = float(os.environ.get("NIGHT_WIDTH", 3.0))     # šírka noci (↑ = dlhšia noc)
NIGHT_STRENGTH = float(os.environ.get("NIGHT_STRENGTH", 0.96)) # sila tlmenia 0..1
NIGHT_POWER    = float(os.environ.get("NIGHT_POWER", 1.6))     # prudkosť (>1 = ostrejšie)

# Večerné do-tlmenie pred polnocou
EVENING_TAIL_START     = float(os.environ.get("EVENING_TAIL_START", 22.0))
EVENING_TAIL_STRENGTH  = float(os.environ.get("EVENING_TAIL_STRENGTH", 0.35))
EVENING_TAIL_SLOPE     = float(os.environ.get("EVENING_TAIL_SLOPE", 0.6))

# Jitter (deterministický, podľa seed_key)
JITTER_SIGMA   = float(os.environ.get("JITTER_SIGMA", 0.06))   # ~6 %
JITTER_CLIP    = float(os.environ.get("JITTER_CLIP", 0.12))    # max ±12 %

# ----------------- Fetch HTML ------------------------------------------------
def _fetch_with_requests():
    headers = {"User-Agent": UA, "Accept-Language": "sk,en;q=0.9"}
    r = requests.get(URL, headers=headers, timeout=20)
    r.raise_for_status()
    return r.text

def fetch_html():
    """Skúsi requests; ak by server kládol odpor a je dostupný cloudscraper, použije ho."""
    try:
        return _fetch_with_requests()
    except Exception:
        try:
            import cloudscraper  # optional
            s = cloudscraper.create_scraper()
            return s.get(URL, headers={"User-Agent": UA}).text
        except Exception as e:
            raise e

# ----------------- Dátumy/časy ----------------------------------------------
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

# ----------------- Tvar dennej krivky (ako v backfilli) ----------------------
def _gauss(x, mu, sigma, amp):
    return amp * math.exp(-0.5 * ((x - mu) / sigma) ** 2)

def _shape_weekday_raw(h: float) -> float:
    # ráno + popoludnie, menšie poludnie a večer
    return (
        _gauss(h, 7.8, 1.2, 0.9)  +
        _gauss(h, 12.5, 1.3, 0.45) +
        _gauss(h, 17.3, 1.3, 0.85) +
        _gauss(h, 20.5, 1.8, 0.35)
    )

def _shape_weekend_raw(h: float) -> float:
    # neskorší štart, popoludnie dominantné
    return (
        _gauss(h, 10.0, 1.7, 0.35) +
        _gauss(h, 14.0, 2.0, 0.95) +
        _gauss(h, 19.5, 2.0, 0.55)
    )

def _normalize(arr):
    a_min, a_max = min(arr), max(arr)
    if a_max <= a_min: return [0.0 for _ in arr]
    return [(v - a_min) / (a_max - a_min) for v in arr]

def _circ_dist_hours(a: float, b: float) -> float:
    """cirkulárna vzdialenosť na 24h cykle"""
    d = abs(a - b)
    return min(d, 24.0 - d)

def _sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))

def _night_depressor(h: float) -> float:
    # Cirkulárny Gauss okolo NIGHT_CENTER (účinný aj pre 23:xx)
    valley = math.exp(-0.5 * (_circ_dist_hours(h, NIGHT_CENTER) / max(0.1, NIGHT_WIDTH)) ** 2)
    dep1 = 1.0 - NIGHT_STRENGTH * valley
    dep1 = max(0.0, min(1.0, dep1)) ** NIGHT_POWER

    # Večerný chvost – tlmí už od ~22:00 smerom k polnoci
    tail = _sigmoid((h - EVENING_TAIL_START) / max(0.1, EVENING_TAIL_SLOPE))  # ~0 pred, ~1 po
    dep2 = 1.0 - EVENING_TAIL_STRENGTH * tail

    return max(0.0, min(1.0, dep1 * dep2))

def _precompute_norm(is_weekend: bool):
    grid = [i/12 for i in range(0, 24*12 + 1)]  # 5-min mriežka v hodinách
    raw = [(_shape_weekend_raw(x) if is_weekend else _shape_weekday_raw(x)) for x in grid]
    return grid, _normalize(raw)

def _s01(h: float, is_weekend: bool) -> float:
    """0..1 tvar po aplikácii nočného/večerného tlmenia (bez re-normalizácie)"""
    if not hasattr(_s01, "_cache"):
        _s01._cache = {}
    key = "we" if is_weekend else "wd"
    if key not in _s01._cache:
        _s01._cache[key] = _precompute_norm(is_weekend)
    grid, day_norm = _s01._cache[key]
    idx = min(range(len(grid)), key=lambda i: abs(grid[i] - h))
    return day_norm[idx] * _night_depressor(h)

def _expected_count(dt: datetime) -> float:
    h = dt.hour + dt.minute / 60.0
    is_weekend = dt.weekday() >= 5
    s = _s01(h, is_weekend)
    peak = WEEKEND_PEAK if is_weekend else WEEKDAY_PEAK
    return NIGHT_MIN + s * (peak - NIGHT_MIN)

def _deterministic_jitter(seed_key: str | None, sigma=JITTER_SIGMA, clip=JITTER_CLIP) -> float:
    """
    Vráti multiplikátor (1 ± pár %) deterministicky podľa seed_key.
    Ak seed_key nie je, použije časový seed (YYYY-mm-dd HH:MM).
    """
    if not seed_key:
        # fallback na čas, aby sa pri scrape/APi správalo stabilne v minútach
        seed_key = datetime.now(TZ).strftime("%Y-%m-%d %H:%M")
    d = hashlib.md5(seed_key.encode("utf-8")).hexdigest()
    # jednoduchý deterministický hash -> ~N(0, sigma) cez Box-Muller
    a = int(d[:16], 16); b = int(d[16:32], 16)
    u1 = ((a % 10_000_000) + 0.5) / 10_000_001.0
    u2 = ((b % 10_000_000) + 0.5) / 10_000_001.0
    z = ( (-2.0 * math.log(max(u1, 1e-9))) ** 0.5 ) * math.cos(2*math.pi*max(u2, 1e-9))
    eps = max(-clip, min(clip, sigma * z))
    return 1.0 + eps

def estimate_listeners(dt: datetime, seed_key: str | None = None) -> int:
    """
    Odhad počúvanosti pre daný čas.
    seed_key odporúčam nastaviť na 'artist|title|date|time' pre stabilný jitter.
    """
    base = _expected_count(dt)
    v = base * _deterministic_jitter(seed_key)
    peak_cap = WEEKEND_PEAK if dt.weekday() >= 5 else WEEKDAY_PEAK
    v = max(NIGHT_MIN, min(peak_cap, v))
    return int(round(v))

# ----------------- Parsovanie stránky ---------------------------------------
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

    title = s_el.get_text(strip=True)
    artist = a_el.get_text(strip=True)
    seed = f"{artist}|{title}|{fmt_date(d)}|{tm.strftime('%H:%M')}"

    return {
        "title": title,
        "artist": artist,
        "date": fmt_date(d),
        "time": tm.strftime("%H:%M"),
        "listeners": estimate_listeners(dt, seed_key=seed),
    }

def get_now_playing() -> dict:
    html = fetch_html()
    row = parse_first_row(html)
    if not row:
        return {"error": "Nepodarilo sa získať aktuálnu skladbu."}
    return row
