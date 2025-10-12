# -*- coding: utf-8 -*-
"""
Jadro: načítanie HTML z radia.sk, parsovanie práve hranej skladby a
odhad poslucháčov. Doplnené je pole 'station' (názov rádia).
"""

from __future__ import annotations
import os, re, math, hashlib, random
from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Konštanty a HTTP
# ---------------------------------------------------------------------------
TZ  = ZoneInfo("Europe/Bratislava")
URL = "https://www.radia.sk/radia/melody/playlist"
UA  = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/129.0 Safari/537.36")

# parametre odhadu (možné prebiť ENV premennými)
SLOW_BUCKET_S = int(os.environ.get("SLOW_BUCKET_S", "30"))   # bucket pre "rýchly" jitter
WEEKDAY_PEAK  = float(os.environ.get("WEEKDAY_PEAK", "3200"))  # denný špičkový dopyt
WEEKEND_PEAK  = float(os.environ.get("WEEKEND_PEAK", "2000"))
NIGHT_MIN     = float(os.environ.get("NIGHT_MIN",  "180"))

SLOW_SIGMA = float(os.environ.get("SLOW_SIGMA", "0.04"))  # ~pomalejší kmit
SLOW_CLIP  = float(os.environ.get("SLOW_CLIP",  "0.08"))
FAST_SIGMA = float(os.environ.get("FAST_SIGMA", "0.02"))  # ~rýchly kmit okolo základnej hodnoty
FAST_CLIP  = float(os.environ.get("FAST_CLIP",  "0.04"))

def _fetch_with_requests() -> str:
    r = requests.get(URL, headers={"User-Agent": UA, "Accept-Language": "sk,en;q=0.9"}, timeout=20)
    r.raise_for_status()
    return r.text

def fetch_html() -> str:
    """
    Načíta HTML – najprv requests, pri problémoch fallback na cloudscraper.
    """
    try:
        return _fetch_with_requests()
    except Exception:
        try:
            import cloudscraper  # type: ignore
            s = cloudscraper.create_scraper()
            r = s.get(URL, headers={"User-Agent": UA}, timeout=20)
            r.raise_for_status()
            return r.text
        except Exception as e:
            raise e

# ---------------------------------------------------------------------------
# Pomocné: dátumy a formátovanie
# ---------------------------------------------------------------------------
def fmt_date(d: date) -> str:
    return d.strftime("%d.%m.%Y")

def parse_date_label(label: str) -> date:
    """
    Na radia.sk býva 'Dnes', 'Včera' alebo priamo '12.10.2025'.
    """
    s = (label or "").strip().lower()
    today = datetime.now(TZ).date()
    if s.startswith("dnes"):
        return today
    if s.startswith("včera") or s.startswith("vcera"):
        return today - timedelta(days=1)

    m = re.search(r"(\d{1,2})\.(\d{1,2})(?:\.(\d{2,4}))?", s)
    if m:
        dd, mm, yy = int(m.group(1)), int(m.group(2)), m.group(3)
        year = today.year if yy is None else int(yy) + (2000 if len(yy) == 2 else 0)
        return date(year, mm, dd)

    # fallback – ak nič nepasuje, vráť dnes
    return today

# ---------------------------------------------------------------------------
# Pomocné: deterministický šum (jitter)
# ---------------------------------------------------------------------------
def _rng_from_key(key: str) -> random.Random:
    h = hashlib.sha256(key.encode("utf-8")).hexdigest()
    seed = int(h[:16], 16)
    return random.Random(seed)

def _clipped_gauss(rng: random.Random, sigma: float, clip: float) -> float:
    """
    0-mean normálne rozdelenie so sigma a orezaním na +/- clip.
    """
    x = rng.gauss(0.0, sigma)
    return max(-clip, min(clip, x))

# ---------------------------------------------------------------------------
# Názov stanice z HTML
# ---------------------------------------------------------------------------
def parse_station_name(html: str) -> str | None:
    """
    1) <h1 class="radio_nazov">…</h1>
    2) alt/title z <img class="radio_logo_obrazok">
    3) <title>…</title> (oreže suffixy za pomlčkou)
    """
    soup = BeautifulSoup(html, "html.parser")

    el = soup.select_one("h1.radio_nazov, .radio_nazov")
    if el:
        return re.sub(r"\s+", " ", el.get_text(strip=True))

    logo = soup.select_one("img.radio_logo_obrazok")
    if logo:
        alt = (logo.get("alt") or logo.get("title") or "").strip()
        if alt:
            return alt

    if soup.title and soup.title.string:
        t = soup.title.string.strip()
        m = re.match(r"^(.*?)(?:\s*[-–|].*)?$", t)
        base = (m.group(1) if m else t).strip()
        return base or None

    return None

# ---------------------------------------------------------------------------
# Denná krivka a odhad poslucháčov
# ---------------------------------------------------------------------------
def _day_curve_base(dt: datetime) -> float:
    """
    Hladká denná krivka (suma dvoch gaussov) – vracia 0..1 (relatívne).
    Špičky okolo 9:30 a 16:00; v noci nízke hodnoty.
    """
    h = dt.hour + dt.minute / 60.0
    # dve "kopce" počas dňa
    m1 = math.exp(-((h - 9.5) / 2.2) ** 2)
    m2 = math.exp(-((h - 16.0) / 2.8) ** 2)
    base = max(m1, m2)  # brúsime vyšší z dvoch "kopcov"
    # v noci to padá – jemná penalizácia medzi 0:00–5:00
    if 0 <= h < 5:
        base *= 0.25 + 0.15 * (h / 5.0)
    return max(0.0, min(1.0, base))

def estimate_listeners(
    dt: datetime,
    *,
    seed_key: str = "",
    ts_ms: int | None = None,
    debug: bool = False
) -> int | dict:
    """
    Deterministický odhad poslucháčov podľa času + jemný šum.

    - víkend má nižšiu špičku
    - NIGHT_MIN je spodok
    - WEEKDAY_PEAK/WEEKEND_PEAK je horná hranica
    - *pomalejší* jitter viazaný na 'seed_key' (skladba/dátum/čas)
    - *rýchly* jitter viazaný na 'ts_ms' (alebo bucket so SLOW_BUCKET_S)
    """
    is_weekend = dt.weekday() >= 5
    peak = WEEKEND_PEAK if is_weekend else WEEKDAY_PEAK

    base_rel = _day_curve_base(dt)        # 0..1
    base_abs = NIGHT_MIN + (peak - NIGHT_MIN) * base_rel

    # pomaly šum (kompozície sa mierne odlišujú)
    rng_slow = _rng_from_key(f"slow::{seed_key or dt.isoformat()}")
    slow_eps = _clipped_gauss(rng_slow, SLOW_SIGMA, SLOW_CLIP)

    # rýchly šum (živé tiky, mení sa každých SLOW_BUCKET_S)
    if ts_ms is None:
        bucket = int(dt.timestamp() // SLOW_BUCKET_S)
    else:
        bucket = (ts_ms // 1000) // SLOW_BUCKET_S
    rng_fast = _rng_from_key(f"fast::{bucket}")
    fast_eps = _clipped_gauss(rng_fast, FAST_SIGMA, FAST_CLIP)

    val = base_abs * (1.0 + slow_eps + fast_eps)
    val = max(0.0, val)
    out = int(round(val))

    if debug:
        return {
            "value": out,
            "base_rel": round(base_rel, 4),
            "base_abs": round(base_abs, 1),
            "slow_eps": round(slow_eps, 4),
            "fast_eps": round(fast_eps, 4),
            "peak": peak,
            "night_min": NIGHT_MIN,
            "weekend": is_weekend,
        }
    return out

# ---------------------------------------------------------------------------
# Parsovanie prvej (aktuálnej) položky z playlistu
# ---------------------------------------------------------------------------
def _find_first_row(soup: BeautifulSoup):
    # typické značkovanie na radia.sk
    row = soup.select_one("div.row.data, div.row_data")
    if row:
        return row
    # fallback – prvý riadok zoznamu
    rows = soup.select("div.row, .row")
    return rows[0] if rows else None

def parse_first_row(html: str) -> dict | None:
    soup = BeautifulSoup(html, "html.parser")
    row = _find_first_row(soup)
    if not row:
        return None

    d_el = row.select_one(".datum, .play_datum, .pl_datum")
    t_el = row.select_one(".cas, .play_cas, .pl_cas")
    a_el = row.select_one(".interpret, .play_interpret, .pl_interpret")
    s_el = row.select_one(".titul, .play_titul, .pl_titul")

    if not all([d_el, t_el, a_el, s_el]):
        return None

    d = parse_date_label(d_el.get_text())
    hhmm = re.findall(r"(\d{1,2}):(\d{2})", t_el.get_text())
    if not hhmm:
        return None
    hh, mm = int(hhmm[0][0]), int(hhmm[0][1])

    return {
        "title":  s_el.get_text(strip=True),
        "artist": a_el.get_text(strip=True),
        "date":   fmt_date(d),
        "time":   f"{hh:02d}:{mm:02d}",
    }

# ---------------------------------------------------------------------------
# Public API – zavolá sa z FastAPI /now
# ---------------------------------------------------------------------------
def get_now_playing(override_ts: int | None = None, debug: bool = False) -> dict:
    html = fetch_html()
    row = parse_first_row(html)
    if not row:
        return {"error": "Nepodarilo sa získať aktuálnu skladbu."}

    # doplň názov rádia
    st = parse_station_name(html)
    if st:
        row["station"] = st

    # pre výpočet odhadu poslucháčov
    d = datetime.strptime(row["date"], "%d.%m.%Y").date()
    hh, mm = [int(x) for x in row["time"].split(":")]
    dtp = datetime.combine(d, time(hour=hh, minute=mm), TZ)

    song_key = f'{row["artist"]}|{row["title"]}|{row["date"]}|{row["time"]}'
    est = estimate_listeners(dtp, seed_key=song_key, ts_ms=override_ts, debug=debug)

    if debug:
        row["listeners"] = est["value"]
        row["_dbg"] = est
    else:
        row["listeners"] = est

    return row
