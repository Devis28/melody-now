# -*- coding: utf-8 -*-
import re, os, math, hashlib, random
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

LIVE_JITTER_BUCKET_SEC = int(os.environ.get("LIVE_JITTER_BUCKET_SEC", "30"))

def _fetch_with_requests():
    headers = {"User-Agent": UA, "Accept-Language": "sk,en;q=0.9"}
    r = requests.get(URL, headers=headers, timeout=20)
    r.raise_for_status()
    return r.text

def fetch_html():
    """Skúsi requests; ak server kladie odpor a je cloudscraper, použije ho."""
    try:
        return _fetch_with_requests()
    except Exception:
        try:
            import cloudscraper
            s = cloudscraper.create_scraper()
            r = s.get(URL, headers={"User-Agent": UA}, timeout=20)
            r.raise_for_status()
            return r.text
        except Exception as e:
            raise e

# ---------------------------------------------------------------------------
# Parsovanie dátumu
# ---------------------------------------------------------------------------
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

# ---------------------------------------------------------------------------
# Odhad počúvanosti – tvar dňa + deterministický jitter
# (zhodné správanie pre históriu; /now pridá "live bucket")
# ---------------------------------------------------------------------------
WEEKDAY_PEAK   = float(os.environ.get("WEEKDAY_PEAK",   3260))
WEEKEND_PEAK   = float(os.environ.get("WEEKEND_PEAK",   1820))
NIGHT_MIN      = float(os.environ.get("NIGHT_MIN",      160))
NIGHT_STRENGTH = float(os.environ.get("NIGHT_STRENGTH", 0.97))  # 0..1
NIGHT_POWER    = float(os.environ.get("NIGHT_POWER",    1.7))   # >1 ostrejšie dno
NIGHT_WIDTH    = float(os.environ.get("NIGHT_WIDTH",    3.2))   # šírka doliny v h
EVENING_TAIL_START   = float(os.environ.get("EVENING_TAIL_START", 21.8))
EVENING_TAIL_STRENGTH= float(os.environ.get("EVENING_TAIL_STRENGTH", 0.45))
EVENING_TAIL_SLOPE   = float(os.environ.get("EVENING_TAIL_SLOPE",   0.55))
MDAY_WOBBLE          = float(os.environ.get("MDAY_WOBBLE",          0.03))

JITTER_SIGMA = float(os.environ.get("JITTER_SIGMA", 0.06))
JITTER_CLIP  = float(os.environ.get("JITTER_CLIP",  0.12))

def _gauss(x, mu, sigma, amp):
    return amp * math.exp(-0.5 * ((x - mu) / sigma) ** 2)

def _shape_weekday_raw(h):
    return (
        _gauss(h, 7.8,  1.2, 0.90) +  # ráno
        _gauss(h, 12.5, 1.3, 0.45) +  # poludnie
        _gauss(h, 17.3, 1.3, 0.85) +  # popoludnie
        _gauss(h, 20.5, 1.8, 0.35)    # večer
    )

def _shape_weekend_raw(h):
    return (
        _gauss(h, 10.0, 1.7, 0.35) +
        _gauss(h, 14.0, 2.0, 0.95) +
        _gauss(h, 19.5, 2.0, 0.55)
    )

def _normalize(arr):
    mn, mx = min(arr), max(arr)
    if mx <= mn: return [0.0 for _ in arr]
    return [(v - mn) / (mx - mn) for v in arr]

def _precompute_norm(is_weekend: bool):
    grid = [i/12 for i in range(0, 24*12 + 1)]  # každých 5 min
    raw  = [(_shape_weekend_raw(x) if is_weekend else _shape_weekday_raw(x)) for x in grid]
    return grid, _normalize(raw)

def _night_depressor(h):
    # diera okolo ~02:30
    valley = math.exp(-0.5 * ((h - 2.5) / (NIGHT_WIDTH/2.355)) ** 2)  # prepočet ~FWHM
    dep = (1.0 - NIGHT_STRENGTH * valley) ** NIGHT_POWER
    return max(0.0, min(1.0, dep))

def _evening_tail(h):
    if h <= EVENING_TAIL_START: return 1.0
    # plynulý pokles po 21:48
    return max(0.55, 1.0 - EVENING_TAIL_SLOPE * (h - EVENING_TAIL_START))

def _s01(h, is_weekend):
    if not hasattr(_s01, "_cache"):
        _s01._cache = {}
    key = "we" if is_weekend else "wd"
    if key not in _s01._cache:
        _s01._cache[key] = _precompute_norm(is_weekend)
    grid, day_norm = _s01._cache[key]
    idx = min(range(len(grid)), key=lambda i: abs(grid[i] - h))
    base = day_norm[idx]
    # mierne vlnenie cez obed/pop.
    wobble = 1.0 + MDAY_WOBBLE * math.sin(h * 2*math.pi / 5.0)
    return base * _night_depressor(h) * _evening_tail(h) * wobble

def _det_jitter(seed_key: str, sigma=JITTER_SIGMA, clip=JITTER_CLIP) -> float:
    """Deterministický multiplikátor ~ N(0, sigma) ohraničený clipom."""
    d = hashlib.md5(seed_key.encode("utf-8")).hexdigest()
    seed = int(d[:16], 16)
    rng = random.Random(seed)
    u1, u2 = max(rng.random(), 1e-9), max(rng.random(), 1e-9)
    z = ((-2.0 * math.log(u1)) ** 0.5) * math.cos(2*math.pi*u2)
    eps = max(-clip, min(clip, sigma * z))
    return 1.0 + eps

def estimate_listeners(dt: datetime, seed_key: str | None = None) -> int:
    """Odhad poslucháčov. Ak dodáš seed_key, zahrnie sa deterministický jitter."""
    h = dt.hour + dt.minute/60.0
    is_weekend = dt.weekday() >= 5
    peak = WEEKEND_PEAK if is_weekend else WEEKDAY_PEAK
    s = _s01(h, is_weekend)                  # 0..1 po tlmeniach
    base = NIGHT_MIN + s * (peak - NIGHT_MIN)
    if seed_key:
        base *= _det_jitter(seed_key)
    # clamp na povolený rozsah
    return int(round(max(NIGHT_MIN, min(peak, base))))

# ---------------------------------------------------------------------------
# Parsovanie prvej (aktuálnej) položky
# ---------------------------------------------------------------------------
def parse_first_row(html: str) -> dict | None:
    soup = BeautifulSoup(html, "html.parser")
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

    return {
        "title":  s_el.get_text(strip=True),
        "artist": a_el.get_text(strip=True),
        "date":   fmt_date(d),
        "time":   tm.strftime("%H:%M"),
        # "listeners" dorátame v get_now_playing() s live-jitrom
    }

# ---------------------------------------------------------------------------
# Public API: now-playing
# ---------------------------------------------------------------------------
# v melody_core.py

def get_now_playing(override_ts: int | None = None) -> dict:
    html = fetch_html()
    row = parse_first_row(html)
    if not row:
        return {"error": "Nepodarilo sa získať aktuálnu skladbu."}

    # zostav datetime z row
    d = datetime.strptime(row["date"], "%d.%m.%Y").date()
    hh, mm = [int(x) for x in row["time"].split(":")]
    dt = datetime.combine(d, time(hour=hh, minute=mm), TZ)

    # základný seed podľa skladby
    base_seed = f'{row["artist"]}|{row["title"]}|{row["date"]}|{row["time"]}'

    # live bucket – ak prišiel ts z klienta, použijeme ho, inak serverový čas
    if override_ts is not None:
        # ts je v milisekundách
        now_bucket = int((override_ts / 1000.0) // max(1, LIVE_JITTER_BUCKET_SEC))
    else:
        now_bucket = int(datetime.now(TZ).timestamp() // max(1, LIVE_JITTER_BUCKET_SEC))

    seed_live = f"{base_seed}|{now_bucket}"

    row["listeners"] = estimate_listeners(dt, seed_key=seed_live)
    return row

