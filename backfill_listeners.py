# -*- coding: utf-8 -*-
"""
Backfill poslucháčov podľa simulovaného denného profilu:
- pracovný deň peak ≈ WEEKDAY_PEAK
- víkend peak ≈ WEEKEND_PEAK
- noc (cca 00:00–04:00) výrazne utlmená; minimum ~NIGHT_MIN
- deterministický jitter ± pár %

Env-tunables:
  WEEKDAY_PEAK, WEEKEND_PEAK, NIGHT_MIN, NIGHT_STRENGTH, NIGHT_POWER,
  JITTER_SIGMA, JITTER_CLIP
"""
import json, os, math, hashlib, random
from datetime import datetime
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Europe/Bratislava")
PATH = os.environ.get("OUT_PATH", "data/playlist.json")

# ====== nastaviteľné konštanty ===============================================
WEEKDAY_PEAK   = float(os.environ.get("WEEKDAY_PEAK", 3260))   # špička pracovného dňa
WEEKEND_PEAK   = float(os.environ.get("WEEKEND_PEAK", 1820))   # špička víkendu
NIGHT_MIN      = float(os.environ.get("NIGHT_MIN", 140))       # minimum v hlbokej noci
NIGHT_STRENGTH = float(os.environ.get("NIGHT_STRENGTH", 0.96)) # 0..1 (ako silno tlmiť noc)
NIGHT_POWER    = float(os.environ.get("NIGHT_POWER", 1.8))     # >1 = ešte prudšie tlmenie
JITTER_SIGMA   = float(os.environ.get("JITTER_SIGMA", 0.06))   # ~6 %
JITTER_CLIP    = float(os.environ.get("JITTER_CLIP", 0.12))    # max ±12 %

# ====== denné tvary (bez noci) ===============================================
def _gauss(x, mu, sigma, amp):
    return amp * math.exp(-0.5 * ((x - mu) / sigma) ** 2)

def _shape_weekday_raw(h):
    # ráno + popoludnie, menšie poludnie a večer
    return (
        _gauss(h, 7.8, 1.2, 0.9)  +
        _gauss(h, 12.5, 1.3, 0.45) +
        _gauss(h, 17.3, 1.3, 0.85) +
        _gauss(h, 20.5, 1.8, 0.35)
    )

def _shape_weekend_raw(h):
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

# predpočítame normalizovaný dený tvar BEZ nočného tlmenia
def _precompute_norm(is_weekend: bool):
    grid = [i/12 for i in range(0, 24*12 + 1)]  # 5-min mriežka v hodinách
    raw = [(_shape_weekend_raw(x) if is_weekend else _shape_weekday_raw(x)) for x in grid]
    return grid, _normalize(raw)

# nočný depressor: 1 - k*valley, potom mocnina
def _night_depressor(h):
    # valley ~1 okolo 02:30, ~0 cez deň
    valley = math.exp(-0.5 * ((h - 2.5) / 2.0) ** 2)
    dep = 1.0 - NIGHT_STRENGTH * valley  #  (1 - 0.85) = 0.15 v najhlbšej noci
    return max(0.0, min(1.0, dep)) ** NIGHT_POWER

# vráti s01 v [0,1] po aplikácii nočného tlmenia (ale už bez re-normalizácie)
def _s01(h, is_weekend):
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
    s = _s01(h, is_weekend)  # 0..1 po nočnom tlmení
    peak = WEEKEND_PEAK if is_weekend else WEEKDAY_PEAK
    return NIGHT_MIN + s * (peak - NIGHT_MIN)

def _deterministic_jitter(key: str, sigma=JITTER_SIGMA, clip=JITTER_CLIP) -> float:
    d = hashlib.md5(key.encode("utf-8")).hexdigest()
    seed = int(d[:16], 16)
    rng = random.Random(seed)
    u1, u2 = max(rng.random(), 1e-9), max(rng.random(), 1e-9)
    z = ( (-2.0 * math.log(u1)) ** 0.5 ) * math.cos(2*math.pi*u2)
    eps = max(-clip, min(clip, sigma * z))
    return 1.0 + eps

def estimate_from_curve(dt: datetime, item_key: str) -> int:
    base = _expected_count(dt)
    v = base * _deterministic_jitter(item_key)
    peak_cap = WEEKEND_PEAK if dt.weekday() >= 5 else WEEKDAY_PEAK
    # spodný limit používame NIGHT_MIN, nie fixných 200
    v = max(NIGHT_MIN, min(peak_cap, v))
    return int(round(v))

# ====== IO & main ============================================================
def load(path):
    if not os.path.exists(path): return []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def save(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def parse_dt(item):
    dt = datetime.strptime(f'{item["date"]} {item["time"]}', "%d.%m.%Y %H:%M")
    return dt.replace(tzinfo=TZ)

def item_key(item) -> str:
    return f'{item.get("artist","")}|{item.get("title","")}|{item.get("date","")}|{item.get("time","")}'

if __name__ == "__main__":
    data = load(PATH)
    changed = 0
    for it in data:
        if "listeners" not in it or it["listeners"] in (None, "", 0):
            dt = parse_dt(it)
            it["listeners"] = estimate_from_curve(dt, item_key(it))
            changed += 1
    save(PATH, data)
    print(f"Backfilled {changed} items into {PATH}")
