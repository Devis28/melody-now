# -*- coding: utf-8 -*-
"""
Backfill poslucháčov podľa simulovaného denného profilu:
- pracovný deň peak ≈ WEEKDAY_PEAK
- víkend peak ≈ WEEKEND_PEAK
- noc utlmená (cirkulárne okolo NIGHT_CENTER) + večerný chvost
- spodný prah tvaru NIGHT_SFLOOR + minute-of-day wobble
- deterministický jitter z item_key
Env:
  WEEKDAY_PEAK, WEEKEND_PEAK,
  NIGHT_MIN, NIGHT_CENTER, NIGHT_WIDTH, NIGHT_STRENGTH, NIGHT_POWER, NIGHT_SFLOOR,
  EVENING_TAIL_START, EVENING_TAIL_STRENGTH, EVENING_TAIL_SLOPE,
  MDAY_WOBBLE, MDAY_WOBBLE_PHASE,
  JITTER_SIGMA, JITTER_CLIP,
  REWRITE_ALL=0/1
"""
import json, os, math, hashlib
from datetime import datetime
from zoneinfo import ZoneInfo

# ---------- cesty a TZ ----------
TZ = ZoneInfo("Europe/Bratislava")
PATH = os.environ.get("OUT_PATH", "data/playlist.json")
REWRITE_ALL = os.environ.get("REWRITE_ALL", "0") == "1"

# ---------- nastaviteľné konštanty ----------
WEEKDAY_PEAK   = float(os.environ.get("WEEKDAY_PEAK", 3260))
WEEKEND_PEAK   = float(os.environ.get("WEEKEND_PEAK", 1820))

NIGHT_MIN      = float(os.environ.get("NIGHT_MIN", 160))
NIGHT_CENTER   = float(os.environ.get("NIGHT_CENTER", 2.5))
NIGHT_WIDTH    = float(os.environ.get("NIGHT_WIDTH", 3.0))
NIGHT_STRENGTH = float(os.environ.get("NIGHT_STRENGTH", 0.96))
NIGHT_POWER    = float(os.environ.get("NIGHT_POWER", 1.6))
NIGHT_SFLOOR   = float(os.environ.get("NIGHT_SFLOOR", 0.05))

EVENING_TAIL_START     = float(os.environ.get("EVENING_TAIL_START", 22.0))
EVENING_TAIL_STRENGTH  = float(os.environ.get("EVENING_TAIL_STRENGTH", 0.35))
EVENING_TAIL_SLOPE     = float(os.environ.get("EVENING_TAIL_SLOPE", 0.6))

MDAY_WOBBLE       = float(os.environ.get("MDAY_WOBBLE", 0.03))
MDAY_WOBBLE_PHASE = float(os.environ.get("MDAY_WOBBLE_PHASE", 1.2))

JITTER_SIGMA   = float(os.environ.get("JITTER_SIGMA", 0.06))
JITTER_CLIP    = float(os.environ.get("JITTER_CLIP", 0.12))

# ---------- denné tvary (bez noci) ----------
def _gauss(x, mu, sigma, amp):
    return amp * math.exp(-0.5 * ((x - mu) / sigma) ** 2)

def _shape_weekday_raw(h):
    return (
        _gauss(h, 7.8, 1.2, 0.9)  +
        _gauss(h, 12.5, 1.3, 0.45) +
        _gauss(h, 17.3, 1.3, 0.85) +
        _gauss(h, 20.5, 1.8, 0.35)
    )

def _shape_weekend_raw(h):
    return (
        _gauss(h, 10.0, 1.7, 0.35) +
        _gauss(h, 14.0, 2.0, 0.95) +
        _gauss(h, 19.5, 2.0, 0.55)
    )

def _normalize(arr):
    a_min, a_max = min(arr), max(arr)
    if a_max <= a_min: return [0.0 for _ in arr]
    return [(v - a_min) / (a_max - a_min) for v in arr]

# ---------- nočné/večerné tlmenie ----------
def _circ_dist_hours(a, b):
    d = abs(a - b)
    return min(d, 24.0 - d)

def _sigmoid(x):
    return 1.0 / (1.0 + math.exp(-x))

def _night_depressor(h):
    valley = math.exp(-0.5 * (_circ_dist_hours(h, NIGHT_CENTER) / max(0.1, NIGHT_WIDTH)) ** 2)
    dep1 = 1.0 - NIGHT_STRENGTH * valley
    dep1 = max(0.0, min(1.0, dep1)) ** NIGHT_POWER
    tail = _sigmoid((h - EVENING_TAIL_START) / max(0.1, EVENING_TAIL_SLOPE))
    dep2 = 1.0 - EVENING_TAIL_STRENGTH * tail
    return max(0.0, min(1.0, dep1 * dep2))

def _precompute_norm(is_weekend: bool):
    grid = [i/12 for i in range(0, 24*12 + 1)]
    raw = [(_shape_weekend_raw(x) if is_weekend else _shape_weekday_raw(x)) for x in grid]
    return grid, _normalize(raw)

def _s01(h, is_weekend):
    if not hasattr(_s01, "_cache"):
        _s01._cache = {}
    key = "we" if is_weekend else "wd"
    if key not in _s01._cache:
        _s01._cache[key] = _precompute_norm(is_weekend)
    grid, day_norm = _s01._cache[key]
    idx = min(range(len(grid)), key=lambda i: abs(grid[i] - h))
    s = day_norm[idx] * _night_depressor(h)
    s = max(NIGHT_SFLOOR, s)
    wobble = 1.0 + MDAY_WOBBLE * math.sin(2.0 * math.pi * (h / 24.0) + MDAY_WOBBLE_PHASE)
    s = s * wobble
    return min(1.0, max(NIGHT_SFLOOR, s))

# ---------- očakávaný počet + jitter ----------
def _expected_count(dt: datetime) -> float:
    h = dt.hour + dt.minute / 60.0
    is_weekend = dt.weekday() >= 5
    s = _s01(h, is_weekend)
    peak = WEEKEND_PEAK if is_weekend else WEEKDAY_PEAK
    return NIGHT_MIN + s * (peak - NIGHT_MIN)

def _deterministic_jitter(seed_key: str, sigma=JITTER_SIGMA, clip=JITTER_CLIP) -> float:
    d = hashlib.md5(seed_key.encode("utf-8")).hexdigest()
    a = int(d[:16], 16); b = int(d[16:32], 16)
    u1 = ((a % 10_000_000) + 0.5) / 10_000_001.0
    u2 = ((b % 10_000_000) + 0.5) / 10_000_001.0
    z = ( (-2.0 * math.log(max(u1, 1e-9))) ** 0.5 ) * math.cos(2*math.pi*max(u2, 1e-9))
    eps = max(-clip, min(clip, sigma * z))
    return 1.0 + eps

def estimate_from_curve(dt: datetime, item_key: str) -> int:
    base = _expected_count(dt)
    v = base * _deterministic_jitter(item_key)
    peak_cap = WEEKEND_PEAK if dt.weekday() >= 5 else WEEKDAY_PEAK
    v = max(NIGHT_MIN, min(peak_cap, v))
    return int(round(v))

# ---------- IO & main ----------
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
        if REWRITE_ALL or ("listeners" not in it or it["listeners"] in (None, "", 0)):
            dt = parse_dt(it)
            it["listeners"] = estimate_from_curve(dt, item_key(it))
            changed += 1
    save(PATH, data)
    print(f"Backfilled {changed} items into {PATH}")
