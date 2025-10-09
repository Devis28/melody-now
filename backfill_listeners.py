# -*- coding: utf-8 -*-
"""
Backfill poslucháčov podľa simulovaného denného profilu:
- pracovný deň peak ≈ WEEKDAY_PEAK (default 3000)
- víkend peak ≈ WEEKEND_PEAK (default 2000)
- noc všetky dni cca 200–500 (tu NIGHT_MIN = 260)
- deterministický jitter ± ~8 %
"""
import json, os, math, hashlib, random
from datetime import datetime
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Europe/Bratislava")
PATH = os.environ.get("OUT_PATH", "data/playlist.json")

# === nastaviteľné konštanty ==================================================
WEEKDAY_PEAK  = float(os.environ.get("WEEKDAY_PEAK", 3260))   # napr. 3200 ak chceš
WEEKEND_PEAK  = float(os.environ.get("WEEKEND_PEAK", 1820))   # napr. 1800 ak chceš
NIGHT_MIN     = float(os.environ.get("NIGHT_MIN", 150))       # cca 200–500
JITTER_SIGMA  = float(os.environ.get("JITTER_SIGMA", 0.07))   # ~7 %
JITTER_CLIP   = float(os.environ.get("JITTER_CLIP", 0.12))    # max ±12 %

# === tvarovací model =========================================================
def _gauss(x, mu, sigma, amp):
    return amp * math.exp(-0.5 * ((x - mu) / sigma) ** 2)

def _normalize(arr):
    a_min, a_max = min(arr), max(arr)
    if a_max <= a_min: return [0.0 for _ in arr]
    return [(v - a_min) / (a_max - a_min) for v in arr]

def _shape_weekday(h):
    return (
        _gauss(h, 7.8, 1.2, 0.9) +
        _gauss(h, 12.5, 1.3, 0.45) +
        _gauss(h, 17.3, 1.3, 0.85) +
        _gauss(h, 20.5, 1.8, 0.35)
    )

def _shape_weekend(h):
    return (
        _gauss(h, 10.0, 1.7, 0.35) +
        _gauss(h, 14.0, 2.0, 0.95) +
        _gauss(h, 19.5, 2.0, 0.55)
    )

def _night_multiplier(h):
    # najnižšie okolo ~02:30
    night_valley = math.exp(-0.5 * ((h - 2.5) / 2.0) ** 2)
    return 1.0 - 0.25 * night_valley  # 0.75..1.0

def _shape01(h, is_weekend):
    if not hasattr(_shape01, "_cache"):
        _shape01._cache = {}
    key = "we" if is_weekend else "wd"
    if key not in _shape01._cache:
        grid = [i/12 for i in range(0, 24*12 + 1)]
        arr = [(_shape_weekend(x) if is_weekend else _shape_weekday(x)) * _night_multiplier(x) for x in grid]
        _shape01._cache[key] = (grid, _normalize(arr))
    grid, norm = _shape01._cache[key]
    idx = min(range(len(grid)), key=lambda i: abs(grid[i] - h))
    return norm[idx]

def _expected_count(dt: datetime) -> float:
    h = dt.hour + dt.minute / 60.0
    is_weekend = dt.weekday() >= 5
    s01 = _shape01(h, is_weekend)
    peak = WEEKEND_PEAK if is_weekend else WEEKDAY_PEAK
    return NIGHT_MIN + s01 * (peak - NIGHT_MIN)

def _deterministic_jitter(key: str, sigma=JITTER_SIGMA, clip=JITTER_CLIP) -> float:
    digest = hashlib.md5(key.encode("utf-8")).hexdigest()
    seed = int(digest[:16], 16)
    rng = random.Random(seed)
    u1, u2 = max(rng.random(), 1e-9), max(rng.random(), 1e-9)
    z = ( (-2.0 * math.log(u1)) ** 0.5 ) * math.cos(2*math.pi*u2)
    eps = max(-clip, min(clip, sigma * z))
    return 1.0 + eps

def estimate_from_curve(dt: datetime, item_key: str) -> int:
    base = _expected_count(dt)
    v = base * _deterministic_jitter(item_key)
    peak_cap = WEEKEND_PEAK if dt.weekday() >= 5 else WEEKDAY_PEAK
    v = max(200.0, min(peak_cap, v))
    return int(round(v))

# === IO & main ===============================================================
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
