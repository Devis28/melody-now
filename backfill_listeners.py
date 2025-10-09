# -*- coding: utf-8 -*-
"""
Backfill poslucháčov podľa simulovaného denného profilu:

- Pracovný deň: peak ≈ 3000 (ráno ~8:00 a popoludní ~17:30), slušná baseline cez deň.
- Víkend: peak ≈ 2000 (popoludnie ~14–20h), celkovo nižšie.
- Noc všetky dni: cca 200–500 (hlboké minimum okolo 02:30).
- Výsledok je jemne rozkmitaný deterministickým jitterom (± ~8 %).

Ak záznam už 'listeners' má, necháva sa tak. Inak sa dopočíta.
"""

import json, os
from datetime import datetime
from zoneinfo import ZoneInfo
import math, hashlib, random

TZ = ZoneInfo("Europe/Bratislava")
PATH = os.environ.get("OUT_PATH", "data/playlist.json")

# --------------------------- tvarovací model --------------------------------

def _gauss(x, mu, sigma, amp):
    return amp * math.exp(-0.5 * ((x - mu) / sigma) ** 2)

def _normalize(arr):
    amin, amax = min(arr), max(arr)
    if amax - amin <= 0:
        return [0.0 for _ in arr]
    return [(v - amin) / (amax - amin) for v in arr]

def _shape_weekday(h):
    # výrazne ráno + popoludnie, menšie poludnie a večer
    return (
        _gauss(h, 7.8, 1.2, 0.9)   # morning peak
        + _gauss(h, 12.5, 1.3, 0.45)
        + _gauss(h, 17.3, 1.3, 0.85) # afternoon peak
        + _gauss(h, 20.5, 1.8, 0.35)
    )

def _shape_weekend(h):
    # neskorší nábeh, veľké popoludnie, menší podvečer
    return (
        _gauss(h, 10.0, 1.7, 0.35)
        + _gauss(h, 14.0, 2.0, 0.95)
        + _gauss(h, 19.5, 2.0, 0.55)
    )

# predpočítať hladký „nočný útlm“ – vyjadrený multiplikátorom 0.75..1.0
def _night_multiplier(h):
    # maximum okolo 02:30 -> silnejší útlm
    night_valley = math.exp(-0.5 * ((h - 2.5) / 2.0) ** 2)  # 0..1
    return 1.0 - 0.25 * night_valley                        # 0.75..1.0

def _shape01(h, is_weekend):
    base = _shape_weekend(h) if is_weekend else _shape_weekday(h)
    # normalizácia na 0..1 v rámci dňa – spravíme cez samplovanie po 5 min
    # (aby multiplikátor noci mal rozumný vplyv)
    if not hasattr(_shape01, "_cache"):
        _shape01._cache = {"wd": None, "we": None}
    key = "we" if is_weekend else "wd"
    if _shape01._cache[key] is None:
        grid = [i/12 for i in range(0, 24*12 + 1)]  # 5-min mriežka v hodinách
        arr = [(_shape_weekend(x) if is_weekend else _shape_weekday(x)) * _night_multiplier(x)
               for x in grid]
        _shape01._cache[key] = (grid, _normalize(arr))
    grid, norm = _shape01._cache[key]

    # nájdeme najbližší index
    idx = min(range(len(grid)), key=lambda i: abs(grid[i]-h))
    return norm[idx]

def _expected_count(dt: datetime) -> float:
    """
    Vypočíta očakávaný počet poslucháčov pre daný datetime (Europe/Bratislava).
    - pracovný deň peak ~3000, víkend peak ~2000
    - minimá v noci ~200–500 (tu volíme ~260)
    """
    h = dt.hour + dt.minute / 60.0
    is_weekend = dt.weekday() >= 5  # 5=Sat,6=Sun

    s01 = _shape01(h, is_weekend)
    night_min = 260.0
    peak = 2000.0 if is_weekend else 3000.0

    val = night_min + s01 * (peak - night_min)
    return val

def _deterministic_jitter(item_key: str, sigma=0.07, clip=0.12) -> float:
    """
    Vráti multiplikátor (1 + eps), eps ~ N(0, sigma), deterministicky podľa kľúča.
    """
    digest = hashlib.md5(item_key.encode("utf-8")).hexdigest()
    seed = int(digest[:16], 16)
    rng = random.Random(seed)
    # Box-Muller pre normálne rozdelenie:
    u1, u2 = max(rng.random(), 1e-9), max(rng.random(), 1e-9)
    z = ( (-2.0 * math.log(u1)) ** 0.5 ) * math.cos(2*math.pi*u2)
    eps = max(-clip, min(clip, sigma * z))
    return 1.0 + eps

def estimate_from_curve(dt: datetime, item_key: str) -> int:
    base = _expected_count(dt)
    mult = _deterministic_jitter(item_key)
    v = base * mult
    # ohranič, aby „náhodnosť“ nevyletela mimo rozumnej zóny pre daný čas
    is_weekend = dt.weekday() >= 5
    peak = 2000.0 if is_weekend else 3000.0
    floor = 200.0  # úplné minimum
    v = max(floor, min(peak, v))
    return int(round(v))

# ------------------------------ IO a beh ------------------------------------

def load(path):
    if not os.path.exists(path): return []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def save(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def parse_dt(item):
    # item["date"] = dd.mm.yyyy, item["time"] = HH:MM
    dt = datetime.strptime(f'{item["date"]} {item["time"]}', "%d.%m.%Y %H:%M")
    return dt.replace(tzinfo=TZ)

def item_key(item) -> str:
    # stabilný kľúč pre deterministický jitter
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
