# -*- coding: utf-8 -*-
import json, os
from datetime import datetime
from zoneinfo import ZoneInfo

from melody_core import estimate_listeners

TZ = ZoneInfo("Europe/Bratislava")
PATH = os.environ.get("OUT_PATH", "data/playlist.json")

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

if __name__ == "__main__":
    data = load(PATH)
    changed = 0
    for it in data:
        if "listeners" not in it or it["listeners"] in (None, "", 0):
            dt = parse_dt(it)
            it["listeners"] = int(estimate_listeners(dt))
            changed += 1
    save(PATH, data)
    print(f"Backfilled {changed} items into {PATH}")
