# -*- coding: utf-8 -*-
"""
Obohacovanie záznamov o album, release_year, duration_sec/duration_ms,
composers/lyricists/writers, genres. Zdroj: iTunes, Deezer, MusicBrainz.

- Beží nad existujúcim data/playlist.json (backfill).
- Výsledok ukladá späť do data/playlist.json.
- Medzivýsledky (pre (artist,title)) si cache-uje do data/meta_cache.json,
  aby sa API nevolali pri ďalšom behu znova.

Spusti:
    python enrich_metadata.py
"""

import json, os, re, time
from datetime import datetime
from urllib.parse import quote_plus

import requests

PLAYLIST_PATH = os.environ.get("PLAYLIST_PATH", "data/playlist.json")
CACHE_PATH    = os.environ.get("CACHE_PATH", "data/meta_cache.json")

# --- Pomocné ---------------------------------------------------------------

def ensure_dir(path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)

def load_json(path: str, default):
    if not os.path.exists(path): return default
    with open(path, "r", encoding="utf-8") as f:
        try: return json.load(f)
        except Exception: return default

def save_json(path: str, data):
    ensure_dir(path)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def clean_title(s: str) -> str:
    """Agresívna normalizácia názvu pre lepší match v API."""
    s = s.lower()
    s = re.sub(r"\s*-\s*(remaster(?:ed)?(?: \d{4})?|mono|stereo|single|version|mix|edit|radio edit).*", "", s)
    s = re.sub(r"\s*\((?:feat\.?|featuring|with)\s+[^)]*\)", "", s)  # feat. ...
    s = re.sub(r"\s*\((?:live|remaster(?:ed)?|version|mix|edit|radio edit|mono|stereo)[^)]*\)", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def clean_artist(s: str) -> str:
    s = s.lower()
    s = re.sub(r"\s+(?:feat\.?|&|and)\s+.*", "", s)  # odstráň trailing feat/and/...
    s = re.sub(r"\s+", " ", s).strip()
    return s

def norm_key(artist: str, title: str) -> str:
    return f"{clean_artist(artist)}|{clean_title(title)}"

def pick_first(items):
    return items[0] if items else None

def year_from_date(s: str | None):
    if not s: return None
    try: return int(s[:4])
    except Exception: return None

# --- iTunes (Apple) --------------------------------------------------------

def from_itunes(artist: str, title: str) -> dict:
    """
    Bez API kľúča. Často vráti album, release_year, duration_ms, cover, genre.
    """
    q = quote_plus(f"{artist} {title}")
    url = f"https://itunes.apple.com/search?term={q}&entity=song&limit=3&country=sk"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    j = r.json()
    if not j.get("resultCount"):
        return {}
    # heuristika: preferuj výsledek, kde sedí interpret aspoň čiastočne
    cand = None
    a_norm = clean_artist(artist)
    t_norm = clean_title(title)
    for x in j["results"]:
        if clean_artist(x.get("artistName","")) in (a_norm, a_norm.replace("&","and")) or a_norm in clean_artist(x.get("artistName","")):
            cand = x
            if t_norm in clean_title(x.get("trackName","")):
                break
    x = cand or j["results"][0]
    out = {
        "album": x.get("collectionName"),
        "release_year": year_from_date(x.get("releaseDate")),
        "duration_ms": x.get("trackTimeMillis"),
        "duration_sec": int(x["trackTimeMillis"]/1000) if x.get("trackTimeMillis") else None,
        "genres": [x.get("primaryGenreName")] if x.get("primaryGenreName") else [],
        "cover_url": x.get("artworkUrl100"),
        "sources": {"itunes_track_id": x.get("trackId")}
    }
    return {k:v for k,v in out.items() if v not in (None, [], "")}

# --- Deezer ----------------------------------------------------------------

def from_deezer(artist: str, title: str) -> dict:
    """
    Bez kľúča. Dáva duration_sec a album. Na žánre je lepšie ešte doťah album detail.
    """
    url = f'https://api.deezer.com/search?q=artist:"{artist}" track:"{title}"'
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    j = r.json()
    data = j.get("data") or []
    if not data:
        return {}
    x = data[0]
    album = x.get("album") or {}
    out = {
        "album": album.get("title"),
        "duration_sec": x.get("duration"),
        "cover_url": album.get("cover_xl") or album.get("cover"),
        "sources": {"deezer_track_id": x.get("id")}
    }
    # skús získať genres z detailu albumu
    if album.get("id"):
        try:
            aj = requests.get(f'https://api.deezer.com/album/{album["id"]}', timeout=20).json()
            g = [g["name"] for g in (aj.get("genres",{}).get("data") or [])]
            if g: out["genres"] = g
        except Exception:
            pass
    return {k:v for k,v in out.items() if v not in (None, [], "")}

# --- MusicBrainz (najlepšie na autorov/skladateľov) ------------------------

MB_HEADERS = {"User-Agent": "melody-now/1.0 (contact: example@example.com)"}

def mb_search_recording(artist: str, title: str) -> dict | None:
    q = f'recording:"{title}" AND artist:"{artist}"'
    url = f'https://musicbrainz.org/ws/2/recording/?query={quote_plus(q)}&fmt=json&limit=3'
    j = requests.get(url, headers=MB_HEADERS, timeout=20).json()
    recs = j.get("recordings") or []
    return pick_first(recs)

def mb_work_people(work_mbid: str) -> dict:
    """
    Získa mená pre role composer/lyricist/writer z entity Work.
    """
    url = f'https://musicbrainz.org/ws/2/work/{work_mbid}?inc=artist-rels&fmt=json'
    j = requests.get(url, headers=MB_HEADERS, timeout=20).json()
    people = {"composers": [], "lyricists": [], "writers": []}
    for rel in j.get("relations", []):
        if rel.get("type") in ("composer", "lyricist", "writer"):
            name = rel.get("artist", {}).get("name")
            if not name: continue
            if rel["type"] == "composer": people["composers"].append(name)
            elif rel["type"] == "lyricist": people["lyricists"].append(name)
            elif rel["type"] == "writer": people["writers"].append(name)
    # odstráň duplicity
    for k in people:
        people[k] = sorted(set(people[k]))
    return {k:v for k,v in people.items() if v}

def from_musicbrainz(artist: str, title: str) -> dict:
    """
    Najlepšie zdroj na ISRC a kredity (composer/lyricist/writer),
    release_year často z prvého release.
    """
    rec = mb_search_recording(artist, title)
    if not rec:
        return {}
    out = {}
    # rok vydania – z prvého release (ak je)
    year = None
    if rec.get("releases"):
        date = rec["releases"][0].get("date")
        year = year_from_date(date)
    if year: out["release_year"] = year
    # ISRC
    if rec.get("isrcs"):
        out["isrc"] = rec["isrcs"][0]
    # Autori cez Work
    works = [rel.get("work",{}).get("id") for rel in rec.get("relations",[]) if rel.get("type")=="work"]
    # Ak recording neobsahuje relations->work, skús detail
    if not works:
        try:
            det = requests.get(
                f'https://musicbrainz.org/ws/2/recording/{rec["id"]}?inc=work-rels+artist-credits+releases+isrcs&fmt=json',
                headers=MB_HEADERS, timeout=20).json()
            works = [rel.get("work",{}).get("id") for rel in det.get("relations",[]) if rel.get("type")=="work"]
            if det.get("isrcs") and "isrc" not in out:
                out["isrc"] = det["isrcs"][0]
            if det.get("releases") and "release_year" not in out:
                out["release_year"] = year_from_date(det["releases"][0].get("date"))
        except Exception:
            pass
    # doťahni mená z Work
    composers, lyricists, writers = set(), set(), set()
    for wid in works[:2]:  # stačí 1–2 worky
        if not wid: continue
        try:
            ppl = mb_work_people(wid)
            composers |= set(ppl.get("composers", []))
            lyricists |= set(ppl.get("lyricists", []))
            writers   |= set(ppl.get("writers", []))
            time.sleep(0.4)  # ohľaduplne
        except Exception:
            pass
    if composers: out["composers"] = sorted(composers)
    if lyricists: out["lyricists"] = sorted(lyricists)
    if writers:   out["writers"]   = sorted(writers)
    out.setdefault("sources", {})["musicbrainz_recording"] = rec.get("id")
    return {k:v for k,v in out.items() if v not in (None, [], "")}

# --- Spájanie výsledkov z rôznych zdrojov ----------------------------------

PREFERRED_ORDER = ("album", "release_year", "duration_ms", "duration_sec",
                   "composers", "lyricists", "writers", "genres", "isrc", "cover_url")

def merge_meta(*dicts) -> dict:
    result = {}
    for d in dicts:
        for k, v in (d or {}).items():
            if v in (None, "", []):
                continue
            if k in ("genres", "composers", "lyricists", "writers"):
                # zluč zoznamy a odstráň duplicity
                cur = set(result.get(k, []))
                add = set(v if isinstance(v, list) else [v])
                result[k] = sorted(cur | add)
            elif k == "sources":
                cur = result.get("sources", {})
                cur.update(v)
                result["sources"] = cur
            else:
                # ak už existuje hodnota, ponechaj prvú (priorita podľa volania merge_meta)
                result.setdefault(k, v)
    # preferuj duration_ms, ale doplň aj duration_sec ak chýba
    if "duration_ms" in result and "duration_sec" not in result:
        result["duration_sec"] = int(round(result["duration_ms"]/1000))
    return result

# --- Cache -----------------------------------------------------------------

def load_cache() -> dict:
    return load_json(CACHE_PATH, {})

def save_cache(cache: dict):
    save_json(CACHE_PATH, cache)

def need_enrichment(item: dict) -> bool:
    """Rozhodni, či záznam ešte treba obohatiť."""
    targets = ("album", "release_year", "duration_ms", "duration_sec", "composers", "lyricists", "writers", "genres")
    return any(k not in item or item.get(k) in (None, "", [], 0) for k in targets)

# --- Hlavný proces ---------------------------------------------------------

def enrich_pair(artist: str, title: str) -> dict:
    """
    Zavolá API v rozumnom poradí a vráti spojené metadáta.
    Poradie (dôležité kvôli priorite):
      - MusicBrainz (autori, isrc, rok)
      - iTunes (album, rok, duration_ms, genre)
      - Deezer (album, duration_sec, genres cez album)
    """
    mb  = from_musicbrainz(artist, title)
    # krátka pauza je slušnosť voči MusicBrainz
    time.sleep(0.4)
    it  = from_itunes(artist, title)
    dz  = from_deezer(artist, title)
    return merge_meta(mb, it, dz)

def run_backfill():
    playlist = load_json(PLAYLIST_PATH, [])
    cache = load_cache()
    changed_items = 0
    touched_keys = 0

    # unikátne dvojice (artist, title), ktoré chýbajú v cache alebo potrebujú doplniť
    need_keys = set()
    for it in playlist:
        if need_enrichment(it):
            need_keys.add(norm_key(it["artist"], it["title"]))

    for k in sorted(need_keys):
        touched_keys += 1
        if k not in cache:
            a, t = k.split("|", 1)
            meta = enrich_pair(a, t)
            cache[k] = meta or {}
            # ohľaduplné throttle medzi API volaniami
            time.sleep(0.3)
        # inak použije existujúcu cache

    if touched_keys:
        save_cache(cache)

    # doplň metadáta do položiek
    for it in playlist:
        if not need_enrichment(it):
            continue
        key = norm_key(it["artist"], it["title"])
        meta = cache.get(key, {})
        if not meta:
            continue
        before = json.dumps({k: it.get(k) for k in PREFERRED_ORDER}, ensure_ascii=False, sort_keys=True)
        # aplikuj meta do záznamu
        for k, v in meta.items():
            if k in ("genres", "composers", "lyricists", "writers"):
                cur = set(it.get(k, []))
                add = set(v)
                if add - cur:
                    it[k] = sorted(cur | add)
            elif k == "sources":
                cur = it.get("sources", {})
                cur.update(v)
                it["sources"] = cur
            else:
                if k not in it or it[k] in (None, "", [], 0):
                    it[k] = v
        after = json.dumps({k: it.get(k) for k in PREFERRED_ORDER}, ensure_ascii=False, sort_keys=True)
        if before != after:
            changed_items += 1

    if changed_items:
        save_json(PLAYLIST_PATH, playlist)

    print(f"Pairs touched: {touched_keys}, items updated: {changed_items}, cache size: {len(cache)}")

if __name__ == "__main__":
    run_backfill()
