# -*- coding: utf-8 -*-
"""
Backfill + enrichment do data/playlist.json:
- album, release_year, duration_ms
- composers, lyricists, writers
- genres (normalizované do kanonických)
- artist_country (ISO-3166-1 alpha-2, napr. 'US', 'SK')

Ak sa údaj nepodarí získať, zapíše sa explicitne null.
Nepoužívame: cover_url, sources, duration_sec.

Spustenie:  python enrich_metadata.py
"""

import json, os, re, time
from urllib.parse import quote_plus
import requests

PLAYLIST_PATH = os.environ.get("PLAYLIST_PATH", "data/playlist.json")
CACHE_PATH    = os.environ.get("CACHE_PATH", "data/meta_cache.json")

# ----- utils -----

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
    s = s.lower()
    s = re.sub(r"\s*-\s*(remaster(?:ed)?(?: \d{4})?|mono|stereo|single|version|mix|edit|radio edit).*", "", s)
    s = re.sub(r"\s*\((?:feat\.?|featuring|with)\s+[^)]*\)", "", s)
    s = re.sub(r"\s*\((?:live|remaster(?:ed)?|version|mix|edit|radio edit|mono|stereo)[^)]*\)", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def clean_artist(s: str) -> str:
    s = s.lower()
    s = re.sub(r"\s+(?:feat\.?|&|and)\s+.*", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def norm_key(artist: str, title: str) -> str:
    return f"{clean_artist(artist)}|{clean_title(title)}"

def year_from_date(s: str | None):
    if not s: return None
    try: return int(s[:4])
    except Exception: return None

# ----- genre normalization -----

_CANON = {
    "pop": ["pop","k-pop","kpop","j-pop","jpop","mandopop","cantopop","europop",
            "french pop","international pop","latin pop","synthpop","indie pop",
            "dance pop","pop rock"],
    "rock": ["rock","hard rock","soft rock","alternative rock","alt rock","classic rock","indie rock","punk rock","metalcore"],
    "hip-hop": ["hip hop","hip-hop","rap","trap"],
    "r&b": ["r&b","r&b/soul","soul","neo-soul","contemporary r&b"],
    "electronic": ["electronic","edm","dance","house","techno","trance","electro","drum and bass","dnb","dubstep"],
    "metal": ["metal","heavy metal","thrash metal","death metal"],
    "classical": ["classical","orchestral","baroque","symphony"],
    "jazz": ["jazz","smooth jazz","acid jazz"],
    "blues": ["blues"],
    "country": ["country"],
    "folk": ["folk","singer-songwriter"],
    "reggae": ["reggae","dancehall","ska"],
}
_FALLBACK = [
    ("hip hop","hip-hop"),("hip-hop","hip-hop"),("rap","hip-hop"),
    ("r&b","r&b"),("soul","r&b"),
    ("rock","rock"),("metal","metal"),("jazz","jazz"),("blues","blues"),
    ("country","country"),("folk","folk"),("reggae","reggae"),
    ("dance","electronic"),("edm","electronic"),("house","electronic"),
    ("techno","electronic"),("trance","electronic"),
    ("electro","electronic"),("drum and bass","electronic"),("dubstep","electronic"),
]

def _canon_display(name: str) -> str:
    if name == "hip-hop": return "Hip-Hop"
    if name == "r&b": return "R&B"
    return name.capitalize()

def normalize_genres(genres: list[str]) -> list[str]:
    out = set()
    for g in genres or []:
        s = (g or "").strip().lower()
        if not s: continue
        mapped = None
        for canon, alts in _CANON.items():
            if s in alts:
                mapped = canon; break
        if not mapped:
            for needle, canon in _FALLBACK:
                if needle in s:
                    mapped = canon; break
        if mapped: out.add(mapped)
    return sorted((_canon_display(x) for x in out))

# ----- iTunes -----

def from_itunes(artist: str, title: str) -> dict:
    q = quote_plus(f"{artist} {title}")
    url = f"https://itunes.apple.com/search?term={q}&entity=song&limit=3&country=sk"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    j = r.json()
    if not j.get("resultCount"): return {}
    a_norm, t_norm = clean_artist(artist), clean_title(title)
    cand = None
    for x in j["results"]:
        if a_norm in clean_artist(x.get("artistName","")):
            cand = x
            if t_norm in clean_title(x.get("trackName","")): break
    x = cand or j["results"][0]
    out = {
        "album": x.get("collectionName"),
        "release_year": year_from_date(x.get("releaseDate")),
        "duration_ms": x.get("trackTimeMillis"),
        "genres_raw": [x.get("primaryGenreName")] if x.get("primaryGenreName") else []
    }
    return {k:v for k,v in out.items() if v not in (None, [], "")}

# ----- Deezer -----

def from_deezer(artist: str, title: str) -> dict:
    url = f'https://api.deezer.com/search?q=artist:"{artist}" track:"{title}"'
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    j = r.json()
    data = j.get("data") or []
    if not data: return {}
    x = data[0]; album = x.get("album") or {}
    out = {
        "album": album.get("title"),
        "duration_ms": int(x["duration"])*1000 if x.get("duration") else None,
    }
    # genres z albumu
    if album.get("id"):
        try:
            aj = requests.get(f'https://api.deezer.com/album/{album["id"]}', timeout=20).json()
            g = [g["name"] for g in (aj.get("genres",{}).get("data") or []) if g.get("name")]
            if g: out["genres_raw"] = g
        except Exception:
            pass
    return {k:v for k,v in out.items() if v not in (None, [], "")}

# ----- MusicBrainz -----

MB_HEADERS = {"User-Agent": "melody-now/1.0 (contact: example@example.com)"}

def mb_search_recording(artist: str, title: str) -> dict | None:
    q = f'recording:"{title}" AND artist:"{artist}"'
    url = f'https://musicbrainz.org/ws/2/recording/?query={quote_plus(q)}&fmt=json&limit=3'
    j = requests.get(url, headers=MB_HEADERS, timeout=20).json()
    recs = j.get("recordings") or []
    return recs[0] if recs else None

def mb_artist_country_from_id(artist_mbid: str) -> str | None:
    try:
        url = f'https://musicbrainz.org/ws/2/artist/{artist_mbid}?fmt=json'
        j = requests.get(url, headers=MB_HEADERS, timeout=20).json()
        area = j.get("area") or {}
        codes = area.get("iso_3166_1_codes") or []
        if codes: return codes[0]
        if j.get("country"): return j["country"]
    except Exception:
        pass
    return None

def mb_work_people(work_mbid: str) -> dict:
    url = f'https://musicbrainz.org/ws/2/work/{work_mbid}?inc=artist-rels&fmt=json'
    j = requests.get(url, headers=MB_HEADERS, timeout=20).json()
    people = {"composers": [], "lyricists": [], "writers": []}
    for rel in j.get("relations", []):
        typ = rel.get("type")
        if typ in ("composer","lyricist","writer"):
            name = (rel.get("artist") or {}).get("name")
            if name: people[typ + "s"].append(name)
    for k in people: people[k] = sorted(set(people[k]))
    return {k:v for k,v in people.items() if v}

def from_musicbrainz(artist: str, title: str) -> dict:
    rec = mb_search_recording(artist, title)
    if not rec: return {}
    out = {}
    # rok vydania
    if rec.get("releases"):
        y = year_from_date(rec["releases"][0].get("date"))
        if y: out["release_year"] = y
    # artist country
    ac = rec.get("artist-credit") or rec.get("artist_credits") or []
    if ac:
        a_id = (ac[0].get("artist") or {}).get("id")
        if a_id:
            time.sleep(0.35)
            cc = mb_artist_country_from_id(a_id)
            if cc: out["artist_country"] = cc
    # works → people
    works = [rel.get("work",{}).get("id") for rel in rec.get("relations",[]) if rel.get("type")=="work"]
    if not works:
        try:
            det = requests.get(
                f'https://musicbrainz.org/ws/2/recording/{rec["id"]}?inc=work-rels+artist-credits+releases&fmt=json',
                headers=MB_HEADERS, timeout=20).json()
            works = [rel.get("work",{}).get("id") for rel in det.get("relations",[]) if rel.get("type")=="work"]
            if "artist_country" not in out:
                ac2 = det.get("artist-credit") or []
                if ac2:
                    a2 = (ac2[0].get("artist") or {}).get("id")
                    if a2:
                        time.sleep(0.35)
                        cc2 = mb_artist_country_from_id(a2)
                        if cc2: out["artist_country"] = cc2
            if "release_year" not in out and det.get("releases"):
                y2 = year_from_date(det["releases"][0].get("date"))
                if y2: out["release_year"] = y2
        except Exception:
            pass
    comp, lyr, writ = set(), set(), set()
    for wid in works[:2]:
        if not wid: continue
        try:
            ppl = mb_work_people(wid)
            comp |= set(ppl.get("composers", []))
            lyr  |= set(ppl.get("lyricists", []))
            writ |= set(ppl.get("writers", []))
            time.sleep(0.35)
        except Exception:
            pass
    if comp: out["composers"] = sorted(comp)
    if lyr:  out["lyricists"] = sorted(lyr)
    if writ: out["writers"]   = sorted(writ)
    return {k:v for k,v in out.items() if v not in (None, "", [], 0)}

# ----- merge (bez cover_url/sources/duration_sec) -----

SCALAR_FIELDS = ("album","release_year","duration_ms","artist_country")
LIST_FIELDS   = ("composers","lyricists","writers","genres")
ALL_FIELDS    = SCALAR_FIELDS + LIST_FIELDS

def merge_meta(*dicts) -> dict:
    result = {}
    raw_genres = []
    for d in dicts:
        if not d: continue
        for k, v in d.items():
            if v in (None, "", [], 0): continue
            if k == "genres_raw":
                raw_genres.extend(v if isinstance(v, list) else [v])
            elif k in LIST_FIELDS:
                cur = set(result.get(k, []))
                add = set(v if isinstance(v, list) else [v])
                result[k] = sorted(cur | add)
            elif k in SCALAR_FIELDS:
                result.setdefault(k, v)
            # ignore any other keys (cover_url, sources, duration_sec ...)
    norm = normalize_genres(raw_genres)
    if norm: result["genres"] = norm
    return result

# ----- cache -----

def load_cache() -> dict:
    return load_json(CACHE_PATH, {})

def save_cache(cache: dict):
    save_json(CACHE_PATH, cache)

def needs_any(item: dict) -> bool:
    return any(item.get(k) in (None, "", [], 0) or k not in item for k in ALL_FIELDS)

# ----- main flow -----

def enrich_pair(artist: str, title: str) -> dict:
    mb = from_musicbrainz(artist, title); time.sleep(0.35)
    it = from_itunes(artist, title);      time.sleep(0.2)
    dz = from_deezer(artist, title)
    return merge_meta(mb, it, dz)

def apply_schema_with_nulls(item: dict, meta: dict):
    """
    Do záznamu doplní všetky polia z ALL_FIELDS.
    Ak meta ani pôvodný záznam nič nemajú → nastaví None (=> null v JSON).
    Zoznamy spája; ak výsledok prázdny → None.
    """
    # odstráň legacy duration_sec, ak by existovalo
    if "duration_sec" in item:
        del item["duration_sec"]

    # 1) scalary
    for k in SCALAR_FIELDS:
        if item.get(k) not in (None, "", [], 0):
            continue
        v = meta.get(k)
        item[k] = v if v not in (None, "", [], 0) else None

    # 2) listy (union)
    for k in LIST_FIELDS:
        existing = set(item.get(k) or [])
        incoming = set(meta.get(k) or [])
        merged = sorted(existing | incoming)
        item[k] = merged if merged else None

def run_backfill():
    playlist = load_json(PLAYLIST_PATH, [])
    cache = load_cache()
    touched, updated = 0, 0

    # ktorým dvojiciam ešte niečo chýba
    need_keys = set()
    for it in playlist:
        if needs_any(it):
            need_keys.add(norm_key(it["artist"], it["title"]))

    # doplň cache
    for k in sorted(need_keys):
        touched += 1
        if k not in cache:
            a, t = k.split("|", 1)
            cache[k] = enrich_pair(a, t) or {}
            time.sleep(0.25)
    if touched:
        save_cache(cache)

    # aplikuj do záznamov (a vynúť prítomnosť všetkých polí s null)
    for it in playlist:
        before = json.dumps({kk: it.get(kk) for kk in ALL_FIELDS}, ensure_ascii=False, sort_keys=True)
        key = norm_key(it["artist"], it["title"])
        meta = cache.get(key, {})
        apply_schema_with_nulls(it, meta)
        after  = json.dumps({kk: it.get(kk) for kk in ALL_FIELDS}, ensure_ascii=False, sort_keys=True)
        if before != after:
            updated += 1

    if updated:
        save_json(PLAYLIST_PATH, playlist)

    print(f"keys touched: {touched}, items updated: {updated}, cache size: {len(cache)}")

if __name__ == "__main__":
    run_backfill()
