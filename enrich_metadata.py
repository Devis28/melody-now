# -*- coding: utf-8 -*-
"""
Obohacovanie záznamov v data/playlist.json o:
- album
- release_year
- duration_ms (pozor: už NIE duration_sec)
- composers / lyricists / writers (MusicBrainz)
- artist_country (ISO-3166-1 alpha-2 kód krajiny interpreta)
- genres (zjednotené do kanonických kategórií)

Zdrojové API:
- MusicBrainz (MB): rok (z release), ISRC (voliteľne), autori (cez Work), krajina interpreta
- iTunes Search API: album, release_year, duration_ms, primárny žáner
- Deezer API: album, duration (prevedieme na ms), žánre z detailu albumu

NEPRIDÁVAME: cover_url, sources
NEPOUŽÍVAME: duration_sec

Spustenie:
    python enrich_metadata.py
"""

import json, os, re, time
from urllib.parse import quote_plus
import requests

PLAYLIST_PATH = os.environ.get("PLAYLIST_PATH", "data/playlist.json")
CACHE_PATH    = os.environ.get("CACHE_PATH", "data/meta_cache.json")

# ------------------------------ Pomocné -------------------------------------

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
    """Agresívnejšie čistenie názvu kvôli presnejšiemu matchu v API."""
    s = s.lower()
    s = re.sub(r"\s*-\s*(remaster(?:ed)?(?: \d{4})?|mono|stereo|single|version|mix|edit|radio edit).*", "", s)
    s = re.sub(r"\s*\((?:feat\.?|featuring|with)\s+[^)]*\)", "", s)  # feat(...)
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

def year_from_date(s: str | None):
    if not s: return None
    try: return int(s[:4])
    except Exception: return None

# -------------------------- Normalizácia žánrov -----------------------------

_CANON = {
    "pop": [
        "pop", "k-pop", "kpop", "j-pop", "jpop", "mandopop", "cantopop",
        "europop", "french pop", "international pop", "latin pop",
        "synthpop", "indie pop", "dance pop", "pop rock"  # pop-rock radšej do Pop
    ],
    "rock": [
        "rock", "hard rock", "soft rock", "alternative rock", "alt rock",
        "classic rock", "indie rock", "punk rock", "metalcore"
    ],
    "hip-hop": [
        "hip hop", "hip-hop", "rap", "trap"
    ],
    "r&b": [
        "r&b", "r&b/soul", "soul", "neo-soul", "contemporary r&b"
    ],
    "electronic": [
        "electronic", "edm", "dance", "house", "techno", "trance",
        "electro", "drum and bass", "dnb", "dubstep"
    ],
    "metal": ["metal", "heavy metal", "thrash metal", "death metal"],
    "classical": ["classical", "orchestral", "baroque", "symphony"],
    "jazz": ["jazz", "smooth jazz", "acid jazz"],
    "blues": ["blues"],
    "country": ["country"],
    "folk": ["folk", "singer-songwriter"],
    "reggae": ["reggae", "dancehall", "ska"],
}

# rýchly fallback podľa subreťazcov
_FALLBACK_RULES = [
    ("pop", "pop"),
    ("hip hop", "hip-hop"),
    ("hip-hop", "hip-hop"),
    ("rap", "hip-hop"),
    ("r&b", "r&b"),
    ("soul", "r&b"),
    ("rock", "rock"),
    ("metal", "metal"),
    ("jazz", "jazz"),
    ("blues", "blues"),
    ("country", "country"),
    ("folk", "folk"),
    ("reggae", "reggae"),
    ("dance", "electronic"),
    ("edm", "electronic"),
    ("house", "electronic"),
    ("techno", "electronic"),
    ("trance", "electronic"),
    ("electro", "electronic"),
    ("drum and bass", "electronic"),
    ("dubstep", "electronic"),
]

def _canon_display(name: str) -> str:
    # špeciálne veľké písmená pre Hip-Hop / R&B
    if name == "hip-hop": return "Hip-Hop"
    if name == "r&b": return "R&B"
    return name.capitalize()

def normalize_genres(genres: list[str]) -> list[str]:
    out = set()
    for g in genres or []:
        s = (g or "").strip().lower()
        if not s:
            continue
        # priame mapovanie
        mapped = None
        for canon, alts in _CANON.items():
            if s in alts:
                mapped = canon
                break
        # fallback cez substringy
        if not mapped:
            for needle, canon in _FALLBACK_RULES:
                if needle in s:
                    mapped = canon
                    break
        if mapped:
            out.add(mapped)
    return sorted((_canon_display(x) for x in out))

# ------------------------------ iTunes --------------------------------------

def from_itunes(artist: str, title: str) -> dict:
    """
    Bez API kľúča.
    Vracia: album, release_year, duration_ms, genres (nenormalizované).
    """
    q = quote_plus(f"{artist} {title}")
    url = f"https://itunes.apple.com/search?term={q}&entity=song&limit=3&country=sk"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    j = r.json()
    if not j.get("resultCount"):
        return {}
    a_norm = clean_artist(artist)
    t_norm = clean_title(title)
    cand = None
    for x in j["results"]:
        if a_norm in clean_artist(x.get("artistName","")):
            cand = x
            if t_norm in clean_title(x.get("trackName","")):
                break
    x = cand or j["results"][0]
    out = {
        "album": x.get("collectionName"),
        "release_year": year_from_date(x.get("releaseDate")),
        "duration_ms": x.get("trackTimeMillis"),
        "genres_raw": [x.get("primaryGenreName")] if x.get("primaryGenreName") else []
    }
    return {k:v for k,v in out.items() if v not in (None, [], "")}

# ------------------------------ Deezer --------------------------------------

def from_deezer(artist: str, title: str) -> dict:
    """
    Bez kľúča. Získame album, duration (prevedieme na ms), a žánre z detailu albumu.
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
        # Deezer dáva sekundy → prepočítame na ms (bez vytvárania duration_sec)
        "duration_ms": int(x["duration"]) * 1000 if x.get("duration") else None,
    }
    # žánre z albumu
    genres = []
    if album.get("id"):
        try:
            aj = requests.get(f'https://api.deezer.com/album/{album["id"]}', timeout=20).json()
            genres = [g["name"] for g in (aj.get("genres",{}).get("data") or []) if g.get("name")]
        except Exception:
            pass
    if genres:
        out["genres_raw"] = genres
    return {k:v for k,v in out.items() if v not in (None, [], "")}

# ---------------------------- MusicBrainz -----------------------------------

MB_HEADERS = {"User-Agent": "melody-now/1.0 (contact: example@example.com)"}

def mb_search_recording(artist: str, title: str) -> dict | None:
    q = f'recording:"{title}" AND artist:"{artist}"'
    url = f'https://musicbrainz.org/ws/2/recording/?query={quote_plus(q)}&fmt=json&limit=3'
    j = requests.get(url, headers=MB_HEADERS, timeout=20).json()
    recs = j.get("recordings") or []
    return recs[0] if recs else None

def mb_artist_country_from_id(artist_mbid: str) -> str | None:
    """
    Vráti ISO-3166-1 alpha-2 kód krajiny interpreta (napr. 'US', 'SK'), ak je k dispozícii.
    """
    try:
        url = f'https://musicbrainz.org/ws/2/artist/{artist_mbid}?fmt=json'
        j = requests.get(url, headers=MB_HEADERS, timeout=20).json()
        area = j.get("area") or {}
        codes = area.get("iso_3166_1_codes") or []
        if codes:
            return codes[0]
        # fallback: niektorí majú 'country' priamo (zriedkavé)
        if j.get("country"):
            return j["country"]
    except Exception:
        pass
    return None

def mb_work_people(work_mbid: str) -> dict:
    """
    Získa mená pre role composer/lyricist/writer z entity Work.
    """
    url = f'https://musicbrainz.org/ws/2/work/{work_mbid}?inc=artist-rels&fmt=json'
    j = requests.get(url, headers=MB_HEADERS, timeout=20).json()
    people = {"composers": [], "lyricists": [], "writers": []}
    for rel in j.get("relations", []):
        typ = rel.get("type")
        if typ in ("composer", "lyricist", "writer"):
            name = rel.get("artist", {}).get("name")
            if not name:
                continue
            people[typ + "s"].append(name)
    # uniquify & sort
    for k in people:
        people[k] = sorted(set(people[k]))
    return {k:v for k,v in people.items() if v}

def from_musicbrainz(artist: str, title: str) -> dict:
    """
    Vracia: release_year, (voliteľne isrc), composers/lyricists/writers, artist_country.
    """
    rec = mb_search_recording(artist, title)
    if not rec:
        return {}
    out = {}

    # rok vydania z prvého release (ak je)
    if rec.get("releases"):
        year = year_from_date(rec["releases"][0].get("date"))
        if year: out["release_year"] = year

    # autori cez Work
    works = [rel.get("work",{}).get("id") for rel in rec.get("relations",[]) if rel.get("type")=="work"]
    if not works:
        try:
            det = requests.get(
                f'https://musicbrainz.org/ws/2/recording/{rec["id"]}?inc=work-rels+artist-credits+releases+isrcs&fmt=json',
                headers=MB_HEADERS, timeout=20).json()
            works = [rel.get("work",{}).get("id") for rel in det.get("relations",[]) if rel.get("type")=="work"]
            # fallback pre rok
            if "release_year" not in out and det.get("releases"):
                year = year_from_date(det["releases"][0].get("date"))
                if year: out["release_year"] = year
            # artist country cez artist-credit
            ac = det.get("artist-credit") or det.get("artist_credits") or []
            if ac:
                a_id = (ac[0].get("artist") or {}).get("id")
                if a_id:
                    time.sleep(0.35)
                    cc = mb_artist_country_from_id(a_id)
                    if cc:
                        out["artist_country"] = cc
        except Exception:
            pass
    else:
        # artist country cez recording search (artist-credit môže byť už v rec)
        ac = rec.get("artist-credit") or rec.get("artist_credits") or []
        if ac:
            a_id = (ac[0].get("artist") or {}).get("id")
            if a_id:
                time.sleep(0.35)
                cc = mb_artist_country_from_id(a_id)
                if cc:
                    out["artist_country"] = cc

    # stiahni mená z Work entít (stačia 1–2)
    composers, lyricists, writers = set(), set(), set()
    for wid in works[:2]:
        if not wid:
            continue
        try:
            ppl = mb_work_people(wid)
            composers |= set(ppl.get("composers", []))
            lyricists |= set(ppl.get("lyricists", []))
            writers   |= set(ppl.get("writers", []))
            time.sleep(0.35)
        except Exception:
            pass
    if composers: out["composers"] = sorted(composers)
    if lyricists: out["lyricists"] = sorted(lyricists)
    if writers:   out["writers"]   = sorted(writers)

    return {k:v for k,v in out.items() if v not in (None, "", [], 0)}

# --------------------------- Spájanie výsledkov -----------------------------

# v merge už nekombinujeme "cover_url" ani "sources"; duration_sec ignorujeme
PREFERRED_ORDER = ("album", "release_year", "duration_ms",
                   "composers", "lyricists", "writers", "genres", "artist_country")

def merge_meta(*dicts) -> dict:
    result = {}
    raw_genres = []

    for d in dicts:
        if not d:
            continue
        for k, v in d.items():
            if v in (None, "", [], 0):
                continue
            if k == "genres_raw":
                raw_genres.extend(v if isinstance(v, list) else [v])
            elif k in ("genres", "cover_url", "sources", "duration_sec"):
                # nepoužívame priamo; genres spracujeme cez normalize
                continue
            elif k in ("composers", "lyricists", "writers"):
                cur = set(result.get(k, []))
                add = set(v if isinstance(v, list) else [v])
                result[k] = sorted(cur | add)
            else:
                result.setdefault(k, v)

    # normalizuj žánre
    norm = normalize_genres(raw_genres)
    if norm:
        result["genres"] = norm

    return result

# ------------------------------- Cache --------------------------------------

def load_cache() -> dict:
    return load_json(CACHE_PATH, {})

def save_cache(cache: dict):
    save_json(CACHE_PATH, cache)

def need_enrichment(item: dict) -> bool:
    targets = ("album", "release_year", "duration_ms",
               "composers", "lyricists", "writers", "genres", "artist_country")
    return any(k not in item or item.get(k) in (None, "", [], 0) for k in targets)

# ------------------------------ Hlavný tok ----------------------------------

def enrich_pair(artist: str, title: str) -> dict:
    """
    Poradie a fallback:
      1) MusicBrainz – release_year, composers/lyricists/writers, artist_country
      2) iTunes      – album, release_year, duration_ms, genres_raw
      3) Deezer      – album, duration_ms (z sekúnd), genres_raw (z albumu)
    """
    mb  = from_musicbrainz(artist, title); time.sleep(0.35)
    it  = from_itunes(artist, title);      time.sleep(0.2)
    dz  = from_deezer(artist, title);      # time.sleep(0.2) voliteľné
    return merge_meta(mb, it, dz)

def run_backfill():
    playlist = load_json(PLAYLIST_PATH, [])
    cache = load_cache()
    changed_items = 0
    touched_keys = 0

    # unikátne dvojice, ktoré niečo potrebujú doplniť
    need_keys = set()
    for it in playlist:
        if need_enrichment(it):
            need_keys.add(norm_key(it["artist"], it["title"]))

    # doplnenie cache
    for k in sorted(need_keys):
        touched_keys += 1
        if k not in cache:
            a, t = k.split("|", 1)
            meta = enrich_pair(a, t)
            cache[k] = meta or {}
            time.sleep(0.25)  # ohľaduplne
    if touched_keys:
        save_cache(cache)

    # aplikácia do záznamov
    for it in playlist:
        if not need_enrichment(it):
            continue
        key = norm_key(it["artist"], it["title"])
        meta = cache.get(key, {})
        if not meta:
            continue

        before = json.dumps({k: it.get(k) for k in PREFERRED_ORDER}, ensure_ascii=False, sort_keys=True)

        # dopĺňaj len chýbajúce / prázdne hodnoty; zoznamy zluč
        for k, v in meta.items():
            if k in ("composers", "lyricists", "writers", "genres"):
                cur = set(it.get(k, []))
                add = set(v)
                if add - cur:
                    it[k] = sorted(cur | add)
            else:
                if k not in it or it[k] in (None, "", [], 0):
                    it[k] = v

        # odstraň legacy duration_sec, ak by tam bolo
        if "duration_sec" in it:
            del it["duration_sec"]

        after = json.dumps({k: it.get(k) for k in PREFERRED_ORDER}, ensure_ascii=False, sort_keys=True)
        if before != after:
            changed_items += 1

    if changed_items:
        save_json(PLAYLIST_PATH, playlist)

    print(f"Pairs touched: {touched_keys}, items updated: {changed_items}, cache size: {len(cache)}")

if __name__ == "__main__":
    run_backfill()
