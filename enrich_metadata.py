# -*- coding: utf-8 -*-
"""
Enrichment + backfill do data/playlist.json:
- album, release_year, duration_ms
- lyricists (zjednotený zoznam autorov: MB recording/work relácie + Deezer contributors)
- genres (normalizované do kanonických)
- artist_country (ISO-3166-1 alpha-2, napr. 'US', 'SK')

Chýbajúce údaje sa zapisujú ako null.
Nepoužívame: cover_url, sources, duration_sec.

Robustné voči výpadkom: retry + exponenciálny backoff + jitter.
MusicBrainz throttling: default 1.0 s medzi volaniami (MB_THROTTLE_SEC).

Env:
  PLAYLIST_PATH    = data/playlist.json
  CACHE_PATH       = data/meta_cache.json
  MAX_KEYS_PER_RUN = 120        # koľko (artist,title) spracovať za jeden beh; "0" = bez limitu
  MB_THROTTLE_SEC  = 1.0        # minimálny rozostup medzi MB volaniami
  MB_USER_AGENT    = 'melody-now/1.0 (contact: example@example.com)'
"""

import json, os, re, time, random, difflib, unicodedata
from urllib.parse import quote_plus
import requests

PLAYLIST_PATH     = os.environ.get("PLAYLIST_PATH", "data/playlist.json")
CACHE_PATH        = os.environ.get("CACHE_PATH", "data/meta_cache.json")
MAX_KEYS_PER_RUN  = int(os.environ.get("MAX_KEYS_PER_RUN", "120"))
MB_THROTTLE_SEC   = float(os.environ.get("MB_THROTTLE_SEC", "1.0"))
MB_USER_AGENT     = os.environ.get("MB_USER_AGENT", "melody-now/1.0 (contact: example@example.com)")

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
    s = s or ""
    s = s.lower()
    s = re.sub(r"\s*-\s*(remaster(?:ed)?(?: \d{4})?|mono|stereo|single|version|mix|edit|radio edit).*", "", s)
    s = re.sub(r"\s*\((?:feat\.?|featuring|with)\s+[^)]*\)", "", s)
    s = re.sub(r"\s*\((?:live|remaster(?:ed)?|version|mix|edit|radio edit|mono|stereo)[^)]*\)", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def clean_artist(s: str) -> str:
    s = s or ""
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

def ascii_fold(s: str) -> str:
    return unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode("ascii")

def _clean_name_basic(s: str) -> str:
    s = re.sub(r"\s+\(.*?\)", "", s or "", flags=re.IGNORECASE)
    s = re.sub(r"\s+(feat\.?|ft\.?|featuring|with)\s+.*$", "", s, flags=re.IGNORECASE)
    return " ".join(s.split())

def norm_name(s: str) -> str:
    return ascii_fold(_clean_name_basic(s)).casefold()

def norm_title_for_match(s: str) -> str:
    return ascii_fold(clean_title(s)).casefold()

def primary_artist(raw: str) -> str:
    s = _clean_name_basic((raw or "").strip())
    # ber prvú časť pri "A & B", "A × B", "A / B"...
    for sep in [" & ", " × ", " x ", " + ", " / ", ";", " and "]:
        if sep in s:
            s = s.split(sep, 1)[0].strip()
            break
    # "Patejdl, Vašo" -> "Vašo Patejdl"
    if "," in s:
        left, right = [p.strip() for p in s.split(",", 1)]
        if left and right:
            s = f"{right} {left}"
    return s

# ----------------------- HTTP helper s retry/backoff ------------------------

def safe_get_json(url, *, headers=None, timeout=25, retries=4, backoff=1.8, kind="HTTP"):
    """
    Bezpečné GET -> JSON s exponenciálnym backoffom + jitter.
    Vráti dict alebo None.
    """
    last_err = None
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            sleep = (backoff ** attempt) + random.uniform(0.0, 0.4)
            print(f"[warn] {kind} fetch failed (try {attempt+1}/{retries}) -> {type(e).__name__}: {e}; sleep {sleep:.2f}s")
            time.sleep(sleep)
    print(f"[error] {kind} fetch giving up for URL: {url}")
    return None

# MusicBrainz – šetrný throttling medzi volaniami
_MB_LAST_CALL = 0.0
MB_HEADERS = {"User-Agent": MB_USER_AGENT}

def mb_get_json(url):
    global _MB_LAST_CALL
    now = time.time()
    wait = MB_THROTTLE_SEC - (now - _MB_LAST_CALL)
    if wait > 0:
        time.sleep(wait)
    data = safe_get_json(url, headers=MB_HEADERS, timeout=25, retries=4, backoff=2.0, kind="MB")
    _MB_LAST_CALL = time.time()
    return data

# -------------------------- Normalizácia žánrov -----------------------------

_CANON = {
    "pop": ["pop","k-pop","kpop","j-pop","jpop","mandopop","cantopop","europop",
            "french pop","international pop","latin pop","synthpop","indie pop","dance pop","pop rock"],
    "rock": ["rock","hard rock","soft rock","alternative rock","alt rock","classic rock","indie rock","punk rock","metalcore"],
    "hip-hop": ["hip hop","hip-hop","rap","trap"],
    "r&b": ["r&b","r&b/soul","soul","neo-soul","contemporary r&b","conemporary r&b"],
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

# ------------------------------ iTunes --------------------------------------

def from_itunes(artist: str, title: str) -> dict:
    q = quote_plus(f"{artist} {title}")
    url = f"https://itunes.apple.com/search?term={q}&entity=song&limit=3&country=sk"
    j = safe_get_json(url, timeout=20, retries=3, backoff=1.7, kind="iTunes")
    if not j or not j.get("resultCount"): return {}
    a_norm, t_norm = norm_name(artist), norm_title_for_match(title)
    cand = None
    for x in j["results"]:
        if a_norm in norm_name(x.get("artistName","")):
            cand = x
            if t_norm in norm_title_for_match(x.get("trackName","")): break
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
    Deezer:
      - /search -> basic match
      - /album/{id} -> albumové žánre
      - /track/{id} -> contributors[] (role: composer/lyricist/writer/author)
    """
    j = safe_get_json(f'https://api.deezer.com/search?q=artist:"{artist}" track:"{title}"',
                      timeout=20, retries=3, backoff=1.7, kind="Deezer")
    if not j or not j.get("data"): return {}
    x = j["data"][0]
    album = x.get("album") or {}
    out = {
        "album": album.get("title"),
        "duration_ms": int(x["duration"])*1000 if x.get("duration") else None,
    }

    # album žánre
    if album.get("id"):
        aj = safe_get_json(f'https://api.deezer.com/album/{album["id"]}',
                           timeout=20, retries=3, backoff=1.7, kind="DeezerAlbum")
        if aj:
            g = [g["name"] for g in (aj.get("genres",{}).get("data") or []) if g.get("name")]
            if g: out["genres_raw"] = g

    # contributors => lyricists
    track_id = x.get("id")
    if track_id:
        tj = safe_get_json(f'https://api.deezer.com/track/{track_id}',
                           timeout=20, retries=3, backoff=1.7, kind="DeezerTrack")
        if tj:
            contribs = tj.get("contributors") or []
            names = []
            for c in contribs:
                role = (c.get("role") or "").strip().lower()
                if role in ("composer", "lyricist", "writer", "author"):
                    nm = c.get("name")
                    if nm: names.append(nm)
            if names:
                out["lyricists"] = sorted(set(names))

    return {k:v for k,v in out.items() if v not in (None, "", [], 0)}

# ---------------------------- MusicBrainz (ADV) ------------------------------

def mb_search_recording_id(artist: str, title: str) -> str | None:
    """
    Pokročilé vyhľadanie recording-u: kombinuje MB score + podobnosť názvu a prítomnosť interpreta.
    Vráti MBID recording-u alebo None.
    """
    a_prim = primary_artist(artist)
    q = f'artist:"{a_prim}" AND recording:"{title}"'
    url = f'https://musicbrainz.org/ws/2/recording/?query={quote_plus(q)}&fmt=json&limit=10'
    j = mb_get_json(url)
    recs = (j or {}).get("recordings") or []
    if not recs: return None

    want_t = norm_title_for_match(title)
    want_a = norm_name(a_prim)

    best_id, best_score = None, 0.0
    for rec in recs:
        rec_title = rec.get("title","")
        rec_score_mb = rec.get("score", 0) / 100.0
        title_ratio = difflib.SequenceMatcher(a=norm_title_for_match(rec_title), b=want_t).ratio()

        # bonus, ak medzi artist-credit je náš interpret
        artists = []
        for ac in rec.get("artist-credit", []):
            if isinstance(ac, dict) and "name" in ac:
                artists.append(ac.get("name", ""))
            elif isinstance(ac, str):
                artists.append(ac)
        artist_hit = any(difflib.SequenceMatcher(a=norm_name(nm), b=want_a).ratio() > 0.85 for nm in artists)

        score = 0.6 * title_ratio + 0.35 * rec_score_mb + (0.05 if artist_hit else 0.0)
        if score > best_score:
            best_id, best_score = rec.get("id"), score

    return best_id

def mb_recording_details(rec_id: str) -> dict | None:
    inc = "artists+releases+work-rels+artist-rels+recording-rels+work-level-rels+writers+composer+lyricist+relations"
    url = f'https://musicbrainz.org/ws/2/recording/{rec_id}?inc={quote_plus(inc)}&fmt=json'
    return mb_get_json(url)

def mb_work_details(work_id: str) -> dict | None:
    inc = "artist-rels+relations+writers+composer+lyricist"
    url = f'https://musicbrainz.org/ws/2/work/{work_id}?inc={quote_plus(inc)}&fmt=json'
    return mb_get_json(url)

def collect_names_from_rels(rels: list[dict]) -> set[str]:
    names = set()
    for rel in rels or []:
        if rel.get("type") in {"writer", "composer", "lyricist", "author"}:
            art = rel.get("artist") or {}
            nm = (art.get("name") or art.get("sort-name") or "").strip()
            if nm:
                names.add(nm)
    return names

def from_musicbrainz(artist: str, title: str) -> dict:
    """
    Získa:
      - release_year (z releases)
      - artist_country (z artist-credit -> artist.area.iso_3166_1_codes | country)
      - lyricists (z recording.relations + z naviazaných work-ov, max 2)
    """
    out = {}

    rec_id = mb_search_recording_id(artist, title)
    if not rec_id:
        # fallback: skús otočené poradie mena (Darina Rolincová vs Rolincová Darina)
        parts = primary_artist(artist).split()
        if len(parts) == 2:
            alt = f"{parts[1]} {parts[0]}"
            rec_id = mb_search_recording_id(alt, title)
    if not rec_id:
        return out

    det = mb_recording_details(rec_id)
    if not det:
        return out

    # release_year
    if det.get("releases"):
        y = year_from_date(det["releases"][0].get("date"))
        if y: out["release_year"] = y

    # artist_country
    ac = det.get("artist-credit") or []
    if ac:
        a_id = (ac[0].get("artist") or {}).get("id")
        if a_id:
            ajson = mb_get_json(f'https://musicbrainz.org/ws/2/artist/{a_id}?fmt=json')
            if ajson:
                country = ajson.get("country")
                if not country:
                    area = ajson.get("area") or {}
                    codes = area.get("iso_3166_1_codes") or area.get("iso-3166-1-codes") or []
                    country = codes[0] if codes else None
                if country: out["artist_country"] = country

    # lyricists z recording.relations
    names = collect_names_from_rels(det.get("relations") or [])

    # works -> doplň mená (max 2)
    work_rels = [rel for rel in (det.get("relations") or []) if rel.get("target-type") == "work"]
    for rel in work_rels[:2]:
        wid = (rel.get("work") or {}).get("id")
        if not wid: continue
        wdet = mb_work_details(wid)
        if wdet:
            names |= collect_names_from_rels(wdet.get("relations") or [])

    if names:
        out["lyricists"] = sorted(names)

    return out

# --------------------------- Spájanie výsledkov -----------------------------

SCALAR_FIELDS = ("album","release_year","duration_ms","artist_country")
# zoznamové polia už len "lyricists" a "genres"
LIST_FIELDS   = ("lyricists","genres")
ALL_FIELDS    = SCALAR_FIELDS + LIST_FIELDS

def merge_meta(*dicts) -> dict:
    """
    Zlučuje dáta zo zdrojov. Ak sa objavia 'composers'/'writers', mapujú sa do 'lyricists'.
    """
    result, raw_genres = {}, []
    lyricists_union = set()

    for d in dicts:
        if not d: continue
        # zozbieraj autorov zo všetkých možných kľúčov
        for role_key in ("composers", "lyricists", "writers"):
            vals = d.get(role_key) or []
            if isinstance(vals, list):
                lyricists_union |= set(vals)
            elif vals:
                lyricists_union.add(vals)
        # ostatné polia
        for k, v in d.items():
            if v in (None, "", [], 0): continue
            if k == "genres_raw":
                raw_genres.extend(v if isinstance(v, list) else [v])
            elif k in SCALAR_FIELDS:
                result.setdefault(k, v)

    if lyricists_union:
        result["lyricists"] = sorted(lyricists_union)

    norm = normalize_genres(raw_genres)
    if norm:
        result["genres"] = norm

    return result

# ------------------------------- Cache --------------------------------------

def load_cache() -> dict:
    return load_json(CACHE_PATH, {})

def save_cache(cache: dict):
    save_json(CACHE_PATH, cache)

def needs_any(item: dict) -> bool:
    return any(item.get(k) in (None, "", [], 0) or k not in item for k in ALL_FIELDS)

# ------------------------------ Hlavný tok ----------------------------------

def enrich_pair(artist: str, title: str) -> dict:
    # Poradie: MB (rok/krajina/autori) -> iTunes (album/rok/duration_ms/genre) -> Deezer (album/duration_ms/genres+contributors)
    mb = from_musicbrainz(artist, title)
    it = from_itunes(artist, title)
    dz = from_deezer(artist, title)
    return merge_meta(mb, it, dz)

def apply_schema_with_nulls(item: dict, meta: dict):
    """
    - odstráni legacy 'duration_sec'
    - zlúči existujúce item.composers/writers/lyricists -> item.lyricists
    - zmaže legacy 'composers' a 'writers'
    - doplní SCALAR_FIELDS a LIST_FIELDS; ak nič, nastaví null
    """
    if "duration_sec" in item:
        del item["duration_sec"]

    # 1) zlúč staré polia autorov do 'lyricists'
    legacy_set = set(item.get("lyricists") or [])
    for old in ("composers", "writers"):
        legacy_set |= set(item.get(old) or [])
    incoming = set(meta.get("lyricists") or [])
    merged_authors = sorted(legacy_set | incoming)
    item["lyricists"] = merged_authors if merged_authors else None
    # odstráň legacy kľúče
    if "composers" in item: del item["composers"]
    if "writers" in item:   del item["writers"]

    # 2) scalary: doplň alebo null
    for k in SCALAR_FIELDS:
        if item.get(k) not in (None, "", [], 0):
            continue
        v = meta.get(k)
        item[k] = v if v not in (None, "", [], 0) else None

    # 3) genres: union; ak prázdne -> null
    existing_genres = set(item.get("genres") or [])
    incoming_genres = set(meta.get("genres") or [])
    genres_merged = sorted(existing_genres | incoming_genres)
    item["genres"] = genres_merged if genres_merged else None

def run_backfill():
    playlist = load_json(PLAYLIST_PATH, [])
    cache = load_cache()
    touched, updated = 0, 0

    # zozbieraj potrebné kľúče
    need_keys = []
    seen = set()
    for it in playlist:
        if needs_any(it):
            k = norm_key(it["artist"], it["title"])
            if k not in seen:
                need_keys.append(k)
                seen.add(k)

    # obmedz rozsah na beh
    if MAX_KEYS_PER_RUN and len(need_keys) > MAX_KEYS_PER_RUN:
        print(f"[info] limiting to first {MAX_KEYS_PER_RUN} keys (of {len(need_keys)}) this run")
        need_keys = need_keys[:MAX_KEYS_PER_RUN]

    # doplň cache
    for k in need_keys:
        touched += 1
        if k not in cache:
            try:
                a, t = k.split("|", 1)
                cache[k] = enrich_pair(a, t) or {}
            except Exception as e:
                print(f"[error] enrich_pair failed for {k}: {type(e).__name__}: {e}")
                cache[k] = {}
    if touched:
        save_cache(cache)

    # aplikuj do záznamov – vynúť ALL_FIELDS, legacy cleanup
    for it in playlist:
        before = json.dumps({kk: it.get(kk) for kk in ALL_FIELDS + ("composers","writers")}, ensure_ascii=False, sort_keys=True)
        key = norm_key(it["artist"], it["title"])
        meta = cache.get(key, {})
        apply_schema_with_nulls(it, meta)
        after  = json.dumps({kk: it.get(kk) for kk in ALL_FIELDS + ("composers","writers")}, ensure_ascii=False, sort_keys=True)
        if before != after:
            updated += 1

    if updated:
        save_json(PLAYLIST_PATH, playlist)

    print(f"keys touched: {touched}, items updated: {updated}, cache size: {len(cache)}")

if __name__ == "__main__":
    run_backfill()
