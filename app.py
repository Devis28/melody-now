from fastapi import FastAPI, Query, Response, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from melody_core import get_now_playing
from fastapi.responses import PlainTextResponse

app = FastAPI(title="Melody Now - live", version="0.2.0")

# ak potrebuješ CORS pre GitHub Pages
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def _no_cache(resp: Response):
    resp.headers["Cache-Control"] = "no-store, no-cache, max-age=0, must-revalidate"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"

@app.get("/health")
def health(response: Response):
    _no_cache(response)
    return {"ok": True}

@app.get("/now")
def now(
    ts: int | None = Query(default=None, description="client Date.now() in ms"),
    debug: int | None = Query(default=None),
    response: Response = None
):
    """Živá skladba. `ts` spôsobí jemnú (±2 %) zmenu na každý klik."""
    data = get_now_playing(override_ts=ts, debug=bool(debug))
    if response is not None:
        _no_cache(response)
    return data

@app.get("/listeners", response_class=PlainTextResponse)
def listeners_plain(
    ts: int | None = Query(default=None, description="client Date.now() in ms"),
    response: Response = None
):
    try:
        data = get_now_playing(override_ts=ts)
        n = int(data.get("listeners", 0))
    except Exception:
        # keď sa nepodarí dohľadať, vráť HTTP 503
        raise HTTPException(status_code=503, detail="listeners unavailable")
    if response is not None:
        _no_cache(response)
    return str(n)
