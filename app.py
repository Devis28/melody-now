from fastapi import FastAPI, Query, Response, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from melody_core import get_now_playing
from fastapi.responses import PlainTextResponse
import os

app = FastAPI(title="Melody Now - live", version="0.2.0")
STATION_NAME = os.getenv("STATION_NAME", "Rádio Melody")

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

    # 'station' bude prvý kľúč v odpovedi
    return {"station": STATION_NAME, **data}

@app.get("/listeners")
def listeners(
    ts: int | None = Query(default=None, description="client Date.now() in ms"),
    response: Response = None,
):
    data = get_now_playing(override_ts=ts)
    _no_cache(response)
    return {"listeners": int(data.get("listeners", 0))}
