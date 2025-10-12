from fastapi import FastAPI, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from melody_core import get_now_playing

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

@app.get("/noww")
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
