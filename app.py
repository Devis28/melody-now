# app.py
from fastapi import FastAPI, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from melody_core import get_now_playing

app = FastAPI(title="Melody Now - live", version="0.1.0")

# (ak potrebuješ CORS pre GitHub Pages)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health(response: Response):
    response.headers["Cache-Control"] = "no-store, no-cache, max-age=0"
    return {"ok": True}

@app.get("/now")
def now(ts: int | None = Query(default=None, description="client ms since epoch"),
        response: Response = None):
    """
    ts = client-side Date.now() v milisekundách.
    Použijeme na 'live' jitter bucket a zároveň zneplatníme cache.
    """
    data = get_now_playing(override_ts=ts)
    if response is not None:
        response.headers["Cache-Control"] = "no-store, no-cache, max-age=0"
    return data
