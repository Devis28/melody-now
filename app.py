from fastapi import FastAPI, Query, Response, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from melody_core import get_now_playing
from fastapi.responses import PlainTextResponse
import  os
from fastapi import WebSocket, WebSocketDisconnect
import asyncio


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
    out = {"station": STATION_NAME}
    out.update(data)
    return out

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


@app.websocket("/ws/now")
@app.websocket("/ws/now/")
async def ws_now(ws: WebSocket):
    await ws.accept()
    last = None
    try:
        while True:
            d = get_now_playing()
            payload = {"station": STATION_NAME, **d}
            if payload != last:
                await ws.send_json(payload)
                last = payload
            await asyncio.sleep(5)
    except WebSocketDisconnect:
        pass

# jednoduchý testovací WS – pošle „pong“ a zatvorí
@app.websocket("/ws/ping")
async def ws_ping(ws: WebSocket):
    await ws.accept()
    await ws.send_text("pong")
    await ws.close()

# voliteľné: rýchly prehľad trás (pre debug v prehliadači)
@app.get("/_routes")
def _routes():
    return [getattr(r, "path", None) or getattr(r, "path_format", None)
            for r in app.routes]