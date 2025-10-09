from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from melody_core import get_now_playing

app = FastAPI(title="Melody Now - live", version="0.1.1")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {"status": "ok", "try": "/now"}

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/now", summary="Now", description="Živá aktuálna pesnička + odhad počúvanosti.")
def now():
    return get_now_playing()
