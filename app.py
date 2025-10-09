# -*- coding: utf-8 -*-
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from melody_core import get_now_playing

app = FastAPI(title="Melody Now - live")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # prípadne nahraď svojou doménou
    allow_methods=["GET"],
    allow_headers=["*"],
)

@app.get("/now")
def now():
    """Živá aktuálna pesnička + odhad počúvanosti."""
    return get_now_playing()
