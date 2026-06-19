"""
oi_tracker / routes_fastapi.py
------------------------------
FastAPI wiring for the live OI tab. Your app is FastAPI, so use THIS file.

IMPORTANT — placement: your main.py ends with
    app.mount("/", StaticFiles(directory="static", html=True), name="static")
That mount is greedy and will shadow anything added after it. Register this
router ABOVE that line. Concretely, just before the "# Serve frontend" /
app.mount line near the bottom of main.py, add:

    from oi_tracker.routes_fastapi import router as oi_router, init_oi
    app.include_router(oi_router)

    @app.on_event("startup")
    async def _start_oi():
        init_oi()

Endpoints:
    GET  /oi                 -> the new tab (HTML)
    POST /oi/api/set-token   -> browser hands its EXISTING token to the server
    GET  /oi/api/snapshot    -> JSON rows for the table (page polls this)
    GET  /oi/api/status      -> health / connection info
"""

import logging
import os
from fastapi import APIRouter
from fastapi.responses import JSONResponse, HTMLResponse
from pydantic import BaseModel

from . import config
from . import token_store
from .ticker_worker import start_ticker, get_status
from .snapshotter import start_snapshotter, get_view

log = logging.getLogger("oi_tracker.routes_fastapi")

router = APIRouter(prefix="/oi")
_initialized = False
_TEMPLATE = os.path.join(os.path.dirname(__file__), "templates", "oi_tab.html")


def init_oi():
    """Start the snapshot loop at boot. The ticker starts as soon as the
    browser primes a token (or immediately if a token is already stored)."""
    global _initialized
    if _initialized:
        return
    _initialized = True
    start_snapshotter()
    start_ticker()   # will no-op gracefully if no token yet


class SetToken(BaseModel):
    access_token: str
    api_key: str | None = None


@router.post("/api/set-token")
def set_token(body: SetToken):
    """
    Receive the token the browser ALREADY has (from your existing login) and
    store it server-side so the WebSocket can reuse it. This does NOT log in.
    """
    ok = token_store.save_token(body.access_token, body.api_key)
    if not ok:
        return JSONResponse({"ok": False, "error": "empty token"}, status_code=400)
    started = start_ticker()   # start/restart the WebSocket now that we have a token
    return {"ok": True, "ticker_started": started, "status": get_status()}


def _render():
    with open(_TEMPLATE, "r") as f:
        html = f.read()
    html = html.replace("{{ interval }}", str(config.SNAPSHOT_INTERVAL_SEC))
    html = html.replace("{{ instrument_count }}", str(len(config.TOKENS)))
    return html


@router.get("", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse)
def oi_tab():
    init_oi()
    return HTMLResponse(_render())


@router.get("/api/snapshot")
def api_snapshot():
    return JSONResponse({"rows": get_view(), "status": get_status()})


@router.get("/api/status")
def api_status():
    meta = token_store.get_meta()
    s = get_status()
    s.update({"server_has_token": meta["has_token"], "server_has_api_key": meta["has_api_key"]})
    return JSONResponse(s)
