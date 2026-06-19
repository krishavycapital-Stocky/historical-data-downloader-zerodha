"""
oi_tracker / routes_fastapi.py
------------------------------
FastAPI routes for the live OI tab.

Register this router in main.py ABOVE the StaticFiles mount:

    from oi_tracker.routes_fastapi import router as oi_router
    app.include_router(oi_router)

Endpoints:
    GET  /oi               -> the OI tab (HTML)
    GET  /oi/api/snapshot  -> poll Kite REST, return JSON rows
    GET  /oi/api/status    -> token / config health check
"""

import os
import logging

from fastapi import APIRouter, Query
from fastapi.responses import HTMLResponse, JSONResponse
from typing import Optional

from . import config
from . import token_store
from . import poller

log = logging.getLogger("oi_tracker.routes_fastapi")

router = APIRouter(prefix="/oi")
_TEMPLATE = os.path.join(os.path.dirname(__file__), "templates", "oi_tab.html")


def _render():
    with open(_TEMPLATE, "r") as f:
        html = f.read()
    html = html.replace("{{ interval }}", str(config.SNAPSHOT_INTERVAL_SEC))
    html = html.replace("{{ instrument_count }}", str(len(config.TOKENS)))
    return html


@router.get("", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse)
def oi_tab():
    return HTMLResponse(_render())


@router.get("/api/snapshot")
async def api_snapshot(
    api_key: Optional[str] = Query(default=None),
    access_token: Optional[str] = Query(default=None),
):
    result = await poller.poll_once(api_key=api_key, access_token=access_token)
    return JSONResponse(result)


@router.get("/api/status")
def api_status():
    meta = token_store.get_meta()
    return JSONResponse({
        "server_has_token": meta["has_token"],
        "server_has_api_key": meta["has_api_key"],
        "instrument_count": len(config.TOKENS),
        "interval_sec": config.SNAPSHOT_INTERVAL_SEC,
    })
