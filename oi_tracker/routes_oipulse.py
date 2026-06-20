"""
oi_tracker / routes_oipulse.py
-------------------------------
FastAPI routes for the OI Pulse page.

Register in main.py ABOVE the StaticFiles mount:

    from oi_tracker.routes_oipulse import router as oipulse_router
    app.include_router(oipulse_router)

Endpoints:
    GET  /oipulse               -> OI Pulse page (HTML)
    GET  /oipulse/api/snapshot  -> poll, return JSON snapshot history + status
"""

import logging
import os

from fastapi import APIRouter
from fastapi.responses import HTMLResponse, JSONResponse

from . import poller_pulse

log = logging.getLogger("oi_tracker.routes_oipulse")

router = APIRouter(prefix="/oipulse")
_TEMPLATE = os.path.join(os.path.dirname(__file__), "templates", "oipulse.html")


def _render() -> str:
    with open(_TEMPLATE, "r") as f:
        return f.read()


@router.get("", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse)
def oipulse_tab():
    return HTMLResponse(_render())


@router.get("/api/snapshot")
async def api_snapshot():
    # Lazy import avoids circular dependency (main imports this module at startup).
    import main as _main
    nfo_df = None
    try:
        nfo_df = await _main._get_exchange_df("NFO")
    except Exception as exc:
        log.warning("Could not fetch NFO instrument data: %s", exc)

    result = await poller_pulse.poll_once(nfo_df)
    return JSONResponse(result)
