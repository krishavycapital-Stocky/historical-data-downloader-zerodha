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

import asyncio
import datetime as dt
import logging
import os
from zoneinfo import ZoneInfo

from fastapi import APIRouter
from fastapi.responses import HTMLResponse, JSONResponse

from . import poller_pulse

log = logging.getLogger("oi_tracker.routes_oipulse")

IST = ZoneInfo("Asia/Kolkata")
router = APIRouter(prefix="/oipulse")
_TEMPLATE = os.path.join(os.path.dirname(__file__), "templates", "oipulse.html")

# Once-per-day backfill state (single async event loop — no lock needed)
_backfill_done_for: dt.date | None = None
_backfilling: bool = False
_backfill_msg: str = ""


def _render() -> str:
    with open(_TEMPLATE, "r") as f:
        return f.read()


@router.get("", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse)
def oipulse_tab():
    return HTMLResponse(_render())


@router.get("/api/snapshot")
async def api_snapshot():
    global _backfill_done_for, _backfilling, _backfill_msg

    # Lazy import avoids circular dependency (main imports this module at startup).
    import main as _main
    nfo_df = None
    try:
        nfo_df = await _main._get_exchange_df("NFO")
    except Exception as exc:
        log.warning("Could not fetch NFO instrument data: %s", exc)

    # ── Once-per-day backfill from 09:15 IST ─────────────────────────────
    today_ist = dt.datetime.now(tz=IST).date()
    if (nfo_df is not None
            and not nfo_df.empty
            and _backfill_done_for != today_ist
            and not _backfilling):
        # Only set _backfilling=True here; _backfill_done_for is set ONLY on success
        _backfilling = True
        _backfill_msg = "Backfilling 09:15 → now…"

        from .poller_backfill import backfill_today

        async def _run_backfill():
            global _backfill_done_for, _backfilling, _backfill_msg
            try:
                count = await backfill_today(nfo_df)
                if count > 0:
                    _backfill_done_for = today_ist   # success — don't retry
                    _backfill_msg = f"Morning backfilled: {count} bars"
                    log.info("Backfill succeeded: %d bars written", count)
                else:
                    # count == 0 means something went wrong; leave _backfill_done_for
                    # unchanged so the next poll will retry
                    _backfill_msg = "Backfill failed — will retry"
                    log.warning("Backfill returned 0 — will retry on next poll")
            except Exception as exc:
                _backfill_msg = "Backfill failed — will retry"
                log.warning("Backfill error: %s — will retry on next poll", exc)
            finally:
                _backfilling = False

        asyncio.create_task(_run_backfill())

    # Return live snapshot immediately; backfilled history appears in later polls.
    result = await poller_pulse.poll_once(nfo_df)
    result["status"]["backfilling"] = _backfilling
    result["status"]["backfill"] = _backfill_msg
    return JSONResponse(result)
