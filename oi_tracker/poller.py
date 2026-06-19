"""
oi_tracker / poller.py
----------------------
On-demand REST poller. No background threads, no WebSocket.

When the browser hits /oi/api/snapshot, poll_once() is called:
  1. Reads api_key + access_token from token_store (written there by
     main.py's generate_token endpoint at login time).
  2. Calls Kite GET /quote for every configured instrument token.
  3. Updates in-memory prev and day-open baselines.
  4. Returns rows for the table.

Day-open baseline: the first OI value we see each day is stored as the
baseline. If it was captured after 9:20 IST, the column header says
"since HH:MM" instead of "day open" so you're never shown a misleading number.

3-min delta: None (rendered as "—") if there's no previous snapshot yet.
"""

import asyncio
import datetime as dt
import logging
import time
import threading

import httpx

from . import config
from . import token_store

log = logging.getLogger("oi_tracker.poller")

KITE_BASE = "https://api.kite.trade"

_LOCK = threading.Lock()
_PREV = {}       # token -> {"oi": int, "ltp": float, "ts": float}
_DAY_OPEN = {}   # token -> {"oi": int, "ltp": float, "captured_at": datetime}
_TODAY = None    # date, reset trigger


def _reset_if_new_day():
    global _TODAY
    today = dt.date.today()
    if _TODAY != today:
        _TODAY = today
        _PREV.clear()
        _DAY_OPEN.clear()
        log.info("New trading day %s — baselines and prev reset.", today)


def _baseline_label(captured_at: dt.datetime) -> str:
    """Return 'day open' if captured near 9:15, else 'since HH:MM'."""
    cutoff = captured_at.replace(hour=9, minute=20, second=0, microsecond=0)
    if captured_at <= cutoff:
        return "day open"
    return f"since {captured_at.strftime('%H:%M')}"


async def poll_once(api_key: str = None, access_token: str = None) -> dict:
    """Fetch quotes for all configured instruments and return table rows + status."""
    # Browser passes keys from localStorage; fall back to server-side store
    api_key = api_key or token_store.load_api_key()
    access_token = access_token or token_store.load_access_token()

    if not api_key or not access_token:
        return {
            "rows": [],
            "status": {"error": "No token — log in on the main tab first.", "polled_at": None},
        }

    tokens = config.TOKENS
    if not tokens:
        return {
            "rows": [],
            "status": {"error": "No instruments configured. Set KITE_OI_TOKENS.", "polled_at": None},
        }

    params = [("i", str(t)) for t in tokens]
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                f"{KITE_BASE}/quote",
                params=params,
                headers={
                    "X-Kite-Version": "3",
                    "Authorization": f"token {api_key}:{access_token}",
                },
            )
    except Exception as e:
        log.warning("Quote fetch failed: %s", e)
        return {"rows": [], "status": {"error": f"Network error: {e}", "polled_at": None}}

    if resp.status_code != 200:
        return {
            "rows": [],
            "status": {"error": f"Kite API {resp.status_code}: {resp.text[:200]}", "polled_at": None},
        }

    data = resp.json().get("data", {})
    now = time.time()
    now_dt = dt.datetime.now()

    with _LOCK:
        _reset_if_new_day()

        rows = []
        for tok in tokens:
            label = config.LABELS.get(tok, str(tok))
            q = data.get(str(tok))

            if not q:
                rows.append({
                    "token": tok, "label": label,
                    "ltp": None, "oi": None,
                    "d_oi_prev": None, "d_oi_open": None,
                    "baseline_label": None,
                })
                continue

            ltp = q.get("last_price")
            oi = q.get("oi")

            # Set day-open baseline on first sighting
            if tok not in _DAY_OPEN and oi is not None:
                _DAY_OPEN[tok] = {"oi": oi, "ltp": ltp, "captured_at": now_dt}

            prev = _PREV.get(tok)
            d_oi_prev = (
                (oi - prev["oi"])
                if (prev and oi is not None and prev.get("oi") is not None)
                else None
            )

            base = _DAY_OPEN.get(tok)
            d_oi_open = (oi - base["oi"]) if (base and oi is not None) else None
            baseline_label = _baseline_label(base["captured_at"]) if base else None

            rows.append({
                "token": tok,
                "label": label,
                "ltp": ltp,
                "oi": oi,
                "d_oi_prev": d_oi_prev,
                "d_oi_open": d_oi_open,
                "baseline_label": baseline_label,
            })

            _PREV[tok] = {"oi": oi, "ltp": ltp, "ts": now}

    return {"rows": rows, "status": {"error": None, "polled_at": now}}
