"""
oi_tracker / ticker_worker.py
-----------------------------
Opens ONE Kite WebSocket (KiteTicker) in FULL mode using the api_key + access
token held SERVER-SIDE (see token_store.py), subscribes to your instrument list,
and keeps the latest tick per instrument in memory.

Safety for your constraint:
  * One connection per process (singleton-guarded). Kite allows up to 3
    simultaneous connections per api_key, so this coexists with your historical
    tab's HTTP calls.
  * Runs in a background thread (threaded=True) — never blocks FastAPI/uvicorn.
  * If the token is missing/rejected it logs and STOPS. It never re-logs-in, so
    it can never invalidate your historical session.
  * Can be (re)started when the browser primes a token, and restarted if a new
    day's token replaces the old one.
"""

import logging
import threading
import time

from kiteconnect import KiteTicker

from . import config
from . import token_store

log = logging.getLogger("oi_tracker.ticker")

_LOCK = threading.Lock()
LATEST = {}            # instrument_token -> dict(...)
STATUS = {
    "running": False,
    "connected": False,
    "token_loaded": False,
    "api_key_loaded": False,
    "last_tick_ts": None,
    "last_error": None,
    "instrument_count": len(config.TOKENS),
}

_ticker = None
_active_token = None
_start_lock = threading.Lock()


def _on_ticks(ws, ticks):
    now = time.time()
    with _LOCK:
        for t in ticks:
            tok = t.get("instrument_token")
            if tok is None:
                continue
            ohlc = t.get("ohlc") or {}
            LATEST[tok] = {
                "ltp": t.get("last_price"),
                "oi": t.get("oi"),
                "oi_day_high": t.get("oi_day_high"),
                "oi_day_low": t.get("oi_day_low"),
                "day_open": ohlc.get("open"),
                "prev_close": ohlc.get("close"),
                "volume": t.get("volume_traded"),
                "ts": now,
            }
        STATUS["last_tick_ts"] = now


def _on_connect(ws, response):
    log.info("KiteTicker connected. Subscribing to %d instruments.", len(config.TOKENS))
    STATUS["connected"] = True
    if config.TOKENS:
        ws.subscribe(config.TOKENS)
        ws.set_mode(ws.MODE_FULL, config.TOKENS)   # FULL = the only mode carrying oi
    else:
        log.warning("No instrument tokens configured (set KITE_OI_TOKENS or config.INSTRUMENTS).")


def _on_close(ws, code, reason):
    log.warning("KiteTicker closed: %s %s", code, reason)
    STATUS["connected"] = False


def _on_error(ws, code, reason):
    log.error("KiteTicker error: %s %s", code, reason)
    STATUS["connected"] = False
    STATUS["last_error"] = f"{code}: {reason}"


def _on_reconnect(ws, attempts):
    log.info("KiteTicker reconnecting… attempt %s", attempts)


def _on_noreconnect(ws):
    log.error("KiteTicker stopped reconnecting (token may be stale for the day).")
    STATUS["connected"] = False
    STATUS["last_error"] = "websocket could not reconnect (token may be stale)"


def start_ticker():
    """
    Start (or restart on a new token) the single background ticker.
    Returns True if a ticker is running. Safe to call repeatedly — it only acts
    when something actually needs to start or the token changed.
    """
    global _ticker, _active_token
    with _start_lock:
        api_key = token_store.load_api_key()
        access_token = token_store.load_access_token()
        STATUS["api_key_loaded"] = bool(api_key)
        STATUS["token_loaded"] = bool(access_token)

        if not api_key:
            STATUS["last_error"] = "api_key not available yet"
            return False
        if not access_token:
            STATUS["last_error"] = "access token not available yet — open the OI tab to reuse your session"
            return False

        # Already running on the same token? nothing to do.
        if STATUS["running"] and _active_token == access_token:
            return True

        # Token changed (e.g. new trading day) — close the old connection first.
        if _ticker is not None:
            try:
                _ticker.close()
            except Exception:  # noqa
                pass
            _ticker = None
            STATUS["running"] = False
            STATUS["connected"] = False

        kws = KiteTicker(api_key, access_token)
        kws.on_ticks = _on_ticks
        kws.on_connect = _on_connect
        kws.on_close = _on_close
        kws.on_error = _on_error
        kws.on_reconnect = _on_reconnect
        kws.on_noreconnect = _on_noreconnect
        kws.connect(threaded=True)   # own reactor thread; auto-reconnect on by default

        _ticker = kws
        _active_token = access_token
        STATUS["running"] = True
        STATUS["last_error"] = None
        log.info("OI ticker started in background thread.")
        return True


def get_latest_snapshot():
    with _LOCK:
        return {tok: dict(v) for tok, v in LATEST.items()}


def get_status():
    with _LOCK:
        return dict(STATUS)
