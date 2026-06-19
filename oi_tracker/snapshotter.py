"""
oi_tracker / snapshotter.py
---------------------------
Every ~3 minutes (aligned to the wall clock) this takes a snapshot of the
latest OI for every instrument, then computes:

  * OI change vs the PREVIOUS snapshot
  * OI change vs the DAY-OPEN baseline
  * price change vs the previous snapshot
  * a price-vs-OI BUILDUP classification

About "day-open OI": Kite's full-mode feed gives current `oi`, `oi_day_high`,
`oi_day_low`, and the price `ohlc.open` — but NOT an explicit "OI at 9:15".
So we use the first OI we observe each day as the day-open baseline. That is
the standard practical approach; it's accurate as long as the tracker is
running near the open. (If you start it mid-session, the baseline is "OI when
tracking began" and the table labels it honestly.)

Buildup logic (over previous -> current interval):

    price up   + OI up    -> Long Buildup      (fresh longs, bullish)
    price down + OI up    -> Short Buildup      (fresh shorts, bearish)
    price up   + OI down  -> Short Covering     (shorts exiting, bullish-lite)
    price down + OI down  -> Long Unwinding     (longs exiting, bearish-lite)
    (moves under the epsilon thresholds) -> Neutral
"""

import csv
import datetime as dt
import logging
import os
import threading
import time

from . import config
from .ticker_worker import get_latest_snapshot

log = logging.getLogger("oi_tracker.snapshotter")

_LOCK = threading.Lock()
# Per instrument: list of snapshot dicts (oldest -> newest), capped at MAX_SNAPSHOTS.
_HISTORY = {}                 # token -> [ {ts, oi, ltp, ...}, ... ]
_DAY_OPEN_OI = {}             # token -> first observed OI today
_DAY_OPEN_PRICE = {}          # token -> price ohlc.open (from feed) or first ltp
_CURRENT_DAY = None           # date of the baseline, to reset at a new session
_snap_thread = None
_thread_started = False


# --------------------------------------------------------------------------- #
# Buildup classification
# --------------------------------------------------------------------------- #
def classify_buildup(d_price, d_oi):
    if d_price is None or d_oi is None:
        return "—"
    if abs(d_oi) <= config.OI_EPSILON and abs(d_price) <= config.PRICE_EPSILON:
        return "Neutral"
    if d_price > config.PRICE_EPSILON and d_oi > config.OI_EPSILON:
        return "Long Buildup"
    if d_price < -config.PRICE_EPSILON and d_oi > config.OI_EPSILON:
        return "Short Buildup"
    if d_price > config.PRICE_EPSILON and d_oi < -config.OI_EPSILON:
        return "Short Covering"
    if d_price < -config.PRICE_EPSILON and d_oi < -config.OI_EPSILON:
        return "Long Unwinding"
    return "Neutral"


# --------------------------------------------------------------------------- #
# Day baseline handling
# --------------------------------------------------------------------------- #
def _maybe_reset_day(live):
    """Reset the day-open baselines when a new trading day's data starts."""
    global _CURRENT_DAY
    today = dt.date.today()
    if _CURRENT_DAY != today:
        _CURRENT_DAY = today
        _HISTORY.clear()
        _DAY_OPEN_OI.clear()
        _DAY_OPEN_PRICE.clear()
        log.info("New trading day %s — baselines reset.", today)

    for tok, v in live.items():
        if tok not in _DAY_OPEN_OI and v.get("oi") is not None:
            _DAY_OPEN_OI[tok] = v["oi"]
        if tok not in _DAY_OPEN_PRICE:
            _DAY_OPEN_PRICE[tok] = v.get("day_open") or v.get("ltp")


# --------------------------------------------------------------------------- #
# Take one snapshot
# --------------------------------------------------------------------------- #
def take_snapshot():
    live = get_latest_snapshot()
    if not live:
        log.debug("No live ticks yet; skipping snapshot.")
        return None

    now = time.time()
    with _LOCK:
        _maybe_reset_day(live)
        snap_rows = []
        for tok in config.TOKENS:
            v = live.get(tok)
            if not v:
                continue
            hist = _HISTORY.setdefault(tok, [])
            prev = hist[-1] if hist else None

            oi = v.get("oi")
            ltp = v.get("ltp")

            d_oi_prev = (oi - prev["oi"]) if (prev and oi is not None and prev.get("oi") is not None) else None
            d_price_prev = (ltp - prev["ltp"]) if (prev and ltp is not None and prev.get("ltp") is not None) else None

            base_oi = _DAY_OPEN_OI.get(tok)
            base_price = _DAY_OPEN_PRICE.get(tok)
            d_oi_open = (oi - base_oi) if (oi is not None and base_oi is not None) else None
            d_price_open = (ltp - base_price) if (ltp is not None and base_price is not None) else None

            row = {
                "token": tok,
                "label": config.LABELS.get(tok, str(tok)),
                "ts": now,
                "oi": oi,
                "ltp": ltp,
                "oi_day_high": v.get("oi_day_high"),
                "oi_day_low": v.get("oi_day_low"),
                "volume": v.get("volume"),
                "d_oi_prev": d_oi_prev,
                "d_price_prev": d_price_prev,
                "d_oi_open": d_oi_open,
                "d_price_open": d_price_open,
                "base_oi": base_oi,
                "buildup": classify_buildup(d_price_prev, d_oi_prev),
            }
            hist.append({"ts": now, "oi": oi, "ltp": ltp})
            if len(hist) > config.MAX_SNAPSHOTS:
                del hist[0:len(hist) - config.MAX_SNAPSHOTS]
            snap_rows.append(row)

    _append_csv(snap_rows)
    log.info("Snapshot taken: %d instruments.", len(snap_rows))
    return snap_rows


def _append_csv(rows):
    if not config.CSV_LOG_DIR or not rows:
        return
    try:
        os.makedirs(config.CSV_LOG_DIR, exist_ok=True)
        fname = os.path.join(
            config.CSV_LOG_DIR, f"oi_{dt.date.today().isoformat()}.csv"
        )
        new = not os.path.exists(fname)
        with open(fname, "a", newline="") as f:
            w = csv.writer(f)
            if new:
                w.writerow(["time", "token", "label", "oi", "ltp",
                            "d_oi_prev", "d_price_prev", "d_oi_open",
                            "d_price_open", "buildup"])
            ts_str = dt.datetime.now().strftime("%H:%M:%S")
            for r in rows:
                w.writerow([ts_str, r["token"], r["label"], r["oi"], r["ltp"],
                            r["d_oi_prev"], r["d_price_prev"], r["d_oi_open"],
                            r["d_price_open"], r["buildup"]])
    except Exception as e:  # noqa
        log.warning("CSV append failed: %s", e)


# --------------------------------------------------------------------------- #
# The 3-minute loop (background thread)
# --------------------------------------------------------------------------- #
def _seconds_to_next_tick():
    interval = config.SNAPSHOT_INTERVAL_SEC
    if not config.ALIGN_TO_CLOCK:
        return interval
    now = time.time()
    return interval - (now % interval)


def _loop():
    log.info("Snapshot loop running every %ds (clock-aligned=%s).",
             config.SNAPSHOT_INTERVAL_SEC, config.ALIGN_TO_CLOCK)
    while True:
        wait = _seconds_to_next_tick()
        time.sleep(max(1.0, wait))
        try:
            take_snapshot()
        except Exception as e:  # noqa
            log.exception("Snapshot failed: %s", e)


def start_snapshotter():
    """Start the snapshot loop once. Safe to call repeatedly."""
    global _snap_thread, _thread_started
    if _thread_started:
        return
    _thread_started = True
    _snap_thread = threading.Thread(target=_loop, name="oi-snapshotter", daemon=True)
    _snap_thread.start()


# --------------------------------------------------------------------------- #
# What the frontend reads
# --------------------------------------------------------------------------- #
def get_view():
    """
    Return the latest computed row per instrument for the UI. Uses the most
    recent snapshot in history, but recomputes deltas against live ticks so the
    table feels current between 3-min snapshots.
    """
    live = get_latest_snapshot()
    rows = []
    with _LOCK:
        for tok in config.TOKENS:
            v = live.get(tok)
            hist = _HISTORY.get(tok, [])
            prev = hist[-1] if hist else None
            if not v:
                rows.append({"token": tok, "label": config.LABELS.get(tok, str(tok)),
                             "oi": None, "ltp": None, "buildup": "—",
                             "d_oi_prev": None, "d_price_prev": None,
                             "d_oi_open": None, "d_price_open": None,
                             "base_oi": _DAY_OPEN_OI.get(tok)})
                continue
            oi, ltp = v.get("oi"), v.get("ltp")
            d_oi_prev = (oi - prev["oi"]) if (prev and oi is not None and prev.get("oi") is not None) else None
            d_price_prev = (ltp - prev["ltp"]) if (prev and ltp is not None and prev.get("ltp") is not None) else None
            base_oi = _DAY_OPEN_OI.get(tok)
            base_price = _DAY_OPEN_PRICE.get(tok)
            rows.append({
                "token": tok,
                "label": config.LABELS.get(tok, str(tok)),
                "oi": oi,
                "ltp": ltp,
                "oi_day_high": v.get("oi_day_high"),
                "oi_day_low": v.get("oi_day_low"),
                "d_oi_prev": d_oi_prev,
                "d_price_prev": d_price_prev,
                "d_oi_open": (oi - base_oi) if (oi is not None and base_oi is not None) else None,
                "d_price_open": (ltp - base_price) if (ltp is not None and base_price is not None) else None,
                "base_oi": base_oi,
                "buildup": classify_buildup(d_price_prev, d_oi_prev),
            })
    return rows
