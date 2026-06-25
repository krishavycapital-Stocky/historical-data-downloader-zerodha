"""
oi_tracker / poller_pulse.py
----------------------------
On-demand OI Pulse poller. No background threads.

poll_once(nfo_df) is called from routes_oipulse.py. It fetches a weighted
two-expiry OI snapshot for NIFTY, appends to an in-memory deque, and
returns the full snapshot history plus a status block.
"""

import datetime as dt
import logging
import math
import threading
from collections import deque
from zoneinfo import ZoneInfo

import httpx

from . import token_store
import broker_dhan as broker

log = logging.getLogger("oi_tracker.poller_pulse")

KITE_BASE = "https://api.kite.trade"
NIFTY_SPOT_TOKEN = "256265"
NIFTY_STEP = 50
STRIKE_WINGS = 10        # ATM ± 10 strikes → 21 total
MAX_SNAPSHOTS = 200
SPIKE_THRESHOLD = 2.0
IST = ZoneInfo("Asia/Kolkata")

_LOCK = threading.Lock()
_snapshots: deque = deque(maxlen=MAX_SNAPSHOTS)
# Store last 6 totals so we can compute up to 5 consecutive deltas.
_vel_history: deque = deque(maxlen=6)
_TODAY_IST = None


def _ist_now() -> dt.datetime:
    return dt.datetime.now(tz=IST)


def _reset_if_new_day(today: dt.date):
    global _TODAY_IST
    if _TODAY_IST != today:
        _TODAY_IST = today
        _snapshots.clear()
        _vel_history.clear()
        log.info("New IST day %s — pulse snapshots reset.", today)


def _two_nearest_expiries(nfo_df, today: dt.date) -> list[str]:
    """Return up to 2 nearest NIFTY expiry date strings >= today, ascending."""
    mask = (
        nfo_df["name"].astype(str).str.upper().eq("NIFTY") &
        nfo_df["instrument_type"].isin(["CE", "PE"])
    )
    sub = nfo_df[mask]
    if sub.empty:
        return []
    today_str = today.isoformat()
    expiries = sorted({
        e for e in sub["expiry"].dropna().astype(str).unique()
        if e and e != "nan" and e >= today_str
    })
    return expiries[:2]


def _option_tokens_for_expiry(nfo_df, expiry: str, strikes: list) -> dict:
    """Return {(strike_float, 'CE'|'PE'): token_str} for the given expiry."""
    mask = (
        nfo_df["name"].astype(str).str.upper().eq("NIFTY") &
        nfo_df["expiry"].astype(str).eq(expiry) &
        nfo_df["instrument_type"].isin(["CE", "PE"])
    )
    sub = nfo_df[mask]
    result = {}
    strikes_set = set(float(s) for s in strikes)
    for _, row in sub.iterrows():
        s = float(row["strike"])
        if s in strikes_set:
            result[(s, str(row["instrument_type"]))] = str(int(row["instrument_token"]))
    return result


def _velocity(history: list, current_ce: float, current_pe: float):
    """
    history: list of {"ce_total": float, "pe_total": float} (up to 5 prior values).
    Returns (ce_velocity, pe_velocity), always >= 0.
    """
    if not history:
        return 0.0, 0.0

    ce_vals = [h["ce_total"] for h in history] + [current_ce]
    pe_vals = [h["pe_total"] for h in history] + [current_pe]

    ce_deltas = [abs(ce_vals[i + 1] - ce_vals[i]) for i in range(len(ce_vals) - 1)]
    pe_deltas = [abs(pe_vals[i + 1] - pe_vals[i]) for i in range(len(pe_vals) - 1)]

    # Use last up to 5 deltas for the average.
    last_ce = ce_deltas[-5:]
    last_pe = pe_deltas[-5:]

    avg_d_ce = sum(last_ce) / len(last_ce) if last_ce else 0.0
    avg_d_pe = sum(last_pe) / len(last_pe) if last_pe else 0.0

    d_ce = abs(current_ce - history[-1]["ce_total"])
    d_pe = abs(current_pe - history[-1]["pe_total"])

    ce_vel = (d_ce / avg_d_ce) if avg_d_ce > 0 else 0.0
    pe_vel = (d_pe / avg_d_pe) if avg_d_pe > 0 else 0.0
    return ce_vel, pe_vel


async def poll_once(nfo_df) -> dict:
    """
    Fetch one OI Pulse snapshot.
    nfo_df — the cached NFO instrument DataFrame from _get_exchange_df("NFO")
              (passed in by the route to avoid a circular import with main.py).
    Returns {"snapshots": [...], "status": {"last_updated": iso_str|None, "error": str|None}}.
    """
    api_key = token_store.load_client_id()
    access_token = token_store.load_access_token()

    def _err(msg):
        with _LOCK:
            return {
                "snapshots": list(_snapshots),
                "status": {"error": msg, "last_updated": None},
            }

    if not api_key or not access_token:
        return _err("No token — log in on the main tab first.")

    if nfo_df is None or nfo_df.empty:
        return _err("NFO instrument data not available yet.")

    now_ist = _ist_now()
    today = now_ist.date()

    # ── 1. Two nearest expiries ──────────────────────────────────────────
    expiries = _two_nearest_expiries(nfo_df, today)
    if not expiries:
        return _err("No NIFTY expiries found in NFO data.")

    # ── 2. Expiry weights: 1/sqrt(days), zero on-expiry after 13:00 IST ─
    expiry_weights: dict[str, float] = {}
    for exp_str in expiries:
        exp_date = dt.date.fromisoformat(exp_str)
        days = max((exp_date - today).days, 0.5)
        weight = 1.0 / math.sqrt(days)
        if exp_date == today and now_ist.hour >= 13:
            weight = 0.0
        expiry_weights[exp_str] = weight

    active = [e for e in expiries if expiry_weights[e] > 0]
    if not active:
        active = expiries   # fallback so page isn't blank

    # ── 3. Fetch spot ────────────────────────────────────────────────────
    try:
        master = broker.load_scrip_master()
        nifty_idx = master[(master["exchange_segment"] == "IDX_I") &
                           (master["name"].str.upper().isin(["NIFTY 50", "NIFTY"]))]
        spot_token = str(nifty_idx.iloc[0]["instrument_token"]) if not nifty_idx.empty else NIFTY_SPOT_TOKEN
        qd = await broker.fetch_quotes([spot_token])
    except Exception as exc:
        log.warning("Spot quote failed: %s", exc)
        return _err(f"Network error (spot): {exc}")
    spot_q = qd.get(str(spot_token), {})
    spot = spot_q.get("last_price")
    if spot is None:
        return _err("Spot price missing in Dhan response.")

    atm = round(spot / NIFTY_STEP) * NIFTY_STEP
    strikes = [float(atm + i * NIFTY_STEP) for i in range(-STRIKE_WINGS, STRIKE_WINGS + 1)]

    # ── 4. Collect option tokens for active expiries ──────────────────────
    token_map: dict[str, tuple] = {}   # token_str -> (exp_str, strike, otype)
    for exp_str in active:
        for (strike, otype), tok in _option_tokens_for_expiry(nfo_df, exp_str, strikes).items():
            token_map[tok] = (exp_str, strike, otype)

    if not token_map:
        return _err("No option tokens found for NIFTY strikes/expiries.")

    # ── 5. Batch quote for all option tokens (one API call) ───────────────
    try:
        opt_data = await broker.fetch_quotes(list(token_map.keys()))
    except Exception as exc:
        log.warning("Options quote failed: %s", exc)
        return _err(f"Network error (options): {exc}")

    # ── 6. Weighted OI per strike ─────────────────────────────────────────
    weighted_ce: dict[float, float] = {s: 0.0 for s in strikes}
    weighted_pe: dict[float, float] = {s: 0.0 for s in strikes}

    for tok, (exp_str, strike, otype) in token_map.items():
        q = opt_data.get(tok, {})
        oi = float(q.get("oi") or 0)
        w = expiry_weights.get(exp_str, 0.0)
        if otype == "CE":
            weighted_ce[strike] = weighted_ce.get(strike, 0.0) + oi * w
        else:
            weighted_pe[strike] = weighted_pe.get(strike, 0.0) + oi * w

    # ── 7. OI Bands ───────────────────────────────────────────────────────
    ce_wall = max(strikes, key=lambda s: weighted_ce[s])
    pe_wall = max(strikes, key=lambda s: weighted_pe[s])
    band_mid = (ce_wall + pe_wall) / 2.0

    # ── 8. COG ────────────────────────────────────────────────────────────
    ce_total = sum(weighted_ce[s] for s in strikes)
    pe_total = sum(weighted_pe[s] for s in strikes)

    ce_cog = (sum(s * weighted_ce[s] for s in strikes) / ce_total) if ce_total > 0 else float(atm)
    pe_cog = (sum(s * weighted_pe[s] for s in strikes) / pe_total) if pe_total > 0 else float(atm)
    cog_mid = (ce_cog + pe_cog) / 2.0

    # ── 9. Velocity + append snapshot (under lock) ─────────────────────────
    with _LOCK:
        _reset_if_new_day(today)
        history_copy = list(_vel_history)

    ce_vel, pe_vel = _velocity(history_copy, ce_total, pe_total)

    with _LOCK:
        _vel_history.append({"ce_total": ce_total, "pe_total": pe_total})
        _snapshots.append({
            "ts": now_ist.isoformat(),
            "spot": spot,
            "atm": float(atm),
            "ce_wall": ce_wall,
            "pe_wall": pe_wall,
            "band_mid": band_mid,
            "ce_cog": ce_cog,
            "pe_cog": pe_cog,
            "cog_mid": cog_mid,
            "ce_velocity": ce_vel,
            "pe_velocity": pe_vel,
            "ce_total": ce_total,
            "pe_total": pe_total,
        })
        snaps = list(_snapshots)

    return {
        "snapshots": snaps,
        "status": {"error": None, "last_updated": now_ist.isoformat()},
    }
