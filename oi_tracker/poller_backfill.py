"""
oi_tracker / poller_backfill.py
--------------------------------
Backfill OI Pulse snapshots from 09:15 IST to now using 3-min historical data.

Called once per trading day as a background task from routes_oipulse.py.
Replaces poller_pulse._snapshots / _vel_history under _LOCK so the next
live poll sees the full morning history.
"""

import asyncio
import datetime as dt
import logging
import math
from zoneinfo import ZoneInfo

import httpx

from . import token_store
from . import poller_pulse

log = logging.getLogger("oi_tracker.poller_backfill")

IST = ZoneInfo("Asia/Kolkata")
KITE_BASE = "https://api.kite.trade"
NIFTY_SPOT_TOKEN = poller_pulse.NIFTY_SPOT_TOKEN
NIFTY_STEP = poller_pulse.NIFTY_STEP
STRIKE_WINGS = poller_pulse.STRIKE_WINGS

# Max 3 concurrent historical-API requests
_SEM = asyncio.Semaphore(3)


def _parse_ts(ts_str: str) -> dt.datetime:
    ts = dt.datetime.fromisoformat(ts_str)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=IST)
    return ts


async def _fetch_hist(client: httpx.AsyncClient, token: str,
                      from_dt: dt.datetime, to_dt: dt.datetime,
                      oi: bool, headers: dict) -> list:
    """Fetch 3-min candles for one token. Returns [] on any failure."""
    params = {
        "from": from_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "to":   to_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "continuous": 0,
        "oi": int(oi),
    }
    async with _SEM:
        try:
            r = await client.get(
                f"{KITE_BASE}/instruments/historical/{token}/3minute",
                params=params,
                headers=headers,
                timeout=30,
            )
            # Brief pause inside the semaphore slot to stay ≤ 3 req/s
            await asyncio.sleep(0.35)
            if r.status_code != 200:
                log.debug("Backfill: token %s HTTP %s", token, r.status_code)
                return []
            return r.json()["data"]["candles"]
        except Exception as exc:
            log.debug("Backfill: token %s fetch error: %s", token, exc)
            return []


async def backfill_today(nfo_df) -> None:
    """Rebuild poller_pulse._snapshots from 09:15 IST today up to now."""
    api_key = token_store.load_api_key()
    access_token = token_store.load_access_token()
    if not api_key or not access_token:
        log.warning("Backfill skipped: no Kite token")
        return

    now_ist = dt.datetime.now(tz=IST)
    today = now_ist.date()

    market_open = dt.datetime(today.year, today.month, today.day,
                              9, 15, tzinfo=IST)
    if now_ist <= market_open:
        log.info("Backfill skipped: before market open")
        return

    headers = {
        "X-Kite-Version": "3",
        "Authorization": f"token {api_key}:{access_token}",
    }

    log.info("Backfill starting: 09:15 → %s IST", now_ist.strftime("%H:%M"))

    # ── a. NIFTY spot 3-min candles ───────────────────────────────────────
    async with httpx.AsyncClient(timeout=30) as spot_client:
        spot_candles = await _fetch_hist(
            spot_client, NIFTY_SPOT_TOKEN,
            market_open, now_ist, False, headers
        )

    if not spot_candles:
        log.warning("Backfill: no spot candles — aborting")
        return

    spot_map: dict[dt.datetime, float] = {}
    for c in spot_candles:
        ts = _parse_ts(c[0])
        spot_map[ts] = float(c[4])  # close price

    timestamps = sorted(spot_map.keys())
    if not timestamps:
        return

    # ── b. Strike universe wide enough for ATM±10 at every timestamp ─────
    all_spots = list(spot_map.values())
    buffer_strikes = 5   # extra strikes beyond STRIKE_WINGS on each side
    uni_low  = (math.floor(min(all_spots) / NIFTY_STEP) * NIFTY_STEP
                - (STRIKE_WINGS + buffer_strikes) * NIFTY_STEP)
    uni_high = (math.ceil(max(all_spots) / NIFTY_STEP) * NIFTY_STEP
                + (STRIKE_WINGS + buffer_strikes) * NIFTY_STEP)
    universe = [float(s) for s in
                range(int(uni_low), int(uni_high) + NIFTY_STEP, NIFTY_STEP)]

    # ── c. Expiries and token map ─────────────────────────────────────────
    expiries = poller_pulse._two_nearest_expiries(nfo_df, today)
    if not expiries:
        log.warning("Backfill: no NIFTY expiries found — aborting")
        return

    token_map: dict[str, tuple] = {}   # token -> (expiry, strike, otype)
    for exp_str in expiries:
        for (strike, otype), tok in poller_pulse._option_tokens_for_expiry(
                nfo_df, exp_str, universe).items():
            token_map[tok] = (exp_str, strike, otype)

    if not token_map:
        log.warning("Backfill: no option tokens in universe — aborting")
        return

    log.info("Backfill: fetching OI history for %d contracts…", len(token_map))

    # ── d. Fetch historical OI for all contracts (throttled) ──────────────
    oi_data: dict[str, dict[dt.datetime, float]] = {}

    async with httpx.AsyncClient(timeout=30) as client:
        async def _fetch_one(tok: str):
            candles = await _fetch_hist(
                client, tok, market_open, now_ist, True, headers
            )
            ts_oi: dict[dt.datetime, float] = {}
            for c in candles:
                ts_c = _parse_ts(c[0])
                ts_oi[ts_c] = float(c[6]) if len(c) > 6 else 0.0
            return tok, ts_oi

        results = await asyncio.gather(
            *[_fetch_one(tok) for tok in token_map],
            return_exceptions=True,
        )

    for res in results:
        if isinstance(res, Exception):
            log.debug("Backfill gather exception: %s", res)
            continue
        tok, ts_oi = res
        oi_data[tok] = ts_oi

    # ── e. Build synthetic snapshots for each 3-min bar ───────────────────
    new_snapshots: list[dict] = []
    vel_build: list[dict] = []   # rolling history for _velocity()

    for ts in timestamps:
        spot_t = spot_map[ts]
        atm_t  = round(spot_t / NIFTY_STEP) * NIFTY_STEP
        strikes = [float(atm_t + i * NIFTY_STEP)
                   for i in range(-STRIKE_WINGS, STRIKE_WINGS + 1)]

        # Expiry weights using THAT timestamp's time (not "now")
        expiry_weights: dict[str, float] = {}
        for exp_str in expiries:
            exp_date = dt.date.fromisoformat(exp_str)
            days = max((exp_date - today).days, 0.5)
            weight = 1.0 / math.sqrt(days)
            if exp_date == today and ts.hour >= 13:
                weight = 0.0
            expiry_weights[exp_str] = weight

        # Weighted CE/PE OI per strike
        weighted_ce: dict[float, float] = {s: 0.0 for s in strikes}
        weighted_pe: dict[float, float] = {s: 0.0 for s in strikes}

        for tok, (exp_str, strike, otype) in token_map.items():
            if strike not in weighted_ce:
                continue
            oi_val = oi_data.get(tok, {}).get(ts, 0.0)
            w = expiry_weights.get(exp_str, 0.0)
            if otype == "CE":
                weighted_ce[strike] += oi_val * w
            else:
                weighted_pe[strike] += oi_val * w

        ce_wall  = max(strikes, key=lambda s: weighted_ce[s])
        pe_wall  = max(strikes, key=lambda s: weighted_pe[s])
        band_mid = (ce_wall + pe_wall) / 2.0

        ce_total = sum(weighted_ce[s] for s in strikes)
        pe_total = sum(weighted_pe[s] for s in strikes)

        ce_cog = (sum(s * weighted_ce[s] for s in strikes) / ce_total
                  if ce_total > 0 else float(atm_t))
        pe_cog = (sum(s * weighted_pe[s] for s in strikes) / pe_total
                  if pe_total > 0 else float(atm_t))
        cog_mid = (ce_cog + pe_cog) / 2.0

        # Velocity — same helper as poll_once, feeding up to last 5 prior points
        history_slice = vel_build[-5:]
        ce_vel, pe_vel = poller_pulse._velocity(history_slice, ce_total, pe_total)

        vel_build.append({"ce_total": ce_total, "pe_total": pe_total})
        if len(vel_build) > 6:
            vel_build = vel_build[-6:]

        new_snapshots.append({
            "ts":          ts.isoformat(),
            "spot":        spot_t,
            "atm":         float(atm_t),
            "ce_wall":     ce_wall,
            "pe_wall":     pe_wall,
            "band_mid":    band_mid,
            "ce_cog":      ce_cog,
            "pe_cog":      pe_cog,
            "cog_mid":     cog_mid,
            "ce_velocity": ce_vel,
            "pe_velocity": pe_vel,
            "ce_total":    ce_total,
            "pe_total":    pe_total,
        })

    if not new_snapshots:
        log.warning("Backfill: produced 0 snapshots")
        return

    # ── f. Replace poller_pulse state under its lock ──────────────────────
    with poller_pulse._LOCK:
        # Guard against midnight rollover: only replace if still today
        if poller_pulse._TODAY_IST == today:
            poller_pulse._snapshots.clear()
            for snap in new_snapshots[-poller_pulse.MAX_SNAPSHOTS:]:
                poller_pulse._snapshots.append(snap)

            poller_pulse._vel_history.clear()
            for vh in vel_build[-6:]:
                poller_pulse._vel_history.append(vh)

    log.info("Backfill complete: %d snapshots (%s → %s IST)",
             len(new_snapshots),
             timestamps[0].strftime("%H:%M"),
             timestamps[-1].strftime("%H:%M"))
