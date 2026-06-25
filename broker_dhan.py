"""
broker_dhan.py
==============
The ONLY file that talks to Dhan. Everything it returns is shaped to look
exactly like what your old Zerodha code already expected, so the rest of the
app barely changes.

It gives you three things:
  1. load_scrip_master()            -> a pandas DataFrame with the SAME column
                                       names your Kite code used (instrument_token,
                                       tradingsymbol, name, expiry, strike,
                                       instrument_type, exchange, lot_size).
  2. await fetch_candles(...)       -> a list of candle rows [date,o,h,l,c,v,(oi)]
                                       just like Kite's /historical. Handles the
                                       intervals Dhan doesn't have (3/10/30/75 min)
                                       by fetching 1- or 15-min and resampling.
  3. await fetch_quotes(ids)        -> {security_id: {"last_price":.., "oi":..}}
                                       just like Kite's /quote.

Credentials (Dhan access token + client id) are read from oi_tracker.token_store,
which your owner fills by pasting the daily token into the existing Set-Token box.
"""

import io
import time
import datetime as dt
from typing import Optional

import httpx
import pandas as pd

from oi_tracker import token_store   # reuse your existing server-side token store

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #
BASE = "https://api.dhan.co/v2"                                   # all REST calls
SCRIP_URL = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"  # daily master

# Dhan serves these intraday candles natively. Anything else we build by
# resampling the nearest native base candle (key = wanted minutes, value = base).
NATIVE_MIN = {1, 5, 15, 25, 60}
RESAMPLE_BASE = {3: 1, 10: 5, 30: 15, 75: 15}

# Map your Kite-style interval words to a number of minutes ("day" handled apart).
KITE_INTERVAL_MIN = {
    "minute": 1, "3minute": 3, "5minute": 5, "10minute": 10,
    "15minute": 15, "30minute": 30, "60minute": 60, "75minute": 75,
}

# 1980-01-01 IST expressed in normal Unix seconds (used only as a safety net for
# timestamp decoding — see _to_ist_naive).
_DHAN_1980_OFFSET = 315513000


# --------------------------------------------------------------------------- #
# Credentials
# --------------------------------------------------------------------------- #
def _creds() -> tuple:
    """Return (access_token, client_id). Raises if not set yet."""
    token = token_store.load_access_token() or ""
    client_id = token_store.load_client_id() or ""
    if not token or not client_id:
        raise RuntimeError("Dhan token/client-id not set — owner must paste them.")
    return token, client_id


def _data_headers(with_client: bool = False) -> dict:
    """Headers for Dhan data calls. marketfeed/optionchain/instrument need client-id."""
    token, client_id = _creds()
    h = {"access-token": token, "Content-Type": "application/json", "Accept": "application/json"}
    if with_client:
        h["client-id"] = client_id
    return h


# --------------------------------------------------------------------------- #
# Scrip master (instrument list)  — cached for 8h like your old code
# --------------------------------------------------------------------------- #
_master_df: Optional[pd.DataFrame] = None
_master_at: Optional[dt.datetime] = None
_seg_by_id: dict = {}      # security_id(str) -> exchangeSegment e.g. "NSE_FNO"
_instr_by_id: dict = {}    # security_id(str) -> dhan instrument e.g. "OPTIDX"

# Translate the master's (exchange, segment) into Dhan's API segment + a Kite-like
# exchange label your app already uses ("NFO","BFO","NSE","BSE").
_SEG_MAP = {
    ("NSE", "E"): ("NSE_EQ", "NSE"),
    ("BSE", "E"): ("BSE_EQ", "BSE"),
    ("NSE", "D"): ("NSE_FNO", "NFO"),
    ("BSE", "D"): ("BSE_FNO", "BFO"),
    ("NSE", "C"): ("NSE_CURRENCY", "CDS"),
    ("MCX", "M"): ("MCX_COMM", "MCX"),
    ("NSE", "I"): ("IDX_I", "NSE"),
    ("BSE", "I"): ("IDX_I", "BSE"),
}


def load_scrip_master(force: bool = False) -> pd.DataFrame:
    """Download Dhan's daily scrip master and return it with Kite-style columns."""
    global _master_df, _master_at, _seg_by_id, _instr_by_id
    now = dt.datetime.now()
    if (not force and _master_df is not None and _master_at is not None
            and (now - _master_at).total_seconds() < 8 * 3600):
        return _master_df

raw = httpx.get(SCRIP_URL, timeout=120).text          # public CSV, no auth needed
       _WANT = {
           "SEM_SMST_SECURITY_ID", "SEM_TRADING_SYMBOL", "SEM_CUSTOM_SYMBOL",
           "SM_SYMBOL_NAME", "SEM_EXPIRY_DATE", "SEM_STRIKE_PRICE", "SEM_OPTION_TYPE",
           "SEM_INSTRUMENT_NAME", "SEM_EXCH_INSTRUMENT_TYPE", "SEM_EXM_EXCH_ID",
           "SEM_SEGMENT", "SEM_LOT_UNITS",
       }
       src = pd.read_csv(io.StringIO(raw), usecols=lambda c: c in _WANT, dtype=str)
       del raw                                                # free the big text blob
       if "SEM_SEGMENT" in src.columns:                       # keep only equity / F&O / index
           src = src[src["SEM_SEGMENT"].astype(str).str.upper().isin(["E", "D", "I"])].copy()

    def col(*names):
        """Pick the first column that exists (Dhan has renamed a few over time)."""
        for n in names:
            if n in src.columns:
                return src[n]
        return pd.Series([""] * len(src))

    sec_id = col("SEM_SMST_SECURITY_ID").astype(str)
    tsym   = col("SEM_TRADING_SYMBOL", "SEM_CUSTOM_SYMBOL").astype(str)
    uname  = col("SM_SYMBOL_NAME", "SEM_TRADING_SYMBOL").astype(str)   # underlying, e.g. NIFTY
    expiry = col("SEM_EXPIRY_DATE").astype(str).str.slice(0, 10)        # keep YYYY-MM-DD
    strike = pd.to_numeric(col("SEM_STRIKE_PRICE"), errors="coerce")
    otype  = col("SEM_OPTION_TYPE").astype(str).str.upper()            # CE / PE / blank
    instr  = col("SEM_INSTRUMENT_NAME", "SEM_EXCH_INSTRUMENT_TYPE").astype(str).str.upper()
    exch   = col("SEM_EXM_EXCH_ID").astype(str).str.upper()
    seg    = col("SEM_SEGMENT").astype(str).str.upper()
    lot    = pd.to_numeric(col("SEM_LOT_UNITS"), errors="coerce").fillna(0).astype(int)

    # instrument_type the app expects: CE / PE / FUT / EQ / INDEX
    def _itype(o, i):
        if o in ("CE", "PE"):
            return o
        if i.startswith("FUT"):
            return "FUT"
        if "INDEX" in i:
            return "INDEX"
        return "EQ"
    itype = [_itype(o, i) for o, i in zip(otype, instr)]

    # Dhan exchangeSegment + Kite-like exchange label, row by row.
    segs, exlabels = [], []
    for e, s in zip(exch, seg):
        ds, lab = _SEG_MAP.get((e, s), ("NSE_EQ", "NSE"))
        segs.append(ds); exlabels.append(lab)

    df = pd.DataFrame({
        "instrument_token": sec_id,        # = Dhan securityId (kept under old name)
        "tradingsymbol": tsym,
        "name": uname.str.upper(),
        "expiry": expiry.where(expiry.str.len() == 10, ""),
        "strike": strike.fillna(0.0),
        "instrument_type": itype,
        "exchange": exlabels,              # NFO / BFO / NSE / BSE
        "lot_size": lot,
        "exchange_segment": segs,          # NSE_FNO etc. (extra, used internally)
        "dhan_instrument": instr,          # OPTIDX/FUTIDX/EQUITY/INDEX (extra)
    })

    # fast lookups for fetch_candles / fetch_quotes
    _seg_by_id = dict(zip(df["instrument_token"], df["exchange_segment"]))
    _instr_by_id = dict(zip(df["instrument_token"], df["dhan_instrument"]))

    _master_df, _master_at = df, now
    return df


def _resolve(security_id) -> tuple:
    """Return (exchangeSegment, dhanInstrument) for a security id; load master if needed."""
    sid = str(security_id)
    if sid not in _seg_by_id:
        load_scrip_master()
    seg = _seg_by_id.get(sid, "NSE_EQ")
    instr = _instr_by_id.get(sid, "EQUITY")
    # historical API wants a coarse instrument word
    if instr.startswith("OPT"):
        instr = "OPTIDX" if instr == "OPTIDX" else "OPTSTK"
    elif instr.startswith("FUT"):
        instr = "FUTIDX" if instr == "FUTIDX" else "FUTSTK"
    elif "INDEX" in instr:
        instr = "INDEX"
    else:
        instr = "EQUITY"
    return seg, instr


# --------------------------------------------------------------------------- #
# Timestamp helper
# --------------------------------------------------------------------------- #
def _to_ist_naive(timestamps) -> pd.Series:
    """Convert Dhan epoch seconds -> naive IST datetimes (matches Kite's output)."""
    ts = pd.to_numeric(pd.Series(timestamps), errors="coerce")
    out = pd.to_datetime(ts, unit="s", utc=True).dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
    # Safety net: if dates look pre-2005, the feed used the 1980 base — shift it.
    if out.notna().any() and out.dropna().dt.year.median() < 2005:
        out = (pd.to_datetime(ts + _DHAN_1980_OFFSET, unit="s", utc=True)
               .dt.tz_convert("Asia/Kolkata").dt.tz_localize(None))
    return out


# --------------------------------------------------------------------------- #
# Date chunking (Dhan intraday allows max 90 days per call)
# --------------------------------------------------------------------------- #
def _chunks(from_date: str, to_date: str, daily: bool):
    fmt = "%Y-%m-%d"
    start = dt.datetime.strptime(from_date[:10], fmt)
    end = dt.datetime.strptime(to_date[:10], fmt)
    span = 365 if daily else 80          # stay under the 90-day intraday cap
    cur, out = start, []
    while cur <= end:
        ce = min(cur + dt.timedelta(days=span - 1), end)
        out.append((cur.strftime(fmt), ce.strftime(fmt)))
        cur = ce + dt.timedelta(days=1)
    return out


# --------------------------------------------------------------------------- #
# One raw Dhan candle call -> list of rows [date,o,h,l,c,v,(oi)]
# --------------------------------------------------------------------------- #
async def _raw_candles(client, security_id, seg, instrument, dhan_interval, from_date, to_date, oi, daily):
    if daily:
        url = f"{BASE}/charts/historical"
        body = {"securityId": str(security_id), "exchangeSegment": seg,
                "instrument": instrument, "oi": bool(oi),
                "fromDate": from_date[:10], "toDate": to_date[:10]}
    else:
        url = f"{BASE}/charts/intraday"
        body = {"securityId": str(security_id), "exchangeSegment": seg,
                "instrument": instrument, "interval": str(dhan_interval), "oi": bool(oi),
                "fromDate": f"{from_date[:10]} 09:15:00", "toDate": f"{to_date[:10]} 15:30:00"}

    r = await client.post(url, json=body, headers=_data_headers())
    if r.status_code != 200:
        raise RuntimeError(f"Dhan {r.status_code}: {r.text[:300]}")
    d = r.json() or {}

    opens = d.get("open") or []
    if not opens:
        return []
    dates = _to_ist_naive(d.get("timestamp") or [])
    cols = [dates, d.get("open"), d.get("high"), d.get("low"), d.get("close"), d.get("volume")]
    has_oi = bool(d.get("open_interest") or d.get("oi"))
    if has_oi:
        cols.append(d.get("open_interest") or d.get("oi"))
    rows = list(map(list, zip(*cols)))    # column arrays -> row tuples (Kite shape)
    return rows


def _resample_rows(rows, minutes):
    """Resample raw native-minute rows up to `minutes` (e.g. 1->3, 5->10, 15->75)."""
    if not rows:
        return rows
    has_oi = len(rows[0]) == 7
    cols = ["date", "open", "high", "low", "close", "volume"] + (["oi"] if has_oi else [])
    df = pd.DataFrame(rows, columns=cols)
    df["date"] = pd.to_datetime(df["date"])
    agg = {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    if has_oi:
        agg["oi"] = "last"
    parts = []
    rule = f"{minutes}min"
    # Resample each day separately, anchored to that day's first candle (9:15),
    # so buckets line up with the exchange clock just like Kite's candles do.
    for _, g in df.groupby(df["date"].dt.date):
        g = g.set_index("date")
        out = g.resample(rule, origin=g.index.min()).agg(agg).dropna(subset=["open"])
        parts.append(out.reset_index())
    res = pd.concat(parts, ignore_index=True)
    order = ["date", "open", "high", "low", "close", "volume"] + (["oi"] if has_oi else [])
    return res[order].values.tolist()


async def fetch_candles(security_id, interval, from_date, to_date, oi=True):
    """Drop-in replacement for your old Kite _fetch_candles (minus `continuous`).
    Returns list of rows [date,o,h,l,c,v,(oi)]."""
    seg, instrument = _resolve(security_id)
    daily = (interval == "day")
    wanted_min = None if daily else KITE_INTERVAL_MIN.get(interval, 1)

    # Decide which native candle to actually request from Dhan.
    if daily:
        base_interval = None
        need_resample = False
    elif wanted_min in NATIVE_MIN:
        base_interval, need_resample = wanted_min, False
    else:
        base_interval = RESAMPLE_BASE.get(wanted_min, 1)
        need_resample = True

    rows = []
    async with httpx.AsyncClient(timeout=90) as client:
        for cf, ct in _chunks(from_date, to_date, daily):
            rows.extend(await _raw_candles(
                client, security_id, seg, instrument, base_interval, cf, ct, oi, daily))

    if need_resample and rows:
        rows = _resample_rows(rows, wanted_min)
    return rows


# --------------------------------------------------------------------------- #
# Live quotes (LTP + OI)  — replaces Kite /quote
# --------------------------------------------------------------------------- #
async def fetch_quotes(security_ids) -> dict:
    """Return {security_id(str): {"last_price": float, "oi": int}} for the given ids."""
    ids = [str(s) for s in security_ids]
    if not ids:
        return {}

    # Dhan groups requested ids by exchange segment.
    by_seg: dict = {}
    for sid in ids:
        seg, _ = _resolve(sid)
        by_seg.setdefault(seg, []).append(int(sid) if sid.isdigit() else sid)

    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(f"{BASE}/marketfeed/quote", json=by_seg,
                              headers=_data_headers(with_client=True))
    if r.status_code != 200:
        raise RuntimeError(f"Dhan quote {r.status_code}: {r.text[:300]}")

    data = (r.json() or {}).get("data", {}) or {}
    out: dict = {}
    for seg, block in data.items():               # block: {security_id: {...fields...}}
        if not isinstance(block, dict):
            continue
        for sid, q in block.items():
            out[str(sid)] = {
                "last_price": q.get("last_price") or q.get("ltp"),
                "oi": q.get("oi"),
            }
    return out


# --------------------------------------------------------------------------- #
# Token validity check  — replaces Kite /user/profile
# --------------------------------------------------------------------------- #
async def validate() -> dict:
    """Return {'valid': bool, 'user_name': str, 'email': str} using Dhan /profile."""
    try:
        token, _ = _creds()
    except Exception:
        return {"valid": False, "user_name": "", "email": ""}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(f"{BASE}/profile", headers={"access-token": token})
        if r.status_code == 200:
            d = r.json() or {}
            return {"valid": True, "user_name": d.get("dhanClientId", ""), "email": ""}
    except Exception:
        pass
    return {"valid": False, "user_name": "", "email": ""}
