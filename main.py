from fastapi import FastAPI, HTTPException, Query, Request, Form
from fastapi.staticfiles import StaticFiles
from fastapi.responses import StreamingResponse, JSONResponse, HTMLResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from pydantic import BaseModel
from typing import Optional
import httpx
import pandas as pd
import io
import json
import os
import secrets
from datetime import datetime, timedelta
import asyncio
try:
    import fastparquet
    PARQUET_OK = True
except ImportError:
    PARQUET_OK = False

import broker_dhan as broker

app = FastAPI(title="ZetaPull — Historical Data Downloader")

from oi_tracker.token_store import (
    save_token as _oi_save_token,
    load_access_token as _oi_load_token,
    load_api_key as _oi_load_api_key,
)
from oi_tracker.routes_fastapi import router as oi_router
app.include_router(oi_router)

from oi_tracker.routes_oipulse import router as oipulse_router
app.include_router(oipulse_router)

# ── App-level auth setup ────────────────────────────────────────────────────────────

def _parse_users() -> dict:
    """Parse APP_USERS env var: 'alice:pass1,bob:pass2' → {alice: pass1, bob: pass2}"""
    users: dict = {}
    for entry in os.environ.get("APP_USERS", "").split(","):
        entry = entry.strip()
        if ":" in entry:
            u, p = entry.split(":", 1)
            u, p = u.strip(), p.strip()
            if u and p:
                users[u] = p
    return users

_APP_USERS: dict = _parse_users()

_OWNER_USERNAME = os.environ.get("OWNER_USERNAME", "").strip()
_KITE_API_KEY   = os.environ.get("KITE_API_KEY",   "").strip()
_KITE_API_SECRET = os.environ.get("KITE_API_SECRET", "").strip()

_SECRET_KEY = os.environ.get("SECRET_KEY", "")
if not _SECRET_KEY:
    import logging as _logging
    _logging.getLogger("main").warning(
        "SECRET_KEY env var is not set — sessions are insecure. Set it on Render."
    )
    _SECRET_KEY = "insecure-dev-only-change-me-before-deploying"

_LOGIN_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>ZetaPull — Sign in</title>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Sans:wght@300;400;500;600&display=swap" rel="stylesheet"/>
<style>
*{box-sizing:border-box;margin:0;padding:0}
:root{--bg:#0a0e14;--surf:#111720;--surf2:#18202e;--bdr:#1e2d44;--grn:#00d4aa;--blu:#0091ff;--red:#ff4d6d;--txt:#e0eaf8;--muted:#5a7a9a;--mono:'IBM Plex Mono',monospace;--sans:'IBM Plex Sans',sans-serif}
body{background:var(--bg);color:var(--txt);font-family:var(--sans);font-size:14px;min-height:100vh;display:flex;align-items:center;justify-content:center;padding:40px 20px}
.box{background:var(--surf);border:1px solid var(--bdr);border-radius:12px;padding:40px;width:100%;max-width:400px}
.logo{font-family:var(--mono);font-size:26px;font-weight:600;color:var(--grn);margin-bottom:4px}
.logo span{color:var(--muted);font-weight:400}
.tagline{color:var(--muted);font-size:13px;margin-bottom:28px}
.lbl{font-family:var(--mono);font-size:11px;text-transform:uppercase;letter-spacing:.8px;color:var(--muted);margin-bottom:5px;display:block}
.inp{width:100%;background:var(--surf2);border:1px solid var(--bdr);border-radius:6px;color:var(--txt);font-family:var(--mono);font-size:13px;padding:10px 13px;outline:none;margin-bottom:14px;transition:border-color .2s}
.inp:focus{border-color:var(--blu)}
.btn{width:100%;padding:12px;border:none;border-radius:6px;font-family:var(--mono);font-size:13px;font-weight:600;cursor:pointer;background:var(--grn);color:#000;margin-top:4px;transition:opacity .2s}
.btn:hover{opacity:.85}
.errmsg{background:rgba(255,77,109,.1);border:1px solid rgba(255,77,109,.3);color:var(--red);border-radius:5px;padding:9px 13px;font-family:var(--mono);font-size:12px;margin-bottom:14px}
</style>
</head>
<body>
<div class="box">
  <div class="logo">Zeta<span>Pull</span></div>
  <div class="tagline">Sign in to continue</div>
  <!--ERROR-->
  <form method="POST" action="/login">
    <label class="lbl">Username</label>
    <input class="inp" type="text" name="username" autofocus autocomplete="username" required/>
    <label class="lbl">Password</label>
    <input class="inp" type="password" name="password" autocomplete="current-password" required/>
    <button class="btn" type="submit">Sign in →</button>
  </form>
</div>
</body>
</html>"""


class _AuthMiddleware(BaseHTTPMiddleware):
    """Require a signed-in app session for every route except /login and /logout."""
    _PUBLIC = frozenset({"/login", "/logout"})

    async def dispatch(self, request: Request, call_next):
        if request.url.path in self._PUBLIC or request.method == "OPTIONS":
            return await call_next(request)
        if not request.session.get("user"):
            if "/api/" in request.url.path:
                return JSONResponse({"detail": "Not authenticated"}, status_code=401)
            return RedirectResponse("/login", status_code=302)
        return await call_next(request)


_ALLOWED_ORIGINS: list = [
    o.strip() for o in os.environ.get("ALLOWED_ORIGINS", "").split(",") if o.strip()
] or ["http://localhost:8000"]

# Middleware — added in reverse execution order (last added = outermost = runs first):
#   CORSMiddleware → SessionMiddleware → _AuthMiddleware → route handler
app.add_middleware(_AuthMiddleware)
app.add_middleware(SessionMiddleware, secret_key=_SECRET_KEY,
                   session_cookie="zp_sess", https_only=False, same_site="lax")
app.add_middleware(
    CORSMiddleware,
    allow_origins=_ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type"],
)

KITE_BASE = "https://api.kite.trade"

_NO_TOKEN_MSG = "Daily Kite token not set yet — the owner needs to refresh it."

def _get_kite_creds():
    return "", ""

# ── Instrument cache ─────────────────────────────────────────────────────────────────────────────────
_instrument_df: Optional[pd.DataFrame] = None
_instrument_last_fetched: Optional[datetime] = None
_per_exchange_cache: dict = {}

async def _load_all_instruments():
    return broker.load_scrip_master()

async def _get_exchange_df(exchange: str):
    df = broker.load_scrip_master()
    sub = df[df["exchange"] == exchange].copy()
    if sub.empty:
        raise HTTPException(status_code=404, detail=f"No instruments for {exchange}")
    return sub


@app.on_event("startup")
async def startup_event():
    import logging
    _log = logging.getLogger("main")
    asyncio.create_task(_load_all_instruments())

    _INDEX_NAMES = {
        "NIFTY":       "NIFTY 50",
        "BANKNIFTY":   "NIFTY BANK",
        "FINNIFTY":    "NIFTY FIN SERVICE",
        "MIDCPNIFTY":  "NIFTY MID SELECT",
        "SENSEX":      "SENSEX",
        "BANKEX":      "BANKEX",
    }
    try:
        master = broker.load_scrip_master()
        idx_df = master[master["exchange_segment"] == "IDX_I"]
        skipped = []
        for key, name in _INDEX_NAMES.items():
            match = idx_df[idx_df["name"].str.upper() == name.upper()]
            if match.empty:
                skipped.append(key)
            else:
                SPOT_TOKENS[key] = str(match.iloc[0]["instrument_token"])
        if skipped:
            _log.warning("SPOT_TOKENS: could not find Dhan ids for: %s", skipped)
        _log.info("SPOT_TOKENS after Dhan lookup: %s", SPOT_TOKENS)
    except Exception as exc:
        _log.error("SPOT_TOKENS rebuild failed: %s", exc)


# ── App login / logout ─────────────────────────────────────────────────────────────────────

@app.get("/login", response_class=HTMLResponse, include_in_schema=False)
async def login_page():
    return HTMLResponse(_LOGIN_HTML)


@app.post("/login", include_in_schema=False)
async def do_login(request: Request,
                   username: str = Form(...),
                   password: str = Form(...)):
    stored = _APP_USERS.get(username, "")
    if stored and secrets.compare_digest(stored, password):
        request.session["user"] = username
        return RedirectResponse("/", status_code=302)
    error = '<div class="errmsg">Invalid username or password.</div>'
    return HTMLResponse(_LOGIN_HTML.replace("<!--ERROR-->", error), status_code=401)


@app.get("/logout", include_in_schema=False)
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login", status_code=302)


@app.get("/api/me")
async def get_me(request: Request):
    user = request.session.get("user")
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return {"username": user, "is_owner": (user == _OWNER_USERNAME)}


# ── Kite Auth ──────────────────────────────────────────────────────────────────────────────

def _require_owner(request: Request):
    """Raise 403 unless the logged-in session user is the configured owner."""
    if not _OWNER_USERNAME:
        raise HTTPException(status_code=500, detail="OWNER_USERNAME env var is not set.")
    if request.session.get("user") != _OWNER_USERNAME:
        raise HTTPException(status_code=403, detail="Owner access required.")


@app.get("/api/kite-login-url")
async def kite_login_url(request: Request):
    """Return the Kite OAuth redirect URL, built server-side from KITE_API_KEY.
    Only the owner can fetch this — non-owners get 403."""
    _require_owner(request)
    if not _KITE_API_KEY:
        raise HTTPException(status_code=500, detail="KITE_API_KEY env var is not set.")
    return {"url": f"https://kite.zerodha.com/connect/login?api_key={_KITE_API_KEY}&v=3"}


class TokenRequest(BaseModel):
    request_token: str   # api_key and api_secret now come from server env vars only


@app.post("/api/generate-token")
async def generate_token(req: TokenRequest, request: Request):
    _require_owner(request)
    if not _KITE_API_KEY or not _KITE_API_SECRET:
        raise HTTPException(status_code=500,
                            detail="KITE_API_KEY / KITE_API_SECRET env vars are not set.")
    import hashlib
    checksum = hashlib.sha256(
        f"{_KITE_API_KEY}{req.request_token}{_KITE_API_SECRET}".encode()
    ).hexdigest()
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            f"{KITE_BASE}/session/token",
            data={"api_key": _KITE_API_KEY, "request_token": req.request_token,
                  "checksum": checksum},
            headers={"X-Kite-Version": "3"},
        )
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    access_token = resp.json()["data"]["access_token"]
    _oi_save_token(access_token, _KITE_API_KEY)
    return {"access_token": access_token}


@app.get("/api/validate-token")
async def validate_token():
    return await broker.validate()


class SetTokenRequest(BaseModel):
    access_token: str


@app.post("/api/set-token")
async def set_token(req: SetTokenRequest, request: Request):
    """Owner-only: store a manually pasted access token server-side."""
    _require_owner(request)
    tok = req.access_token.strip()
    if not tok:
        raise HTTPException(status_code=400, detail="access_token is empty.")
    _oi_save_token(tok, _KITE_API_KEY)
    return {"ok": True}


# ── Instruments search ─────────────────────────────────────────────────────────────────────────

@app.get("/api/instruments")
async def get_instruments(exchange: str = "NFO", search: str = ""):
    try:
        df = await _get_exchange_df(exchange)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    if search:
        mask = df["tradingsymbol"].astype(str).str.contains(search.upper(), na=False)
        df = df[mask].head(50)
    else:
        df = df.head(100)

    cols = ["instrument_token", "tradingsymbol", "name", "expiry",
            "strike", "instrument_type", "exchange", "lot_size"]
    existing = [c for c in cols if c in df.columns]
    result = df[existing].fillna("").to_dict(orient="records")
    return JSONResponse(content=result)


# ── Options helpers ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/options/expiries")
async def get_expiries(underlying: str, exchange: str = "NFO"):
    df = await _get_exchange_df(exchange)
    mask = (
        df["name"].astype(str).str.upper().eq(underlying.upper()) &
        df["instrument_type"].isin(["CE", "PE"])
    )
    sub = df[mask]
    if sub.empty:
        return {"expiries": []}
    expiries = sorted(sub["expiry"].dropna().unique().tolist())
    return {"expiries": expiries}


@app.get("/api/options/strikes")
async def get_strikes(underlying: str, expiry: str, exchange: str = "NFO"):
    df = await _get_exchange_df(exchange)
    mask = (
        df["name"].astype(str).str.upper().eq(underlying.upper()) &
        df["expiry"].astype(str).eq(expiry) &
        df["instrument_type"].isin(["CE", "PE"])
    )
    sub = df[mask]
    if sub.empty:
        return {"strikes": []}
    strikes = sorted(sub["strike"].dropna().unique().tolist())
    return {"strikes": strikes}


@app.get("/api/options/token")
async def get_option_token(underlying: str, expiry: str, strike: float,
                            option_type: str, exchange: str = "NFO"):
    df = await _get_exchange_df(exchange)
    mask = (
        df["name"].astype(str).str.upper().eq(underlying.upper()) &
        df["expiry"].astype(str).eq(expiry) &
        df["strike"].eq(strike) &
        df["instrument_type"].eq(option_type.upper())
    )
    sub = df[mask]
    if sub.empty:
        raise HTTPException(status_code=404, detail="Option contract not found.")
    row = sub.iloc[0]
    return {"instrument_token": str(row["instrument_token"]), "tradingsymbol": row["tradingsymbol"]}


# ── Futures helpers ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/futures/expiries")
async def get_futures_expiries(underlying: str, exchange: str = "NFO"):
    df = await _get_exchange_df(exchange)
    mask = (
        df["name"].astype(str).str.upper().eq(underlying.upper()) &
        df["instrument_type"].eq("FUT")
    )
    sub = df[mask]
    expiries = sorted(sub["expiry"].dropna().unique().tolist())
    return {"expiries": expiries}


@app.get("/api/futures/token")
async def get_futures_token(underlying: str, expiry: str, exchange: str = "NFO"):
    df = await _get_exchange_df(exchange)
    mask = (
        df["name"].astype(str).str.upper().eq(underlying.upper()) &
        df["expiry"].astype(str).eq(expiry) &
        df["instrument_type"].eq("FUT")
    )
    sub = df[mask]
    if sub.empty:
        raise HTTPException(status_code=404, detail="Futures contract not found.")
    row = sub.iloc[0]
    return {"instrument_token": str(row["instrument_token"]), "tradingsymbol": row["tradingsymbol"]}


# ── Historical Data ──────────────────────────────────────────────────────────────────────────────

def _date_chunks(from_date: str, to_date: str, interval: str):
    fmt = "%Y-%m-%d"
    start = datetime.strptime(from_date, fmt)
    end   = datetime.strptime(to_date, fmt)
    chunk_days = 400 if interval == "day" else 60
    chunks, cur = [], start
    while cur <= end:
        chunk_end = min(cur + timedelta(days=chunk_days - 1), end)
        chunks.append((cur.strftime(fmt), chunk_end.strftime(fmt)))
        cur = chunk_end + timedelta(days=1)
    return chunks

async def _fetch_candles(instrument_token, from_date, to_date, interval, continuous, oi):
    return await broker.fetch_candles(instrument_token, interval, from_date, to_date, oi)

def _build_df(candles):
    if not candles:
        return pd.DataFrame()
    cols = ["date","open","high","low","close","volume","oi"] if len(candles[0]) == 7 \
           else ["date","open","high","low","close","volume"]
    df = pd.DataFrame(candles, columns=cols)
    df["date"] = pd.to_datetime(df["date"])
    return df


class HistoricalRequest(BaseModel):
    instrument_token: str
    from_date: str
    to_date: str
    interval: str
    continuous: bool = False
    oi: bool = True
    file_format: str = "csv"


@app.post("/api/historical")
async def download_historical(req: HistoricalRequest):
    candles = await _fetch_candles(req.instrument_token,
                                    req.from_date, req.to_date, req.interval,
                                    req.continuous, req.oi)
    if not candles:
        raise HTTPException(status_code=404, detail="No data returned for this range.")
    df = _build_df(candles)
    fname = f"{req.instrument_token}_{req.interval}_{req.from_date}_{req.to_date}"
    if req.file_format == "csv":
        buf = io.StringIO(); df.to_csv(buf, index=False)
        return StreamingResponse(io.BytesIO(buf.getvalue().encode()), media_type="text/csv",
                                  headers={"Content-Disposition": f'attachment; filename="{fname}.csv"'})
    elif req.file_format == "json":
        df["date"] = df["date"].astype(str)
        return StreamingResponse(io.BytesIO(df.to_json(orient="records", indent=2).encode()),
                                  media_type="application/json",
                                  headers={"Content-Disposition": f'attachment; filename="{fname}.json"'})
    elif req.file_format == "parquet":
        if not PARQUET_OK:
            raise HTTPException(status_code=400, detail="Parquet not available on this server.")
        buf = io.BytesIO()
        df.to_parquet(buf, index=False, engine="fastparquet")
        buf.seek(0)
        return StreamingResponse(buf,
                                  media_type="application/octet-stream",
                                  headers={"Content-Disposition": f'attachment; filename="{fname}.parquet"'})
    raise HTTPException(status_code=400, detail="Use csv, json, or parquet.")


@app.post("/api/preview")
async def preview_historical(req: HistoricalRequest):
    fmt = "%Y-%m-%d"
    preview_end = min(
        datetime.strptime(req.from_date, fmt) + timedelta(days=5),
        datetime.strptime(req.to_date, fmt),
    ).strftime(fmt)
    candles = await _fetch_candles(req.instrument_token,
                                    req.from_date, preview_end, req.interval,
                                    req.continuous, req.oi)
    if not candles:
        return {"rows": [], "total": 0}
    df = _build_df(candles[:5])
    df["date"] = df["date"].astype(str)
    return {"rows": df.to_dict(orient="records"), "total": len(candles)}


# ── Spot token map ──────────────────────────────────────────────────────────────────────────────────
# FIX: Added FINNIFTY, MIDCPNIFTY, SENSEX, BANKEX spot tokens
SPOT_TOKENS = {
    "NIFTY":       "256265",   # NSE:NIFTY 50
    "BANKNIFTY":   "260105",   # NSE:NIFTY BANK
    "FINNIFTY":    "257801",   # NSE:NIFTY FIN SERVICE
    "MIDCPNIFTY":  "288009",   # NSE:NIFTY MID SELECT
    "SENSEX":      "265",      # BSE:SENSEX
    "BANKEX":      "274441",   # BSE:BANKEX
}

STEP_SIZE = {
    "NIFTY":      50,
    "BANKNIFTY":  100,
    "FINNIFTY":   50,
    "MIDCPNIFTY": 25,
    "SENSEX":     100,
    "BANKEX":     100,
}

# ── Options Chain endpoint ────────────────────────────────────────────────────────────────────────────

class ChainRequest(BaseModel):
    index: str
    expiry_date: str
    expiry_number: int
    expiry_type: str
    strike_range: int
    interval: str
    from_date: str
    to_date: str
    file_format: str = "parquet"

def _resample_75min(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date")
    extra_cols = [c for c in df.columns if c not in ["open","high","low","close","volume","oi"]]
    ohlcv = df[["open","high","low","close","volume","oi"]].resample("75min", label="left", closed="left").agg({
        "open": "first", "high": "max", "low": "min", "close": "last",
        "volume": "sum", "oi": "last"
    }).dropna(subset=["open"])
    for col in extra_cols:
        ohlcv[col] = df[col].resample("75min", label="left", closed="left").last()
    return ohlcv.reset_index()

@app.post("/api/options-chain")
async def download_options_chain(req: ChainRequest):
    index = req.index.upper()
    exchange = "BFO" if index in ("SENSEX", "BANKEX") else "NFO"
    step = STEP_SIZE.get(index, 50)
    actual_interval = "15minute" if req.interval == "75minute" else req.interval

    interval_label_map = {
        "minute": 1, "3minute": 3, "5minute": 5, "10minute": 10,
        "15minute": 15, "30minute": 30, "60minute": 60, "75minute": 75, "day": 1440
    }
    interval_min = interval_label_map.get(req.interval, 0)

    # ── 1. Get spot data ────────────────────────────────────────────────────────────────────
    spot_token = SPOT_TOKENS.get(index)
    spot_df = pd.DataFrame()
    if spot_token:
        try:
            spot_candles = await _fetch_candles(
                spot_token,
                req.from_date, req.to_date, actual_interval, False, False
            )
            if spot_candles:
                spot_df = _build_df(spot_candles)
                spot_df = spot_df[["date","close"]].rename(columns={"close":"spot"})
                spot_df["date"] = pd.to_datetime(spot_df["date"])
                if req.interval == "75minute":
                    spot_df = spot_df.set_index("date")["spot"].resample(
                        "75min", label="left", closed="left"
                    ).last().reset_index()
        except Exception:
            pass

    # ── 2. Get instrument list for this expiry ─────────────────────────────────────────────
    df_instr = await _get_exchange_df(exchange)
    mask = (
        df_instr["name"].astype(str).str.upper().eq(index) &
        df_instr["expiry"].astype(str).eq(req.expiry_date) &
        df_instr["instrument_type"].isin(["CE","PE"])
    )
    contracts = df_instr[mask].copy()

    if contracts.empty:
        raise HTTPException(status_code=404, detail=f"No contracts found for {index} expiry {req.expiry_date}")

    # ── 3. Determine ATM strike ──────────────────────────────────────────────────────────────────
    if not spot_df.empty:
        last_spot = float(spot_df["spot"].dropna().iloc[-1])
    else:
        strikes_available = sorted(contracts["strike"].unique())
        last_spot = float(strikes_available[len(strikes_available)//2])

    atm_raw = round(last_spot / step) * step

    # ── 4. Build strike list ─────────────────────────────────────────────────────────────────────
    strikes = [atm_raw + i * step for i in range(-req.strike_range, req.strike_range + 1)]

    # ── 5. Download each strike ─────────────────────────────────────────────────────────────────────
    all_frames = []

    for otype in ["CE", "PE"]:
        for i, strike in enumerate(strikes):
            atm_offset = i - req.strike_range

            match = contracts[
                (contracts["strike"] == strike) &
                (contracts["instrument_type"] == otype)
            ]
            if match.empty:
                continue

            token = str(match.iloc[0]["instrument_token"])
            sym   = match.iloc[0]["tradingsymbol"]

            try:
                candles = await _fetch_candles(
                    token,
                    req.from_date, req.to_date, actual_interval, False, True
                )
            except Exception:
                continue

            if not candles:
                continue

            df = _build_df(candles)
            if df.empty:
                continue

            df["date"] = pd.to_datetime(df["date"])

            if req.interval == "75minute":
                df = _resample_75min(df)

            if not spot_df.empty:
                spot_df["date"] = pd.to_datetime(spot_df["date"])
                df = df.merge(spot_df, on="date", how="left")
                df["spot"] = df["spot"].ffill()
            else:
                df["spot"] = None

            if "spot" in df.columns and df["spot"].notna().any():
                df["atm_strike"] = (df["spot"] / step).round() * step
                df["strike_offset"] = ((strike - df["atm_strike"]) / step).round().astype(int)
                df["strike_index"] = df["strike_offset"].apply(
                    lambda x: "ATM" if x == 0 else (f"ATM+{x}" if x > 0 else f"ATM{x}")
                )
            else:
                atm_label = "ATM" if atm_offset == 0 else (f"ATM+{atm_offset}" if atm_offset > 0 else f"ATM{atm_offset}")
                df["strike_index"] = atm_label

            df["strike"]        = strike
            df["symbol"]        = sym
            df["option_type"]   = "CALL" if otype == "CE" else "PUT"
            df["expiry_date"]   = req.expiry_date
            df["expiry_number"] = req.expiry_number
            expiry_label = {"weekly": "WEEK", "monthly": "MONTHLY", "quarterly": "QUARTERLY"}.get(req.expiry_type.lower(), req.expiry_type.upper())
            df["expiry_type"]   = expiry_label
            df["interval_min"]  = interval_min
            df["index"]         = index

            all_frames.append(df)
            await asyncio.sleep(0.1)

    if not all_frames:
        raise HTTPException(status_code=404, detail="No data found for any strike in this expiry.")

    combined = pd.concat(all_frames, ignore_index=True)
    combined = combined.sort_values(["date","option_type","strike"]).reset_index(drop=True)

    combined["datetime"] = combined["date"].astype(str)
    combined["timestamp"] = pd.to_datetime(combined["date"]).astype("int64") // 10**9
    cols_order = ["timestamp","datetime","open","high","low","close","volume","oi",
                  "spot","strike","strike_index","symbol","option_type",
                  "expiry_date","expiry_number","expiry_type","index","interval_min"]
    existing = [c for c in cols_order if c in combined.columns]
    combined = combined[existing]

    step_label = str(interval_min) + "min" if req.interval != "day" else "day"
    expiry_label = {"weekly": "WEEK", "monthly": "MONTHLY", "quarterly": "QUARTERLY"}.get(req.expiry_type.lower(), req.expiry_type.upper())
    fname = f"{index}_{expiry_label}_{req.expiry_date}_{step_label}"

    if req.file_format == "parquet":
        if not PARQUET_OK:
            raise HTTPException(status_code=400, detail="Parquet not available.")
        buf = io.BytesIO()
        combined.to_parquet(buf, index=False, engine="fastparquet")
        buf.seek(0)
        return StreamingResponse(buf, media_type="application/octet-stream",
            headers={"Content-Disposition": f'attachment; filename="{fname}.parquet"'})
    else:
        combined["datetime"] = combined["datetime"].astype(str)
        buf = io.StringIO()
        combined.to_csv(buf, index=False)
        return StreamingResponse(io.BytesIO(buf.getvalue().encode()), media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{fname}.csv"'})

# ── Serve frontend ──────────────────────────────────────────────────────────────────────────────────
app.mount("/", StaticFiles(directory="static", html=True), name="static")
