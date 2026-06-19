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

app = FastAPI(title="ZetaPull — Historical Data Downloader")

from oi_tracker.token_store import save_token as _oi_save_token
from oi_tracker.routes_fastapi import router as oi_router
app.include_router(oi_router)

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


# Middleware — added in reverse execution order (last added = outermost = runs first):
#   CORSMiddleware → SessionMiddleware → _AuthMiddleware → route handler
app.add_middleware(_AuthMiddleware)
app.add_middleware(SessionMiddleware, secret_key=_SECRET_KEY,
                   session_cookie="zp_sess", https_only=False, same_site="lax")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

KITE_BASE = "https://api.kite.trade"

# ── Instrument cache ─────────────────────────────────────────────────────────────────────────────────
_instrument_df: Optional[pd.DataFrame] = None
_instrument_last_fetched: Optional[datetime] = None
_per_exchange_cache: dict = {}

async def _load_all_instruments():
    global _instrument_df, _instrument_last_fetched
    now = datetime.now()
    if _instrument_df is not None and _instrument_last_fetched is not None:
        if (now - _instrument_last_fetched).total_seconds() < 3600 * 8:
            return _instrument_df
    try:
        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.get(
                "https://api.kite.trade/instruments",
                headers={"X-Kite-Version": "3"},
            )
        if resp.status_code == 200:
            _instrument_df = pd.read_csv(io.StringIO(resp.text))
            _instrument_last_fetched = now
            return _instrument_df
    except Exception:
        pass
    return None

async def _get_exchange_df(api_key: str, access_token: str, exchange: str) -> pd.DataFrame:
    global _per_exchange_cache
    cache_key = exchange
    cached = _per_exchange_cache.get(cache_key)
    if cached is not None:
        ts, df = cached
        if (datetime.now() - ts).total_seconds() < 3600 * 8:
            return df

    all_df = await _load_all_instruments()
    if all_df is not None and not all_df.empty:
        df = all_df[all_df["exchange"] == exchange].copy()
        if not df.empty:
            _per_exchange_cache[cache_key] = (datetime.now(), df)
            return df

    try:
        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.get(
                f"{KITE_BASE}/instruments/{exchange}",
                headers={
                    "X-Kite-Version": "3",
                    "Authorization": f"token {api_key}:{access_token}",
                },
            )
        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code,
                                detail=f"Kite API {resp.status_code}: {resp.text[:300]}")
        df = pd.read_csv(io.StringIO(resp.text))
        _per_exchange_cache[cache_key] = (datetime.now(), df)
        return df
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=504, detail=f"Timeout fetching {exchange} instruments. Try again in 30s.")


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(_load_all_instruments())


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
    return {"username": user}


# ── Kite Auth ──────────────────────────────────────────────────────────────────────────────

class TokenRequest(BaseModel):
    api_key: str
    request_token: str
    api_secret: str

@app.post("/api/generate-token")
async def generate_token(req: TokenRequest):
    import hashlib
    checksum = hashlib.sha256(
        f"{req.api_key}{req.request_token}{req.api_secret}".encode()
    ).hexdigest()
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            f"{KITE_BASE}/session/token",
            data={"api_key": req.api_key, "request_token": req.request_token, "checksum": checksum},
            headers={"X-Kite-Version": "3"},
        )
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    access_token = resp.json()["data"]["access_token"]
    _oi_save_token(access_token, req.api_key)
    return {"access_token": access_token}


@app.get("/api/validate-token")
async def validate_token(api_key: str, access_token: str):
    """Check if a saved token is still valid. Called on app load."""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                f"{KITE_BASE}/user/profile",
                headers={"X-Kite-Version": "3", "Authorization": f"token {api_key}:{access_token}"},
            )
        if resp.status_code == 200:
            data = resp.json().get("data", {})
            return {"valid": True, "user_name": data.get("user_name", ""), "email": data.get("email", "")}
    except Exception:
        pass
    return {"valid": False, "user_name": "", "email": ""}


# ── Instruments search ─────────────────────────────────────────────────────────────────────────

@app.get("/api/instruments")
async def get_instruments(api_key: str, access_token: str, exchange: str = "NFO", search: str = ""):
    try:
        df = await _get_exchange_df(api_key, access_token, exchange)
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
async def get_expiries(api_key: str, access_token: str, underlying: str, exchange: str = "NFO"):
    df = await _get_exchange_df(api_key, access_token, exchange)
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
async def get_strikes(api_key: str, access_token: str, underlying: str, expiry: str, exchange: str = "NFO"):
    df = await _get_exchange_df(api_key, access_token, exchange)
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
async def get_option_token(api_key: str, access_token: str, underlying: str,
                            expiry: str, strike: float, option_type: str, exchange: str = "NFO"):
    df = await _get_exchange_df(api_key, access_token, exchange)
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
async def get_futures_expiries(api_key: str, access_token: str, underlying: str, exchange: str = "NFO"):
    df = await _get_exchange_df(api_key, access_token, exchange)
    mask = (
        df["name"].astype(str).str.upper().eq(underlying.upper()) &
        df["instrument_type"].eq("FUT")
    )
    sub = df[mask]
    expiries = sorted(sub["expiry"].dropna().unique().tolist())
    return {"expiries": expiries}


@app.get("/api/futures/token")
async def get_futures_token(api_key: str, access_token: str, underlying: str,
                             expiry: str, exchange: str = "NFO"):
    df = await _get_exchange_df(api_key, access_token, exchange)
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

async def _fetch_candles(api_key, access_token, instrument_token,
                          from_date, to_date, interval, continuous, oi):
    chunks = _date_chunks(from_date, to_date, interval)
    all_candles = []
    async with httpx.AsyncClient(timeout=60) as client:
        for chunk_from, chunk_to in chunks:
            resp = await client.get(
                f"{KITE_BASE}/instruments/historical/{instrument_token}/{interval}",
                params={"from": chunk_from, "to": chunk_to,
                        "continuous": int(continuous), "oi": int(oi)},
                headers={"X-Kite-Version": "3",
                         "Authorization": f"token {api_key}:{access_token}"},
            )
            if resp.status_code != 200:
                raise HTTPException(status_code=resp.status_code, detail=resp.text)
            all_candles.extend(resp.json()["data"]["candles"])
    return all_candles

def _build_df(candles):
    if not candles:
        return pd.DataFrame()
    cols = ["date","open","high","low","close","volume","oi"] if len(candles[0]) == 7 \
           else ["date","open","high","low","close","volume"]
    df = pd.DataFrame(candles, columns=cols)
    df["date"] = pd.to_datetime(df["date"])
    return df


class HistoricalRequest(BaseModel):
    api_key: str
    access_token: str
    instrument_token: str
    from_date: str
    to_date: str
    interval: str
    continuous: bool = False
    oi: bool = True
    file_format: str = "csv"


@app.post("/api/historical")
async def download_historical(req: HistoricalRequest):
    candles = await _fetch_candles(req.api_key, req.access_token, req.instrument_token,
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
    candles = await _fetch_candles(req.api_key, req.access_token, req.instrument_token,
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
    api_key: str
    access_token: str
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
                req.api_key, req.access_token, spot_token,
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
    df_instr = await _get_exchange_df(req.api_key, req.access_token, exchange)
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
                    req.api_key, req.access_token, token,
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
