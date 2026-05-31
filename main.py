from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import httpx
import pandas as pd
import io
import json
from datetime import datetime, timedelta
import asyncio
try:
    import fastparquet
    PARQUET_OK = True
except ImportError:
    PARQUET_OK = False

app = FastAPI(title="ZetaPull — Historical Data Downloader")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

KITE_BASE = "https://api.kite.trade"

# ── Instrument cache ───────────────────────────────────────────────────────────
# Zerodha publishes a public instruments CSV daily. No auth needed.
# We fetch it once on startup and cache it for the day.
# Per Zerodha docs: fetch once daily, ideally at 08:30 AM.

_instrument_df: Optional[pd.DataFrame] = None
_instrument_last_fetched: Optional[datetime] = None
_per_exchange_cache: dict = {}

async def _load_all_instruments():
    """Fetch the complete instrument dump from Zerodha (public, no auth)."""
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
    """Get instruments for a specific exchange. Uses cached all-instruments if available,
    else falls back to authenticated per-exchange endpoint."""
    global _per_exchange_cache

    cache_key = exchange
    cached = _per_exchange_cache.get(cache_key)
    if cached is not None:
        ts, df = cached
        if (datetime.now() - ts).total_seconds() < 3600 * 8:
            return df

    # Try public all-instruments dump first
    all_df = await _load_all_instruments()
    if all_df is not None and not all_df.empty:
        df = all_df[all_df["exchange"] == exchange].copy()
        if not df.empty:
            _per_exchange_cache[cache_key] = (datetime.now(), df)
            return df

    # Fallback: authenticated per-exchange endpoint
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
    """Pre-load instruments on startup so first user request is fast."""
    asyncio.create_task(_load_all_instruments())


# ── Auth ───────────────────────────────────────────────────────────────────────

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
    return {"access_token": resp.json()["data"]["access_token"]}


@app.get("/api/validate-token")
async def validate_token(api_key: str, access_token: str):
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                f"{KITE_BASE}/user/profile",
                headers={"X-Kite-Version": "3", "Authorization": f"token {api_key}:{access_token}"},
            )
        if resp.status_code == 200:
            return {"valid": True, "user": resp.json().get("data", {}).get("user_name", "")}
    except Exception:
        pass
    return {"valid": False, "user": ""}


# ── Instruments search ─────────────────────────────────────────────────────────

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


# ── Options helpers ────────────────────────────────────────────────────────────

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


# ── Futures helpers ────────────────────────────────────────────────────────────

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


# ── Historical Data ────────────────────────────────────────────────────────────

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


# ── Serve frontend ─────────────────────────────────────────────────────────────
app.mount("/", StaticFiles(directory="static", html=True), name="static")
