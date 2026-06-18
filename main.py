from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import httpx
import pandas as pd
import io
import json
import asyncio
from datetime import datetime, date

app = FastAPI(title="ZetaPull — Historical Data Downloader")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

KITE_BASE = "https://api.kite.trade"

# ── In-memory instrument cache ────────────────────────────────────────────────
# Stored as a list of dicts after fetching. Cleared on new fetch.
_instrument_cache: list = []
_cache_fetched_at: Optional[str] = None


# ── Auth ──────────────────────────────────────────────────────────────────────

class TokenRequest(BaseModel):
    api_key: str
    request_token: str
    api_secret: str


@app.post("/api/generate-token")
async def generate_token(req: TokenRequest):
    """Exchange request_token for access_token using Kite login flow."""
    import hashlib
    checksum = hashlib.sha256(
        f"{req.api_key}{req.request_token}{req.api_secret}".encode()
    ).hexdigest()
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{KITE_BASE}/session/token",
            data={
                "api_key": req.api_key,
                "request_token": req.request_token,
                "checksum": checksum,
            },
            headers={"X-Kite-Version": "3"},
        )
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    data = resp.json()
    return {"access_token": data["data"]["access_token"]}


# ── Fetch Instruments (all exchanges, cached) ─────────────────────────────────

class FetchInstrumentsRequest(BaseModel):
    api_key: str
    access_token: str


async def _fetch_exchange(client: httpx.AsyncClient, api_key: str, access_token: str, exchange: str) -> list:
    """Fetch instruments for a single exchange and return as list of dicts."""
    try:
        resp = await client.get(
            f"{KITE_BASE}/instruments/{exchange}",
            headers={
                "X-Kite-Version": "3",
                "Authorization": f"token {api_key}:{access_token}",
            },
            timeout=60,
        )
        if resp.status_code != 200:
            return []
        df = pd.read_csv(io.StringIO(resp.text))
        df["exchange"] = exchange
        # Keep only essential columns to reduce memory
        keep_cols = [c for c in [
            "instrument_token", "tradingsymbol", "name", "exchange",
            "segment", "instrument_type", "expiry", "strike", "lot_size", "tick_size"
        ] if c in df.columns]
        df = df[keep_cols]
        # Convert expiry to string safely
        if "expiry" in df.columns:
            df["expiry"] = df["expiry"].astype(str).replace("nan", "")
        if "strike" in df.columns:
            df["strike"] = pd.to_numeric(df["strike"], errors="coerce").fillna(0)
        return df.to_dict(orient="records")
    except Exception as e:
        return []


@app.post("/api/fetch-instruments")
async def fetch_instruments(req: FetchInstrumentsRequest):
    """Fetch instruments from NFO, BFO, NSE, BSE in parallel and cache in memory."""
    global _instrument_cache, _cache_fetched_at

    exchanges = ["NFO", "BFO", "NSE", "BSE"]
    async with httpx.AsyncClient() as client:
        results = await asyncio.gather(*[
            _fetch_exchange(client, req.api_key, req.access_token, ex)
            for ex in exchanges
        ])

    all_instruments = []
    counts = {}
    for ex, records in zip(exchanges, results):
        counts[ex] = len(records)
        all_instruments.extend(records)

    _instrument_cache = all_instruments
    _cache_fetched_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return {
        "total": len(all_instruments),
        "counts": counts,
        "fetched_at": _cache_fetched_at,
    }


@app.get("/api/instruments/symbols")
async def get_symbols(exchange: str = "", segment: str = ""):
    """Return unique symbols from the cached instrument list, optionally filtered."""
    if not _instrument_cache:
        raise HTTPException(status_code=400, detail="Instruments not fetched yet. Click 'Fetch Instruments' first.")

    df = pd.DataFrame(_instrument_cache)
    if exchange:
        df = df[df["exchange"] == exchange]
    if segment:
        df = df[df["segment"] == segment]

    symbols = sorted(df["tradingsymbol"].dropna().unique().tolist())
    return {"symbols": symbols, "total": len(symbols)}


@app.get("/api/instruments/search")
async def search_instruments(q: str, exchange: str = "", instrument_type: str = ""):
    """Search instruments by symbol name from cache."""
    if not _instrument_cache:
        raise HTTPException(status_code=400, detail="Instruments not fetched yet.")

    df = pd.DataFrame(_instrument_cache)
    q_upper = q.upper()

    if exchange:
        df = df[df["exchange"] == exchange]
    if instrument_type:
        df = df[df["instrument_type"] == instrument_type]

    mask = df["tradingsymbol"].str.upper().str.contains(q_upper, na=False)
    results = df[mask].head(50).to_dict(orient="records")
    return {"results": results, "total": len(results)}


@app.get("/api/instruments/expiries")
async def get_expiries(underlying: str, exchange: str = "NFO"):
    """Return sorted expiry dates for a given underlying (e.g. NIFTY, BANKNIFTY)."""
    if not _instrument_cache:
        raise HTTPException(status_code=400, detail="Instruments not fetched yet.")

    df = pd.DataFrame(_instrument_cache)
    df = df[df["exchange"] == exchange]
    df = df[df["name"].str.upper() == underlying.upper()] if "name" in df.columns else df[df["tradingsymbol"].str.startswith(underlying.upper())]
    df = df[df["expiry"].str.len() > 0]
    expiries = sorted(df["expiry"].dropna().unique().tolist())
    return {"expiries": expiries}


@app.get("/api/instruments/strikes")
async def get_strikes(underlying: str, expiry: str, exchange: str = "NFO"):
    """Return sorted strikes for a given underlying + expiry."""
    if not _instrument_cache:
        raise HTTPException(status_code=400, detail="Instruments not fetched yet.")

    df = pd.DataFrame(_instrument_cache)
    df = df[df["exchange"] == exchange]
    if "name" in df.columns:
        df = df[df["name"].str.upper() == underlying.upper()]
    df = df[df["expiry"] == expiry]
    df = df[df["instrument_type"].isin(["CE", "PE"])]
    df = df[df["strike"] > 0]
    strikes = sorted(df["strike"].unique().tolist())
    return {"strikes": strikes}


@app.get("/api/instruments/resolve")
async def resolve_token(underlying: str, expiry: str, strike: float, option_type: str, exchange: str = "NFO"):
    """Resolve instrument_token for a specific option contract."""
    if not _instrument_cache:
        raise HTTPException(status_code=400, detail="Instruments not fetched yet.")

    df = pd.DataFrame(_instrument_cache)
    df = df[df["exchange"] == exchange]
    if "name" in df.columns:
        df = df[df["name"].str.upper() == underlying.upper()]
    df = df[df["expiry"] == expiry]
    df = df[df["instrument_type"] == option_type.upper()]
    df = df[df["strike"] == strike]

    if df.empty:
        raise HTTPException(status_code=404, detail=f"No instrument found for {underlying} {expiry} {strike} {option_type}")

    row = df.iloc[0]
    return {
        "instrument_token": str(row["instrument_token"]),
        "tradingsymbol": row["tradingsymbol"],
        "exchange": row["exchange"],
        "lot_size": int(row.get("lot_size", 0)),
    }


@app.get("/api/instruments/resolve-futures")
async def resolve_futures_token(underlying: str, expiry: str, exchange: str = "NFO"):
    """Resolve instrument_token for a futures contract."""
    if not _instrument_cache:
        raise HTTPException(status_code=400, detail="Instruments not fetched yet.")

    df = pd.DataFrame(_instrument_cache)
    df = df[df["exchange"] == exchange]
    if "name" in df.columns:
        df = df[df["name"].str.upper() == underlying.upper()]
    df = df[df["expiry"] == expiry]
    df = df[df["instrument_type"] == "FUT"]

    if df.empty:
        raise HTTPException(status_code=404, detail=f"No futures found for {underlying} {expiry}")

    row = df.iloc[0]
    return {
        "instrument_token": str(row["instrument_token"]),
        "tradingsymbol": row["tradingsymbol"],
        "exchange": row["exchange"],
        "lot_size": int(row.get("lot_size", 0)),
    }


@app.get("/api/instruments/resolve-equity")
async def resolve_equity_token(symbol: str, exchange: str = "NSE"):
    """Resolve instrument_token for an equity/ETF."""
    if not _instrument_cache:
        raise HTTPException(status_code=400, detail="Instruments not fetched yet.")

    df = pd.DataFrame(_instrument_cache)
    df = df[df["exchange"] == exchange]
    df = df[df["tradingsymbol"].str.upper() == symbol.upper()]
    df = df[df["instrument_type"] == "EQ"] if "EQ" in df["instrument_type"].values else df

    if df.empty:
        raise HTTPException(status_code=404, detail=f"No equity found for {symbol} on {exchange}")

    row = df.iloc[0]
    return {
        "instrument_token": str(row["instrument_token"]),
        "tradingsymbol": row["tradingsymbol"],
        "exchange": row["exchange"],
    }


@app.get("/api/instruments/cache-status")
async def cache_status():
    """Return how many instruments are cached and when."""
    return {
        "cached": len(_instrument_cache),
        "fetched_at": _cache_fetched_at,
    }


# ── Historical Data ───────────────────────────────────────────────────────────

class HistoricalRequest(BaseModel):
    api_key: str
    access_token: str
    instrument_token: str
    tradingsymbol: str = ""     # for filename
    from_date: str              # YYYY-MM-DD
    to_date: str                # YYYY-MM-DD
    interval: str               # minute, 3minute, 5minute, 10minute, 15minute, 30minute, 60minute, day
    continuous: bool = False
    oi: bool = True
    file_format: str = "csv"    # csv | json


def _build_df(candles: list) -> pd.DataFrame:
    df = pd.DataFrame(candles, columns=["date", "open", "high", "low", "close", "volume", "oi"])
    df["date"] = pd.to_datetime(df["date"])
    return df


@app.post("/api/historical")
async def download_historical(req: HistoricalRequest):
    """Download historical OHLCV+OI data and return as file."""
    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.get(
            f"{KITE_BASE}/instruments/historical/{req.instrument_token}/{req.interval}",
            params={
                "from": req.from_date,
                "to": req.to_date,
                "continuous": int(req.continuous),
                "oi": int(req.oi),
            },
            headers={
                "X-Kite-Version": "3",
                "Authorization": f"token {req.api_key}:{req.access_token}",
            },
        )
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)

    candles = resp.json()["data"]["candles"]
    if not candles:
        raise HTTPException(status_code=404, detail="No data returned for this range.")

    df = _build_df(candles)

    fmt = req.file_format.lower()
    sym = req.tradingsymbol or req.instrument_token
    fname_base = f"{sym}_{req.interval}_{req.from_date}_{req.to_date}"

    if fmt == "csv":
        buf = io.StringIO()
        df.to_csv(buf, index=False)
        buf.seek(0)
        return StreamingResponse(
            io.BytesIO(buf.getvalue().encode()),
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{fname_base}.csv"'},
        )
    elif fmt == "json":
        df["date"] = df["date"].astype(str)
        buf = io.BytesIO(df.to_json(orient="records", indent=2).encode())
        return StreamingResponse(
            buf,
            media_type="application/json",
            headers={"Content-Disposition": f'attachment; filename="{fname_base}.json"'},
        )
    else:
        raise HTTPException(status_code=400, detail="Invalid file format. Use csv or json.")


# ── Preview ───────────────────────────────────────────────────────────────────

@app.post("/api/preview")
async def preview_historical(req: HistoricalRequest):
    """Return first 5 rows as JSON for preview."""
    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.get(
            f"{KITE_BASE}/instruments/historical/{req.instrument_token}/{req.interval}",
            params={
                "from": req.from_date,
                "to": req.to_date,
                "continuous": int(req.continuous),
                "oi": int(req.oi),
            },
            headers={
                "X-Kite-Version": "3",
                "Authorization": f"token {req.api_key}:{req.access_token}",
            },
        )
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)

    candles = resp.json()["data"]["candles"]
    if not candles:
        return {"rows": [], "total": 0}

    df = _build_df(candles[:5])
    df["date"] = df["date"].astype(str)
    return {"rows": df.to_dict(orient="records"), "total": len(candles)}


# ── Serve frontend ────────────────────────────────────────────────────────────
app.mount("/", StaticFiles(directory="static", html=True), name="static")
