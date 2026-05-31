from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List
import httpx
import pandas as pd
import io
import json
from datetime import datetime, timedelta

app = FastAPI(title="ZetaPull — Historical Data Downloader")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

KITE_BASE = "https://api.kite.trade"


# ── Auth ──────────────────────────────────────────────────────────────────────

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


# ── Validate token ────────────────────────────────────────────────────────────

@app.get("/api/validate-token")
async def validate_token(api_key: str, access_token: str):
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(
            f"{KITE_BASE}/user/profile",
            headers={
                "X-Kite-Version": "3",
                "Authorization": f"token {api_key}:{access_token}",
            },
        )
    if resp.status_code == 200:
        data = resp.json()
        return {"valid": True, "user": data.get("data", {}).get("user_name", "")}
    return {"valid": False, "user": ""}


# ── Instruments ───────────────────────────────────────────────────────────────

_instrument_cache: dict = {}

async def _fetch_instruments(api_key: str, access_token: str, exchange: str) -> pd.DataFrame:
    cache_key = f"{api_key}:{exchange}"
    if cache_key in _instrument_cache:
        return _instrument_cache[cache_key]
    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.get(
            f"{KITE_BASE}/instruments/{exchange}",
            headers={
                "X-Kite-Version": "3",
                "Authorization": f"token {api_key}:{access_token}",
            },
        )
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    df = pd.read_csv(io.StringIO(resp.text))
    _instrument_cache[cache_key] = df
    return df


@app.get("/api/instruments")
async def get_instruments(
    api_key: str,
    access_token: str,
    exchange: str = "NFO",
    search: str = "",
):
    df = await _fetch_instruments(api_key, access_token, exchange)
    if search:
        mask = df["tradingsymbol"].str.contains(search.upper(), na=False)
        df = df[mask].head(50)
    else:
        df = df.head(100)
    cols = ["instrument_token", "tradingsymbol", "name", "expiry",
            "strike", "instrument_type", "exchange", "lot_size"]
    existing = [c for c in cols if c in df.columns]
    return df[existing].to_dict(orient="records")


# ── Options helpers ───────────────────────────────────────────────────────────

@app.get("/api/options/expiries")
async def get_expiries(
    api_key: str,
    access_token: str,
    underlying: str,          # e.g. NIFTY, BANKNIFTY, SENSEX, MIDCPNIFTY
    exchange: str = "NFO",
):
    """Return sorted list of expiry dates for an underlying."""
    df = await _fetch_instruments(api_key, access_token, exchange)
    mask = (
        df["name"].str.upper().eq(underlying.upper()) &
        df["instrument_type"].isin(["CE", "PE"])
    )
    sub = df[mask]
    if sub.empty:
        return {"expiries": []}
    expiries = sorted(sub["expiry"].dropna().unique().tolist())
    return {"expiries": expiries}


@app.get("/api/options/strikes")
async def get_strikes(
    api_key: str,
    access_token: str,
    underlying: str,
    expiry: str,               # YYYY-MM-DD
    exchange: str = "NFO",
):
    """Return sorted strike list for underlying + expiry."""
    df = await _fetch_instruments(api_key, access_token, exchange)
    mask = (
        df["name"].str.upper().eq(underlying.upper()) &
        df["expiry"].eq(expiry) &
        df["instrument_type"].isin(["CE", "PE"])
    )
    sub = df[mask]
    if sub.empty:
        return {"strikes": []}
    strikes = sorted(sub["strike"].dropna().unique().tolist())
    return {"strikes": strikes}


@app.get("/api/options/token")
async def get_option_token(
    api_key: str,
    access_token: str,
    underlying: str,
    expiry: str,
    strike: float,
    option_type: str,          # CE or PE
    exchange: str = "NFO",
):
    """Return instrument_token + tradingsymbol for a specific option."""
    df = await _fetch_instruments(api_key, access_token, exchange)
    mask = (
        df["name"].str.upper().eq(underlying.upper()) &
        df["expiry"].eq(expiry) &
        df["strike"].eq(strike) &
        df["instrument_type"].eq(option_type.upper())
    )
    sub = df[mask]
    if sub.empty:
        raise HTTPException(status_code=404, detail="Option contract not found.")
    row = sub.iloc[0]
    return {
        "instrument_token": str(row["instrument_token"]),
        "tradingsymbol": row["tradingsymbol"],
    }


# ── Futures helper ────────────────────────────────────────────────────────────

@app.get("/api/futures/expiries")
async def get_futures_expiries(
    api_key: str,
    access_token: str,
    underlying: str,
    exchange: str = "NFO",
):
    df = await _fetch_instruments(api_key, access_token, exchange)
    mask = (
        df["name"].str.upper().eq(underlying.upper()) &
        df["instrument_type"].eq("FUT")
    )
    sub = df[mask]
    expiries = sorted(sub["expiry"].dropna().unique().tolist())
    return {"expiries": expiries}


@app.get("/api/futures/token")
async def get_futures_token(
    api_key: str,
    access_token: str,
    underlying: str,
    expiry: str,
    exchange: str = "NFO",
):
    df = await _fetch_instruments(api_key, access_token, exchange)
    mask = (
        df["name"].str.upper().eq(underlying.upper()) &
        df["expiry"].eq(expiry) &
        df["instrument_type"].eq("FUT")
    )
    sub = df[mask]
    if sub.empty:
        raise HTTPException(status_code=404, detail="Futures contract not found.")
    row = sub.iloc[0]
    return {
        "instrument_token": str(row["instrument_token"]),
        "tradingsymbol": row["tradingsymbol"],
    }


# ── Historical Data (chunked to handle large date ranges) ────────────────────

def _date_chunks(from_date: str, to_date: str, interval: str):
    """
    Kite API limits:
      minute/3minute/5minute: 60 days per call
      10minute/15minute/30minute/60minute: 60 days per call
      day: 400 days per call
    We use conservative 60-day chunks for intraday, 400-day for day.
    """
    fmt = "%Y-%m-%d"
    start = datetime.strptime(from_date, fmt)
    end = datetime.strptime(to_date, fmt)
    chunk_days = 400 if interval == "day" else 60
    chunks = []
    cur = start
    while cur <= end:
        chunk_end = min(cur + timedelta(days=chunk_days - 1), end)
        chunks.append((cur.strftime(fmt), chunk_end.strftime(fmt)))
        cur = chunk_end + timedelta(days=1)
    return chunks


async def _fetch_candles(
    api_key: str,
    access_token: str,
    instrument_token: str,
    from_date: str,
    to_date: str,
    interval: str,
    continuous: bool,
    oi: bool,
) -> list:
    chunks = _date_chunks(from_date, to_date, interval)
    all_candles = []
    async with httpx.AsyncClient(timeout=60) as client:
        for chunk_from, chunk_to in chunks:
            resp = await client.get(
                f"{KITE_BASE}/instruments/historical/{instrument_token}/{interval}",
                params={
                    "from": chunk_from,
                    "to": chunk_to,
                    "continuous": int(continuous),
                    "oi": int(oi),
                },
                headers={
                    "X-Kite-Version": "3",
                    "Authorization": f"token {api_key}:{access_token}",
                },
            )
            if resp.status_code != 200:
                raise HTTPException(status_code=resp.status_code, detail=resp.text)
            candles = resp.json()["data"]["candles"]
            all_candles.extend(candles)
    return all_candles


def _build_df(candles: list, oi: bool) -> pd.DataFrame:
    if not candles:
        return pd.DataFrame()
    sample = candles[0]
    if len(sample) == 7:
        cols = ["date", "open", "high", "low", "close", "volume", "oi"]
    else:
        cols = ["date", "open", "high", "low", "close", "volume"]
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
    candles = await _fetch_candles(
        req.api_key, req.access_token, req.instrument_token,
        req.from_date, req.to_date, req.interval,
        req.continuous, req.oi,
    )
    if not candles:
        raise HTTPException(status_code=404, detail="No data returned for this range.")

    df = _build_df(candles, req.oi)
    fname_base = f"{req.instrument_token}_{req.interval}_{req.from_date}_{req.to_date}"
    fmt = req.file_format.lower()

    if fmt == "csv":
        buf = io.StringIO()
        df.to_csv(buf, index=False)
        return StreamingResponse(
            io.BytesIO(buf.getvalue().encode()),
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{fname_base}.csv"'},
        )
    elif fmt == "json":
        df["date"] = df["date"].astype(str)
        return StreamingResponse(
            io.BytesIO(df.to_json(orient="records", indent=2).encode()),
            media_type="application/json",
            headers={"Content-Disposition": f'attachment; filename="{fname_base}.json"'},
        )
    else:
        raise HTTPException(status_code=400, detail="Invalid file format. Use csv or json.")


@app.post("/api/preview")
async def preview_historical(req: HistoricalRequest):
    # For preview use a small window: from_date to min(from_date+5days, to_date)
    fmt = "%Y-%m-%d"
    preview_end = min(
        datetime.strptime(req.from_date, fmt) + timedelta(days=5),
        datetime.strptime(req.to_date, fmt),
    ).strftime(fmt)

    candles = await _fetch_candles(
        req.api_key, req.access_token, req.instrument_token,
        req.from_date, preview_end, req.interval,
        req.continuous, req.oi,
    )
    if not candles:
        return {"rows": [], "total": 0}
    df = _build_df(candles[:5], req.oi)
    df["date"] = df["date"].astype(str)
    return {"rows": df.to_dict(orient="records"), "total": len(candles)}


# ── Serve frontend ────────────────────────────────────────────────────────────
app.mount("/", StaticFiles(directory="static", html=True), name="static")
