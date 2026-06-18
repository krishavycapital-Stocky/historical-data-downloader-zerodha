from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import httpx
import pandas as pd
import io
import asyncio
from datetime import datetime

app = FastAPI(title="ZetaPull — Historical Data Downloader")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

KITE_BASE = "https://api.kite.trade"

# ── In-memory instrument cache ─────────────────────────────────────────────────
_instrument_cache: list = []
_cache_fetched_at: Optional[str] = None


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


# ── Fetch Instruments ──────────────────────────────────────────────────────────

class FetchInstrumentsRequest(BaseModel):
    api_key: str
    access_token: str


async def _fetch_exchange(client: httpx.AsyncClient, api_key: str, access_token: str, exchange: str) -> list:
    try:
        resp = await client.get(
            f"{KITE_BASE}/instruments/{exchange}",
            headers={"X-Kite-Version": "3", "Authorization": f"token {api_key}:{access_token}"},
            timeout=90,
        )
        if resp.status_code != 200:
            return []
        df = pd.read_csv(io.StringIO(resp.text))
        df["exchange"] = exchange
        keep_cols = [c for c in [
            "instrument_token", "tradingsymbol", "name", "exchange",
            "segment", "instrument_type", "expiry", "strike", "lot_size"
        ] if c in df.columns]
        df = df[keep_cols]
        if "expiry" in df.columns:
            df["expiry"] = df["expiry"].fillna("").astype(str)
            df["expiry"] = df["expiry"].replace("nan", "")
        if "strike" in df.columns:
            df["strike"] = pd.to_numeric(df["strike"], errors="coerce").fillna(0)
        if "name" in df.columns:
            df["name"] = df["name"].fillna("").astype(str)
        return df.to_dict(orient="records")
    except Exception:
        return []


@app.post("/api/fetch-instruments")
async def fetch_instruments(req: FetchInstrumentsRequest):
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
    return {"total": len(all_instruments), "counts": counts, "fetched_at": _cache_fetched_at}


@app.get("/api/instruments/cache-status")
async def cache_status():
    return {"cached": len(_instrument_cache), "fetched_at": _cache_fetched_at}


@app.get("/api/instruments/expiries")
async def get_expiries(underlying: str, exchange: str = "NFO"):
    if not _instrument_cache:
        raise HTTPException(status_code=400, detail="Instruments not fetched yet.")
    df = pd.DataFrame(_instrument_cache)
    df = df[df["exchange"] == exchange]
    # match by name column first, fallback to tradingsymbol prefix
    if "name" in df.columns:
        matched = df[df["name"].str.upper() == underlying.upper()]
        if matched.empty:
            matched = df[df["tradingsymbol"].str.upper().str.startswith(underlying.upper())]
    else:
        matched = df[df["tradingsymbol"].str.upper().str.startswith(underlying.upper())]
    matched = matched[matched["expiry"].str.len() > 0]
    expiries = sorted(matched["expiry"].dropna().unique().tolist())
    return {"expiries": expiries}


@app.get("/api/instruments/strikes")
async def get_strikes(underlying: str, expiry: str, exchange: str = "NFO"):
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
        raise HTTPException(status_code=404, detail=f"Contract not found: {underlying} {expiry} {strike} {option_type}")
    row = df.iloc[0]
    return {"instrument_token": str(row["instrument_token"]), "tradingsymbol": row["tradingsymbol"], "lot_size": int(row.get("lot_size", 0))}


@app.get("/api/instruments/resolve-futures")
async def resolve_futures_token(underlying: str, expiry: str, exchange: str = "NFO"):
    if not _instrument_cache:
        raise HTTPException(status_code=400, detail="Instruments not fetched yet.")
    df = pd.DataFrame(_instrument_cache)
    df = df[df["exchange"] == exchange]
    if "name" in df.columns:
        df = df[df["name"].str.upper() == underlying.upper()]
    df = df[df["expiry"] == expiry]
    df = df[df["instrument_type"] == "FUT"]
    if df.empty:
        raise HTTPException(status_code=404, detail=f"Futures not found: {underlying} {expiry}")
    row = df.iloc[0]
    return {"instrument_token": str(row["instrument_token"]), "tradingsymbol": row["tradingsymbol"], "lot_size": int(row.get("lot_size", 0))}


@app.get("/api/instruments/resolve-equity")
async def resolve_equity_token(symbol: str, exchange: str = "NSE"):
    if not _instrument_cache:
        raise HTTPException(status_code=400, detail="Instruments not fetched yet.")
    df = pd.DataFrame(_instrument_cache)
    df = df[df["exchange"] == exchange]
    df = df[df["tradingsymbol"].str.upper() == symbol.upper()]
    # Prefer EQ type, but accept any if EQ not present (handles ETFs like GOLDBEES)
    eq_df = df[df["instrument_type"] == "EQ"]
    df = eq_df if not eq_df.empty else df
    if df.empty:
        raise HTTPException(status_code=404, detail=f"Symbol not found: {symbol} on {exchange}")
    row = df.iloc[0]
    return {"instrument_token": str(row["instrument_token"]), "tradingsymbol": row["tradingsymbol"], "instrument_type": str(row.get("instrument_type", ""))}


# ── Historical Data ────────────────────────────────────────────────────────────

class HistoricalRequest(BaseModel):
    api_key: str
    access_token: str
    instrument_token: str
    tradingsymbol: str = ""
    from_date: str
    to_date: str
    interval: str
    continuous: bool = False
    oi: bool = True
    file_format: str = "csv"


def _build_df(candles: list) -> pd.DataFrame:
    if not candles:
        return pd.DataFrame()
    cols = ["date", "open", "high", "low", "close", "volume", "oi"] if len(candles[0]) == 7 \
           else ["date", "open", "high", "low", "close", "volume"]
    df = pd.DataFrame(candles, columns=cols)
    df["date"] = pd.to_datetime(df["date"])
    return df


@app.post("/api/historical")
async def download_historical(req: HistoricalRequest):
    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.get(
            f"{KITE_BASE}/instruments/historical/{req.instrument_token}/{req.interval}",
            params={"from": req.from_date, "to": req.to_date, "continuous": int(req.continuous), "oi": int(req.oi)},
            headers={"X-Kite-Version": "3", "Authorization": f"token {req.api_key}:{req.access_token}"},
        )
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    candles = resp.json()["data"]["candles"]
    if not candles:
        raise HTTPException(status_code=404, detail="No data returned for this range.")
    df = _build_df(candles)
    sym = req.tradingsymbol or req.instrument_token
    fname = f"{sym}_{req.interval}_{req.from_date}_{req.to_date}"
    if req.file_format == "csv":
        buf = io.StringIO()
        df.to_csv(buf, index=False)
        return StreamingResponse(io.BytesIO(buf.getvalue().encode()), media_type="text/csv",
                                 headers={"Content-Disposition": f'attachment; filename="{fname}.csv"'})
    elif req.file_format == "json":
        df["date"] = df["date"].astype(str)
        return StreamingResponse(io.BytesIO(df.to_json(orient="records", indent=2).encode()), media_type="application/json",
                                 headers={"Content-Disposition": f'attachment; filename="{fname}.json"'})
    raise HTTPException(status_code=400, detail="Use csv or json.")


@app.post("/api/preview")
async def preview_historical(req: HistoricalRequest):
    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.get(
            f"{KITE_BASE}/instruments/historical/{req.instrument_token}/{req.interval}",
            params={"from": req.from_date, "to": req.to_date, "continuous": int(req.continuous), "oi": int(req.oi)},
            headers={"X-Kite-Version": "3", "Authorization": f"token {req.api_key}:{req.access_token}"},
        )
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    candles = resp.json()["data"]["candles"]
    if not candles:
        return {"rows": [], "total": 0}
    df = _build_df(candles[:5])
    df["date"] = df["date"].astype(str)
    return {"rows": df.to_dict(orient="records"), "total": len(candles)}


# ── Serve frontend ─────────────────────────────────────────────────────────────
app.mount("/", StaticFiles(directory="static", html=True), name="static")


# ── Token Verification ─────────────────────────────────────────────────────────

@app.get("/api/verify-token")
async def verify_token(api_key: str, access_token: str):
    """
    Ping Zerodha /user/profile to confirm token is valid right now.
    Returns {valid: true, user_name: "..."} or {valid: false, error: "..."}
    Never raises — always returns JSON so frontend can handle gracefully.
    """
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                f"{KITE_BASE}/user/profile",
                headers={
                    "X-Kite-Version": "3",
                    "Authorization": f"token {api_key}:{access_token}",
                },
            )
        if resp.status_code == 200:
            data = resp.json().get("data", {})
            return {
                "valid": True,
                "user_name": data.get("user_name", ""),
                "user_id": data.get("user_id", ""),
                "email": data.get("email", ""),
            }
        else:
            return {"valid": False, "error": "Token expired or invalid — please re-login."}
    except Exception as e:
        return {"valid": False, "error": str(e)}
