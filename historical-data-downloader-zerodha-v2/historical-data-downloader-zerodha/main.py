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
import pyarrow as pa
import pyarrow.parquet as pq

app = FastAPI(title="Historical Data Downloader - Zerodha")

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

class AccessTokenRequest(BaseModel):
    api_key: str
    access_token: str

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


# ── Instruments ───────────────────────────────────────────────────────────────

@app.get("/api/instruments")
async def get_instruments(api_key: str, access_token: str, exchange: str = "NFO"):
    """Fetch instrument list from Kite for an exchange."""
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(
            f"{KITE_BASE}/instruments/{exchange}",
            headers={
                "X-Kite-Version": "3",
                "Authorization": f"token {api_key}:{access_token}",
            },
        )
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    # parse CSV response
    df = pd.read_csv(io.StringIO(resp.text))
    return df.to_dict(orient="records")


# ── Historical Data ───────────────────────────────────────────────────────────

class HistoricalRequest(BaseModel):
    api_key: str
    access_token: str
    instrument_token: str
    from_date: str          # YYYY-MM-DD
    to_date: str            # YYYY-MM-DD
    interval: str           # minute, 5minute, 15minute, 30minute, 60minute, day
    continuous: bool = False
    oi: bool = True
    file_format: str = "csv"   # csv | parquet | json

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
    fname_base = f"{req.instrument_token}_{req.interval}_{req.from_date}_{req.to_date}"

    if fmt == "csv":
        buf = io.StringIO()
        df.to_csv(buf, index=False)
        buf.seek(0)
        return StreamingResponse(
            io.BytesIO(buf.getvalue().encode()),
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{fname_base}.csv"'},
        )
    elif fmt == "parquet":
        buf = io.BytesIO()
        table = pa.Table.from_pandas(df)
        pq.write_table(table, buf)
        buf.seek(0)
        return StreamingResponse(
            buf,
            media_type="application/octet-stream",
            headers={"Content-Disposition": f'attachment; filename="{fname_base}.parquet"'},
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
        raise HTTPException(status_code=400, detail="Invalid file format.")


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
