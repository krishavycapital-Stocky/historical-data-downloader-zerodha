# Live OI Tracker — integration guide

Adds a `/oi` tab to ZetaPull for live Open-Interest tracking using Kite's
REST `/quote` API. No WebSocket, no background thread — the browser tab drives
everything.

---

## How it works

1. You log in on the main ZetaPull tab as usual. The `generate_token` endpoint
   exchanges your `request_token` for a Kite `access_token` and simultaneously
   writes it to the server-side token store (`oi_tracker/token_store.py`).
2. Open `/oi`. The page reads `api_key` and `access_token` from the browser's
   `localStorage` (keys `zp_k` / `zp_t`) and passes them as query parameters
   on every poll to `/oi/api/snapshot`.
3. The server calls Kite `GET /quote` for each configured instrument token,
   updates in-memory deltas, and returns JSON rows to the browser.
4. The browser re-polls every 180 seconds (configurable via `KITE_OI_INTERVAL`).
   Keeping the tab open also keeps the Render free-tier dyno awake.

If the dyno sleeps and loses its in-memory token, the browser-supplied keys
recover it automatically on the next poll — no re-login needed.

---

## Files

```
oi_tracker/
├── config.py             # instruments to track + timing (edit this)
├── token_store.py        # server-side token cache (read by poller)
├── poller.py             # on-demand REST fetcher called per browser poll
├── routes_fastapi.py     # FastAPI router registered in main.py
└── templates/oi_tab.html # the /oi page
```

---

## Wiring into main.py (already done)

The three relevant changes in `main.py`:

```python
# 1. Import — placed just after app = FastAPI(...)
from oi_tracker.token_store import save_token as _oi_save_token
from oi_tracker.routes_fastapi import router as oi_router
app.include_router(oi_router)   # must be BEFORE app.mount("/", StaticFiles(...))

# 2. Inside generate_token endpoint — saves token server-side on every login
access_token = resp.json()["data"]["access_token"]
_oi_save_token(access_token, req.api_key)
return {"access_token": access_token}
```

No startup event, no background tasks.

---

## Configuring instruments

Edit `oi_tracker/config.py` — the `INSTRUMENTS` list:

```python
INSTRUMENTS = [
    (256265,  "NIFTY 50"),
    (260105,  "BANKNIFTY"),
    (13568258, "NIFTY 25000 CE"),
]
```

Each entry is `(instrument_token_integer, "display label")`. The token is
Zerodha's internal integer ID — find it by searching the instrument CSV at
`https://api.kite.trade/instruments/NFO`.

Alternatively set env var `KITE_OI_TOKENS=256265,260105` on Render (overrides
the list above; labels default to the token number).

---

## Endpoints

```
GET  /oi                              the OI tab (HTML)
GET  /oi/api/snapshot?api_key=&access_token=   table data (polled by browser)
GET  /oi/api/status                   token / config health check
```

---

## Column meanings

- **Δ OI (3m):** change vs the previous poll. Shows `—` until the second poll.
- **Δ OI (day open):** change vs the first OI reading captured today. If the
  tab was opened after 9:20 IST the header says "since HH:MM" to be honest
  about the baseline.

---

## Render free tier notes

- Run with one worker (uvicorn default): `uvicorn main:app --host 0.0.0.0 --port $PORT`
- In-memory deltas reset if the dyno sleeps. The day-open baseline will restart
  from whenever you first open the tab after a sleep, and the header will say
  "since HH:MM" to reflect that.
