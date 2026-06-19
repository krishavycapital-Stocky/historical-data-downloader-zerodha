# Live OI Tracker — drop-in module for your FastAPI app (ZetaPull)

Adds a new **`/oi` tab** to your existing app
(`historical-data-downloader-zerodha.onrender.com`) for live Open-Interest
tracking. Detected from your `main.py` + `requirements.txt`:

- **Framework: FastAPI** (uvicorn). ✔ wiring below is FastAPI.
- **Token storage today: none on the server.** Your app generates the token in
  the browser and passes `api_key`/`access_token` on every request. A backend
  WebSocket can't reach a browser-only token — so this module adds a small
  **server-side token store** that your already-logged-in browser primes. No
  second login: it reuses the exact token you generated this morning.

It never calls login/`generate_session`, so it cannot start a second login or
invalidate your historical session. (All of this is tested.)

---

## Files
```
oi_tracker/
├── config.py             # what to track + timing (the only file you usually edit)
├── token_store.py        # SERVER-SIDE token store (primed by your browser session)
├── ticker_worker.py      # one KiteTicker WebSocket (FULL mode) -> latest OI in memory
├── snapshotter.py        # 3-min snapshots, deltas vs prev + day-open, buildup
├── instruments.py        # optional: symbol -> instrument_token helper
├── routes_fastapi.py     # the wiring you paste into main.py
└── templates/oi_tab.html # the new tab (auto-reuses your logged-in session)
```

---

## Install — 4 steps

### 1. Add the folder + one dependency
Copy `oi_tracker/` next to your `main.py`. Then add **one line** to
`requirements.txt` (your app doesn't currently include it — it talks to Kite
over httpx, but the live WebSocket needs the official client):
```
kiteconnect==5.2.0
```

### 2. Choose instruments
Easiest: set a Render environment variable (no code edit):
```
KITE_OI_TOKENS = 256265,260105,13568258,13568514
```
`256265`=NIFTY 50, `260105`=BANKNIFTY (these spot tokens are already in your
main.py). For option tokens you already have an endpoint — your app's
`/api/options/token` returns the `instrument_token` for a strike. Grab the ones
you want and add them to the list.

### 3. Wire it into main.py — **placement matters**
Your file ends with:
```python
app.mount("/", StaticFiles(directory="static", html=True), name="static")
```
That mount is greedy and shadows anything added **after** it. So paste these
lines **just ABOVE that line** (right under the `# ── Serve frontend ──` comment):
```python
from oi_tracker.routes_fastapi import router as oi_router, init_oi
app.include_router(oi_router)

@app.on_event("startup")
async def _start_oi():
    init_oi()
```
(Your app already uses `@app.on_event("startup")`, so this pattern fits.)

### 4. Add a link to the tab (optional)
In your `static` frontend add `<a href="/oi">Live OI</a>`, or just open
`/oi` directly once you're logged in.

That's it. Open `/oi` while logged in: the page hands the server the token your
browser already holds, the WebSocket connects, and the table fills in.

---

## How the session priming works (no second login)

1. You log in as usual on your main app → browser stores today's token (as it
   already does).
2. You open `/oi`. The page first asks the server "do you already have a token?"
   If yes (primed earlier today / persisted), it just shows data.
3. If not, the page reads the token from your browser's storage (same domain, so
   it's visible) and POSTs it to `/oi/api/set-token`. The server saves it in
   memory **and** to a small file (`oi_runtime/oi_session.json`) so the WebSocket
   can reuse it across requests and restarts.
4. If the page can't auto-detect the token (unusual key name), it shows a small
   one-time panel pre-filled with whatever it found — confirm and click once.
   You're pasting the token you already have, not logging in again.

Each new trading day, you log in on the main app as usual; the first time you
open `/oi` that day it re-primes with the fresh token and the ticker restarts
automatically.

> Add `oi_runtime/` to `.gitignore` — it holds a runtime token copy and should
> never be committed.

---

## Two Render settings that matter

1. **Single worker.** The live OI lives in memory in one process. Run uvicorn
   with one worker (this is uvicorn's default, so you're likely fine already):
   ```
   uvicorn main:app --host 0.0.0.0 --port $PORT --workers 1
   ```
2. **Stay awake during market hours.** On Render's free tier the service sleeps
   when idle and the WebSocket dies. For unattended running through the session,
   use a paid always-on instance. While you're watching `/oi` in a browser, the
   polling keeps it awake.

---

## What the numbers mean
- **Δ Price / Δ OI (3m):** change vs the previous 3-minute snapshot.
- **Δ OI (day):** change vs the day-open baseline = first OI seen today. Kite's
  feed has current OI but no explicit "OI at 9:15", so first-observed is the
  standard baseline. Start near the open for a true day-open.
- **Buildup:** price↑ OI↑ = Long Buildup · price↓ OI↑ = Short Buildup ·
  price↑ OI↓ = Short Covering · price↓ OI↓ = Long Unwinding.

Every snapshot also appends to `oi_snapshots/oi_YYYY-MM-DD.csv` for audit
(set `KITE_OI_CSV_DIR=""` to disable).

---

## Endpoints added
```
GET  /oi                 the new tab
POST /oi/api/set-token   browser hands its existing token to the server
GET  /oi/api/snapshot    table data (page polls this every 15s)
GET  /oi/api/status      health / connection info
```

## Redistribution note
Personal, single-user view. Kite Connect terms don't allow redistributing the
live feed to other users / embedding in another product — keep that as a
separate licensing question.
