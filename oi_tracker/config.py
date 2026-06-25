"""
oi_tracker / config.py
----------------------
One place for all knobs. Nothing here ever logs in or talks to Zerodha —
it only reads values you already have. Edit the INSTRUMENTS list (or set the
KITE_OI_TOKENS env var) and you're done.
"""

import os

# ---------------------------------------------------------------------------
# 1. API KEY  (usually you can ignore this)
#    Your app passes api_key + access_token from the browser, so the OI tab
#    receives the api_key when it primes the session — you normally don't need
#    to set anything here. KITE_API_KEY is only an optional fallback/override
#    (e.g. for local testing). Leave it unset in production.
# ---------------------------------------------------------------------------
API_KEY = os.environ.get("KITE_API_KEY", "").strip()

# ---------------------------------------------------------------------------
# 2. INSTRUMENTS TO TRACK
#    A "defined list of instrument tokens" as you asked. Two ways to set them:
#
#    (a) Env var (recommended on Render — no code edit, no redeploy of logic):
#        KITE_OI_TOKENS="13568258,13568514,256265"
#
#    (b) Or hardcode below. Each entry is (instrument_token, friendly_label).
#        The label is only for display — call it whatever helps you read the
#        table fast (e.g. "NIFTY 25000 CE").
#
#    instrument_token is the NUMBER Zerodha uses internally, NOT the symbol.
#    Find it in the CSV at https://api.kite.trade/instruments/NFO.
# ---------------------------------------------------------------------------
INSTRUMENTS = [
    (256265, "NIFTY 50"),
    (260105, "BANKNIFTY"),
    (13568258, "NIFTY 25000 CE"),
    (13568514, "NIFTY 25000 PE"),
]

# If KITE_OI_TOKENS is set, it overrides the list above (labels become the token).
_env_tokens = os.environ.get("DHAN_OI_IDS", "").strip()
if _env_tokens:
    INSTRUMENTS = []
    for part in _env_tokens.split(","):
        part = part.strip()
        if not part:
            continue
        if ":" in part:
            sid, label = part.split(":", 1)
            sid, label = sid.strip(), label.strip()
        else:
            sid = label = part
        INSTRUMENTS.append((int(sid), label))

# Convenience views used elsewhere
TOKENS = [tok for tok, _ in INSTRUMENTS]
LABELS = {tok: label for tok, label in INSTRUMENTS}

# ---------------------------------------------------------------------------
# 3. SNAPSHOT TIMING
#    Zerodha publishes OI roughly every 3 minutes, so 180s is the natural cadence.
#    We also align snapshots to the wall clock (…:00, :03, :06…) so your rows
#    line up with the exchange's own 3-min OI updates.
# ---------------------------------------------------------------------------
SNAPSHOT_INTERVAL_SEC = int(os.environ.get("KITE_OI_INTERVAL", "180"))
ALIGN_TO_CLOCK = True

# How many snapshots to keep in memory per instrument (1 trading day ~ 125 at 3min).
MAX_SNAPSHOTS = int(os.environ.get("KITE_OI_HISTORY", "200"))

# Noise filter: ignore OI/price moves smaller than these when classifying buildup.
OI_EPSILON = int(os.environ.get("KITE_OI_EPSILON", "0"))       # contracts/shares
PRICE_EPSILON = float(os.environ.get("KITE_OI_PRICE_EPS", "0.0"))  # rupees

# Optional: append every snapshot to a daily CSV for audit/debugging.
# Set to "" to disable. File is written next to the app.
CSV_LOG_DIR = os.environ.get("KITE_OI_CSV_DIR", "oi_snapshots")
