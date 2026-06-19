"""
oi_tracker / instruments.py
---------------------------
Optional helper. KiteTicker needs numeric instrument_tokens, but you think in
symbols like "NIFTY25JUN25000CE". This turns symbols into tokens using Kite's
public instrument dump. It uses a read-only REST call (kite.instruments()),
which does NOT affect your session or trigger a login — it just needs the same
stored token.

Run this ONCE (e.g. locally, or via a one-off route) to discover the tokens you
want, then paste them into config.INSTRUMENTS or the KITE_OI_TOKENS env var.
You do not need to run it on every boot.
"""

import logging
from kiteconnect import KiteConnect

from . import config
from .token_store import load_access_token, load_api_key

log = logging.getLogger("oi_tracker.instruments")


def _kite():
    token = load_access_token()
    api_key = load_api_key()
    if not token or not api_key:
        raise RuntimeError("API key or stored access token missing. Open the OI tab once to prime the session.")
    k = KiteConnect(api_key=api_key)
    k.set_access_token(token)   # read-only: attaches the EXISTING token, no login
    return k


def resolve_tokens(symbols, exchange="NFO"):
    """
    symbols: list of tradingsymbols, e.g. ["NIFTY25JUN25000CE", "NIFTY25JUN25000PE"]
    Returns: list of (instrument_token, tradingsymbol) tuples found.
    """
    k = _kite()
    dump = k.instruments(exchange)   # list of dicts
    wanted = {s.upper() for s in symbols}
    out = []
    for inst in dump:
        if inst["tradingsymbol"].upper() in wanted:
            out.append((inst["instrument_token"], inst["tradingsymbol"]))
    found = {ts for _, ts in out}
    missing = wanted - {s.upper() for s in found}
    if missing:
        log.warning("Symbols not found in %s: %s", exchange, ", ".join(sorted(missing)))
    return out


def option_chain_tokens(name, expiry, strikes, exchange="NFO"):
    """
    Convenience: get CE+PE tokens for a list of strikes of one underlying/expiry.
    name:   e.g. "NIFTY"
    expiry: a datetime.date matching the contract expiry
    strikes: iterable of strike prices, e.g. range(24800, 25300, 100)
    Returns list of (instrument_token, tradingsymbol).
    """
    k = _kite()
    dump = k.instruments(exchange)
    strikes = set(float(s) for s in strikes)
    out = []
    for inst in dump:
        if (inst.get("name") == name
                and inst.get("expiry") == expiry
                and float(inst.get("strike", 0)) in strikes
                and inst.get("instrument_type") in ("CE", "PE")):
            out.append((inst["instrument_token"], inst["tradingsymbol"]))
    return out


if __name__ == "__main__":
    # Quick manual use: edit and run `python -m oi_tracker.instruments`
    import sys
    syms = sys.argv[1:]
    if not syms:
        print("Usage: python -m oi_tracker.instruments SYMBOL1 SYMBOL2 ...")
    else:
        for tok, ts in resolve_tokens(syms):
            print(f"{tok}\t{ts}")
