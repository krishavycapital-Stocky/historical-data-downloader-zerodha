"""
oi_tracker / token_store.py
---------------------------
SERVER-SIDE token store. Your existing app keeps the daily Kite access token in
the BROWSER (it is passed to the backend on every request). A backend WebSocket
cannot reach a browser-only token, so this module gives the server its own copy.

How it's filled (NO second login — it reuses the token you already generated):
  * The OI tab page reads the token your browser already holds and POSTs it to
    /oi/api/set-token, which calls save_token() here.
  * save_token() keeps it in memory AND writes it to a small runtime JSON file,
    so the WebSocket can reuse it across requests and across in-process restarts.

Read order for load_access_token():
    1. In-memory value primed this session
    2. Runtime file (oi_runtime/oi_session.json)
    3. ENV var KITE_ACCESS_TOKEN  (optional override for local testing)
    4. Legacy *.txt / *.json token files (kept for flexibility)

This module NEVER calls generate_session()/login. It only reads/saves a token
that already exists, so it cannot start a second login or invalidate your
historical session.
"""

import json
import os
import threading
import time
import logging

log = logging.getLogger("oi_tracker.token_store")

_LOCK = threading.Lock()
_MEM = {"api_key": None, "access_token": None, "saved_at": None}

# Runtime file lives next to the app, in a folder you can .gitignore.
_RUNTIME_DIR = os.environ.get("KITE_OI_RUNTIME_DIR", "oi_runtime")
_RUNTIME_FILE = os.path.join(_RUNTIME_DIR, "oi_session.json")

# Legacy fallbacks (kept from v1 — harmless if absent)
_CANDIDATE_TEXT_FILES = ["access_token.txt", "token.txt", "kite_access_token.txt"]
_CANDIDATE_JSON_FILES = ["token.json", "access_token.json", "session.json"]
_JSON_KEYS = ["access_token", "accessToken", "token"]


# --------------------------------------------------------------------------- #
# Save (priming from the logged-in browser session)
# --------------------------------------------------------------------------- #
def save_token(access_token, api_key=None):
    """Store the EXISTING token server-side (memory + runtime file)."""
    access_token = (access_token or "").strip()
    if not access_token:
        return False
    with _LOCK:
        _MEM["access_token"] = access_token
        if api_key:
            _MEM["api_key"] = api_key.strip()
        _MEM["saved_at"] = time.time()
        try:
            os.makedirs(_RUNTIME_DIR, exist_ok=True)
            with open(_RUNTIME_FILE, "w") as f:
                json.dump(_MEM, f)
        except Exception as e:  # noqa
            log.warning("Could not persist token to %s: %s", _RUNTIME_FILE, e)
    log.info("Access token saved server-side (len=%d).", len(access_token))
    return True


def _load_runtime_file():
    try:
        with open(_RUNTIME_FILE, "r") as f:
            data = json.load(f)
        if isinstance(data, dict) and data.get("access_token"):
            return data
    except FileNotFoundError:
        return None
    except Exception as e:  # noqa
        log.warning("Could not read %s: %s", _RUNTIME_FILE, e)
    return None


# --------------------------------------------------------------------------- #
# Load
# --------------------------------------------------------------------------- #
def load_access_token(search_dirs=None):
    """Return the stored access token (server-side), or None. Read-only."""
    with _LOCK:
        if _MEM["access_token"]:
            return _MEM["access_token"]

    rt = _load_runtime_file()
    if rt:
        with _LOCK:
            _MEM.update({k: rt.get(k) for k in ("api_key", "access_token", "saved_at")})
        return rt["access_token"]

    env_tok = os.environ.get("KITE_ACCESS_TOKEN", "").strip()
    if env_tok:
        return env_tok

    if search_dirs is None:
        here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        search_dirs = [here, os.getcwd()]
    for d in search_dirs:
        for name in _CANDIDATE_TEXT_FILES:
            p = os.path.join(d, name)
            if os.path.exists(p):
                try:
                    val = open(p).read().strip()
                    if val:
                        return val
                except Exception:  # noqa
                    pass
        for name in _CANDIDATE_JSON_FILES:
            p = os.path.join(d, name)
            if os.path.exists(p):
                try:
                    data = json.load(open(p))
                    if isinstance(data, dict):
                        for k in _JSON_KEYS:
                            if data.get(k):
                                return str(data[k]).strip()
                except Exception:  # noqa
                    pass

    log.error("No stored access token yet. The OI tab will prompt to reuse your "
              "logged-in session. (The tracker never logs in on its own.)")
    return None


def load_client_id():
    """Dhan client id from env (set once on Render)."""
    import os
    return os.environ.get("DHAN_CLIENT_ID", "").strip() or None


def load_api_key():
    """Return the api_key: stored (from the browser session) or env override."""
    with _LOCK:
        if _MEM["api_key"]:
            return _MEM["api_key"]
    rt = _load_runtime_file()
    if rt and rt.get("api_key"):
        return rt["api_key"]
    return os.environ.get("KITE_API_KEY", "").strip() or None


def get_meta():
    # NOTE: read mem fields under the lock, then call helpers OUTSIDE it.
    # load_api_key()/_load_runtime_file() acquire the same (non-reentrant) lock,
    # so calling them while holding it would deadlock.
    with _LOCK:
        mem_token = _MEM["access_token"]
        saved_at = _MEM["saved_at"]
    has_token = bool(mem_token) or bool(_load_runtime_file())
    has_api_key = bool(load_api_key())
    return {"has_token": has_token, "has_api_key": has_api_key, "saved_at": saved_at}
