"""
oi_tracker
----------
A drop-in live Open-Interest tracking module for an existing Kite Connect app.

It reuses your existing api_key and the access token your historical tab already
stored today. It never logs in and never writes a new token, so it cannot start a
second login or invalidate your historical session.

Quick start (Flask):
    from oi_tracker.routes_flask import oi_bp, init_oi
    app.register_blueprint(oi_bp)
    init_oi()
"""

from .config import INSTRUMENTS, TOKENS, SNAPSHOT_INTERVAL_SEC  # noqa: F401

__all__ = ["INSTRUMENTS", "TOKENS", "SNAPSHOT_INTERVAL_SEC"]
__version__ = "1.0.0"
