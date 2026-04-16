"""
BrightMatter Supabase Client — Dual-Project

Two Supabase connections:

    get_supabase()       — mh-brain dedicated project (read/write for all
                           mh-brain tables: episodic_memory, semantic_patterns,
                           guidance_cache, client_platform_data, etc.)

    get_mhos_supabase()  — MH-OS shared project (read-only for the shared
                           event bus: events, signals, transcripts)

Env vars (mh-brain — primary):
    SUPABASE_URL              — mh-brain Supabase project URL
    SUPABASE_KEY              — mh-brain service role key
    SUPABASE_SERVICE_ROLE_KEY — alias for SUPABASE_KEY

Env vars (MH-OS — shared event bus):
    MHOS_SUPABASE_URL         — MH-OS Supabase project URL
    MHOS_SUPABASE_KEY         — MH-OS service role key (read-only preferred)
"""

from __future__ import annotations

import logging
import os
import threading
from typing import Optional

logger = logging.getLogger(__name__)

_client = None
_lock = threading.Lock()

_mhos_client = None
_mhos_lock = threading.Lock()


def get_supabase():
    """Get or create the singleton mh-brain Supabase client.

    Raises ImportError if the ``supabase`` package is not installed.
    Raises ValueError if required env vars are missing.
    """
    global _client

    if _client is not None:
        return _client

    with _lock:
        if _client is not None:
            return _client

        try:
            from supabase import create_client
        except ImportError:
            raise ImportError(
                "supabase package required. Install with: pip install supabase"
            )

        url = os.environ.get("SUPABASE_URL", "")
        key = os.environ.get(
            "SUPABASE_KEY",
            os.environ.get("SUPABASE_SERVICE_ROLE_KEY", ""),
        )

        if not url or not key:
            raise ValueError(
                "SUPABASE_URL and SUPABASE_KEY (or SUPABASE_SERVICE_ROLE_KEY) "
                "must be set as environment variables."
            )

        _client = create_client(url, key)
        logger.info("Supabase client initialized (mh-brain)")
        return _client


def get_supabase_or_none() -> Optional[object]:
    """Get the mh-brain Supabase client, returning None on any failure."""
    try:
        return get_supabase()
    except (ImportError, ValueError) as e:
        logger.debug(f"Supabase unavailable: {e}")
        return None


def get_mhos_supabase():
    """Get or create the singleton MH-OS Supabase client.

    Used for reading from the shared event bus (events, signals, transcripts).
    Falls back to the primary client if MHOS env vars aren't set, so the
    system works unchanged during the transition period.
    """
    global _mhos_client

    if _mhos_client is not None:
        return _mhos_client

    with _mhos_lock:
        if _mhos_client is not None:
            return _mhos_client

        url = os.environ.get("MHOS_SUPABASE_URL", "")
        key = os.environ.get("MHOS_SUPABASE_KEY", "")

        if not url or not key:
            logger.info("MHOS_SUPABASE_URL/KEY not set — falling back to primary client")
            _mhos_client = get_supabase()
            return _mhos_client

        try:
            from supabase import create_client
        except ImportError:
            raise ImportError(
                "supabase package required. Install with: pip install supabase"
            )

        _mhos_client = create_client(url, key)
        logger.info("MH-OS Supabase client initialized (shared event bus)")
        return _mhos_client


def get_mhos_supabase_or_none() -> Optional[object]:
    """Get the MH-OS Supabase client, returning None on any failure."""
    try:
        return get_mhos_supabase()
    except (ImportError, ValueError) as e:
        logger.debug(f"MH-OS Supabase unavailable: {e}")
        return None
