"""
MH1HQ Event Emitter — writes execution events to Supabase shared DB

Copy this file to mh1-hq/lib/execution/event_emitter.py.

Emits events to the Supabase `events` table after skill/plan execution.
BrightMatter's worker cron picks these up and processes them through
the learning pipeline.

Usage in engine.py (after node completion):
    from lib.execution.event_emitter import emit_skill_completed
    emit_skill_completed(
        skill_name="lifecycle-audit",
        client_id="soko-glam",
        result=node_result,
        metrics={"contacts_processed": 5000},
        context={"module_id": "mod-123", "node_id": "node-abc"},
    )

Usage in cloud_engine.py (after plan completion):
    from lib.execution.event_emitter import emit_plan_completed
    emit_plan_completed(
        module_id="lifecycle-audit-20260322",
        client_id="soko-glam",
        execution_data={"skill_plan": [...], "checkpoints": [...]},
    )

Env vars:
    SUPABASE_URL              — Supabase project URL
    SUPABASE_KEY              — Supabase service role key
    SUPABASE_SERVICE_ROLE_KEY — Alias for SUPABASE_KEY
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

_supabase = None


def _get_supabase():
    """Lazy-load Supabase client for MH-OS shared event bus.

    Prefers MHOS_SUPABASE_URL/KEY (the shared project where events live),
    falls back to SUPABASE_URL/KEY for backwards compatibility.
    """
    global _supabase
    if _supabase is not None:
        return _supabase

    try:
        from supabase import create_client
    except ImportError:
        logger.warning("supabase package not installed — event emission disabled")
        return None

    url = os.environ.get("MHOS_SUPABASE_URL") or os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("MHOS_SUPABASE_KEY") or os.environ.get(
        "SUPABASE_KEY", os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
    )
    if not url or not key:
        logger.debug("Supabase credentials not set — event emission disabled")
        return None

    _supabase = create_client(url, key)
    return _supabase


def emit_event(
    source: str,
    event_type: str,
    skill_name: str,
    client_id: str,
    result: Optional[Dict[str, Any]] = None,
    metrics: Optional[Dict[str, Any]] = None,
    context: Optional[Dict[str, Any]] = None,
    domain: str = "generic",
    channel_context: Optional[Dict[str, Any]] = None,
) -> bool:
    """Write an event to the Supabase shared events table.

    Returns True if the event was written, False on any failure
    (graceful — never raises, never blocks execution).
    """
    sb = _get_supabase()
    if sb is None:
        return False

    try:
        sb.table("events").insert({
            "source": source,
            "event_type": event_type,
            "skill_name": skill_name,
            "client_id": client_id,
            "domain": domain,
            "result": result or {},
            "metrics": metrics or {},
            "context": context or {},
            "channel_context": channel_context,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "processed_by_bm": False,
        }).execute()
        return True
    except Exception as e:
        logger.warning(f"Failed to emit event {event_type}/{skill_name}: {e}")
        return False


def emit_skill_completed(
    skill_name: str,
    client_id: str,
    result: Optional[Dict[str, Any]] = None,
    metrics: Optional[Dict[str, Any]] = None,
    context: Optional[Dict[str, Any]] = None,
    domain: str = "generic",
    delivery_metadata: Optional[Dict[str, Any]] = None,
) -> bool:
    """Emit a skill_completed event after node execution."""
    ctx = dict(context or {})
    if delivery_metadata:
        ctx["delivery_metadata"] = delivery_metadata
    return emit_event(
        source="mh1-hq",
        event_type="skill_completed",
        skill_name=skill_name,
        client_id=client_id,
        result=result,
        metrics=metrics,
        context=ctx,
        domain=domain,
    )


def emit_plan_completed(
    module_id: str,
    client_id: str,
    execution_data: Optional[Dict[str, Any]] = None,
    domain: str = "generic",
) -> bool:
    """Emit a plan_completed event after module execution."""
    return emit_event(
        source="mh1-hq",
        event_type="plan_completed",
        skill_name=module_id,
        client_id=client_id,
        result=execution_data,
        domain=domain,
    )


def emit_feedback(
    prediction_id: str,
    client_id: str,
    rating: float,
    correction: Optional[str] = None,
) -> bool:
    """Emit a human_feedback event."""
    return emit_event(
        source="mh1-hq",
        event_type="human_feedback",
        skill_name="",
        client_id=client_id,
        result={
            "prediction_id": prediction_id,
            "rating": rating,
            "correction": correction,
        },
    )
