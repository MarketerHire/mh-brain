"""
Supabase Storage Adapter

Drop-in replacement for FirebaseClient that memory stores use.
Same method signatures: set_document, get_document, get_collection,
update_document, delete_document, query, list_subcollections.
Backed by Supabase Postgres.

The memory stores (episodic, semantic, procedural, working) call these
methods using Firebase-style collection paths. This adapter translates
those paths to Supabase table names and extracts embedded filters
(tenant_id, skill_name, domain) from the path segments.

Usage:
    from lib.supabase_storage import SupabaseStorage
    storage = SupabaseStorage()

    # Works identically to FirebaseClient for memory stores:
    storage.set_document("system/intelligence/episodic/acme/lifecycle-audit", "ep-1", {...})
    storage.get_collection("system/intelligence/semantic/content/patterns")
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_TABLE_MAP = {
    "system/intelligence/episodic": "episodic_memory",
    "system/intelligence/archive": "episodic_memory",
    "system/intelligence/semantic": "semantic_patterns",
    "system/intelligence/procedural": "procedural_knowledge",
    "system/intelligence/working_memory/predictions": "working_predictions",
    "system/intelligence/working_predictions": "working_predictions",
    "system/intelligence/pending_outcomes": "pending_outcomes",
    "system/intelligence/shadow_state": "shadow_state",
    "system/intelligence/shadow_history": "shadow_history",
    "system/intelligence/accuracy_reports": "accuracy_reports",
    "system/intelligence/error_history": "error_history",
    "system/intelligence/channel_timing": "channel_timing",
    "system/intelligence/gold_standards": "gold_standards",
    "system/intelligence/benchmark_results": "benchmark_results",
}

_PK_MAP = {
    "episodic_memory": "episode_id",
    "semantic_patterns": "pattern_id",
    "procedural_knowledge": "knowledge_id",
    "working_predictions": "prediction_id",
    "pending_outcomes": "prediction_id",
}


class SupabaseStorage:
    """Firebase-compatible storage adapter backed by Supabase Postgres."""

    def __init__(self):
        from lib.supabase_client import get_supabase
        self._db = get_supabase()

    # ── Path resolution ────────────────────────────────────────────

    @staticmethod
    def _resolve_table(collection_path: str) -> str:
        path = collection_path.rstrip("/")
        for prefix in sorted(_TABLE_MAP, key=len, reverse=True):
            if path == prefix or path.startswith(prefix + "/"):
                return _TABLE_MAP[prefix]
        raise ValueError(f"No Supabase table mapped for: {collection_path}")

    @staticmethod
    def _get_pk(table: str) -> str:
        return _PK_MAP.get(table, "id")

    @staticmethod
    def _extract_filters(collection_path: str) -> Dict[str, str]:
        """Extract tenant_id / skill_name / domain / client_id from Firebase-style paths.

        Episodic:         system/intelligence/episodic/{tenant}/{skill}
        Archive:          system/intelligence/archive/{tenant}/{skill}
        Semantic:         system/intelligence/semantic/{domain}/patterns
        Pending outcomes: system/intelligence/pending_outcomes/{client_id}
        """
        path = collection_path.rstrip("/")
        filters: Dict[str, str] = {}

        for prefix in ("system/intelligence/episodic/", "system/intelligence/archive/"):
            if path.startswith(prefix):
                parts = path[len(prefix):].split("/")
                if len(parts) >= 1 and parts[0]:
                    filters["tenant_id"] = parts[0]
                if len(parts) >= 2 and parts[1]:
                    filters["skill_name"] = parts[1]
                return filters

        if path.startswith("system/intelligence/semantic/"):
            parts = path[len("system/intelligence/semantic/"):].split("/")
            if len(parts) >= 1 and parts[0]:
                filters["domain"] = parts[0]
            return filters

        if path.startswith("system/intelligence/pending_outcomes/"):
            parts = path[len("system/intelligence/pending_outcomes/"):].split("/")
            if len(parts) >= 1 and parts[0]:
                filters["client_id"] = parts[0]
            return filters

        return filters

    @staticmethod
    def _is_generic_jsonb_table(table: str) -> bool:
        return table in (
            "shadow_state", "shadow_history", "accuracy_reports",
            "error_history", "channel_timing", "gold_standards",
            "benchmark_results",
        )

    # ── CRUD operations ────────────────────────────────────────────

    def set_document(
        self,
        collection: str,
        doc_id: str,
        data: dict,
        merge: bool = False,
    ) -> None:
        table = self._resolve_table(collection)
        pk = self._get_pk(table)

        if self._is_generic_jsonb_table(table):
            row = {"id": doc_id, "data": data, "updated_at": _now_iso()}
        else:
            row = {**data, pk: doc_id, "updated_at": _now_iso()}
            path_filters = self._extract_filters(collection)
            for k, v in path_filters.items():
                row.setdefault(k, v)

        self._db.table(table).upsert(row, on_conflict=pk).execute()

    def get_document(self, collection: str, doc_id: str) -> Optional[dict]:
        table = self._resolve_table(collection)
        pk = self._get_pk(table)
        result = (
            self._db.table(table)
            .select("*")
            .eq(pk, doc_id)
            .limit(1)
            .execute()
        )
        if not result.data:
            return None
        row = result.data[0]
        if self._is_generic_jsonb_table(table):
            return row.get("data", row)
        return row

    def get_collection(
        self,
        collection: str,
        limit: Optional[int] = None,
        order_by: Optional[str] = None,
        order_direction: Optional[str] = None,
        **_kwargs,
    ) -> list:
        table = self._resolve_table(collection)
        filters = self._extract_filters(collection)

        q = self._db.table(table).select("*")
        for k, v in filters.items():
            q = q.eq(k, v)

        if order_by:
            desc = (order_direction or "").upper().startswith("DESC")
            q = q.order(order_by, desc=desc)
        if limit:
            q = q.limit(limit)

        result = q.execute()
        rows = result.data or []

        if self._is_generic_jsonb_table(table):
            return [r.get("data", r) for r in rows]
        return rows

    def update_document(self, collection: str, doc_id: str, data: dict) -> None:
        table = self._resolve_table(collection)
        pk = self._get_pk(table)

        payload = {**data, "updated_at": _now_iso()}
        if self._is_generic_jsonb_table(table):
            existing = self.get_document(collection, doc_id)
            if existing and isinstance(existing, dict):
                existing.update(data)
                payload = {"data": existing, "updated_at": _now_iso()}
            else:
                payload = {"data": data, "updated_at": _now_iso()}

        self._db.table(table).update(payload).eq(pk, doc_id).execute()

    def delete_document(self, collection: str, doc_id: str) -> None:
        table = self._resolve_table(collection)
        pk = self._get_pk(table)
        self._db.table(table).delete().eq(pk, doc_id).execute()

    def query(
        self,
        collection: str,
        filters: Optional[list] = None,
        limit: Optional[int] = None,
        order_by: Optional[str] = None,
        order_direction: Optional[str] = None,
        **_kwargs,
    ) -> list:
        table = self._resolve_table(collection)
        q = self._db.table(table).select("*")

        path_filters = self._extract_filters(collection)
        for k, v in path_filters.items():
            q = q.eq(k, v)

        if filters:
            for f in filters:
                field, op, value = f
                q = _apply_filter(q, field, op, value)

        if order_by:
            desc = (order_direction or "").upper().startswith("DESC")
            q = q.order(order_by, desc=desc)
        if limit:
            q = q.limit(limit)

        result = q.execute()
        rows = result.data or []

        if self._is_generic_jsonb_table(table):
            return [r.get("data", r) for r in rows]
        return rows

    def list_subcollections(self, doc_path: str) -> list:
        """Firebase subcollection enumeration → DISTINCT query.

        system/intelligence/episodic           → DISTINCT tenant_id
        system/intelligence/episodic/{tenant}  → DISTINCT skill_name WHERE tenant_id
        """
        path = doc_path.rstrip("/")

        if path == "system/intelligence/episodic":
            result = (
                self._db.table("episodic_memory")
                .select("tenant_id")
                .is_("archived_at", "null")
                .execute()
            )
            return list({r["tenant_id"] for r in (result.data or [])})

        if path.startswith("system/intelligence/episodic/"):
            tenant_id = path[len("system/intelligence/episodic/"):].split("/")[0]
            result = (
                self._db.table("episodic_memory")
                .select("skill_name")
                .eq("tenant_id", tenant_id)
                .is_("archived_at", "null")
                .execute()
            )
            return list({r["skill_name"] for r in (result.data or [])})

        return []


# ── Helpers ────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _apply_filter(q, field: str, op: str, value):
    if op == "==":
        return q.eq(field, value)
    if op == "!=":
        return q.neq(field, value)
    if op == "<":
        return q.lt(field, value)
    if op == "<=":
        return q.lte(field, value)
    if op == ">":
        return q.gt(field, value)
    if op == ">=":
        return q.gte(field, value)
    if op == "in":
        return q.in_(field, value)
    logger.warning("Unsupported filter operator %s, skipping", op)
    return q
