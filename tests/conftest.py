"""
Shared pytest fixtures and fakes.

FakeFirebase mimics the Firebase-style interface the memory stores call
(set_document / get_document / get_collection / update_document /
delete_document / query / list_subcollections). Rows are stored in a
nested dict keyed by collection path → doc_id → doc data, so tests can
exercise the full consolidation pipeline without standing up a real
Firestore or Supabase instance.
"""

from __future__ import annotations

import copy
from typing import Any, Dict, List, Optional, Tuple


class FakeFirebase:
    """In-memory Firebase / Supabase-style adapter used in tests."""

    def __init__(self):
        self._collections: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self.calls: List[Tuple[str, tuple, dict]] = []

    def _bucket(self, collection: str) -> Dict[str, Dict[str, Any]]:
        return self._collections.setdefault(collection, {})

    def _record(self, name: str, *args, **kwargs) -> None:
        self.calls.append((name, args, kwargs))

    def set_document(
        self,
        collection: str,
        doc_id: str,
        data: dict,
        merge: bool = False,
    ) -> None:
        self._record("set_document", collection, doc_id, merge=merge)
        bucket = self._bucket(collection)
        if merge and doc_id in bucket:
            bucket[doc_id] = {**bucket[doc_id], **data}
        else:
            bucket[doc_id] = copy.deepcopy(data)

    def get_document(self, collection: str, doc_id: str) -> Optional[dict]:
        self._record("get_document", collection, doc_id)
        return copy.deepcopy(self._bucket(collection).get(doc_id))

    def get_collection(
        self,
        collection: str,
        limit: Optional[int] = None,
        order_by: Optional[str] = None,
        order_direction: Optional[str] = None,
        **_kwargs,
    ) -> list:
        self._record("get_collection", collection, limit=limit)
        rows = [copy.deepcopy(v) for v in self._bucket(collection).values()]
        if order_by:
            rows.sort(
                key=lambda r: r.get(order_by, 0),
                reverse=(order_direction or "").upper().startswith("DESC"),
            )
        if limit:
            rows = rows[:limit]
        return rows

    def update_document(self, collection: str, doc_id: str, data: dict) -> None:
        self._record("update_document", collection, doc_id)
        bucket = self._bucket(collection)
        if doc_id in bucket:
            bucket[doc_id] = {**bucket[doc_id], **data}
        else:
            bucket[doc_id] = copy.deepcopy(data)

    def delete_document(self, collection: str, doc_id: str) -> None:
        self._record("delete_document", collection, doc_id)
        self._bucket(collection).pop(doc_id, None)

    def query(
        self,
        collection: str,
        filters: Optional[list] = None,
        limit: Optional[int] = None,
        order_by: Optional[str] = None,
        order_direction: Optional[str] = None,
        **_kwargs,
    ) -> list:
        self._record("query", collection, filters=filters, limit=limit)
        rows = [copy.deepcopy(v) for v in self._bucket(collection).values()]
        for f in filters or []:
            field, op, value = f
            rows = [r for r in rows if _matches(r.get(field), op, value)]
        if order_by:
            rows.sort(
                key=lambda r: r.get(order_by, 0),
                reverse=(order_direction or "").upper().startswith("DESC"),
            )
        if limit:
            rows = rows[:limit]
        return rows

    def list_subcollections(self, doc_path: str) -> list:
        self._record("list_subcollections", doc_path)
        prefix = doc_path.rstrip("/") + "/"
        subs = set()
        for path in self._collections:
            if path.startswith(prefix):
                rest = path[len(prefix):]
                head = rest.split("/")[0]
                if head:
                    subs.add(head)
        return sorted(subs)


def _matches(value: Any, op: str, target: Any) -> bool:
    if op == "==":
        return value == target
    if op == "!=":
        return value != target
    if op == "<":
        return value is not None and value < target
    if op == "<=":
        return value is not None and value <= target
    if op == ">":
        return value is not None and value > target
    if op == ">=":
        return value is not None and value >= target
    if op == "in":
        return value in (target or [])
    return True
