"""FMT Data Sync — reads fmt_analytics views via MCP, writes to our BigQuery.

Flow per view:
  1. Discover columns via MCP bq_describe_table
  2. Build PII-safe SQL (pii.py)
  3. Query via MCP bq_query with LIMIT/OFFSET pagination
  4. Write rows to our BQ via firebase-adminsdk (bq_writer.py)
  5. Track watermark in Supabase
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .bq_writer import BQWriter
from .client import MCPClient
from .pii import AGGREGATE_TABLES, build_sync_query

logger = logging.getLogger(__name__)

_BATCH_SIZE = 10_000
_INTER_PAGE_DELAY = 0.5


class FMTDataSync:
    """Orchestrates daily sync of FMT application data into our BQ."""

    TARGET_DATASET = "fastmold_app"

    ALL_VIEWS = [
        "booking_requests",
        "inspections",
        "reports",
        "report_rooms",
        "report_samples",
        "report_data",
        "qb_quotes",
        "leads",
        "interactions",
        "campaigns",
        "creatives",
        "campaign_daily_spend",
        "ad_lead_ratings",
        "ad_slot_assignments",
        "businesses",
        "job_offers",
        "locations",
        "users",
        "ledger_entries",
        "report_shares",
        "report_share_views",
        "reviews",
        "partner_clicks",
        "notifications",
        "impersonation_logs",
        "cloud_errors",
        "cloud_errors_by_function",
        "cloud_recent_logs",
        "cloud_warnings",
    ]

    def __init__(
        self,
        mcp_url: str | None = None,
        mcp_token: str | None = None,
        writer: BQWriter | None = None,
    ):
        self.mcp = MCPClient(
            url=mcp_url or os.environ["FMT_MCP_URL"],
            auth_token=mcp_token or os.environ.get("FMT_MCP_AUTH_TOKEN", ""),
        )
        self.writer = writer or BQWriter()

    def run(self) -> Dict[str, Any]:
        """Sync all views. Returns stats dict."""
        self.writer.ensure_dataset(self.TARGET_DATASET)

        stats: Dict[str, Any] = {
            "synced": 0,
            "rows_written": 0,
            "errors": [],
            "started_at": datetime.now(timezone.utc).isoformat(),
        }

        for view in self.ALL_VIEWS:
            try:
                count = self._sync_view(view)
                stats["synced"] += 1
                stats["rows_written"] += count
            except Exception as exc:
                logger.error("Failed to sync %s: %s", view, exc, exc_info=True)
                stats["errors"].append(f"{view}: {exc}")

        stats["finished_at"] = datetime.now(timezone.utc).isoformat()
        self._update_watermark(stats)

        logger.info(
            "FMT sync complete: %d/%d views, %d rows, %d errors",
            stats["synced"], len(self.ALL_VIEWS),
            stats["rows_written"], len(stats["errors"]),
        )
        return stats

    # ------------------------------------------------------------------
    # Per-view sync
    # ------------------------------------------------------------------

    def _sync_view(self, view_name: str) -> int:
        columns = self._discover_columns(view_name)
        sql = build_sync_query(view_name, columns)
        rows = self._query_with_pagination(sql, view_name)

        if rows:
            self.writer.write_table(self.TARGET_DATASET, view_name, rows)
        else:
            logger.info("%s: 0 rows returned — writing empty table", view_name)

        return len(rows)

    # ------------------------------------------------------------------
    # Column discovery
    # ------------------------------------------------------------------

    def _discover_columns(self, view_name: str) -> List[str]:
        """Use MCP bq_describe_table to get column names from the source view."""
        if view_name in AGGREGATE_TABLES:
            return []

        try:
            result = self.mcp.call("bq_describe_table", {
                "dataset": "fmt_analytics",
                "table": view_name,
            })
        except Exception as exc:
            logger.warning(
                "bq_describe_table failed for %s, falling back to SELECT *: %s",
                view_name, exc,
            )
            return []

        if result is None:
            return []

        if isinstance(result, dict):
            cols = result.get("columns") or result.get("schema") or result.get("fields") or []
        elif isinstance(result, list):
            cols = result
        else:
            return []

        names = []
        for col in cols:
            if isinstance(col, dict):
                names.append(col.get("name") or col.get("column_name", ""))
            elif isinstance(col, str):
                names.append(col)
        return [n for n in names if n]

    # ------------------------------------------------------------------
    # Paginated MCP query
    # ------------------------------------------------------------------

    def _query_with_pagination(
        self, sql: str, view_name: str,
    ) -> List[Dict[str, Any]]:
        """Query MCP bq_query with LIMIT/OFFSET pagination."""
        all_rows: List[Dict[str, Any]] = []
        offset = 0
        page = 0

        while True:
            paginated_sql = f"{sql} LIMIT {_BATCH_SIZE} OFFSET {offset}"
            result = self.mcp.call("bq_query", {"query": paginated_sql})

            rows = _extract_rows(result)
            if not rows:
                break

            all_rows.extend(rows)
            page += 1

            if len(rows) < _BATCH_SIZE:
                break

            offset += _BATCH_SIZE
            time.sleep(_INTER_PAGE_DELAY)

        logger.info(
            "%s: fetched %d rows in %d page(s)", view_name, len(all_rows), page or 1,
        )
        return all_rows

    # ------------------------------------------------------------------
    # Watermark tracking (Supabase bm_watermarks)
    # ------------------------------------------------------------------

    def _update_watermark(self, stats: Dict[str, Any]) -> None:
        try:
            from lib.supabase_client import get_supabase
            sb = get_supabase()
            now = datetime.now(timezone.utc).isoformat()
            sb.table("bm_watermarks").upsert({
                "source": "fmt-data-sync",
                "last_processed_at": now,
                "updated_at": now,
                "metadata": {
                    "synced": stats.get("synced", 0),
                    "rows_written": stats.get("rows_written", 0),
                    "errors": len(stats.get("errors", [])),
                },
            }, on_conflict="source").execute()
        except Exception as exc:
            logger.warning("Watermark update failed (non-fatal): %s", exc)


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _extract_rows(result: Any) -> List[Dict[str, Any]]:
    """Normalise the MCP bq_query response into a list of row dicts."""
    if result is None:
        return []
    if isinstance(result, list):
        return result
    if isinstance(result, dict):
        rows = result.get("rows") or result.get("data") or []
        if isinstance(rows, list):
            return rows
    return []
