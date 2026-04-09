"""Power BI to BigQuery sync orchestrator.

Pulls 16 tables from Mr. Christmas's Power BI workspace (Sales Reports V2 /
Dynamics 365 Business Central) into BigQuery dataset `mrchristmas_powerbi`.

PII policy: columns listed in pii.PII_EXCLUDE_COLUMNS are stripped before
extraction. High-volume tables listed in pii.AGGREGATE_TABLES are pulled
as monthly aggregates via SUMMARIZECOLUMNS instead of raw rows.
"""

from __future__ import annotations

import logging
import os
import re
from typing import Any, Dict, List, Optional

from .client import PowerBIClient
from .pii import AGGREGATE_TABLES, PII_EXCLUDE_COLUMNS, build_aggregation_dax

logger = logging.getLogger(__name__)

DATASET_ID = "mrchristmas_powerbi"

_WORKSPACE_ID = "173d8d23-58ad-4b92-bf29-321b4a569584"
_PBI_DATASET_ID = "5a5baf5b-4538-4884-8d21-a97b8221c2fa"

TABLE_CONFIG: List[Dict[str, Optional[str]]] = [
    {"pbi_name": "Invoice Lines",          "bq_name": "invoice_lines",        "date_column": "postingDate"},
    {"pbi_name": "Open Orders Line",       "bq_name": "open_orders_line",     "date_column": "postingDate"},
    {"pbi_name": "purchaseInvoices",       "bq_name": "purchase_invoices",    "date_column": "invoiceDate"},
    {"pbi_name": "generalLedgerEntries",   "bq_name": "general_ledger_entries","date_column": "postingDate"},
    {"pbi_name": "Open PO",                "bq_name": "open_po",              "date_column": "postingDate"},
    {"pbi_name": "purchaseOrderLINES",     "bq_name": "purchase_order_lines", "date_column": "postingDate"},
    {"pbi_name": "items (2)",              "bq_name": "items",                "date_column": None},
    {"pbi_name": "customers (2)",          "bq_name": "customers",            "date_column": None},
    {"pbi_name": "ItemByLocation",         "bq_name": "item_by_location",     "date_column": None},
    {"pbi_name": "vendors",                "bq_name": "vendors",              "date_column": None},
    {"pbi_name": "accounts",               "bq_name": "accounts",             "date_column": None},
    {"pbi_name": "Dates",                  "bq_name": "dates",                "date_column": None},
    {"pbi_name": "salesOrders LOCATION",   "bq_name": "sales_orders_location","date_column": None},
    {"pbi_name": "Locations",              "bq_name": "locations",            "date_column": None},
    {"pbi_name": "2025 Sales Budget & EST",     "bq_name": "budget_2025",     "date_column": None},
    {"pbi_name": "2026 Projections SP",    "bq_name": "projections_2026",     "date_column": None},
]

_NON_ALNUM = re.compile(r"[^a-z0-9]+")


def _strip_pii(table_name: str, columns: List[str]) -> List[str]:
    """Remove PII columns and internal RowNumber columns."""
    exclude = PII_EXCLUDE_COLUMNS.get(table_name, set())
    return [
        c for c in columns
        if c not in exclude and not c.startswith("RowNumber-")
    ]


def _sanitize_column(name: str) -> str:
    """Convert an arbitrary PBI column name to a BQ-safe snake_case identifier."""
    s = _NON_ALNUM.sub("_", name.lower()).strip("_")
    s = re.sub(r"_+", "_", s)
    if s and s[0].isdigit():
        s = f"col_{s}"
    return s


def _normalize_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Sanitize column names across all rows and deduplicate collisions."""
    if not rows:
        return rows

    sample = rows[0]
    col_map: Dict[str, str] = {}
    seen: Dict[str, int] = {}

    for original in sample:
        safe = _sanitize_column(original)
        if safe in seen:
            seen[safe] += 1
            safe = f"{safe}_{seen[safe]}"
        else:
            seen[safe] = 1
        col_map[original] = safe

    return [{col_map.get(k, k): v for k, v in row.items()} for row in rows]


class PowerBISync:
    """Orchestrate the PBI → BQ pipeline for Mr. Christmas."""

    def __init__(self):
        self.pbi = PowerBIClient(
            tenant_id=os.environ["MRC_POWERBI_TENANT_ID"],
            client_id=os.environ["MRC_POWERBI_CLIENT_ID"],
            client_secret=os.environ["MRC_POWERBI_CLIENT_SECRET"],
        )

    def run(self) -> Dict[str, Any]:
        from lib.mcp_sync.bq_writer import BQWriter

        bq = BQWriter()
        bq.ensure_dataset(DATASET_ID)

        try:
            refreshes = self.pbi.get_refresh_history(
                _WORKSPACE_ID, _PBI_DATASET_ID, top=1
            )
            if refreshes:
                latest = refreshes[0]
                logger.info(
                    "Latest PBI refresh: status=%s end=%s",
                    latest.get("status"),
                    latest.get("endTime"),
                )
        except Exception as exc:
            logger.warning("Could not fetch refresh history: %s", exc)

        column_map = self._build_column_map()

        stats: Dict[str, Any] = {"tables": {}, "total_rows": 0, "errors": []}

        for entry in TABLE_CONFIG:
            pbi_name = entry["pbi_name"]
            bq_name = entry["bq_name"]
            date_col = entry["date_column"]

            try:
                columns = column_map.get(pbi_name)
                if not columns:
                    logger.warning("Table '%s' not found in COLUMNSTATISTICS — skipping", pbi_name)
                    stats["errors"].append({"table": pbi_name, "error": "not found in dataset"})
                    continue

                if pbi_name in AGGREGATE_TABLES:
                    rows = self._extract_aggregated(pbi_name)
                    mode = "aggregated"
                else:
                    safe_cols = _strip_pii(pbi_name, columns)
                    row_count = self.pbi.count_rows(_WORKSPACE_ID, _PBI_DATASET_ID, pbi_name)
                    logger.info("Table '%s': %d rows, %d/%d columns (PII stripped)",
                                pbi_name, row_count, len(safe_cols), len(columns))
                    rows = self.pbi.extract_table(
                        _WORKSPACE_ID, _PBI_DATASET_ID, pbi_name,
                        safe_cols, row_count=row_count, date_column=date_col,
                    )
                    mode = "raw"

                rows = _normalize_rows(rows)
                written = bq.write_table(DATASET_ID, bq_name, rows)
                stats["tables"][bq_name] = {"rows": written, "mode": mode}
                stats["total_rows"] += written

            except Exception as exc:
                logger.error("Failed to sync '%s': %s", pbi_name, exc, exc_info=True)
                stats["errors"].append({"table": pbi_name, "error": str(exc)})

        logger.info(
            "Sync complete: %d tables, %d total rows, %d errors",
            len(stats["tables"]),
            stats["total_rows"],
            len(stats["errors"]),
        )
        return stats

    def _extract_aggregated(self, pbi_name: str) -> List[Dict[str, Any]]:
        """Extract an aggregated table by running monthly SUMMARIZECOLUMNS."""
        cfg = AGGREGATE_TABLES[pbi_name]
        date_col = cfg["date_column"]

        bounds = self.pbi._execute_dax(
            _WORKSPACE_ID, _PBI_DATASET_ID,
            f"EVALUATE ROW("
            f"\"mn\", MIN('{pbi_name}'[{date_col}]), "
            f"\"mx\", MAX('{pbi_name}'[{date_col}]))",
        )
        if not bounds:
            return []

        vals = list(bounds[0].values())
        min_year = int(str(vals[0])[:4])
        max_year = int(str(vals[1])[:4])
        logger.info(
            "Aggregating '%s' by month: %d to %d", pbi_name, min_year, max_year,
        )

        all_rows: List[Dict[str, Any]] = []
        for year in range(min_year, max_year + 1):
            for month in range(1, 13):
                dax = build_aggregation_dax(pbi_name, year, month)
                try:
                    batch = self.pbi._execute_dax(
                        _WORKSPACE_ID, _PBI_DATASET_ID, dax, timeout=300,
                    )
                except Exception as exc:
                    logger.warning("  agg %d-%02d failed: %s", year, month, exc)
                    continue
                if batch:
                    all_rows.extend(batch)
                    logger.info("  agg %d-%02d: %d rows", year, month, len(batch))

        logger.info("Aggregated '%s': %d rows (from raw)", pbi_name, len(all_rows))
        return all_rows

    def _build_column_map(self) -> Dict[str, List[str]]:
        """Discover all tables via COLUMNSTATISTICS and return {name: [columns]}."""
        tables = self.pbi.discover_tables(_WORKSPACE_ID, _PBI_DATASET_ID)
        return {t["name"]: t["columns"] for t in tables}
