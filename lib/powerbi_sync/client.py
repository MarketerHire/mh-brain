"""Power BI REST API client — OAuth2 service principal auth + DAX query execution.

Handles token lifecycle, table discovery, row counting, and paginated extraction
via month-based partitioning for large tables.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

_TOKEN_URL = "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
_PBI_API = "https://api.powerbi.com/v1.0/myorg"
_SCOPE = "https://analysis.windows.net/powerbi/api/.default"

MAX_ROWS_PER_QUERY = 50_000


class PowerBIClient:
    """OAuth2 service principal client for Power BI REST API."""

    def __init__(self, tenant_id: str, client_id: str, client_secret: str):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self._token: Optional[str] = None
        self._token_expires: float = 0

    def _ensure_token(self):
        if self._token and time.time() < self._token_expires - 60:
            return

        resp = requests.post(
            _TOKEN_URL.format(tenant_id=self.tenant_id),
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "scope": _SCOPE,
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        self._token = data["access_token"]
        self._token_expires = time.time() + data.get("expires_in", 3599)
        logger.info("Power BI token acquired (expires in %ds)", data.get("expires_in", 0))

    def _headers(self) -> Dict[str, str]:
        self._ensure_token()
        return {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }

    def _execute_dax(
        self,
        workspace_id: str,
        dataset_id: str,
        dax: str,
        timeout: int = 120,
    ) -> List[Dict[str, Any]]:
        """Execute a DAX query and return cleaned rows."""
        resp = requests.post(
            f"{_PBI_API}/groups/{workspace_id}/datasets/{dataset_id}/executeQueries",
            headers=self._headers(),
            json={
                "queries": [{"query": dax}],
                "serializerSettings": {"includeNulls": True},
            },
            timeout=timeout,
        )
        resp.raise_for_status()

        result = resp.json()
        if "error" in result:
            raise RuntimeError(f"DAX query failed: {result['error']}")

        rows = result["results"][0]["tables"][0]["rows"]
        return self._clean_rows(rows)

    def discover_tables(
        self, workspace_id: str, dataset_id: str
    ) -> List[Dict[str, Any]]:
        """Discover all tables and their columns via COLUMNSTATISTICS()."""
        rows = self._execute_dax(
            workspace_id, dataset_id, "EVALUATE COLUMNSTATISTICS()"
        )
        tables: Dict[str, List[str]] = {}
        for r in rows:
            tname = r.get("Table Name", "")
            cname = r.get("Column Name", "")
            if tname not in tables:
                tables[tname] = []
            tables[tname].append(cname)

        return [{"name": t, "columns": cols} for t, cols in tables.items()]

    def count_rows(
        self, workspace_id: str, dataset_id: str, table: str
    ) -> int:
        """Get row count for a table."""
        rows = self._execute_dax(
            workspace_id,
            dataset_id,
            f"EVALUATE ROW(\"count\", COUNTROWS('{table}'))",
        )
        return int(list(rows[0].values())[0]) if rows else 0

    def extract_table(
        self,
        workspace_id: str,
        dataset_id: str,
        table: str,
        columns: List[str],
        row_count: int = 0,
        date_column: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Extract all rows from a table.

        Small tables (<MAX_ROWS_PER_QUERY): single query.
        Large tables: partitioned by month on date_column (plain FILTER, no
        SELECTCOLUMNS) to support special characters in column names.
        """
        usable = [c for c in columns if not c.startswith("RowNumber-")]
        col_expr = ", ".join(f'"{c}", [{c}]' for c in usable)

        if row_count <= MAX_ROWS_PER_QUERY:
            dax = f"EVALUATE SELECTCOLUMNS('{table}', {col_expr})"
            return self._execute_dax(workspace_id, dataset_id, dax, timeout=180)

        if not date_column:
            raise ValueError(
                f"Table '{table}' has {row_count:,} rows but no date_column for partitioning"
            )

        return self._extract_partitioned(
            workspace_id, dataset_id, table, col_expr, date_column
        )

    def _extract_partitioned(
        self,
        workspace_id: str,
        dataset_id: str,
        table: str,
        col_expr: str,
        date_column: str,
    ) -> List[Dict[str, Any]]:
        """Extract a large table by month partitioning.

        Uses plain FILTER (no SELECTCOLUMNS) to avoid DAX parse issues
        with special chars in column names like PO#, Invoice #, Item No.
        """
        bounds = self._execute_dax(
            workspace_id,
            dataset_id,
            f"EVALUATE ROW("
            f"\"mn\", MIN('{table}'[{date_column}]), "
            f"\"mx\", MAX('{table}'[{date_column}]))",
        )
        if not bounds:
            return []

        vals = list(bounds[0].values())
        min_val, max_val = vals[0], vals[1]
        if not min_val or not max_val:
            return []

        min_year = int(str(min_val)[:4])
        max_year = int(str(max_val)[:4])
        logger.info(
            "Partitioning '%s' by month: %d to %d", table, min_year, max_year,
        )

        all_rows: List[Dict[str, Any]] = []
        for year in range(min_year, max_year + 1):
            for month in range(1, 13):
                dax = (
                    f"EVALUATE FILTER('{table}', "
                    f"YEAR([{date_column}]) = {year} && "
                    f"MONTH([{date_column}]) = {month})"
                )
                try:
                    batch = self._execute_dax(
                        workspace_id, dataset_id, dax, timeout=300,
                    )
                except Exception as exc:
                    logger.warning("  %d-%02d failed: %s", year, month, exc)
                    continue
                if batch:
                    all_rows.extend(batch)
                    logger.info("  %d-%02d: %d rows", year, month, len(batch))

        logger.info("Total extracted from '%s': %d rows", table, len(all_rows))
        return all_rows

    @staticmethod
    def _clean_rows(rows: List[Dict]) -> List[Dict]:
        """Strip bracket notation from DAX output column names."""
        cleaned = []
        for r in rows:
            clean = {}
            for k, v in r.items():
                col = k.strip("[]")
                clean[col] = v
            cleaned.append(clean)
        return cleaned

    def get_refresh_history(
        self, workspace_id: str, dataset_id: str, top: int = 5
    ) -> List[Dict[str, Any]]:
        """Return recent refresh history for monitoring."""
        self._ensure_token()
        resp = requests.get(
            f"{_PBI_API}/groups/{workspace_id}/datasets/{dataset_id}/refreshes",
            headers=self._headers(),
            params={"$top": top},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json().get("value", [])
