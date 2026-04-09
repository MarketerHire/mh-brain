"""GTM config sync — snapshots tags, triggers, and variables via MCP.

GTM configuration lives outside BigQuery, so we use the MCP gtm_list_*
tools and write the results as snapshot tables in fastmold_gtm.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

from .bq_writer import BQWriter
from .client import MCPClient

logger = logging.getLogger(__name__)

_GTM_TOOLS: List[Tuple[str, str]] = [
    ("gtm_list_tags", "tags"),
    ("gtm_list_triggers", "triggers"),
    ("gtm_list_variables", "variables"),
]


class GTMSync:
    """Sync GTM config from FMT's MCP server into our BigQuery."""

    TARGET_DATASET = "fastmold_gtm"

    def __init__(
        self,
        mcp_url: str | None = None,
        mcp_token: str | None = None,
    ):
        self.mcp = MCPClient(
            url=mcp_url or os.environ["FMT_MCP_URL"],
            auth_token=mcp_token or os.environ.get("FMT_MCP_AUTH_TOKEN", ""),
        )

    def sync(self, writer: BQWriter) -> Dict[str, Any]:
        """Pull GTM config via MCP and write snapshot tables."""
        writer.ensure_dataset(self.TARGET_DATASET)
        now = datetime.now(timezone.utc).isoformat()
        stats: Dict[str, Any] = {"tables": 0, "rows": 0, "errors": []}

        for tool_name, table_name in _GTM_TOOLS:
            try:
                items = self._fetch(tool_name)
                if not items:
                    logger.info("GTM %s: no items returned", tool_name)
                    continue

                for item in items:
                    item["_synced_at"] = now

                written = writer.write_table(
                    self.TARGET_DATASET, table_name, items,
                )
                stats["tables"] += 1
                stats["rows"] += written
                logger.info("GTM %s → %s: %d items", tool_name, table_name, written)

            except Exception as exc:
                logger.error("GTM %s failed: %s", tool_name, exc, exc_info=True)
                stats["errors"].append(f"{tool_name}: {exc}")

        return stats

    def _fetch(self, tool_name: str) -> List[Dict[str, Any]]:
        result = self.mcp.call(tool_name, {})
        if isinstance(result, list):
            return self._flatten_items(result)
        if isinstance(result, dict):
            items = (
                result.get("tags")
                or result.get("triggers")
                or result.get("variables")
                or result.get("items")
                or result.get("data")
                or []
            )
            return self._flatten_items(items)
        return []

    @staticmethod
    def _flatten_items(items: list) -> List[Dict[str, Any]]:
        """Ensure every item is a flat dict suitable for BQ load."""
        flat: List[Dict[str, Any]] = []
        for item in items:
            if isinstance(item, dict):
                row: Dict[str, Any] = {}
                for k, v in item.items():
                    if isinstance(v, (dict, list)):
                        import json
                        row[k] = json.dumps(v)
                    else:
                        row[k] = v
                flat.append(row)
        return flat
