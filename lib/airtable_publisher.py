"""
BrightMatter Airtable Publisher

Pushes high-confidence semantic patterns to Airtable so they're
visible alongside MH-OS recommendations. Uses the same REST API
pattern as MH-OS Trigger.dev tasks (no SDK, just requests).

Supports client-specific Airtable bases: pass a different base_id
and recommendations_table_id per client.

Env vars:
    AIRTABLE_API_KEY   — Personal Access Token (pat...)
    AIRTABLE_BASE_ID   — Base ID (default: appfuhAEXKZBKcDLi — MH-OS ops base)

Usage:
    from lib.airtable_publisher import AirtablePublisher
    publisher = AirtablePublisher()
    publisher.publish_patterns(patterns)
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

DEFAULT_BASE_ID = "appfuhAEXKZBKcDLi"
DEFAULT_RECOMMENDATIONS_TABLE_ID = "tblGQSQMSGSBpOXQj"
AIRTABLE_API_URL = "https://api.airtable.com/v0"
BATCH_SIZE = 10  # Airtable max records per request


class AirtablePublisher:
    """Publishes BrightMatter patterns to an Airtable table."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_id: Optional[str] = None,
        table_name: str = "BrightMatter Patterns",
        recommendations_table_id: Optional[str] = None,
    ):
        self._api_key = api_key or os.environ.get("AIRTABLE_API_KEY", "")
        self._base_id = base_id or os.environ.get("AIRTABLE_BASE_ID", DEFAULT_BASE_ID)
        self._table_name = table_name
        self._rec_table_id = recommendations_table_id or DEFAULT_RECOMMENDATIONS_TABLE_ID

        if not self._api_key:
            raise ValueError("AIRTABLE_API_KEY must be set")

    @property
    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
        }

    @property
    def _table_url(self) -> str:
        return f"{AIRTABLE_API_URL}/{self._base_id}/{self._table_name}"

    def _pattern_to_fields(self, pattern: Dict[str, Any]) -> Dict[str, Any]:
        """Convert a semantic_patterns row to Airtable field format."""
        import json

        confidence = pattern.get("confidence", 0)
        evidence = pattern.get("evidence_count", 0)
        successes = pattern.get("successes", 0)
        success_rate = successes / evidence if evidence > 0 else 0

        condition = pattern.get("condition", {})
        recommendation = pattern.get("recommendation", {})

        return {
            "Pattern ID": pattern.get("pattern_id", ""),
            "Skill": pattern.get("skill_name", ""),
            "Domain": pattern.get("domain", ""),
            "Level": pattern.get("pattern_level", "segment"),
            "Condition": json.dumps(condition) if isinstance(condition, dict) else str(condition),
            "Recommendation": json.dumps(recommendation) if isinstance(recommendation, dict) else str(recommendation),
            "Confidence": round(confidence, 3),
            "Evidence Count": evidence,
            "Success Rate": round(success_rate, 3),
            "Expected Value": pattern.get("expected_value", 0),
            "Last Reinforced": (
                pattern.get("last_reinforced_at", "")
                if pattern.get("last_reinforced_at")
                else ""
            ),
            "Status": "Archived" if pattern.get("archived_at") else "Active",
            "Source": "BrightMatter",
            "Updated": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        }

    def publish_patterns(
        self, patterns: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Batch upsert patterns to Airtable.

        Returns stats dict with counts of created/updated/failed.
        """
        import requests

        stats = {"total": len(patterns), "published": 0, "failed": 0, "errors": []}

        for i in range(0, len(patterns), BATCH_SIZE):
            batch = patterns[i : i + BATCH_SIZE]
            records = [
                {"fields": self._pattern_to_fields(p)} for p in batch
            ]

            payload = {
                "records": records,
                "typecast": True,
            }

            try:
                resp = requests.patch(
                    self._table_url,
                    headers=self._headers,
                    json={
                        **payload,
                        "performUpsert": {
                            "fieldsToMergeOn": ["Pattern ID"],
                        },
                    },
                    timeout=30,
                )

                if resp.status_code == 200:
                    result = resp.json()
                    stats["published"] += len(result.get("records", []))
                    logger.info(
                        f"Airtable batch {i // BATCH_SIZE + 1}: "
                        f"{len(result.get('records', []))} records upserted"
                    )
                else:
                    stats["failed"] += len(batch)
                    error_msg = resp.text[:500]
                    stats["errors"].append(error_msg)
                    logger.error(
                        f"Airtable batch {i // BATCH_SIZE + 1} failed "
                        f"({resp.status_code}): {error_msg}"
                    )
            except Exception as e:
                stats["failed"] += len(batch)
                stats["errors"].append(str(e))
                logger.error(f"Airtable batch {i // BATCH_SIZE + 1} error: {e}")

        return stats

    def publish_as_recommendations(
        self, recommendations: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Publish pre-formatted recommendations to the Recommendations table.

        Accepts dicts with keys matching the Recommendation schema:
            source, type, summary, details, channels, confidence

        These are produced by lib.recommendation_formatter.format_pattern().
        """
        import requests

        stats = {"total": len(recommendations), "published": 0, "failed": 0, "errors": []}
        rec_url = f"{AIRTABLE_API_URL}/{self._base_id}/{self._rec_table_id}"

        for i in range(0, len(recommendations), BATCH_SIZE):
            batch = recommendations[i : i + BATCH_SIZE]
            records = []
            for rec in batch:
                fields: Dict[str, Any] = {
                    "Date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                    "Source": rec.get("source", "BrightMatter"),
                    "Type": rec.get("type", "other"),
                    "Summary": rec.get("summary", "")[:200],
                    "Details": rec.get("details", ""),
                    "Confidence": rec.get("confidence", "Medium"),
                    "Status": "Open",
                    "Created At": datetime.now(timezone.utc).isoformat(),
                }
                channels = rec.get("channels")
                if channels:
                    fields["Channels"] = ", ".join(channels) if isinstance(channels, list) else str(channels)
                records.append({"fields": fields})

            try:
                resp = requests.post(
                    rec_url,
                    headers=self._headers,
                    json={"records": records, "typecast": True},
                    timeout=30,
                )
                if resp.status_code == 200:
                    stats["published"] += len(resp.json().get("records", []))
                else:
                    stats["failed"] += len(batch)
                    stats["errors"].append(resp.text[:500])
                    logger.error(
                        f"Airtable recommendations batch {i // BATCH_SIZE + 1} "
                        f"failed ({resp.status_code}): {resp.text[:200]}"
                    )
            except Exception as e:
                stats["failed"] += len(batch)
                stats["errors"].append(str(e))
                logger.error(f"Airtable recommendations batch error: {e}")

        return stats
