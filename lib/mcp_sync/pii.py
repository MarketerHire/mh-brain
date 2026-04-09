"""PII column exclusions, aggregation rules, and SQL builder.

PII exclusion happens at the SQL level — the SELECT sent to MCP's bq_query
omits sensitive columns so they never leave FMT's project.

High-volume tables are aggregated in the query itself to reduce data transfer.
"""

from __future__ import annotations

from typing import Dict, List, Set

# ---------------------------------------------------------------------------
# 1. Columns to EXCLUDE per view (everything else passes through)
# ---------------------------------------------------------------------------

PII_EXCLUDE_COLUMNS: Dict[str, Set[str]] = {
    "users": {
        "name", "email", "phone", "address", "password_hash",
        "profile_image_url", "first_name", "last_name",
        "display_name", "photo_url",
    },
    "booking_requests": {
        "customer_name", "customer_email", "customer_phone",
        "address", "street", "city_detail", "zip_code",
        "contact_name", "contact_email", "contact_phone",
    },
    "inspections": {
        "customer_name", "customer_email", "customer_phone",
        "property_address", "contact_name", "contact_email",
    },
    "businesses": {
        "primary_contact_email", "primary_contact_phone",
        "billing_address", "contact_email", "contact_phone",
    },
    "reviews": {
        "reviewer_name", "reviewer_email", "author_name", "author_email",
    },
    "job_offers": {
        "inspector_email", "inspector_phone", "inspector_name",
    },
    "notifications": {
        "recipient_email", "recipient_name", "message_body",
        "recipient_phone",
    },
    "impersonation_logs": {
        "target_user_email", "target_user_name",
        "impersonator_email", "impersonator_name",
    },
}

# ---------------------------------------------------------------------------
# 2. Aggregation rules for high-volume tables
# ---------------------------------------------------------------------------

AGGREGATE_TABLES: Dict[str, dict] = {
    "interactions": {
        "group_by": [
            "DATE(created_at) AS metric_date",
            "campaign_id",
            "ad_creative_id",
            "type",
        ],
        "metrics": ["COUNT(*) AS event_count"],
    },
    "report_share_views": {
        "group_by": [
            "DATE(viewed_at) AS metric_date",
            "share_id",
        ],
        "metrics": [
            "COUNT(*) AS view_count",
            "COUNT(DISTINCT session_id) AS unique_views",
        ],
    },
    "partner_clicks": {
        "group_by": [
            "DATE(clicked_at) AS metric_date",
            "inspection_id",
        ],
        "metrics": ["COUNT(*) AS click_count"],
    },
    "cloud_errors": {
        "group_by": [
            "DATE(timestamp) AS metric_date",
            "function_name",
            "error_type",
        ],
        "metrics": ["COUNT(*) AS error_count"],
    },
    "cloud_warnings": {
        "group_by": [
            "DATE(timestamp) AS metric_date",
            "function_name",
        ],
        "metrics": ["COUNT(*) AS warning_count"],
    },
    "cloud_recent_logs": {
        "group_by": [
            "DATE(timestamp) AS metric_date",
            "function_name",
            "severity",
        ],
        "metrics": ["COUNT(*) AS log_count"],
    },
}

# ---------------------------------------------------------------------------
# 3. SQL builder
# ---------------------------------------------------------------------------

_SOURCE_DATASET = "fmt_analytics"


def build_sync_query(view_name: str, columns: List[str]) -> str:
    """Build a SELECT statement with PII excluded and aggregation applied.

    For aggregated tables the column list is ignored — the query uses
    pre-defined GROUP BY / metric expressions.
    """
    if view_name in AGGREGATE_TABLES:
        agg = AGGREGATE_TABLES[view_name]
        group_cols = ", ".join(agg["group_by"])
        metric_cols = ", ".join(agg["metrics"])
        return (
            f"SELECT {group_cols}, {metric_cols} "
            f"FROM {_SOURCE_DATASET}.{view_name} "
            f"GROUP BY ALL"
        )

    exclude = PII_EXCLUDE_COLUMNS.get(view_name, set())
    safe_cols = [c for c in columns if c not in exclude]
    if not safe_cols:
        safe_cols = ["*"]
    col_list = ", ".join(safe_cols)
    return f"SELECT {col_list} FROM {_SOURCE_DATASET}.{view_name}"
