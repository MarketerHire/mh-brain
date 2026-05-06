"""Google Search Console adapter — daily aggregated search analytics.

Pulls clicks / impressions / ctr / position from the Search Console
``searchAnalytics.query`` endpoint, broken down by date, query, page, and
device. The endpoint never returns user-level data (no IPs, no logged-in
identifiers, no per-user fingerprints) — only aggregate counters keyed by
the dimensions we request. We still enforce a strict ``_SAFE_FIELDS``
allowlist as defense-in-depth in case Google adds user-level dimensions
later.

Auth flow mirrors ``google_ads.py``: OAuth2 refresh-token → access-token
exchange against ``oauth2.googleapis.com``. Refresh tokens are expected
in ``config.credentials`` already (resolved upstream from Firebase /
env), so we don't run a browser auth flow here.

API references:
    https://developers.google.com/webmaster-tools/v1/searchanalytics/query
    https://developers.google.com/webmaster-tools/v1/api_reference_index

Site URL format (resolved from ``config.credentials.site_url``):
    "sc-domain:example.com"     → Domain property
    "https://example.com/"      → URL-prefix property

Multi-site support: pass ``config.extra["additional_sites"]`` as a list
of ``{"site_url": "...", "label": "..."}`` dicts to ingest more than one
property per client. Each site emits rows tagged with its own ``label``
inside the ``metrics`` payload.
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from .base import BasePlatformAdapter, DailyMetricRow, PlatformConfig

logger = logging.getLogger(__name__)

GSC_BASE = "https://searchconsole.googleapis.com/webmasters/v3"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
_GSC_UA = "MH1-BrightMatter/1.0"

# ── PII allowlists — only these fields survive _strip_to_safe ─────
# GSC returns no user-level data by design, but we allowlist anyway so
# any future schema additions don't silently leak through.
_SAFE_FIELDS: Dict[str, set] = {
    "row":  {"keys", "clicks", "impressions", "ctr", "position"},
}

# Only these dimensions are ever requested from the API.
_ALLOWED_DIMENSIONS = {"date", "query", "page", "device", "country"}


def _strip_to_safe(rows: List[Dict[str, Any]], entity: str) -> List[Dict[str, Any]]:
    """Drop everything except allowlisted fields."""
    safe = _SAFE_FIELDS.get(entity, {"keys"})
    return [{k: v for k, v in r.items() if k in safe} for r in rows]


def _gsc_post(
    site_url: str, body: Dict[str, Any], access_token: str,
) -> Dict[str, Any]:
    encoded = urllib.parse.quote(site_url, safe="")
    url = f"{GSC_BASE}/sites/{encoded}/searchAnalytics/query"
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": _GSC_UA,
        },
        method="POST",
    )
    resp = urllib.request.urlopen(req, timeout=120)
    return json.loads(resp.read())


def _exchange_refresh_token(
    client_id: str, client_secret: str, refresh_token: str,
) -> str:
    """OAuth2 refresh-token → access-token. Mirrors google_ads.py."""
    body = urllib.parse.urlencode({
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
    }).encode()
    req = urllib.request.Request(
        GOOGLE_TOKEN_URL,
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    resp = urllib.request.urlopen(req, timeout=30)
    return json.loads(resp.read())["access_token"]


class GoogleSearchConsoleAdapter(BasePlatformAdapter):
    """Search Console adapter — aggregated daily search analytics, zero PII."""

    PLATFORM = "gsc"
    RATE_LIMIT_DELAY = 0.5  # GSC: 1200 QPM/project; we stay well below.
    MAX_LOOKBACK_DAYS = 480  # GSC retains ~16 months of search analytics data.

    DEFAULT_ROW_LIMIT = 25000  # API max per request.

    # ── Entry point ───────────────────────────────────────────────

    def pull_daily_metrics(
        self,
        config: PlatformConfig,
        start_date: date,
        end_date: date,
    ) -> List[DailyMetricRow]:
        creds = config.credentials or {}
        client_id = creds.get("client_id", "")
        client_secret = creds.get("client_secret", "")
        refresh_token = creds.get("refresh_token", "") or os.environ.get(
            "GSC_REFRESH_TOKEN", ""
        )
        if not (client_id and client_secret and refresh_token):
            logger.warning(
                f"GSC: missing OAuth credentials for {config.client_name}"
            )
            return []

        sites = self._resolve_sites(config)
        if not sites:
            logger.warning(f"GSC: no site_url configured for {config.client_name}")
            return []

        try:
            access_token = _exchange_refresh_token(
                client_id, client_secret, refresh_token,
            )
        except Exception as e:
            logger.error(f"GSC OAuth refresh failed for {config.client_name}: {e}")
            return []

        row_limit_queries = int(config.extra.get("row_limit_queries", 50))
        row_limit_pages = int(config.extra.get("row_limit_pages", 50))

        memory_rows: List[DailyMetricRow] = []
        bq_rows: List[Dict[str, Any]] = []

        ingestion_type = "daily" if start_date == end_date else "backfill"
        ingested_at = datetime.now(timezone.utc).isoformat()

        for site_url, label in sites:
            try:
                site_rows = self._pull_site(
                    site_url=site_url,
                    label=label,
                    access_token=access_token,
                    start_date=start_date,
                    end_date=end_date,
                    row_limit_queries=row_limit_queries,
                    row_limit_pages=row_limit_pages,
                )
            except urllib.error.HTTPError as e:
                body_snippet = ""
                try:
                    body_snippet = e.read().decode()[:300]
                except Exception:
                    pass
                logger.error(
                    f"GSC site {site_url} failed: HTTP {e.code} — {body_snippet}"
                )
                continue
            except Exception as e:
                logger.error(f"GSC site {site_url} failed: {e}")
                continue

            for r in site_rows:
                memory_rows.append(DailyMetricRow(
                    metric_date=r["metric_date"],
                    metrics=r["metrics"],
                    record_count=int(r["metrics"].get("clicks", 0) or 0),
                    breakdown=r["breakdown"],
                ))
                bq_rows.append({
                    "metric_date": r["metric_date"].isoformat(),
                    "site_url": site_url,
                    "site_label": label,
                    "breakdown": r["breakdown"] or "site_total",
                    "dimension_value": r["dimension_value"],
                    "clicks": int(r["metrics"].get("clicks", 0) or 0),
                    "impressions": int(r["metrics"].get("impressions", 0) or 0),
                    "ctr": float(r["metrics"].get("ctr", 0.0) or 0.0),
                    "position": float(r["metrics"].get("position", 0.0) or 0.0),
                    "ingestion_type": ingestion_type,
                    "ingested_at": ingested_at,
                })

        if bq_rows:
            self._write_to_bq(config, bq_rows)

        logger.info(
            f"GSC: {len(memory_rows)} rows across {len(sites)} sites "
            f"for {config.client_name}"
        )
        return memory_rows

    # ── Site resolution ──────────────────────────────────────────

    def _resolve_sites(
        self, config: PlatformConfig,
    ) -> List[Tuple[str, str]]:
        creds = config.credentials or {}
        primary = creds.get("site_url") or config.extra.get("site_url", "")
        primary_label = config.extra.get("label", "primary")

        sites: List[Tuple[str, str]] = []
        if primary:
            sites.append((primary, primary_label))

        for extra in config.extra.get("additional_sites", []) or []:
            site = extra.get("site_url") or extra.get("siteUrl") or ""
            label = extra.get("label", f"site_{len(sites) + 1}")
            if site and not any(s == site for s, _ in sites):
                sites.append((site, label))

        return sites

    # ── Per-site pull ────────────────────────────────────────────

    def _pull_site(
        self,
        site_url: str,
        label: str,
        access_token: str,
        start_date: date,
        end_date: date,
        row_limit_queries: int,
        row_limit_pages: int,
    ) -> List[Dict[str, Any]]:
        start_iso = start_date.isoformat()
        end_iso = end_date.isoformat()
        out: List[Dict[str, Any]] = []

        # 1. Daily site totals — one row per day.
        daily_rows = self._query_with_pagination(
            site_url, access_token,
            body={
                "startDate": start_iso,
                "endDate": end_iso,
                "dimensions": ["date"],
                "rowLimit": self.DEFAULT_ROW_LIMIT,
            },
        )
        for row in daily_rows:
            keys = row.get("keys") or []
            if not keys:
                continue
            d = self._parse_date(keys[0])
            if not d:
                continue
            out.append({
                "metric_date": d,
                "breakdown": None,
                "dimension_value": "",
                "metrics": self._extract_metrics(row),
            })

        # 2. Daily × query — top N queries per day.
        date_query_rows = self._query_per_day(
            site_url, access_token, start_date, end_date,
            dimensions=["date", "query"],
            row_limit=row_limit_queries,
        )
        for r in date_query_rows:
            out.append({
                "metric_date": r["date"],
                "breakdown": "query",
                "dimension_value": r["dimension_value"],
                "metrics": r["metrics"],
            })

        # 3. Daily × page — top N pages per day.
        date_page_rows = self._query_per_day(
            site_url, access_token, start_date, end_date,
            dimensions=["date", "page"],
            row_limit=row_limit_pages,
        )
        for r in date_page_rows:
            out.append({
                "metric_date": r["date"],
                "breakdown": "page",
                "dimension_value": r["dimension_value"],
                "metrics": r["metrics"],
            })

        # 4. Daily × device — small fixed cardinality (mobile/desktop/tablet).
        date_device_rows = self._query_per_day(
            site_url, access_token, start_date, end_date,
            dimensions=["date", "device"],
            row_limit=10,
        )
        for r in date_device_rows:
            out.append({
                "metric_date": r["date"],
                "breakdown": "device",
                "dimension_value": r["dimension_value"],
                "metrics": r["metrics"],
            })

        return out

    def _query_per_day(
        self,
        site_url: str,
        access_token: str,
        start_date: date,
        end_date: date,
        dimensions: List[str],
        row_limit: int,
    ) -> List[Dict[str, Any]]:
        """Issue one request per day so we can apply per-day row_limit semantics
        cleanly. The API accepts ``dimensions=[date, X]`` with a global row
        limit, but we want top-N *per day*; the simplest way to guarantee that
        is one request per day."""
        for d in dimensions:
            if d not in _ALLOWED_DIMENSIONS:
                raise ValueError(f"GSC: dimension '{d}' not in allowlist")

        # Strip the leading "date" dimension when building each per-day query —
        # the API still emits the date field via the request window.
        sub_dims = [d for d in dimensions if d != "date"]

        out: List[Dict[str, Any]] = []
        cursor = start_date
        while cursor <= end_date:
            iso = cursor.isoformat()
            rows = self._query_with_pagination(
                site_url, access_token,
                body={
                    "startDate": iso,
                    "endDate": iso,
                    "dimensions": sub_dims,
                    "rowLimit": row_limit,
                },
            )
            for row in rows:
                keys = row.get("keys") or []
                if not keys:
                    continue
                out.append({
                    "date": cursor,
                    "dimension_value": str(keys[0]),
                    "metrics": self._extract_metrics(row),
                })
            cursor += timedelta(days=1)
            # Stagger per-day calls so backfills don't burst against the
            # 1200 QPM project quota.
            time.sleep(self.RATE_LIMIT_DELAY)

        return out

    def _query_with_pagination(
        self,
        site_url: str,
        access_token: str,
        body: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """Page through ``rowLimit``-sized chunks until the API returns fewer
        rows than requested. GSC tops out at 25k rows per call and uses
        ``startRow`` for offset-based pagination."""
        row_limit = int(body.get("rowLimit", self.DEFAULT_ROW_LIMIT))
        if row_limit > self.DEFAULT_ROW_LIMIT:
            row_limit = self.DEFAULT_ROW_LIMIT
            body = {**body, "rowLimit": row_limit}

        all_rows: List[Dict[str, Any]] = []
        start_row = 0
        max_pages = 40  # safety bound: 40 × 25k = 1M rows

        for _ in range(max_pages):
            paged_body = {**body, "startRow": start_row}
            data = _gsc_post(site_url, paged_body, access_token)
            raw = data.get("rows", []) or []
            safe = _strip_to_safe(raw, "row")
            del raw

            all_rows.extend(safe)
            if len(safe) < row_limit:
                break
            start_row += row_limit
            time.sleep(self.RATE_LIMIT_DELAY)

        return all_rows

    # ── Helpers ──────────────────────────────────────────────────

    @staticmethod
    def _parse_date(value: Any) -> Optional[date]:
        if not value or not isinstance(value, str):
            return None
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None

    def _extract_metrics(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "clicks": self._safe_int(row.get("clicks")),
            "impressions": self._safe_int(row.get("impressions")),
            "ctr": self._safe_float(row.get("ctr")),
            "position": self._safe_float(row.get("position")),
        }

    # ── BigQuery sink ────────────────────────────────────────────

    def _write_to_bq(self, config: PlatformConfig, rows: List[Dict[str, Any]]) -> None:
        bq_dataset = config.extra.get("bq_dataset", "")
        if not bq_dataset:
            logger.info("GSC BQ: no bq_dataset in config, skipping BQ write")
            return

        table = config.extra.get("bq_table", "gsc_daily_metrics")
        target_project = config.extra.get("bq_project", "moe-platform-479917")

        client = self._get_bq_client(target_project=target_project)
        if not client:
            return

        from google.cloud.bigquery import (
            LoadJobConfig, SchemaField, WriteDisposition,
        )

        schema = [
            SchemaField("metric_date", "DATE"),
            SchemaField("site_url", "STRING"),
            SchemaField("site_label", "STRING"),
            SchemaField("breakdown", "STRING"),
            SchemaField("dimension_value", "STRING"),
            SchemaField("clicks", "INTEGER"),
            SchemaField("impressions", "INTEGER"),
            SchemaField("ctr", "FLOAT"),
            SchemaField("position", "FLOAT"),
            SchemaField("ingestion_type", "STRING"),
            SchemaField("ingested_at", "TIMESTAMP"),
        ]

        table_id = f"{target_project}.{bq_dataset}.{table}"
        staging_id = f"{target_project}.{bq_dataset}._staging_{table}"

        try:
            self._ensure_table(
                client, table_id, schema,
                partition_field="metric_date",
                clustering_fields=["site_url", "breakdown"],
            )

            job = client.load_table_from_json(
                rows,
                staging_id,
                job_config=LoadJobConfig(
                    schema=schema,
                    write_disposition=WriteDisposition.WRITE_TRUNCATE,
                ),
            )
            job.result()

            merge_sql = f"""
            MERGE `{table_id}` T
            USING `{staging_id}` S
            ON T.metric_date = S.metric_date
               AND T.site_url = S.site_url
               AND T.breakdown = S.breakdown
               AND T.dimension_value = S.dimension_value
            WHEN MATCHED THEN UPDATE SET
                site_label = S.site_label,
                clicks = S.clicks,
                impressions = S.impressions,
                ctr = S.ctr,
                position = S.position,
                ingestion_type = S.ingestion_type,
                ingested_at = S.ingested_at
            WHEN NOT MATCHED THEN INSERT ROW
            """
            merge_job = client.query(merge_sql)
            merge_job.result()
            affected = merge_job.num_dml_affected_rows or len(rows)

            client.delete_table(staging_id, not_found_ok=True)

            logger.info(
                f"GSC BQ: upserted {affected} rows to {table_id} "
                f"({len(rows)} staged)"
            )
        except Exception as e:
            logger.warning(f"GSC BQ write failed (non-fatal): {e}")

    def _ensure_table(
        self,
        client,
        table_id: str,
        schema,
        partition_field: Optional[str] = None,
        clustering_fields: Optional[List[str]] = None,
    ) -> None:
        from google.api_core.exceptions import NotFound
        from google.cloud.bigquery import Table, TimePartitioning, TimePartitioningType

        try:
            client.get_table(table_id)
            return
        except NotFound:
            pass

        table = Table(table_id, schema=schema)
        if partition_field:
            table.time_partitioning = TimePartitioning(
                type_=TimePartitioningType.DAY,
                field=partition_field,
            )
        if clustering_fields:
            table.clustering_fields = clustering_fields

        client.create_table(table)
        logger.info(f"GSC BQ: created table {table_id}")

    def _get_bq_client(self, target_project: str = "moe-platform-479917"):
        """Resolve BQ credentials from env. Mirrors ghl.py / late.py."""
        import tempfile

        for env_var in (
            "FIREBASE_SERVICE_ACCOUNT_JSON",
            "FIREBASE_CREDENTIALS_JSON",
            "DATAPLANE_BQ_CREDENTIALS_JSON",
            "BIGQUERY_CREDENTIALS_JSON",
        ):
            creds_json = os.environ.get(env_var, "")
            if creds_json:
                logger.debug(f"GSC BQ: using credentials from {env_var}")
                break
        else:
            logger.warning("GSC BQ: no BQ credentials found in env")
            return None

        try:
            from google.cloud import bigquery
            creds_dict = json.loads(creds_json)
            tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
            json.dump(creds_dict, tmp)
            tmp.close()
            client = bigquery.Client.from_service_account_json(
                tmp.name, project=target_project,
            )
            os.unlink(tmp.name)
            return client
        except Exception as e:
            logger.warning(f"GSC BQ client init failed: {e}")
            return None
