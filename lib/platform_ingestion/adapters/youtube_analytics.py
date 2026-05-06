"""YouTube Analytics adapter — channel-owner aggregated metrics, zero PII.

Pulls daily channel KPIs (views, watch time, subscribers, likes, comments,
shares, average view duration / percentage), per-video views/watch-time,
traffic source mix, and top-country views from the YouTube Analytics
Reports API.

The Reports API only returns aggregate counters for the channel owner —
no commenter usernames, no per-viewer fingerprints, no liker identities.
We still enforce a strict ``_SAFE_FIELDS`` allowlist to keep the surface
explicit and protect against future schema additions.

Auth flow mirrors ``google_ads.py``: OAuth2 refresh-token → access-token
exchange against ``oauth2.googleapis.com``. The required scope is
``https://www.googleapis.com/auth/yt-analytics.readonly``; the refresh
token must have been minted with that scope upstream.

API references:
    https://developers.google.com/youtube/analytics/reference/reports/query
    https://developers.google.com/youtube/analytics/dimensions
    https://developers.google.com/youtube/analytics/metrics
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
from typing import Any, Dict, List, Optional

from .base import BasePlatformAdapter, DailyMetricRow, PlatformConfig

logger = logging.getLogger(__name__)

YT_REPORTS_URL = "https://youtubeanalytics.googleapis.com/v2/reports"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
_YT_UA = "MH1-BrightMatter/1.0"

# ── PII allowlists ────────────────────────────────────────────────
# YouTube Analytics already returns no user-level data when the channel
# owner is the requesting principal. The allowlist below covers every
# dimension/metric value we ever expose downstream.
_SAFE_DIMENSIONS = {
    "day", "video", "insightTrafficSourceType", "country",
}
_SAFE_METRICS = {
    "views", "estimatedMinutesWatched", "averageViewDuration",
    "averageViewPercentage", "subscribersGained", "subscribersLost",
    "likes", "comments", "shares",
}


def _strip_to_safe_row(
    row: List[Any], headers: List[str],
) -> Optional[Dict[str, Any]]:
    """Map a result row onto its column headers, dropping anything that
    isn't in the allowlist. Returns None if the row is empty."""
    if not row or not headers:
        return None
    out: Dict[str, Any] = {}
    for header, value in zip(headers, row):
        if header in _SAFE_DIMENSIONS or header in _SAFE_METRICS:
            out[header] = value
    return out or None


def _yt_get(
    params: Dict[str, Any], access_token: str,
) -> Dict[str, Any]:
    qs = urllib.parse.urlencode(
        {k: v for k, v in params.items() if v not in (None, "")}
    )
    url = f"{YT_REPORTS_URL}?{qs}"
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "User-Agent": _YT_UA,
        },
    )
    resp = urllib.request.urlopen(req, timeout=120)
    return json.loads(resp.read())


def _exchange_refresh_token(
    client_id: str, client_secret: str, refresh_token: str,
) -> str:
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


class YouTubeAnalyticsAdapter(BasePlatformAdapter):
    """YouTube Analytics adapter — channel-owner daily metrics, zero PII."""

    PLATFORM = "youtube_analytics"
    RATE_LIMIT_DELAY = 0.5
    MAX_LOOKBACK_DAYS = 730  # Practical retention for the Reports API.

    # ── Entry point ──────────────────────────────────────────────

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
            "YT_REFRESH_TOKEN", ""
        ) or os.environ.get("GA4_REFRESH_TOKEN", "")
        channel_id = (
            config.account_id
            or creds.get("channel_id")
            or config.extra.get("channel_id")
            or ""
        )
        if not (client_id and client_secret and refresh_token and channel_id):
            logger.warning(
                f"YouTube: missing OAuth credentials or channel_id for "
                f"{config.client_name}"
            )
            return []

        try:
            access_token = _exchange_refresh_token(
                client_id, client_secret, refresh_token,
            )
        except Exception as e:
            logger.error(
                f"YouTube OAuth refresh failed for {config.client_name}: {e}"
            )
            return []

        ids = f"channel=={channel_id}"
        start_iso = start_date.isoformat()
        end_iso = end_date.isoformat()
        ingestion_type = "daily" if start_date == end_date else "backfill"
        ingested_at = datetime.now(timezone.utc).isoformat()

        memory_rows: List[DailyMetricRow] = []
        bq_rows: List[Dict[str, Any]] = []

        # 1. Channel-level daily totals — one row per day, no breakdown.
        try:
            channel_rows = self._query_report(
                access_token,
                params={
                    "ids": ids,
                    "startDate": start_iso,
                    "endDate": end_iso,
                    "metrics": (
                        "views,estimatedMinutesWatched,subscribersGained,"
                        "subscribersLost,likes,comments,shares,"
                        "averageViewDuration,averageViewPercentage"
                    ),
                    "dimensions": "day",
                    "sort": "day",
                },
            )
        except urllib.error.HTTPError as e:
            channel_rows = []
            self._log_http_error("channel daily", e)
        except Exception as e:
            channel_rows = []
            logger.error(f"YouTube channel daily failed: {e}")

        for r in channel_rows:
            d = self._parse_date(r.get("day"))
            if not d:
                continue
            metrics = self._channel_metrics(r)
            if not any(metrics.values()):
                continue

            memory_rows.append(DailyMetricRow(
                metric_date=d,
                metrics={**metrics, "channel_id": channel_id},
                record_count=int(metrics.get("views", 0) or 0),
            ))
            bq_rows.append({
                "metric_date": d.isoformat(),
                "channel_id": channel_id,
                "breakdown": "channel_total",
                "dimension_value": "",
                **metrics,
                "ingestion_type": ingestion_type,
                "ingested_at": ingested_at,
            })
            time.sleep(0)  # cheap loop; rate limit applied at API call boundary.
        time.sleep(self.RATE_LIMIT_DELAY)

        # 2. Top videos per day. The Reports API ranks across the window;
        #    we pull day × video to keep day attribution accurate, then cap
        #    to 25 videos per day in-process.
        top_videos_per_day = int(config.extra.get("top_videos_per_day", 25))
        try:
            video_rows = self._query_report(
                access_token,
                params={
                    "ids": ids,
                    "startDate": start_iso,
                    "endDate": end_iso,
                    "metrics": "views,estimatedMinutesWatched",
                    "dimensions": "day,video",
                    "maxResults": 200,
                    "sort": "-views",
                },
            )
        except urllib.error.HTTPError as e:
            video_rows = []
            self._log_http_error("top videos", e)
        except Exception as e:
            video_rows = []
            logger.error(f"YouTube top videos failed: {e}")

        # Keep only the top N videos per day, ranked by views.
        videos_by_day: Dict[date, List[Dict[str, Any]]] = {}
        for r in video_rows:
            d = self._parse_date(r.get("day"))
            if not d:
                continue
            videos_by_day.setdefault(d, []).append(r)

        for d, group in videos_by_day.items():
            group.sort(key=lambda r: self._safe_int(r.get("views")), reverse=True)
            for r in group[:top_videos_per_day]:
                video_id = str(r.get("video", "") or "")
                metrics = {
                    "views": self._safe_int(r.get("views")),
                    "watch_time_minutes": self._safe_float(
                        r.get("estimatedMinutesWatched")
                    ),
                }
                if not video_id or not any(metrics.values()):
                    continue
                memory_rows.append(DailyMetricRow(
                    metric_date=d,
                    metrics={**metrics, "channel_id": channel_id, "video_id": video_id},
                    record_count=metrics["views"],
                    breakdown="video_id",
                ))
                bq_rows.append({
                    "metric_date": d.isoformat(),
                    "channel_id": channel_id,
                    "breakdown": "video_id",
                    "dimension_value": video_id,
                    "views": metrics["views"],
                    "estimatedMinutesWatched": metrics["watch_time_minutes"],
                    "ingestion_type": ingestion_type,
                    "ingested_at": ingested_at,
                })
        time.sleep(self.RATE_LIMIT_DELAY)

        # 3. Traffic source breakdown per day.
        try:
            traffic_rows = self._query_report(
                access_token,
                params={
                    "ids": ids,
                    "startDate": start_iso,
                    "endDate": end_iso,
                    "metrics": "views",
                    "dimensions": "day,insightTrafficSourceType",
                    "maxResults": 200,
                    "sort": "day",
                },
            )
        except urllib.error.HTTPError as e:
            traffic_rows = []
            self._log_http_error("traffic source", e)
        except Exception as e:
            traffic_rows = []
            logger.error(f"YouTube traffic source failed: {e}")

        for r in traffic_rows:
            d = self._parse_date(r.get("day"))
            source = str(r.get("insightTrafficSourceType", "") or "").lower()
            views = self._safe_int(r.get("views"))
            if not d or not source or views <= 0:
                continue
            memory_rows.append(DailyMetricRow(
                metric_date=d,
                metrics={"views": views, "channel_id": channel_id, "traffic_source": source},
                record_count=views,
                breakdown="traffic_source",
            ))
            bq_rows.append({
                "metric_date": d.isoformat(),
                "channel_id": channel_id,
                "breakdown": "traffic_source",
                "dimension_value": source,
                "views": views,
                "ingestion_type": ingestion_type,
                "ingested_at": ingested_at,
            })
        time.sleep(self.RATE_LIMIT_DELAY)

        # 4. Top countries per day (cap at 10/day).
        top_countries_per_day = int(config.extra.get("top_countries_per_day", 10))
        try:
            country_rows = self._query_report(
                access_token,
                params={
                    "ids": ids,
                    "startDate": start_iso,
                    "endDate": end_iso,
                    "metrics": "views",
                    "dimensions": "day,country",
                    "maxResults": 200,
                    "sort": "-views",
                },
            )
        except urllib.error.HTTPError as e:
            country_rows = []
            self._log_http_error("country", e)
        except Exception as e:
            country_rows = []
            logger.error(f"YouTube country breakdown failed: {e}")

        countries_by_day: Dict[date, List[Dict[str, Any]]] = {}
        for r in country_rows:
            d = self._parse_date(r.get("day"))
            if not d:
                continue
            countries_by_day.setdefault(d, []).append(r)

        for d, group in countries_by_day.items():
            group.sort(key=lambda r: self._safe_int(r.get("views")), reverse=True)
            for r in group[:top_countries_per_day]:
                country = str(r.get("country", "") or "").upper()
                views = self._safe_int(r.get("views"))
                if not country or views <= 0:
                    continue
                memory_rows.append(DailyMetricRow(
                    metric_date=d,
                    metrics={"views": views, "channel_id": channel_id, "country": country},
                    record_count=views,
                    breakdown="country",
                ))
                bq_rows.append({
                    "metric_date": d.isoformat(),
                    "channel_id": channel_id,
                    "breakdown": "country",
                    "dimension_value": country,
                    "views": views,
                    "ingestion_type": ingestion_type,
                    "ingested_at": ingested_at,
                })

        if bq_rows:
            self._write_to_bq(config, bq_rows)

        logger.info(
            f"YouTube: {len(memory_rows)} memory rows, {len(bq_rows)} BQ rows "
            f"for {config.client_name} (channel {channel_id[:12]})"
        )
        return memory_rows

    # ── Report query helper ──────────────────────────────────────

    def _query_report(
        self,
        access_token: str,
        params: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """Issue a Reports.query call and project rows onto a safe dict shape."""
        for dim in (params.get("dimensions") or "").split(","):
            dim = dim.strip()
            if dim and dim not in _SAFE_DIMENSIONS:
                raise ValueError(f"YouTube: dimension '{dim}' not in allowlist")
        for metric in (params.get("metrics") or "").split(","):
            metric = metric.strip()
            if metric and metric not in _SAFE_METRICS:
                raise ValueError(f"YouTube: metric '{metric}' not in allowlist")

        data = _yt_get(params, access_token)
        headers = [c.get("name", "") for c in (data.get("columnHeaders") or [])]
        out: List[Dict[str, Any]] = []
        for row in (data.get("rows") or []):
            safe = _strip_to_safe_row(row, headers)
            if safe:
                out.append(safe)
        return out

    # ── Metric extraction ────────────────────────────────────────

    def _channel_metrics(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "views": self._safe_int(row.get("views")),
            "watch_time_minutes": self._safe_float(
                row.get("estimatedMinutesWatched")
            ),
            "subscribers_gained": self._safe_int(row.get("subscribersGained")),
            "subscribers_lost": self._safe_int(row.get("subscribersLost")),
            "likes": self._safe_int(row.get("likes")),
            "comments": self._safe_int(row.get("comments")),
            "shares": self._safe_int(row.get("shares")),
            "average_view_duration": self._safe_float(
                row.get("averageViewDuration")
            ),
            "average_view_percentage": self._safe_float(
                row.get("averageViewPercentage")
            ),
        }

    @staticmethod
    def _parse_date(value: Any) -> Optional[date]:
        if not value or not isinstance(value, str):
            return None
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None

    def _log_http_error(self, label: str, e: urllib.error.HTTPError) -> None:
        try:
            body = e.read().decode()[:300]
        except Exception:
            body = ""
        logger.error(f"YouTube {label}: HTTP {e.code} — {body}")

    # ── BigQuery sink ────────────────────────────────────────────

    def _write_to_bq(self, config: PlatformConfig, rows: List[Dict[str, Any]]) -> None:
        bq_dataset = config.extra.get("bq_dataset", "")
        if not bq_dataset:
            logger.info(
                "YouTube BQ: no bq_dataset in config, skipping BQ write"
            )
            return

        table = config.extra.get("bq_table", "youtube_daily_metrics")
        target_project = config.extra.get("bq_project", "moe-platform-479917")

        client = self._get_bq_client(target_project=target_project)
        if not client:
            return

        from google.cloud.bigquery import (
            LoadJobConfig, SchemaField, WriteDisposition,
        )

        schema = [
            SchemaField("metric_date", "DATE"),
            SchemaField("channel_id", "STRING"),
            SchemaField("breakdown", "STRING"),
            SchemaField("dimension_value", "STRING"),
            SchemaField("views", "INTEGER"),
            SchemaField("watch_time_minutes", "FLOAT"),
            SchemaField("estimatedMinutesWatched", "FLOAT"),
            SchemaField("subscribers_gained", "INTEGER"),
            SchemaField("subscribers_lost", "INTEGER"),
            SchemaField("likes", "INTEGER"),
            SchemaField("comments", "INTEGER"),
            SchemaField("shares", "INTEGER"),
            SchemaField("average_view_duration", "FLOAT"),
            SchemaField("average_view_percentage", "FLOAT"),
            SchemaField("ingestion_type", "STRING"),
            SchemaField("ingested_at", "TIMESTAMP"),
        ]

        # Normalise rows to the full schema (BQ load tolerates missing keys
        # only when the schema matches).
        all_keys = {f.name for f in schema}
        normalised: List[Dict[str, Any]] = []
        for r in rows:
            row = {k: r.get(k) for k in all_keys}
            normalised.append(row)

        table_id = f"{target_project}.{bq_dataset}.{table}"
        staging_id = f"{target_project}.{bq_dataset}._staging_{table}"

        try:
            self._ensure_table(
                client, table_id, schema,
                partition_field="metric_date",
                clustering_fields=["channel_id", "breakdown"],
            )

            job = client.load_table_from_json(
                normalised,
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
               AND T.channel_id = S.channel_id
               AND T.breakdown = S.breakdown
               AND T.dimension_value = S.dimension_value
            WHEN MATCHED THEN UPDATE SET
                views = S.views,
                watch_time_minutes = S.watch_time_minutes,
                estimatedMinutesWatched = S.estimatedMinutesWatched,
                subscribers_gained = S.subscribers_gained,
                subscribers_lost = S.subscribers_lost,
                likes = S.likes,
                comments = S.comments,
                shares = S.shares,
                average_view_duration = S.average_view_duration,
                average_view_percentage = S.average_view_percentage,
                ingestion_type = S.ingestion_type,
                ingested_at = S.ingested_at
            WHEN NOT MATCHED THEN INSERT ROW
            """
            merge_job = client.query(merge_sql)
            merge_job.result()
            affected = merge_job.num_dml_affected_rows or len(normalised)

            client.delete_table(staging_id, not_found_ok=True)

            logger.info(
                f"YouTube BQ: upserted {affected} rows to {table_id} "
                f"({len(normalised)} staged)"
            )
        except Exception as e:
            logger.warning(f"YouTube BQ write failed (non-fatal): {e}")

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
        logger.info(f"YouTube BQ: created table {table_id}")

    def _get_bq_client(self, target_project: str = "moe-platform-479917"):
        import tempfile

        for env_var in (
            "FIREBASE_SERVICE_ACCOUNT_JSON",
            "FIREBASE_CREDENTIALS_JSON",
            "DATAPLANE_BQ_CREDENTIALS_JSON",
            "BIGQUERY_CREDENTIALS_JSON",
        ):
            creds_json = os.environ.get(env_var, "")
            if creds_json:
                logger.debug(f"YouTube BQ: using credentials from {env_var}")
                break
        else:
            logger.warning("YouTube BQ: no BQ credentials found in env")
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
            logger.warning(f"YouTube BQ client init failed: {e}")
            return None
