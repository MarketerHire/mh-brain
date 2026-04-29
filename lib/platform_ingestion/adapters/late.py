"""Late (Zernio) adapter — organic social daily metrics + follower stats.

Late (zernio.com, formerly getlate.dev) is a social publishing + analytics
platform. Each MH1 client has a `lateProfileId` that scopes the data.
Accounts hang off the profile (Instagram, Facebook, TikTok, LinkedIn, …).

What this adapter pulls (daily cron):
    - /v1/analytics/daily-metrics  (engagement per day, per platform)
    - /v1/accounts/follower-stats  (follower time-series per account)
    - /v1/accounts                 (account metadata: handle, platform, follower count)

No PII. Every row is an aggregated count (posts, likes, comments, shares,
impressions, reach, saves, clicks, views, followers). No user identifiers
beyond the account handle the client owns.

BQ output (two tables per client under `config.extra["bq_dataset"]`):
    - late_daily_metrics    (per metric_date × platform × account_id)
    - late_follower_stats   (per metric_date × account_id)

Schema is created if missing. UPSERT via staging table + MERGE.
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from .base import BasePlatformAdapter, DailyMetricRow, PlatformConfig

logger = logging.getLogger(__name__)

# Using the stable base URL that the production dashboards already use.
# zernio.com is the newer brand; getlate.dev continues to serve the same API.
LATE_BASE = "https://getlate.dev/api"
_LATE_UA = "MH1-BrightMatter/1.0"


def _late_get(path: str, params: Dict[str, Any], token: str) -> Dict[str, Any]:
    """GET helper for the Late API. Returns parsed JSON or raises."""
    qs = urllib.parse.urlencode({k: v for k, v in params.items() if v not in (None, "")})
    url = f"{LATE_BASE}{path}?{qs}" if qs else f"{LATE_BASE}{path}"
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": _LATE_UA,
        },
    )
    resp = urllib.request.urlopen(req, timeout=60)
    return json.loads(resp.read())


class LateAdapter(BasePlatformAdapter):
    """Late/Zernio organic social adapter.

    Pulls daily engagement metrics + follower stats for every connected
    account under the client's `profile_id`. Writes two tables to BQ.
    """

    PLATFORM = "late"
    RATE_LIMIT_DELAY = 0.2
    MAX_LOOKBACK_DAYS = 730  # Late retains ~2 years of analytics.

    # ── Entry point ───────────────────────────────────────────────

    def pull_daily_metrics(
        self,
        config: PlatformConfig,
        start_date: date,
        end_date: date,
    ) -> List[DailyMetricRow]:
        token, profile_id = self._resolve_auth(config)
        if not token or not profile_id:
            logger.warning(
                f"Late: missing LATE_API_KEY or profile_id for {config.client_name}"
            )
            return []

        from_iso = start_date.isoformat()
        to_iso = end_date.isoformat()

        # 1. Enumerate connected accounts (platform, id, handle, follower count).
        accounts = self._fetch_accounts(profile_id, token)
        if not accounts:
            logger.warning(
                f"Late: no connected accounts for {config.client_name} "
                f"(profile {profile_id[:8]})"
            )
            return []

        # 2. Daily engagement metrics: pull one API call per platform, because
        #    the endpoint aggregates across all accounts on that platform.
        daily_rows: List[Dict[str, Any]] = []
        memory_rows: List[DailyMetricRow] = []

        platforms_seen = {a["platform"] for a in accounts}
        for platform in sorted(platforms_seen):
            platform_accounts = [a for a in accounts if a["platform"] == platform]
            # If there are multiple accounts per platform we attribute the
            # aggregated platform metrics to a synthetic "platform-level"
            # breakdown (account_id = "") and also write per-account follower
            # stats separately.
            try:
                data = _late_get(
                    "/v1/analytics/daily-metrics",
                    {
                        "profileId": profile_id,
                        "fromDate": from_iso,
                        "toDate": to_iso,
                        "platform": platform,
                    },
                    token,
                )
            except urllib.error.HTTPError as e:
                logger.warning(
                    f"Late daily-metrics {platform}: HTTP {e.code} — {e.reason}"
                )
                continue
            except Exception as e:
                logger.warning(f"Late daily-metrics {platform}: {e}")
                continue

            by_day = self._parse_daily_metrics(data, platform)
            for d, metrics in sorted(by_day.items()):
                if not any(v for v in metrics.values() if isinstance(v, (int, float))):
                    continue

                # Per-account emission: attribute platform metrics to each
                # account on that platform. Usually there's exactly one.
                for acct in platform_accounts:
                    row = {
                        "metric_date": d,
                        "platform": platform,
                        "account_id": acct["id"],
                        "account_handle": acct["handle"],
                        "post_count": metrics["post_count"],
                        "impressions": metrics["impressions"],
                        "reach": metrics["reach"],
                        "likes": metrics["likes"],
                        "comments": metrics["comments"],
                        "shares": metrics["shares"],
                        "saves": metrics["saves"],
                        "clicks": metrics["clicks"],
                        "views": metrics["views"],
                        "ingestion_type": "daily" if start_date == end_date else "backfill",
                        "ingested_at": datetime.now(timezone.utc).isoformat(),
                    }
                    daily_rows.append(row)

                    # Emit one DailyMetricRow per (day, platform, account) for
                    # BrightMatter's episodic memory pipeline.
                    memory_rows.append(DailyMetricRow(
                        metric_date=date.fromisoformat(d),
                        metrics={
                            **metrics,
                            "platform": platform,
                            "account_id": acct["id"],
                            "account_handle": acct["handle"],
                            "current_followers": acct["current_followers"],
                        },
                        record_count=metrics["post_count"],
                        breakdown=f"{platform}:{acct['handle']}",
                    ))

            time.sleep(self.RATE_LIMIT_DELAY)

        # 3. Follower stats — one call covers all accounts.
        follower_rows: List[Dict[str, Any]] = []
        try:
            fdata = _late_get(
                "/v1/accounts/follower-stats",
                {
                    "profileId": profile_id,
                    "fromDate": from_iso,
                    "toDate": to_iso,
                },
                token,
            )
            follower_rows = self._parse_follower_stats(
                fdata, accounts, start_date, end_date,
            )
        except urllib.error.HTTPError as e:
            logger.warning(
                f"Late follower-stats: HTTP {e.code} — {e.reason}"
            )
        except Exception as e:
            logger.warning(f"Late follower-stats: {e}")

        # 4. Write to BigQuery (two tables).
        if daily_rows:
            self._write_daily_metrics_to_bq(config, daily_rows)
        if follower_rows:
            self._write_follower_stats_to_bq(config, follower_rows)

        logger.info(
            f"Late: {len(memory_rows)} memory rows, "
            f"{len(daily_rows)} daily_metrics rows, "
            f"{len(follower_rows)} follower_stats rows "
            f"across {len(accounts)} accounts for {config.client_name}"
        )
        return memory_rows

    # ── Auth resolution ───────────────────────────────────────────

    def _resolve_auth(self, config: PlatformConfig) -> Tuple[str, str]:
        """Resolve (api_key, profile_id).

        - LATE_API_KEY comes from env (mh1-shared-tools secret). Adapters may
          also accept a credential override for future multi-tenant keys.
        - profile_id comes from config.extra["profile_id"] (populated by
          config_resolver from datasources.json).
        """
        token = (
            config.credentials.get("api_key")
            or os.environ.get("LATE_API_KEY", "")
        ).strip()

        profile_id = (
            config.extra.get("profile_id")
            or config.extra.get("profileId")
            or config.extra.get("lateProfileId")
            or config.account_id
            or ""
        ).strip()

        return token, profile_id

    # ── Accounts ──────────────────────────────────────────────────

    def _fetch_accounts(self, profile_id: str, token: str) -> List[Dict[str, Any]]:
        """Return [{id, platform, handle, current_followers}, ...]."""
        try:
            data = _late_get(
                "/v1/accounts",
                {"profileId": profile_id},
                token,
            )
        except Exception as e:
            logger.warning(f"Late accounts fetch: {e}")
            return []

        raw = data.get("accounts", []) if isinstance(data, dict) else []
        accounts: List[Dict[str, Any]] = []
        for a in raw:
            acct_id = a.get("_id") or a.get("id") or ""
            platform = (a.get("platform") or "").lower()
            handle = a.get("username") or a.get("displayName") or ""
            followers = self._safe_int(
                a.get("currentFollowers")
                or a.get("followers")
                or 0
            )
            if acct_id and platform:
                accounts.append({
                    "id": acct_id,
                    "platform": platform,
                    "handle": handle,
                    "current_followers": followers,
                })
        return accounts

    # ── Parse daily metrics ───────────────────────────────────────

    def _parse_daily_metrics(
        self, data: Dict[str, Any], platform: str,
    ) -> Dict[str, Dict[str, Any]]:
        """Transform Late response into {iso_date: metrics_dict}."""
        result: Dict[str, Dict[str, Any]] = defaultdict(self._empty_metrics)

        for row in (data.get("dailyData") or []):
            d = (row.get("date") or "")[:10]
            if not d:
                continue
            m = row.get("metrics") or {}
            r = result[d]

            # Use the platform-specific post count when available, otherwise
            # fall back to overall postCount.
            plat_post_count = 0
            platforms = row.get("platforms") or {}
            if isinstance(platforms, dict) and platform in platforms:
                plat_post_count = self._safe_int(platforms[platform])
            else:
                plat_post_count = self._safe_int(row.get("postCount"))

            r["post_count"] += plat_post_count
            r["impressions"] += self._safe_int(m.get("impressions"))
            r["reach"] += self._safe_int(m.get("reach"))
            r["likes"] += self._safe_int(m.get("likes"))
            r["comments"] += self._safe_int(m.get("comments"))
            r["shares"] += self._safe_int(m.get("shares"))
            r["saves"] += self._safe_int(m.get("saves"))
            r["clicks"] += self._safe_int(m.get("clicks"))
            r["views"] += self._safe_int(m.get("views"))

        return result

    @staticmethod
    def _empty_metrics() -> Dict[str, int]:
        return {
            "post_count": 0,
            "impressions": 0,
            "reach": 0,
            "likes": 0,
            "comments": 0,
            "shares": 0,
            "saves": 0,
            "clicks": 0,
            "views": 0,
        }

    # ── Parse follower stats ──────────────────────────────────────

    def _parse_follower_stats(
        self,
        data: Dict[str, Any],
        accounts: List[Dict[str, Any]],
        start_date: date,
        end_date: date,
    ) -> List[Dict[str, Any]]:
        """Flatten the {account_id: [{date, followers}, ...]} shape into rows."""
        stats = (data.get("stats") or {}) if isinstance(data, dict) else {}
        by_id = {a["id"]: a for a in accounts}
        start_iso = start_date.isoformat()
        end_iso = end_date.isoformat()
        ingestion_type = "daily" if start_date == end_date else "backfill"
        ingested_at = datetime.now(timezone.utc).isoformat()

        rows: List[Dict[str, Any]] = []
        for acct_id, series in stats.items():
            acct = by_id.get(acct_id)
            if not acct or not isinstance(series, list):
                continue

            for point in series:
                d = (point.get("date") or "")[:10]
                if not d or d < start_iso or d > end_iso:
                    continue
                rows.append({
                    "metric_date": d,
                    "account_id": acct_id,
                    "platform": acct["platform"],
                    "account_handle": acct["handle"],
                    "followers": self._safe_int(point.get("followers")),
                    "ingestion_type": ingestion_type,
                    "ingested_at": ingested_at,
                })
        return rows

    # ── BigQuery: daily_metrics table ─────────────────────────────

    def _write_daily_metrics_to_bq(
        self, config: PlatformConfig, rows: List[Dict[str, Any]],
    ) -> None:
        bq_dataset = config.extra.get("bq_dataset", "")
        if not bq_dataset:
            logger.info("Late BQ: no bq_dataset in config, skipping BQ write")
            return

        table = config.extra.get("bq_daily_metrics_table", "late_daily_metrics")
        target_project = config.extra.get("bq_project", "moe-platform-479917")

        client = self._get_bq_client(target_project=target_project)
        if not client:
            return

        from google.cloud.bigquery import (
            LoadJobConfig, SchemaField, WriteDisposition, Table, TimePartitioning,
            TimePartitioningType,
        )

        schema = [
            SchemaField("metric_date", "DATE"),
            SchemaField("platform", "STRING"),
            SchemaField("account_id", "STRING"),
            SchemaField("account_handle", "STRING"),
            SchemaField("post_count", "INTEGER"),
            SchemaField("impressions", "INTEGER"),
            SchemaField("reach", "INTEGER"),
            SchemaField("likes", "INTEGER"),
            SchemaField("comments", "INTEGER"),
            SchemaField("shares", "INTEGER"),
            SchemaField("saves", "INTEGER"),
            SchemaField("clicks", "INTEGER"),
            SchemaField("views", "INTEGER"),
            SchemaField("ingestion_type", "STRING"),
            SchemaField("ingested_at", "TIMESTAMP"),
        ]

        table_id = f"{target_project}.{bq_dataset}.{table}"
        staging_id = f"{target_project}.{bq_dataset}._staging_{table}"

        try:
            self._ensure_table(
                client, table_id, schema,
                partition_field="metric_date",
                clustering_fields=["platform", "account_id"],
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
               AND T.platform = S.platform
               AND T.account_id = S.account_id
            WHEN MATCHED THEN UPDATE SET
                account_handle = S.account_handle,
                post_count = S.post_count,
                impressions = S.impressions,
                reach = S.reach,
                likes = S.likes,
                comments = S.comments,
                shares = S.shares,
                saves = S.saves,
                clicks = S.clicks,
                views = S.views,
                ingestion_type = S.ingestion_type,
                ingested_at = S.ingested_at
            WHEN NOT MATCHED THEN INSERT ROW
            """
            merge_job = client.query(merge_sql)
            merge_job.result()
            affected = merge_job.num_dml_affected_rows or len(rows)

            client.delete_table(staging_id, not_found_ok=True)

            logger.info(
                f"Late BQ: upserted {affected} rows to {table_id} "
                f"({len(rows)} staged)"
            )
        except Exception as e:
            logger.warning(f"Late BQ daily_metrics write failed (non-fatal): {e}")

    # ── BigQuery: follower_stats table ────────────────────────────

    def _write_follower_stats_to_bq(
        self, config: PlatformConfig, rows: List[Dict[str, Any]],
    ) -> None:
        bq_dataset = config.extra.get("bq_dataset", "")
        if not bq_dataset:
            return

        table = config.extra.get("bq_follower_stats_table", "late_follower_stats")
        target_project = config.extra.get("bq_project", "moe-platform-479917")

        client = self._get_bq_client(target_project=target_project)
        if not client:
            return

        from google.cloud.bigquery import (
            LoadJobConfig, SchemaField, WriteDisposition,
        )

        schema = [
            SchemaField("metric_date", "DATE"),
            SchemaField("account_id", "STRING"),
            SchemaField("platform", "STRING"),
            SchemaField("account_handle", "STRING"),
            SchemaField("followers", "INTEGER"),
            SchemaField("ingestion_type", "STRING"),
            SchemaField("ingested_at", "TIMESTAMP"),
        ]

        table_id = f"{target_project}.{bq_dataset}.{table}"
        staging_id = f"{target_project}.{bq_dataset}._staging_{table}"

        try:
            self._ensure_table(
                client, table_id, schema,
                partition_field="metric_date",
                clustering_fields=["platform", "account_id"],
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
               AND T.account_id = S.account_id
            WHEN MATCHED THEN UPDATE SET
                platform = S.platform,
                account_handle = S.account_handle,
                followers = S.followers,
                ingestion_type = S.ingestion_type,
                ingested_at = S.ingested_at
            WHEN NOT MATCHED THEN INSERT ROW
            """
            merge_job = client.query(merge_sql)
            merge_job.result()
            affected = merge_job.num_dml_affected_rows or len(rows)

            client.delete_table(staging_id, not_found_ok=True)

            logger.info(
                f"Late BQ: upserted {affected} rows to {table_id} "
                f"({len(rows)} staged)"
            )
        except Exception as e:
            logger.warning(f"Late BQ follower_stats write failed (non-fatal): {e}")

    # ── BigQuery helpers ──────────────────────────────────────────

    def _ensure_table(
        self,
        client,
        table_id: str,
        schema,
        partition_field: Optional[str] = None,
        clustering_fields: Optional[List[str]] = None,
    ) -> None:
        """Create the table if it doesn't exist. Dataset must already exist."""
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
        logger.info(f"Late BQ: created table {table_id}")

    def _get_bq_client(self, target_project: str = "moe-platform-479917"):
        """Resolve BQ credentials from env, preferring the data-plane SA."""
        import tempfile

        for env_var in (
            "FIREBASE_SERVICE_ACCOUNT_JSON",
            "FIREBASE_CREDENTIALS_JSON",
            "DATAPLANE_BQ_CREDENTIALS_JSON",
            "BIGQUERY_CREDENTIALS_JSON",
        ):
            creds_json = os.environ.get(env_var, "")
            if creds_json:
                logger.debug(f"Late BQ: using credentials from {env_var}")
                break
        else:
            logger.warning("Late BQ: no BQ credentials found in env")
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
            logger.warning(f"Late BQ client init failed: {e}")
            return None
