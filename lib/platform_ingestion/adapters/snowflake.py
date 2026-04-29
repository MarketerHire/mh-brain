"""Snowflake adapter — aggregated daily marketplace metrics, zero PII.

PII Protection:
    Snowflake tables contain record-level data (user IDs, emails, etc.).
    This adapter runs only pre-defined aggregation queries server-side
    in Snowflake (GROUP BY DATE). The result set contains daily counts,
    sums, and averages — never individual records, identifiers, or PII.
    No PII is ever held in memory or written to Supabase / BigQuery.

    BQ output schema: daily_metrics (marketplace KPIs per day).

Auth:
    Supports programmatic access token (preferred) and JWT key pair
    (fallback). Credentials come from the client's datasources.json
    warehouse block via config_resolver.py.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from .base import BasePlatformAdapter, DailyMetricRow, PlatformConfig

logger = logging.getLogger(__name__)

_BQ_PROJECT = "moe-platform-479917"

_BOOKINGS_SQL = """
SELECT DATE AS metric_date,
       COUNT(*) AS total_bookings,
       COUNT(CASE WHEN STATUS = 1 THEN 1 END) AS confirmed_bookings,
       COUNT(CASE WHEN STATUS = 3 THEN 1 END) AS completed_bookings,
       COUNT(CASE WHEN STATUS IN (2, 4) THEN 1 END) AS cancelled_bookings,
       COALESCE(SUM(RESERVATION_PRICE_TOTAL), 0) AS gmv,
       COALESCE(SUM(SERVICE_FEE), 0) AS service_fee_revenue,
       COALESCE(SUM(HOST_FEE), 0) AS host_fee_revenue,
       COUNT(DISTINCT POOL_ID) AS unique_pools_booked,
       COALESCE(AVG(ADULT_GUESTS + CHILD_GUESTS), 0) AS avg_group_size
FROM {database}.SWIMPLY_SWIMPLY_PROD.BOOKINGS
WHERE DATE BETWEEN %(start)s AND %(end)s
GROUP BY DATE
ORDER BY DATE
"""

_POOLS_SQL = """
SELECT CREATED_AT::DATE AS metric_date,
       COUNT(*) AS new_listings,
       COUNT(CASE WHEN STATUS = 1 THEN 1 END) AS active_listings,
       COUNT(CASE WHEN INSTANT_BOOK = TRUE THEN 1 END) AS instant_book_listings
FROM {database}.SWIMPLY_SWIMPLY_PROD.POOLS
WHERE CREATED_AT::DATE BETWEEN %(start)s AND %(end)s
GROUP BY metric_date
ORDER BY metric_date
"""

_SEARCH_SQL = """
SELECT TIMESTAMP::DATE AS metric_date,
       COUNT(*) AS searches,
       COUNT(DISTINCT ANONYMOUS_ID) AS unique_searchers
FROM {database}.FACTS.STG_SEARCH_EVENTS_FORMATTED
WHERE TIMESTAMP::DATE BETWEEN %(start)s AND %(end)s
GROUP BY metric_date
ORDER BY metric_date
"""

_BACKFILL_CHUNK_DAYS = 30


class SnowflakeAdapter(BasePlatformAdapter):
    PLATFORM = "snowflake"
    MAX_LOOKBACK_DAYS = 1095

    def pull_daily_metrics(
        self,
        config: PlatformConfig,
        start_date: date,
        end_date: date,
    ) -> List[DailyMetricRow]:
        conn = self._connect(config)
        if not conn:
            return []

        database = config.extra.get("database", "FIVETRAN_DATABASE")
        bq_rows: List[Dict[str, Any]] = []
        all_rows: List[DailyMetricRow] = []

        try:
            day_span = (end_date - start_date).days
            if day_span > _BACKFILL_CHUNK_DAYS:
                chunks = self._chunk_range(start_date, end_date, _BACKFILL_CHUNK_DAYS)
            else:
                chunks = [(start_date, end_date)]

            for chunk_start, chunk_end in chunks:
                day_metrics = self._pull_chunk(
                    conn, database, chunk_start, chunk_end, config.client_name,
                )
                is_backfill = day_span > 1

                for d in sorted(day_metrics.keys()):
                    m = day_metrics[d]
                    if not any(v for k, v in m.items()):
                        continue

                    all_rows.append(DailyMetricRow(
                        metric_date=date.fromisoformat(d),
                        metrics=m,
                        record_count=m.get("total_bookings", 0) + m.get("searches", 0),
                    ))

                    bq_rows.append({
                        "metric_date": d,
                        **m,
                        "ingestion_type": "backfill" if is_backfill else "daily",
                        "ingested_at": datetime.now(timezone.utc).isoformat(),
                    })
        finally:
            try:
                conn.close()
            except Exception:
                pass

        if bq_rows:
            self._write_to_bq(config, bq_rows)

        logger.info(
            f"Snowflake: {len(all_rows)} rows for {config.client_name} "
            f"({start_date} to {end_date})"
        )
        return all_rows

    # ── Snowflake connection ──────────────────────────────────────

    def _connect(self, config: PlatformConfig):
        try:
            import snowflake.connector
        except ImportError:
            logger.error("snowflake-connector-python not installed")
            return None

        creds = config.credentials
        account = creds.get("account", "")
        user = creds.get("user", "")
        token = creds.get("token", "")
        role = creds.get("role", "")
        warehouse = creds.get("warehouse_name", "")
        database = creds.get("database", "")

        if not account or not user:
            logger.warning(f"Snowflake: missing account or user for {config.client_name}")
            return None

        connect_kwargs: Dict[str, Any] = {
            "account": account,
            "user": user,
            "role": role,
            "warehouse": warehouse,
            "database": database,
        }

        if token:
            connect_kwargs["token"] = token
            connect_kwargs["authenticator"] = "oauth"
        else:
            private_key_pem = creds.get("private_key_pem", "")
            if private_key_pem:
                from cryptography.hazmat.primitives import serialization
                p_key = serialization.load_pem_private_key(
                    private_key_pem.encode(), password=None,
                )
                connect_kwargs["private_key"] = p_key.private_bytes(
                    encoding=serialization.Encoding.DER,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption(),
                )
            else:
                logger.warning(f"Snowflake: no token or private key for {config.client_name}")
                return None

        try:
            conn = snowflake.connector.connect(**connect_kwargs)
            logger.info(f"Snowflake: connected to {account} as {user}")
            return conn
        except Exception as e:
            logger.error(f"Snowflake connection failed for {config.client_name}: {e}")
            return None

    # ── Per-chunk pull ────────────────────────────────────────────

    def _pull_chunk(
        self,
        conn,
        database: str,
        start_date: date,
        end_date: date,
        client_name: str,
    ) -> Dict[str, Dict[str, Any]]:
        day_metrics: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
            "total_bookings": 0,
            "confirmed_bookings": 0,
            "completed_bookings": 0,
            "cancelled_bookings": 0,
            "gmv": 0.0,
            "service_fee_revenue": 0.0,
            "host_fee_revenue": 0.0,
            "unique_pools_booked": 0,
            "avg_group_size": 0.0,
            "new_listings": 0,
            "active_listings": 0,
            "instant_book_listings": 0,
            "searches": 0,
            "unique_searchers": 0,
        })

        params = {"start": start_date.isoformat(), "end": end_date.isoformat()}

        self._run_agg_query(
            conn, _BOOKINGS_SQL.format(database=database), params,
            day_metrics, "bookings", client_name,
        )
        self._run_agg_query(
            conn, _POOLS_SQL.format(database=database), params,
            day_metrics, "pools", client_name,
        )
        self._run_agg_query(
            conn, _SEARCH_SQL.format(database=database), params,
            day_metrics, "search", client_name,
        )

        return day_metrics

    def _run_agg_query(
        self,
        conn,
        sql: str,
        params: Dict[str, str],
        day_metrics: Dict[str, Dict[str, Any]],
        source_label: str,
        client_name: str,
    ) -> None:
        try:
            cur = conn.cursor()
            cur.execute(sql, params)
            columns = [desc[0].lower() for desc in cur.description]
            for row in cur:
                row_dict = dict(zip(columns, row))
                md = row_dict.pop("metric_date", None)
                if md is None:
                    continue
                if isinstance(md, date):
                    md = md.isoformat()
                else:
                    md = str(md)[:10]

                for k, v in row_dict.items():
                    if v is not None:
                        day_metrics[md][k] = self._safe_float(v)

            cur.close()
            logger.debug(f"Snowflake {source_label}: query ok for {client_name}")
        except Exception as e:
            logger.warning(f"Snowflake {source_label} query failed for {client_name}: {e}")

    # ── Chunking helper ───────────────────────────────────────────

    @staticmethod
    def _chunk_range(
        start: date, end: date, chunk_days: int,
    ) -> List[tuple]:
        chunks = []
        current = start
        while current <= end:
            chunk_end = min(current + timedelta(days=chunk_days - 1), end)
            chunks.append((current, chunk_end))
            current = chunk_end + timedelta(days=1)
        return chunks

    # ── BigQuery sink (aggregated counts only) ────────────────────

    def _write_to_bq(self, config: PlatformConfig, rows: List[Dict[str, Any]]) -> None:
        bq_dataset = config.extra.get("bq_dataset", "")
        if not bq_dataset:
            logger.info("Snowflake BQ: no bq_dataset in config, skipping BQ write")
            return

        try:
            client = self._get_bq_client()
            if not client:
                return

            table_id = f"{_BQ_PROJECT}.{bq_dataset}.daily_metrics"
            staging_id = f"{_BQ_PROJECT}.{bq_dataset}._staging_daily_metrics"

            from google.cloud.bigquery import (
                LoadJobConfig, SchemaField, WriteDisposition,
            )

            schema = [
                SchemaField("metric_date", "DATE"),
                SchemaField("total_bookings", "INTEGER"),
                SchemaField("confirmed_bookings", "INTEGER"),
                SchemaField("completed_bookings", "INTEGER"),
                SchemaField("cancelled_bookings", "INTEGER"),
                SchemaField("gmv", "FLOAT"),
                SchemaField("service_fee_revenue", "FLOAT"),
                SchemaField("host_fee_revenue", "FLOAT"),
                SchemaField("unique_pools_booked", "INTEGER"),
                SchemaField("avg_group_size", "FLOAT"),
                SchemaField("new_listings", "INTEGER"),
                SchemaField("active_listings", "INTEGER"),
                SchemaField("instant_book_listings", "INTEGER"),
                SchemaField("searches", "INTEGER"),
                SchemaField("unique_searchers", "INTEGER"),
                SchemaField("ingestion_type", "STRING"),
                SchemaField("ingested_at", "TIMESTAMP"),
            ]

            int_fields = {
                "total_bookings", "confirmed_bookings", "completed_bookings",
                "cancelled_bookings", "unique_pools_booked", "new_listings",
                "active_listings", "instant_book_listings", "searches",
                "unique_searchers",
            }
            for r in rows:
                for f in int_fields:
                    if f in r:
                        r[f] = int(r[f])

            job_config = LoadJobConfig(
                schema=schema,
                write_disposition=WriteDisposition.WRITE_TRUNCATE,
            )
            job = client.load_table_from_json(rows, staging_id, job_config=job_config)
            job.result()

            merge_sql = f"""
            MERGE `{table_id}` T
            USING `{staging_id}` S
            ON T.metric_date = S.metric_date
            WHEN MATCHED THEN UPDATE SET
                total_bookings = S.total_bookings,
                confirmed_bookings = S.confirmed_bookings,
                completed_bookings = S.completed_bookings,
                cancelled_bookings = S.cancelled_bookings,
                gmv = S.gmv,
                service_fee_revenue = S.service_fee_revenue,
                host_fee_revenue = S.host_fee_revenue,
                unique_pools_booked = S.unique_pools_booked,
                avg_group_size = S.avg_group_size,
                new_listings = S.new_listings,
                active_listings = S.active_listings,
                instant_book_listings = S.instant_book_listings,
                searches = S.searches,
                unique_searchers = S.unique_searchers,
                ingestion_type = S.ingestion_type,
                ingested_at = S.ingested_at
            WHEN NOT MATCHED THEN INSERT ROW
            """
            merge_job = client.query(merge_sql)
            merge_job.result()
            affected = merge_job.num_dml_affected_rows or len(rows)

            client.delete_table(staging_id, not_found_ok=True)

            logger.info(
                f"Snowflake BQ: upserted {affected} rows to {table_id} "
                f"({len(rows)} staged)"
            )
        except Exception as e:
            logger.warning(f"Snowflake BQ write failed (non-fatal): {e}")

    def _get_bq_client(self):
        for env_var in (
            "FIREBASE_SERVICE_ACCOUNT_JSON",
            "FIREBASE_CREDENTIALS_JSON",
            "DATAPLANE_BQ_CREDENTIALS_JSON",
            "BIGQUERY_CREDENTIALS_JSON",
        ):
            creds_json = os.environ.get(env_var, "")
            if creds_json:
                logger.debug(f"Snowflake BQ: using credentials from {env_var}")
                break
        else:
            logger.warning("Snowflake BQ: no BQ credentials found in env")
            return None

        try:
            from google.cloud import bigquery
            creds_dict = json.loads(creds_json)

            tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
            json.dump(creds_dict, tmp)
            tmp.close()
            client = bigquery.Client.from_service_account_json(
                tmp.name, project=_BQ_PROJECT,
            )
            os.unlink(tmp.name)
            return client
        except Exception as e:
            logger.warning(f"Snowflake BQ client init failed: {e}")
            return None
