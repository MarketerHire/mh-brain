"""GoHighLevel adapter — aggregated daily metrics only, zero PII.

PII Protection:
    GHL API returns full records (names, emails, phones, addresses).
    This adapter enforces a strict allowlist: every API response is stripped
    to ONLY the safe fields listed in _SAFE_FIELDS before any processing.
    No PII is ever held in memory beyond the initial strip, and no PII
    is ever written to Supabase or BigQuery.

    BQ output schema: daily_metrics (counts + revenue per day per location).
    No record-level data. No identifiers. No contact details.

Supports multi-location pulls via `additional_locations` in config.
"""

from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.request
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from .base import BasePlatformAdapter, DailyMetricRow, PlatformConfig

logger = logging.getLogger(__name__)

GHL_BASE = "https://services.leadconnectorhq.com"
GHL_API_VERSION = "2021-07-28"
_GHL_UA = "MH1-DataBridge/1.0"

# ── PII allowlists — only these fields survive _strip_to_safe ─────
_SAFE_FIELDS: Dict[str, set] = {
    "contact":      {"id", "dateAdded", "tags", "source", "country"},
    "opportunity":  {"id", "dateAdded", "createdAt", "status", "monetaryValue",
                     "pipelineId", "pipelineStageId", "source"},
    "event":        {"id", "startTime", "start", "endTime", "end",
                     "calendarId", "status", "appointmentStatus"},
    "conversation": {"id", "dateAdded", "createdAt", "type", "status",
                     "lastMessageType", "lastMessageDirection"},
}


def _strip_to_safe(records: List[Dict], entity: str) -> List[Dict]:
    """Strip each record to only the allowed safe fields. Everything else is dropped."""
    safe = _SAFE_FIELDS.get(entity, {"id", "dateAdded"})
    return [{k: v for k, v in r.items() if k in safe} for r in records]


def _ghl_get(url: str, token: str) -> Dict[str, Any]:
    headers = {
        "Authorization": f"Bearer {token}",
        "Version": GHL_API_VERSION,
        "Accept": "application/json",
        "User-Agent": _GHL_UA,
    }
    req = urllib.request.Request(url, headers=headers)
    resp = urllib.request.urlopen(req, timeout=60)
    return json.loads(resp.read())


def _ghl_post(url: str, token: str, body: dict) -> Dict[str, Any]:
    headers = {
        "Authorization": f"Bearer {token}",
        "Version": GHL_API_VERSION,
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": _GHL_UA,
    }
    req = urllib.request.Request(
        url, data=json.dumps(body).encode(), headers=headers, method="POST",
    )
    resp = urllib.request.urlopen(req, timeout=60)
    return json.loads(resp.read())


class GoHighLevelAdapter(BasePlatformAdapter):
    PLATFORM = "ghl"
    RATE_LIMIT_DELAY = 0.25
    MAX_LOOKBACK_DAYS = 365

    def pull_daily_metrics(
        self,
        config: PlatformConfig,
        start_date: date,
        end_date: date,
    ) -> List[DailyMetricRow]:
        locations = self._resolve_locations(config)
        if not locations:
            logger.warning(f"GHL: no valid locations for {config.client_name}")
            return []

        all_rows: List[DailyMetricRow] = []
        bq_rows: List[Dict[str, Any]] = []

        for token, loc_id, label in locations:
            day_metrics = self._pull_location(
                token, loc_id, start_date, end_date, config.client_name,
            )

            for d, m in sorted(day_metrics.items()):
                if not any(v for v in m.values()):
                    continue

                all_rows.append(DailyMetricRow(
                    metric_date=date.fromisoformat(d),
                    metrics={**m, "location_id": loc_id, "location_label": label},
                    record_count=sum(
                        m[k] for k in ("contacts_created", "opportunities_created", "bookings")
                    ),
                ))

                bq_rows.append({
                    "metric_date": d,
                    "location_id": loc_id,
                    "location_label": label,
                    **{k: v for k, v in m.items()},
                    "ingestion_type": "daily" if start_date == end_date else "backfill",
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                })

        if bq_rows:
            self._write_to_bq(config, bq_rows)

        logger.info(
            f"GHL: {len(all_rows)} rows across {len(locations)} locations "
            f"for {config.client_name}"
        )
        return all_rows

    # ── Location resolution ───────────────────────────────────────

    def _resolve_locations(self, config: PlatformConfig) -> List[Tuple[str, str, str]]:
        token = config.credentials.get("api_key", "")
        loc_id = config.credentials.get("location_id", "")
        label = config.extra.get("label", "primary")

        locations: List[Tuple[str, str, str]] = []
        if token and loc_id:
            locations.append((token, loc_id, label))

        for extra in config.extra.get("additional_locations", []):
            extra_token = extra.get("api_key") or extra.get("pit_key") or token
            extra_loc = extra.get("location_id") or extra.get("locationId") or ""
            extra_label = extra.get("label", f"location_{len(locations) + 1}")
            if extra_token and extra_loc:
                locations.append((extra_token, extra_loc, extra_label))

        return locations

    # ── Per-location pull ─────────────────────────────────────────

    def _pull_location(
        self,
        token: str,
        location_id: str,
        start_date: date,
        end_date: date,
        client_name: str,
    ) -> Dict[str, Dict[str, Any]]:
        day_metrics: Dict[str, Dict[str, Any]] = defaultdict(
            lambda: {
                "contacts_created": 0,
                "opportunities_created": 0,
                "opportunities_won": 0,
                "opportunities_lost": 0,
                "revenue": 0.0,
                "bookings": 0,
                "conversations": 0,
                "tasks_created": 0,
            }
        )

        self._pull_contacts(token, location_id, start_date, end_date, day_metrics)
        self._pull_opportunities(token, location_id, start_date, end_date, day_metrics)
        self._pull_calendar_events(token, location_id, start_date, end_date, day_metrics)
        self._pull_conversations(token, location_id, start_date, end_date, day_metrics)

        logger.info(
            f"GHL location {location_id[:8]}: {len(day_metrics)} days for {client_name}"
        )
        return day_metrics

    # ── Contacts (PII stripped immediately) ────────────────────────

    def _pull_contacts(
        self,
        token: str,
        location_id: str,
        start_date: date,
        end_date: date,
        day_metrics: Dict[str, Dict[str, Any]],
    ) -> None:
        """Pull contact counts by date. Results arrive newest-first, so we
        skip contacts newer than end_date and stop once we pass start_date."""
        start_epoch = int(
            datetime.combine(start_date, datetime.min.time(), tzinfo=timezone.utc)
            .timestamp() * 1000
        )
        start_iso = start_date.isoformat()
        end_iso = end_date.isoformat()
        fetched = 0
        next_page = None
        try:
            while True:
                url = (
                    f"{GHL_BASE}/contacts/"
                    f"?locationId={location_id}"
                    f"&startAfter={start_epoch}"
                    f"&limit=100"
                )
                if next_page:
                    url += f"&startAfterId={next_page}"

                data = _ghl_get(url, token)
                raw = data.get("contacts", [])

                contacts = _strip_to_safe(raw, "contact")
                del raw

                if not contacts:
                    break

                past_range = False
                for c in contacts:
                    created = c.get("dateAdded", "")
                    if not created:
                        continue
                    cd = created[:10]
                    if cd > end_iso:
                        continue
                    if cd < start_iso:
                        past_range = True
                        break
                    day_metrics[cd]["contacts_created"] += 1
                    fetched += 1

                if past_range:
                    break

                meta = data.get("meta", {})
                next_page = meta.get("nextPageUrl") or meta.get("startAfterId")
                if not next_page or fetched > 50000:
                    break
                if isinstance(next_page, str) and "startAfterId=" in next_page:
                    next_page = next_page.split("startAfterId=")[-1].split("&")[0]

                time.sleep(self.RATE_LIMIT_DELAY)
        except Exception as e:
            logger.warning(f"GHL contacts fetch: {e}")

    # ── Opportunities (PII stripped immediately) ──────────────────

    def _pull_opportunities(
        self,
        token: str,
        location_id: str,
        start_date: date,
        end_date: date,
        day_metrics: Dict[str, Dict[str, Any]],
    ) -> None:
        try:
            data = _ghl_post(f"{GHL_BASE}/opportunities/search", token, {
                "location_id": location_id,
                "filters": [
                    {"field": "date_added", "operator": ">=", "value": start_date.isoformat()},
                    {"field": "date_added", "operator": "<=", "value": end_date.isoformat()},
                ],
                "page": 1,
                "limit": 100,
            })

            raw = data.get("opportunities", [])
            opps = _strip_to_safe(raw, "opportunity")
            del raw

            for opp in opps:
                created = (opp.get("dateAdded") or opp.get("createdAt") or "")[:10]
                if not created or not (start_date.isoformat() <= created <= end_date.isoformat()):
                    continue

                day_metrics[created]["opportunities_created"] += 1
                status = (opp.get("status") or "").lower()
                monetary = self._safe_float(opp.get("monetaryValue"))

                if status == "won":
                    day_metrics[created]["opportunities_won"] += 1
                    day_metrics[created]["revenue"] += monetary
                elif status == "lost":
                    day_metrics[created]["opportunities_lost"] += 1

        except urllib.error.HTTPError as e:
            if e.code in (400, 422):
                self._pull_opportunities_fallback(
                    token, location_id, start_date, end_date, day_metrics,
                )
            else:
                logger.warning(f"GHL opportunities fetch: {e}")
        except Exception as e:
            logger.warning(f"GHL opportunities fetch: {e}")

    def _pull_opportunities_fallback(
        self,
        token: str,
        location_id: str,
        start_date: date,
        end_date: date,
        day_metrics: Dict[str, Dict[str, Any]],
    ) -> None:
        try:
            pipelines_data = _ghl_get(
                f"{GHL_BASE}/opportunities/pipelines?locationId={location_id}",
                token,
            )
            for pipeline in pipelines_data.get("pipelines", []):
                pid = pipeline.get("id", "")
                if not pid:
                    continue
                url = (
                    f"{GHL_BASE}/opportunities/pipelines/{pid}"
                    f"?locationId={location_id}&limit=100"
                )
                opp_data = _ghl_get(url, token)
                raw = opp_data.get("opportunities", [])
                opps = _strip_to_safe(raw, "opportunity")
                del raw

                for opp in opps:
                    created = (opp.get("dateAdded") or opp.get("createdAt") or "")[:10]
                    if not created or not (start_date.isoformat() <= created <= end_date.isoformat()):
                        continue
                    day_metrics[created]["opportunities_created"] += 1
                    status = (opp.get("status") or "").lower()
                    monetary = self._safe_float(opp.get("monetaryValue"))
                    if status == "won":
                        day_metrics[created]["opportunities_won"] += 1
                        day_metrics[created]["revenue"] += monetary
                    elif status == "lost":
                        day_metrics[created]["opportunities_lost"] += 1
                time.sleep(self.RATE_LIMIT_DELAY)
        except Exception as e:
            logger.warning(f"GHL opportunities fallback: {e}")

    # ── Calendar Events (PII stripped immediately) ────────────────

    def _pull_calendar_events(
        self,
        token: str,
        location_id: str,
        start_date: date,
        end_date: date,
        day_metrics: Dict[str, Dict[str, Any]],
    ) -> None:
        try:
            cals_data = _ghl_get(
                f"{GHL_BASE}/calendars/?locationId={location_id}", token,
            )
            for cal in cals_data.get("calendars", []):
                cal_id = cal.get("id", "")
                if not cal_id:
                    continue

                start_ts = int(
                    datetime.combine(start_date, datetime.min.time(), tzinfo=timezone.utc)
                    .timestamp() * 1000
                )
                end_ts = int(
                    datetime.combine(end_date + timedelta(days=1), datetime.min.time(),
                                     tzinfo=timezone.utc)
                    .timestamp() * 1000
                )

                url = (
                    f"{GHL_BASE}/calendars/events"
                    f"?locationId={location_id}"
                    f"&calendarId={cal_id}"
                    f"&startTime={start_ts}"
                    f"&endTime={end_ts}"
                )
                events_data = _ghl_get(url, token)
                raw = events_data.get("events", [])
                events = _strip_to_safe(raw, "event")
                del raw

                for event in events:
                    event_start = (event.get("startTime") or event.get("start") or "")[:10]
                    if event_start and start_date.isoformat() <= event_start <= end_date.isoformat():
                        day_metrics[event_start]["bookings"] += 1

                time.sleep(self.RATE_LIMIT_DELAY)
        except Exception as e:
            logger.warning(f"GHL calendar events fetch: {e}")

    # ── Conversations (PII stripped immediately) ──────────────────

    def _pull_conversations(
        self,
        token: str,
        location_id: str,
        start_date: date,
        end_date: date,
        day_metrics: Dict[str, Dict[str, Any]],
    ) -> None:
        start_iso = start_date.isoformat()
        end_iso = end_date.isoformat()

        for endpoint in (
            f"{GHL_BASE}/conversations/search",
            f"{GHL_BASE}/conversations/?locationId={location_id}&limit=100",
        ):
            try:
                if "search" in endpoint:
                    data = _ghl_post(endpoint, token, {
                        "locationId": location_id, "limit": 100,
                    })
                else:
                    data = _ghl_get(endpoint, token)

                raw = data.get("conversations", [])
                convos = _strip_to_safe(raw, "conversation")
                del raw

                for conv in convos:
                    created = (conv.get("dateAdded") or conv.get("createdAt") or "")[:10]
                    if created and start_iso <= created <= end_iso:
                        day_metrics[created]["conversations"] += 1
                return
            except urllib.error.HTTPError as e:
                if e.code in (404, 400):
                    continue
                logger.warning(f"GHL conversations fetch: {e}")
                return
            except Exception as e:
                logger.warning(f"GHL conversations fetch: {e}")
                return

        logger.debug(f"GHL conversations: no working endpoint for {location_id[:8]}")

    # ── BigQuery sink (aggregated counts only) ────────────────────

    def _write_to_bq(self, config: PlatformConfig, rows: List[Dict[str, Any]]) -> None:
        bq_dataset = config.extra.get("bq_dataset", "")
        if not bq_dataset:
            logger.info("GHL BQ: no bq_dataset in config, skipping BQ write")
            return

        target_project = "moe-platform-479917"
        try:
            client = self._get_bq_client(target_project=target_project)
            if not client:
                return

            table_id = f"{target_project}.{bq_dataset}.daily_metrics"
            staging_id = f"{target_project}.{bq_dataset}._staging_daily_metrics"

            from google.cloud.bigquery import (
                LoadJobConfig, SchemaField, WriteDisposition,
            )

            schema = [
                SchemaField("metric_date", "DATE"),
                SchemaField("location_id", "STRING"),
                SchemaField("location_label", "STRING"),
                SchemaField("contacts_created", "INTEGER"),
                SchemaField("opportunities_created", "INTEGER"),
                SchemaField("opportunities_won", "INTEGER"),
                SchemaField("opportunities_lost", "INTEGER"),
                SchemaField("revenue", "FLOAT"),
                SchemaField("bookings", "INTEGER"),
                SchemaField("conversations", "INTEGER"),
                SchemaField("tasks_created", "INTEGER"),
                SchemaField("ingestion_type", "STRING"),
                SchemaField("ingested_at", "TIMESTAMP"),
            ]

            job_config = LoadJobConfig(
                schema=schema,
                write_disposition=WriteDisposition.WRITE_TRUNCATE,
            )
            job = client.load_table_from_json(rows, staging_id, job_config=job_config)
            job.result()

            merge_sql = f"""
            MERGE `{table_id}` T
            USING `{staging_id}` S
            ON T.metric_date = S.metric_date AND T.location_id = S.location_id
            WHEN MATCHED THEN UPDATE SET
                location_label = S.location_label,
                contacts_created = S.contacts_created,
                opportunities_created = S.opportunities_created,
                opportunities_won = S.opportunities_won,
                opportunities_lost = S.opportunities_lost,
                revenue = S.revenue,
                bookings = S.bookings,
                conversations = S.conversations,
                tasks_created = S.tasks_created,
                ingestion_type = S.ingestion_type,
                ingested_at = S.ingested_at
            WHEN NOT MATCHED THEN INSERT ROW
            """
            merge_job = client.query(merge_sql)
            result = merge_job.result()
            affected = merge_job.num_dml_affected_rows or len(rows)

            client.delete_table(staging_id, not_found_ok=True)

            logger.info(
                f"GHL BQ: upserted {affected} rows to {table_id} "
                f"({len(rows)} staged)"
            )
        except Exception as e:
            logger.warning(f"GHL BQ write failed (non-fatal): {e}")

    def _get_bq_client(self, target_project: str = "moe-platform-479917"):
        """Resolve BQ credentials from env, preferring the data-plane SA."""
        import os
        import tempfile

        for env_var in (
            "FIREBASE_SERVICE_ACCOUNT_JSON",
            "FIREBASE_CREDENTIALS_JSON",
            "DATAPLANE_BQ_CREDENTIALS_JSON",
            "BIGQUERY_CREDENTIALS_JSON",
        ):
            creds_json = os.environ.get(env_var, "")
            if creds_json:
                logger.debug(f"GHL BQ: using credentials from {env_var}")
                break
        else:
            logger.warning("GHL BQ: no BQ credentials found in env")
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
            logger.warning(f"GHL BQ client init failed: {e}")
            return None
