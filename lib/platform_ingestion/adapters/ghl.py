"""GoHighLevel adapter — contacts, opportunities, calendar bookings, conversations."""

from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.request
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from .base import BasePlatformAdapter, DailyMetricRow, PlatformConfig

logger = logging.getLogger(__name__)

GHL_BASE = "https://services.leadconnectorhq.com"
GHL_API_VERSION = "2021-07-28"


def _ghl_get(
    url: str,
    token: str,
    location_id: Optional[str] = None,
) -> Dict[str, Any]:
    headers = {
        "Authorization": f"Bearer {token}",
        "Version": GHL_API_VERSION,
        "Accept": "application/json",
    }
    req = urllib.request.Request(url, headers=headers)
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
        token = config.credentials.get("api_key", "")
        location_id = config.credentials.get("location_id", "")
        if not token:
            logger.warning(f"GHL: no api_key for {config.client_name}")
            return []
        if not location_id:
            logger.warning(f"GHL: no location_id for {config.client_name}")
            return []

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

        rows = [
            DailyMetricRow(
                metric_date=date.fromisoformat(d),
                metrics=dict(m),
                record_count=sum(
                    m[k] for k in ("contacts_created", "opportunities_created", "bookings")
                ),
            )
            for d, m in sorted(day_metrics.items())
            if any(v for v in m.values())
        ]
        logger.info(f"GHL: {len(rows)} days for {config.client_name}")
        return rows

    # ── Contacts ──────────────────────────────────────────────────

    def _pull_contacts(
        self,
        token: str,
        location_id: str,
        start_date: date,
        end_date: date,
        day_metrics: Dict[str, Dict[str, Any]],
    ) -> None:
        start_epoch = int(datetime.combine(start_date, datetime.min.time(),
                                           tzinfo=timezone.utc).timestamp() * 1000)
        end_epoch = int(datetime.combine(end_date + timedelta(days=1),
                                         datetime.min.time(),
                                         tzinfo=timezone.utc).timestamp() * 1000)
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
                contacts = data.get("contacts", [])
                if not contacts:
                    break

                for c in contacts:
                    created = c.get("dateAdded", "")
                    if not created:
                        continue
                    cd = created[:10]
                    if start_date.isoformat() <= cd <= end_date.isoformat():
                        day_metrics[cd]["contacts_created"] += 1
                        fetched += 1

                meta = data.get("meta", {})
                next_page = meta.get("nextPageUrl") or meta.get("startAfterId")
                if not next_page or not contacts or fetched > 50000:
                    break
                if isinstance(next_page, str) and "startAfterId=" in next_page:
                    next_page = next_page.split("startAfterId=")[-1].split("&")[0]

                time.sleep(self.RATE_LIMIT_DELAY)
        except Exception as e:
            logger.warning(f"GHL contacts fetch: {e}")

    # ── Opportunities (Pipeline) ──────────────────────────────────

    def _pull_opportunities(
        self,
        token: str,
        location_id: str,
        start_date: date,
        end_date: date,
        day_metrics: Dict[str, Dict[str, Any]],
    ) -> None:
        try:
            url = f"{GHL_BASE}/opportunities/search"
            payload = json.dumps({
                "location_id": location_id,
                "filters": [{
                    "field": "date_added",
                    "operator": ">=",
                    "value": start_date.isoformat(),
                }, {
                    "field": "date_added",
                    "operator": "<=",
                    "value": end_date.isoformat(),
                }],
                "page": 1,
                "limit": 100,
            }).encode()

            headers = {
                "Authorization": f"Bearer {token}",
                "Version": GHL_API_VERSION,
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
            req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
            resp = urllib.request.urlopen(req, timeout=60)
            data = json.loads(resp.read())

            for opp in data.get("opportunities", []):
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
            if e.code == 422:
                self._pull_opportunities_fallback(token, location_id, start_date, end_date, day_metrics)
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
        """Fallback: list pipelines then list opportunities per pipeline."""
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
                for opp in opp_data.get("opportunities", []):
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

    # ── Calendar Events (Bookings) ────────────────────────────────

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
                f"{GHL_BASE}/calendars/?locationId={location_id}",
                token,
            )
            for cal in cals_data.get("calendars", []):
                cal_id = cal.get("id", "")
                if not cal_id:
                    continue

                start_ts = datetime.combine(start_date, datetime.min.time(),
                                            tzinfo=timezone.utc).timestamp() * 1000
                end_ts = datetime.combine(end_date + timedelta(days=1),
                                          datetime.min.time(),
                                          tzinfo=timezone.utc).timestamp() * 1000

                url = (
                    f"{GHL_BASE}/calendars/events"
                    f"?locationId={location_id}"
                    f"&calendarId={cal_id}"
                    f"&startTime={int(start_ts)}"
                    f"&endTime={int(end_ts)}"
                )
                events_data = _ghl_get(url, token)
                for event in events_data.get("events", []):
                    event_start = (event.get("startTime") or event.get("start") or "")[:10]
                    if event_start and start_date.isoformat() <= event_start <= end_date.isoformat():
                        day_metrics[event_start]["bookings"] += 1

                time.sleep(self.RATE_LIMIT_DELAY)
        except Exception as e:
            logger.warning(f"GHL calendar events fetch: {e}")

    # ── Conversations ─────────────────────────────────────────────

    def _pull_conversations(
        self,
        token: str,
        location_id: str,
        start_date: date,
        end_date: date,
        day_metrics: Dict[str, Dict[str, Any]],
    ) -> None:
        fetched = 0
        try:
            url = f"{GHL_BASE}/conversations/search"
            payload = json.dumps({
                "locationId": location_id,
                "limit": 100,
            }).encode()
            headers = {
                "Authorization": f"Bearer {token}",
                "Version": GHL_API_VERSION,
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
            req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
            resp = urllib.request.urlopen(req, timeout=60)
            data = json.loads(resp.read())

            for conv in data.get("conversations", []):
                created = (conv.get("dateAdded") or conv.get("createdAt") or "")[:10]
                if not created:
                    continue
                if start_date.isoformat() <= created <= end_date.isoformat():
                    day_metrics[created]["conversations"] += 1
                    fetched += 1

        except Exception as e:
            logger.warning(f"GHL conversations fetch: {e}")
