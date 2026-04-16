"""Platform Data Orchestrator — pulls metrics from all client platforms into Supabase."""

from __future__ import annotations

import hashlib
import logging
import os
import uuid
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from .adapters.base import BasePlatformAdapter, DailyMetricRow, PlatformConfig
from .adapters.google_ads import GoogleAdsAdapter
from .adapters.meta_ads import MetaAdsAdapter
from .adapters.klaviyo import KlaviyoAdapter
from .adapters.hubspot import HubSpotAdapter
from .adapters.shopify import ShopifyAdapter
from .adapters.generic import (
    BrazeAdapter, IterableAdapter, BeehiivAdapter,
    TripleWhaleAdapter, AmplitudeAdapter, CustomerIOAdapter, AppsFlyerAdapter,
)
from .config_resolver import detect_platforms, resolve_config
from .rate_limiter import RateLimiter

logger = logging.getLogger(__name__)

_ADAPTERS: Dict[str, BasePlatformAdapter] = {
    "google_ads": GoogleAdsAdapter(),
    "meta_ads": MetaAdsAdapter(),
    "klaviyo": KlaviyoAdapter(),
    "hubspot": HubSpotAdapter(),
    "shopify": ShopifyAdapter(),
    "braze": BrazeAdapter(),
    "iterable": IterableAdapter(),
    "beehiiv": BeehiivAdapter(),
    "triple_whale": TripleWhaleAdapter(),
    "amplitude": AmplitudeAdapter(),
    "customerio": CustomerIOAdapter(),
    "appsflyer": AppsFlyerAdapter(),
}

# Platforms that need rate-limit staggering
_RATE_LIMITED = {"google_ads", "meta_ads"}

# Clients excluded from platform ingestion
_EXCLUDED_CLIENTS = {"glowguide", "hedley-bennett-33ca4ac1"}

# Readable platform labels for stream_id
_PLATFORM_LABELS: Dict[str, str] = {
    "google_ads": "GoogleAds",
    "meta_ads": "MetaAds",
    "klaviyo": "Klaviyo",
    "hubspot": "HubSpot",
    "shopify": "Shopify",
    "braze": "Braze",
    "iterable": "Iterable",
    "beehiiv": "Beehiiv",
    "triple_whale": "TripleWhale",
    "amplitude": "Amplitude",
    "customerio": "CustomerIO",
    "appsflyer": "AppsFlyer",
    "tiktok_ads": "TikTokAds",
    "ga4": "GA4",
    "polar_analytics": "PolarAnalytics",
}

# Platform-level category used for richer context inside the episode
# metadata. We keep this for downstream analytics / dashboards — it is
# *not* the canonical ``Domain`` enum value, which is stored separately
# and limited to CONTENT / REVENUE / HEALTH / CAMPAIGN / GENERIC.
_PLATFORM_CATEGORIES: Dict[str, str] = {
    "google_ads": "paid_media",
    "meta_ads": "paid_media",
    "klaviyo": "email",
    "hubspot": "crm",
    "shopify": "ecommerce",
    "braze": "lifecycle",
    "iterable": "email",
    "beehiiv": "email",
    "triple_whale": "ecommerce",
    "amplitude": "product_analytics",
    "customerio": "lifecycle",
    "appsflyer": "mobile",
}

# Canonical BrightMatter Domain enum values for each platform. These map
# directly onto ``lib.intelligence.types.Domain`` (enforced via the
# ``normalize_domain`` helper on the read side). Writing the canonical
# value at ingestion time means consolidation doesn't have to guess and
# downstream queries grouped by ``domain`` match the enum exactly.
_PLATFORM_DOMAINS: Dict[str, str] = {
    "google_ads":        "campaign",
    "meta_ads":          "campaign",
    "klaviyo":           "content",
    "hubspot":           "revenue",
    "shopify":           "revenue",
    "braze":             "content",
    "iterable":          "content",
    "beehiiv":           "content",
    "triple_whale":      "revenue",
    "amplitude":         "health",
    "customerio":        "content",
    "appsflyer":         "campaign",
}


def _make_stream_id(platform: str, client_name: str) -> str:
    label = _PLATFORM_LABELS.get(platform, platform.title())
    return f"{label}-{client_name}"


class PlatformDataOrchestrator:
    """Main loop: reads clients from Firebase, pulls platform data, writes to Supabase."""

    def __init__(self, *, backfill: bool = False):
        from lib.supabase_client import get_supabase
        self.supabase = get_supabase()
        self.rate_limiter = RateLimiter(backfill=backfill)
        self.backfill = backfill

    # ── Public entry points ──────────────────────────────────────

    def run_daily(self) -> Dict[str, Any]:
        """Daily pull: yesterday's data for all clients."""
        yesterday = date.today() - timedelta(days=1)
        return self._run(start_date=yesterday, end_date=yesterday, ingestion_type="daily")

    def run_backfill(
        self,
        lookback_years: int = 3,
        client_filter: Optional[str] = None,
        platform_filter: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Historical backfill with tiered granularity."""
        today = date.today()
        start = today - timedelta(days=lookback_years * 365)
        return self._run(
            start_date=start,
            end_date=today - timedelta(days=1),
            ingestion_type="backfill",
            client_filter=client_filter,
            platform_filter=platform_filter,
        )

    # ── Core loop ────────────────────────────────────────────────

    def _run(
        self,
        start_date: date,
        end_date: date,
        ingestion_type: str = "daily",
        client_filter: Optional[str] = None,
        platform_filter: Optional[str] = None,
    ) -> Dict[str, Any]:
        stats: Dict[str, Any] = {
            "clients_processed": 0,
            "streams_pulled": 0,
            "rows_written": 0,
            "episodes_created": 0,
            "errors": [],
        }

        clients = self._load_clients()
        if not clients:
            logger.warning("No clients found")
            return stats

        # Separate rate-limited platforms to process last
        non_limited: List[Tuple[Dict, str, Dict]] = []
        limited: List[Tuple[Dict, str, Dict]] = []

        for client in clients:
            if client["id"] in _EXCLUDED_CLIENTS:
                logger.debug(f"Skipping excluded client: {client['name']}")
                continue
            if client_filter and client_filter.lower() not in client["name"].lower():
                continue

            datasources = self._load_datasources(client["id"])
            if not datasources:
                continue

            platforms = detect_platforms(datasources)
            for platform, raw_config in platforms:
                if platform_filter and platform != platform_filter:
                    continue
                if platform not in _ADAPTERS:
                    logger.debug(f"No adapter for platform: {platform}")
                    continue

                entry = (client, platform, raw_config)
                if platform in _RATE_LIMITED:
                    limited.append(entry)
                else:
                    non_limited.append(entry)

        # Process non-rate-limited platforms first
        for client, platform, raw_config in non_limited:
            try:
                self._process_stream(client, platform, raw_config, start_date, end_date, ingestion_type, stats)
            except Exception as e:
                stream_id = _make_stream_id(platform, client["name"])
                logger.error(f"Stream {stream_id} crashed: {e}", exc_info=True)
                stats["errors"].append(f"{stream_id}: {e}")

        # Then rate-limited platforms with inter-account delays
        last_platform = ""
        for client, platform, raw_config in limited:
            if platform == last_platform:
                self.rate_limiter.inter_account_wait(platform)
            last_platform = platform
            try:
                self._process_stream(client, platform, raw_config, start_date, end_date, ingestion_type, stats)
            except Exception as e:
                stream_id = _make_stream_id(platform, client["name"])
                logger.error(f"Stream {stream_id} crashed: {e}", exc_info=True)
                stats["errors"].append(f"{stream_id}: {e}")

        logger.info(
            f"Platform ingestion complete: {stats['clients_processed']} clients, "
            f"{stats['streams_pulled']} streams, {stats['rows_written']} rows"
        )
        return stats

    def _process_stream(
        self,
        client: Dict[str, Any],
        platform: str,
        raw_config: Dict[str, Any],
        start_date: date,
        end_date: date,
        ingestion_type: str,
        stats: Dict[str, Any],
    ) -> None:
        client_id = client["id"]
        client_name = client["name"]
        stream_id = _make_stream_id(platform, client_name)

        config = resolve_config(platform, raw_config, client_id, client_name,
                                client.get("datasources", {}))
        if not config:
            logger.debug(f"Skipping {stream_id}: config resolution failed")
            return

        # Check watermark for daily pulls
        effective_start = start_date
        if ingestion_type == "daily":
            wm = self._get_watermark(stream_id)
            if wm and wm >= start_date:
                logger.debug(f"Skipping {stream_id}: already processed through {wm}")
                return

        adapter = _ADAPTERS[platform]
        try:
            rows = adapter.pull_daily_metrics(config, effective_start, end_date)
        except Exception as e:
            err_msg = f"{stream_id}: {e}"
            logger.error(f"Adapter failed: {err_msg}")
            stats["errors"].append(err_msg)
            return

        if not rows:
            logger.debug(f"No data from {stream_id}")
            return

        # Write to Supabase
        written = self._write_rows(client_id, client_name, platform, stream_id,
                                    config.account_id, rows, ingestion_type)
        stats["rows_written"] += written

        # Create episodic memories for BrightMatter learning
        episodes = self._create_episodes(client_id, client_name, platform, stream_id, rows)
        stats["episodes_created"] += episodes

        # Update watermark
        max_date = max(r.metric_date for r in rows)
        self._set_watermark(stream_id, max_date)

        stats["streams_pulled"] += 1
        stats["clients_processed"] = len(set(
            c["id"] for c, _, _ in [(client, platform, raw_config)]
        ))
        logger.info(f"Stream {stream_id}: {written} rows, {episodes} episodes")

    # ── Data writers ─────────────────────────────────────────────

    def _write_rows(
        self,
        client_id: str,
        client_name: str,
        platform: str,
        stream_id: str,
        account_id: Optional[str],
        rows: List[DailyMetricRow],
        ingestion_type: str,
    ) -> int:
        """Upsert daily metric rows into client_platform_data."""
        written = 0
        batch: List[Dict[str, Any]] = []

        for row in rows:
            record = {
                "client_id": client_id,
                "client_name": client_name,
                "platform": platform,
                "stream_id": stream_id,
                "metric_date": row.metric_date.isoformat(),
                "granularity": "daily",
                "metrics": row.metrics,
                "raw_record_count": row.record_count,
                "source_account": account_id or "",
                "ingestion_type": ingestion_type,
            }
            batch.append(record)

            if len(batch) >= 50:
                written += self._upsert_batch(batch)
                batch = []

        if batch:
            written += self._upsert_batch(batch)

        return written

    def _upsert_batch(self, batch: List[Dict[str, Any]]) -> int:
        try:
            result = (
                self.supabase.table("client_platform_data")
                .upsert(batch, on_conflict="stream_id,metric_date,granularity")
                .execute()
            )
            return len(result.data) if result.data else len(batch)
        except Exception as e:
            logger.error(f"Supabase upsert failed: {e}")
            return 0

    def _create_episodes(
        self,
        client_id: str,
        client_name: str,
        platform: str,
        stream_id: str,
        rows: List[DailyMetricRow],
    ) -> int:
        """Create episodic memories from daily metric rows."""
        domain = _PLATFORM_DOMAINS.get(platform, "generic")
        platform_category = _PLATFORM_CATEGORIES.get(platform, "generic")
        skill_name = f"platform-daily:{stream_id}"
        created = 0
        batch: List[Dict[str, Any]] = []

        # Chronological order so we can use yesterday's signal as today's
        # baseline for ratio-based consolidation (observed_signal /
        # observed_baseline). Falls back to 1.0 when no prior row exists.
        sorted_rows = sorted(rows, key=lambda r: r.metric_date)
        prev_signal: Optional[float] = None

        for row in sorted_rows:
            ep_id = hashlib.sha256(
                f"platform-{stream_id}-{row.metric_date.isoformat()}".encode()
            ).hexdigest()[:24]

            raw_primary = _primary_metric(platform, row.metrics)
            primary_metric = float(raw_primary) if raw_primary is not None else 0.0

            # Baseline = prior day's signal where possible, otherwise 1.0
            # so ratio math never divides by zero and doesn't collapse to
            # absurd values when a metric is missing upstream.
            if prev_signal is not None and prev_signal > 0:
                observed_baseline = prev_signal
            else:
                observed_baseline = 1.0

            goal_completed = raw_primary is not None and primary_metric > 0

            episode = {
                "episode_id": f"pi-{ep_id}",
                "tenant_id": client_id,
                "skill_name": skill_name,
                "domain": domain,
                "prediction": {
                    "skill_name": skill_name,
                    "tenant_id": client_id,
                    "domain": domain,
                    "context": {
                        "platform": platform,
                        "platform_category": platform_category,
                        "client_id": client_id,
                        "client_name": client_name,
                        "stream_id": stream_id,
                        "date": row.metric_date.isoformat(),
                    },
                    "expected_signal": 0.0,
                    "expected_baseline": observed_baseline,
                    "patterns_used": [],
                },
                "outcome": {
                    "observed_signal": primary_metric,
                    "observed_baseline": observed_baseline,
                    "goal_completed": goal_completed,
                    "business_impact": primary_metric,
                    "metadata": {
                        "source": "platform-ingestion",
                        "record_count": row.record_count,
                        "metrics": row.metrics,
                        "platform": platform,
                        "platform_category": platform_category,
                        "has_primary_metric": raw_primary is not None,
                    },
                },
                "weight": 1.0,
                "source": "platform-ingestion",
            }
            batch.append(episode)

            prev_signal = primary_metric

            if len(batch) >= 50:
                created += self._upsert_episodes(batch)
                batch = []

        if batch:
            created += self._upsert_episodes(batch)

        return created

    def _upsert_episodes(self, batch: List[Dict[str, Any]]) -> int:
        try:
            result = (
                self.supabase.table("episodic_memory")
                .upsert(batch, on_conflict="episode_id")
                .execute()
            )
            return len(result.data) if result.data else len(batch)
        except Exception as e:
            logger.error(f"Episode upsert failed: {e}")
            return 0

    # ── Client loading ───────────────────────────────────────────

    def _load_clients(self) -> List[Dict[str, Any]]:
        """Load client list from Firebase."""
        try:
            from lib.firebase_client import get_firebase_client
            fb = get_firebase_client()
            clients_raw = fb.get_collection("clients", limit=50)
            result = []
            for doc in clients_raw:
                cid = doc.get("_id", "")
                name = (doc.get("name") or doc.get("displayName")
                        or doc.get("company_name") or cid)
                status = (doc.get("status") or "").lower()
                if status in ("archived", "deleted"):
                    continue
                result.append({"id": cid, "name": name, "doc": doc})
            logger.info(f"Loaded {len(result)} clients from Firebase")
            return result
        except Exception as e:
            logger.error(f"Failed to load clients from Firebase: {e}")
            return self._load_clients_from_filesystem()

    def _load_clients_from_filesystem(self) -> List[Dict[str, Any]]:
        """Fallback: load clients from local filesystem."""
        import glob
        import json

        clients_dir = os.environ.get("MH1_CLIENTS_DIR", "/Applications/MH1/mh1-hq/clients")
        result = []
        for ds_path in glob.glob(f"{clients_dir}/*/config/datasources.json"):
            parts = ds_path.split("/")
            slug = parts[-3]
            name = slug.replace("-", " ").title()
            # Try to read client name from config
            try:
                with open(ds_path) as f:
                    ds = json.load(f)
                name = (ds.get("client", {}).get("name")
                        or ds.get("client_name")
                        or name)
            except Exception:
                pass
            result.append({"id": slug, "name": name, "doc": {}})
        logger.info(f"Loaded {len(result)} clients from filesystem")
        return result

    def _load_datasources(self, client_id: str) -> Optional[Dict[str, Any]]:
        """Load datasources.json for a client from Firebase, with FS fallback."""
        try:
            from lib.firebase_client import get_firebase_client
            fb = get_firebase_client()
            doc = fb.get_document("clients", client_id, subcollection="config",
                                  subdoc_id="datasources")
            if doc:
                return doc
        except Exception as e:
            logger.debug(f"Firebase datasources for {client_id}: {e}")

        return self._load_datasources_from_filesystem(client_id)

    def _load_datasources_from_filesystem(self, client_id: str) -> Optional[Dict[str, Any]]:
        """Fallback: read datasources.json from local filesystem."""
        import json
        clients_dir = os.environ.get("MH1_CLIENTS_DIR", "/Applications/MH1/mh1-hq/clients")
        path = os.path.join(clients_dir, client_id, "config", "datasources.json")
        if not os.path.isfile(path):
            return None
        try:
            with open(path) as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to read {path}: {e}")
            return None

    # ── Watermarks ───────────────────────────────────────────────

    def _get_watermark(self, stream_id: str) -> Optional[date]:
        try:
            key = f"platform-{stream_id}"
            res = (
                self.supabase.table("bm_watermarks")
                .select("last_processed_at")
                .eq("source", key)
                .execute()
            )
            if res.data:
                ts = res.data[0].get("last_processed_at", "")
                if ts:
                    return date.fromisoformat(ts[:10])
        except Exception:
            pass
        return None

    def _set_watermark(self, stream_id: str, last_date: date) -> None:
        try:
            key = f"platform-{stream_id}"
            now = datetime.now(timezone.utc).isoformat()
            self.supabase.table("bm_watermarks").upsert({
                "source": key,
                "last_processed_at": last_date.isoformat(),
                "updated_at": now,
                "metadata": {"stream_id": stream_id, "last_run": now},
            }, on_conflict="source").execute()
        except Exception as e:
            logger.warning(f"Watermark update failed for {stream_id}: {e}")


def _primary_metric(platform: str, metrics: Dict[str, Any]) -> Optional[float]:
    """Extract the primary metric for BrightMatter's learning signal."""
    if platform in ("google_ads", "meta_ads"):
        return metrics.get("spend")
    elif platform in ("klaviyo", "iterable", "customerio", "beehiiv", "braze"):
        return metrics.get("revenue", metrics.get("sends", metrics.get("campaigns")))
    elif platform in ("shopify", "triple_whale"):
        return metrics.get("revenue")
    elif platform == "hubspot":
        return metrics.get("contacts_created")
    elif platform in ("amplitude",):
        return metrics.get("active_users")
    elif platform == "appsflyer":
        return metrics.get("installs")
    return None
