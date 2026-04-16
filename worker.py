"""
BrightMatter Cron Worker

Six-tier data ingestion + learning pipeline:
    Tier 1 — Ingest signals from shared Supabase (MH-OS Trigger.dev output)
    Tier 2 — Ingest daily spend deltas from BigQuery (when credentials available)
    Tier 3 — Publish high-confidence patterns to Airtable (when credentials available)
    Tier 4 — Ingest call transcripts from MH-OS Supabase
    Tier 5 — Ingest client dashboard data from Firebase clientData
    Tier 6 — Multi-platform client data (separate cron: platform_data_cron)

Plus: pull events, process through learning pipeline, consolidate,
refresh guidance cache.

Run modes:
    python worker.py                     # single cycle
    python worker.py --loop --interval 900  # continuous (every 15 min)

Requires:
    SUPABASE_URL      — Supabase project URL
    SUPABASE_KEY      — Supabase service role key
Optional:
    BIGQUERY_CREDENTIALS_JSON — enables Tier 2 (BQ daily delta)
    AIRTABLE_API_KEY          — enables Tier 3 (pattern publishing)
    FIREBASE_PROJECT_ID + FIREBASE_SERVICE_ACCOUNT_JSON — enables Tier 5 (Firebase clientData)
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import os
import time
import uuid
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("brightmatter.worker")

LEVER_TO_DOMAIN = {
    "Lead Volume": "campaign",
    "Conversion Rate": "campaign",
    "Pipeline": "revenue",
    "Revenue": "revenue",
    "Retention": "health",
    "Churn": "health",
    "Content": "content",
    "All": "generic",
}


class BrightMatterWorker:
    """Pulls events from Supabase, processes through learning pipeline, updates guidance."""

    def __init__(self):
        from lib.intelligence import IntelligenceEngine
        from lib.intelligence_bridge import IntelligenceBridge

        self.engine = IntelligenceEngine()
        self.bridge = IntelligenceBridge(engine=self.engine)
        self._supabase = None

    @property
    def supabase(self):
        if self._supabase is None:
            from lib.supabase_client import get_supabase
            self._supabase = get_supabase()
        return self._supabase

    def run_cycle(self) -> Dict[str, Any]:
        """Run one full processing cycle with all three tiers."""
        stats = {
            "events_pulled": 0,
            "events_processed": 0,
            "events_failed": 0,
            "signals_ingested": 0,
            "bq_episodes": 0,
            "patterns_published": 0,
            "guidance_refreshed": 0,
            "consolidation": {},
        }

        try:
            # ── Tier 1: Ingest signals from shared Supabase ──────────
            try:
                sig_stats = self._ingest_signals()
                stats["signals_ingested"] = sig_stats.get("ingested", 0)
                logger.info(f"Tier 1 signals: {sig_stats}")
            except Exception as e:
                logger.error(f"Tier 1 signal ingestion failed: {e}")

            # ── Tier 2: Ingest daily BQ delta (if credentials available) ─
            try:
                bq_stats = self._ingest_bq_delta()
                stats["bq_episodes"] = bq_stats.get("episodes_created", 0)
                logger.info(f"Tier 2 BQ delta: {bq_stats}")
            except Exception as e:
                logger.debug(f"Tier 2 BQ ingestion skipped or failed: {e}")

            # ── Standard event processing ────────────────────────────
            new_events = self._pull_new_events()
            stats["events_pulled"] = len(new_events)
            logger.info(f"Pulled {len(new_events)} new events from Supabase")

            for event in new_events:
                try:
                    self._process_event(event)
                    stats["events_processed"] += 1

                    self.supabase.table("events").update(
                        {"processed_by_bm": True}
                    ).eq("id", event["id"]).execute()
                except Exception as e:
                    logger.error(f"Failed to process event {event.get('id')}: {e}")
                    stats["events_failed"] += 1

            # ── Consolidation ────────────────────────────────────────
            consolidation_stats = self.engine.run_consolidation()
            stats["consolidation"] = consolidation_stats
            logger.info(f"Consolidation: {consolidation_stats}")

            # ── Checkpoint processing ────────────────────────────────
            try:
                from lib.intelligence.outcomes.checkpoint_processor import CheckpointProcessor
                processor = CheckpointProcessor(self.engine.storage, self.bridge)
                checkpoint_stats = processor.process_all_due()
                stats["checkpoints"] = checkpoint_stats
            except Exception as e:
                logger.error(f"Checkpoint processing failed: {e}")

            # ── Tier 4: Ingest call transcripts from MH-OS ──────────
            try:
                transcript_stats = self._ingest_call_transcripts()
                stats["transcript_episodes"] = transcript_stats.get("episodes_created", 0)
                logger.info(f"Tier 4 transcripts: {transcript_stats}")
            except Exception as e:
                logger.debug(f"Tier 4 transcript ingestion skipped or failed: {e}")

            # ── Tier 5: Ingest client data from Firebase ────────────
            try:
                fb_stats = self._ingest_firebase_client_data()
                stats["firebase_episodes"] = fb_stats.get("episodes_created", 0)
                logger.info(f"Tier 5 Firebase clientData: {fb_stats}")
            except Exception as e:
                logger.debug(f"Tier 5 Firebase ingestion skipped or failed: {e}")

            # ── Tier 3: Publish patterns to Airtable ─────────────────
            try:
                pub_stats = self._publish_patterns_to_airtable()
                stats["patterns_published"] = pub_stats.get("published", 0)
                logger.info(f"Tier 3 Airtable publish: {pub_stats}")
            except Exception as e:
                logger.debug(f"Tier 3 Airtable publish skipped or failed: {e}")

            # ── Refresh guidance cache ───────────────────────────────
            refreshed = self._refresh_guidance_cache()
            stats["guidance_refreshed"] = refreshed
            logger.info(f"Guidance cache refreshed for {refreshed} pairs")

        except Exception as e:
            logger.error(f"Worker cycle failed: {e}")
            stats["error"] = str(e)

        return stats

    def _pull_new_events(self, limit: int = 500) -> List[Dict[str, Any]]:
        """Pull unprocessed events from Supabase."""
        result = (
            self.supabase.table("events")
            .select("*")
            .eq("processed_by_bm", False)
            .order("created_at")
            .limit(limit)
            .execute()
        )
        return result.data or []

    def _process_event(self, event: Dict[str, Any]):
        """Route event to appropriate handler, with idempotency checks."""
        event_type = event.get("event_type", "")
        event_id = event.get("id") or ""

        # Idempotency: if this event was already represented in episodic
        # memory (keyed by `_event_id` in outcome.metadata), skip it.
        # Events are supposed to be marked `processed_by_bm=True` after
        # processing, but a retry after a crash can re-deliver the same
        # row before the flag flips. Without this guard we'd double-
        # count signals and double-score predictions.
        if event_id and self._event_already_processed(event_id):
            logger.info(f"Skipping already-processed event {event_id}")
            return

        if event_type == "signal":
            self._process_signal(event)
        elif event_type == "skill_completed":
            self._process_skill_completion(event)
        elif event_type == "plan_completed":
            self._process_plan_completion(event)
        elif event_type == "human_feedback":
            self._process_feedback(event)
        elif event_type == "expert_override":
            self._process_expert_override(event)
        else:
            logger.warning(f"Unknown event_type: {event_type}")

    def _event_already_processed(self, event_id: str) -> bool:
        """Return True when we've already written an episode for this event.

        Looks up ``episodic_memory`` rows whose ``outcome->>'_event_id'``
        JSONB field matches ``event_id``. Failures are treated as
        "unknown" so we err on the side of processing — ``processed_by_bm``
        is a belt-and-suspenders second guard.
        """
        if not event_id:
            return False
        try:
            result = (
                self.supabase.table("episodic_memory")
                .select("episode_id")
                .filter("outcome->metadata->>_event_id", "eq", event_id)
                .limit(1)
                .execute()
            )
            if result.data:
                return True
        except Exception as e:
            logger.debug(f"dedup lookup failed for {event_id}: {e}")

        # Legacy rows written before Phase 2 stored metrics at
        # outcome.metrics (not outcome.metadata). Accept either shape
        # so we don't double-process events straddling the migration.
        try:
            result = (
                self.supabase.table("episodic_memory")
                .select("episode_id")
                .filter("outcome->>_event_id", "eq", event_id)
                .limit(1)
                .execute()
            )
            return bool(result.data)
        except Exception as e:
            logger.debug(f"legacy dedup lookup failed for {event_id}: {e}")
            return False

    def _process_signal(self, event: Dict[str, Any]):
        """Convert a signal event (e.g., from MH-OS) into an episode.

        Wires the Predictor to populate expected_signal/patterns_used,
        and the Learner to update patterns after storing.
        """
        from lib.intelligence.types import (
            EpisodicMemory, Prediction, Outcome, EpisodeSource, normalize_domain,
        )

        skill_name = event.get("skill_name") or event.get("source", "signal")
        client_id = event.get("client_id", "unknown")
        domain = normalize_domain(event.get("domain"))

        metrics = event.get("metrics", {})
        context = event.get("context", {})

        primary_signal = 0.5
        for key in ["primary_metric", "value", "signal", "score"]:
            if key in metrics and isinstance(metrics[key], (int, float)):
                primary_signal = float(metrics[key])
                break

        guidance = None
        try:
            guidance = self.engine.predictor.get_guidance(
                skill_name=skill_name,
                tenant_id=client_id,
                domain=domain,
                context=context,
            )
        except Exception as e:
            logger.debug(f"Signal predictor guidance failed: {e}")

        prediction = Prediction(
            prediction_id=str(uuid.uuid4())[:12],
            skill_name=skill_name,
            tenant_id=client_id,
            domain=domain,
            expected_signal=guidance.expected_signal if guidance else 0.5,
            expected_baseline=guidance.expected_baseline if guidance else 1.0,
            confidence=guidance.confidence if guidance else 0.5,
            patterns_used=guidance.patterns_used if guidance else [],
            context=context,
        )

        outcome = Outcome(
            prediction_id=prediction.prediction_id,
            observed_signal=primary_signal,
            observed_baseline=1.0,
            goal_completed=primary_signal > 0.5,
            metadata={
                "_source": event.get("source", ""),
                "_event_id": event.get("id", ""),
                "_episode_source": EpisodeSource.MARKET_OBSERVATION.value,
                **metrics,
            },
        )

        episode = EpisodicMemory(prediction=prediction, outcome=outcome)
        self.engine.episodic.store(episode)

        if guidance:
            try:
                self.engine.learner.learn_from_outcome(prediction, outcome)
            except Exception as e:
                logger.debug(f"Signal learner update failed: {e}")

    def _process_skill_completion(self, event: Dict[str, Any]):
        """Process a completed skill execution event.

        Two modes, decided by whether the event carries a ``tracking_id``:

        1. **Close-the-loop mode** (preferred): the upstream system
           registered a prediction via ``start_tracking`` when the skill
           began. The ``tracking_id`` is included on the completion event.
           We resolve that prediction with real outcomes — which is what
           the learning loop needs to update patterns. Never create a
           duplicate tracking cycle in this path.

        2. **Backfill mode**: no ``tracking_id`` present (e.g. legacy
           completions, events from systems that don't integrate with
           the bridge). Register a fresh prediction and immediately
           close it as deferred so we at least get an episode on disk.
        """
        skill_name = event.get("skill_name", "")
        client_id = event.get("client_id", "")
        result = event.get("result", {})
        metrics = event.get("metrics", {})
        context = event.get("context", {})
        tracking_id = (
            event.get("tracking_id")
            or (event.get("result") or {}).get("tracking_id")
            or (event.get("metrics") or {}).get("tracking_id")
            or ""
        )

        if tracking_id and tracking_id in self.bridge._tracking:
            # Close-the-loop: resolve the already-registered prediction.
            self.bridge.complete_tracking(
                tracking_id=tracking_id,
                result=result,
                metrics=metrics,
                deferred=True,
            )
            return

        if tracking_id:
            logger.debug(
                f"tracking_id {tracking_id} on event {event.get('id')} "
                "not registered with bridge — falling back to backfill mode"
            )

        guidance = self.bridge.get_skill_guidance(
            skill_name=skill_name,
            client_id=client_id,
            inputs=context,
        )

        new_tracking_id = self.bridge.start_tracking(
            skill_name=skill_name,
            client_id=client_id,
            guidance=guidance,
            context=context,
        )

        self.bridge.complete_tracking(
            tracking_id=new_tracking_id,
            result=result,
            metrics=metrics,
            deferred=True,
        )

    def _process_plan_completion(self, event: Dict[str, Any]):
        """Process a completed plan/module execution event."""
        module_id = event.get("skill_name", "") or event.get("module_id", "")
        client_id = event.get("client_id", "")
        execution_data = event.get("result", {})

        self.bridge.consolidate_from_module(
            module_id=module_id,
            client_id=client_id,
            execution_data=execution_data,
        )

    def _process_feedback(self, event: Dict[str, Any]):
        """Process human feedback on a prediction."""
        result = event.get("result", {})
        prediction_id = result.get("prediction_id", "")
        rating = result.get("rating", 0.5)

        if prediction_id:
            self.engine.record_user_feedback(
                prediction_id=prediction_id,
                user_rating=float(rating),
                user_correction=result.get("correction"),
            )

    def _process_expert_override(self, event: Dict[str, Any]):
        """Process an expert override — highest-value learning signal.

        When a marketer rejects generated output and writes their own,
        this is the strongest signal about what the right answer looks like.
        """
        from lib.intelligence.types import (
            EpisodicMemory, Prediction, Outcome, EpisodeSource, normalize_domain,
        )

        skill_name = event.get("skill_name", "")
        client_id = event.get("client_id", "")
        result = event.get("result", {})
        context = event.get("context", {})
        domain = normalize_domain(event.get("domain"))

        prediction = Prediction(
            prediction_id=str(uuid.uuid4())[:12],
            skill_name=skill_name,
            tenant_id=client_id,
            domain=domain,
            expected_signal=0.5,
            expected_baseline=1.0,
            context=context,
        )

        outcome = Outcome(
            prediction_id=prediction.prediction_id,
            observed_signal=1.0,
            observed_baseline=1.0,
            goal_completed=True,
            business_impact=0.0,
            metadata={
                "_source": "expert_override",
                "_episode_source": EpisodeSource.OPERATOR_FEEDBACK.value,
                "_event_id": event.get("id", ""),
                "_expert_output": result.get("expert_output", ""),
                "_original_output": result.get("original_output", ""),
                "_override_reason": result.get("reason", ""),
            },
        )

        episode = EpisodicMemory(
            prediction=prediction,
            outcome=outcome,
            weight=2.0,
        )
        self.engine.episodic.store(episode)

    # ── Tier 1: Signal Ingestion ───────────────────────────────────

    def _ingest_signals(self, limit: int = 200) -> Dict[str, Any]:
        """Poll the shared `signals` table for rows not yet ingested.

        Uses bm_watermarks to track the last processed signal timestamp
        so we never re-process rows. Each signal becomes an episodic memory.
        """
        stats = {"checked": 0, "ingested": 0, "skipped": 0}

        watermark = self._get_watermark("signals")
        last_ts = watermark.get("last_processed_at") if watermark else None

        query = (
            self.supabase.table("signals")
            .select("*")
            .order("created_at", desc=False)
            .limit(limit)
        )
        if last_ts:
            query = query.gt("created_at", last_ts)

        result = query.execute()
        rows = result.data or []
        stats["checked"] = len(rows)

        if not rows:
            return stats

        from lib.intelligence.types import (
            Domain, EpisodicMemory, Prediction, Outcome, EpisodeSource,
        )

        latest_ts = last_ts
        for row in rows:
            signal_id = row.get("id", "")
            source = row.get("source", "signal")
            lever = row.get("lever", "All")
            metrics = row.get("metrics") or {}
            dt = row.get("date", "")

            domain_str = LEVER_TO_DOMAIN.get(lever, "generic")
            try:
                domain = Domain(domain_str)
            except ValueError:
                domain = Domain.GENERIC

            primary_signal = 0.5
            for key in ["primary_metric", "value", "signal", "score"]:
                if key in metrics and isinstance(metrics[key], (int, float)):
                    primary_signal = float(metrics[key])
                    break

            ep_hash = hashlib.md5(f"sig-{signal_id}".encode()).hexdigest()[:12]
            episode_id = f"ep-sig-{ep_hash}"

            prediction = Prediction(
                prediction_id=episode_id,
                skill_name=source,
                tenant_id="marketerhire",
                domain=domain,
                expected_signal=0.5,
                expected_baseline=1.0,
                context={
                    "lever": lever,
                    "cadence": row.get("cadence", ""),
                    "date": dt,
                    "summary": row.get("summary", "")[:200],
                    "source": "mh-os-signal",
                },
            )

            outcome = Outcome(
                prediction_id=episode_id,
                observed_signal=primary_signal,
                observed_baseline=1.0,
                goal_completed=primary_signal > 0.5,
                metadata={
                    "_source": source,
                    "_signal_id": signal_id,
                    "_episode_source": EpisodeSource.MARKET_OBSERVATION.value,
                    **metrics,
                },
            )

            episode = EpisodicMemory(prediction=prediction, outcome=outcome)
            try:
                self.engine.episodic.store(episode)
                stats["ingested"] += 1
            except Exception as e:
                logger.warning(f"Failed to store signal episode {episode_id}: {e}")
                stats["skipped"] += 1

            row_ts = row.get("created_at", "")
            if row_ts and (not latest_ts or row_ts > latest_ts):
                latest_ts = row_ts

        if latest_ts:
            self._set_watermark("signals", last_processed_at=latest_ts)

        return stats

    # ── Channel Resolution ─────────────────────────────────────

    _BQ_CHANNEL_TO_REGISTRY = {
        "Google": "paid_search.google",
        "Facebook": "paid_social.meta",
        "Instagram": "paid_social.meta",
        "TikTok": "paid_social.tiktok",
        "Tiktok": "paid_social.tiktok",
        "LinkedIn": "organic_social.linkedin",
        "Reddit": "paid_social.meta",
        "Twitter": "paid_social.meta",
        "YouTube": "paid_social.meta",
        "Bing": "paid_search.google",
        "Email": "email.broadcast",
        "SMS": "sms_push.promotional",
        "Vibe": "landing_page",
        "quora": "paid_social.meta",
    }

    _CORRELATION_SIGNATURES = {
        "creative_fatigue": {
            "conditions": {"cpa": "rising", "ctr": "falling", "impressions": "rising"},
            "skill_name": "correlation-creative-fatigue",
        },
        "budget_scaling_inefficiency": {
            "conditions": {"spend": "rising", "cpa": "rising", "roas": "falling"},
            "skill_name": "correlation-budget-scaling",
        },
        "channel_saturation": {
            "conditions": {"impressions": "flat", "cpc": "rising", "ctr": "falling"},
            "skill_name": "correlation-channel-saturation",
        },
    }

    def _resolve_channel_id(self, bq_channel: str) -> str:
        return self._BQ_CHANNEL_TO_REGISTRY.get(bq_channel, "")

    def _get_channel_config(self, channel_id: str):
        from lib.intelligence.adapters.channels import get_channel_config
        return get_channel_config(channel_id) if channel_id else None

    def _check_disqualifiers(
        self, context: Dict[str, Any], channel_config
    ) -> Tuple[bool, str]:
        """Return (skip, reason) if this episode should skip the Learner."""
        dormancy = context.get("dormancy_days")
        spend = context.get("spend", 0)
        if dormancy is not None and dormancy > 30 and spend == 0:
            return True, "channel_dormant"
        if context.get("zero_spend_streak", 0) >= 7:
            return True, "zero_spend_period"
        if context.get("brand_crisis"):
            return True, "brand_crisis_active"
        if channel_config:
            min_vol = channel_config.min_sample_size
            impressions = context.get("impressions", 0)
            if min_vol and isinstance(impressions, (int, float)) and impressions < min_vol:
                return True, "insufficient_volume"
        return False, ""

    def _infer_funnel_stage(self, campaign_name: str, strategy: str) -> str:
        lower = (campaign_name + " " + strategy).lower()
        if any(k in lower for k in ("brand", "prospecting", "awareness", "reach")):
            return "awareness"
        if any(k in lower for k in ("non-brand", "retargeting", "remarketing", "decision", "conversion")):
            return "decision"
        return "consideration"

    # ── Tier 2: BigQuery Daily + Campaign Delta ─────────────────

    _MH_CLIENT_CONTEXT = {
        "client_id": "marketerhire",
        "client_name": "MarketerHire",
        "industry": "b2b_marketplace",
        "region": "US",
    }

    _CHANNEL_CONFIGS = {
        "Google": {
            "platform": "google_ads",
            "account_type": "paid_search_display",
            "strategies": [
                "search-brand", "search-non-brand", "search-remarketing",
                "search-competitor", "search-core", "search-roles",
                "pmax", "display-remarketing",
            ],
        },
        "Facebook": {
            "platform": "meta_ads",
            "account_type": "paid_social",
            "strategies": ["prospecting", "retargeting", "remarketing", "lookalike", "reach"],
        },
        "LinkedIn": {
            "platform": "linkedin_ads",
            "account_type": "paid_social_b2b",
            "strategies": ["prospecting", "retargeting", "engagement", "conversions"],
        },
        "Reddit": {"platform": "reddit_ads", "account_type": "paid_social", "strategies": ["reach", "conversions"]},
        "Tiktok": {"platform": "tiktok_ads", "account_type": "paid_social", "strategies": ["reach", "conversions"]},
        "Twitter": {"platform": "twitter_ads", "account_type": "paid_social"},
        "YouTube": {"platform": "youtube_ads", "account_type": "paid_video"},
        "Bing": {"platform": "microsoft_ads", "account_type": "paid_search"},
        "Vibe": {"platform": "vibe_tv", "account_type": "ctv"},
        "quora": {"platform": "quora_ads", "account_type": "paid_social"},
    }

    _TRIGGER_SIGNAL_SKILLS = frozenset({
        "daily-pulse", "spend-pacing", "channel-advisor",
        "google-ads-roas", "google-ads-qs", "google-ads-ad-copy",
        "google-ads-search-terms", "google-ads-weekly",
    })

    def _ingest_bq_delta(self) -> Dict[str, Any]:
        """Pull recent spend data from BigQuery and create episodes.

        Two sub-flows:
          A) Channel-level daily spend → one episode per (channel, date)
          B) Campaign-level weekly rollup → one episode per (channel, campaign, week)

        Wires the full evaluation pipeline:
          - Resolves channel_id via _BQ_CHANNEL_TO_REGISTRY
          - Populates ChannelContext (dormancy, active days, account age)
          - Computes WoW change ratio as observed_signal
          - Generates composite skill names
          - Calls Predictor.get_guidance() before storing
          - Calls Learner.learn_from_outcome() after storing
          - Detects cross-metric correlations
        """
        if not (
            os.environ.get("BIGQUERY_CREDENTIALS_JSON")
            or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        ):
            return {"skipped": True, "reason": "no BQ credentials"}

        from lib.bigquery_client import get_bq_client
        from lib.intelligence.types import (
            Domain, EpisodicMemory, Prediction, Outcome, EpisodeSource,
        )

        stats = {
            "channel_episodes": 0, "campaign_episodes": 0,
            "skipped_dedup": 0, "skipped_zero": 0, "skipped_disqualified": 0,
            "correlations": 0, "failed": 0,
        }

        watermark = self._get_watermark("bigquery")
        last_date_str = None
        if watermark and watermark.get("metadata"):
            last_date_str = watermark["metadata"].get("last_date")

        if last_date_str:
            start = (
                datetime.fromisoformat(last_date_str) + timedelta(days=1)
            ).strftime("%Y-%m-%d")
        else:
            start = (date.today() - timedelta(days=7)).isoformat()

        end = date.today().isoformat()

        if start >= end:
            return {"skipped": True, "reason": "already up to date"}

        bq = get_bq_client()
        covered_dates = self._get_triggerdev_covered_dates()

        channel_context_cache: Dict[str, Dict[str, Any]] = {}
        prior_metrics_by_channel: Dict[str, Dict[str, float]] = {}
        daily_metrics_for_correlation: Dict[Tuple[str, str], Dict[str, float]] = {}

        _f = lambda v: float(v) if v is not None else 0.0

        def _compute_channel_context(ch: str) -> Dict[str, Any]:
            if ch in channel_context_cache:
                return channel_context_cache[ch]
            ctx: Dict[str, Any] = {}
            try:
                hist = bq.query(
                    f"SELECT MIN(date) as first_date, MAX(date) as last_date, "
                    f"COUNT(DISTINCT CASE WHEN spend > 0 THEN date END) as active_days "
                    f"FROM `marketerhire.dbt_mh.fact_daily_spend__by_channel` "
                    f"WHERE channel = '{ch}'"
                )
                if hist:
                    row = hist[0] if isinstance(hist, list) else hist
                    first = row.get("first_date")
                    last = row.get("last_date")
                    if first and hasattr(first, "isoformat"):
                        ctx["account_age_days"] = (date.today() - first).days
                    if last and hasattr(last, "isoformat"):
                        ctx["dormancy_days"] = (date.today() - last).days
                    ctx["active_days_last_90"] = int(row.get("active_days") or 0)
            except Exception as e:
                logger.debug(f"Channel context query failed for {ch}: {e}")
            channel_context_cache[ch] = ctx
            return ctx

        # ── A) Channel-level daily episodes ─────────────────────
        rows = bq.query_daily_spend(start, end)

        for row in rows:
            channel = row.get("channel", "unknown")
            dt = row.get("dt")
            dt_str = dt.isoformat()[:10] if hasattr(dt, "isoformat") else str(dt)[:10]

            spend = _f(row.get("spend"))
            appt = _f(row.get("nbr_appt"))
            cpa = spend / appt if appt > 0 else 0

            if spend == 0 and appt == 0:
                stats["skipped_zero"] += 1
                continue
            if dt_str in covered_dates:
                stats["skipped_dedup"] += 1
                continue

            channel_id = self._resolve_channel_id(channel)
            cc = self._get_channel_config(channel_id)
            ch_ctx = _compute_channel_context(channel)
            local_config = self._CHANNEL_CONFIGS.get(channel, {})

            prior = prior_metrics_by_channel.get(channel, {})
            prior_cpa = prior.get("cpa", cpa)
            wow_ratio = cpa / prior_cpa if prior_cpa > 0 else 1.0

            composite_skill = f"channel-spend-daily:{channel_id}" if channel_id else "channel-spend-daily"

            context = {
                **self._MH_CLIENT_CONTEXT,
                "channel": channel,
                "channel_id": channel_id,
                "platform": local_config.get("platform", channel.lower()),
                "account_type": local_config.get("account_type", "unknown"),
                "date": dt_str,
                "window": "daily",
                "source": "bigquery-delta",
                **{k: v for k, v in ch_ctx.items() if v is not None},
            }

            skip, reason = self._check_disqualifiers(
                {**context, "spend": spend, "impressions": _f(row.get("impressions", 0))}, cc,
            )

            guidance = None
            if not skip:
                try:
                    guidance = self.engine.predictor.get_guidance(
                        skill_name=composite_skill,
                        tenant_id="marketerhire",
                        domain=cc.domain if cc else Domain.CAMPAIGN,
                        context=context,
                    )
                except Exception as e:
                    logger.debug(f"Predictor guidance failed: {e}")

            ep_raw = f"bq-daily-{channel}-{dt_str}"
            ep_hash = hashlib.md5(ep_raw.encode()).hexdigest()[:12]
            episode_id = f"ep-bq-{ep_hash}"

            prediction = Prediction(
                prediction_id=episode_id,
                skill_name=composite_skill,
                tenant_id="marketerhire",
                domain=cc.domain if cc else Domain.CAMPAIGN,
                expected_signal=guidance.expected_signal if guidance else 0.5,
                expected_baseline=guidance.expected_baseline if guidance else prior_cpa if prior_cpa else 1.0,
                confidence=guidance.confidence if guidance else 0.5,
                patterns_used=guidance.patterns_used if guidance else [],
                context=context,
            )

            outcome = Outcome(
                prediction_id=episode_id,
                observed_signal=wow_ratio,
                observed_baseline=prior_cpa if prior_cpa else 1.0,
                goal_completed=cpa < prior_cpa if prior_cpa else False,
                metadata={
                    "_source": "bigquery-delta",
                    "_episode_source": EpisodeSource.MARKET_OBSERVATION.value,
                    "absolute_cpa": cpa,
                    "spend": spend,
                    "ff": _f(row.get("nbr_ff")),
                    "appt": appt,
                    "mql": _f(row.get("nbr_mql")),
                    "sql_leads": _f(row.get("nbr_sql")),
                    "cs": _f(row.get("nbr_cs")),
                    "deal_value": _f(row.get("cs_deal_value")),
                    "cpa": cpa,
                },
            )

            episode = EpisodicMemory(prediction=prediction, outcome=outcome)
            try:
                self.engine.episodic.store(episode)
                stats["channel_episodes"] += 1
            except Exception as e:
                logger.warning(f"BQ channel episode {episode_id} failed: {e}")
                stats["failed"] += 1
                continue

            if not skip and guidance:
                try:
                    self.engine.learner.learn_from_outcome(prediction, outcome)
                except Exception as e:
                    logger.debug(f"Learner update failed: {e}")
            elif skip:
                stats["skipped_disqualified"] += 1

            ctr = _f(row.get("clicks", 0)) / _f(row.get("impressions", 1)) if _f(row.get("impressions", 0)) > 0 else 0
            prior_metrics_by_channel[channel] = {"cpa": cpa, "spend": spend, "ctr": ctr}
            daily_metrics_for_correlation[(channel, dt_str)] = {
                "cpa": wow_ratio, "spend": spend,
                "ctr": ctr, "impressions": _f(row.get("impressions", 0)),
            }

        # ── A.corr) Cross-metric correlations ───────────────────
        corr_count = self._detect_metric_correlations(daily_metrics_for_correlation)
        stats["correlations"] = corr_count

        # ── B) Campaign-level weekly episodes ───────────────────
        week_start = (date.fromisoformat(start)
                      - timedelta(days=date.fromisoformat(start).weekday())).isoformat()
        campaign_rows = bq.query_campaign_spend(week_start, end)

        weekly_campaigns: Dict[tuple, Dict] = defaultdict(lambda: {
            "spend": 0, "clicks": 0, "impressions": 0,
            "ff": 0, "appt": 0, "cs": 0, "revenue": 0,
            "first_date": None,
        })

        for row in campaign_rows:
            dt = row.get("dt")
            if hasattr(dt, "isocalendar"):
                iso = dt.isocalendar()
                wk_key = f"{iso[0]}-W{iso[1]:02d}"
            else:
                wk_key = str(dt)[:7]

            channel = row.get("channel", "unknown")
            campaign = row.get("campaign", "unknown")
            key = (channel, campaign, wk_key)

            weekly_campaigns[key]["spend"] += _f(row.get("spend"))
            weekly_campaigns[key]["clicks"] += _f(row.get("clicks"))
            weekly_campaigns[key]["impressions"] += _f(row.get("impressions"))
            weekly_campaigns[key]["ff"] += _f(row.get("nbr_ff"))
            weekly_campaigns[key]["appt"] += _f(row.get("nbr_appt_sch"))
            weekly_campaigns[key]["cs"] += _f(row.get("nbr_cs"))
            weekly_campaigns[key]["revenue"] += _f(row.get("total_net_rev"))
            if weekly_campaigns[key]["first_date"] is None and dt:
                weekly_campaigns[key]["first_date"] = dt

        for (channel, campaign, wk_key), m in weekly_campaigns.items():
            if m["spend"] == 0 and m["impressions"] == 0:
                continue

            channel_id = self._resolve_channel_id(channel)
            cc = self._get_channel_config(channel_id)
            local_config = self._CHANNEL_CONFIGS.get(channel, {})

            strategy = "other"
            camp_lower = campaign.lower()
            for s in local_config.get("strategies", []):
                if s.replace("-", " ") in camp_lower.replace("-", " "):
                    strategy = s
                    break

            cpa = m["spend"] / m["appt"] if m["appt"] > 0 else 0
            ctr = m["clicks"] / m["impressions"] if m["impressions"] > 0 else 0
            funnel_stage = self._infer_funnel_stage(campaign, strategy)

            maturity_days = None
            if m.get("first_date") and hasattr(m["first_date"], "isoformat"):
                try:
                    camp_first_q = bq.query(
                        f"SELECT MIN(date) as first_date FROM "
                        f"`marketerhire.dbt_mh.fact_daily_spend__by_campaign` "
                        f"WHERE campaign = '{campaign.replace(chr(39), '')}' "
                        f"AND channel = '{channel}'"
                    )
                    if camp_first_q:
                        fd = camp_first_q[0].get("first_date") if isinstance(camp_first_q, list) else camp_first_q.get("first_date")
                        if fd and hasattr(fd, "isoformat"):
                            maturity_days = (date.today() - fd).days
                except Exception:
                    pass

            composite_skill = (
                f"campaign-perf-weekly:{channel_id}:{strategy}"
                if channel_id else f"campaign-perf-weekly:{strategy}"
            )

            context = {
                **self._MH_CLIENT_CONTEXT,
                "channel": channel,
                "channel_id": channel_id,
                "campaign": campaign,
                "strategy": strategy,
                "funnel_stage": funnel_stage,
                "platform": local_config.get("platform", channel.lower()),
                "account_type": local_config.get("account_type", "unknown"),
                "week": wk_key,
                "window": "weekly",
                "source": "bigquery-campaign",
            }
            if maturity_days is not None:
                context["campaign_maturity_days"] = maturity_days

            guidance = None
            try:
                guidance = self.engine.predictor.get_guidance(
                    skill_name=composite_skill,
                    tenant_id="marketerhire",
                    domain=cc.domain if cc else Domain.CAMPAIGN,
                    context=context,
                )
            except Exception as e:
                logger.debug(f"Campaign predictor guidance failed: {e}")

            ep_raw = f"bq-campaign-{channel}-{campaign}-{wk_key}"
            ep_hash = hashlib.md5(ep_raw.encode()).hexdigest()[:12]
            episode_id = f"ep-camp-{ep_hash}"

            prediction = Prediction(
                prediction_id=episode_id,
                skill_name=composite_skill,
                tenant_id="marketerhire",
                domain=cc.domain if cc else Domain.CAMPAIGN,
                expected_signal=guidance.expected_signal if guidance else 0.5,
                expected_baseline=guidance.expected_baseline if guidance else 1.0,
                confidence=guidance.confidence if guidance else 0.5,
                patterns_used=guidance.patterns_used if guidance else [],
                context=context,
            )

            outcome = Outcome(
                prediction_id=episode_id,
                observed_signal=cpa if cpa else 0.5,
                observed_baseline=1.0,
                goal_completed=False,
                metadata={
                    "_source": "bigquery-campaign",
                    "_episode_source": EpisodeSource.MARKET_OBSERVATION.value,
                    "spend": m["spend"],
                    "clicks": m["clicks"],
                    "impressions": m["impressions"],
                    "ctr": round(ctr, 6),
                    "ff": m["ff"],
                    "appt": m["appt"],
                    "cs": m["cs"],
                    "revenue": m["revenue"],
                    "cpa": cpa,
                },
            )

            episode = EpisodicMemory(prediction=prediction, outcome=outcome)
            try:
                self.engine.episodic.store(episode)
                stats["campaign_episodes"] += 1
            except Exception as e:
                logger.warning(f"BQ campaign episode {episode_id} failed: {e}")
                stats["failed"] += 1
                continue

            if guidance:
                try:
                    self.engine.learner.learn_from_outcome(prediction, outcome)
                except Exception as e:
                    logger.debug(f"Campaign learner update failed: {e}")

        if rows:
            last_dt = rows[-1].get("dt")
            last_dt_str = (
                last_dt.isoformat()[:10]
                if hasattr(last_dt, "isoformat")
                else str(last_dt)[:10]
            )
            self._set_watermark(
                "bigquery",
                last_processed_at=datetime.now(timezone.utc).isoformat(),
                metadata={"last_date": last_dt_str},
            )

        return stats

    def _detect_metric_correlations(
        self, daily_metrics: Dict[Tuple[str, str], Dict[str, float]]
    ) -> int:
        """Detect multi-metric correlation signatures and create episodes."""
        from lib.intelligence.types import (
            Domain, EpisodicMemory, Prediction, Outcome, EpisodeSource,
        )

        count = 0
        channels_by_date: Dict[str, Dict[str, Dict[str, float]]] = defaultdict(dict)
        for (channel, dt_str), metrics in daily_metrics.items():
            channels_by_date[dt_str][channel] = metrics

        for dt_str, channels in channels_by_date.items():
            for channel, metrics in channels.items():
                for sig_name, sig in self._CORRELATION_SIGNATURES.items():
                    conditions = sig["conditions"]
                    matched = True
                    for metric, direction in conditions.items():
                        val = metrics.get(metric)
                        if val is None:
                            matched = False
                            break
                        if direction == "rising" and val <= 1.05:
                            matched = False
                        elif direction == "falling" and val >= 0.95:
                            matched = False
                        elif direction == "flat" and not (0.95 <= val <= 1.05):
                            matched = False
                    if not matched:
                        continue

                    channel_id = self._resolve_channel_id(channel)
                    ep_raw = f"corr-{sig_name}-{channel}-{dt_str}"
                    ep_hash = hashlib.md5(ep_raw.encode()).hexdigest()[:12]

                    prediction = Prediction(
                        prediction_id=f"ep-corr-{ep_hash}",
                        skill_name=sig["skill_name"],
                        tenant_id="marketerhire",
                        domain=Domain.CAMPAIGN,
                        expected_signal=0.5,
                        expected_baseline=1.0,
                        context={
                            **self._MH_CLIENT_CONTEXT,
                            "channel": channel,
                            "channel_id": channel_id,
                            "date": dt_str,
                            "correlation_type": sig_name,
                            "source": "cross-metric-correlation",
                        },
                    )
                    outcome = Outcome(
                        prediction_id=prediction.prediction_id,
                        observed_signal=1.0,
                        observed_baseline=1.0,
                        goal_completed=False,
                        metadata={
                            "_source": "cross-metric-correlation",
                            "_episode_source": EpisodeSource.MARKET_OBSERVATION.value,
                            "correlation_type": sig_name,
                            **{k: v for k, v in metrics.items()},
                        },
                    )
                    episode = EpisodicMemory(prediction=prediction, outcome=outcome)
                    try:
                        self.engine.episodic.store(episode)
                        count += 1
                    except Exception as e:
                        logger.debug(f"Correlation episode failed: {e}")
        return count

    def _get_triggerdev_covered_dates(self) -> set:
        """Return dates already covered by Trigger.dev recommendation signals."""
        covered = set()
        for skill in self._TRIGGER_SIGNAL_SKILLS:
            try:
                res = (
                    self.supabase.table("episodic_memory")
                    .select("prediction")
                    .eq("skill_name", skill)
                    .order("created_at", desc=True)
                    .limit(30)
                    .execute()
                )
                for ep in (res.data or []):
                    dt = (ep.get("prediction") or {}).get("context", {}).get("date", "")
                    if dt:
                        covered.add(dt)
            except Exception:
                pass
        return covered

    # ── Tier 4: Call Transcript Ingestion ─────────────────────────

    _SKIP_TRANSCRIPT_TYPES = frozenset({
        "freelancer_interview", "internal_sync", "vendor_call",
    })

    def _ingest_call_transcripts(self) -> Dict[str, Any]:
        """Pull processed transcripts from Supabase and create episodes."""
        stats = {"checked": 0, "episodes_created": 0, "skipped": 0}

        watermark = self._get_watermark("transcript_ingest")
        last_ts = watermark.get("last_processed_at") if watermark else None

        query = (
            self.supabase.table("transcripts")
            .select("*")
            .order("processed_at", desc=False)
            .limit(50)
        )
        if last_ts:
            query = query.gt("processed_at", last_ts)

        try:
            result = query.execute()
        except Exception as e:
            logger.debug(f"Transcripts table query failed: {e}")
            return {"skipped": True, "reason": str(e)}

        rows = result.data or []
        stats["checked"] = len(rows)

        if not rows:
            return stats

        latest_ts = last_ts
        for row in rows:
            try:
                created = self._process_transcript(row)
                stats["episodes_created"] += created
            except Exception as e:
                logger.warning(f"Transcript processing failed: {e}")
                stats["skipped"] += 1

            row_ts = row.get("processed_at", "")
            if row_ts and (not latest_ts or row_ts > latest_ts):
                latest_ts = row_ts

        if latest_ts:
            self._set_watermark("transcript_ingest", last_processed_at=latest_ts)

        return stats

    def _process_transcript(self, row: Dict[str, Any]) -> int:
        """Extract strategic signals from a single transcript row.

        Returns the number of episodic memories created.
        """
        from lib.intelligence.types import (
            Domain, EpisodicMemory, Prediction, Outcome, EpisodeSource,
        )

        intelligence = row.get("intelligence") or row.get("extracted_data") or {}
        if not intelligence:
            return 0

        meeting_type = row.get("meeting_type", "")
        if meeting_type in self._SKIP_TRANSCRIPT_TYPES:
            return 0

        entities = intelligence.get("entities", {})
        topics = intelligence.get("topics_discussed", [])
        deal_signals = intelligence.get("deal_signals", {})
        expertise = intelligence.get("expertise_signals", {})
        voc = intelligence.get("voice_of_customer", {})
        stakeholders = intelligence.get("stakeholders", [])
        key_quotes = intelligence.get("key_quotes", [])

        client_id = row.get("client_id") or self._infer_client_from_participants(stakeholders)
        transcript_id = row.get("id", str(uuid.uuid4())[:12])
        transcript_date = row.get("meeting_date") or row.get("processed_at", "")
        count = 0

        # ── Channel Strategy Episodes ──
        channels_discussed = entities.get("channels_discussed", [])
        for ch in channels_discussed:
            channel_id = self._resolve_channel_id(ch) or ch.lower()
            relevant_quotes = [
                q for q in key_quotes
                if isinstance(q, str) and ch.lower() in q.lower()
            ]

            ep_hash = hashlib.md5(
                f"call-strategy-{transcript_id}-{ch}".encode()
            ).hexdigest()[:12]

            prediction = Prediction(
                prediction_id=f"ep-call-{ep_hash}",
                skill_name=f"call-strategy-recommendation:{channel_id}",
                tenant_id=client_id or "cross-client",
                domain=Domain.CAMPAIGN,
                expected_signal=0.5,
                expected_baseline=1.0,
                context={
                    "channel_id": channel_id,
                    "channel": ch,
                    "meeting_type": meeting_type,
                    "source": "call_transcript",
                    "date": transcript_date[:10] if transcript_date else "",
                },
            )
            outcome = Outcome(
                prediction_id=prediction.prediction_id,
                observed_signal=0.7,
                observed_baseline=1.0,
                goal_completed=False,
                metadata={
                    "_source": "call_transcript",
                    "_episode_source": EpisodeSource.OPERATOR_FEEDBACK.value,
                    "_transcript_id": transcript_id,
                    "quotes": relevant_quotes[:3],
                    "topics": [t for t in topics if isinstance(t, str)][:5],
                },
            )
            episode = EpisodicMemory(prediction=prediction, outcome=outcome, weight=1.5)
            try:
                self.engine.episodic.store(episode)
                count += 1
            except Exception as e:
                logger.debug(f"Channel strategy episode failed: {e}")

        # ── Budget Allocation Episodes ──
        budget_mentions = deal_signals.get("budget_mentions", [])
        if budget_mentions or any("budget" in str(t).lower() for t in topics):
            ep_hash = hashlib.md5(
                f"call-budget-{transcript_id}".encode()
            ).hexdigest()[:12]

            prediction = Prediction(
                prediction_id=f"ep-budget-{ep_hash}",
                skill_name="call-budget-reallocation",
                tenant_id=client_id or "cross-client",
                domain=Domain.CAMPAIGN,
                expected_signal=0.5,
                expected_baseline=1.0,
                context={
                    "source": "call_transcript",
                    "meeting_type": meeting_type,
                    "channels_discussed": channels_discussed[:5],
                    "date": transcript_date[:10] if transcript_date else "",
                },
            )
            outcome = Outcome(
                prediction_id=prediction.prediction_id,
                observed_signal=0.6,
                observed_baseline=1.0,
                goal_completed=False,
                metadata={
                    "_source": "call_transcript",
                    "_episode_source": EpisodeSource.OPERATOR_FEEDBACK.value,
                    "_transcript_id": transcript_id,
                    "budget_mentions": budget_mentions[:5] if isinstance(budget_mentions, list) else [],
                },
            )
            episode = EpisodicMemory(prediction=prediction, outcome=outcome, weight=1.5)
            try:
                self.engine.episodic.store(episode)
                count += 1
            except Exception as e:
                logger.debug(f"Budget allocation episode failed: {e}")

        # ── Expert Observation Episodes ──
        case_studies = expertise.get("case_studies", [])
        frameworks = expertise.get("frameworks_mentioned", [])
        for obs in (case_studies[:3] if isinstance(case_studies, list) else []):
            obs_text = obs if isinstance(obs, str) else str(obs)
            ep_hash = hashlib.md5(
                f"call-expert-{transcript_id}-{obs_text[:30]}".encode()
            ).hexdigest()[:12]

            prediction = Prediction(
                prediction_id=f"ep-expert-{ep_hash}",
                skill_name="expert-observation",
                tenant_id=client_id or "cross-client",
                domain=Domain.GENERIC,
                expected_signal=0.5,
                expected_baseline=1.0,
                context={
                    "source": "call_transcript",
                    "meeting_type": meeting_type,
                    "date": transcript_date[:10] if transcript_date else "",
                },
            )
            outcome = Outcome(
                prediction_id=prediction.prediction_id,
                observed_signal=0.8,
                observed_baseline=1.0,
                goal_completed=True,
                metadata={
                    "_source": "call_transcript",
                    "_episode_source": EpisodeSource.OPERATOR_FEEDBACK.value,
                    "_transcript_id": transcript_id,
                    "case_study": obs_text[:500],
                    "frameworks": frameworks[:5] if isinstance(frameworks, list) else [],
                },
            )
            episode = EpisodicMemory(prediction=prediction, outcome=outcome, weight=2.0)
            try:
                self.engine.episodic.store(episode)
                count += 1
            except Exception as e:
                logger.debug(f"Expert observation episode failed: {e}")

        # ── Client Preference Episodes (procedural) ──
        pain_points = voc.get("pain_points", [])
        preferences = voc.get("preferences", [])
        if (pain_points or preferences) and client_id:
            ep_hash = hashlib.md5(
                f"call-pref-{transcript_id}".encode()
            ).hexdigest()[:12]

            prediction = Prediction(
                prediction_id=f"ep-pref-{ep_hash}",
                skill_name="client-preference",
                tenant_id=client_id,
                domain=Domain.GENERIC,
                expected_signal=1.0,
                expected_baseline=1.0,
                context={
                    "source": "call_transcript",
                    "client_id": client_id,
                    "date": transcript_date[:10] if transcript_date else "",
                },
            )
            outcome = Outcome(
                prediction_id=prediction.prediction_id,
                observed_signal=1.0,
                observed_baseline=1.0,
                goal_completed=True,
                metadata={
                    "_source": "call_transcript",
                    "_episode_source": EpisodeSource.CLIENT_FEEDBACK.value,
                    "_transcript_id": transcript_id,
                    "pain_points": pain_points[:5] if isinstance(pain_points, list) else [],
                    "preferences": preferences[:5] if isinstance(preferences, list) else [],
                },
            )
            episode = EpisodicMemory(prediction=prediction, outcome=outcome, weight=2.0)
            try:
                self.engine.episodic.store(episode)
                count += 1
            except Exception as e:
                logger.debug(f"Client preference episode failed: {e}")

        return count

    def _infer_client_from_participants(self, stakeholders: List) -> str:
        """Best-effort client_id inference from transcript stakeholders."""
        for s in (stakeholders or []):
            if isinstance(s, dict):
                company = s.get("company", "")
                if company and company.lower() not in ("marketerhire", "mh1", ""):
                    return company.lower().replace(" ", "-")
        return ""

    # ── Tier 3: Airtable Pattern Publishing ──────────────────────

    def _publish_patterns_to_airtable(self) -> Dict[str, Any]:
        """Push high-confidence patterns to Airtable after consolidation.

        Only runs if AIRTABLE_API_KEY is set. Publishes patterns with
        confidence >= 0.7 and evidence_count >= 3.
        """
        if not os.environ.get("AIRTABLE_API_KEY"):
            return {"skipped": True, "reason": "no AIRTABLE_API_KEY"}

        result = (
            self.supabase.table("semantic_patterns")
            .select("*")
            .gte("confidence", 0.7)
            .gte("evidence_count", 3)
            .is_("archived_at", "null")
            .execute()
        )
        patterns = result.data or []

        if not patterns:
            return {"total": 0, "published": 0, "reason": "no qualifying patterns"}

        watermark = self._get_watermark("airtable-publish")
        last_publish = watermark.get("last_processed_at") if watermark else None

        if last_publish:
            fresh_patterns = [
                p for p in patterns
                if (p.get("updated_at") or "") > last_publish
            ]
        else:
            fresh_patterns = patterns

        if not fresh_patterns:
            return {"total": len(patterns), "published": 0, "reason": "no new patterns since last publish"}

        from lib.airtable_publisher import AirtablePublisher
        publisher = AirtablePublisher()
        pub_stats = publisher.publish_as_recommendations(fresh_patterns)

        if pub_stats.get("published", 0) > 0:
            self._set_watermark(
                "airtable-publish",
                last_processed_at=datetime.now(timezone.utc).isoformat(),
            )

        return pub_stats

    # ── Watermark Helpers ────────────────────────────────────────

    def _get_watermark(self, source: str) -> Optional[Dict[str, Any]]:
        """Read the watermark for a given source from bm_watermarks."""
        try:
            result = (
                self.supabase.table("bm_watermarks")
                .select("*")
                .eq("source", source)
                .limit(1)
                .execute()
            )
            rows = result.data or []
            return rows[0] if rows else None
        except Exception as e:
            logger.debug(f"Watermark read failed for {source}: {e}")
            return None

    def _set_watermark(
        self,
        source: str,
        last_processed_id: Optional[str] = None,
        last_processed_at: Optional[str] = None,
        metadata: Optional[Dict] = None,
    ):
        """Upsert a watermark for a given source."""
        row: Dict[str, Any] = {
            "source": source,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        if last_processed_id is not None:
            row["last_processed_id"] = last_processed_id
        if last_processed_at is not None:
            row["last_processed_at"] = last_processed_at
        if metadata is not None:
            row["metadata"] = metadata

        try:
            self.supabase.table("bm_watermarks").upsert(
                row, on_conflict="source"
            ).execute()
        except Exception as e:
            logger.warning(f"Watermark write failed for {source}: {e}")

    # ── Guidance Cache ───────────────────────────────────────────

    def _refresh_guidance_cache(self) -> int:
        """Recompute guidance for active (skill, client) pairs."""
        pairs = self._get_active_skill_client_pairs()
        refreshed = 0

        for skill_name, client_id in pairs:
            try:
                from lib.intelligence.types import Domain as DomainEnum
                domain_str = self.bridge.get_domain_name(skill_name)
                try:
                    domain = DomainEnum(domain_str)
                except ValueError:
                    domain = DomainEnum.GENERIC

                guidance = self.engine.get_guidance(
                    skill_name=skill_name,
                    tenant_id=client_id,
                    domain=domain,
                )

                guidance_dict = guidance.to_dict() if hasattr(guidance, "to_dict") else {}

                self.supabase.table("guidance_cache").upsert({
                    "skill_name": skill_name,
                    "client_id": client_id,
                    "domain": domain_str,
                    "parameters": guidance_dict.get("parameters", {}),
                    "confidence": guidance_dict.get("confidence", 0.5),
                    "expected_value": guidance_dict.get("pattern_expected_value"),
                    "is_exploration": guidance_dict.get("is_exploration", True),
                    "patterns_used": guidance_dict.get("patterns_used", []),
                    "predicted_outcome": guidance_dict.get("predicted_outcome"),
                    "predicted_baseline": guidance_dict.get("predicted_baseline"),
                    "pattern_expected_value": guidance_dict.get("pattern_expected_value"),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }).execute()

                refreshed += 1
            except Exception as e:
                logger.debug(f"Failed to refresh guidance for {skill_name}/{client_id}: {e}")

        return refreshed

    def _get_active_skill_client_pairs(self) -> List[tuple]:
        """Get recently active (skill_name, client_id) pairs from events."""
        try:
            result = (
                self.supabase.rpc(
                    "get_active_pairs",
                    {"lookback_days": 30},
                ).execute()
            )
            if result.data:
                return [(r["skill_name"], r["client_id"]) for r in result.data]
        except Exception:
            pass

        # Fallback: query events directly
        try:
            result = (
                self.supabase.table("events")
                .select("skill_name, client_id")
                .neq("skill_name", None)
                .order("created_at", desc=True)
                .limit(200)
                .execute()
            )
            pairs = set()
            for row in (result.data or []):
                sn = row.get("skill_name")
                ci = row.get("client_id")
                if sn and ci:
                    pairs.add((sn, ci))
            return list(pairs)
        except Exception as e:
            logger.warning(f"Could not get active pairs: {e}")
            return []


    # ── Tier 5: Firebase clientData ─────────────────────────────────

    def _ingest_firebase_client_data(self) -> Dict[str, Any]:
        """Pull client dashboard metrics and execution data from Firebase clientData."""
        project_id = os.environ.get("FIREBASE_PROJECT_ID") or os.environ.get("GCP_PROJECT_ID")
        if not project_id:
            sa_json = os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON") or os.environ.get("FIREBASE_CREDENTIALS_JSON")
            if sa_json:
                import json as _json
                try:
                    project_id = _json.loads(sa_json).get("project_id")
                except Exception:
                    pass
        if not project_id:
            return {"skipped": True, "reason": "no firebase credentials"}

        from lib.firebase_client import get_firebase_client
        fb = get_firebase_client(project_id=project_id)

        watermark_key = "firebase-clientdata"
        wm_res = self.supabase.table("bm_watermarks").select("*").eq("source", watermark_key).execute()
        wm = (wm_res.data or [{}])[0] if wm_res.data else {}
        last_ts = wm.get("last_processed_at", "2020-01-01T00:00:00Z")

        stats = {"clients_scanned": 0, "episodes_created": 0, "errors": 0}

        try:
            clients = fb.get_collection("clients", limit=50)
        except Exception as e:
            logger.warning(f"Firebase: could not list clients: {e}")
            return {"skipped": True, "reason": str(e)}

        _f = lambda v: float(v) if v is not None else 0.0

        for client_doc in clients:
            client_id = client_doc.get("_id", "")
            client_name = client_doc.get("name") or client_doc.get("displayName") or client_doc.get("company_name") or client_id
            status = client_doc.get("status", "")
            if status in ("archived", "deleted"):
                continue

            stats["clients_scanned"] += 1

            subcollections = ["dashboard-metrics", "dashboard-creatives"]
            for sub_name in subcollections:
                try:
                    docs = fb.get_collection(
                        sub_name,
                        parent_collection="clientData",
                        parent_doc=client_id,
                        limit=20,
                    )
                except Exception:
                    continue

                for doc in docs:
                    doc_id = doc.get("_id", "")
                    updated = doc.get("_updated_at") or doc.get("updated_at") or doc.get("refreshed_at", "")
                    if updated and updated <= last_ts:
                        continue

                    metrics = doc.get("metrics") or doc.get("data") or {}
                    if not metrics and not doc.get("rows"):
                        continue

                    try:
                        skill_name = f"client-dashboard-{sub_name.replace('dashboard-', '')}"
                        domain_str = "campaign" if "metric" in sub_name else "content" if "creative" in sub_name else "generic"

                        from lib.intelligence.types import Domain, Prediction, Outcome, EpisodicMemory

                        try:
                            domain = Domain(domain_str)
                        except ValueError:
                            domain = Domain.GENERIC

                        ctx = {
                            "client_id": client_id,
                            "client_name": client_name,
                            "source": "firebase-clientdata",
                            "dashboard_doc": doc_id,
                            "sub_collection": sub_name,
                        }

                        summary_parts = []
                        if isinstance(metrics, dict):
                            for mk, mv in list(metrics.items())[:6]:
                                if isinstance(mv, (int, float)):
                                    ctx[mk] = mv
                                    if mv > 0:
                                        summary_parts.append(f"{mk}: {mv:,.0f}" if mv > 100 else f"{mk}: {mv:.2f}")
                        elif isinstance(metrics, list):
                            ctx["row_count"] = len(metrics)
                            summary_parts.append(f"{len(metrics)} rows")

                        if doc.get("rows"):
                            ctx["row_count"] = len(doc["rows"])
                            summary_parts.append(f"{len(doc['rows'])} creatives")

                        meta = {
                            "_episode_source": "firebase-clientdata",
                            "summary": f"{client_name} {sub_name}: {', '.join(summary_parts)}" if summary_parts else f"{client_name} {sub_name}",
                        }

                        prediction = Prediction(
                            skill_name=skill_name,
                            tenant_id="marketerhire",
                            domain=domain,
                            expected_signal=1.0,
                            expected_baseline=1.0,
                            context=ctx,
                        )
                        outcome = Outcome(
                            prediction_id=prediction.prediction_id,
                            observed_signal=1.0,
                            observed_baseline=1.0,
                            goal_completed=True,
                            metadata=meta,
                        )
                        episode = EpisodicMemory(
                            skill_name=skill_name,
                            tenant_id="marketerhire",
                            domain=domain,
                            prediction=prediction,
                            outcome=outcome,
                        )

                        ep_store = self.engine._episodic
                        ep_store.store(episode)
                        stats["episodes_created"] += 1

                    except Exception as e:
                        logger.debug(f"Firebase episode failed for {client_id}/{doc_id}: {e}")
                        stats["errors"] += 1

            try:
                exec_subs = fb.list_subcollections(f"clientData/{client_id}")
                exec_subs = [s for s in exec_subs if s.startswith("exec-")]
                for exec_sub in exec_subs[:5]:
                    try:
                        exec_docs = fb.get_collection(
                            exec_sub,
                            parent_collection="clientData",
                            parent_doc=client_id,
                            limit=5,
                        )
                    except Exception:
                        continue

                    for doc in exec_docs:
                        doc_id = doc.get("_id", "")
                        if doc_id != "latest":
                            continue
                        updated = doc.get("_updated_at") or ""
                        if updated and updated <= last_ts:
                            continue

                        result_data = doc.get("result") or doc.get("data") or {}
                        if not result_data:
                            continue

                        try:
                            skill_label = exec_sub.replace("exec-", "")
                            from lib.intelligence.types import Domain, Prediction, Outcome, EpisodicMemory

                            ctx = {
                                "client_id": client_id,
                                "client_name": client_name,
                                "source": "firebase-clientdata",
                                "exec_skill": skill_label,
                            }
                            summary = result_data.get("summary", "") or f"{client_name} {skill_label} execution"
                            meta = {
                                "_episode_source": "firebase-clientdata",
                                "summary": summary[:200] if isinstance(summary, str) else str(summary)[:200],
                            }

                            prediction = Prediction(
                                skill_name=f"client-exec-{skill_label}",
                                tenant_id="marketerhire",
                                domain=Domain.GENERIC,
                                expected_signal=1.0,
                                expected_baseline=1.0,
                                context=ctx,
                            )
                            outcome = Outcome(
                                prediction_id=prediction.prediction_id,
                                observed_signal=1.0,
                                observed_baseline=1.0,
                                goal_completed=True,
                                metadata=meta,
                            )
                            episode = EpisodicMemory(
                                skill_name=f"client-exec-{skill_label}",
                                tenant_id="marketerhire",
                                domain=Domain.GENERIC,
                                prediction=prediction,
                                outcome=outcome,
                            )

                            ep_store = self.engine._episodic
                            ep_store.store(episode)
                            stats["episodes_created"] += 1

                        except Exception as e:
                            logger.debug(f"Firebase exec episode failed: {e}")
                            stats["errors"] += 1

            except Exception as e:
                logger.debug(f"Firebase exec subcollections failed for {client_id}: {e}")

        now_iso = datetime.now(timezone.utc).isoformat()
        self.supabase.table("bm_watermarks").upsert({
            "source": watermark_key,
            "last_processed_at": now_iso,
            "updated_at": now_iso,
        }, on_conflict="source").execute()

        return stats


def main():
    parser = argparse.ArgumentParser(description="BrightMatter cron worker")
    parser.add_argument("--loop", action="store_true", help="Run continuously")
    parser.add_argument("--interval", type=int, default=900, help="Seconds between cycles (default 15 min)")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    worker = BrightMatterWorker()

    if args.loop:
        logger.info(f"Starting continuous worker (interval={args.interval}s)")
        while True:
            try:
                stats = worker.run_cycle()
                logger.info(f"Cycle complete: {stats}")
            except Exception as e:
                logger.error(f"Cycle error: {e}")
            time.sleep(args.interval)
    else:
        stats = worker.run_cycle()
        logger.info(f"Single cycle complete: {stats}")
        return stats


if __name__ == "__main__":
    main()
