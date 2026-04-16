"""
Checkpoint Processor

Runs on a cron schedule. Finds pending outcomes that are due for
measurement (24h, 7d, or channel-specific intervals), collects
platform metrics using the delivery_metadata stored at tracking time,
and closes the deferred outcome to trigger learning.

Data sources (in priority order):
1. MH1HQ MCP — query execution results, report data
2. Platform APIs via delivery_metadata — campaign_id, ad_set_id
3. MH-OS signals (future) — BigQuery metrics, anomaly context
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from .pending_store import PendingOutcome, PendingOutcomeStore

logger = logging.getLogger(__name__)


class CheckpointProcessor:
    """
    Processes due outcome checkpoints on a cron schedule.

    Queries PendingOutcomeStore for outcomes whose 24h or 7d windows have
    elapsed, collects metrics from available data sources, and closes the
    loop by calling bridge.close_deferred_outcome().
    """

    def __init__(self, firebase_client: Any, bridge: Any, supabase_client: Any = None):
        self._store = PendingOutcomeStore(firebase_client)
        self._bridge = bridge
        self._firebase = firebase_client
        self._supabase = supabase_client
        self._mhos_supabase = None

    @property
    def supabase(self):
        if self._supabase is None:
            try:
                from lib.supabase_client import get_supabase_or_none
                self._supabase = get_supabase_or_none()
            except Exception:
                pass
        return self._supabase

    @property
    def mhos_supabase(self):
        """MH-OS Supabase for reading shared event bus tables."""
        if self._mhos_supabase is None:
            try:
                from lib.supabase_client import get_mhos_supabase_or_none
                self._mhos_supabase = get_mhos_supabase_or_none()
            except Exception:
                pass
        return self._mhos_supabase

    def process_all_due(self) -> Dict[str, Any]:
        """Process all due checkpoints. Called by cron."""
        results = {"processed": 0, "failed": 0, "skipped": 0, "details": []}

        for checkpoint_type in ["24h", "7d"]:
            due = self._store.query_due(checkpoint_type)
            logger.info(
                f"Checkpoint {checkpoint_type}: {len(due)} outcomes due"
            )

            for pending in due:
                try:
                    metrics = self._collect_metrics(pending)

                    if not metrics:
                        results["skipped"] += 1
                        results["details"].append({
                            "prediction_id": pending.prediction_id,
                            "action": "skipped",
                            "reason": "no_metrics_collected",
                        })
                        continue

                    classification = self._classify(
                        pending, metrics["primary_signal"]
                    )

                    # Tag external context (seasonality, anomalies, etc.)
                    external_tags = self._tag_external_context(
                        pending, metrics
                    )
                    if external_tags:
                        metrics["_external_context"] = external_tags
                        if external_tags.get("anomaly_flag"):
                            metrics["_anomaly_discount"] = 0.3

                    self._bridge.close_deferred_outcome(
                        prediction_id=pending.prediction_id,
                        client_id=pending.client_id,
                        observed_signal=metrics["primary_signal"],
                        business_impact=metrics.get("business_impact", 0),
                        platform_metrics=metrics,
                        projection_classification=classification,
                    )

                    # Record checkpoint in store to advance status
                    from .pending_store import OutcomeCheckpoint
                    self._store.record_checkpoint(
                        client_id=pending.client_id,
                        prediction_id=pending.prediction_id,
                        checkpoint=OutcomeCheckpoint(
                            checkpoint_type=checkpoint_type,
                            platform_metrics=metrics,
                            composite_signal=metrics["primary_signal"],
                        ),
                    )

                    if checkpoint_type == "7d":
                        self._store.close(
                            client_id=pending.client_id,
                            prediction_id=pending.prediction_id,
                            composite_score=metrics["primary_signal"],
                            projection_classification=classification,
                        )

                    results["processed"] += 1
                    results["details"].append({
                        "prediction_id": pending.prediction_id,
                        "action": "processed",
                        "checkpoint": checkpoint_type,
                        "classification": classification,
                        "signal": metrics["primary_signal"],
                    })

                except Exception as e:
                    logger.error(
                        f"Checkpoint failed for {pending.prediction_id}: {e}"
                    )
                    results["failed"] += 1
                    results["details"].append({
                        "prediction_id": pending.prediction_id,
                        "action": "failed",
                        "error": str(e),
                    })

        # Expire stale outcomes that were never resolved
        expired = self._store.expire_stale(max_age_days=14)
        results["expired"] = expired

        logger.info(
            f"Checkpoint processing complete: "
            f"processed={results['processed']}, "
            f"failed={results['failed']}, "
            f"skipped={results['skipped']}, "
            f"expired={expired}"
        )
        return results

    def _collect_metrics(
        self, pending: PendingOutcome
    ) -> Optional[Dict[str, Any]]:
        """Collect platform metrics using delivery_metadata."""
        delivery = pending.delivery_metadata or {}
        metrics: Dict[str, Any] = {}

        # Route 1: MH1HQ MCP — query skill run results
        if delivery.get("skill_run_id") or delivery.get("module_id"):
            try:
                run_data = self._query_mh1hq(delivery)
                if run_data:
                    metrics.update(run_data)
            except Exception as e:
                logger.debug(f"MH1HQ query failed: {e}")

        # Route 2: Direct platform metrics via campaign/ad IDs
        if delivery.get("campaign_id") or delivery.get("ad_set_id"):
            try:
                platform_data = self._query_platform(delivery)
                if platform_data:
                    metrics.update(platform_data)
            except Exception as e:
                logger.debug(f"Platform query failed: {e}")

        # Route 3: Report URL — check if report was viewed/actioned
        if delivery.get("report_url"):
            try:
                report_data = self._check_report(delivery)
                if report_data:
                    metrics.update(report_data)
            except Exception as e:
                logger.debug(f"Report check failed: {e}")

        # Compute primary signal from collected metrics
        if metrics:
            channel_config = None
            if pending.channel_id:
                try:
                    from lib.intelligence.adapters.channels import (
                        get_channel_config,
                    )
                    channel_config = get_channel_config(pending.channel_id)
                except ImportError:
                    pass

            try:
                from lib.intelligence.outcomes.signal_computer import (
                    CompositeSignalComputer,
                )
                computer = CompositeSignalComputer()

                expected_score = pending.prediction.get("expected_signal", 0.5)
                expected_baseline = pending.prediction.get("expected_baseline", 1.0)
                if expected_baseline > 0:
                    expected_ratio = expected_score / expected_baseline
                else:
                    expected_ratio = expected_score

                composite = computer.compute(
                    skill_name=pending.skill_name,
                    expected_score=expected_ratio,
                    platform_metrics=metrics,
                    channel_config=channel_config,
                )
                metrics["primary_signal"] = composite.composite_score
                metrics["projection_classification"] = composite.projection_classification
            except Exception as e:
                logger.debug(f"Composite signal computation failed: {e}")
                # Fallback: use first numeric metric as primary signal
                for v in metrics.values():
                    if isinstance(v, (int, float)):
                        metrics["primary_signal"] = float(v)
                        break

        return metrics if metrics.get("primary_signal") is not None else None

    def _classify(self, pending: PendingOutcome, observed_signal: float) -> str:
        """Classify outcome vs prediction."""
        expected = pending.prediction.get("expected_signal", 0.5)
        if expected == 0:
            return "accurate_projection"
        ratio = observed_signal / expected if expected != 0 else 1.0
        if ratio < 0.8:
            return "under_projection"
        elif ratio > 1.2:
            return "over_projection"
        return "accurate_projection"

    def _tag_external_context(
        self,
        pending: PendingOutcome,
        metrics: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Check for external events and tag the episode.

        Tags include seasonality, day-of-week context, and anomaly flags
        from MH-OS (when available). Episodes tagged with anomaly_flag
        get a reduced learning weight so external noise doesn't corrupt
        pattern confidence.
        """
        from datetime import datetime as dt

        tags: Dict[str, Any] = {}

        try:
            created = dt.fromisoformat(
                pending.created_at.replace("Z", "+00:00")
            )
        except (ValueError, AttributeError):
            return tags

        month = created.month
        if month in [11, 12]:
            tags["seasonality"] = "holiday_q4"
        elif month in [1, 2]:
            tags["seasonality"] = "post_holiday_q1"
        elif month in [6, 7, 8]:
            tags["seasonality"] = "summer"

        dow = created.weekday()
        if dow >= 5:
            tags["day_context"] = "weekend"

        # MH-OS anomaly check (Supabase query when available)
        try:
            anomalies = self._check_mh_os_anomalies(
                client_id=pending.client_id,
                start_date=pending.created_at,
                end_date=metrics.get("measured_at", pending.created_at),
            )
            if anomalies:
                tags["external_anomalies"] = anomalies
                tags["anomaly_flag"] = True
        except Exception:
            pass

        if metrics.get("platform_status") == "degraded":
            tags["platform_issue"] = True
            tags["anomaly_flag"] = True

        return tags

    def _check_mh_os_anomalies(
        self,
        client_id: str,
        start_date: str,
        end_date: str,
    ) -> Optional[List[Dict[str, Any]]]:
        """Query MH-OS Supabase for severity signals that overlap this window."""
        if not self.mhos_supabase:
            return None
        try:
            result = (
                self.mhos_supabase.table("events")
                .select("result, metrics, context, created_at")
                .eq("source", "mh-os")
                .eq("event_type", "signal")
                .eq("client_id", client_id)
                .gte("created_at", start_date)
                .lte("created_at", end_date)
                .execute()
            )
            anomalies = []
            for row in (result.data or []):
                severity = (
                    (row.get("result") or {}).get("severity")
                    or (row.get("metrics") or {}).get("deviation_severity")
                )
                if severity in ("warning", "critical"):
                    anomalies.append({
                        "severity": severity,
                        "metrics": row.get("metrics", {}),
                        "created_at": row.get("created_at", ""),
                    })
            return anomalies if anomalies else None
        except Exception as e:
            logger.debug(f"MH-OS anomaly query failed: {e}")
            return None

    def _query_mh1hq(self, delivery: Dict[str, Any]) -> Optional[Dict]:
        """Query MH-OS events table for MH1HQ skill execution results."""
        if not self.mhos_supabase:
            return None
        try:
            skill_run_id = delivery.get("skill_run_id", "")
            module_id = delivery.get("module_id", "")
            client_id = delivery.get("client_id", "")

            query = (
                self.mhos_supabase.table("events")
                .select("result, metrics, context")
                .eq("source", "mh1-hq")
                .eq("event_type", "skill_completed")
            )
            if client_id:
                query = query.eq("client_id", client_id)

            result = query.order("created_at", desc=True).limit(5).execute()

            for row in (result.data or []):
                ctx = row.get("context") or {}
                if skill_run_id and ctx.get("skill_run_id") == skill_run_id:
                    return {**(row.get("metrics") or {}), **(row.get("result") or {})}
                if module_id and ctx.get("module_id") == module_id:
                    return {**(row.get("metrics") or {}), **(row.get("result") or {})}

            # If no exact match, return most recent result for this client
            if result.data:
                row = result.data[0]
                return {**(row.get("metrics") or {}), **(row.get("result") or {})}

            return None
        except Exception as e:
            logger.debug(f"MH1HQ query failed: {e}")
            return None

    def _query_platform(self, delivery: Dict[str, Any]) -> Optional[Dict]:
        """Query Supabase for platform metrics by campaign/ad set ID."""
        if not self.supabase:
            return None
        try:
            campaign_id = delivery.get("campaign_id", "")
            ad_set_id = delivery.get("ad_set_id", "")

            result = (
                self.mhos_supabase.table("events")
                .select("result, metrics")
                .eq("event_type", "platform_metrics")
                .order("created_at", desc=True)
                .limit(10)
                .execute()
            )

            for row in (result.data or []):
                ctx = row.get("context") or row.get("result") or {}
                if campaign_id and ctx.get("campaign_id") == campaign_id:
                    return row.get("metrics") or row.get("result")
                if ad_set_id and ctx.get("ad_set_id") == ad_set_id:
                    return row.get("metrics") or row.get("result")

            return None
        except Exception as e:
            logger.debug(f"Platform query failed: {e}")
            return None

    def _check_report(self, delivery: Dict[str, Any]) -> Optional[Dict]:
        """Query MH-OS Supabase for report engagement events."""
        if not self.mhos_supabase:
            return None
        try:
            report_url = delivery.get("report_url", "")
            if not report_url:
                return None

            result = (
                self.mhos_supabase.table("events")
                .select("event_type, result, metrics, created_at")
                .in_("event_type", ["report_viewed", "report_shared", "report_feedback"])
                .order("created_at", desc=True)
                .execute()
            )

            views = 0
            shared = False
            scroll_depth = 0.0
            first_view_at = None

            for row in (result.data or []):
                ctx = row.get("context") or row.get("result") or {}
                if ctx.get("report_url") != report_url:
                    continue

                if row.get("event_type") == "report_viewed":
                    views += 1
                    if first_view_at is None:
                        first_view_at = row.get("created_at", "")
                    metrics = row.get("metrics") or {}
                    sd = metrics.get("scroll_depth_pct", 0)
                    if isinstance(sd, (int, float)) and sd > scroll_depth:
                        scroll_depth = float(sd)

                elif row.get("event_type") == "report_shared":
                    shared = True

            if views == 0:
                return None

            report_metrics: Dict[str, Any] = {
                "report_views": views,
                "shared": shared,
                "scroll_depth_pct": scroll_depth,
            }

            if first_view_at and delivery.get("delivered_at"):
                try:
                    from datetime import datetime
                    delivered = datetime.fromisoformat(
                        delivery["delivered_at"].replace("Z", "+00:00")
                    )
                    viewed = datetime.fromisoformat(
                        first_view_at.replace("Z", "+00:00")
                    )
                    hours = (viewed - delivered).total_seconds() / 3600
                    report_metrics["time_to_first_view_hours"] = round(hours, 1)
                except Exception:
                    pass

            return report_metrics
        except Exception as e:
            logger.debug(f"Report check failed: {e}")
            return None


__all__ = ["CheckpointProcessor"]
