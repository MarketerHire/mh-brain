"""Unit tests for the YouTube Analytics adapter.

Mirrors the GSC adapter tests: mocks every outbound HTTP call, asserts
DailyMetricRow shape across the channel-total / video / traffic-source /
country breakdowns, asserts the PII allowlist drops anything that isn't
in ``_SAFE_DIMENSIONS`` ∪ ``_SAFE_METRICS``, and asserts the rate-limit
delay is invoked between API calls.
"""

from __future__ import annotations

import json
import unittest
from datetime import date
from io import BytesIO
from typing import Any, Dict, List, Tuple
from unittest import mock

from lib.platform_ingestion.adapters.base import PlatformConfig
from lib.platform_ingestion.adapters import youtube_analytics as yt_mod
from lib.platform_ingestion.adapters.youtube_analytics import (
    YouTubeAnalyticsAdapter,
    _SAFE_DIMENSIONS,
    _SAFE_METRICS,
    _strip_to_safe_row,
)


def _fake_response(payload: Dict[str, Any]):
    return BytesIO(json.dumps(payload).encode())


class FakeUrlopen:
    def __init__(self, route_table: List[Tuple[str, Dict[str, Any]]]):
        self.route_table = list(route_table)
        self.calls: List[Tuple[str, Any]] = []

    def __call__(self, request, timeout=None):
        url = request.full_url if hasattr(request, "full_url") else str(request)
        body = None
        if hasattr(request, "data") and request.data:
            try:
                body = json.loads(request.data.decode())
            except Exception:
                body = request.data
        self.calls.append((url, body))

        for i, (substr, payload) in enumerate(self.route_table):
            if substr in url:
                self.route_table.pop(i)
                return _fake_response(payload)
        return _fake_response({"rows": []})


class YouTubeAdapterTests(unittest.TestCase):

    def setUp(self):
        self.adapter = YouTubeAnalyticsAdapter()
        self.config = PlatformConfig(
            platform="youtube_analytics",
            client_id="acme",
            client_name="Acme Co",
            credentials={
                "client_id": "oauth-id",
                "client_secret": "oauth-secret",
                "refresh_token": "refresh-token",
            },
            account_id="UCabcdef123456",
            extra={
                "top_videos_per_day": 2,
                "top_countries_per_day": 2,
            },
        )

    def _build_routes(self) -> List[Tuple[str, Dict[str, Any]]]:
        routes: List[Tuple[str, Dict[str, Any]]] = []
        # Token exchange
        routes.append((
            "oauth2.googleapis.com/token",
            {"access_token": "test-access"},
        ))

        # 1. Channel daily totals (2 days).
        channel_headers = [
            {"name": h} for h in [
                "day", "views", "estimatedMinutesWatched",
                "subscribersGained", "subscribersLost", "likes",
                "comments", "shares", "averageViewDuration",
                "averageViewPercentage",
            ]
        ]
        routes.append((
            "youtubeanalytics.googleapis.com/v2/reports",
            {
                "columnHeaders": channel_headers,
                "rows": [
                    ["2025-01-01", 1000, 500.0, 5, 1, 50, 10, 5, 30.0, 25.0],
                    ["2025-01-02", 2000, 800.0, 8, 2, 80, 15, 7, 35.0, 28.0],
                ],
            },
        ))

        # 2. Top videos (day, video) — 3 videos for day 1, 1 for day 2.
        video_headers = [
            {"name": h} for h in ["day", "video", "views", "estimatedMinutesWatched"]
        ]
        routes.append((
            "youtubeanalytics.googleapis.com/v2/reports",
            {
                "columnHeaders": video_headers,
                "rows": [
                    ["2025-01-01", "vid_a", 500, 200.0],
                    ["2025-01-01", "vid_b", 300, 120.0],
                    ["2025-01-01", "vid_c", 200, 80.0],
                    ["2025-01-02", "vid_a", 1000, 400.0],
                ],
            },
        ))

        # 3. Traffic sources (day, insightTrafficSourceType).
        traffic_headers = [
            {"name": h} for h in ["day", "insightTrafficSourceType", "views"]
        ]
        routes.append((
            "youtubeanalytics.googleapis.com/v2/reports",
            {
                "columnHeaders": traffic_headers,
                "rows": [
                    ["2025-01-01", "YT_SEARCH", 600],
                    ["2025-01-01", "SUGGESTED_VIDEO", 300],
                    ["2025-01-02", "YT_SEARCH", 1500],
                ],
            },
        ))

        # 4. Country breakdown (day, country) — 3 countries for day 1.
        country_headers = [
            {"name": h} for h in ["day", "country", "views"]
        ]
        routes.append((
            "youtubeanalytics.googleapis.com/v2/reports",
            {
                "columnHeaders": country_headers,
                "rows": [
                    ["2025-01-01", "US", 700],
                    ["2025-01-01", "GB", 200],
                    ["2025-01-01", "DE", 100],
                    ["2025-01-02", "US", 1800],
                ],
            },
        ))

        return routes

    def test_pii_allowlist_strips_unknown_fields(self):
        headers = ["day", "views", "user_id", "viewerEmail"]
        row = ["2025-01-01", 100, "U-leak", "leak@example.com"]
        safe = _strip_to_safe_row(row, headers)
        self.assertIsNotNone(safe)
        self.assertIn("day", safe)
        self.assertIn("views", safe)
        self.assertNotIn("user_id", safe)
        self.assertNotIn("viewerEmail", safe)

    def test_pull_daily_metrics_emits_rows_per_breakdown(self):
        routes = self._build_routes()
        fake = FakeUrlopen(routes)

        with mock.patch.object(yt_mod.urllib.request, "urlopen", fake), \
             mock.patch.object(yt_mod.time, "sleep") as fake_sleep, \
             mock.patch.object(
                 YouTubeAnalyticsAdapter, "_write_to_bq", return_value=None,
             ):
            rows = self.adapter.pull_daily_metrics(
                self.config,
                start_date=date(2025, 1, 1),
                end_date=date(2025, 1, 2),
            )

        by_breakdown: Dict[str, int] = {}
        for r in rows:
            key = r.breakdown or "channel_total"
            by_breakdown[key] = by_breakdown.get(key, 0) + 1

        # 2 channel totals, top_videos_per_day=2 → 2+1 video rows,
        # 3 traffic-source rows, top_countries_per_day=2 → 2+1 country rows.
        self.assertEqual(by_breakdown.get("channel_total"), 2)
        self.assertEqual(by_breakdown.get("video_id"), 3)
        self.assertEqual(by_breakdown.get("traffic_source"), 3)
        self.assertEqual(by_breakdown.get("country"), 3)

        # Channel total payload carries the full set of metrics.
        channel_rows = [r for r in rows if r.breakdown is None]
        self.assertTrue(channel_rows)
        self.assertEqual(channel_rows[0].metrics["views"], 1000)
        self.assertEqual(channel_rows[0].metrics["watch_time_minutes"], 500.0)
        self.assertEqual(channel_rows[0].metrics["channel_id"], "UCabcdef123456")

        # Top-video filtering: per-day cap honored.
        video_rows_day1 = [
            r for r in rows
            if r.breakdown == "video_id" and r.metric_date == date(2025, 1, 1)
        ]
        self.assertEqual(len(video_rows_day1), 2)
        # Highest-view video must be present (vid_a / 500 views).
        self.assertEqual(
            sorted(r.metrics["video_id"] for r in video_rows_day1),
            ["vid_a", "vid_b"],
        )

        # Rate-limit delay must be invoked between report calls.
        sleeps_called = [c for c in fake_sleep.call_args_list if c.args == (0.5,)]
        self.assertGreaterEqual(len(sleeps_called), 1)

    def test_dimension_allowlist_rejects_unknown(self):
        # The query helper must raise if asked for an unsafe dimension.
        adapter = YouTubeAnalyticsAdapter()
        with self.assertRaises(ValueError):
            adapter._query_report(
                access_token="t",
                params={
                    "ids": "channel==UC",
                    "startDate": "2025-01-01",
                    "endDate": "2025-01-02",
                    "metrics": "views",
                    "dimensions": "subscribedStatus",  # not allowlisted
                },
            )

    def test_safe_constants_match_expected_set(self):
        # Tighten the allowlists with explicit assertions so accidental
        # widening trips a test.
        self.assertEqual(
            _SAFE_DIMENSIONS,
            {"day", "video", "insightTrafficSourceType", "country"},
        )
        self.assertIn("views", _SAFE_METRICS)
        self.assertIn("estimatedMinutesWatched", _SAFE_METRICS)
        # Sanity check: nothing user-identifying snuck in.
        for forbidden in ("user_id", "viewerEmail", "ipAddress"):
            self.assertNotIn(forbidden, _SAFE_DIMENSIONS)
            self.assertNotIn(forbidden, _SAFE_METRICS)


if __name__ == "__main__":
    unittest.main()
