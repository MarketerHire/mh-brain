"""Unit tests for the Google Search Console adapter.

Mocks every outbound HTTP call so the test suite never touches the real
GSC or OAuth endpoints. Asserts:

    - DailyMetricRow shape: one row per day for the site-total query, plus
      additional rows tagged with breakdown=query/page/device.
    - PII allowlist enforcement: extra fields appended to a mock response
      are stripped before they reach the row payload.
    - Rate-limit delay: time.sleep is invoked between paginated calls.
    - Pagination: the adapter pages through the API until the response is
      shorter than rowLimit.
"""

from __future__ import annotations

import json
import unittest
from datetime import date
from io import BytesIO
from typing import Any, Dict, List, Tuple
from unittest import mock

from lib.platform_ingestion.adapters.base import PlatformConfig
from lib.platform_ingestion.adapters import gsc as gsc_mod
from lib.platform_ingestion.adapters.gsc import (
    GoogleSearchConsoleAdapter,
    _SAFE_FIELDS,
    _strip_to_safe,
)


def _fake_response(payload: Dict[str, Any]):
    body = json.dumps(payload).encode()
    fp = BytesIO(body)
    fp.read = fp.read  # noqa: just keep urlopen-compatible
    return fp


class FakeUrlopen:
    """Replays a queue of fake responses keyed by URL substring."""

    def __init__(self, route_table: List[Tuple[str, Dict[str, Any]]]):
        # Each entry: (url_substring_match, payload). Consumed in order
        # for any matching call.
        self.route_table = list(route_table)
        self.calls: List[Tuple[str, Any]] = []

    def __call__(self, request, timeout=None):  # noqa: signature mirrors urlopen
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

        # Default: empty response
        return _fake_response({"rows": []})


class GSCAdapterTests(unittest.TestCase):

    def setUp(self):
        self.adapter = GoogleSearchConsoleAdapter()
        self.config = PlatformConfig(
            platform="gsc",
            client_id="acme",
            client_name="Acme Co",
            credentials={
                "client_id": "oauth-id",
                "client_secret": "oauth-secret",
                "refresh_token": "refresh-token",
                "site_url": "sc-domain:acme.com",
            },
            extra={
                "row_limit_queries": 5,
                "row_limit_pages": 5,
            },
        )

    def _build_routes(self) -> List[Tuple[str, Dict[str, Any]]]:
        # Token exchange
        routes: List[Tuple[str, Dict[str, Any]]] = [
            ("oauth2.googleapis.com/token", {"access_token": "test-access"}),
        ]

        # Daily site totals (single page).
        routes.append((
            "searchAnalytics/query",
            {
                "rows": [
                    {
                        "keys": ["2025-01-01"],
                        "clicks": 10, "impressions": 100,
                        "ctr": 0.10, "position": 7.0,
                        # PII red-team: extra field that must be stripped.
                        "user_email": "leak@example.com",
                    },
                    {
                        "keys": ["2025-01-02"],
                        "clicks": 12, "impressions": 120,
                        "ctr": 0.10, "position": 6.5,
                    },
                ],
            },
        ))

        # Per-day query breakdown — 2 days × 1 page each.
        routes.append((
            "searchAnalytics/query",
            {"rows": [
                {"keys": ["term-a"], "clicks": 4, "impressions": 40,
                 "ctr": 0.10, "position": 8.0},
                {"keys": ["term-b"], "clicks": 3, "impressions": 30,
                 "ctr": 0.10, "position": 9.0},
            ]},
        ))
        routes.append((
            "searchAnalytics/query",
            {"rows": [
                {"keys": ["term-a"], "clicks": 5, "impressions": 50,
                 "ctr": 0.10, "position": 7.0},
            ]},
        ))

        # Per-day page breakdown — 2 days × 1 page each.
        routes.append((
            "searchAnalytics/query",
            {"rows": [
                {"keys": ["/foo"], "clicks": 7, "impressions": 70,
                 "ctr": 0.10, "position": 6.0},
            ]},
        ))
        routes.append((
            "searchAnalytics/query",
            {"rows": [
                {"keys": ["/bar"], "clicks": 8, "impressions": 80,
                 "ctr": 0.10, "position": 5.0},
            ]},
        ))

        # Per-day device breakdown.
        routes.append((
            "searchAnalytics/query",
            {"rows": [
                {"keys": ["MOBILE"], "clicks": 6, "impressions": 60,
                 "ctr": 0.10, "position": 7.5},
                {"keys": ["DESKTOP"], "clicks": 4, "impressions": 40,
                 "ctr": 0.10, "position": 6.5},
            ]},
        ))
        routes.append((
            "searchAnalytics/query",
            {"rows": [
                {"keys": ["MOBILE"], "clicks": 8, "impressions": 80,
                 "ctr": 0.10, "position": 6.0},
            ]},
        ))

        return routes

    def test_pii_allowlist_strips_unknown_fields(self):
        raw = [
            {
                "keys": ["2025-01-01"],
                "clicks": 1,
                "impressions": 2,
                "ctr": 0.5,
                "position": 1.5,
                "user_email": "leak@example.com",
                "ipAddress": "192.0.2.1",
                "session_id": "abc123",
            }
        ]
        safe = _strip_to_safe(raw, "row")
        self.assertEqual(set(safe[0].keys()), _SAFE_FIELDS["row"])
        self.assertNotIn("user_email", safe[0])
        self.assertNotIn("ipAddress", safe[0])

    def test_pull_daily_metrics_emits_rows_per_breakdown(self):
        routes = self._build_routes()
        fake_urlopen = FakeUrlopen(routes)

        with mock.patch.object(gsc_mod.urllib.request, "urlopen", fake_urlopen), \
             mock.patch.object(gsc_mod.time, "sleep") as fake_sleep, \
             mock.patch.object(
                 GoogleSearchConsoleAdapter, "_write_to_bq", return_value=None,
             ):
            rows = self.adapter.pull_daily_metrics(
                self.config,
                start_date=date(2025, 1, 1),
                end_date=date(2025, 1, 2),
            )

        # Build a tally by breakdown.
        by_breakdown: Dict[str, int] = {}
        for r in rows:
            key = r.breakdown or "site_total"
            by_breakdown[key] = by_breakdown.get(key, 0) + 1

        # 2 daily site totals (2 days), plus 3 query rows, 2 page rows,
        # 3 device rows.
        self.assertEqual(by_breakdown.get("site_total"), 2)
        self.assertEqual(by_breakdown.get("query"), 3)
        self.assertEqual(by_breakdown.get("page"), 2)
        self.assertEqual(by_breakdown.get("device"), 3)

        # PII red-team: any extra fields appended to the mock response
        # should never appear in the metrics payload.
        for r in rows:
            for field in ("user_email", "ipAddress", "session_id"):
                self.assertNotIn(field, r.metrics)

        # Site total payload carries the four GSC metrics.
        site_totals = [r for r in rows if r.breakdown is None]
        self.assertTrue(site_totals)
        self.assertEqual(
            set(site_totals[0].metrics.keys()),
            {"clicks", "impressions", "ctr", "position"},
        )

        # Rate-limit delay should be invoked at least once across the
        # paginated/per-day requests.
        self.assertTrue(fake_sleep.called)

    def test_pagination_pages_until_short_response(self):
        # Token exchange + two-page result for the daily query.
        full_page_rows = [
            {"keys": [f"2025-01-{d:02d}"], "clicks": d, "impressions": d * 10,
             "ctr": 0.1, "position": 5.0}
            for d in range(1, 6)  # 5 rows == row_limit on page 1
        ]
        short_page_rows = [
            {"keys": ["2025-01-06"], "clicks": 6, "impressions": 60,
             "ctr": 0.1, "position": 5.0},
        ]
        token_payload = {"access_token": "test-access"}

        # The adapter requests rowLimit==DEFAULT_ROW_LIMIT for the daily
        # query, so we monkeypatch DEFAULT_ROW_LIMIT for this test only.
        with mock.patch.object(GoogleSearchConsoleAdapter, "DEFAULT_ROW_LIMIT", 5):
            fake = FakeUrlopen([
                ("oauth2.googleapis.com/token", token_payload),
                ("searchAnalytics/query", {"rows": full_page_rows}),
                ("searchAnalytics/query", {"rows": short_page_rows}),
                # Subsequent breakdowns immediately return empty so the test
                # only measures the daily-query pagination.
                ("searchAnalytics/query", {"rows": []}),
                ("searchAnalytics/query", {"rows": []}),
                ("searchAnalytics/query", {"rows": []}),
                ("searchAnalytics/query", {"rows": []}),
                ("searchAnalytics/query", {"rows": []}),
                ("searchAnalytics/query", {"rows": []}),
            ])

            with mock.patch.object(gsc_mod.urllib.request, "urlopen", fake), \
                 mock.patch.object(gsc_mod.time, "sleep"), \
                 mock.patch.object(
                     GoogleSearchConsoleAdapter, "_write_to_bq", return_value=None,
                 ):
                rows = self.adapter.pull_daily_metrics(
                    self.config,
                    start_date=date(2025, 1, 1),
                    end_date=date(2025, 1, 6),
                )

        site_totals = [r for r in rows if r.breakdown is None]
        # 5 + 1 = 6 unique daily-site-total rows from pagination.
        self.assertEqual(len(site_totals), 6)


if __name__ == "__main__":
    unittest.main()
