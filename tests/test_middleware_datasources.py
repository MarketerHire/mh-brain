"""Tests for the middleware-fetch path in ``orchestrator._load_datasources``.

These exercise the new primary path (middleware) and verify the fallback
chain still works for clients that haven't been migrated yet.

Specifically asserted:

    - ``_middleware_response_to_datasources`` produces the legacy
      ``datasources.json`` shape, with platform aliases resolved (e.g.
      "Google Search Console" → ``gsc``) and ``status: "connected"`` set
      so ``detect_platforms`` keeps the entry.
    - ``detect_platforms`` finds the platform after conversion and returns
      a config dict that carries the per-client field (``site_url``).
    - ``_load_datasources`` calls the middleware first when ``MH1_API_URL``
      and ``MH1_API_KEY`` are set, and short-circuits before touching
      Firebase.
    - On HTTP 404 / network error / unset env, the middleware path
      returns ``None`` and the orchestrator falls through to Firebase.
"""

from __future__ import annotations

import json
import unittest
import urllib.error
from io import BytesIO
from typing import Any, Dict
from unittest import mock

from lib.platform_ingestion.config_resolver import detect_platforms
from lib.platform_ingestion.orchestrator import (
    PlatformDataOrchestrator,
    _middleware_response_to_datasources,
)


_MINDRX_PAYLOAD: Dict[str, Any] = {
    "datasets": [
        {
            "datasetId": "mindrx_google_search_console",
            "service": "Google Search Console",
            "config": {"site_url": "sc-domain:mindrxgroup.com"},
        },
        {
            "datasetId": "mindrx_youtube_analytics",
            "service": "YouTube Analytics",
            "config": {"channel_id": "UC_test_channel"},
        },
        # No adapter registered for this; should be carried through harmlessly.
        {"datasetId": "marts_mindrx", "service": "dbt_marts"},
    ]
}


def _fake_http_response(payload: Dict[str, Any]):
    """Mimic urllib.request.urlopen's context-manager response object."""

    class _Resp:
        def __init__(self, body: bytes):
            self._body = body

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def read(self):
            return self._body

    return _Resp(json.dumps(payload).encode())


class MiddlewareConverterTests(unittest.TestCase):
    """Unit tests for the pure converter (no I/O)."""

    def test_emits_legacy_integrations_shape(self):
        ds = _middleware_response_to_datasources(_MINDRX_PAYLOAD)

        self.assertIn("integrations", ds)
        self.assertIn("gsc", ds["integrations"])
        self.assertIn("youtube_analytics", ds["integrations"])

        gsc = ds["integrations"]["gsc"]
        self.assertEqual(gsc["site_url"], "sc-domain:mindrxgroup.com")
        # status must be "connected" so detect_platforms doesn't filter.
        self.assertEqual(gsc["status"], "connected")
        # dataset_id is preserved so adapters can address a specific dataset.
        self.assertEqual(gsc["dataset_id"], "mindrx_google_search_console")

        yt = ds["integrations"]["youtube_analytics"]
        self.assertEqual(yt["channel_id"], "UC_test_channel")
        self.assertEqual(yt["status"], "connected")

    def test_unknown_service_is_carried_through_unaliased(self):
        # dbt_marts has no adapter, but the converter must not drop it —
        # the orchestrator's adapter dispatch is responsible for skipping.
        ds = _middleware_response_to_datasources(_MINDRX_PAYLOAD)
        self.assertIn("dbt_marts", ds["integrations"])

    def test_first_write_wins_for_duplicate_platform(self):
        payload = {
            "datasets": [
                {"datasetId": "first", "service": "Google Search Console",
                 "config": {"site_url": "sc-domain:first.com"}},
                {"datasetId": "second", "service": "Google Search Console",
                 "config": {"site_url": "sc-domain:second.com"}},
            ]
        }
        ds = _middleware_response_to_datasources(payload)
        self.assertEqual(ds["integrations"]["gsc"]["site_url"],
                         "sc-domain:first.com")

    def test_empty_payload_is_safe(self):
        self.assertEqual(_middleware_response_to_datasources({}),
                         {"integrations": {}})
        self.assertEqual(_middleware_response_to_datasources({"datasets": []}),
                         {"integrations": {}})

    def test_detect_platforms_finds_converted_entries(self):
        ds = _middleware_response_to_datasources(_MINDRX_PAYLOAD)
        found = dict(detect_platforms(ds))

        # The GSC + YouTube aliases live in `_INTEGRATION_ALIASES`, so the
        # converter normalises to the canonical platform keys.
        self.assertIn("gsc", found)
        self.assertIn("youtube_analytics", found)

        # The per-client field must round-trip through the converter so
        # `resolve_config(...)` (PR #11) can pick it up unchanged.
        self.assertEqual(found["gsc"].get("site_url"),
                         "sc-domain:mindrxgroup.com")
        self.assertEqual(found["youtube_analytics"].get("channel_id"),
                         "UC_test_channel")


class OrchestratorLoadDatasourcesTests(unittest.TestCase):
    """Integration tests for the resolution-order logic."""

    def setUp(self):
        # Don't touch the real Supabase / RateLimiter constructor — just
        # build a bare instance and patch in the methods we need.
        self.orchestrator = PlatformDataOrchestrator.__new__(
            PlatformDataOrchestrator
        )

    def test_middleware_path_is_used_when_env_is_set(self):
        with mock.patch.dict("os.environ", {
            "MH1_API_URL": "https://example.test",
            "MH1_API_KEY": "test-key",
        }, clear=False), \
             mock.patch(
                 "lib.platform_ingestion.orchestrator.urllib.request.urlopen",
                 return_value=_fake_http_response(_MINDRX_PAYLOAD),
             ) as fake_open, \
             mock.patch(
                 "lib.firebase_client.get_firebase_client",
             ) as fake_fb:
            ds = self.orchestrator._load_datasources("mindrxgroup.com")

        self.assertIsNotNone(ds)
        self.assertIn("gsc", ds["integrations"])
        # Firebase should never have been consulted.
        fake_fb.assert_not_called()

        # Auth header + path were set correctly.
        request = fake_open.call_args[0][0]
        self.assertEqual(
            request.full_url,
            "https://example.test/api/clients/mindrxgroup.com/data/datasets",
        )
        self.assertEqual(request.headers.get("X-api-key"), "test-key")

    def test_falls_back_to_firebase_on_404(self):
        # Simulate the middleware returning 404 (client not registered).
        http_404 = urllib.error.HTTPError(
            url="...", code=404, msg="Not Found", hdrs=None,  # type: ignore[arg-type]
            fp=BytesIO(b""),
        )

        # Firebase mock returns a legacy-shape dict.
        legacy_doc = {
            "integrations": {
                "klaviyo": {"api_key": "pk_legacy", "status": "connected"}
            }
        }

        fake_fb = mock.MagicMock()
        fake_fb.get_document.return_value = legacy_doc

        with mock.patch.dict("os.environ", {
            "MH1_API_URL": "https://example.test",
            "MH1_API_KEY": "test-key",
        }, clear=False), \
             mock.patch(
                 "lib.platform_ingestion.orchestrator.urllib.request.urlopen",
                 side_effect=http_404,
             ), \
             mock.patch(
                 "lib.firebase_client.get_firebase_client",
                 return_value=fake_fb,
             ):
            ds = self.orchestrator._load_datasources("legacy-client")

        self.assertEqual(ds, legacy_doc)
        fake_fb.get_document.assert_called_once()

    def test_falls_back_to_firebase_on_network_error(self):
        legacy_doc = {"integrations": {"hubspot": {"access_token": "tok",
                                                     "status": "connected"}}}
        fake_fb = mock.MagicMock()
        fake_fb.get_document.return_value = legacy_doc

        with mock.patch.dict("os.environ", {
            "MH1_API_URL": "https://example.test",
            "MH1_API_KEY": "test-key",
        }, clear=False), \
             mock.patch(
                 "lib.platform_ingestion.orchestrator.urllib.request.urlopen",
                 side_effect=ConnectionError("DNS failure"),
             ), \
             mock.patch(
                 "lib.firebase_client.get_firebase_client",
                 return_value=fake_fb,
             ):
            ds = self.orchestrator._load_datasources("legacy-client")

        self.assertEqual(ds, legacy_doc)

    def test_skips_middleware_when_env_unset(self):
        legacy_doc = {"integrations": {"shopify": {"access_token": "tok",
                                                    "status": "connected"}}}
        fake_fb = mock.MagicMock()
        fake_fb.get_document.return_value = legacy_doc

        # Strip MH1_API_URL / MH1_API_KEY from the environment for this test.
        with mock.patch.dict("os.environ", {
            "MH1_API_URL": "",
            "MH1_API_KEY": "",
        }, clear=False), \
             mock.patch(
                 "lib.platform_ingestion.orchestrator.urllib.request.urlopen",
             ) as fake_open, \
             mock.patch(
                 "lib.firebase_client.get_firebase_client",
                 return_value=fake_fb,
             ):
            ds = self.orchestrator._load_datasources("legacy-client")

        # Middleware was never called.
        fake_open.assert_not_called()
        # Firebase doc was returned.
        self.assertEqual(ds, legacy_doc)


if __name__ == "__main__":
    unittest.main()
