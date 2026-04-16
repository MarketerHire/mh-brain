"""Phase 2 regression tests — domain alias resolution + platform outcome shape."""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List

import pytest

from lib.intelligence.memory.episodic import (
    EpisodicMemoryConfig,
    EpisodicMemoryStore,
)
from lib.intelligence.memory.semantic import (
    SemanticMemoryConfig,
    SemanticMemoryStore,
)
from lib.intelligence.types import (
    Domain,
    Outcome,
    Prediction,
    SemanticPattern,
    normalize_domain,
)


# ────────────────────────────────────────────────────────────────────────
# A10 — normalize_domain helper
# ────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "raw,expected",
    [
        # Platform ingestion aliases → canonical Domain values
        ("paid_media", Domain.CAMPAIGN),
        ("email", Domain.CONTENT),
        ("crm", Domain.REVENUE),
        ("ecommerce", Domain.REVENUE),
        ("lifecycle", Domain.CONTENT),
        ("product_analytics", Domain.HEALTH),
        ("mobile", Domain.CAMPAIGN),
        # Other historical aliases
        ("campaigns", Domain.CAMPAIGN),
        ("engagement", Domain.CONTENT),
        ("sales", Domain.REVENUE),
        ("retention", Domain.HEALTH),
        ("churn", Domain.HEALTH),
        ("pipeline", Domain.REVENUE),
        ("growth", Domain.CONTENT),
        ("attribution", Domain.CAMPAIGN),
        # Canonical enum values (identity)
        ("content", Domain.CONTENT),
        ("revenue", Domain.REVENUE),
        ("health", Domain.HEALTH),
        ("campaign", Domain.CAMPAIGN),
        ("generic", Domain.GENERIC),
    ],
)
def test_a10_normalize_domain_maps_aliases(raw, expected):
    assert normalize_domain(raw) == expected


def test_a10_normalize_domain_handles_enum_passthrough():
    assert normalize_domain(Domain.REVENUE) is Domain.REVENUE


def test_a10_normalize_domain_falls_back_to_generic():
    assert normalize_domain("completely-unknown") == Domain.GENERIC
    assert normalize_domain(None) == Domain.GENERIC
    assert normalize_domain("") == Domain.GENERIC
    assert normalize_domain(42) == Domain.GENERIC


def test_a10_normalize_domain_custom_default():
    assert normalize_domain("bogus", default=Domain.CONTENT) == Domain.CONTENT
    assert normalize_domain(None, default=Domain.HEALTH) == Domain.HEALTH


def test_a10_prediction_from_dict_uses_alias_map():
    p = Prediction.from_dict({"skill_name": "test", "domain": "paid_media"})
    assert p.domain == Domain.CAMPAIGN


def test_a10_prediction_from_dict_unknown_domain_does_not_raise():
    # Before Phase 2, Prediction.from_dict would raise ValueError on unknown
    # strings. Now it falls back to GENERIC.
    p = Prediction.from_dict({"skill_name": "test", "domain": "unrecognised"})
    assert p.domain == Domain.GENERIC


def test_a10_semantic_pattern_from_dict_uses_alias_map():
    sp = SemanticPattern.from_dict({"pattern_id": "x", "domain": "ecommerce"})
    assert sp.domain == Domain.REVENUE


def test_a10_semantic_pattern_from_dict_unknown_domain_does_not_raise():
    sp = SemanticPattern.from_dict({"pattern_id": "x", "domain": "mystery"})
    assert sp.domain == Domain.GENERIC


# ────────────────────────────────────────────────────────────────────────
# A10 — episodic _doc_to_episode surfaces alias from top-level column
# ────────────────────────────────────────────────────────────────────────


def _platform_doc(**overrides: Any) -> Dict[str, Any]:
    """Platform-ingestion-shaped Supabase row."""
    base = {
        "episode_id": "pi-test",
        "tenant_id": "acme",
        "skill_name": "platform-daily:GoogleAds-acme",
        "domain": "paid_media",
        "prediction": {
            "context": {"platform": "google_ads", "client_id": "acme"},
        },
        "outcome": {
            "observed_signal": 1000.0,
            "observed_baseline": 1.0,
            "goal_completed": True,
            "metadata": {
                "source": "platform-ingestion",
                "metrics": {"spend": 1000.0, "clicks": 42},
            },
        },
        "weight": 1.0,
    }
    base.update(overrides)
    return base


def test_a10_doc_to_episode_maps_platform_alias(fake_firebase_factory):
    fb = fake_firebase_factory()
    store = EpisodicMemoryStore(firebase_client=fb, config=EpisodicMemoryConfig())

    # prediction JSONB is empty for the domain; top-level column says "paid_media"
    doc = _platform_doc(prediction={})

    ep = store._doc_to_episode(doc)
    assert ep is not None
    # After Phase 2, "paid_media" resolves to CAMPAIGN — not GENERIC.
    assert ep.prediction.domain == Domain.CAMPAIGN


def test_a10_doc_to_episode_legacy_alias_inside_prediction_jsonb(
    fake_firebase_factory,
):
    """Phase 1 added top-level backfill; Phase 2 adds alias resolution
    inside Prediction.from_dict too, so even when the JSONB itself
    carries an alias it no longer raises."""
    fb = fake_firebase_factory()
    store = EpisodicMemoryStore(firebase_client=fb, config=EpisodicMemoryConfig())

    doc = _platform_doc(
        prediction={
            "skill_name": "platform-daily:GoogleAds-acme",
            "tenant_id": "acme",
            "domain": "paid_media",  # alias inside the blob
        }
    )

    ep = store._doc_to_episode(doc)
    assert ep is not None
    assert ep.prediction.domain == Domain.CAMPAIGN


# ────────────────────────────────────────────────────────────────────────
# A9 — platform_ingestion writes canonical outcome shape
# ────────────────────────────────────────────────────────────────────────


class _FakeRow:
    def __init__(self, metric_date: date, metrics: Dict[str, Any], record_count: int = 1):
        self.metric_date = metric_date
        self.metrics = metrics
        self.record_count = record_count


def _build_platform_episodes(
    platform: str,
    rows: List[_FakeRow],
    *,
    client_id: str = "acme",
    client_name: str = "Acme Corp",
) -> List[Dict[str, Any]]:
    """Invoke PlatformDataOrchestrator._create_episodes without Supabase."""
    from lib.platform_ingestion import orchestrator as orc

    captured: List[Dict[str, Any]] = []

    class _Stub(orc.PlatformDataOrchestrator):
        def __init__(self):
            # Skip __init__ that instantiates Supabase
            pass

        def _upsert_episodes(self, batch):
            captured.extend(batch)
            return len(batch)

    stream_id = orc._make_stream_id(platform, client_name)
    _Stub()._create_episodes(
        client_id=client_id,
        client_name=client_name,
        platform=platform,
        stream_id=stream_id,
        rows=rows,
    )
    return captured


def test_a9_outcome_has_metrics_under_metadata():
    rows = [_FakeRow(date(2026, 1, 1), {"spend": 1000, "clicks": 42})]
    episodes = _build_platform_episodes("google_ads", rows)

    assert len(episodes) == 1
    outcome = episodes[0]["outcome"]

    # metrics must NOT live at the top level of outcome
    assert "metrics" not in outcome, (
        "metrics at outcome top-level are silently dropped by Outcome.from_dict"
    )
    # Instead they live under metadata.metrics
    assert outcome["metadata"]["metrics"] == {"spend": 1000, "clicks": 42}


def test_a9_outcome_has_canonical_domain():
    rows = [_FakeRow(date(2026, 1, 1), {"spend": 1000})]

    ads = _build_platform_episodes("google_ads", rows)[0]
    assert ads["domain"] == "campaign"  # canonical enum value, not "paid_media"

    klaviyo = _build_platform_episodes("klaviyo", [_FakeRow(date(2026, 1, 1), {"revenue": 50})])[0]
    assert klaviyo["domain"] == "content"


def test_a9_prediction_carries_skill_name_in_jsonb():
    """Backfill-on-read (Phase 1) is a safety net — platform_ingestion
    must also write skill_name / tenant_id / domain INTO the prediction
    JSONB so future-written episodes are internally consistent."""
    rows = [_FakeRow(date(2026, 1, 1), {"spend": 1000})]
    ep = _build_platform_episodes("google_ads", rows, client_id="acme")[0]

    pred = ep["prediction"]
    assert pred["skill_name"] == ep["skill_name"]
    assert pred["tenant_id"] == "acme"
    assert pred["domain"] == "campaign"


def test_a9_handles_missing_primary_metric_without_crashing():
    # record_count-only days (e.g. a platform with no meaningful metric)
    rows = [_FakeRow(date(2026, 1, 1), {"active_users": None}, record_count=5)]
    episodes = _build_platform_episodes("amplitude", rows)

    ep = episodes[0]
    assert ep["outcome"]["observed_signal"] == 0.0, \
        "None primary metric must coerce to 0.0 so ratio math doesn't crash"
    assert ep["outcome"]["observed_baseline"] == 1.0
    assert ep["outcome"]["goal_completed"] is False
    assert ep["outcome"]["metadata"]["has_primary_metric"] is False


def test_a9_uses_prior_day_signal_as_baseline():
    rows = [
        _FakeRow(date(2026, 1, 1), {"spend": 1000}),
        _FakeRow(date(2026, 1, 2), {"spend": 1500}),
        _FakeRow(date(2026, 1, 3), {"spend": 2000}),
    ]
    episodes = _build_platform_episodes("google_ads", rows)
    assert len(episodes) == 3

    # First day has no prior → baseline defaults to 1.0
    assert episodes[0]["outcome"]["observed_baseline"] == 1.0
    # Second day uses first day's signal as baseline
    assert episodes[1]["outcome"]["observed_baseline"] == 1000.0
    # Third day uses second day's signal
    assert episodes[2]["outcome"]["observed_baseline"] == 1500.0


def test_a9_ratios_are_reasonable_for_consolidation(fake_firebase_factory):
    """Regression: before Phase 2, observed_baseline defaulted to 1.0 so
    consolidate_from_episodes saw ratios like 1500/1 = 1500 and produced
    nonsense patterns. With prior-day baselines, day-over-day ratios
    should hover around 1.0."""
    fb = fake_firebase_factory()
    semantic = SemanticMemoryStore(firebase_client=fb, config=SemanticMemoryConfig())

    rows = [
        _FakeRow(date(2026, 1, d), {"spend": 1000 + (d * 100)})
        for d in range(1, 6)
    ]
    raw_episodes = _build_platform_episodes("google_ads", rows, client_id="acme")

    # Hydrate into EpisodicMemory objects via the normal read path.
    episodic = EpisodicMemoryStore(firebase_client=fb, config=EpisodicMemoryConfig())
    episodes = []
    for raw in raw_episodes:
        # Simulate round-trip through Supabase → _doc_to_episode.
        ep = episodic._doc_to_episode(raw)
        assert ep is not None
        episodes.append(ep)

    # Day 1 has baseline 1.0 (no prior), so skip it for the ratio sanity check.
    day_over_day = episodes[1:]
    for ep in day_over_day:
        if ep.outcome.observed_baseline > 0:
            ratio = ep.outcome.observed_signal / ep.outcome.observed_baseline
            assert 0.5 < ratio < 2.0, f"Day-over-day ratio should be ~1.0, got {ratio}"

    # Consolidation should succeed and map to CAMPAIGN (not GENERIC, not raise)
    pattern = semantic.consolidate_from_episodes(episodes)
    assert pattern is not None
    assert pattern.domain == Domain.CAMPAIGN


# ────────────────────────────────────────────────────────────────────────
# Integration: full write → read → consolidate with canonical shape
# ────────────────────────────────────────────────────────────────────────


def test_phase2_integration_canonical_shape_roundtrip(fake_firebase_factory):
    """End-to-end: a platform writes canonical episodes, they round-trip
    through _doc_to_episode, and consolidation produces sensible patterns
    mapped to the correct Domain."""
    fb = fake_firebase_factory()
    episodic = EpisodicMemoryStore(firebase_client=fb, config=EpisodicMemoryConfig())
    semantic = SemanticMemoryStore(firebase_client=fb, config=SemanticMemoryConfig())

    # Shopify → revenue domain
    shopify_rows = [_FakeRow(date(2026, 1, d), {"revenue": 5000 + d * 50}) for d in range(1, 6)]
    shopify_episodes = _build_platform_episodes(
        "shopify", shopify_rows, client_id="acme", client_name="Acme Corp"
    )
    hydrated = [episodic._doc_to_episode(raw) for raw in shopify_episodes]
    assert all(ep is not None for ep in hydrated)
    pattern = semantic.consolidate_from_episodes(hydrated)
    assert pattern is not None
    assert pattern.domain == Domain.REVENUE
    assert pattern.skill_name == "platform-daily:Shopify-Acme Corp"
    assert len(pattern.source_episodes) == 5

    # Amplitude → health domain
    amp_rows = [_FakeRow(date(2026, 1, d), {"active_users": 100 + d}) for d in range(1, 6)]
    amp_episodes = _build_platform_episodes(
        "amplitude", amp_rows, client_id="acme", client_name="Acme Corp"
    )
    amp_hydrated = [episodic._doc_to_episode(raw) for raw in amp_episodes]
    amp_pattern = semantic.consolidate_from_episodes(amp_hydrated)
    assert amp_pattern is not None
    assert amp_pattern.domain == Domain.HEALTH
