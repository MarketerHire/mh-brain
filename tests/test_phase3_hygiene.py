"""
Phase 3 regression tests: hygiene fixes.

Covers:
  A11 - SemanticMemoryStore._persist_pattern exists, refreshes updated_at,
        preserves/unions tenant_ids across calls, and writes via
        set_document.
  #2  - Predictor accepts an optional episodic_store and exposes recent
        episode stats via guidance.metadata["recent_episodes"]. The
        predictor widens uncertainty when episodic evidence diverges
        from the pattern prediction but never narrows it.
  A12 - Worker event router skips events already represented in episodic
        memory, routes skill completions with tracking_id through the
        close-loop path, and normalises unknown domains via
        normalize_domain instead of raising ValueError.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List
from unittest.mock import MagicMock

import pytest

from lib.intelligence.learning.predictor import (
    ExplorationConfig,
    Guidance,
    Predictor,
)
from lib.intelligence.memory.episodic import (
    EpisodicMemoryConfig,
    EpisodicMemoryStore,
)
from lib.intelligence.memory.semantic import SemanticMemoryStore
from lib.intelligence.memory.procedural import ProceduralMemoryStore
from lib.intelligence.types import (
    Domain,
    EpisodicMemory,
    Outcome,
    Prediction,
    SemanticPattern,
)


# ────────────────────────────────────────────────────────────────────────
# A11 — _persist_pattern on SemanticMemoryStore
# ────────────────────────────────────────────────────────────────────────


def _make_pattern(**overrides) -> SemanticPattern:
    # ``expected_value`` is a computed property, not a constructor arg —
    # assign it after construction so the underscore-prefixed backing
    # field gets set via the setter.
    expected_value = overrides.pop("expected_value", 1.2)
    defaults = dict(
        pattern_id="pat-001",
        skill_name="email-copy-generator",
        domain=Domain.CONTENT,
        confidence=0.6,
        evidence_count=5,
        recommendation={"tone": "direct"},
    )
    defaults.update(overrides)
    pattern = SemanticPattern(**defaults)
    pattern.expected_value = expected_value
    return pattern


def test_a11_persist_pattern_method_exists_on_semantic_store(fake_firebase_factory):
    fb = fake_firebase_factory()
    store = SemanticMemoryStore(firebase_client=fb)
    assert hasattr(store, "_persist_pattern")


def test_a11_persist_pattern_refreshes_updated_at(fake_firebase_factory):
    fb = fake_firebase_factory()
    store = SemanticMemoryStore(firebase_client=fb)
    pattern = _make_pattern(updated_at="2020-01-01T00:00:00+00:00")

    store._persist_pattern(pattern, tenant_id="acme")

    assert pattern.updated_at != "2020-01-01T00:00:00+00:00"
    dt = datetime.fromisoformat(pattern.updated_at.replace("Z", "+00:00"))
    now = datetime.now(timezone.utc)
    assert abs((now - dt).total_seconds()) < 5


def test_a11_persist_pattern_unions_tenant_ids(fake_firebase_factory):
    fb = fake_firebase_factory()
    store = SemanticMemoryStore(firebase_client=fb)
    pattern = _make_pattern()

    store._persist_pattern(pattern, tenant_id="acme")
    store._persist_pattern(pattern, tenant_id="widgets")
    store._persist_pattern(pattern, tenant_id="acme")

    collection = store._get_collection_path(Domain.CONTENT)
    doc = fb.get_document(collection, pattern.pattern_id)
    assert sorted(doc["tenant_ids"]) == ["acme", "widgets"]


def test_a11_persist_pattern_empty_tenant_id_is_global(fake_firebase_factory):
    fb = fake_firebase_factory()
    store = SemanticMemoryStore(firebase_client=fb)
    pattern = _make_pattern()

    store._persist_pattern(pattern, tenant_id="")
    store._persist_pattern(pattern, tenant_id="")

    collection = store._get_collection_path(Domain.CONTENT)
    doc = fb.get_document(collection, pattern.pattern_id)
    assert doc["tenant_ids"] == []


def test_a11_persist_pattern_requires_skill_name(fake_firebase_factory):
    fb = fake_firebase_factory()
    store = SemanticMemoryStore(firebase_client=fb)
    pattern = _make_pattern(skill_name="")

    with pytest.raises(ValueError, match="skill_name"):
        store._persist_pattern(pattern, tenant_id="acme")


def test_a11_persist_pattern_writes_full_pattern_payload(fake_firebase_factory):
    fb = fake_firebase_factory()
    store = SemanticMemoryStore(firebase_client=fb)
    pattern = _make_pattern(confidence=0.83, evidence_count=21)

    store._persist_pattern(pattern, tenant_id="acme")

    collection = store._get_collection_path(Domain.CONTENT)
    doc = fb.get_document(collection, pattern.pattern_id)
    assert doc["confidence"] == 0.83
    assert doc["evidence_count"] == 21
    assert doc["recommendation"] == {"tone": "direct"}


# ────────────────────────────────────────────────────────────────────────
# #2 — Predictor wired to episodic store
# ────────────────────────────────────────────────────────────────────────


def _make_episode(
    *,
    observed_signal: float,
    observed_baseline: float = 1.0,
    goal: bool = True,
    episode_id: str = "",
) -> EpisodicMemory:
    eid = episode_id or f"ep-{observed_signal}-{goal}"
    return EpisodicMemory(
        episode_id=eid,
        prediction=Prediction(
            skill_name="email-copy-generator",
            tenant_id="acme",
            domain=Domain.CONTENT,
        ),
        outcome=Outcome(
            observed_signal=observed_signal,
            observed_baseline=observed_baseline,
            goal_completed=goal,
        ),
        created_at=datetime.now(timezone.utc).isoformat(),
    )


def test_fix2_predictor_accepts_episodic_store(fake_firebase_factory):
    fb = fake_firebase_factory()
    semantic = SemanticMemoryStore(firebase_client=fb)
    procedural = ProceduralMemoryStore(firebase_client=fb)
    episodic = EpisodicMemoryStore(firebase_client=fb)

    predictor = Predictor(
        semantic_store=semantic,
        procedural_store=procedural,
        episodic_store=episodic,
    )

    assert predictor._episodic_store is episodic


def test_fix2_predictor_episodic_store_is_optional(fake_firebase_factory):
    fb = fake_firebase_factory()
    semantic = SemanticMemoryStore(firebase_client=fb)
    procedural = ProceduralMemoryStore(firebase_client=fb)

    predictor = Predictor(
        semantic_store=semantic,
        procedural_store=procedural,
    )

    assert predictor._episodic_store is None
    guidance = predictor.get_guidance(
        skill_name="email-copy-generator",
        tenant_id="acme",
        domain=Domain.CONTENT,
        context={},
    )
    assert isinstance(guidance, Guidance)
    assert "recent_episodes" not in guidance.metadata


def test_fix2_summarize_episodes_handles_empty_list():
    assert Predictor._summarize_episodes([]) == {}


def test_fix2_summarize_episodes_computes_means():
    episodes = [
        _make_episode(observed_signal=100.0, observed_baseline=50.0, goal=True),
        _make_episode(observed_signal=150.0, observed_baseline=50.0, goal=True),
        _make_episode(observed_signal=25.0, observed_baseline=50.0, goal=False),
    ]

    summary = Predictor._summarize_episodes(episodes)

    assert summary["count"] == 3
    assert summary["mean_observed_signal"] == pytest.approx(91.6666, abs=1e-3)
    assert summary["mean_signal_ratio"] == pytest.approx(1.8333, abs=1e-3)
    assert summary["goal_completion_rate"] == pytest.approx(2 / 3)
    assert "last_observed_at" in summary
    assert len(summary["episode_ids"]) == 3


def test_fix2_get_guidance_surfaces_recent_episodes(fake_firebase_factory):
    fb = fake_firebase_factory()
    semantic = SemanticMemoryStore(firebase_client=fb)
    procedural = ProceduralMemoryStore(firebase_client=fb)
    episodic = EpisodicMemoryStore(firebase_client=fb)

    for i in range(5):
        episodic.store(_make_episode(
            observed_signal=100.0 + i,
            observed_baseline=50.0,
            episode_id=f"ep-{i}",
        ))

    predictor = Predictor(
        semantic_store=semantic,
        procedural_store=procedural,
        episodic_store=episodic,
        config=ExplorationConfig(base_exploration_rate=0.0),
    )

    guidance = predictor.get_guidance(
        skill_name="email-copy-generator",
        tenant_id="acme",
        domain=Domain.CONTENT,
        context={},
    )

    stats = guidance.metadata.get("recent_episodes")
    assert stats is not None
    assert stats["count"] >= 1
    assert stats["mean_signal_ratio"] > 1.5


def test_fix2_uncertainty_widens_on_episodic_divergence(fake_firebase_factory):
    """When episodes sharply disagree with the pattern, confidence drops."""
    fb = fake_firebase_factory()
    semantic = SemanticMemoryStore(firebase_client=fb)
    procedural = ProceduralMemoryStore(firebase_client=fb)
    episodic = EpisodicMemoryStore(firebase_client=fb)

    pattern = _make_pattern(
        pattern_id="pat-001",
        confidence=0.9,
        expected_value=2.0,
        evidence_count=50,
    )
    semantic.store(pattern)

    for i in range(5):
        episodic.store(_make_episode(
            observed_signal=50.0,
            observed_baseline=100.0,
            episode_id=f"ep-low-{i}",
        ))

    predictor = Predictor(
        semantic_store=semantic,
        procedural_store=procedural,
        episodic_store=episodic,
        config=ExplorationConfig(base_exploration_rate=0.0),
    )
    guidance = predictor.get_guidance(
        skill_name="email-copy-generator",
        tenant_id="acme",
        domain=Domain.CONTENT,
        context={},
    )

    divergence = guidance.metadata.get("episodic_divergence")
    assert divergence is not None
    assert divergence["pattern_expected_value"] == 2.0
    assert divergence["observed_mean_ratio"] == pytest.approx(0.5)
    assert guidance.uncertainty >= 0.1


def test_fix2_episodic_divergence_absent_when_values_agree(fake_firebase_factory):
    """When episodes agree with the pattern, no divergence warning."""
    fb = fake_firebase_factory()
    semantic = SemanticMemoryStore(firebase_client=fb)
    procedural = ProceduralMemoryStore(firebase_client=fb)
    episodic = EpisodicMemoryStore(firebase_client=fb)

    pattern = _make_pattern(
        pattern_id="pat-001",
        confidence=0.9,
        expected_value=2.0,
        evidence_count=50,
    )
    semantic.store(pattern)

    for i in range(5):
        episodic.store(_make_episode(
            observed_signal=210.0,
            observed_baseline=100.0,
            episode_id=f"ep-hi-{i}",
        ))

    predictor = Predictor(
        semantic_store=semantic,
        procedural_store=procedural,
        episodic_store=episodic,
        config=ExplorationConfig(base_exploration_rate=0.0),
    )
    guidance = predictor.get_guidance(
        skill_name="email-copy-generator",
        tenant_id="acme",
        domain=Domain.CONTENT,
        context={},
    )

    assert "episodic_divergence" not in guidance.metadata


def test_fix2_retrieve_episodes_no_crash_when_store_errors(fake_firebase_factory):
    fb = fake_firebase_factory()
    semantic = SemanticMemoryStore(firebase_client=fb)
    procedural = ProceduralMemoryStore(firebase_client=fb)

    broken_store = MagicMock()
    broken_store.retrieve.side_effect = RuntimeError("boom")

    predictor = Predictor(
        semantic_store=semantic,
        procedural_store=procedural,
        episodic_store=broken_store,
    )

    guidance = predictor.get_guidance(
        skill_name="x",
        tenant_id="acme",
        domain=Domain.CONTENT,
        context={},
    )
    assert isinstance(guidance, Guidance)
    assert "recent_episodes" not in guidance.metadata


# ────────────────────────────────────────────────────────────────────────
# A12 — Worker event routing hygiene
# ────────────────────────────────────────────────────────────────────────


class _FakeSupabaseTable:
    def __init__(self, rows: Dict[str, Any]):
        self._rows = rows
        self._filter_keys: List[str] = []
        self._filter_values: List[str] = []

    def select(self, *_a, **_k): return self
    def eq(self, *_a, **_k): return self
    def limit(self, *_a, **_k): return self
    def update(self, *_a, **_k): return self
    def order(self, *_a, **_k): return self
    def execute(self):
        return MagicMock(data=self._rows.get("data", []))

    def filter(self, field: str, op: str, value: Any):
        self._filter_keys.append(field)
        self._filter_values.append(value)
        rows = self._rows.get("by_event_id", {})
        if value in rows:
            self._rows = {"data": [{"episode_id": rows[value]}]}
        else:
            self._rows = {"data": []}
        return self


class _FakeSupabase:
    def __init__(self, processed_event_ids: List[str] = None):
        self._event_ids = processed_event_ids or []

    def table(self, _name: str):
        return _FakeSupabaseTable({
            "by_event_id": {eid: f"ep-{eid}" for eid in self._event_ids},
            "data": [],
        })


def _make_worker_stub(processed_event_ids: List[str] = None):
    """Construct a BrightMatterWorker stub without booting the real engine."""
    from worker import BrightMatterWorker

    worker = BrightMatterWorker.__new__(BrightMatterWorker)
    worker.engine = MagicMock()
    worker.bridge = MagicMock()
    worker.bridge._tracking = {}
    worker._supabase = _FakeSupabase(processed_event_ids=processed_event_ids or [])
    return worker


def test_a12_process_event_skips_already_processed():
    worker = _make_worker_stub(processed_event_ids=["evt-dedup"])
    event = {
        "id": "evt-dedup",
        "event_type": "signal",
        "skill_name": "email",
        "client_id": "acme",
        "metrics": {"primary_metric": 0.7},
        "context": {},
    }

    worker._process_event(event)

    worker.engine.episodic.store.assert_not_called()


def test_a12_process_event_runs_when_not_previously_processed():
    worker = _make_worker_stub(processed_event_ids=[])
    worker.engine.predictor.get_guidance.return_value = Guidance(
        parameters={}, confidence=0.5, uncertainty=0.5
    )
    event = {
        "id": "evt-new",
        "event_type": "signal",
        "skill_name": "email",
        "client_id": "acme",
        "metrics": {"primary_metric": 0.7},
        "context": {},
    }

    worker._process_event(event)

    assert worker.engine.episodic.store.call_count == 1


def test_a12_skill_completion_closes_existing_tracking_when_id_present():
    worker = _make_worker_stub()
    worker.bridge._tracking["track-abc"] = {"skill_name": "x"}

    event = {
        "id": "evt-closed",
        "event_type": "skill_completed",
        "skill_name": "email",
        "client_id": "acme",
        "tracking_id": "track-abc",
        "result": {"output": "hi"},
        "metrics": {"primary_metric": 0.8},
        "context": {},
    }

    worker._process_skill_completion(event)

    assert worker.bridge.start_tracking.call_count == 0
    worker.bridge.complete_tracking.assert_called_once()
    kwargs = worker.bridge.complete_tracking.call_args.kwargs
    assert kwargs["tracking_id"] == "track-abc"


def test_a12_skill_completion_falls_back_to_backfill_when_no_tracking_id():
    worker = _make_worker_stub()
    worker.bridge.start_tracking.return_value = "new-track-id"

    event = {
        "id": "evt-backfill",
        "event_type": "skill_completed",
        "skill_name": "email",
        "client_id": "acme",
        "result": {"output": "hi"},
        "metrics": {"primary_metric": 0.8},
        "context": {},
    }

    worker._process_skill_completion(event)

    worker.bridge.get_skill_guidance.assert_called_once()
    worker.bridge.start_tracking.assert_called_once()
    worker.bridge.complete_tracking.assert_called_once()
    complete_kwargs = worker.bridge.complete_tracking.call_args.kwargs
    assert complete_kwargs["tracking_id"] == "new-track-id"


def test_a12_skill_completion_backfills_when_tracking_id_not_registered():
    """Defensive path: we receive a tracking_id we've never seen (e.g.,
    distributed workers, lost state). Behave like backfill mode so we
    still record an episode."""
    worker = _make_worker_stub()
    worker.bridge.start_tracking.return_value = "new-track-id"

    event = {
        "id": "evt-orphan",
        "event_type": "skill_completed",
        "skill_name": "email",
        "client_id": "acme",
        "tracking_id": "stranger-id",
        "result": {"output": "hi"},
        "metrics": {},
        "context": {},
    }

    worker._process_skill_completion(event)

    worker.bridge.start_tracking.assert_called_once()
    worker.bridge.complete_tracking.assert_called_once()


def test_a12_process_signal_accepts_unknown_domain_string():
    """Phase 2 alignment: signals with domain='paid_media' or any other
    non-enum string must not crash the worker."""
    worker = _make_worker_stub()
    worker.engine.predictor.get_guidance.return_value = Guidance(
        parameters={}, confidence=0.5, uncertainty=0.5
    )

    event = {
        "id": "evt-pm",
        "event_type": "signal",
        "skill_name": "platform-daily:ga",
        "client_id": "acme",
        "domain": "paid_media",
        "metrics": {"primary_metric": 100.0},
        "context": {},
    }

    worker._process_event(event)
    assert worker.engine.episodic.store.call_count == 1
    stored_episode = worker.engine.episodic.store.call_args.args[0]
    assert stored_episode.prediction.domain == Domain.CAMPAIGN


def test_a12_process_expert_override_accepts_unknown_domain_string():
    worker = _make_worker_stub()

    event = {
        "id": "evt-ovr",
        "event_type": "expert_override",
        "skill_name": "email-copy-generator",
        "client_id": "acme",
        "domain": "not_a_real_domain",
        "result": {
            "expert_output": "better",
            "original_output": "bad",
            "reason": "off-brand",
        },
        "context": {},
    }

    worker._process_event(event)
    assert worker.engine.episodic.store.call_count == 1
    stored_episode = worker.engine.episodic.store.call_args.args[0]
    assert stored_episode.prediction.domain == Domain.GENERIC
