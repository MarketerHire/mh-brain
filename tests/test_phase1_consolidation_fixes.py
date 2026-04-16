"""
Phase 1 regression tests: unblock consolidation.

Covers:
  A1 - _doc_to_episode backfills prediction.skill_name / tenant_id /
       domain from top-level Supabase columns so consolidate_from_episodes
       no longer raises ValueError("Pattern must have skill_name").
  A8 - _consolidate_ready_episodes calls mark_consolidated even when
       _build_trajectory_from_episodes returns an empty trajectory.
  #4 - consolidate_from_episodes de-duplicates source_episodes on repeat
       invocations with overlapping episode sets.
  #3 - SupabaseStorage routes system/intelligence/pending_outcomes to the
       pending_outcomes table with prediction_id as the primary key and
       extracts client_id from the path.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import pytest

from lib.intelligence.memory.consolidation import (
    ConsolidationConfig,
    MemoryConsolidationManager,
)
from lib.intelligence.memory.episodic import (
    EpisodicMemoryConfig,
    EpisodicMemoryStore,
)
from lib.intelligence.memory.semantic import (
    SemanticMemoryConfig,
    SemanticMemoryStore,
)
from lib.intelligence.memory.procedural import (
    ProceduralMemoryConfig,
    ProceduralMemoryStore,
)
from lib.intelligence.types import Domain, EpisodicMemory, Outcome, Prediction
from lib.supabase_storage import SupabaseStorage


TENANT = "acme"
SKILL = "platform-daily:GoogleAds-acme"


# ────────────────────────────────────────────────────────────────────────
# A1: _doc_to_episode backfill
# ────────────────────────────────────────────────────────────────────────


def _platform_doc(
    *,
    episode_id: str = "pi-1",
    skill_name: str = SKILL,
    tenant_id: str = TENANT,
    domain: str = "generic",
    weight: float = 0.2,
) -> dict:
    """Shape that platform_ingestion/orchestrator.py writes to Supabase."""
    return {
        "episode_id": episode_id,
        "tenant_id": tenant_id,
        "skill_name": skill_name,
        "domain": domain,
        "prediction": {
            "context": {"platform": "google_ads", "client_id": tenant_id},
            "expected_signal": None,
            "patterns_used": [],
        },
        "outcome": {
            "observed_signal": 5432.0,
            "metadata": {"source": "platform-ingestion", "record_count": 1},
        },
        "weight": weight,
        "source": "platform-ingestion",
    }


def test_a1_doc_to_episode_backfills_from_top_level_columns(fake_firebase_factory):
    fb = fake_firebase_factory()
    store = EpisodicMemoryStore(firebase_client=fb, config=EpisodicMemoryConfig())

    doc = _platform_doc()

    episode = store._doc_to_episode(doc)

    assert episode is not None, "Episode should be reconstructed"
    assert episode.prediction.skill_name == SKILL, \
        "skill_name should be backfilled from top-level column"
    assert episode.prediction.tenant_id == TENANT, \
        "tenant_id should be backfilled from top-level column"
    assert episode.prediction.domain == Domain.GENERIC


def test_a1_doc_to_episode_coerces_unknown_domain_to_generic(
    fake_firebase_factory,
):
    fb = fake_firebase_factory()
    store = EpisodicMemoryStore(firebase_client=fb, config=EpisodicMemoryConfig())

    # Platform ingestion writes custom domain strings (e.g. "paid_media",
    # "email") that are NOT members of the Domain enum. A1 must tolerate
    # these without raising.
    doc = _platform_doc(domain="paid_media")

    episode = store._doc_to_episode(doc)

    assert episode is not None
    assert episode.prediction.domain == Domain.GENERIC, \
        "Unknown domain should degrade to GENERIC instead of raising"


def test_a1_doc_to_episode_preserves_jsonb_prediction_fields(
    fake_firebase_factory,
):
    """When the prediction JSONB already has skill_name/tenant_id/domain,
    we must NOT overwrite them with the top-level column values."""
    fb = fake_firebase_factory()
    store = EpisodicMemoryStore(firebase_client=fb, config=EpisodicMemoryConfig())

    doc = {
        "episode_id": "sk-1",
        "skill_name": "wrong-top-level",
        "tenant_id": "wrong-top-tenant",
        "domain": "generic",
        "prediction": {
            "skill_name": "correct-skill",
            "tenant_id": "correct-tenant",
            "domain": "revenue",
            "context": {},
            "patterns_used": [],
        },
        "outcome": {"observed_signal": 1.0, "observed_baseline": 1.0},
        "weight": 0.2,
    }

    episode = store._doc_to_episode(doc)

    assert episode is not None
    assert episode.prediction.skill_name == "correct-skill"
    assert episode.prediction.tenant_id == "correct-tenant"
    assert episode.prediction.domain == Domain.REVENUE


# ────────────────────────────────────────────────────────────────────────
# #4: consolidate_from_episodes dedupes source_episodes
# ────────────────────────────────────────────────────────────────────────


def _make_episode(
    episode_id: str,
    *,
    skill_name: str = SKILL,
    tenant_id: str = TENANT,
    domain: Domain = Domain.CONTENT,
    goal_completed: bool = True,
    signal: float = 1.2,
    baseline: float = 1.0,
) -> EpisodicMemory:
    prediction = Prediction(
        skill_name=skill_name,
        tenant_id=tenant_id,
        domain=domain,
        context={"channel_id": "email.lifecycle", "region": "NA"},
    )
    outcome = Outcome(
        observed_signal=signal,
        observed_baseline=baseline,
        goal_completed=goal_completed,
    )
    return EpisodicMemory(
        episode_id=episode_id,
        prediction=prediction,
        outcome=outcome,
        weight=0.2,
        created_at=datetime.now(timezone.utc).isoformat(),
    )


def test_phase1_fix_4_dedupes_source_episodes_on_repeat_consolidation(
    fake_firebase_factory,
):
    fb = fake_firebase_factory()
    semantic = SemanticMemoryStore(firebase_client=fb, config=SemanticMemoryConfig())

    episodes = [_make_episode(f"ep-{i}") for i in range(5)]

    # First consolidation: should create the pattern with 5 episodes.
    pattern_1 = semantic.consolidate_from_episodes(episodes)
    assert pattern_1 is not None
    assert sorted(pattern_1.source_episodes) == [f"ep-{i}" for i in range(5)]

    # Second consolidation with the SAME episodes (the scenario where A8's
    # bogus `continue` used to leave episodes marked unconsolidated,
    # triggering the same batch to be re-promoted indefinitely).
    pattern_2 = semantic.consolidate_from_episodes(episodes)
    assert pattern_2 is not None
    assert pattern_2.pattern_id == pattern_1.pattern_id, \
        "Repeat consolidation should update the same pattern"

    assert len(pattern_2.source_episodes) == 5, (
        f"source_episodes grew from 5 to {len(pattern_2.source_episodes)} — "
        "dedup protection regressed"
    )


def test_phase1_fix_4_appends_new_episodes_without_duplicates(
    fake_firebase_factory,
):
    fb = fake_firebase_factory()
    semantic = SemanticMemoryStore(firebase_client=fb, config=SemanticMemoryConfig())

    first_batch = [_make_episode(f"ep-{i}") for i in range(5)]
    second_batch = first_batch[-2:] + [_make_episode(f"ep-{i}") for i in range(5, 8)]

    pattern = semantic.consolidate_from_episodes(first_batch)
    assert pattern is not None

    pattern = semantic.consolidate_from_episodes(second_batch)
    assert pattern is not None

    # After the merge we should have the union: ep-0..ep-7 (8 unique IDs),
    # with no duplicates even though ep-3 and ep-4 appeared in both batches.
    assert sorted(pattern.source_episodes) == [f"ep-{i}" for i in range(8)]


# ────────────────────────────────────────────────────────────────────────
# A8: mark_consolidated runs when trajectory is empty
# ────────────────────────────────────────────────────────────────────────


class _RecordingEpisodic(EpisodicMemoryStore):
    """EpisodicMemoryStore that captures mark_consolidated calls."""

    def __init__(self, *args, seeded: List[EpisodicMemory], **kwargs):
        super().__init__(*args, **kwargs)
        self._seeded = seeded
        self.marked: List[str] = []

    def _list_tenants(self):  # override — we know them
        return [TENANT]

    def _list_skills_for_tenant(self, tenant_id: str):  # override
        return [SKILL]

    def decay_all(self, tenant_id=None):  # pretend decay ran
        return {"decayed": len(self._seeded), "to_consolidate": len(self._seeded), "archived": 0}

    def get_for_consolidation(self, tenant_id, skill_name, limit=20):
        return list(self._seeded)

    def mark_consolidated(self, episode_id, tenant_id, skill_name):
        self.marked.append(episode_id)

    def cleanup_old_episodes(self, *args, **kwargs):
        return 0


class _NoopProcedural(ProceduralMemoryStore):
    def create_from_patterns(self, *args, **kwargs):
        return None

    def decay_all(self):
        return 0


def test_a8_consolidation_marks_episodes_even_without_trajectory(
    fake_firebase_factory,
):
    fb = fake_firebase_factory()
    # Platform-ingestion-style episodes: no checkpoint_day metadata, so
    # _build_trajectory_from_episodes returns [].
    episodes = [_make_episode(f"ep-{i}") for i in range(5)]

    episodic = _RecordingEpisodic(
        firebase_client=fb,
        config=EpisodicMemoryConfig(),
        seeded=episodes,
    )
    semantic = SemanticMemoryStore(firebase_client=fb, config=SemanticMemoryConfig())
    procedural = _NoopProcedural(firebase_client=fb, config=ProceduralMemoryConfig())

    manager = MemoryConsolidationManager(
        episodic_store=episodic,
        semantic_store=semantic,
        procedural_store=procedural,
        config=ConsolidationConfig(
            min_episodes_for_consolidation=5,
            consolidation_batch_size=20,
        ),
    )

    # Run only the step we care about. run_consolidation_cycle has many
    # downstream steps that require more elaborate mocking.
    stats = manager._consolidate_ready_episodes(tenant_id=TENANT)

    assert stats["episodes_consolidated"] == 5, (
        "Every ready episode must be marked consolidated, even when "
        "_build_trajectory_from_episodes returns empty"
    )
    assert set(episodic.marked) == {f"ep-{i}" for i in range(5)}, (
        f"mark_consolidated should have been called for every episode; "
        f"got {episodic.marked}"
    )


# ────────────────────────────────────────────────────────────────────────
# Integration: full platform_ingestion → consolidation path
# ────────────────────────────────────────────────────────────────────────


def test_phase1_integration_platform_ingestion_consolidation_no_crash(
    fake_firebase_factory,
):
    """Regression test for the original consolidation crash.

    Before Phase 1:
      platform_ingestion wrote prediction JSONB without skill_name.
      _doc_to_episode left prediction.skill_name = "".
      consolidate_from_episodes tried to store a pattern with skill_name = "".
      semantic.store() raised ValueError("Pattern must have skill_name").

    After Phase 1: the read side backfills from top-level columns, so the
    full path completes without raising.
    """
    fb = fake_firebase_factory()
    episodic = EpisodicMemoryStore(firebase_client=fb, config=EpisodicMemoryConfig())
    semantic = SemanticMemoryStore(firebase_client=fb, config=SemanticMemoryConfig())

    # Simulate a batch of platform-ingestion-shaped rows in Supabase.
    raw_docs = [
        _platform_doc(
            episode_id=f"pi-{i}",
            skill_name="platform-daily:GoogleAds-acme",
            tenant_id="acme",
            domain="paid_media",  # unknown domain — must not crash
        )
        for i in range(5)
    ]

    episodes = [episodic._doc_to_episode(d) for d in raw_docs]
    assert all(ep is not None for ep in episodes)
    assert all(ep.prediction.skill_name for ep in episodes), \
        "Every episode should have skill_name populated after A1 backfill"

    pattern = semantic.consolidate_from_episodes(episodes)
    assert pattern is not None, "Consolidation should succeed"
    assert pattern.skill_name == "platform-daily:GoogleAds-acme"
    # Unknown platform domain was coerced to GENERIC in _doc_to_episode, so
    # the consolidated pattern carries the safe fallback.
    assert pattern.domain == Domain.GENERIC


# ────────────────────────────────────────────────────────────────────────
# #3: pending_outcomes routing
# ────────────────────────────────────────────────────────────────────────


def test_phase1_fix_3_pending_outcomes_routes_to_new_table():
    path_root = "system/intelligence/pending_outcomes"
    path_client = "system/intelligence/pending_outcomes/acme-corp"

    assert SupabaseStorage._resolve_table(path_root) == "pending_outcomes"
    assert SupabaseStorage._resolve_table(path_client) == "pending_outcomes"
    assert SupabaseStorage._get_pk("pending_outcomes") == "prediction_id"


def test_phase1_fix_3_extract_filters_pulls_client_id_from_path():
    filters_root = SupabaseStorage._extract_filters(
        "system/intelligence/pending_outcomes"
    )
    filters_scoped = SupabaseStorage._extract_filters(
        "system/intelligence/pending_outcomes/acme-corp"
    )

    assert filters_root == {}
    assert filters_scoped == {"client_id": "acme-corp"}


def test_phase1_fix_3_pending_outcomes_in_table_map():
    from lib.supabase_storage import _PK_MAP, _TABLE_MAP

    assert _TABLE_MAP["system/intelligence/pending_outcomes"] == "pending_outcomes"
    assert _PK_MAP["pending_outcomes"] == "prediction_id"


# ────────────────────────────────────────────────────────────────────────
# Fixtures
# ────────────────────────────────────────────────────────────────────────


@pytest.fixture
def fake_firebase_factory():
    from tests.conftest import FakeFirebase

    def _factory():
        return FakeFirebase()

    return _factory
