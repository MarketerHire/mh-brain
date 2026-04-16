"""
MH1 Intelligence System Types

Core data structures for the memory and learning systems.
"""

import uuid
from enum import Enum
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple


class MemoryLayer(Enum):
    """Memory layer types following cognitive architecture."""
    WORKING = "working"         # Session-scoped, immediate context
    EPISODIC = "episodic"       # Specific experiences with decay
    SEMANTIC = "semantic"       # Learned patterns with confidence
    PROCEDURAL = "procedural"   # Cross-skill generalizations


class Domain(Enum):
    """Business domains for scoring adaptation."""
    CONTENT = "content"         # Engagement, impressions, growth
    REVENUE = "revenue"         # Deals, pipeline, velocity
    HEALTH = "health"           # Churn, retention, satisfaction
    CAMPAIGN = "campaign"       # ROI, attribution, CPL
    GENERIC = "generic"         # Fallback domain


# Aliases from platform-ingestion / legacy domain strings to the canonical
# Domain enum. Consumers should call ``normalize_domain(raw)`` rather than
# ``Domain(raw)`` so unexpected strings become GENERIC instead of raising.
_DOMAIN_ALIASES: Dict[str, "Domain"] = {
    # Platform ingestion writes these per lib/platform_ingestion/orchestrator.py
    "paid_media": None,          # resolved below once Domain is available
    "email": None,
    "crm": None,
    "ecommerce": None,
    "lifecycle": None,
    "product_analytics": None,
    "mobile": None,
    # Historical / internal aliases
    "campaigns": None,
    "engagement": None,
    "sales": None,
    "retention": None,
    "churn": None,
    "pipeline": None,
    "growth": None,
    "attribution": None,
}


def _init_domain_aliases() -> None:
    _DOMAIN_ALIASES.update({
        "paid_media": Domain.CAMPAIGN,
        "email": Domain.CONTENT,
        "crm": Domain.REVENUE,
        "ecommerce": Domain.REVENUE,
        "lifecycle": Domain.CONTENT,
        "product_analytics": Domain.HEALTH,
        "mobile": Domain.CAMPAIGN,
        "campaigns": Domain.CAMPAIGN,
        "engagement": Domain.CONTENT,
        "sales": Domain.REVENUE,
        "retention": Domain.HEALTH,
        "churn": Domain.HEALTH,
        "pipeline": Domain.REVENUE,
        "growth": Domain.CONTENT,
        "attribution": Domain.CAMPAIGN,
    })


_init_domain_aliases()


def normalize_domain(value: Any, *, default: "Domain" = None) -> "Domain":
    """Coerce any input into a valid ``Domain``.

    Resolution order:
      1. Already a ``Domain`` instance → return as-is.
      2. Canonical enum value (``"content"``, ``"revenue"``, ...) → enum.
      3. Platform / legacy alias → mapped enum (see ``_DOMAIN_ALIASES``).
      4. Anything else → ``default`` (defaults to ``Domain.GENERIC``).

    Never raises ``ValueError`` for unknown strings — the learning loop
    cannot afford to crash on a single bad episode.
    """
    fallback = default if default is not None else Domain.GENERIC
    if value is None or value == "":
        return fallback
    if isinstance(value, Domain):
        return value
    if isinstance(value, str):
        try:
            return Domain(value)
        except ValueError:
            pass
        return _DOMAIN_ALIASES.get(value.lower(), fallback)
    return fallback


class EpisodeSource(Enum):
    """Origin of an episodic memory entry."""
    SKILL_EXECUTION = "skill"
    JARVIS_INTERACTION = "jarvis"
    OPERATOR_FEEDBACK = "operator"
    CLIENT_FEEDBACK = "client"
    MARKET_OBSERVATION = "market"


@dataclass
class Prediction:
    """A prediction registered before skill execution."""
    prediction_id: str = field(default_factory=lambda: str(uuid.uuid4())[:12])
    skill_name: str = ""
    tenant_id: str = ""
    domain: Domain = Domain.GENERIC
    
    # What we predict
    expected_signal: float = 0.0
    expected_baseline: float = 1.0
    confidence: float = 0.5
    confidence_interval: Tuple[float, float] = (0.0, 1.0)
    
    # Context at prediction time
    context: Dict[str, Any] = field(default_factory=dict)
    patterns_used: List[str] = field(default_factory=list)  # IDs of semantic patterns
    is_exploration: bool = False
    
    # Timestamps
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "prediction_id": self.prediction_id,
            "skill_name": self.skill_name,
            "tenant_id": self.tenant_id,
            "domain": self.domain.value,
            "expected_signal": self.expected_signal,
            "expected_baseline": self.expected_baseline,
            "confidence": self.confidence,
            "confidence_interval": list(self.confidence_interval),
            "context": self.context,
            "patterns_used": self.patterns_used,
            "is_exploration": self.is_exploration,
            "created_at": self.created_at,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Prediction":
        """Reconstruct Prediction from dictionary."""
        domain = normalize_domain(data.get("domain", "generic"))

        confidence_interval = data.get("confidence_interval", [0.0, 1.0])
        if isinstance(confidence_interval, list):
            confidence_interval = tuple(confidence_interval)
        
        return cls(
            prediction_id=data.get("prediction_id", str(uuid.uuid4())[:12]),
            skill_name=data.get("skill_name", ""),
            tenant_id=data.get("tenant_id", ""),
            domain=domain,
            expected_signal=data.get("expected_signal", 0.0),
            expected_baseline=data.get("expected_baseline", 1.0),
            confidence=data.get("confidence", 0.5),
            confidence_interval=confidence_interval,
            context=data.get("context", {}),
            patterns_used=data.get("patterns_used", []),
            is_exploration=data.get("is_exploration", False),
            created_at=data.get("created_at", datetime.now(timezone.utc).isoformat()),
        )


@dataclass
class Outcome:
    """An observed outcome after skill execution."""
    outcome_id: str = field(default_factory=lambda: str(uuid.uuid4())[:12])
    prediction_id: str = ""
    
    # What actually happened
    observed_signal: float = 0.0
    observed_baseline: float = 1.0
    
    # Computed metrics
    prediction_error: float = 0.0
    goal_completed: bool = False
    business_impact: float = 0.0
    
    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)
    observed_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "outcome_id": self.outcome_id,
            "prediction_id": self.prediction_id,
            "observed_signal": self.observed_signal,
            "observed_baseline": self.observed_baseline,
            "prediction_error": self.prediction_error,
            "goal_completed": self.goal_completed,
            "business_impact": self.business_impact,
            "metadata": self.metadata,
            "observed_at": self.observed_at,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Outcome":
        """Reconstruct Outcome from dictionary."""
        return cls(
            outcome_id=data.get("outcome_id", str(uuid.uuid4())[:12]),
            prediction_id=data.get("prediction_id", ""),
            observed_signal=data.get("observed_signal", 0.0),
            observed_baseline=data.get("observed_baseline", 1.0),
            prediction_error=data.get("prediction_error", 0.0),
            goal_completed=data.get("goal_completed", False),
            business_impact=data.get("business_impact", 0.0),
            metadata=data.get("metadata", {}),
            observed_at=data.get("observed_at", datetime.now(timezone.utc).isoformat()),
        )


@dataclass
class EpisodicMemory:
    """
    A specific experience stored in episodic memory.
    Decays over time and consolidates into semantic patterns.
    """
    episode_id: str = field(default_factory=lambda: str(uuid.uuid4())[:12])
    prediction: Prediction = field(default_factory=Prediction)
    outcome: Outcome = field(default_factory=Outcome)
    
    # Memory properties
    weight: float = 1.0                    # Decays over time
    retrieval_count: int = 0               # How often retrieved
    last_retrieved_at: Optional[str] = None
    
    # Lifecycle
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    consolidated_at: Optional[str] = None  # When compressed to semantic
    archived_at: Optional[str] = None      # When moved to archive
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "episode_id": self.episode_id,
            "prediction": self.prediction.to_dict(),
            "outcome": self.outcome.to_dict(),
            "weight": self.weight,
            "retrieval_count": self.retrieval_count,
            "last_retrieved_at": self.last_retrieved_at,
            "created_at": self.created_at,
            "consolidated_at": self.consolidated_at,
            "archived_at": self.archived_at,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "EpisodicMemory":
        """Reconstruct EpisodicMemory from dictionary."""
        prediction_data = data.get("prediction", {})
        outcome_data = data.get("outcome", {})
        
        return cls(
            episode_id=data.get("episode_id", str(uuid.uuid4())[:12]),
            prediction=Prediction.from_dict(prediction_data) if prediction_data else Prediction(),
            outcome=Outcome.from_dict(outcome_data) if outcome_data else Outcome(),
            weight=data.get("weight", 1.0),
            retrieval_count=data.get("retrieval_count", 0),
            last_retrieved_at=data.get("last_retrieved_at"),
            created_at=data.get("created_at", datetime.now(timezone.utc).isoformat()),
            consolidated_at=data.get("consolidated_at"),
            archived_at=data.get("archived_at"),
        )


@dataclass
class TrajectoryPoint:
    """A single checkpoint in a pattern's expected trajectory.

    Patterns with trajectories predict how a metric evolves over time
    (e.g., churn spikes at day 7 then recovers by day 14) instead of
    recording a single final outcome ratio.
    """
    checkpoint_days: int
    expected_ratio: float
    confidence: float = 0.5
    observation_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "checkpoint_days": self.checkpoint_days,
            "expected_ratio": self.expected_ratio,
            "confidence": self.confidence,
            "observation_count": self.observation_count,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TrajectoryPoint":
        return cls(
            checkpoint_days=data.get("checkpoint_days", 0),
            expected_ratio=data.get("expected_ratio", 1.0),
            confidence=data.get("confidence", 0.5),
            observation_count=data.get("observation_count", 0),
        )


@dataclass
class SemanticPattern:
    """
    A learned pattern in semantic memory.
    Generalizes from multiple episodic memories.
    """
    pattern_id: str = field(default_factory=lambda: str(uuid.uuid4())[:12])
    
    # Pattern definition
    skill_name: str = ""
    domain: Domain = Domain.GENERIC
    condition: Dict[str, Any] = field(default_factory=dict)
    recommendation: Dict[str, Any] = field(default_factory=dict)
    
    # Learning state
    confidence: float = 0.5
    _expected_value: float = 1.0
    variance: float = 1.0
    
    # Trajectory (replaces single expected_value for multi-checkpoint patterns)
    expected_trajectory: List[TrajectoryPoint] = field(default_factory=list)
    expected_time_to_target_days: Optional[float] = None
    variance_days: Optional[float] = None
    
    # Evidence tracking
    evidence_count: int = 0
    successes: int = 0
    failures: int = 0
    recent_accuracy: float = 0.5
    
    # Lifecycle
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    source_episodes: List[str] = field(default_factory=list)

    @property
    def expected_value(self) -> float:
        """Backward-compatible: returns the final trajectory point's ratio."""
        if self.expected_trajectory:
            return self.expected_trajectory[-1].expected_ratio
        return self._expected_value

    @expected_value.setter
    def expected_value(self, value: float):
        self._expected_value = value
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "pattern_id": self.pattern_id,
            "skill_name": self.skill_name,
            "domain": self.domain.value,
            "condition": self.condition,
            "recommendation": self.recommendation,
            "confidence": self.confidence,
            "expected_value": self._expected_value,
            "variance": self.variance,
            "expected_trajectory": [tp.to_dict() for tp in self.expected_trajectory],
            "expected_time_to_target_days": self.expected_time_to_target_days,
            "variance_days": self.variance_days,
            "evidence_count": self.evidence_count,
            "successes": self.successes,
            "failures": self.failures,
            "recent_accuracy": self.recent_accuracy,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "source_episodes": self.source_episodes,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SemanticPattern":
        """Reconstruct SemanticPattern from dictionary."""
        domain = normalize_domain(data.get("domain", "generic"))

        trajectory = [
            TrajectoryPoint.from_dict(tp)
            for tp in data.get("expected_trajectory", [])
        ]
        
        pattern = cls(
            pattern_id=data.get("pattern_id", str(uuid.uuid4())[:12]),
            skill_name=data.get("skill_name", ""),
            domain=domain,
            condition=data.get("condition", {}),
            recommendation=data.get("recommendation", {}),
            confidence=data.get("confidence", 0.5),
            variance=data.get("variance", 1.0),
            expected_trajectory=trajectory,
            expected_time_to_target_days=data.get("expected_time_to_target_days"),
            variance_days=data.get("variance_days"),
            evidence_count=data.get("evidence_count", 0),
            successes=data.get("successes", 0),
            failures=data.get("failures", 0),
            recent_accuracy=data.get("recent_accuracy", 0.5),
            created_at=data.get("created_at", datetime.now(timezone.utc).isoformat()),
            updated_at=data.get("updated_at", datetime.now(timezone.utc).isoformat()),
            source_episodes=data.get("source_episodes", []),
        )
        pattern._expected_value = data.get("expected_value", 1.0)
        return pattern


@dataclass
class ProceduralKnowledge:
    """
    Cross-skill procedural knowledge in procedural memory.
    
    Procedural knowledge represents generalizations that apply across multiple skills.
    Unlike semantic patterns (which are skill-specific), procedural knowledge emerges
    when similar patterns are validated across different skill contexts.
    
    This enables the system to learn meta-strategies like:
    - "Morning sends work better for engagement across content, email, and social skills"
    - "Gradual rollouts reduce risk in campaigns, features, and pricing changes"
    - "Personalization improves outcomes across email, ads, and landing pages"
    """
    knowledge_id: str = field(default_factory=lambda: str(uuid.uuid4())[:12])
    
    # What the knowledge represents
    description: str = ""                    # Human-readable description
    pattern_type: str = ""                   # Category: timing, personalization, rollout, etc.
    knowledge: Dict[str, Any] = field(default_factory=dict)  # The actual knowledge/recommendations
    
    # Cross-skill applicability
    applicable_skills: List[str] = field(default_factory=list)  # Skills this applies to
    applicable_domains: List[str] = field(default_factory=list)  # Domains this applies to
    
    # Validation tracking
    validating_skills: Dict[str, float] = field(default_factory=dict)  # skill_name -> accuracy
    cross_skill_confidence: float = 0.5     # Confidence across all validating skills
    source_patterns: List[str] = field(default_factory=list)  # Pattern IDs that contributed
    
    # Lifecycle
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "knowledge_id": self.knowledge_id,
            "description": self.description,
            "pattern_type": self.pattern_type,
            "knowledge": self.knowledge,
            "applicable_skills": self.applicable_skills,
            "applicable_domains": self.applicable_domains,
            "validating_skills": self.validating_skills,
            "cross_skill_confidence": self.cross_skill_confidence,
            "source_patterns": self.source_patterns,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ProceduralKnowledge":
        """Reconstruct ProceduralKnowledge from dictionary."""
        return cls(
            knowledge_id=data.get("knowledge_id", str(uuid.uuid4())[:12]),
            description=data.get("description", ""),
            pattern_type=data.get("pattern_type", ""),
            knowledge=data.get("knowledge", {}),
            applicable_skills=data.get("applicable_skills", []),
            applicable_domains=data.get("applicable_domains", []),
            validating_skills=data.get("validating_skills", {}),
            cross_skill_confidence=data.get("cross_skill_confidence", 0.5),
            source_patterns=data.get("source_patterns", []),
            created_at=data.get("created_at", datetime.now(timezone.utc).isoformat()),
            updated_at=data.get("updated_at", datetime.now(timezone.utc).isoformat()),
        )


@dataclass
class ChannelContext:
    """Required context for any execution episode.

    Platforms (MH1HQ, MH-OS, mh1-skills) should populate this when
    writing events to the shared store. BrightMatter uses it for
    context matching, range computation, and pattern splitting.
    """
    client_id: str = ""
    industry: str = ""
    region: str = ""

    channel_id: str = ""
    account_age_days: Optional[int] = None
    active_days_last_90: Optional[int] = None
    dormancy_days: Optional[int] = None

    historical_primary_metric: Optional[float] = None
    monthly_spend: Optional[float] = None
    list_size: Optional[int] = None

    month: Optional[int] = None
    quarter: Optional[int] = None
    day_of_week: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in {
            "client_id": self.client_id,
            "industry": self.industry,
            "region": self.region,
            "channel_id": self.channel_id,
            "account_age_days": self.account_age_days,
            "active_days_last_90": self.active_days_last_90,
            "dormancy_days": self.dormancy_days,
            "historical_primary_metric": self.historical_primary_metric,
            "monthly_spend": self.monthly_spend,
            "list_size": self.list_size,
            "month": self.month,
            "quarter": self.quarter,
            "day_of_week": self.day_of_week,
        }.items() if v is not None}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ChannelContext":
        fields = {f.name for f in cls.__dataclass_fields__.values()}
        return cls(**{k: v for k, v in data.items() if k in fields})


_CHANNEL_CONTEXT_REQUIRED = ["client_id", "channel_id"]
_CHANNEL_CONTEXT_RECOMMENDED = ["industry", "account_age_days", "dormancy_days"]


def validate_channel_context(
    context: Dict[str, Any],
) -> Tuple[bool, str]:
    """Validate that channel context has required fields.

    Returns (is_valid, message). Missing recommended fields produce
    a warning message but still return is_valid=True.
    """
    import logging
    _logger = logging.getLogger(__name__)

    cc = context.get("channel_context", {})
    missing_req = [k for k in _CHANNEL_CONTEXT_REQUIRED if not cc.get(k)]
    missing_rec = [k for k in _CHANNEL_CONTEXT_RECOMMENDED if not cc.get(k)]

    if missing_req:
        return False, f"Missing required channel context: {missing_req}"
    if missing_rec:
        _logger.warning(
            f"Missing recommended channel context (lower pattern quality): {missing_rec}"
        )
    return True, ""


__all__ = [
    "MemoryLayer",
    "Domain",
    "normalize_domain",
    "EpisodeSource",
    "Prediction",
    "Outcome",
    "EpisodicMemory",
    "TrajectoryPoint",
    "SemanticPattern",
    "ProceduralKnowledge",
    "ChannelContext",
    "validate_channel_context",
]
