"""
MH1 Intelligence Predictor

Implements exploration/exploitation decision making for skill parameter guidance.
Uses semantic patterns and procedural knowledge to make informed predictions,
while maintaining exploration to discover new effective strategies.

The predictor balances:
- Exploitation: Using learned patterns with high confidence
- Exploration: Trying new parameter combinations to discover improvements
"""

import logging
import math
import random
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

from ..types import Domain, SemanticPattern, ProceduralKnowledge, EpisodicMemory
from ..adapters.channels import get_channel_config, ChannelConfig

if TYPE_CHECKING:
    from ..memory.semantic import SemanticMemoryStore
    from ..memory.procedural import ProceduralMemoryStore
    from ..memory.episodic import EpisodicMemoryStore

logger = logging.getLogger(__name__)


@dataclass
class Guidance:
    """
    Output from the predictor providing parameter guidance for skill execution.
    
    Contains recommended parameters, confidence levels, prediction data,
    and metadata about whether this is an exploration or exploitation decision.
    """
    parameters: Dict[str, Any] = field(default_factory=dict)
    confidence: float = 0.5
    uncertainty: float = 0.5
    is_exploration: bool = False
    exploration_reason: str = ""
    patterns_used: List[str] = field(default_factory=list)
    procedural_applied: List[str] = field(default_factory=list)
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    predicted_outcome: Optional[float] = None
    predicted_baseline: Optional[float] = None
    pattern_expected_value: Optional[float] = None
    context_matched: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def expected_signal(self) -> float:
        if self.predicted_outcome is not None:
            return self.predicted_outcome
        if self.pattern_expected_value is not None:
            return self.pattern_expected_value
        return 0.5

    @property
    def expected_baseline(self) -> float:
        return self.predicted_baseline if self.predicted_baseline is not None else 1.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "parameters": self.parameters,
            "confidence": self.confidence,
            "uncertainty": self.uncertainty,
            "is_exploration": self.is_exploration,
            "exploration_reason": self.exploration_reason,
            "patterns_used": self.patterns_used,
            "procedural_applied": self.procedural_applied,
            "created_at": self.created_at,
            "predicted_outcome": self.predicted_outcome,
            "predicted_baseline": self.predicted_baseline,
            "pattern_expected_value": self.pattern_expected_value,
            "context_matched": self.context_matched,
            "metadata": self.metadata,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Guidance":
        """Reconstruct Guidance from dictionary."""
        return cls(
            parameters=data.get("parameters", {}),
            confidence=data.get("confidence", 0.5),
            uncertainty=data.get("uncertainty", 0.5),
            is_exploration=data.get("is_exploration", False),
            exploration_reason=data.get("exploration_reason", ""),
            patterns_used=data.get("patterns_used", []),
            procedural_applied=data.get("procedural_applied", []),
            created_at=data.get("created_at", datetime.now(timezone.utc).isoformat()),
            predicted_outcome=data.get("predicted_outcome"),
            predicted_baseline=data.get("predicted_baseline"),
            pattern_expected_value=data.get("pattern_expected_value"),
            context_matched=data.get("context_matched", {}),
        )


@dataclass
class ExplorationConfig:
    """
    Configuration for exploration/exploitation balance.
    
    Attributes:
        base_exploration_rate: Probability of random exploration (default 15%)
        uncertainty_threshold: Explore if best pattern confidence below this (default 0.7)
        novelty_boost: Extra exploration probability for new/unseen skills (default 0.1)
        decay_exploration_with_evidence: Reduce exploration as evidence accumulates
    """
    base_exploration_rate: float = 0.15
    uncertainty_threshold: float = 0.7
    novelty_boost: float = 0.1
    decay_exploration_with_evidence: bool = True


class Predictor:
    """
    Makes exploration/exploitation decisions for skill parameter guidance.
    
    The predictor retrieves learned patterns from semantic memory and
    cross-skill knowledge from procedural memory to generate parameter
    recommendations. It maintains a balance between exploiting known
    good strategies and exploring potentially better alternatives.
    
    Exploration triggers:
    - Random exploration (base_exploration_rate)
    - No patterns available for skill/domain
    - Low confidence in best pattern
    - Novel context not seen before
    """
    
    def __init__(
        self,
        semantic_store: "SemanticMemoryStore",
        procedural_store: "ProceduralMemoryStore",
        config: Optional[ExplorationConfig] = None,
        episodic_store: Optional["EpisodicMemoryStore"] = None,
    ):
        """
        Initialize the predictor.

        Args:
            semantic_store: Store for skill-specific learned patterns
            procedural_store: Store for cross-skill procedural knowledge
            config: Exploration/exploitation configuration
            episodic_store: Optional store for raw past episodes. When
                provided, the predictor can supplement pattern-based
                reasoning with recent real-world observations
                (``_recent_episodes`` metadata on Guidance).
        """
        self._semantic_store = semantic_store
        self._procedural_store = procedural_store
        self._episodic_store = episodic_store
        self._config = config or ExplorationConfig()
    
    def get_guidance(
        self,
        skill_name: str,
        tenant_id: str,
        domain: Domain,
        context: Dict[str, Any]
    ) -> Guidance:
        """
        Main entry point: get parameter guidance for a skill execution.
        
        Retrieves relevant patterns and procedural knowledge, then decides
        whether to explore new parameters or exploit known good ones.
        
        When Phase 0 computed_metrics are present in context (under the key
        ``_phase0_computed_metrics``), the predictor uses real CRM data for
        pattern matching.  For example, if Phase 0 reports churn_rate=0.15,
        the predictor can match patterns that previously produced good
        results at similar churn levels — leading to more accurate guidance
        instead of the default exploration-mode parameters.
        
        Args:
            skill_name: Name of the skill being executed
            tenant_id: Tenant identifier for pattern retrieval
            domain: Business domain for the skill
            context: Current execution context (e.g., customer segment, time of day).
                May contain ``_phase0_computed_metrics`` and ``phase0_*`` keys
                injected by the IntelligenceBridge.
            
        Returns:
            Guidance with recommended parameters and metadata
        """
        logger.debug(f"Getting guidance for {skill_name} in domain {domain.value}")
        
        # Step 1: Retrieve relevant patterns from semantic memory
        patterns = self._retrieve_patterns(skill_name, tenant_id, domain, context)
        
        # Step 2: Get procedural knowledge for this skill/domain
        procedural = self._retrieve_procedural(skill_name, domain)

        # Step 2A: Pull recent episodes (if an episodic store is wired in)
        # so we can surface real observed outcomes alongside pattern-based
        # predictions. This closes the loop between what the semantic
        # patterns *expect* and what episodes actually reported.
        episodes = self._retrieve_episodes(skill_name, tenant_id, domain)
        episode_stats = self._summarize_episodes(episodes)

        # Step 2.5: If Phase 0 data is available, adjust default parameters
        # to reflect actual data state (e.g., set segment_count based on
        # actual lifecycle distribution, thresholds based on real churn rate).
        phase0_adjustments = self._phase0_parameter_adjustments(
            skill_name, domain, context
        )

        # Step 3: Decide explore or exploit
        should_explore, reason = self._should_explore(patterns, context)
        
        # Step 4: Generate guidance
        if should_explore or not patterns:
            guidance = self._explore(skill_name, domain, context, procedural, reason)
        else:
            guidance = self._exploit(
                patterns, procedural, context,
                skill_name=skill_name, tenant_id=tenant_id, domain=domain,
            )
        
        # Step 5: Apply Phase 0 adjustments on top of guidance
        if phase0_adjustments:
            for key, value in phase0_adjustments.items():
                if key not in guidance.parameters:
                    guidance.parameters[key] = value
            # Phase 0 data increases confidence since we have real data
            guidance.confidence = min(1.0, guidance.confidence + 0.1)
            guidance.uncertainty = max(0.0, guidance.uncertainty - 0.1)

        # Step 5.5: Attach episodic evidence to guidance metadata so
        # downstream consumers (and the learning loop) can compare the
        # pattern-based prediction against real recent outcomes.
        if episode_stats:
            guidance.metadata["recent_episodes"] = episode_stats
            # When episodes disagree sharply with the pattern prediction,
            # widen uncertainty a little so the confidence score reflects
            # the conflict. We never *narrow* uncertainty here — only the
            # full learning loop (Learner) has permission to do that.
            pattern_ev = guidance.pattern_expected_value
            mean_ratio = episode_stats.get("mean_signal_ratio")
            if (
                pattern_ev is not None
                and mean_ratio is not None
                and pattern_ev > 0
            ):
                divergence = abs(mean_ratio - pattern_ev) / max(pattern_ev, 1e-6)
                if divergence > 0.5:
                    bump = min(0.2, divergence * 0.1)
                    guidance.uncertainty = min(1.0, guidance.uncertainty + bump)
                    guidance.confidence = max(0.0, guidance.confidence - bump)
                    guidance.metadata.setdefault("episodic_divergence", {})
                    guidance.metadata["episodic_divergence"].update({
                        "pattern_expected_value": pattern_ev,
                        "observed_mean_ratio": mean_ratio,
                        "divergence": divergence,
                    })

        # Step 6: Enrich with reference knowledge (expert frameworks, tactics)
        try:
            ref_enrichments = self._enrich_with_reference_knowledge(
                skill_name, domain, context, guidance
            )
            if ref_enrichments:
                guidance.metadata["reference_knowledge"] = ref_enrichments
        except Exception as e:
            logger.debug(f"Reference knowledge enrichment skipped: {e}")

        # Step 7: Incorporate upstream strategy context
        # When upstream strategy skills have completed (e.g., positioning-angles
        # before email-sequences), their outputs inform this skill's parameters.
        # This closes the feedback loop: strategy → execution → learning.
        upstream = context.get("_upstream_strategy")
        if isinstance(upstream, dict) and upstream:
            strategy_adjustments = self._upstream_strategy_adjustments(
                skill_name, domain, upstream
            )
            if strategy_adjustments:
                for key, value in strategy_adjustments.items():
                    if key not in guidance.parameters:
                        guidance.parameters[key] = value
                guidance.confidence = min(1.0, guidance.confidence + 0.05)
                guidance.uncertainty = max(0.0, guidance.uncertainty - 0.05)

        return guidance
    
    def _retrieve_patterns(
        self,
        skill_name: str,
        tenant_id: str,
        domain: Domain,
        context: Optional[Dict[str, Any]] = None,
    ) -> List[SemanticPattern]:
        """Retrieve relevant semantic patterns, filtering by min_sample_size."""
        try:
            patterns = self._semantic_store.retrieve_patterns(
                skill_name=skill_name,
                domain=domain,
                tenant_id=tenant_id,
            )
            channel_id = (context or {}).get("channel_id", "")
            cc = get_channel_config(channel_id) if channel_id else None
            min_samples = cc.min_sample_size if cc else 0

            if min_samples > 0:
                patterns = [
                    p for p in patterns
                    if p.recommendation.get("sample_count", p.evidence_count) >= min_samples
                ]
            return patterns
        except Exception as e:
            logger.warning(f"Failed to retrieve patterns: {e}")
        return []
    
    def _retrieve_episodes(
        self,
        skill_name: str,
        tenant_id: str,
        domain: Domain,
        limit: int = 20,
    ) -> List["EpisodicMemory"]:
        """Pull recent episodes for this skill/tenant/domain.

        Returns an empty list if no episodic store is wired, the store
        has no matching rows, or retrieval errors out — the predictor
        must never fail because the episodic layer is missing or slow.
        """
        if self._episodic_store is None:
            return []
        try:
            episodes = self._episodic_store.retrieve(
                tenant_id=tenant_id,
                skill_name=skill_name,
                domain=domain,
                limit=limit,
            )
            return episodes or []
        except Exception as e:
            logger.debug(f"Episodic retrieve skipped: {e}")
            return []

    @staticmethod
    def _summarize_episodes(
        episodes: List["EpisodicMemory"],
    ) -> Dict[str, Any]:
        """Collapse a list of episodes into a small metadata dict.

        Returns keys:
          - ``count``: number of episodes summarised
          - ``mean_observed_signal``: arithmetic mean of observed_signal
          - ``mean_signal_ratio``: observed_signal / observed_baseline,
            averaged across episodes with non-zero baseline
          - ``goal_completion_rate``: fraction where goal_completed=True
          - ``last_observed_at``: most recent created_at timestamp
          - ``episode_ids``: top N ids (truncated) for traceability
        """
        if not episodes:
            return {}

        signals: List[float] = []
        ratios: List[float] = []
        goals: List[bool] = []
        timestamps: List[str] = []
        ids: List[str] = []

        for ep in episodes:
            outcome = getattr(ep, "outcome", None)
            if outcome is None:
                continue
            raw_signal = getattr(outcome, "observed_signal", None)
            raw_baseline = getattr(outcome, "observed_baseline", None)

            if isinstance(raw_signal, (int, float)):
                signals.append(float(raw_signal))
                if isinstance(raw_baseline, (int, float)) and raw_baseline > 0:
                    ratios.append(float(raw_signal) / float(raw_baseline))

            goal = getattr(outcome, "goal_completed", None)
            if isinstance(goal, bool):
                goals.append(goal)

            ts = getattr(ep, "created_at", "") or ""
            if ts:
                timestamps.append(ts)
            eid = getattr(ep, "episode_id", "") or ""
            if eid:
                ids.append(eid)

        if not signals and not goals:
            return {}

        summary: Dict[str, Any] = {"count": len(episodes)}
        if signals:
            summary["mean_observed_signal"] = sum(signals) / len(signals)
        if ratios:
            summary["mean_signal_ratio"] = sum(ratios) / len(ratios)
        if goals:
            summary["goal_completion_rate"] = sum(
                1 for g in goals if g
            ) / len(goals)
        if timestamps:
            summary["last_observed_at"] = max(timestamps)
        if ids:
            summary["episode_ids"] = ids[:10]

        return summary

    def _retrieve_procedural(
        self,
        skill_name: str,
        domain: Domain
    ) -> List[ProceduralKnowledge]:
        """Retrieve applicable procedural knowledge."""
        try:
            if hasattr(self._procedural_store, "get_applicable"):
                return self._procedural_store.get_applicable(
                    skill_name=skill_name,
                    domain=domain.value
                )
            elif hasattr(self._procedural_store, "retrieve"):
                return self._procedural_store.retrieve(
                    skill_name=skill_name,
                    domain=domain
                )
        except Exception as e:
            logger.warning(f"Failed to retrieve procedural knowledge: {e}")
        return []
    
    def _should_explore(
        self,
        patterns: List[SemanticPattern],
        context: Dict[str, Any]
    ) -> Tuple[bool, str]:
        """
        Decide whether to explore or exploit.
        
        Returns (should_explore, reason) based on:
        1. Random exploration: random.random() < base_exploration_rate
        2. No patterns available
        3. Best pattern confidence < uncertainty_threshold
        4. No patterns match current context
        
        Args:
            patterns: Available semantic patterns
            context: Current execution context
            
        Returns:
            Tuple of (should_explore, reason_string)
        """
        # Check 1: Random exploration
        if random.random() < self._config.base_exploration_rate:
            return (True, "random_exploration")
        
        # Check 2: No patterns available
        if not patterns:
            return (True, "no_patterns_available")
        
        # Check 3: Best pattern confidence below threshold
        best_pattern = max(patterns, key=lambda p: p.confidence * p.recent_accuracy)
        if best_pattern.confidence < self._config.uncertainty_threshold:
            return (True, f"low_confidence_{best_pattern.confidence:.2f}")
        
        # Check 4: No patterns match the current context
        matching_patterns = [
            p for p in patterns
            if self._context_matches(p.condition, context)
        ]
        if not matching_patterns:
            return (True, "novel_context")
        
        # Exploit: use learned patterns
        return (False, "")
    
    def _explore(
        self,
        skill_name: str,
        domain: Domain,
        context: Dict[str, Any],
        procedural: List[ProceduralKnowledge],
        reason: str
    ) -> Guidance:
        """
        Generate exploratory guidance with perturbed parameters.
        
        When exploring, we:
        1. Start with default parameters for the skill/domain
        2. Apply any applicable procedural knowledge
        3. Perturb parameters to try new values
        
        Args:
            skill_name: Name of the skill
            domain: Business domain
            context: Current execution context
            procedural: Applicable procedural knowledge
            reason: Why we're exploring
            
        Returns:
            Guidance with exploration flag set
        """
        logger.debug(f"Exploring for {skill_name}: {reason}")
        
        # Step 1: Get default parameters
        parameters = self._get_default_parameters(skill_name, domain)
        
        # Step 2: Apply procedural knowledge
        procedural_applied = []
        for knowledge in procedural:
            parameters = self._apply_procedural(parameters, knowledge)
            procedural_applied.append(knowledge.knowledge_id)
        
        # Step 3: Perturb for exploration
        parameters = self._perturb_parameters(parameters)
        
        return Guidance(
            parameters=parameters,
            confidence=0.3,
            uncertainty=0.7,
            is_exploration=True,
            exploration_reason=reason,
            patterns_used=[],
            procedural_applied=procedural_applied,
        )
    
    def _get_blended_guidance(
        self,
        skill_name: str,
        tenant_id: str,
        domain: "Domain",
        patterns: List[SemanticPattern],
        context: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], float, float, List[str]]:
        """Retrieve and blend patterns across three specificity levels.

        Level 3 (Client): client_id=tenant_id, any conditions
        Level 2 (Segment): client_id=None (cross-client), tight context match
        Level 1 (Universal): client_id=None, broad/empty conditions

        Returns (blended_parameters, blended_expected_value, blended_confidence, pattern_ids)
        """
        # Level 3: client-specific (already retrieved for this tenant)
        client_matches = [
            p for p in patterns if self._context_matches(p.condition, context)
        ]

        # Level 2 + 1: cross-client patterns (tenant_id="*")
        segment_patterns: List[SemanticPattern] = []
        try:
            segment_patterns = self._retrieve_patterns(skill_name, "*", domain)
        except Exception:
            pass

        segment_matches = [
            p for p in segment_patterns
            if p.condition and not p.condition.get("_universal")
            and self._context_matches(p.condition, context)
        ]
        universal_matches = [
            p for p in segment_patterns
            if not p.condition or p.condition.get("_universal")
        ]

        def _level_weight(pats: List[SemanticPattern]) -> float:
            return sum(p.evidence_count * p.confidence for p in pats)

        cw = _level_weight(client_matches)
        sw = _level_weight(segment_matches)
        uw = _level_weight(universal_matches)
        total = cw + sw + uw

        if total == 0:
            return {}, 1.0, 0.5, []

        cw_n, sw_n, uw_n = cw / total, sw / total, uw / total

        def _weighted_ev(pats: List[SemanticPattern]) -> Optional[float]:
            if not pats:
                return None
            weights = [p.evidence_count * p.confidence for p in pats]
            tw = sum(weights)
            if tw == 0:
                return None
            return sum(p.expected_value * w for p, w in zip(pats, weights)) / tw

        c_ev = _weighted_ev(client_matches)
        s_ev = _weighted_ev(segment_matches)
        u_ev = _weighted_ev(universal_matches)

        blended_ev = 0.0
        if c_ev is not None:
            blended_ev += cw_n * c_ev
        if s_ev is not None:
            blended_ev += sw_n * s_ev
        if u_ev is not None:
            blended_ev += uw_n * u_ev

        blended_conf = (
            cw_n * max((p.confidence for p in client_matches), default=0.5)
            + sw_n * max((p.confidence for p in segment_matches), default=0.5)
            + uw_n * max((p.confidence for p in universal_matches), default=0.5)
        )

        # Parameters: prefer client > segment > universal
        blended_params: Dict[str, Any] = {}
        for p in universal_matches:
            blended_params.update(p.recommendation)
        for p in segment_matches:
            blended_params.update(p.recommendation)
        for p in client_matches:
            blended_params.update(p.recommendation)

        all_ids = (
            [p.pattern_id for p in client_matches]
            + [p.pattern_id for p in segment_matches]
            + [p.pattern_id for p in universal_matches]
        )

        logger.debug(
            f"Blended guidance: client={cw_n:.0%}({len(client_matches)}p) "
            f"segment={sw_n:.0%}({len(segment_matches)}p) "
            f"universal={uw_n:.0%}({len(universal_matches)}p) "
            f"ev={blended_ev:.3f} conf={blended_conf:.3f}"
        )

        return blended_params, blended_ev, blended_conf, all_ids

    def _exploit(
        self,
        patterns: List[SemanticPattern],
        procedural: List[ProceduralKnowledge],
        context: Dict[str, Any],
        skill_name: str = "",
        tenant_id: str = "",
        domain: Optional["Domain"] = None,
    ) -> Guidance:
        """
        Generate guidance by exploiting learned patterns.

        Uses three-level hierarchical blending (client → segment → universal)
        to produce predictions that smoothly transition from generic to
        client-specific as evidence accumulates.
        """
        # Hierarchical blending across three levels
        blended_params, blended_ev, blended_conf, all_pattern_ids = (
            self._get_blended_guidance(
                skill_name, tenant_id, domain, patterns, context,
            )
        )

        # If blending returned nothing, fall back to single best pattern
        if not blended_params and not all_pattern_ids:
            matching_patterns = [
                p for p in patterns if self._context_matches(p.condition, context)
            ]
            if not matching_patterns:
                matching_patterns = patterns
            best = max(matching_patterns, key=lambda p: p.confidence * p.recent_accuracy)
            blended_params = dict(best.recommendation)
            blended_ev = best.expected_value
            blended_conf = best.confidence * best.recent_accuracy
            all_pattern_ids = [best.pattern_id]

        parameters = dict(blended_params)

        # Apply procedural knowledge
        procedural_applied = []
        for knowledge in procedural:
            parameters = self._apply_procedural(parameters, knowledge)
            procedural_applied.append(knowledge.knowledge_id)

        # Blend procedural confidence
        if procedural and procedural_applied:
            proc_confs = [
                k.cross_skill_confidence for k in procedural
                if k.knowledge_id in procedural_applied
            ]
            if proc_confs:
                blended_conf = (blended_conf + sum(proc_confs) / len(proc_confs)) / 2

        combined_confidence = max(0.0, min(1.0, blended_conf))

        # Compute predicted outcome from blended expected_value × client baseline
        pattern_expected_value = blended_ev
        phase0 = context.get("_phase0_computed_metrics", {})

        client_baseline = (
            phase0.get("churn_rate")
            or phase0.get("current_cpa")
            or phase0.get("current_roas")
            or context.get("baseline_metric")
        )

        predicted_outcome = None
        predicted_baseline = None
        if client_baseline is not None and isinstance(client_baseline, (int, float)):
            predicted_outcome = pattern_expected_value * float(client_baseline)
            predicted_baseline = float(client_baseline)

        context_matched = {}
        for pid in all_pattern_ids[:1]:
            for p in patterns:
                if p.pattern_id == pid:
                    context_matched = {
                        k: v for k, v in context.items() if k in p.condition
                    }
                    break

        logger.debug(
            f"Exploiting {len(all_pattern_ids)} patterns (blended) "
            f"with confidence {combined_confidence:.2f}, "
            f"predicted_outcome={predicted_outcome}"
        )

        return Guidance(
            parameters=parameters,
            confidence=combined_confidence,
            uncertainty=1.0 - combined_confidence,
            is_exploration=False,
            exploration_reason="",
            patterns_used=all_pattern_ids,
            procedural_applied=procedural_applied,
            predicted_outcome=predicted_outcome,
            predicted_baseline=predicted_baseline,
            pattern_expected_value=pattern_expected_value,
            context_matched=context_matched,
        )
    
    def _phase0_parameter_adjustments(
        self,
        skill_name: str,
        domain: Domain,
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Derive parameter adjustments from Phase 0 computed_metrics.

        When Phase 0 retrieval has run, the context contains real CRM data
        under ``_phase0_computed_metrics``.  This method translates that
        data into concrete parameter recommendations:

        - **Health domain**: Uses lifecycle distribution to set segment counts,
          churn rate to set alert thresholds, at-risk percentage to set
          engagement thresholds.
        - **Revenue domain**: Uses pipeline metrics for velocity expectations,
          deal counts for batch sizing.
        - **Campaign domain**: Uses engagement rates for baseline expectations.
        - **Content domain**: Uses audience metrics for targeting parameters.

        Returns an empty dict if no Phase 0 data is available.
        """
        cm = context.get("_phase0_computed_metrics")
        if not cm or not isinstance(cm, dict):
            return {}

        adjustments: Dict[str, Any] = {}

        # ── Universal adjustments ─────────────────────────────────────
        total_records = cm.get("total_records", 0)
        if total_records > 0:
            adjustments["data_record_count"] = total_records
            # Suggest batch size based on data volume
            if total_records > 100000:
                adjustments["batch_size"] = 10000
            elif total_records > 10000:
                adjustments["batch_size"] = 1000

        # ── Domain-specific adjustments ───────────────────────────────
        if domain == Domain.HEALTH:
            churn_rate = cm.get("churn_rate")
            if isinstance(churn_rate, (int, float)):
                # More aggressive threshold when churn is high
                adjustments["alert_threshold"] = max(0.2, min(0.5, churn_rate * 1.5))
                adjustments["focus_retention"] = churn_rate > 0.10

            at_risk_rate = cm.get("at_risk_rate")
            if isinstance(at_risk_rate, (int, float)):
                adjustments["engagement_threshold"] = max(0.05, at_risk_rate * 0.8)

            # Use lifecycle distribution to inform segment count
            lifecycle_dist = cm.get("lifecycle_distribution", {})
            if isinstance(lifecycle_dist, dict) and lifecycle_dist:
                non_trivial = sum(1 for v in lifecycle_dist.values()
                                 if isinstance(v, (int, float)) and v > 0.02)
                adjustments["segment_count"] = max(3, min(8, non_trivial + 1))

        elif domain == Domain.REVENUE:
            avg_deal = cm.get("avg_deal_size")
            if isinstance(avg_deal, (int, float)) and avg_deal > 0:
                adjustments["avg_deal_size_observed"] = avg_deal
                adjustments["include_revenue_impact"] = True

            pipeline_velocity = cm.get("pipeline_velocity")
            if isinstance(pipeline_velocity, (int, float)):
                adjustments["expected_velocity"] = pipeline_velocity

            conversion_rate = cm.get("conversion_rate")
            if isinstance(conversion_rate, (int, float)):
                adjustments["conversion_baseline"] = conversion_rate

        elif domain == Domain.CAMPAIGN:
            open_rate = cm.get("open_rate")
            if isinstance(open_rate, (int, float)):
                adjustments["expected_open_rate"] = open_rate

            click_rate = cm.get("click_rate")
            if isinstance(click_rate, (int, float)):
                adjustments["expected_click_rate"] = click_rate

            engagement_rate = cm.get("engagement_rate")
            if isinstance(engagement_rate, (int, float)):
                adjustments["engagement_baseline"] = engagement_rate

        elif domain == Domain.CONTENT:
            # Content skills benefit from knowing audience size
            audience_size = cm.get("total_contacts") or cm.get("total_records")
            if isinstance(audience_size, (int, float)) and audience_size > 0:
                adjustments["audience_size"] = int(audience_size)

        return adjustments

    def _upstream_strategy_adjustments(
        self,
        skill_name: str,
        domain: Domain,
        upstream: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Derive parameter adjustments from upstream strategy skill outputs.

        When strategy skills (e.g., positioning-angles, lifecycle-audit)
        complete before execution skills (e.g., email-sequences, direct-
        response-copy), their outputs carry recommendations that should
        inform downstream parameter choices.

        This closes the feedback loop: the Predictor records which upstream
        strategy context led to good downstream outcomes, so over time it
        learns to recommend parameters that align with effective strategies.

        Args:
            skill_name: The current skill being guided
            domain: Business domain for the skill
            upstream: Dict mapping dep_node_id → {skill_id, metrics, summary}

        Returns:
            Dict of parameter adjustments (may be empty)
        """
        adjustments: Dict[str, Any] = {}

        # Collect upstream skill IDs for pattern context
        upstream_skills = [
            info.get("skill_id", "")
            for info in upstream.values()
            if isinstance(info, dict)
        ]
        if upstream_skills:
            adjustments["upstream_skills"] = upstream_skills

        # Propagate numeric metrics from upstream strategy nodes
        for _dep_id, info in upstream.items():
            if not isinstance(info, dict):
                continue
            dep_skill = info.get("skill_id", "")
            dep_metrics = info.get("metrics", {})

            if not isinstance(dep_metrics, dict):
                continue

            # Forward strategy-produced metrics with namespaced keys
            # so the learning loop can correlate them with outcomes
            for key, val in dep_metrics.items():
                if isinstance(val, (int, float)):
                    adjustments[f"upstream_{dep_skill}_{key}"] = val

        return adjustments

    _IDENTITY_KEYS = frozenset({
        "client_id", "channel_id", "platform", "account_type",
    })
    _TEMPORAL_KEYS = frozenset({"quarter", "month", "day_of_week"})
    _SPEND_KEYS = frozenset({"monthly_spend", "spend", "budget"})
    _STRATEGY_KEYS = frozenset({"strategy", "funnel_stage", "campaign"})
    _MATCH_THRESHOLD = 0.6

    def _weighted_context_score(
        self,
        pattern_ctx: Dict[str, Any],
        current_ctx: Dict[str, Any],
        channel_config: Optional[ChannelConfig] = None,
        pattern_updated_at: Optional[str] = None,
    ) -> float:
        """Score context match between a pattern and current context (0.0-1.0).

        Dimensions are weighted by ChannelConfig properties:
        - Identity (client_id, channel_id, platform): always 1.0
        - Temporal (quarter, month): weighted by seasonality_weight
        - Spend (monthly_spend, budget): weighted by budget_sensitivity
        - Strategy (strategy, funnel_stage): 0.8
        - Other: 0.5

        Includes era penalty: patterns >2 years old are progressively penalized.
        """
        if not pattern_ctx:
            return 1.0

        seasonality_w = channel_config.seasonality_weight if channel_config else 0.0
        budget_w = channel_config.budget_sensitivity if channel_config else 0.0

        total_weight = 0.0
        matched_weight = 0.0

        for key, pattern_value in pattern_ctx.items():
            if key.startswith("_"):
                continue
            if key not in current_ctx:
                dim_w = self._dimension_weight(key, seasonality_w, budget_w)
                total_weight += dim_w
                continue

            dim_w = self._dimension_weight(key, seasonality_w, budget_w)
            total_weight += dim_w

            current_value = current_ctx[key]
            if self._values_match(pattern_value, current_value):
                matched_weight += dim_w

        if total_weight == 0:
            return 1.0

        score = matched_weight / total_weight

        if pattern_updated_at:
            days_since = self._days_since_iso(pattern_updated_at)
            era_penalty = max(0.0, 1.0 - (days_since / 730))
            score *= era_penalty

        return score

    def _dimension_weight(
        self, key: str, seasonality_w: float, budget_w: float
    ) -> float:
        if key in self._IDENTITY_KEYS:
            return 1.0
        if key in self._TEMPORAL_KEYS:
            return max(0.2, seasonality_w) if seasonality_w > 0 else 0.3
        if key in self._SPEND_KEYS:
            return max(0.2, budget_w) if budget_w > 0 else 0.3
        if key in self._STRATEGY_KEYS:
            return 0.8
        return 0.5

    @staticmethod
    def _values_match(pattern_value: Any, current_value: Any) -> bool:
        if isinstance(pattern_value, (int, float)) and isinstance(current_value, (int, float)):
            if pattern_value == 0:
                return current_value == 0
            tolerance = abs(pattern_value) * 0.3
            return abs(current_value - pattern_value) <= tolerance
        return str(pattern_value) == str(current_value)

    @staticmethod
    def _days_since_iso(iso_ts: str) -> float:
        try:
            dt = datetime.fromisoformat(iso_ts.replace("Z", "+00:00"))
            return max(0.0, (datetime.now(timezone.utc) - dt).total_seconds() / 86400)
        except (ValueError, AttributeError):
            return 0.0

    def _context_matches(
        self,
        pattern_ctx: Dict[str, Any],
        current_ctx: Dict[str, Any],
    ) -> bool:
        """Backward-compatible boolean wrapper around weighted scoring."""
        return self._weighted_context_score(pattern_ctx, current_ctx) >= self._MATCH_THRESHOLD
    
    def _get_default_parameters(
        self,
        skill_name: str,
        domain: Domain
    ) -> Dict[str, Any]:
        """
        Get sensible default parameters for known skills.
        
        Provides reasonable starting points for exploration when
        no learned patterns exist.
        
        Args:
            skill_name: Name of the skill
            domain: Business domain
            
        Returns:
            Dictionary of default parameters
        """
        # Skill-specific defaults
        skill_defaults = {
            "dormant-detection": {
                "inactivity_days": 90,
                "engagement_threshold": 0.1,
                "lookback_window_days": 180,
                "min_previous_activity": 3,
                "include_partial_churn": True,
            },
            "lifecycle-audit": {
                "segment_count": 5,
                "min_segment_size": 100,
                "analyze_transitions": True,
                "cohort_period_days": 30,
                "include_revenue_impact": True,
            },
            "content-strategy": {
                "topics_per_pillar": 5,
                "content_depth": "comprehensive",
                "keyword_density": 0.02,
                "min_word_count": 1500,
                "include_cta": True,
            },
            "email-campaign": {
                "send_hour": 10,
                "send_day": "tuesday",
                "subject_length": 50,
                "personalization_level": "medium",
                "ab_test_split": 0.1,
            },
            "lead-scoring": {
                "score_range_max": 100,
                "decay_rate_per_day": 0.02,
                "activity_weight": 0.4,
                "demographic_weight": 0.3,
                "behavioral_weight": 0.3,
            },
        }
        
        # Domain-specific adjustments
        domain_adjustments = {
            Domain.REVENUE: {
                "include_revenue_impact": True,
                "prioritize_high_value": True,
            },
            Domain.HEALTH: {
                "focus_retention": True,
                "alert_threshold": 0.3,
            },
            Domain.CONTENT: {
                "engagement_focus": True,
                "virality_weight": 0.2,
            },
            Domain.CAMPAIGN: {
                "track_attribution": True,
                "roi_threshold": 1.5,
            },
        }
        
        # Start with skill defaults or empty dict
        params = dict(skill_defaults.get(skill_name, {}))
        
        # Apply domain adjustments
        if domain in domain_adjustments:
            params.update(domain_adjustments[domain])
        
        # If no skill-specific defaults, provide generic parameters
        if not params:
            params = {
                "threshold": 0.5,
                "window_days": 30,
                "limit": 100,
                "include_metadata": True,
            }
        
        return params
    
    def _apply_procedural(
        self,
        parameters: Dict[str, Any],
        knowledge: ProceduralKnowledge
    ) -> Dict[str, Any]:
        """
        Blend procedural knowledge into parameters.
        
        For numeric parameters: 70% existing + 30% procedural
        For non-numeric: procedural overrides if present
        
        Args:
            parameters: Current parameter dictionary
            knowledge: Procedural knowledge to apply
            
        Returns:
            Updated parameters dictionary
        """
        if not knowledge.knowledge:
            return parameters
        
        result = dict(parameters)
        
        for key, procedural_value in knowledge.knowledge.items():
            if key in result:
                existing_value = result[key]
                
                # Numeric blending: 70% existing + 30% procedural
                if isinstance(existing_value, (int, float)) and isinstance(procedural_value, (int, float)):
                    blended = existing_value * 0.7 + procedural_value * 0.3
                    # Preserve int type if both were ints
                    if isinstance(existing_value, int) and isinstance(procedural_value, int):
                        result[key] = int(round(blended))
                    else:
                        result[key] = blended
                else:
                    # Non-numeric: keep existing (procedural doesn't override)
                    pass
            else:
                # New key from procedural: add it
                result[key] = procedural_value
        
        return result
    
    def _perturb_parameters(
        self,
        parameters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Add ±20% noise to numeric parameters for exploration.
        
        This helps discover potentially better parameter values
        by trying variations around the current settings.
        
        Args:
            parameters: Current parameter dictionary
            
        Returns:
            Perturbed parameters dictionary
        """
        result = dict(parameters)
        perturbation_range = 0.2  # ±20%
        
        for key, value in result.items():
            if isinstance(value, (int, float)):
                # Calculate perturbation
                perturbation = value * random.uniform(-perturbation_range, perturbation_range)
                perturbed = value + perturbation
                
                # Preserve int type
                if isinstance(value, int):
                    result[key] = int(round(perturbed))
                else:
                    result[key] = perturbed
                
                # Ensure non-negative for typical parameters
                if result[key] < 0 and value >= 0:
                    result[key] = 0 if isinstance(value, int) else 0.0
        
        return result


    # ── Reference knowledge enrichment ───────────────────────────────

    @staticmethod
    def _build_ref_tags(skill_name: str, domain: Domain, context: Dict[str, Any]) -> List[str]:
        """Derive search tags from skill/domain/context for reference_knowledge queries."""
        tags: List[str] = []
        channel_id = context.get("channel_id", "")
        if channel_id:
            parts = channel_id.split(".")
            tags.extend(parts)
        channel = context.get("channel", "")
        if channel:
            tags.append(channel.lower())
        dom_map = {
            Domain.CAMPAIGN: "advertising",
            Domain.CONTENT: "strategy",
            Domain.HEALTH: "retention",
            Domain.REVENUE: "growth",
        }
        if domain in dom_map:
            tags.append(dom_map[domain])
        strategy = context.get("strategy", "")
        if strategy:
            tags.append(strategy.lower().replace(" ", "-"))
        return [t for t in tags if t]

    def _enrich_with_reference_knowledge(
        self,
        skill_name: str,
        domain: Domain,
        context: Dict[str, Any],
        guidance: Guidance,
    ) -> Dict[str, Any]:
        """Query reference_knowledge and attach relevant frameworks/tactics to guidance.

        Returns a dict with keys:
          - expert_frameworks: list of {expert, framework, purpose, instant_fail_rules}
          - tactics: list of {title, summary, tag}
          - total_matches: int
        """
        try:
            from lib.supabase_client import get_supabase_or_none
        except ImportError:
            return {}

        db = get_supabase_or_none()
        if db is None:
            return {}

        tags = self._build_ref_tags(skill_name, domain, context)
        if not tags:
            return {}

        try:
            result = (
                db.table("reference_knowledge")
                .select("source,title,summary,content,tags,expert_handle,confidence_weight")
                .overlaps("tags", tags)
                .order("confidence_weight", desc=True)
                .limit(15)
                .execute()
            )
        except Exception as e:
            logger.debug(f"reference_knowledge query failed: {e}")
            return {}

        rows = result.data or []
        if not rows:
            return {}

        expert_frameworks: List[Dict[str, Any]] = []
        tactics: List[Dict[str, Any]] = []

        for row in rows:
            src = row.get("source", "")
            content = row.get("content", {})
            if not isinstance(content, dict):
                continue

            if src == "expert-panel":
                instant_fails = content.get("instant_fail_rules", [])
                expert_frameworks.append({
                    "expert": row.get("expert_handle", ""),
                    "framework": content.get("framework_name", row.get("title", "")),
                    "purpose": content.get("purpose", ""),
                    "instant_fail_rules": instant_fails[:3],
                })
            elif src in ("tactics-vault", "b2c-courses"):
                tactics.append({
                    "title": row.get("title", ""),
                    "summary": row.get("summary", ""),
                    "tag": content.get("tag", ""),
                })

        if not expert_frameworks and not tactics:
            return {}

        return {
            "expert_frameworks": expert_frameworks[:5],
            "tactics": tactics[:5],
            "total_matches": len(rows),
        }


__all__ = [
    "ExplorationConfig",
    "Guidance",
    "Predictor",
]
