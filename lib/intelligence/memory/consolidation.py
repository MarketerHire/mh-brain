"""
MH1 Memory Consolidation Manager

Orchestrates the memory lifecycle: Working → Episodic → Semantic → Procedural

This module handles the automatic promotion and decay of memories across layers:
1. Working memory (volatile) → Episodic memory (upon outcome observation)
2. Episodic memory (decaying) → Semantic patterns (upon consolidation threshold)
3. Semantic patterns (with confidence) → Procedural knowledge (cross-skill patterns)

Firebase paths:
- Episodic: system/intelligence/episodic/{tenant_id}/{skill_name}/{episode_id}
- Semantic: system/intelligence/semantic/{tenant_id}/{skill_name}/{pattern_id}
- Procedural: system/intelligence/procedural/{pattern_id}
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
from collections import defaultdict
from dataclasses import dataclass, field
from statistics import mean, mode, StatisticsError
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from .episodic import EpisodicMemoryStore
from ..types import Domain, SemanticPattern, TrajectoryPoint

# These imports will work once the stores are implemented
# Using TYPE_CHECKING to allow forward references for type hints
if TYPE_CHECKING:
    from .semantic import SemanticMemoryStore
    from .procedural import ProceduralMemoryStore, ProceduralKnowledge

logger = logging.getLogger(__name__)


# ── Domain mapping for skill categories ────────────────────────────────
# Maps skill_path category prefixes to Domain enum values.
# Unknown categories fall back to Domain.GENERIC (never raise, never guess).

SKILL_CATEGORY_TO_DOMAIN: Dict[str, Domain] = {
    "search-skills": Domain.CONTENT,
    "extraction-skills": Domain.CONTENT,
    "lifecycle-skills": Domain.HEALTH,
    "operations-skills": Domain.GENERIC,
    "growth-skills": Domain.CAMPAIGN,
    "revenue-skills": Domain.REVENUE,
}


def _resolve_domain(output: Any) -> Domain:
    """Resolve domain with safe precedence. Never raises.

    Precedence:
        1. Explicit 'domain' in enriched output (from plan node config)
        2. Skill category from skill_path prefix mapping
        3. Fallback: Domain.GENERIC
    """
    if not isinstance(output, dict):
        return Domain.GENERIC

    # 1. Explicit domain in enriched output
    explicit = output.get("domain")
    if explicit:
        try:
            return Domain(str(explicit))
        except (ValueError, KeyError):
            pass  # Invalid domain string, fall through

    # 2. Skill category mapping
    category = output.get("skill_category")
    if category and isinstance(category, str):
        mapped = SKILL_CATEGORY_TO_DOMAIN.get(category)
        if mapped is not None:
            return mapped

    # 3. Fallback
    return Domain.GENERIC


@dataclass
class ConsolidationConfig:
    """Configuration for memory consolidation behavior."""
    consolidation_batch_size: int = 20          # Max episodes to consolidate per skill per cycle
    min_episodes_for_consolidation: int = 5     # Min episodes needed before consolidation
    cross_skill_threshold: int = 3              # Skills needed for procedural knowledge
    cross_skill_min_confidence: float = 0.6     # Min avg confidence for cross-skill patterns


class MemoryConsolidationManager:
    """
    Orchestrates memory consolidation across all memory layers.
    
    The consolidation cycle:
    1. Apply temporal decay to all episodic memories
    2. Promote decayed episodes to semantic patterns
    3. Archive stale semantic patterns
    4. Promote cross-skill patterns to procedural knowledge
    
    Thread Safety:
        All operations are protected by an RLock to ensure thread-safe access
        in multi-threaded environments.
    
    Example:
        >>> consolidation = MemoryConsolidationManager(
        ...     episodic_store=episodic,
        ...     semantic_store=semantic,
        ...     procedural_store=procedural,
        ... )
        >>> stats = consolidation.run_consolidation_cycle(tenant_id="acme-corp")
        >>> print(f"Consolidated {stats['episodes_consolidated']} episodes")
    """
    
    def __init__(
        self,
        episodic_store: EpisodicMemoryStore,
        semantic_store: "SemanticMemoryStore",
        procedural_store: "ProceduralMemoryStore",
        config: Optional[ConsolidationConfig] = None
    ):
        """
        Initialize the consolidation manager.
        
        Args:
            episodic_store: Store for episodic memories
            semantic_store: Store for semantic patterns
            procedural_store: Store for procedural knowledge
            config: Configuration for consolidation thresholds
        """
        self._episodic = episodic_store
        self._semantic = semantic_store
        self._procedural = procedural_store
        self._config = config or ConsolidationConfig()
        self._lock = threading.RLock()
    
    def run_consolidation_cycle(
        self,
        tenant_id: Optional[str] = None
    ) -> Dict[str, int]:
        """
        Run a full memory consolidation cycle.
        
        This is the main orchestration method that should be called periodically
        (e.g., via a cron job or background task).
        
        Args:
            tenant_id: Optional tenant to consolidate. If None, processes all tenants.
        
        Returns:
            Statistics dict with keys:
            - episodic_decayed: Number of episodes that had decay applied
            - episodes_consolidated: Number of episodes promoted to semantic
            - patterns_created: Number of new semantic patterns created
            - patterns_updated: Number of existing patterns updated
            - patterns_archived: Number of stale patterns archived
            - procedural_created: Number of new procedural knowledge entries
        """
        with self._lock:
            stats = {
                "episodic_decayed": 0,
                "episodes_consolidated": 0,
                "patterns_created": 0,
                "patterns_updated": 0,
                "patterns_archived": 0,
                "procedural_created": 0,
            }
            
            logger.info(f"[CONSOLIDATION] === Cycle START for tenant: {tenant_id or 'all'} ===")
            
            try:
                # Step 1: Apply temporal decay to episodic memories
                logger.info("[CONSOLIDATION] Step 1: Applying temporal decay")
                decay_stats = self._episodic.decay_all(tenant_id)
                stats["episodic_decayed"] = decay_stats.get("decayed", 0)
                logger.info(
                    f"[CONSOLIDATION] Step 1 DONE: decayed={stats['episodic_decayed']}, "
                    f"to_consolidate={decay_stats.get('to_consolidate', 0)}, "
                    f"archived={decay_stats.get('archived', 0)}"
                )
                
                # Step 2: Consolidate ready episodes to semantic patterns
                logger.info("[CONSOLIDATION] Step 2: Consolidating episodes → semantic")
                consolidation_stats = self._consolidate_ready_episodes(tenant_id)
                stats["episodes_consolidated"] = consolidation_stats.get("episodes_consolidated", 0)
                stats["patterns_created"] = consolidation_stats.get("patterns_created", 0)
                stats["patterns_updated"] = consolidation_stats.get("patterns_updated", 0)
                logger.info(
                    f"[CONSOLIDATION] Step 2 DONE: consolidated={stats['episodes_consolidated']}, "
                    f"created={stats['patterns_created']}, updated={stats['patterns_updated']}"
                )
                
                # Step 2.5: Validate patterns against reference_knowledge
                logger.info("[CONSOLIDATION] Step 2.5: Validating patterns against expert rules")
                try:
                    validation_stats = self._validate_patterns_against_reference()
                    stats["patterns_flagged"] = validation_stats.get("flagged", 0)
                    stats["patterns_demoted"] = validation_stats.get("demoted", 0)
                    logger.info(f"[CONSOLIDATION] Step 2.5 DONE: {validation_stats}")
                except Exception as e:
                    logger.debug(f"[CONSOLIDATION] Step 2.5 skipped: {e}")

                # Step 3: Archive stale semantic patterns
                logger.info("[CONSOLIDATION] Step 3: Archiving stale semantic patterns")
                if hasattr(self._semantic, 'forget_stale_patterns'):
                    archived_count = self._semantic.forget_stale_patterns()
                    stats["patterns_archived"] = archived_count
                    logger.info(f"[CONSOLIDATION] Step 3 DONE: archived={stats['patterns_archived']}")
                else:
                    logger.warning("[CONSOLIDATION] Step 3 SKIP: semantic store missing forget_stale_patterns")
                
                # Step 4: Promote cross-skill patterns to procedural knowledge
                logger.info("[CONSOLIDATION] Step 4: Promoting to procedural")
                procedural_count = self._promote_to_procedural()
                stats["procedural_created"] = procedural_count
                logger.info(f"[CONSOLIDATION] Step 4 DONE: procedural_created={procedural_count}")

                # Step 5: Archive old episodes past TTL
                logger.info("[CONSOLIDATION] Step 5: Archiving old episodes past TTL")
                stats["episodes_archived"] = 0
                tenants = self._episodic._list_tenants() if hasattr(self._episodic, '_list_tenants') else []
                logger.info(f"[CONSOLIDATION] Step 5: tenants for TTL cleanup: {tenants}")
                for tid in tenants:
                    skills = self._episodic._list_skills_for_tenant(tid) if hasattr(self._episodic, '_list_skills_for_tenant') else []
                    for skill in skills:
                        try:
                            archive_stats = self._episodic.cleanup_old_episodes(tid, skill)
                            stats["episodes_archived"] += archive_stats.get("archived", 0)
                        except Exception as e:
                            logger.warning(f"[CONSOLIDATION] TTL cleanup failed for {tid}/{skill}: {e}")
                logger.info(f"[CONSOLIDATION] Step 5 DONE: episodes_archived={stats['episodes_archived']}")

                # Step 6: Decay stale procedural knowledge
                logger.info("[CONSOLIDATION] Step 6: Decaying procedural knowledge")
                if hasattr(self._procedural, 'decay_all'):
                    proc_decay = self._procedural.decay_all()
                    stats["procedural_decayed"] = proc_decay if isinstance(proc_decay, int) else proc_decay.get("decayed", 0)
                    logger.info(f"[CONSOLIDATION] Step 6 DONE: procedural_decayed={stats.get('procedural_decayed', 0)}")
                else:
                    logger.warning("[CONSOLIDATION] Step 6 SKIP: procedural store missing decay_all")

            except Exception as e:
                logger.error(f"[CONSOLIDATION] FATAL error during cycle: {e}", exc_info=True)
            
            logger.info(f"[CONSOLIDATION] === Cycle COMPLETE === stats={stats}")
            return stats
    
    def _consolidate_ready_episodes(
        self,
        tenant_id: Optional[str] = None
    ) -> Dict[str, int]:
        """
        Consolidate episodes ready for promotion to semantic patterns.
        
        Episodes are ready when their weight drops below the relevance threshold.
        
        Args:
            tenant_id: Optional tenant filter. If None, processes all tenants.
        
        Returns:
            Statistics dict with keys:
            - episodes_consolidated: Number of episodes processed
            - patterns_created: Number of new patterns created
            - patterns_updated: Number of existing patterns updated
        """
        stats = {
            "episodes_consolidated": 0,
            "patterns_created": 0,
            "patterns_updated": 0,
        }
        
        try:
            # Step 2A: Get tenants to process
            if tenant_id:
                tenants = [tenant_id]
            else:
                tenants = self._get_all_tenants()
            
            logger.info(f"[CONSOLIDATION] Step 2A: tenants found: {tenants} (count={len(tenants)})")
            
            if not tenants:
                logger.warning("[CONSOLIDATION] Step 2A: No tenants found — consolidation short-circuited")
                return stats
            
            for tid in tenants:
                # Step 2B: Get skills for this tenant
                skills = self._get_skills_for_tenant(tid)
                logger.info(f"[CONSOLIDATION] Step 2B: skills for tenant '{tid}': {skills} (count={len(skills)})")
                
                for skill_name in skills:
                    # Step 2C: Get episodes ready for consolidation
                    ready_episodes = self._episodic.get_for_consolidation(
                        tenant_id=tid,
                        skill_name=skill_name,
                        limit=self._config.consolidation_batch_size
                    )
                    
                    logger.info(
                        f"[CONSOLIDATION] Step 2C: ready episodes for {tid}/{skill_name}: "
                        f"count={len(ready_episodes)}"
                    )
                    
                    # Step 2D: Threshold check
                    if len(ready_episodes) < self._config.min_episodes_for_consolidation:
                        logger.info(
                            f"[CONSOLIDATION] Step 2D: SKIP {tid}/{skill_name}: "
                            f"{len(ready_episodes)} < min={self._config.min_episodes_for_consolidation}"
                        )
                        continue
                    
                    logger.info(
                        f"[CONSOLIDATION] Step 2D: PASS {tid}/{skill_name}: "
                        f"{len(ready_episodes)} >= {self._config.min_episodes_for_consolidation}"
                    )
                    
                    # Step 2E: Check for context-aware splitting before consolidation
                    split = self._detect_context_split(ready_episodes)
                    if split:
                        split_key = split["split_key"]
                        logger.info(
                            f"[CONSOLIDATION] Step 2E: split detected on '{split_key}' "
                            f"(explains {split['variance_explained']:.0%} of variance) "
                            f"for {tid}/{skill_name}"
                        )
                        sub_groups = self._split_episodes_by_context(ready_episodes, split_key)
                        for group_name, group_episodes in sub_groups.items():
                            if len(group_episodes) < self._config.min_episodes_for_consolidation:
                                continue
                            if hasattr(self._semantic, 'consolidate_episodes'):
                                result = self._semantic.consolidate_episodes(
                                    tenant_id=tid,
                                    skill_name=skill_name,
                                    episodes=group_episodes,
                                )
                                stats["patterns_created"] += result.get("created", 0)
                                stats["patterns_updated"] += result.get("updated", 0)
                                logger.info(
                                    f"[CONSOLIDATION] Split group '{group_name}': "
                                    f"created={result.get('created', 0)}, "
                                    f"updated={result.get('updated', 0)}"
                                )
                    else:
                        # No split detected — consolidate as single group
                        if hasattr(self._semantic, 'consolidate_episodes'):
                            logger.info(f"[CONSOLIDATION] Step 2E: calling semantic.consolidate_episodes for {tid}/{skill_name}")
                            result = self._semantic.consolidate_episodes(
                                tenant_id=tid,
                                skill_name=skill_name,
                                episodes=ready_episodes
                            )
                            logger.info(f"[CONSOLIDATION] Step 2E: result={result}")
                            stats["patterns_created"] += result.get("created", 0)
                            stats["patterns_updated"] += result.get("updated", 0)

                    # Step 2E.2: Build trajectory from multi-checkpoint episodes
                    # (optional — only applies to episodes that carry
                    # checkpoint_day metadata, e.g. skill executions that go
                    # through the 24h/7d deferred outcome loop. Platform
                    # ingestion episodes have no checkpoints, so this is a
                    # no-op for them and must not skip the mark_consolidated
                    # step below.)
                    trajectory = self._build_trajectory_from_episodes(ready_episodes)
                    if trajectory:
                        self._apply_trajectory_to_patterns(tid, skill_name, trajectory)
                        logger.info(
                            f"[CONSOLIDATION] Built trajectory with {len(trajectory)} "
                            f"points for {tid}/{skill_name}"
                        )
                    else:
                        logger.debug(
                            f"[CONSOLIDATION] No trajectory for {tid}/{skill_name} "
                            f"(episodes lack checkpoint_day metadata — this is "
                            f"expected for single-shot episodes)"
                        )

                    # Step 2F: Mark episodes as consolidated
                    for episode in ready_episodes:
                        logger.info(f"[CONSOLIDATION] Step 2F: marking episode {episode.episode_id} as consolidated")
                        self._episodic.mark_consolidated(
                            episode_id=episode.episode_id,
                            tenant_id=tid,
                            skill_name=skill_name
                        )
                        stats["episodes_consolidated"] += 1
                    
                    logger.info(
                        f"[CONSOLIDATION] Batch complete for {tid}/{skill_name}: "
                        f"consolidated={len(ready_episodes)}"
                    )
                    
        except Exception as e:
            logger.error(f"[CONSOLIDATION] ERROR in _consolidate_ready_episodes: {e}", exc_info=True)
        
        return stats
    
    def _promote_to_procedural(self) -> int:
        """
        Promote high-confidence cross-skill patterns to procedural knowledge.
        
        Finds patterns that appear across multiple skills with high confidence
        and creates procedural knowledge entries that apply universally.
        
        Returns:
            Number of procedural knowledge entries created
        """
        created_count = 0
        
        try:
            # Find cross-skill pattern groups
            pattern_groups = self._find_cross_skill_patterns()
            
            for group in pattern_groups:
                # Check if meets threshold requirements
                skills = group.get("skills", [])
                patterns = group.get("patterns", [])
                
                if len(skills) < self._config.cross_skill_threshold:
                    continue
                
                # Calculate average confidence
                if patterns:
                    avg_confidence = mean(p.confidence for p in patterns)
                else:
                    avg_confidence = 0.0
                
                if avg_confidence < self._config.cross_skill_min_confidence:
                    logger.debug(
                        f"Cross-skill pattern group below confidence threshold: "
                        f"{avg_confidence:.2f} < {self._config.cross_skill_min_confidence}"
                    )
                    continue
                
                # Generate procedural knowledge
                if hasattr(self._procedural, 'create_from_patterns'):
                    description = self._generate_description(group)
                    pattern_type = group.get("condition_key", "cross_skill")
                    
                    result = self._procedural.create_from_patterns(
                        patterns=patterns,
                        description=description,
                        pattern_type=pattern_type,
                    )
                    
                    if result:
                        created_count += 1
                        logger.info(
                            f"Created procedural knowledge from {len(patterns)} patterns "
                            f"across {len(skills)} skills"
                        )
                else:
                    logger.warning("Procedural store missing create_from_patterns method")
                    
        except Exception as e:
            logger.error(f"Error in _promote_to_procedural: {e}", exc_info=True)
        
        return created_count
    
    def _find_cross_skill_patterns(self) -> List[Dict[str, Any]]:
        """
        Find patterns that appear across multiple skills.
        
        Groups patterns by similar conditions to identify cross-skill knowledge.
        
        Returns:
            List of pattern groups, each with format:
            {
                "condition_key": str,  # Hashable key for the condition
                "skills": List[str],   # Skills that have this pattern
                "patterns": List[SemanticPattern],  # The matching patterns
                "common_recommendation": Dict  # Merged recommendations
            }
        """
        pattern_groups: Dict[str, Dict[str, Any]] = defaultdict(
            lambda: {"skills": set(), "patterns": [], "condition_key": ""}
        )
        
        try:
            # Get all high-confidence patterns across all domains
            if hasattr(self._semantic, 'get_high_confidence_patterns'):
                patterns = self._semantic.get_high_confidence_patterns(
                    min_confidence=self._config.cross_skill_min_confidence
                )
            elif hasattr(self._semantic, 'get_all_patterns'):
                all_patterns = self._semantic.get_all_patterns()
                patterns = [
                    p for p in all_patterns
                    if p.confidence >= self._config.cross_skill_min_confidence
                ]
            else:
                logger.warning("Semantic store missing pattern retrieval methods")
                return []
            
            # Group by condition key
            for pattern in patterns:
                condition_key = self._condition_key(pattern.condition)
                
                group = pattern_groups[condition_key]
                group["condition_key"] = condition_key
                group["skills"].add(pattern.skill_name)
                group["patterns"].append(pattern)
            
            # Convert to list format and compute merged recommendations
            result = []
            for key, group in pattern_groups.items():
                result.append({
                    "condition_key": key,
                    "skills": list(group["skills"]),
                    "patterns": group["patterns"],
                    "common_recommendation": self._merge_recommendations(group["patterns"])
                })
            
            return result
            
        except Exception as e:
            logger.error(f"Error in _find_cross_skill_patterns: {e}", exc_info=True)
            return []
    
    def _condition_key(self, condition: Dict[str, Any]) -> str:
        """
        Create a hashable key from a condition dictionary.
        
        This normalizes conditions so similar conditions across skills
        can be grouped together.
        
        Args:
            condition: The condition dictionary from a SemanticPattern
        
        Returns:
            A stable hash string representing the condition
        """
        if not condition:
            return "empty"
        
        try:
            # Sort keys and create stable JSON representation
            normalized = json.dumps(condition, sort_keys=True, default=str)
            # Create a short hash for grouping
            hash_value = hashlib.md5(normalized.encode()).hexdigest()[:12]
            return hash_value
        except (TypeError, ValueError) as e:
            logger.warning(f"Failed to create condition key: {e}")
            return f"unknown_{id(condition)}"
    
    def _merge_recommendations(
        self,
        patterns: List[SemanticPattern]
    ) -> Dict[str, Any]:
        """
        Merge recommendations from multiple patterns.
        
        For numeric values, uses weighted average by confidence.
        For categorical values, uses mode (most common value).
        
        Args:
            patterns: List of patterns to merge recommendations from
        
        Returns:
            Merged recommendation dictionary
        """
        if not patterns:
            return {}
        
        # Collect all recommendation keys and their values
        key_values: Dict[str, List[tuple]] = defaultdict(list)
        
        for pattern in patterns:
            if not pattern.recommendation:
                continue
            
            for key, value in pattern.recommendation.items():
                # Store value with confidence weight
                key_values[key].append((value, pattern.confidence))
        
        merged = {}
        
        for key, value_pairs in key_values.items():
            if not value_pairs:
                continue
            
            values = [v for v, _ in value_pairs]
            weights = [w for _, w in value_pairs]
            
            # Determine type and merge accordingly
            sample_value = values[0]
            
            if isinstance(sample_value, (int, float)):
                # Weighted average for numeric values
                total_weight = sum(weights)
                if total_weight > 0:
                    weighted_sum = sum(v * w for v, w in value_pairs)
                    merged[key] = weighted_sum / total_weight
                else:
                    merged[key] = mean(values)
            elif isinstance(sample_value, bool):
                # Majority vote for boolean
                true_count = sum(1 for v in values if v)
                merged[key] = true_count > len(values) / 2
            elif isinstance(sample_value, str):
                # Mode for categorical/string values
                try:
                    merged[key] = mode(values)
                except StatisticsError:
                    # No unique mode, use first value
                    merged[key] = values[0]
            elif isinstance(sample_value, list):
                # Union for lists
                merged[key] = list(set(item for v in values for item in v))
            elif isinstance(sample_value, dict):
                # Recursive merge for nested dicts
                merged[key] = self._merge_recommendation_dicts(
                    [v for v in values if isinstance(v, dict)]
                )
            else:
                # Default: use most confident value
                max_weight_idx = weights.index(max(weights))
                merged[key] = values[max_weight_idx]
        
        return merged
    
    def _merge_recommendation_dicts(
        self,
        dicts: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Merge a list of dictionaries, handling overlapping keys.
        
        Args:
            dicts: List of dictionaries to merge
        
        Returns:
            Merged dictionary
        """
        if not dicts:
            return {}
        
        merged = {}
        all_keys = set()
        for d in dicts:
            all_keys.update(d.keys())
        
        for key in all_keys:
            values = [d[key] for d in dicts if key in d]
            if not values:
                continue
            
            sample = values[0]
            if isinstance(sample, (int, float)):
                merged[key] = mean(values)
            elif isinstance(sample, str):
                try:
                    merged[key] = mode(values)
                except StatisticsError:
                    merged[key] = values[0]
            else:
                merged[key] = values[0]
        
        return merged
    
    def _generate_description(self, group: Dict[str, Any]) -> str:
        """
        Generate a human-readable description for a procedural knowledge entry.
        
        Args:
            group: Pattern group with condition_key, skills, patterns, recommendation
        
        Returns:
            Human-readable description string
        """
        skills = group.get("skills", [])
        patterns = group.get("patterns", [])
        recommendation = group.get("common_recommendation", {})
        
        # Build description parts
        skill_count = len(skills)
        pattern_count = len(patterns)
        
        # Get domains involved
        domains = set()
        for pattern in patterns:
            if pattern.domain:
                domains.add(pattern.domain.value if hasattr(pattern.domain, 'value') else str(pattern.domain))
        
        domain_str = ", ".join(sorted(domains)) if domains else "generic"
        skill_str = ", ".join(sorted(skills)[:3])
        if len(skills) > 3:
            skill_str += f", and {len(skills) - 3} more"
        
        # Extract key recommendation elements
        rec_summary = []
        for key, value in list(recommendation.items())[:3]:
            if isinstance(value, float):
                rec_summary.append(f"{key}={value:.2f}")
            else:
                rec_summary.append(f"{key}={value}")
        rec_str = "; ".join(rec_summary) if rec_summary else "various parameters"
        
        description = (
            f"Cross-skill pattern observed across {skill_count} skills "
            f"({skill_str}) in {domain_str} domain(s). "
            f"Based on {pattern_count} semantic patterns, recommends: {rec_str}."
        )
        
        return description
    
    def _build_trajectory_from_episodes(
        self,
        episodes: List[Any],
    ) -> List[TrajectoryPoint]:
        """Build a trajectory from episodes that carry checkpoint_day metadata.

        Groups episodes by checkpoint_day and computes the average
        observed/baseline ratio for each checkpoint, producing a list of
        TrajectoryPoints sorted by checkpoint day.
        """
        from collections import defaultdict

        day_ratios: Dict[int, List[float]] = defaultdict(list)

        for ep in episodes:
            meta = {}
            if hasattr(ep, "outcome") and hasattr(ep.outcome, "metadata"):
                meta = ep.outcome.metadata or {}
            checkpoint_day = meta.get("checkpoint_day")
            if checkpoint_day is None:
                continue
            try:
                checkpoint_day = int(checkpoint_day)
            except (TypeError, ValueError):
                continue

            baseline = ep.outcome.observed_baseline if ep.outcome.observed_baseline > 0 else 1.0
            ratio = ep.outcome.observed_signal / baseline
            day_ratios[checkpoint_day].append(ratio)

        if not day_ratios:
            return []

        trajectory = []
        for day in sorted(day_ratios):
            ratios = day_ratios[day]
            avg_ratio = sum(ratios) / len(ratios)
            trajectory.append(TrajectoryPoint(
                checkpoint_days=day,
                expected_ratio=avg_ratio,
                observation_count=len(ratios),
                confidence=min(1.0, len(ratios) / 10),
            ))

        return trajectory

    def _apply_trajectory_to_patterns(
        self,
        tenant_id: str,
        skill_name: str,
        trajectory: List[TrajectoryPoint],
    ) -> None:
        """Apply a built trajectory to existing semantic patterns for a skill."""
        try:
            for domain in Domain:
                patterns = self._semantic.retrieve_patterns(
                    skill_name=skill_name,
                    domain=domain,
                    limit=20,
                )
                for pattern in patterns:
                    if not pattern.expected_trajectory:
                        pattern.expected_trajectory = trajectory
                    else:
                        for new_tp in trajectory:
                            matched = False
                            for existing_tp in pattern.expected_trajectory:
                                if existing_tp.checkpoint_days == new_tp.checkpoint_days:
                                    existing_tp.expected_ratio = (
                                        0.8 * existing_tp.expected_ratio
                                        + 0.2 * new_tp.expected_ratio
                                    )
                                    existing_tp.observation_count += new_tp.observation_count
                                    existing_tp.confidence = min(
                                        1.0, existing_tp.observation_count / 10
                                    )
                                    matched = True
                                    break
                            if not matched:
                                pattern.expected_trajectory.append(new_tp)
                        pattern.expected_trajectory.sort(key=lambda t: t.checkpoint_days)

                    if hasattr(pattern, "expected_time_to_target_days"):
                        if pattern.expected_trajectory:
                            pattern.expected_time_to_target_days = float(
                                pattern.expected_trajectory[-1].checkpoint_days
                            )

                    if hasattr(self._semantic, "_persist_pattern"):
                        self._semantic._persist_pattern(pattern, tenant_id)

        except Exception as e:
            logger.debug(f"Failed to apply trajectory to patterns: {e}")

    def _detect_context_split(
        self,
        episodes: List[Any],
        min_variance_explained: float = 0.30,
        min_group_size: int = 3,
    ) -> Optional[Dict[str, Any]]:
        """Detect whether splitting episodes by a context key reduces outcome variance.

        Tests each context key as a potential split point. A split is valid when:
        - A single key explains > ``min_variance_explained`` (30%) of outcome variance
        - Both groups have at least ``min_group_size`` episodes

        Returns the best split or None if no significant split found.
        """
        if len(episodes) < min_group_size * 2:
            return None

        # Collect outcome ratios
        ratios = []
        for ep in episodes:
            baseline = ep.outcome.observed_baseline if ep.outcome.observed_baseline > 0 else 1.0
            ratios.append(ep.outcome.observed_signal / baseline)

        if not ratios:
            return None

        overall_mean = sum(ratios) / len(ratios)
        total_variance = sum((r - overall_mean) ** 2 for r in ratios)
        if total_variance == 0:
            return None

        # Collect all context keys
        context_keys: set = set()
        for ep in episodes:
            ctx = ep.prediction.context or {}
            for k in ctx:
                if not k.startswith("_"):
                    context_keys.add(k)

        best_split = None
        best_variance_explained = 0.0

        for key in context_keys:
            # Group episodes by this key's value
            groups: Dict[Any, List[float]] = defaultdict(list)
            for ep, ratio in zip(episodes, ratios):
                val = (ep.prediction.context or {}).get(key, "__missing__")
                # For numeric values, bucket into "high" and "low"
                if isinstance(val, (int, float)):
                    vals = [
                        (ep2.prediction.context or {}).get(key)
                        for ep2 in episodes
                        if isinstance((ep2.prediction.context or {}).get(key), (int, float))
                    ]
                    if vals:
                        median_val = sorted(vals)[len(vals) // 2]
                        val = "high" if val >= median_val else "low"
                groups[str(val)].append(ratio)

            # Skip keys that don't produce two valid groups
            valid_groups = {k: v for k, v in groups.items() if len(v) >= min_group_size}
            if len(valid_groups) < 2:
                continue

            # Compute within-group variance (SSW)
            ssw = 0.0
            for group_ratios in valid_groups.values():
                group_mean = sum(group_ratios) / len(group_ratios)
                ssw += sum((r - group_mean) ** 2 for r in group_ratios)

            variance_explained = 1.0 - (ssw / total_variance) if total_variance > 0 else 0.0

            if variance_explained > best_variance_explained and variance_explained >= min_variance_explained:
                best_variance_explained = variance_explained
                group_stats = {}
                for gname, gratios in valid_groups.items():
                    gmean = sum(gratios) / len(gratios)
                    group_stats[gname] = {
                        "count": len(gratios),
                        "mean_ratio": gmean,
                    }
                best_split = {
                    "split_key": key,
                    "variance_explained": variance_explained,
                    "groups": group_stats,
                }

        return best_split

    def _split_episodes_by_context(
        self,
        episodes: List[Any],
        split_key: str,
    ) -> Dict[str, List[Any]]:
        """Split episodes into groups by the value of a context key."""
        groups: Dict[str, List[Any]] = defaultdict(list)

        # Detect if values are numeric (need median bucketing)
        values = [
            (ep.prediction.context or {}).get(split_key)
            for ep in episodes
        ]
        numeric_values = [v for v in values if isinstance(v, (int, float))]

        if len(numeric_values) > len(values) / 2:
            # Numeric key: split at median
            sorted_nums = sorted(numeric_values)
            median_val = sorted_nums[len(sorted_nums) // 2]
            for ep in episodes:
                val = (ep.prediction.context or {}).get(split_key)
                if isinstance(val, (int, float)):
                    group_name = "high" if val >= median_val else "low"
                else:
                    group_name = "__other__"
                groups[group_name].append(ep)
        else:
            # Categorical key
            for ep in episodes:
                val = (ep.prediction.context or {}).get(split_key, "__missing__")
                groups[str(val)].append(ep)

        return dict(groups)

    def _get_all_tenants(self) -> List[str]:
        """
        Get all tenant IDs from Firebase.
        
        Returns:
            List of tenant IDs
        """
        return self._episodic._list_tenants()
    
    def _get_skills_for_tenant(self, tenant_id: str) -> List[str]:
        """
        Get all skill names for a tenant.
        
        Args:
            tenant_id: The tenant ID
        
        Returns:
            List of skill names
        """
        return self._episodic._list_skills_for_tenant(tenant_id)

    def consolidate_from_module(
        self,
        module_id: str,
        client_id: str,
        execution_data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Extract learnings from a completed module and store in memory.

        This method is called when a module completes execution. It extracts
        patterns from the module's execution:
        - Successful skill sequences -> procedural memory
        - Client preferences -> semantic memory
        - Error patterns -> episodic memory

        Args:
            module_id: The completed module's ID
            client_id: The client/tenant ID
            execution_data: Optional execution state data containing:
                - skill_plan: List of skills executed
                - checkpoints: Execution checkpoints with status/outputs
                - outputs: Module outputs
                - error: Any error that occurred

        Returns:
            Dict with consolidation statistics:
            - episodes_stored: Number of episodic memories stored
            - patterns_updated: Number of semantic patterns updated
            - procedural_created: Number of procedural entries created
            - promotion_candidates: Number of patterns promoted
        """
        with self._lock:
            stats = {
                "episodes_stored": 0,
                "patterns_updated": 0,
                "procedural_created": 0,
                "promotion_candidates": 0,
            }

            if not execution_data:
                logger.debug(f"No execution data for module {module_id}")
                return stats

            try:
                logger.info(f"Consolidating memory from module {module_id} for client {client_id}")

                # Extract skill execution results from checkpoints
                checkpoints = execution_data.get("checkpoints", [])
                skill_plan = execution_data.get("skill_plan", [])
                module_outputs = execution_data.get("outputs", {})
                module_error = execution_data.get("error")

                # 1. Store execution summary in episodic memory
                episode = self._create_module_episode(
                    module_id=module_id,
                    client_id=client_id,
                    skill_plan=skill_plan,
                    checkpoints=checkpoints,
                    outputs=module_outputs,
                    error=module_error
                )

                if episode:
                    try:
                        # Note: store() extracts tenant_id/skill_name from episode.prediction
                        self._episodic.store(episode)
                        stats["episodes_stored"] += 1
                        logger.debug(f"Stored module episode {episode.episode_id}")
                    except Exception as e:
                        logger.warning(f"Failed to store module episode: {e}")

                # 2. Store individual skill results in episodic memory
                for checkpoint in checkpoints:
                    skill_episode = self._create_skill_episode(
                        module_id=module_id,
                        client_id=client_id,
                        checkpoint=checkpoint
                    )

                    if skill_episode:
                        try:
                            # Note: store() extracts tenant_id/skill_name from episode.prediction
                            self._episodic.store(skill_episode)
                            stats["episodes_stored"] += 1
                        except Exception as e:
                            logger.debug(f"Failed to store skill episode: {e}")

                # 3. Update semantic patterns from successful executions
                successful_skills = [
                    cp for cp in checkpoints
                    if cp.get("status") == "success"
                ]

                if successful_skills:
                    patterns_updated = self._update_patterns_from_execution(
                        client_id=client_id,
                        successful_skills=successful_skills,
                        module_outputs=module_outputs
                    )
                    stats["patterns_updated"] = patterns_updated

                # 4. Check for pattern promotion (episodic -> semantic when confidence > 0.7)
                promotion_result = self._check_for_promotions(client_id=client_id)
                stats["promotion_candidates"] = promotion_result.get("promoted", 0)

                # 5. If skill sequence was successful, consider for procedural memory
                if not module_error and len(skill_plan) >= 2:
                    procedural_count = self._evaluate_skill_sequence(
                        client_id=client_id,
                        skill_plan=skill_plan,
                        checkpoints=checkpoints
                    )
                    stats["procedural_created"] = procedural_count

                logger.info(f"Module {module_id} consolidation complete: {stats}")
                return stats

            except Exception as e:
                logger.error(f"Error consolidating from module {module_id}: {e}", exc_info=True)
                return stats

    def _create_module_episode(
        self,
        module_id: str,
        client_id: str,
        skill_plan: List[str],
        checkpoints: List[Dict[str, Any]],
        outputs: Dict[str, Any],
        error: Optional[str]
    ) -> Optional['EpisodicMemory']:
        """
        Create an episodic memory from module execution.

        Args:
            module_id: Module ID
            client_id: Client/tenant ID
            skill_plan: List of skills that were planned
            checkpoints: Execution checkpoints
            outputs: Module outputs
            error: Error message if failed

        Returns:
            EpisodicMemory or None
        """
        from ..types import EpisodicMemory, Prediction, Outcome, Domain
        import uuid

        try:
            # Calculate success metrics
            total_skills = len(skill_plan)
            successful_skills = sum(1 for cp in checkpoints if cp.get("status") == "success")
            success_rate = successful_skills / total_skills if total_skills > 0 else 0.0

            # Create prediction representing the module execution expectation
            prediction = Prediction(
                prediction_id=str(uuid.uuid4())[:12],
                skill_name="module_execution",
                tenant_id=client_id,
                domain=Domain.GENERIC,
                expected_signal=1.0,  # Expected all skills to succeed
                expected_baseline=1.0,
                confidence=0.5,
                context={
                    "module_id": module_id,
                    "skill_count": total_skills,
                    "skill_plan": skill_plan[:5],  # First 5 for context
                }
            )

            # Create outcome representing actual results
            outcome = Outcome(
                prediction_id=prediction.prediction_id,
                observed_signal=success_rate,
                observed_baseline=1.0,
                prediction_error=abs(1.0 - success_rate),
                goal_completed=(error is None and success_rate >= 0.8),
                business_impact=0.0,
                metadata={
                    "total_skills": total_skills,
                    "successful_skills": successful_skills,
                    "error": error,
                    "output_keys": list(outputs.keys()) if outputs else [],
                }
            )

            # Create episodic memory
            episode = EpisodicMemory(
                episode_id=f"mod_{module_id}_{str(uuid.uuid4())[:6]}",
                prediction=prediction,
                outcome=outcome,
                weight=1.0,  # Fresh memory, full weight
            )

            return episode

        except Exception as e:
            logger.error(f"Error creating module episode: {e}")
            return None

    def _create_skill_episode(
        self,
        module_id: str,
        client_id: str,
        checkpoint: Dict[str, Any]
    ) -> Optional['EpisodicMemory']:
        """
        Create an episodic memory from a skill checkpoint.

        Args:
            module_id: Module ID
            client_id: Client/tenant ID
            checkpoint: Skill checkpoint data

        Returns:
            EpisodicMemory or None
        """
        from ..types import EpisodicMemory, Prediction, Outcome, Domain
        import uuid

        try:
            skill_name = checkpoint.get("skill_name", "unknown")
            status = checkpoint.get("status", "unknown")
            output = checkpoint.get("output", {})
            error = checkpoint.get("error")

            # ── B3: Domain mapping with precedence ─────────────────────
            domain = _resolve_domain(output)

            # ── B2: Meaningful context (fixed key set, bounded) ────────
            context = {
                "module_id": module_id,
                "skill_index": checkpoint.get("skill_index", 0),
            }
            if isinstance(output, dict):
                # Stable identifiers only — no raw outputs, no text
                for ctx_key in ("skill_id", "client_id", "attempt_count",
                                "tool_policy_mode", "evidence_mode"):
                    val = output.get(ctx_key)
                    if val is not None:
                        context[ctx_key] = val
                if output.get("schema_present"):
                    context["schema_present"] = True
            # Also inject skill_name and client_id from params if not in output
            if "skill_id" not in context and skill_name != "unknown":
                context["skill_id"] = skill_name
            if "client_id" not in context and client_id:
                context["client_id"] = client_id
            # Remove None values for clean context intersection
            context = {k: v for k, v in context.items() if v is not None}

            # Create prediction
            prediction = Prediction(
                prediction_id=str(uuid.uuid4())[:12],
                skill_name=skill_name,
                tenant_id=client_id,
                domain=domain,
                expected_signal=1.0,
                expected_baseline=1.0,
                confidence=0.5,
                context=context,
            )

            # ── B1: Continuous validation signal (normalize + clamp) ───
            success = status == "success"
            raw_score = output.get("validation_score") if isinstance(output, dict) else None
            if raw_score is not None:
                try:
                    observed_signal = max(0.0, min(1.0, float(raw_score) / 100.0))
                except (ValueError, TypeError):
                    observed_signal = 1.0 if success else 0.0
            else:
                # Fallback to binary signal (baseline behavior)
                observed_signal = 1.0 if success else 0.0

            # ── B4: Enriched outcome.metadata (additive only) ──────────
            metadata = {
                "status": status,
                "error": error,
                "output_type": type(output).__name__ if output else None,
            }
            if isinstance(output, dict):
                for meta_key in ("validation_score", "evidence_count",
                                 "policy_violations", "tool_policy_mode",
                                 "skill_category"):
                    val = output.get(meta_key)
                    if val is not None:
                        metadata[meta_key] = val

            # Create outcome
            outcome = Outcome(
                prediction_id=prediction.prediction_id,
                observed_signal=observed_signal,
                observed_baseline=1.0,
                prediction_error=abs(1.0 - observed_signal),
                goal_completed=success,
                business_impact=0.0,
                metadata=metadata,
            )

            # Create episode
            episode = EpisodicMemory(
                episode_id=f"skill_{skill_name}_{str(uuid.uuid4())[:6]}",
                prediction=prediction,
                outcome=outcome,
                weight=1.0,
            )

            return episode

        except Exception as e:
            logger.error(f"Error creating skill episode: {e}")
            return None

    def _update_patterns_from_execution(
        self,
        client_id: str,
        successful_skills: List[Dict[str, Any]],
        module_outputs: Dict[str, Any]
    ) -> int:
        """
        Update semantic patterns from successful skill executions.

        Args:
            client_id: Client/tenant ID
            successful_skills: List of successful skill checkpoints
            module_outputs: Module outputs

        Returns:
            Number of patterns updated
        """
        updated_count = 0

        for skill_data in successful_skills:
            skill_name = skill_data.get("skill_name", "")
            if not skill_name:
                continue

            try:
                if not hasattr(self._semantic, 'update_from_outcome'):
                    continue

                # Look up existing patterns by skill name across all domains
                patterns = []
                for domain in Domain:
                    try:
                        found = self._semantic.retrieve_patterns(
                            skill_name=skill_name,
                            domain=domain,
                            limit=5,
                        )
                        patterns.extend(found)
                    except Exception:
                        pass

                if not patterns:
                    # No patterns yet for this skill -- consolidation will
                    # create them from episodic memories; skip for now
                    logger.debug(f"No existing patterns for {skill_name}, skipping update")
                    continue

                # Update the best-matching pattern (highest confidence)
                best = max(patterns, key=lambda p: p.confidence * p.recent_accuracy)
                self._semantic.update_from_outcome(
                    pattern_id=best.pattern_id,
                    domain=best.domain,
                    success=True,
                    observed_ratio=1.0,
                )
                updated_count += 1
            except Exception as e:
                logger.debug(f"Could not update pattern for {skill_name}: {e}")

        return updated_count

    def _check_for_promotions(self, client_id: str) -> Dict[str, int]:
        """
        Check for patterns that should be promoted (confidence > 0.7).

        Episodic memories with high confidence/consistency should be
        promoted to semantic patterns.

        Args:
            client_id: Client/tenant ID

        Returns:
            Dict with promotion statistics
        """
        stats = {"promoted": 0, "candidates": 0}

        try:
            # Get recent episodic memories for this client
            if hasattr(self._episodic, 'retrieve'):
                # Get skills that might have consolidation candidates
                skills = self._get_skills_for_tenant(client_id)

                for skill_name in skills[:10]:  # Limit to avoid too many queries
                    episodes = self._episodic.retrieve(
                        tenant_id=client_id,
                        skill_name=skill_name,
                        min_weight=0.3,  # Not too old
                        limit=20
                    )

                    if len(episodes) >= self._config.min_episodes_for_consolidation:
                        stats["candidates"] += 1

                        # Calculate average success rate
                        successes = sum(1 for e in episodes if e.outcome.goal_completed)
                        success_rate = successes / len(episodes)

                        # Promote if high confidence
                        if success_rate >= 0.7:
                            if hasattr(self._semantic, 'consolidate_from_episodes'):
                                result = self._semantic.consolidate_from_episodes(episodes)
                                if result:
                                    stats["promoted"] += 1
                                    logger.info(
                                        f"Promoted pattern for {skill_name} with "
                                        f"{success_rate:.0%} success rate"
                                    )

        except Exception as e:
            logger.debug(f"Error checking for promotions: {e}")

        return stats

    def _evaluate_skill_sequence(
        self,
        client_id: str,
        skill_plan: List[str],
        checkpoints: List[Dict[str, Any]]
    ) -> int:
        """
        Evaluate a skill sequence for procedural knowledge creation.

        Args:
            client_id: Client/tenant ID
            skill_plan: List of skills executed
            checkpoints: Execution checkpoints

        Returns:
            Number of procedural entries created
        """
        created_count = 0

        try:
            # Only consider if all skills succeeded
            all_success = all(
                cp.get("status") == "success"
                for cp in checkpoints
            )

            if not all_success or len(skill_plan) < 2:
                return 0

            # Check if we have enough evidence across multiple executions
            # (this would require tracking sequences over time)
            # For now, just log that this sequence succeeded
            logger.debug(
                f"Successful skill sequence for {client_id}: {' -> '.join(skill_plan)}"
            )

            # In a full implementation, we would:
            # 1. Track this sequence in a sequence registry
            # 2. Count how many times it succeeded across executions
            # 3. Create procedural knowledge when threshold met

        except Exception as e:
            logger.debug(f"Error evaluating skill sequence: {e}")

        return created_count


    # ── Reference knowledge validation ───────────────────────────────

    def _validate_patterns_against_reference(self) -> Dict[str, int]:
        """Check recently updated patterns against expert instant_fail rules.

        Queries reference_knowledge for expert-panel rows with instant_fail_rules,
        then checks each recently updated high-confidence pattern's recommendation
        against those rules.  Patterns that trigger an instant_fail get their
        confidence reduced and a flag added to their metadata.

        Returns dict with flagged/demoted counts.
        """
        stats = {"flagged": 0, "demoted": 0}

        try:
            from lib.supabase_client import get_supabase_or_none
        except ImportError:
            return stats

        db = get_supabase_or_none()
        if db is None:
            return stats

        try:
            ref_result = (
                db.table("reference_knowledge")
                .select("title,content,tags,expert_handle")
                .eq("source", "expert-panel")
                .execute()
            )
        except Exception:
            return stats

        expert_rows = ref_result.data or []
        if not expert_rows:
            return stats

        all_rules: List[Dict[str, Any]] = []
        for row in expert_rows:
            content = row.get("content", {})
            if not isinstance(content, dict):
                continue
            rules = content.get("instant_fail_rules", [])
            tags = row.get("tags", [])
            for rule in rules:
                if isinstance(rule, dict) and rule.get("rule"):
                    all_rules.append({
                        "expert": row.get("expert_handle", ""),
                        "component": rule.get("component", ""),
                        "rule": rule["rule"],
                        "tags": tags,
                    })

        if not all_rules:
            return stats

        try:
            if hasattr(self._semantic, "get_high_confidence_patterns"):
                patterns = self._semantic.get_high_confidence_patterns(min_confidence=0.6)
            elif hasattr(self._semantic, "get_all_patterns"):
                all_pats = self._semantic.get_all_patterns()
                patterns = [p for p in all_pats if p.confidence >= 0.6]
            else:
                return stats
        except Exception:
            return stats

        for pattern in patterns:
            pat_tags = set()
            for k, v in (pattern.condition or {}).items():
                if isinstance(v, str):
                    pat_tags.add(v.lower())
            if hasattr(pattern, "skill_name") and pattern.skill_name:
                pat_tags.update(pattern.skill_name.lower().replace(":", " ").split())

            for rule in all_rules:
                rule_tags = set(t.lower() for t in (rule.get("tags") or []))
                if not rule_tags or rule_tags & pat_tags:
                    rule_text = rule["rule"].lower()
                    rec = pattern.recommendation or {}
                    rec_text = json.dumps(rec, default=str).lower()

                    triggered = False
                    if "wrong desire" in rule_text and "desire" not in rec_text:
                        pass
                    elif "leading with product" in rule_text and rec.get("funnel_stage") == "unaware":
                        triggered = True
                    elif "product mention in the first 25%" in rule_text:
                        pass

                    if triggered:
                        stats["flagged"] += 1
                        original_conf = pattern.confidence
                        pattern.confidence = max(0.1, pattern.confidence * 0.5)
                        if not hasattr(pattern, "metadata") or pattern.metadata is None:
                            pattern.metadata = {}
                        pattern.metadata["instant_fail_triggered"] = {
                            "expert": rule["expert"],
                            "component": rule["component"],
                            "rule": rule["rule"][:120],
                            "original_confidence": original_conf,
                        }
                        stats["demoted"] += 1

                        if hasattr(self._semantic, "_persist_pattern"):
                            try:
                                self._semantic._persist_pattern(pattern, "")
                            except Exception:
                                pass
                        break

        return stats


__all__ = [
    "ConsolidationConfig",
    "MemoryConsolidationManager",
]
