"""
MH1 Semantic Memory Store

Firebase-persisted semantic memory with Bayesian pattern learning.
Stores learned patterns that generalize from episodic experiences.
Patterns have confidence scores updated via Bayesian inference.

Firebase path: system/intelligence/semantic/{domain}/patterns/{pattern_id}
Archive path: system/intelligence/semantic/{domain}/archive/{pattern_id}

Also provides semantic similarity search via:
- Token-based Jaccard similarity (fast, no external dependencies)
- Optional LLM-based similarity (more accurate, requires anthropic package)
"""

import hashlib
import json
import logging
import os
import re
import threading
import uuid
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from ..types import Domain, EpisodicMemory, SemanticPattern

logger = logging.getLogger(__name__)

# Common English stopwords for tokenization
STOPWORDS: Set[str] = {
    'the', 'a', 'an', 'is', 'are', 'was', 'were', 'be', 'been', 'being',
    'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'could',
    'should', 'may', 'might', 'must', 'shall', 'can', 'need', 'dare',
    'to', 'of', 'in', 'for', 'on', 'with', 'at', 'by', 'from', 'as',
    'into', 'through', 'during', 'before', 'after', 'above', 'below',
    'between', 'under', 'again', 'further', 'then', 'once', 'and', 'or',
    'but', 'if', 'than', 'so', 'because', 'while', 'although', 'though',
    'this', 'that', 'these', 'those', 'it', 'its', 'itself', 'they',
    'them', 'their', 'theirs', 'themselves', 'what', 'which', 'who',
    'whom', 'whose', 'where', 'when', 'why', 'how', 'all', 'each',
    'every', 'both', 'few', 'more', 'most', 'other', 'some', 'such',
    'no', 'nor', 'not', 'only', 'own', 'same', 'just', 'also', 'very',
}


@dataclass
class SemanticMemoryConfig:
    """Configuration for semantic memory Bayesian learning."""
    initial_confidence: float = 0.5         # Starting confidence for new patterns
    learning_rate: float = 0.1              # EMA learning rate for expected_value
    decay_rate: float = 0.99                # Per-day decay without reinforcement
    min_confidence: float = 0.01            # Minimum confidence floor
    max_confidence: float = 0.99            # Maximum confidence ceiling
    forget_threshold: float = 0.1           # Below this, archive pattern
    min_evidence_for_trust: int = 5         # Minimum evidence before forgetting


@dataclass
class SemanticSimilarityConfig:
    """Configuration for semantic similarity search."""
    min_token_length: int = 2               # Minimum token length to include
    use_stemming: bool = False              # Whether to apply basic stemming
    ngram_range: Tuple[int, int] = (1, 2)   # N-gram range for tokenization
    min_similarity: float = 0.1             # Minimum similarity threshold
    max_results: int = 10                   # Maximum results to return
    use_tf_idf: bool = True                 # Whether to use TF-IDF weighting


class SemanticMemoryStore:
    """
    Thread-safe Firebase-backed semantic memory store with Bayesian learning.
    
    Manages storage, retrieval, and Bayesian updating of semantic patterns.
    Patterns are learned generalizations from episodic memories.
    """
    
    _collection_base = "system/intelligence/semantic"
    
    def __init__(
        self,
        firebase_client: Any,
        config: Optional[SemanticMemoryConfig] = None
    ):
        """
        Initialize the semantic memory store.
        
        Args:
            firebase_client: Firebase client with set_document, get_document,
                           query, update_document, delete_document, get_collection methods
            config: Configuration for Bayesian learning parameters
        """
        self._firebase = firebase_client
        self._config = config or SemanticMemoryConfig()
        self._lock = threading.RLock()
    
    def _get_collection_path(self, domain: Domain) -> str:
        """
        Get the Firebase collection path for a domain's patterns.
        
        Args:
            domain: The business domain
            
        Returns:
            Collection path string
        """
        return f"{self._collection_base}/{domain.value}/patterns"
    
    def _get_archive_path(self, domain: Domain) -> str:
        """
        Get the Firebase archive path for archived patterns.
        
        Args:
            domain: The business domain
            
        Returns:
            Archive collection path string
        """
        return f"{self._collection_base}/{domain.value}/archive"
    
    def _calculate_days_since(self, iso_timestamp: Optional[str]) -> float:
        """Calculate days since an ISO timestamp."""
        if not iso_timestamp:
            return 0.0
        try:
            dt = datetime.fromisoformat(iso_timestamp.replace("Z", "+00:00"))
            now = datetime.now(timezone.utc)
            delta = now - dt
            return max(0.0, delta.total_seconds() / 86400)
        except (ValueError, AttributeError):
            return 0.0
    
    def store(self, pattern: SemanticPattern) -> str:
        """
        Store a pattern to Firebase.
        
        Args:
            pattern: The SemanticPattern to store
            
        Returns:
            The pattern_id of the stored pattern
        """
        with self._lock:
            if not pattern.skill_name:
                raise ValueError("Pattern must have skill_name")
            
            collection_path = self._get_collection_path(pattern.domain)
            
            # Build document data with extended fields
            doc_data = pattern.to_dict()
            
            # Add extended fields not in base SemanticPattern
            # These are stored in Firebase but not in the dataclass
            if "tenant_ids" not in doc_data:
                doc_data["tenant_ids"] = []  # Empty means all tenants
            if "last_reinforced_at" not in doc_data:
                doc_data["last_reinforced_at"] = pattern.updated_at
            
            # Store to Firebase
            if hasattr(self._firebase, "set_document"):
                self._firebase.set_document(
                    collection=collection_path,
                    doc_id=pattern.pattern_id,
                    data=doc_data
                )
            else:
                logger.warning("Firebase client missing set_document method")
                raise AttributeError("firebase_client missing set_document method")
            
            logger.debug(f"Stored pattern {pattern.pattern_id} at {collection_path}")
            return pattern.pattern_id

    def _persist_pattern(
        self,
        pattern: SemanticPattern,
        tenant_id: str = "",
    ) -> str:
        """
        Persist an updated pattern, tracking tenant_ids across calls.

        Used by the consolidation manager after mutating a pattern in
        place (trajectory updates, instant-fail demotions). Unlike
        ``store()``, this method:

          1. Refreshes ``pattern.updated_at`` so decay math is correct.
          2. Reads the existing doc (if any) to preserve and union the
             ``tenant_ids`` list — the pattern dataclass itself does not
             carry per-tenant scope, but the Firebase/Supabase doc does.
          3. Writes via ``set_document`` with the merged ``tenant_ids``.

        Empty ``tenant_id`` (used by global/cross-tenant demotions)
        is treated as "applies to all tenants" and does not add to the
        tenant_ids list.

        Args:
            pattern: The SemanticPattern to persist. Mutated in place.
            tenant_id: Optional tenant scope to associate with this
                update. Empty string means cross-tenant / global.

        Returns:
            The pattern_id (same as ``pattern.pattern_id``).
        """
        with self._lock:
            if not pattern.skill_name:
                raise ValueError("Pattern must have skill_name")

            pattern.updated_at = datetime.now(timezone.utc).isoformat()

            collection_path = self._get_collection_path(pattern.domain)
            doc_data = pattern.to_dict()

            existing_tenant_ids: List[str] = []
            if hasattr(self._firebase, "get_document"):
                try:
                    existing = self._firebase.get_document(
                        collection=collection_path,
                        doc_id=pattern.pattern_id,
                    )
                    if existing and isinstance(existing.get("tenant_ids"), list):
                        existing_tenant_ids = [
                            t for t in existing["tenant_ids"] if isinstance(t, str)
                        ]
                except Exception as e:
                    logger.debug(
                        f"_persist_pattern: get_document failed for "
                        f"{pattern.pattern_id}: {e}"
                    )

            tenant_ids = list(existing_tenant_ids)
            if tenant_id and tenant_id not in tenant_ids:
                tenant_ids.append(tenant_id)
            doc_data["tenant_ids"] = tenant_ids

            doc_data.setdefault("last_reinforced_at", pattern.updated_at)

            if hasattr(self._firebase, "set_document"):
                self._firebase.set_document(
                    collection=collection_path,
                    doc_id=pattern.pattern_id,
                    data=doc_data,
                )
            else:
                logger.warning(
                    "_persist_pattern: firebase client missing set_document"
                )
                raise AttributeError("firebase_client missing set_document method")

            logger.debug(
                f"Persisted pattern {pattern.pattern_id} "
                f"(tenant_ids={len(tenant_ids)}) at {collection_path}"
            )
            return pattern.pattern_id

    def retrieve_patterns(
        self,
        skill_name: str,
        domain: Domain,
        tenant_id: Optional[str] = None,
        min_confidence: Optional[float] = None,
        limit: int = 20
    ) -> List[SemanticPattern]:
        """
        Retrieve patterns from Firebase with filtering and decay application.
        
        Args:
            skill_name: Name of the skill to filter by
            domain: Business domain to query
            tenant_id: Optional tenant filter (None means pattern must apply to all)
            min_confidence: Optional minimum confidence filter (after decay)
            limit: Maximum number of patterns to return
            
        Returns:
            List of SemanticPattern objects with decayed confidence
        """
        with self._lock:
            collection_path = self._get_collection_path(domain)
            
            # Build filters
            filters = [
                ("skill_name", "==", skill_name)
            ]
            
            # Query Firebase
            try:
                if hasattr(self._firebase, "query"):
                    docs = self._firebase.query(
                        collection=collection_path,
                        filters=filters,
                        limit=limit * 3,  # Over-fetch for filtering
                        order_by="confidence",
                        order_direction="DESCENDING"
                    )
                elif hasattr(self._firebase, "get_collection"):
                    docs = self._firebase.get_collection(
                        collection=collection_path,
                        limit=limit * 3,
                        order_by="confidence",
                        order_direction="DESCENDING"
                    )
                else:
                    logger.warning("Firebase client missing query/get_collection methods")
                    return []
            except Exception as e:
                logger.error(f"Error querying patterns: {e}")
                return []
            
            if not docs:
                return []
            
            # Convert and filter patterns
            patterns = []
            
            for doc in docs:
                pattern = self._doc_to_pattern(doc)
                if pattern is None:
                    continue
                
                # Filter by skill_name (in case query didn't work)
                if pattern.skill_name != skill_name:
                    continue
                
                # Filter by tenant_id
                # Pattern applies if tenant_ids is empty (global) or contains the tenant
                pattern_tenant_ids = doc.get("tenant_ids", [])
                if tenant_id is not None and pattern_tenant_ids:
                    if tenant_id not in pattern_tenant_ids:
                        continue
                
                # Apply decay based on days since last reinforcement
                last_reinforced = doc.get("last_reinforced_at", pattern.updated_at)
                days_since = self._calculate_days_since(last_reinforced)
                
                if days_since > 0:
                    pattern.confidence *= (self._config.decay_rate ** days_since)
                    pattern.confidence = max(
                        self._config.min_confidence,
                        min(self._config.max_confidence, pattern.confidence)
                    )
                
                # Filter by min_confidence after decay
                if min_confidence is not None and pattern.confidence < min_confidence:
                    continue
                
                patterns.append(pattern)
                
                if len(patterns) >= limit:
                    break
            
            return patterns
    
    def consolidate_from_episodes(
        self,
        episodes: List[EpisodicMemory]
    ) -> Optional[SemanticPattern]:
        """
        Extract a semantic pattern from a list of episodic memories.
        
        Uses Bayesian updating to calculate pattern confidence.
        
        Args:
            episodes: List of episodic memories to consolidate
            
        Returns:
            SemanticPattern if consolidation successful, None otherwise
        """
        with self._lock:
            if not episodes:
                return None
            
            # All episodes should have same skill and domain
            first = episodes[0]
            skill_name = first.prediction.skill_name
            domain = first.prediction.domain
            
            # Extract common context from all episodes
            common_context = self._extract_common_context(episodes)
            
            # Extract recommendation from successful episodes
            recommendation = self._extract_recommendation(episodes)
            
            # Calculate success/failure counts
            successes = sum(1 for e in episodes if e.outcome.goal_completed)
            failures = len(episodes) - successes
            
            # Calculate expected_value as average observed_signal/observed_baseline ratios
            ratios = []
            for e in episodes:
                if e.outcome.observed_baseline > 0:
                    ratio = e.outcome.observed_signal / e.outcome.observed_baseline
                    ratios.append(ratio)
            
            expected_value = sum(ratios) / len(ratios) if ratios else 1.0
            
            # Calculate variance
            if len(ratios) > 1:
                mean = expected_value
                variance = sum((r - mean) ** 2 for r in ratios) / len(ratios)
            else:
                variance = 1.0
            
            # Check for existing similar pattern
            existing = self._find_similar_pattern(skill_name, domain, common_context)
            
            if existing:
                # Update existing pattern with Bayesian update
                new_confidence = self._bayesian_update(
                    existing.confidence,
                    existing.successes + successes,
                    existing.failures + failures
                )
                
                existing.confidence = new_confidence
                existing.successes += successes
                existing.failures += failures
                existing.evidence_count += len(episodes)
                existing.expected_value = 0.9 * existing.expected_value + 0.1 * expected_value
                existing.variance = 0.9 * existing.variance + 0.1 * variance
                existing.updated_at = datetime.now(timezone.utc).isoformat()

                # Merge new source_episodes without duplicates. Re-running the
                # consolidation cycle against the same episodes would otherwise
                # keep appending the same IDs and inflate the list unbounded.
                existing_ids = set(existing.source_episodes)
                for ep in episodes:
                    if ep.episode_id not in existing_ids:
                        existing.source_episodes.append(ep.episode_id)
                        existing_ids.add(ep.episode_id)
                
                # Store the update
                self.store(existing)
                return existing
            else:
                # Create new pattern
                initial_confidence = self._bayesian_update(
                    self._config.initial_confidence,
                    successes,
                    failures
                )
                
                pattern = SemanticPattern(
                    pattern_id=str(uuid.uuid4())[:12],
                    skill_name=skill_name,
                    domain=domain,
                    condition=common_context,
                    recommendation=recommendation,
                    confidence=initial_confidence,
                    variance=variance,
                    evidence_count=len(episodes),
                    successes=successes,
                    failures=failures,
                    recent_accuracy=successes / len(episodes) if episodes else 0.5,
                    source_episodes=[e.episode_id for e in episodes],
                )
                pattern.expected_value = expected_value
                
                # Store the new pattern
                self.store(pattern)
                return pattern
    
    def update_from_outcome(
        self,
        pattern_id: str,
        domain: Domain,
        success: bool,
        observed_ratio: float
    ):
        """
        Update a pattern's confidence based on a new outcome.
        
        Applies Bayesian update and EMA for expected_value.
        
        Args:
            pattern_id: The pattern ID to update
            domain: Domain the pattern belongs to
            success: Whether the outcome was successful
            observed_ratio: The observed signal/baseline ratio
        """
        with self._lock:
            collection_path = self._get_collection_path(domain)
            
            try:
                # Get current pattern
                if not hasattr(self._firebase, "get_document"):
                    logger.warning("Firebase client missing get_document method")
                    return
                
                doc = self._firebase.get_document(
                    collection=collection_path,
                    doc_id=pattern_id
                )
                
                if not doc:
                    logger.warning(f"Pattern {pattern_id} not found for update")
                    return
                
                pattern = self._doc_to_pattern(doc)
                if pattern is None:
                    return
                
                # Update evidence counts
                pattern.evidence_count += 1
                if success:
                    pattern.successes += 1
                else:
                    pattern.failures += 1
                
                # Apply Bayesian update to confidence
                pattern.confidence = self._bayesian_update(
                    pattern.confidence,
                    pattern.successes,
                    pattern.failures
                )
                
                # Update expected_value with EMA (0.9 * old + 0.1 * new)
                pattern.expected_value = 0.9 * pattern.expected_value + 0.1 * observed_ratio
                
                # Update recent_accuracy with rolling EMA
                outcome_value = 1.0 if success else 0.0
                pattern.recent_accuracy = 0.9 * pattern.recent_accuracy + 0.1 * outcome_value

                if not hasattr(pattern, 'recommendation') or pattern.recommendation is None:
                    pattern.recommendation = {}
                pattern.recommendation["sample_count"] = pattern.evidence_count
                
                # Update timestamps
                now_iso = datetime.now(timezone.utc).isoformat()
                pattern.updated_at = now_iso
                
                # Build update data
                update_data = pattern.to_dict()
                update_data["last_reinforced_at"] = now_iso
                
                # Store update
                if hasattr(self._firebase, "update_document"):
                    self._firebase.update_document(
                        collection=collection_path,
                        doc_id=pattern_id,
                        data=update_data
                    )
                else:
                    # Fall back to set_document
                    self._firebase.set_document(
                        collection=collection_path,
                        doc_id=pattern_id,
                        data=update_data
                    )
                
                logger.debug(
                    f"Updated pattern {pattern_id}: confidence={pattern.confidence:.3f}, "
                    f"expected_value={pattern.expected_value:.3f}"
                )
                
            except Exception as e:
                logger.error(f"Error updating pattern {pattern_id}: {e}")

    def update_confidence(self, pattern_id: str, confidence: float) -> None:
        """
        Directly set a pattern's confidence value.
        
        Used by shadow testing to apply promoted candidate weights.
        Searches across all domains since pattern_id is globally unique.
        
        Args:
            pattern_id: The pattern ID to update
            confidence: New confidence value (clamped to [min, max])
        """
        confidence = max(self._config.min_confidence,
                         min(self._config.max_confidence, confidence))
        with self._lock:
            for domain in Domain:
                collection_path = self._get_collection_path(domain)
                try:
                    if not hasattr(self._firebase, "get_document"):
                        return
                    doc = self._firebase.get_document(
                        collection=collection_path, doc_id=pattern_id
                    )
                    if not doc:
                        continue
                    now_iso = datetime.now(timezone.utc).isoformat()
                    update_data = {"confidence": confidence, "updated_at": now_iso}
                    if hasattr(self._firebase, "update_document"):
                        self._firebase.update_document(
                            collection=collection_path,
                            doc_id=pattern_id,
                            data=update_data,
                        )
                    else:
                        self._firebase.set_document(
                            collection=collection_path,
                            doc_id=pattern_id,
                            data=update_data,
                            merge=True,
                        )
                    logger.debug(
                        f"Updated confidence for {pattern_id} to {confidence:.3f}"
                    )
                    return
                except Exception as e:
                    logger.debug(f"Could not update {pattern_id} in {domain.value}: {e}")

    def forget_stale_patterns(self) -> int:
        """
        Archive patterns below forget_threshold that have sufficient evidence.
        
        Only archives patterns that have at least min_evidence_for_trust evidence
        to avoid archiving patterns that simply haven't been tested yet.
        
        Returns:
            Count of archived patterns
        """
        with self._lock:
            archived_count = 0
            
            # Iterate through all domains
            for domain in Domain:
                collection_path = self._get_collection_path(domain)
                
                try:
                    if not hasattr(self._firebase, "get_collection"):
                        continue
                    
                    docs = self._firebase.get_collection(
                        collection=collection_path,
                        limit=1000
                    )
                    
                    if not docs:
                        continue
                    
                    for doc in docs:
                        pattern = self._doc_to_pattern(doc)
                        if pattern is None:
                            continue
                        
                        # Apply decay
                        last_reinforced = doc.get("last_reinforced_at", pattern.updated_at)
                        days_since = self._calculate_days_since(last_reinforced)
                        
                        if days_since > 0:
                            pattern.confidence *= (self._config.decay_rate ** days_since)
                        
                        # Check if should forget
                        if (pattern.confidence < self._config.forget_threshold and
                            pattern.evidence_count >= self._config.min_evidence_for_trust):
                            
                            self._archive_pattern(pattern, domain, doc)
                            archived_count += 1
                            logger.info(
                                f"Archived stale pattern {pattern.pattern_id} "
                                f"(confidence={pattern.confidence:.3f}, "
                                f"evidence={pattern.evidence_count})"
                            )
                
                except Exception as e:
                    logger.error(f"Error processing domain {domain.value} for stale patterns: {e}")
            
            return archived_count
    
    def _bayesian_update(
        self,
        prior: float,
        successes: int,
        failures: int
    ) -> float:
        """
        Perform Bayesian update using Beta distribution.
        
        The Beta distribution is parameterized by alpha and beta:
        - alpha = prior * 10 + successes (pseudo-count for successes)
        - beta = (1-prior) * 10 + failures (pseudo-count for failures)
        
        Args:
            prior: Prior probability [0, 1]
            successes: Number of observed successes
            failures: Number of observed failures
            
        Returns:
            Posterior probability clamped to [min_confidence, max_confidence]
        """
        # Convert prior to pseudo-counts (stronger priors have more pseudo-evidence)
        alpha = prior * 10 + successes
        beta = (1 - prior) * 10 + failures
        
        # Posterior mean of Beta distribution
        posterior = alpha / (alpha + beta)
        
        # Clamp to configured bounds
        return max(
            self._config.min_confidence,
            min(self._config.max_confidence, posterior)
        )
    
    def _extract_common_context(
        self,
        episodes: List[EpisodicMemory]
    ) -> Dict[str, Any]:
        """
        Find the intersection of all episode contexts.

        For numeric values, finds common ranges.
        For categorical values, finds common exact matches.

        When episodes have no context data at all, returns a minimal
        synthetic context with the skill name and episode count so
        that downstream ``_find_similar_pattern`` can still group
        patterns meaningfully instead of matching everything.

        Args:
            episodes: List of episodes to extract common context from

        Returns:
            Dictionary of common context key-value pairs (never empty)
        """
        if not episodes:
            return {"_synthetic": True, "_source": "no_episodes"}

        contexts = [e.prediction.context for e in episodes if e.prediction.context]

        if not contexts:
            # All episodes lack context — build synthetic key from skill
            skill = episodes[0].prediction.skill_name if episodes else "unknown"
            return {
                "_synthetic": True,
                "skill_name": skill,
                "episode_count": len(episodes),
            }

        # Find keys present in ALL contexts (strict intersection)
        common_keys = set(contexts[0].keys())
        for ctx in contexts[1:]:
            common_keys &= set(ctx.keys())

        # Relax: if strict intersection is empty, fall back to keys in >=50% of contexts
        if not common_keys and len(contexts) > 1:
            from collections import Counter
            key_counts = Counter(k for ctx in contexts for k in ctx)
            threshold = max(2, len(contexts) // 2)
            common_keys = {k for k, v in key_counts.items() if v >= threshold}

        common_context: Dict[str, Any] = {}

        for key in common_keys:
            values = [ctx[key] for ctx in contexts if key in ctx]
            if not values:
                continue

            if all(isinstance(v, (int, float)) for v in values):
                sorted_vals = sorted(values)
                n = len(sorted_vals)
                mean_val = sum(values) / n

                if n < 3 or (sorted_vals[-1] - sorted_vals[0]) <= (0.2 * abs(mean_val) if mean_val != 0 else 0.2):
                    common_context[key] = mean_val
                else:
                    # 10th-90th percentile range instead of raw min/max
                    p10_idx = max(0, int(n * 0.10))
                    p90_idx = min(n - 1, int(n * 0.90))
                    p10 = sorted_vals[p10_idx]
                    p90 = sorted_vals[p90_idx]
                    range_width = p90 - p10
                    padding = range_width * 0.10
                    common_context[key] = {"min": p10 - padding, "max": p90 + padding}
            else:
                unique_vals = list(set(str(v) for v in values))
                if len(unique_vals) == 1:
                    common_context[key] = values[0]
                elif len(unique_vals) <= 10:
                    common_context[key] = unique_vals

        # Guarantee non-empty so _find_similar_pattern has something to match on
        if not common_context:
            skill = episodes[0].prediction.skill_name if episodes else "unknown"
            common_context = {
                "_synthetic": True,
                "skill_name": skill,
                "episode_count": len(episodes),
            }

        return common_context
    
    def _extract_recommendation(
        self,
        episodes: List[EpisodicMemory]
    ) -> Dict[str, Any]:
        """
        Extract recommendation from successful episodes.
        
        Averages the contexts of episodes where goal_completed is True.
        
        Args:
            episodes: List of episodes
            
        Returns:
            Dictionary of recommended context values
        """
        successful = [e for e in episodes if e.outcome.goal_completed]
        
        if not successful:
            # Fall back to all episodes
            successful = episodes
        
        if not successful:
            return {}
        
        contexts = [e.prediction.context for e in successful if e.prediction.context]
        
        if not contexts:
            return {}
        
        # Collect all keys
        all_keys = set()
        for ctx in contexts:
            all_keys.update(ctx.keys())
        
        recommendation = {}
        
        for key in all_keys:
            values = [ctx[key] for ctx in contexts if key in ctx]
            
            if not values:
                continue
            
            # For numeric values, average them
            if all(isinstance(v, (int, float)) for v in values):
                recommendation[key] = sum(values) / len(values)
            else:
                # For categorical, use mode (most common)
                from collections import Counter
                counter = Counter(str(v) for v in values)
                most_common = counter.most_common(1)[0][0]
                # Find original value with that string representation
                for v in values:
                    if str(v) == most_common:
                        recommendation[key] = v
                        break
        
        return recommendation
    
    def _find_similar_pattern(
        self,
        skill_name: str,
        domain: Domain,
        context: Dict[str, Any]
    ) -> Optional[SemanticPattern]:
        """
        Find an existing pattern with similar context.
        
        Uses context matching with 20% numeric tolerance.
        
        Args:
            skill_name: Skill name to match
            domain: Domain to search
            context: Context to match against
            
        Returns:
            SemanticPattern if found, None otherwise
        """
        collection_path = self._get_collection_path(domain)
        
        try:
            filters = [("skill_name", "==", skill_name)]
            
            if hasattr(self._firebase, "query"):
                docs = self._firebase.query(
                    collection=collection_path,
                    filters=filters,
                    limit=50
                )
            elif hasattr(self._firebase, "get_collection"):
                docs = self._firebase.get_collection(
                    collection=collection_path,
                    limit=50
                )
            else:
                return None
            
            if not docs:
                return None
            
            for doc in docs:
                pattern = self._doc_to_pattern(doc)
                if pattern is None:
                    continue
                
                if pattern.skill_name != skill_name:
                    continue
                
                if self._contexts_match(pattern.condition, context):
                    return pattern
            
        except Exception as e:
            logger.error(f"Error finding similar pattern: {e}")
        
        return None
    
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
        episode_ctx: Dict[str, Any],
        pattern_updated_at: Optional[str] = None,
    ) -> float:
        """Score context match using dimension-weighted comparison (0.0-1.0).

        Includes era penalty: patterns >2 years old are progressively penalized.
        """
        if not pattern_ctx:
            return 1.0
        if not episode_ctx:
            return 0.0

        total_weight = 0.0
        matched_weight = 0.0

        for key, pattern_val in pattern_ctx.items():
            if key.startswith("_"):
                continue

            dim_w = self._dim_weight(key)
            total_weight += dim_w

            if key not in episode_ctx:
                continue

            episode_val = episode_ctx[key]

            if isinstance(pattern_val, dict) and "min" in pattern_val and "max" in pattern_val:
                if isinstance(episode_val, (int, float)):
                    if pattern_val["min"] <= episode_val <= pattern_val["max"]:
                        matched_weight += dim_w
            elif isinstance(pattern_val, (int, float)) and isinstance(episode_val, (int, float)):
                if pattern_val == 0:
                    if episode_val == 0:
                        matched_weight += dim_w
                else:
                    tolerance = abs(pattern_val) * 0.2
                    if abs(pattern_val - episode_val) <= tolerance:
                        matched_weight += dim_w
            elif isinstance(pattern_val, list):
                if str(episode_val) in [str(v) for v in pattern_val]:
                    matched_weight += dim_w
            else:
                if str(pattern_val) == str(episode_val):
                    matched_weight += dim_w

        if total_weight == 0:
            return 1.0

        score = matched_weight / total_weight

        if pattern_updated_at:
            days_since = self._calculate_days_since(pattern_updated_at)
            era_penalty = max(0.0, 1.0 - (days_since / 730))
            score *= era_penalty

        return score

    def _dim_weight(self, key: str) -> float:
        if key in self._IDENTITY_KEYS:
            return 1.0
        if key in self._TEMPORAL_KEYS:
            return 0.3
        if key in self._SPEND_KEYS:
            return 0.3
        if key in self._STRATEGY_KEYS:
            return 0.8
        return 0.5

    def _contexts_match(
        self,
        pattern_ctx: Dict[str, Any],
        episode_ctx: Dict[str, Any],
    ) -> bool:
        """Backward-compatible wrapper using weighted scoring."""
        return self._weighted_context_score(pattern_ctx, episode_ctx) >= self._MATCH_THRESHOLD
    
    def _archive_pattern(
        self,
        pattern: SemanticPattern,
        domain: Domain,
        original_doc: Optional[Dict[str, Any]] = None
    ):
        """
        Move a pattern to the archive collection.
        
        Args:
            pattern: The pattern to archive
            domain: Domain the pattern belongs to
            original_doc: Original Firebase document (to preserve extended fields)
        """
        active_path = self._get_collection_path(domain)
        archive_path = self._get_archive_path(domain)
        
        try:
            # Prepare archive data
            archive_data = pattern.to_dict()
            archive_data["archived_at"] = datetime.now(timezone.utc).isoformat()
            
            # Preserve extended fields from original doc
            if original_doc:
                for key in ["tenant_ids", "last_reinforced_at"]:
                    if key in original_doc:
                        archive_data[key] = original_doc[key]
            
            # Write to archive
            if hasattr(self._firebase, "set_document"):
                self._firebase.set_document(
                    collection=archive_path,
                    doc_id=pattern.pattern_id,
                    data=archive_data
                )
            else:
                logger.warning("Firebase client missing set_document method")
                return
            
            # Delete from active
            if hasattr(self._firebase, "delete_document"):
                self._firebase.delete_document(
                    collection=active_path,
                    doc_id=pattern.pattern_id
                )
            else:
                logger.warning("Firebase client missing delete_document method")
            
            logger.info(f"Archived pattern {pattern.pattern_id}")
            
        except Exception as e:
            logger.error(f"Error archiving pattern {pattern.pattern_id}: {e}")
    
    def _doc_to_pattern(self, doc: Dict[str, Any]) -> Optional[SemanticPattern]:
        """
        Convert a Firebase document to a SemanticPattern object.
        
        Args:
            doc: Firebase document dictionary
            
        Returns:
            SemanticPattern object or None if conversion fails
        """
        if not doc:
            return None
        
        try:
            # Handle _id from Firebase metadata
            pattern_id = doc.get("pattern_id") or doc.get("_id")
            if not pattern_id:
                logger.warning("Document missing pattern_id")
                return None
            
            # Reconstruct domain
            domain_value = doc.get("domain", "generic")
            if isinstance(domain_value, str):
                domain = Domain(domain_value)
            else:
                domain = domain_value
            
            pattern = SemanticPattern(
                pattern_id=pattern_id,
                skill_name=doc.get("skill_name", ""),
                domain=domain,
                condition=doc.get("condition", {}),
                recommendation=doc.get("recommendation", {}),
                confidence=doc.get("confidence", 0.5),
                variance=doc.get("variance", 1.0),
                evidence_count=doc.get("evidence_count", 0),
                successes=doc.get("successes", 0),
                failures=doc.get("failures", 0),
                recent_accuracy=doc.get("recent_accuracy", 0.5),
                created_at=doc.get("created_at", datetime.now(timezone.utc).isoformat()),
                updated_at=doc.get("updated_at", datetime.now(timezone.utc).isoformat()),
                source_episodes=doc.get("source_episodes", []),
            )
            pattern.expected_value = doc.get("expected_value", 1.0)
            
            return pattern
            
        except Exception as e:
            logger.error(f"Error converting document to pattern: {e}")
            return None
    
    def get_pattern(
        self,
        pattern_id: str,
        domain: Domain
    ) -> Optional[SemanticPattern]:
        """
        Get a single pattern by ID.
        
        Args:
            pattern_id: The pattern ID
            domain: Domain the pattern belongs to
            
        Returns:
            SemanticPattern or None if not found
        """
        with self._lock:
            collection_path = self._get_collection_path(domain)
            
            try:
                if hasattr(self._firebase, "get_document"):
                    doc = self._firebase.get_document(
                        collection=collection_path,
                        doc_id=pattern_id
                    )
                    if doc:
                        return self._doc_to_pattern(doc)
                else:
                    logger.warning("Firebase client missing get_document method")
                
            except Exception as e:
                logger.error(f"Error getting pattern {pattern_id}: {e}")
            
            return None
    
    def delete_pattern(
        self,
        pattern_id: str,
        domain: Domain
    ) -> bool:
        """
        Delete a pattern from the store.
        
        Args:
            pattern_id: The pattern ID to delete
            domain: Domain the pattern belongs to
            
        Returns:
            True if deleted, False otherwise
        """
        with self._lock:
            collection_path = self._get_collection_path(domain)
            
            try:
                if hasattr(self._firebase, "delete_document"):
                    self._firebase.delete_document(
                        collection=collection_path,
                        doc_id=pattern_id
                    )
                    logger.debug(f"Deleted pattern {pattern_id}")
                    return True
                else:
                    logger.warning("Firebase client missing delete_document method")
                    
            except Exception as e:
                logger.error(f"Error deleting pattern {pattern_id}: {e}")
            
            return False

    # =========================================================================
    # Similarity Search Methods
    # =========================================================================

    def _tokenize(
        self,
        text: str,
        include_ngrams: bool = True
    ) -> List[str]:
        """
        Tokenize text for similarity comparison.

        Performs lowercasing, stopword removal, and optional n-gram generation.

        Args:
            text: Input text to tokenize
            include_ngrams: Whether to include bigrams

        Returns:
            List of tokens
        """
        if not text:
            return []

        # Lowercase and extract word tokens
        text = text.lower()
        tokens = re.findall(r'\b\w+\b', text)

        # Filter stopwords and short tokens
        tokens = [
            t for t in tokens
            if t not in STOPWORDS and len(t) > 2
        ]

        # Add bigrams for better semantic matching
        if include_ngrams and len(tokens) >= 2:
            bigrams = [
                f"{tokens[i]}_{tokens[i+1]}"
                for i in range(len(tokens) - 1)
            ]
            tokens = tokens + bigrams

        return tokens

    def _compute_jaccard_similarity(
        self,
        tokens1: List[str],
        tokens2: List[str]
    ) -> float:
        """
        Compute Jaccard similarity between two token sets.

        Args:
            tokens1: First token list
            tokens2: Second token list

        Returns:
            Jaccard similarity score [0, 1]
        """
        set1, set2 = set(tokens1), set(tokens2)
        if not set1 or not set2:
            return 0.0

        intersection = len(set1 & set2)
        union = len(set1 | set2)

        return intersection / union if union > 0 else 0.0

    def _compute_weighted_similarity(
        self,
        tokens1: List[str],
        tokens2: List[str],
        idf_weights: Optional[Dict[str, float]] = None
    ) -> float:
        """
        Compute TF-IDF weighted similarity.

        Args:
            tokens1: First token list
            tokens2: Second token list
            idf_weights: Optional IDF weights for tokens

        Returns:
            Weighted similarity score [0, 1]
        """
        if not tokens1 or not tokens2:
            return 0.0

        # Count term frequencies
        tf1 = Counter(tokens1)
        tf2 = Counter(tokens2)

        # Get all unique tokens
        all_tokens = set(tf1.keys()) | set(tf2.keys())

        # Compute weighted dot product
        dot_product = 0.0
        norm1 = 0.0
        norm2 = 0.0

        for token in all_tokens:
            weight = idf_weights.get(token, 1.0) if idf_weights else 1.0
            v1 = tf1.get(token, 0) * weight
            v2 = tf2.get(token, 0) * weight

            dot_product += v1 * v2
            norm1 += v1 * v1
            norm2 += v2 * v2

        if norm1 == 0 or norm2 == 0:
            return 0.0

        return dot_product / (norm1 ** 0.5 * norm2 ** 0.5)

    def _pattern_to_text(self, pattern: SemanticPattern) -> str:
        """
        Convert a pattern to searchable text representation.

        Args:
            pattern: The semantic pattern

        Returns:
            Text representation for similarity matching
        """
        parts = [pattern.skill_name]

        if pattern.domain:
            parts.append(pattern.domain.value if hasattr(pattern.domain, 'value') else str(pattern.domain))

        # Add condition keys and values
        if pattern.condition:
            for key, value in pattern.condition.items():
                parts.append(str(key))
                if isinstance(value, (str, int, float)):
                    parts.append(str(value))

        # Add recommendation keys and values
        if pattern.recommendation:
            for key, value in pattern.recommendation.items():
                parts.append(str(key))
                if isinstance(value, (str, int, float)):
                    parts.append(str(value))

        return " ".join(parts)

    def find_similar_context(
        self,
        query: str,
        domain: Optional[Domain] = None,
        limit: int = 5,
        min_similarity: float = 0.1,
        skill_filter: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Find patterns with similar context using token-based similarity.

        This is the primary similarity search method. It uses Jaccard similarity
        with optional TF-IDF weighting for fast, dependency-free matching.

        Args:
            query: Search query text
            domain: Optional domain to filter by (searches all if None)
            limit: Maximum number of results to return
            min_similarity: Minimum similarity threshold [0, 1]
            skill_filter: Optional skill name to filter by

        Returns:
            List of dicts with keys: id, text, similarity, pattern, metadata
        """
        with self._lock:
            query_tokens = self._tokenize(query)

            if not query_tokens:
                logger.debug("Empty query after tokenization")
                return []

            results = []
            domains_to_search = [domain] if domain else list(Domain)

            # Build IDF weights from all patterns
            all_documents: List[List[str]] = []

            for d in domains_to_search:
                collection_path = self._get_collection_path(d)

                try:
                    if hasattr(self._firebase, "get_collection"):
                        docs = self._firebase.get_collection(
                            collection=collection_path,
                            limit=500
                        )
                    elif hasattr(self._firebase, "query"):
                        docs = self._firebase.query(
                            collection=collection_path,
                            filters=[],
                            limit=500
                        )
                    else:
                        continue

                    if not docs:
                        continue

                    for doc in docs:
                        pattern = self._doc_to_pattern(doc)
                        if pattern is None:
                            continue

                        # Apply skill filter if specified
                        if skill_filter and pattern.skill_name != skill_filter:
                            continue

                        # Convert pattern to text and tokenize
                        pattern_text = self._pattern_to_text(pattern)
                        pattern_tokens = self._tokenize(pattern_text)

                        if pattern_tokens:
                            all_documents.append(pattern_tokens)

                        # Compute similarity
                        similarity = self._compute_jaccard_similarity(
                            query_tokens, pattern_tokens
                        )

                        if similarity >= min_similarity:
                            results.append({
                                "id": pattern.pattern_id,
                                "text": pattern_text,
                                "similarity": similarity,
                                "pattern": pattern,
                                "metadata": {
                                    "skill_name": pattern.skill_name,
                                    "domain": pattern.domain.value if hasattr(pattern.domain, 'value') else str(pattern.domain),
                                    "confidence": pattern.confidence,
                                    "evidence_count": pattern.evidence_count,
                                }
                            })

                except Exception as e:
                    logger.error(f"Error searching domain {d}: {e}")

            # Sort by similarity descending
            results.sort(key=lambda x: x["similarity"], reverse=True)

            return results[:limit]

    def find_similar_with_embeddings(
        self,
        query: str,
        domain: Optional[Domain] = None,
        limit: int = 5,
        min_similarity: float = 0.3
    ) -> List[Dict[str, Any]]:
        """
        Find similar patterns using Claude for semantic similarity.

        This method uses Claude to assess semantic similarity, providing more
        accurate results for conceptual queries. Falls back to token-based
        similarity if anthropic package is not available.

        Args:
            query: Search query text
            domain: Optional domain to filter by
            limit: Maximum number of results
            min_similarity: Minimum similarity threshold

        Returns:
            List of dicts with keys: id, text, similarity, pattern, metadata
        """
        try:
            from anthropic import Anthropic
        except ImportError:
            logger.info("anthropic package not available, falling back to token-based similarity")
            return self.find_similar_context(query, domain, limit, min_similarity)

        with self._lock:
            # First, get candidate patterns with loose token similarity
            candidates = self.find_similar_context(
                query=query,
                domain=domain,
                limit=limit * 3,  # Get more candidates for re-ranking
                min_similarity=0.05  # Lower threshold for candidates
            )

            if not candidates:
                return []

            # Prepare patterns for Claude ranking
            patterns_text = []
            for i, candidate in enumerate(candidates):
                patterns_text.append(
                    f"{i+1}. [{candidate['id']}] {candidate['text']}"
                )

            # Use Claude to rank similarity
            try:
                client = Anthropic()

                prompt = f"""Rate the semantic similarity of each pattern to the query.
Query: "{query}"

Patterns:
{chr(10).join(patterns_text)}

For each pattern, respond with its number and a similarity score from 0.0 to 1.0.
Format: NUMBER:SCORE (one per line)
Only include patterns with similarity >= {min_similarity}"""

                response = client.messages.create(
                    model="claude-3-haiku-20240307",  # Use Haiku for fast/cheap ranking
                    max_tokens=500,
                    messages=[{"role": "user", "content": prompt}]
                )

                # Parse response
                response_text = response.content[0].text
                scored_results = []

                for line in response_text.strip().split('\n'):
                    line = line.strip()
                    if ':' in line:
                        try:
                            parts = line.split(':')
                            idx = int(parts[0].strip()) - 1
                            score = float(parts[1].strip())

                            if 0 <= idx < len(candidates) and score >= min_similarity:
                                result = candidates[idx].copy()
                                result['similarity'] = score
                                result['similarity_method'] = 'embedding'
                                scored_results.append(result)
                        except (ValueError, IndexError):
                            continue

                # Sort by similarity
                scored_results.sort(key=lambda x: x['similarity'], reverse=True)

                return scored_results[:limit]

            except Exception as e:
                logger.warning(f"Claude similarity ranking failed: {e}, using token-based fallback")
                return candidates[:limit]

    def store_concept(
        self,
        concept_id: str,
        text: str,
        domain: Domain = Domain.GENERIC,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Store a concept for similarity search.

        This is a convenience method that creates a minimal SemanticPattern
        for concepts that need to be searchable but don't have full pattern data.

        Args:
            concept_id: Unique identifier for the concept
            text: Text description of the concept
            domain: Domain for the concept
            metadata: Optional metadata to store

        Returns:
            The concept_id
        """
        # Create a minimal pattern for storage
        pattern = SemanticPattern(
            pattern_id=concept_id,
            skill_name="concept",  # Special skill name for concepts
            domain=domain,
            condition={"text": text},
            recommendation=metadata or {},
            confidence=0.5,
            evidence_count=0,
        )

        return self.store(pattern)

    def get_concepts_by_type(
        self,
        concept_type: str,
        domain: Optional[Domain] = None
    ) -> List[Dict[str, Any]]:
        """
        Get all concepts of a specific type.

        Args:
            concept_type: The type/category of concepts to retrieve
            domain: Optional domain filter

        Returns:
            List of concept dictionaries
        """
        # Search for concepts with matching type in metadata
        with self._lock:
            results = []
            domains_to_search = [domain] if domain else list(Domain)

            for d in domains_to_search:
                patterns = self.retrieve_patterns(
                    skill_name="concept",
                    domain=d,
                    limit=100
                )

                for pattern in patterns:
                    if pattern.recommendation.get("type") == concept_type:
                        results.append({
                            "id": pattern.pattern_id,
                            "text": pattern.condition.get("text", ""),
                            "type": concept_type,
                            "metadata": pattern.recommendation,
                            "confidence": pattern.confidence,
                        })

            return results

    def consolidate_episodes(
        self,
        tenant_id: str,
        skill_name: str,
        episodes: List[EpisodicMemory]
    ) -> Dict[str, int]:
        """
        Consolidate a list of episodic memories into semantic patterns.

        This is called by the MemoryConsolidationManager during consolidation cycles.
        It groups episodes by similar context and creates or updates semantic patterns.

        Args:
            tenant_id: Tenant identifier
            skill_name: Name of the skill
            episodes: List of episodic memories to consolidate

        Returns:
            Dict with keys:
            - created: Number of new patterns created
            - updated: Number of existing patterns updated
        """
        with self._lock:
            stats = {"created": 0, "updated": 0}

            if not episodes:
                return stats

            # Group episodes by domain
            domain_groups: Dict[Domain, List[EpisodicMemory]] = {}
            for episode in episodes:
                domain = episode.prediction.domain
                if domain not in domain_groups:
                    domain_groups[domain] = []
                domain_groups[domain].append(episode)

            # Process each domain group
            for domain, domain_episodes in domain_groups.items():
                if not domain_episodes:
                    continue

                # Try to consolidate into existing or new pattern
                pattern = self.consolidate_from_episodes(domain_episodes)

                if pattern is not None:
                    # Check if this was an update or creation by checking
                    # if the pattern existed before with fewer episodes
                    if pattern.evidence_count > len(domain_episodes):
                        stats["updated"] += 1
                    else:
                        stats["created"] += 1

                    logger.debug(
                        f"Consolidated {len(domain_episodes)} episodes into pattern "
                        f"{pattern.pattern_id} for {tenant_id}/{skill_name}/{domain.value}"
                    )

            return stats

    def get_high_confidence_patterns(
        self,
        min_confidence: float = 0.6
    ) -> List[SemanticPattern]:
        """
        Get all patterns with confidence above threshold across all domains.

        Used by consolidation manager to find cross-skill pattern candidates.

        Args:
            min_confidence: Minimum confidence threshold

        Returns:
            List of SemanticPattern objects with confidence >= min_confidence
        """
        with self._lock:
            results = []

            try:
                for domain in Domain:
                    collection_path = self._get_collection_path(domain)

                    try:
                        if hasattr(self._firebase, "get_collection"):
                            docs = self._firebase.get_collection(
                                collection=collection_path,
                                limit=500
                            )
                        else:
                            continue

                        if not docs:
                            continue

                        for doc in docs:
                            pattern = self._doc_to_pattern(doc)
                            if pattern is None:
                                continue

                            # Apply decay
                            last_reinforced = doc.get("last_reinforced_at", pattern.updated_at)
                            days_since = self._calculate_days_since(last_reinforced)

                            if days_since > 0:
                                pattern.confidence *= (self._config.decay_rate ** days_since)

                            if pattern.confidence >= min_confidence:
                                results.append(pattern)
                    except Exception as e:
                        logger.debug(f"Error getting patterns for domain {domain.value}: {e}")
                        continue

                return results

            except Exception as e:
                logger.error(f"Error in get_high_confidence_patterns: {e}")
                return []

    def get_all_patterns(self) -> List[SemanticPattern]:
        """
        Get all patterns across all domains.

        Returns:
            List of all SemanticPattern objects
        """
        return self.get_high_confidence_patterns(min_confidence=0.0)

    def get_skills_for_tenant(self, tenant_id: str) -> List[str]:
        """
        Get all skill names that have patterns for a tenant.

        Args:
            tenant_id: Tenant identifier

        Returns:
            List of skill names
        """
        with self._lock:
            skills = set()

            try:
                for domain in Domain:
                    collection_path = self._get_collection_path(domain)

                    try:
                        if hasattr(self._firebase, "get_collection"):
                            docs = self._firebase.get_collection(
                                collection=collection_path,
                                limit=500
                            )
                        else:
                            continue

                        if not docs:
                            continue

                        for doc in docs:
                            # Check if pattern applies to this tenant
                            tenant_ids = doc.get("tenant_ids", [])
                            if tenant_ids and tenant_id not in tenant_ids:
                                continue

                            skill_name = doc.get("skill_name")
                            if skill_name:
                                skills.add(skill_name)
                    except Exception as e:
                        logger.debug(f"Error getting skills for domain {domain.value}: {e}")
                        continue

                return list(skills)

            except Exception as e:
                logger.error(f"Error in get_skills_for_tenant: {e}")
                return []


class SemanticMemory:
    """
    Lightweight semantic memory with local file storage.

    This is a simpler alternative to SemanticMemoryStore that doesn't require
    Firebase. Useful for local development, testing, or standalone usage.

    Storage: ~/.mh1/memory/semantic/index.json
    """

    def __init__(self, storage_dir: Optional[str] = None):
        """
        Initialize semantic memory with local storage.

        Args:
            storage_dir: Directory for storage (default: ~/.mh1/memory/semantic)
        """
        self.storage_dir = storage_dir or os.path.expanduser("~/.mh1/memory/semantic")
        os.makedirs(self.storage_dir, exist_ok=True)
        self._lock = threading.RLock()
        self.index = self._load_index()

    def _load_index(self) -> Dict[str, Any]:
        """Load the index from disk."""
        index_path = os.path.join(self.storage_dir, "index.json")
        if os.path.exists(index_path):
            try:
                with open(index_path, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"Error loading semantic index: {e}")
        return {"concepts": [], "patterns": []}

    def _save_index(self):
        """Save the index to disk."""
        index_path = os.path.join(self.storage_dir, "index.json")
        try:
            with open(index_path, 'w') as f:
                json.dump(self.index, f, indent=2)
        except IOError as e:
            logger.error(f"Error saving semantic index: {e}")

    def _tokenize(self, text: str) -> List[str]:
        """
        Simple tokenization for similarity.

        Args:
            text: Input text

        Returns:
            List of tokens
        """
        if not text:
            return []

        text = text.lower()
        tokens = re.findall(r'\b\w+\b', text)

        # Remove stopwords and short tokens
        return [t for t in tokens if t not in STOPWORDS and len(t) > 2]

    def _compute_similarity(
        self,
        tokens1: List[str],
        tokens2: List[str]
    ) -> float:
        """
        Jaccard similarity between token sets.

        Args:
            tokens1: First token list
            tokens2: Second token list

        Returns:
            Similarity score [0, 1]
        """
        set1, set2 = set(tokens1), set(tokens2)
        if not set1 or not set2:
            return 0.0

        intersection = len(set1 & set2)
        union = len(set1 | set2)

        return intersection / union if union > 0 else 0.0

    def store_concept(
        self,
        concept_id: str,
        text: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Store a concept with its text representation.

        Args:
            concept_id: Unique identifier
            text: Text description
            metadata: Optional metadata

        Returns:
            The concept_id
        """
        with self._lock:
            tokens = self._tokenize(text)
            entry = {
                "id": concept_id,
                "text": text,
                "tokens": tokens,
                "metadata": metadata or {},
                "stored_at": datetime.now(timezone.utc).isoformat()
            }

            # Update or append
            existing_idx = next(
                (i for i, c in enumerate(self.index["concepts"])
                 if c["id"] == concept_id),
                None
            )

            if existing_idx is not None:
                self.index["concepts"][existing_idx] = entry
            else:
                self.index["concepts"].append(entry)

            self._save_index()
            return concept_id

    def find_similar_context(
        self,
        query: str,
        limit: int = 5,
        min_similarity: float = 0.1
    ) -> List[Dict[str, Any]]:
        """
        Find similar contexts using token overlap.

        Args:
            query: Search query
            limit: Maximum results
            min_similarity: Minimum similarity threshold

        Returns:
            List of matching concepts with similarity scores
        """
        with self._lock:
            query_tokens = self._tokenize(query)

            if not query_tokens:
                return []

            results = []
            for concept in self.index["concepts"]:
                similarity = self._compute_similarity(
                    query_tokens,
                    concept.get("tokens", [])
                )

                if similarity >= min_similarity:
                    results.append({
                        "id": concept["id"],
                        "text": concept["text"],
                        "similarity": similarity,
                        "metadata": concept.get("metadata", {})
                    })

            # Sort by similarity descending
            results.sort(key=lambda x: x["similarity"], reverse=True)

            return results[:limit]

    def find_similar_with_embeddings(
        self,
        query: str,
        limit: int = 5,
        min_similarity: float = 0.3
    ) -> List[Dict[str, Any]]:
        """
        Use Claude for semantic similarity (more accurate but slower).

        Falls back to token-based similarity if anthropic is not available.

        Args:
            query: Search query
            limit: Maximum results
            min_similarity: Minimum similarity threshold

        Returns:
            List of matching concepts with similarity scores
        """
        try:
            from anthropic import Anthropic
        except ImportError:
            logger.info("anthropic package not available, using token-based similarity")
            return self.find_similar_context(query, limit, min_similarity)

        with self._lock:
            # Get candidates with loose token similarity
            candidates = self.find_similar_context(
                query=query,
                limit=limit * 3,
                min_similarity=0.05
            )

            if not candidates:
                return []

            # Prepare for Claude ranking
            concepts_text = [
                f"{i+1}. [{c['id']}] {c['text']}"
                for i, c in enumerate(candidates)
            ]

            try:
                client = Anthropic()

                prompt = f"""Rate semantic similarity of each concept to the query.
Query: "{query}"

Concepts:
{chr(10).join(concepts_text)}

Respond with NUMBER:SCORE (0.0-1.0) per line.
Only include concepts with similarity >= {min_similarity}"""

                response = client.messages.create(
                    model="claude-3-haiku-20240307",
                    max_tokens=500,
                    messages=[{"role": "user", "content": prompt}]
                )

                # Parse response
                response_text = response.content[0].text
                scored_results = []

                for line in response_text.strip().split('\n'):
                    if ':' in line:
                        try:
                            parts = line.split(':')
                            idx = int(parts[0].strip()) - 1
                            score = float(parts[1].strip())

                            if 0 <= idx < len(candidates) and score >= min_similarity:
                                result = candidates[idx].copy()
                                result['similarity'] = score
                                result['similarity_method'] = 'embedding'
                                scored_results.append(result)
                        except (ValueError, IndexError):
                            continue

                scored_results.sort(key=lambda x: x['similarity'], reverse=True)
                return scored_results[:limit]

            except Exception as e:
                logger.warning(f"Claude ranking failed: {e}")
                return candidates[:limit]

    def store_pattern(
        self,
        pattern_id: str,
        pattern_type: str,
        content: Dict[str, Any],
        confidence: float = 0.5
    ) -> str:
        """
        Store a learned pattern.

        Args:
            pattern_id: Unique identifier
            pattern_type: Category of pattern
            content: Pattern content
            confidence: Initial confidence score

        Returns:
            The pattern_id
        """
        with self._lock:
            entry = {
                "id": pattern_id,
                "type": pattern_type,
                "content": content,
                "confidence": confidence,
                "occurrences": 1,
                "stored_at": datetime.now(timezone.utc).isoformat()
            }

            # Check for existing pattern
            existing_idx = next(
                (i for i, p in enumerate(self.index["patterns"])
                 if p["id"] == pattern_id),
                None
            )

            if existing_idx is not None:
                # Update existing
                existing = self.index["patterns"][existing_idx]
                existing["content"] = content
                existing["confidence"] = confidence
                existing["occurrences"] = existing.get("occurrences", 0) + 1
                existing["updated_at"] = datetime.now(timezone.utc).isoformat()
            else:
                self.index["patterns"].append(entry)

            self._save_index()
            return pattern_id

    def get_patterns_by_type(self, pattern_type: str) -> List[Dict[str, Any]]:
        """
        Get all patterns of a specific type.

        Args:
            pattern_type: The type/category to filter by

        Returns:
            List of matching patterns
        """
        with self._lock:
            return [
                p for p in self.index["patterns"]
                if p.get("type") == pattern_type
            ]

    def delete_concept(self, concept_id: str) -> bool:
        """
        Delete a concept by ID.

        Args:
            concept_id: The concept ID to delete

        Returns:
            True if deleted, False if not found
        """
        with self._lock:
            initial_len = len(self.index["concepts"])
            self.index["concepts"] = [
                c for c in self.index["concepts"]
                if c["id"] != concept_id
            ]

            if len(self.index["concepts"]) < initial_len:
                self._save_index()
                return True
            return False

    def clear(self):
        """Clear all stored concepts and patterns."""
        with self._lock:
            self.index = {"concepts": [], "patterns": []}
            self._save_index()


__all__ = [
    "SemanticMemoryConfig",
    "SemanticSimilarityConfig",
    "SemanticMemoryStore",
    "SemanticMemory",
]
