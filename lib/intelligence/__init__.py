"""
MH1 Intelligence System: Main interface.

Usage:
    from lib.intelligence import IntelligenceEngine, Domain
    
    engine = IntelligenceEngine()
    
    # Before skill execution
    guidance = engine.get_guidance("dormant-detection", tenant_id, Domain.REVENUE)
    prediction = engine.register_prediction(skill, tenant_id, Domain.REVENUE, expected, baseline)
    
    # After skill execution
    engine.record_outcome(prediction_id, observed_signal, goal_completed=True)
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, Optional, TYPE_CHECKING

# Core types
from .types import (
    Domain,
    EpisodicMemory,
    MemoryLayer,
    Outcome,
    Prediction,
    ProceduralKnowledge,
    SemanticPattern,
    TrajectoryPoint,
)

# Memory stores
from .memory import (
    EpisodicMemoryConfig,
    EpisodicMemoryStore,
    ProceduralMemoryConfig,
    ProceduralMemoryStore,
    SemanticMemoryConfig,
    SemanticMemoryStore,
)
from .memory.working import WorkingMemory, WorkingMemoryConfig
from .memory.consolidation import ConsolidationConfig, MemoryConsolidationManager

# Learning components
from .learning import (
    ExplorationConfig,
    Guidance,
    Learner,
    LearningConfig,
    Predictor,
)
from .learning.shadow import ShadowConfig, ShadowManager
from .learning.accuracy import AccuracyScorer
from .learning.gold_standard import GoldStandardValidator

# Domain adapters
from .adapters import (
    BaseDomainAdapter,
    CampaignAdapter,
    ContentAdapter,
    HealthAdapter,
    RevenueAdapter,
    ScoringResult,
)

if TYPE_CHECKING:
    from lib.firebase_client import FirebaseClient

logger = logging.getLogger(__name__)


def _auto_select_storage():
    """Pick storage backend: Supabase if configured, then Firebase, then None."""
    if os.environ.get("SUPABASE_URL"):
        try:
            from lib.supabase_storage import SupabaseStorage
            client = SupabaseStorage()
            logger.info("Using Supabase for memory storage")
            return client
        except Exception as e:
            logger.warning("Supabase configured but init failed: %s", e)

    try:
        from lib.firebase_client import get_firebase_client
        client = get_firebase_client()
        logger.info("Using Firebase for memory storage")
        return client
    except ImportError:
        pass
    except Exception as e:
        logger.warning("Firebase init failed: %s", e)

    logger.warning(
        "No storage backend available — intelligence memory will be in-memory only"
    )
    return None


class IntelligenceEngine:
    """
    Main entry point for the MH1 Intelligence System.
    
    The IntelligenceEngine provides a unified interface to the memory,
    learning, and prediction subsystems. It coordinates:
    
    - Memory layers (working, episodic, semantic, procedural)
    - Prediction and outcome tracking
    - Continuous learning from execution results
    - Domain-specific scoring and adaptation
    - Memory consolidation and knowledge transfer
    
    Typical workflow:
    
    1. Before skill execution:
       - Get guidance based on historical patterns
       - Register a prediction with expected outcomes
    
    2. After skill execution:
       - Record the observed outcome
       - System automatically learns from prediction errors
    
    3. Periodically:
       - Run consolidation to promote patterns to long-term memory
    
    Example:
        >>> from lib.intelligence import IntelligenceEngine, Domain
        >>> 
        >>> engine = IntelligenceEngine()
        >>> 
        >>> # Get guidance before running a skill
        >>> guidance = engine.get_guidance(
        ...     skill_name="dormant-detection",
        ...     tenant_id="acme-corp",
        ...     domain=Domain.REVENUE
        ... )
        >>> 
        >>> # Register prediction
        >>> pred_id = engine.register_prediction(
        ...     skill_name="dormant-detection",
        ...     tenant_id="acme-corp",
        ...     domain=Domain.REVENUE,
        ...     expected_signal=0.15,
        ...     expected_baseline=1.0,
        ...     guidance=guidance
        ... )
        >>> 
        >>> # ... run skill ...
        >>> 
        >>> # Record outcome
        >>> result = engine.record_outcome(
        ...     prediction_id=pred_id,
        ...     observed_signal=0.18,
        ...     goal_completed=True,
        ...     business_impact=5000.0
        ... )
        >>> print(f"Prediction error: {result['prediction_error']}")
    
    Thread Safety:
        The IntelligenceEngine itself is stateless regarding thread safety,
        but delegates to thread-safe components. Working memory and all
        persistent stores use locking for thread-safe operations.
    """
    
    def __init__(self, firebase_client: Optional["FirebaseClient"] = None):
        """
        Initialize the Intelligence Engine.
        
        Args:
            firebase_client: Optional storage client for persistent memory.
                If not provided, auto-selects: Supabase when SUPABASE_URL is
                set, otherwise Firebase, otherwise in-memory only.
        """
        if firebase_client is None:
            firebase_client = _auto_select_storage()
        
        self._firebase = firebase_client
        self.storage = firebase_client
        
        # Initialize memory layers
        self.working = WorkingMemory(firebase_client=firebase_client)
        self.episodic = EpisodicMemoryStore(firebase_client)
        self.semantic = SemanticMemoryStore(firebase_client)
        self.procedural = ProceduralMemoryStore(firebase_client)
        
        # Initialize consolidation manager
        self._consolidation = MemoryConsolidationManager(
            episodic_store=self.episodic,
            semantic_store=self.semantic,
            procedural_store=self.procedural,
        )
        
        # Initialize learning components. Pass the episodic store so the
        # predictor can supplement pattern-based reasoning with recent
        # observed outcomes (Phase 3 hygiene fix — closes the loop
        # between semantic predictions and episodic evidence).
        self.predictor = Predictor(
            semantic_store=self.semantic,
            procedural_store=self.procedural,
            episodic_store=self.episodic,
        )
        # Initialize shadow testing (before Learner so it can be injected)
        self._shadow = ShadowManager(
            firebase_client=firebase_client,
            semantic_store=self.semantic,
        )

        self.learner = Learner(
            episodic_store=self.episodic,
            semantic_store=self.semantic,
            shadow_manager=self._shadow,
        )

        # Initialize accuracy scorer and gold standard validator
        self._accuracy_scorer = AccuracyScorer(
            episodic_store=self.episodic,
            firebase_client=firebase_client,
        )
        self._gold_validator = GoldStandardValidator(
            firebase_client=firebase_client,
        )

        # Initialize domain adapters
        self._adapters: Dict[Domain, BaseDomainAdapter] = {
            Domain.CONTENT: ContentAdapter(),
            Domain.REVENUE: RevenueAdapter(),
            Domain.HEALTH: HealthAdapter(),
            Domain.CAMPAIGN: CampaignAdapter(),
        }

        # Load shadow-promoted channel timing overrides from Firebase
        if firebase_client is not None:
            try:
                from .adapters.channels import load_channel_overrides_from_firebase
                load_channel_overrides_from_firebase(firebase_client)
            except Exception as e:
                logger.debug(f"Channel timing override load skipped: {e}")
        
        logger.info("IntelligenceEngine initialized")

    @property
    def consolidation_manager(self) -> MemoryConsolidationManager:
        return self._consolidation

    @property
    def episodic_memory(self) -> EpisodicMemoryStore:
        return self.episodic

    @property
    def semantic_memory(self) -> SemanticMemoryStore:
        return self.semantic

    @property
    def shadow_manager(self) -> ShadowManager:
        return self._shadow

    @property
    def accuracy_scorer(self) -> AccuracyScorer:
        return self._accuracy_scorer

    @property
    def gold_validator(self) -> GoldStandardValidator:
        return self._gold_validator

    def get_guidance(
        self,
        skill_name: str,
        tenant_id: str,
        domain: Domain,
        context: Optional[Dict[str, Any]] = None,
    ) -> Guidance:
        """
        Get guidance for skill execution based on historical patterns.
        
        Queries semantic and procedural memory to find relevant patterns,
        then generates guidance with predictions and recommendations.
        
        Args:
            skill_name: Name of the skill requesting guidance
            tenant_id: Tenant/client identifier
            domain: Business domain for the skill
            context: Optional context dict with current conditions
        
        Returns:
            Guidance object containing:
            - predicted_signal: Expected outcome signal
            - predicted_baseline: Expected baseline value
            - confidence: Confidence in prediction (0-1)
            - recommendations: Suggested parameter values
            - patterns_used: IDs of patterns that informed guidance
            - is_exploration: Whether this is an exploratory recommendation
        
        Example:
            >>> guidance = engine.get_guidance(
            ...     skill_name="email-optimizer",
            ...     tenant_id="acme-corp",
            ...     domain=Domain.CONTENT,
            ...     context={"segment": "enterprise", "time_of_day": "morning"}
            ... )
            >>> print(f"Predicted signal: {guidance.predicted_signal}")
            >>> print(f"Confidence: {guidance.confidence}")
        """
        return self.predictor.get_guidance(
            skill_name=skill_name,
            tenant_id=tenant_id,
            domain=domain,
            context=context or {},
        )
    
    def register_prediction(
        self,
        skill_name: str,
        tenant_id: str,
        domain: Domain,
        expected_signal: float,
        expected_baseline: float,
        confidence: float = 0.5,
        context: Optional[Dict[str, Any]] = None,
        guidance: Optional[Guidance] = None,
    ) -> str:
        """
        Register a prediction before skill execution.
        
        Creates a prediction record in working memory that will be matched
        with an outcome after skill execution completes.
        
        Args:
            skill_name: Name of the skill making the prediction
            tenant_id: Tenant/client identifier
            domain: Business domain for scoring
            expected_signal: Predicted signal value (e.g., lift, rate)
            expected_baseline: Baseline for comparison
            confidence: Prediction confidence (0-1), defaults to 0.5
            context: Optional context dict capturing conditions at prediction time
            guidance: Optional Guidance object that informed this prediction
        
        Returns:
            prediction_id: Unique identifier to use when recording outcome
        
        Example:
            >>> pred_id = engine.register_prediction(
            ...     skill_name="dormant-detection",
            ...     tenant_id="acme-corp",
            ...     domain=Domain.REVENUE,
            ...     expected_signal=0.15,
            ...     expected_baseline=1.0,
            ...     confidence=0.7,
            ...     context={"segment": "enterprise"},
            ...     guidance=guidance
            ... )
        """
        # Extract patterns used from guidance if provided
        patterns_used = []
        is_exploration = False
        if guidance is not None:
            patterns_used = guidance.patterns_used or []
            is_exploration = guidance.is_exploration
            # Use guidance confidence if not explicitly provided
            if confidence == 0.5 and guidance.confidence != 0.5:
                confidence = guidance.confidence
        
        # Create prediction object
        prediction = Prediction(
            skill_name=skill_name,
            tenant_id=tenant_id,
            domain=domain,
            expected_signal=expected_signal,
            expected_baseline=expected_baseline,
            confidence=confidence,
            context=context or {},
            patterns_used=patterns_used,
            is_exploration=is_exploration,
        )
        
        # Register in working memory
        prediction_id = self.working.register_prediction(prediction)
        
        logger.debug(
            f"Registered prediction {prediction_id} for {skill_name}/{tenant_id}: "
            f"expected={expected_signal}/{expected_baseline}, confidence={confidence}"
        )
        
        return prediction_id
    
    def record_outcome(
        self,
        prediction_id: str,
        observed_signal: float,
        observed_baseline: Optional[float] = None,
        goal_completed: bool = False,
        business_impact: float = 0.0,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Record an observed outcome and trigger learning.
        
        This method:
        1. Retrieves the prediction from working memory
        2. Creates an outcome record with prediction error
        3. Stores the episode to episodic memory
        4. Triggers learning to update semantic patterns
        
        Args:
            prediction_id: ID returned from register_prediction()
            observed_signal: The actual signal value observed
            observed_baseline: The actual baseline (uses expected if None)
            goal_completed: Whether the skill achieved its goal
            business_impact: Monetary or business value impact
            metadata: Optional additional metadata about the outcome
        
        Returns:
            Result dict with keys:
            - success: Whether outcome was recorded successfully
            - episode_id: ID of the created episodic memory (if successful)
            - prediction_error: Difference between predicted and observed
            - learning_result: Results from the learning update
        
        Raises:
            ValueError: If prediction_id is not found in working memory
        
        Example:
            >>> result = engine.record_outcome(
            ...     prediction_id=pred_id,
            ...     observed_signal=0.18,
            ...     observed_baseline=1.0,
            ...     goal_completed=True,
            ...     business_impact=5000.0,
            ...     metadata={"contacts_reactivated": 42}
            ... )
            >>> if result["success"]:
            ...     print(f"Episode ID: {result['episode_id']}")
            ...     print(f"Prediction error: {result['prediction_error']}")
        """
        result = {
            "success": False,
            "episode_id": None,
            "prediction_error": 0.0,
            "learning_result": None,
        }
        
        # Get prediction from working memory
        prediction = self.working.get_prediction(prediction_id)
        if prediction is None:
            logger.warning(f"Prediction {prediction_id} not found in working memory")
            raise ValueError(f"Prediction {prediction_id} not found")
        
        # Use expected baseline if observed not provided
        if observed_baseline is None:
            observed_baseline = prediction.expected_baseline
        
        # Create outcome object
        outcome = Outcome(
            prediction_id=prediction_id,
            observed_signal=observed_signal,
            observed_baseline=observed_baseline,
            goal_completed=goal_completed,
            business_impact=business_impact,
            metadata=metadata or {},
        )
        
        # Complete prediction in working memory (creates EpisodicMemory)
        episode = self.working.complete_prediction(prediction_id, outcome)
        
        if episode is None:
            logger.error(f"Failed to complete prediction {prediction_id}")
            return result
        
        # Store episode to episodic memory
        try:
            self.episodic.store(episode=episode)
            result["episode_id"] = episode.episode_id
            result["prediction_error"] = episode.outcome.prediction_error
            
            logger.debug(
                f"Stored episode {episode.episode_id} for {prediction.skill_name}/{prediction.tenant_id}, "
                f"error={episode.outcome.prediction_error:.4f}"
            )
        except Exception as e:
            logger.error(f"Failed to store episode: {e}")
            # Continue with learning even if storage fails
        
        # Trigger learning from the outcome
        try:
            learning_result = self.learner.learn_from_outcome(
                prediction=prediction,
                outcome=outcome,
            )
            result["learning_result"] = learning_result
            
            logger.debug(f"Learning complete: {learning_result}")
        except Exception as e:
            logger.error(f"Learning failed: {e}")
            result["learning_result"] = {"error": str(e)}
        
        result["success"] = True
        return result
    
    def score(
        self,
        domain: Domain,
        event: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None,
    ) -> ScoringResult:
        """
        Calculate a domain-specific score for an event.
        
        Uses the appropriate domain adapter to apply domain-specific
        scoring logic (signal extraction, baseline calculation, context
        multipliers, etc.).
        
        Args:
            domain: Business domain for scoring
            event: Event data dict containing the raw metrics
            context: Optional context for score adjustment
        
        Returns:
            ScoringResult with:
            - signal: Extracted signal value
            - baseline: Calculated baseline
            - score: Final score (signal/baseline * adjustments)
            - confidence: Confidence in the score
            - metadata: Additional scoring details
        
        Example:
            >>> result = engine.score(
            ...     domain=Domain.REVENUE,
            ...     event={
            ...         "deal_amount": 50000,
            ...         "days_in_stage": 15,
            ...         "close_probability": 0.6
            ...     },
            ...     context={"industry": "saas"}
            ... )
            >>> print(f"Score: {result.score}")
        """
        context = context or {}
        
        # Get domain adapter
        adapter = self._adapters.get(domain)
        
        if adapter is not None:
            return adapter.calculate_score(event=event, context=context)
        
        # Fallback to generic scoring
        logger.debug(f"No adapter for domain {domain}, using generic scoring")
        
        # Generic scoring: look for common signal/baseline fields
        signal = event.get("signal", event.get("value", event.get("metric", 0.0)))
        baseline = event.get("baseline", event.get("expected", 1.0))
        
        if baseline == 0:
            baseline = 1.0
        
        score = signal / baseline
        
        return ScoringResult(
            signal=float(signal),
            baseline=float(baseline),
            score=score,
            confidence=0.5,
            metadata={"domain": domain.value, "method": "generic"},
        )
    
    def run_consolidation(
        self,
        tenant_id: Optional[str] = None,
    ) -> Dict[str, int]:
        """
        Run memory consolidation cycle.
        
        Consolidation promotes memories from lower to higher memory layers:
        - Decays episodic memories over time
        - Promotes decayed episodes to semantic patterns
        - Archives stale semantic patterns
        - Promotes cross-skill patterns to procedural knowledge
        
        This should be run periodically (e.g., daily) to maintain
        memory health and enable long-term learning.
        
        Args:
            tenant_id: Optional tenant to consolidate. If None, all tenants.
        
        Returns:
            Statistics dict with keys:
            - episodic_decayed: Episodes with decay applied
            - episodes_consolidated: Episodes promoted to semantic
            - patterns_created: New semantic patterns created
            - patterns_updated: Existing patterns updated
            - patterns_archived: Stale patterns archived
            - procedural_created: New procedural knowledge entries
        
        Example:
            >>> stats = engine.run_consolidation(tenant_id="acme-corp")
            >>> print(f"Consolidated {stats['episodes_consolidated']} episodes")
            >>> print(f"Created {stats['patterns_created']} new patterns")
        """
        logger.info(f"Running consolidation for tenant: {tenant_id or 'all'}")
        
        return self._consolidation.run_consolidation_cycle(tenant_id=tenant_id)
    
    def record_user_feedback(
        self,
        prediction_id: str,
        user_rating: float,
        user_correction: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Record user feedback on a prediction to update pattern confidence.
        
        This method allows users to provide explicit feedback on predictions,
        which updates the confidence of patterns that contributed to the prediction.
        User feedback has a stronger weight than automated outcome tracking
        because it represents explicit human judgment.
        
        Use cases:
        - User approves/rejects skill output (updates confidence positively/negatively)
        - User provides correction text (stored for future learning)
        - User rates quality on a scale (adjusts pattern confidence proportionally)
        
        Args:
            prediction_id: ID of the prediction to provide feedback on.
                Must be a prediction that was registered via register_prediction()
                and still exists in working memory OR has been stored in episodic memory.
            user_rating: User rating from 0.0 (completely wrong) to 1.0 (perfect).
                - 0.0-0.3: Strong negative signal, significantly decreases confidence
                - 0.3-0.5: Mild negative signal
                - 0.5: Neutral, minimal confidence change
                - 0.5-0.7: Mild positive signal
                - 0.7-1.0: Strong positive signal, significantly increases confidence
            user_correction: Optional text explaining what was wrong or how to improve.
                Stored in episodic memory for future pattern extraction.
        
        Returns:
            Dict with keys:
            - success: bool - Whether feedback was recorded
            - patterns_updated: int - Number of patterns whose confidence was updated
            - prediction_found: bool - Whether the prediction was found
            - confidence_delta: float - Average confidence change applied
            - message: str - Human-readable status message
        
        Example:
            >>> # User approves a prediction - strong positive signal
            >>> result = engine.record_user_feedback(
            ...     prediction_id="pred-abc123",
            ...     user_rating=0.9
            ... )
            >>> print(f"Updated {result['patterns_updated']} patterns")
            
            >>> # User rejects with correction - negative signal with context
            >>> result = engine.record_user_feedback(
            ...     prediction_id="pred-xyz789",
            ...     user_rating=0.2,
            ...     user_correction="The tone was too formal for this audience"
            ... )
        """
        result = {
            "success": False,
            "patterns_updated": 0,
            "prediction_found": False,
            "confidence_delta": 0.0,
            "message": "",
        }
        
        # Step 1: Try to find prediction in working memory
        prediction = self.working.get_prediction(prediction_id)
        
        # Step 2: If not in working memory, check recent outcomes
        if prediction is None:
            recent = self.working.get_recent_outcomes()
            for episode in recent:
                if episode.prediction.prediction_id == prediction_id:
                    prediction = episode.prediction
                    break
        
        if prediction is None:
            result["message"] = f"Prediction {prediction_id} not found in working or recent memory"
            logger.warning(result["message"])
            return result
        
        result["prediction_found"] = True
        
        # Step 3: Calculate confidence adjustment based on rating
        # user_rating is 0-1, we convert to a success probability for Bayesian update
        # Ratings < 0.5 are treated as failures, >= 0.5 as successes
        # The strength of the signal is proportional to distance from 0.5
        is_success = user_rating >= 0.5
        
        # Step 4: Update patterns that contributed to this prediction
        patterns_used = prediction.patterns_used or []
        total_delta = 0.0
        
        for pattern_id in patterns_used:
            try:
                # Get pattern from semantic store
                pattern = self.semantic.get_pattern(pattern_id, prediction.domain)
                if pattern is None:
                    logger.debug(f"Pattern {pattern_id} not found for feedback update")
                    continue
                
                old_confidence = pattern.confidence
                
                # Update pattern using semantic store's update method
                # user_rating serves as the "observed_ratio" for learning
                self.semantic.update_from_outcome(
                    pattern_id=pattern_id,
                    domain=prediction.domain,
                    success=is_success,
                    observed_ratio=user_rating,  # Use rating as observed value
                )
                
                # Calculate confidence delta (approximate, pattern may have been updated)
                # For strong ratings (close to 0 or 1), apply additional reinforcement
                if user_rating >= 0.8 or user_rating <= 0.2:
                    # Strong signal - apply extra update
                    self.semantic.update_from_outcome(
                        pattern_id=pattern_id,
                        domain=prediction.domain,
                        success=is_success,
                        observed_ratio=user_rating,
                    )
                
                result["patterns_updated"] += 1
                total_delta += (0.1 if is_success else -0.1)  # Approximate delta
                
                logger.debug(
                    f"Updated pattern {pattern_id} from user feedback: "
                    f"rating={user_rating}, success={is_success}"
                )
                
            except Exception as e:
                logger.error(f"Failed to update pattern {pattern_id} from feedback: {e}")
        
        # Step 5: Store feedback in episodic memory if correction provided
        if user_correction:
            try:
                # Create an episodic record with feedback metadata
                from datetime import datetime, timezone
                
                feedback_outcome = Outcome(
                    prediction_id=prediction_id,
                    observed_signal=user_rating,
                    observed_baseline=1.0,
                    goal_completed=is_success,
                    business_impact=0.0,
                    metadata={
                        "feedback_type": "user_correction",
                        "user_correction": user_correction,
                        "user_rating": user_rating,
                        "recorded_at": datetime.now(timezone.utc).isoformat(),
                    },
                )
                
                episode = EpisodicMemory(
                    prediction=prediction,
                    outcome=feedback_outcome,
                )
                
                self.episodic.store(episode=episode)
                
                logger.debug(
                    f"Stored user feedback episode for prediction {prediction_id}"
                )
                
            except Exception as e:
                logger.error(f"Failed to store feedback episode: {e}")
        
        # Step 6: Compute average confidence delta
        if result["patterns_updated"] > 0:
            result["confidence_delta"] = total_delta / result["patterns_updated"]
        
        result["success"] = True
        result["message"] = (
            f"Recorded feedback (rating={user_rating}) for {result['patterns_updated']} patterns"
        )
        
        logger.info(result["message"])
        return result
    
    def clear_session(self) -> None:
        """
        Clear working memory (session state).
        
        This removes:
        - All active predictions
        - All recent outcomes
        - All session context
        
        Does NOT affect persistent memory (episodic, semantic, procedural).
        
        Use when starting a new session or resetting state between runs.
        
        Example:
            >>> engine.clear_session()
        """
        self.working.clear()
        logger.debug("Working memory cleared")
    
    def __repr__(self) -> str:
        return (
            f"IntelligenceEngine("
            f"working={self.working}, "
            f"adapters={list(self._adapters.keys())})"
        )


__all__ = [
    # Main interface
    "IntelligenceEngine",
    
    # Core types
    "Domain",
    "MemoryLayer",
    "Prediction",
    "Outcome",
    "Guidance",
    "EpisodicMemory",
    "TrajectoryPoint",
    "SemanticPattern",
    "ProceduralKnowledge",
    
    # Scoring
    "ScoringResult",
    
    # Memory stores
    "WorkingMemory",
    "WorkingMemoryConfig",
    "EpisodicMemoryStore",
    "EpisodicMemoryConfig",
    "SemanticMemoryStore",
    "SemanticMemoryConfig",
    "ProceduralMemoryStore",
    "ProceduralMemoryConfig",
    
    # Consolidation
    "ConsolidationConfig",
    "MemoryConsolidationManager",
    
    # Learning
    "Predictor",
    "ExplorationConfig",
    "Learner",
    "LearningConfig",

    # Shadow testing & evaluation (V4)
    "ShadowConfig",
    "ShadowManager",
    "AccuracyScorer",
    "GoldStandardValidator",

    # Domain adapters
    "BaseDomainAdapter",
    "ContentAdapter",
    "RevenueAdapter",
    "HealthAdapter",
    "CampaignAdapter",
]
