"""
MH1 Episodic Memory Store

Firebase-persisted episodic memory with temporal decay.
Stores specific experiences (prediction + outcome pairs) that decay over time
and can be consolidated into semantic patterns.

Firebase path: system/intelligence/episodic/{tenant_id}/{skill_name}/{episode_id}
Archive path: system/intelligence/archive/{tenant_id}/{skill_name}/{episode_id}
"""

import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ..types import Domain, EpisodicMemory, Prediction, Outcome, normalize_domain

logger = logging.getLogger(__name__)


@dataclass
class EpisodicMemoryConfig:
    """Configuration for episodic memory behavior."""
    decay_rate: float = 0.95              # Per-day decay multiplier
    relevance_threshold: float = 0.3      # Below this, ready for consolidation
    max_episodes_per_skill: int = 1000    # Cap per skill to prevent unbounded growth
    ttl_days: int = 90                    # Days before archival
    consolidation_threshold: int = 10     # Episodes needed before pattern extraction


class EpisodicMemoryStore:
    """
    Thread-safe Firebase-backed episodic memory store.
    
    Manages storage, retrieval, decay, and archival of episodic memories.
    Episodes decay over time based on age and retrieval frequency.
    """
    
    _collection_base = "system/intelligence/episodic"
    _archive_base = "system/intelligence/archive"
    
    def __init__(
        self,
        firebase_client: Any,
        config: Optional[EpisodicMemoryConfig] = None
    ):
        """
        Initialize the episodic memory store.
        
        Args:
            firebase_client: Firebase client with set_document, get_document, 
                           query, update_document, delete_document, get_collection methods
            config: Configuration for decay, thresholds, and limits
        """
        self._firebase = firebase_client
        self._config = config or EpisodicMemoryConfig()
        self._lock = threading.RLock()
    
    def _get_collection_path(self, tenant_id: str, skill_name: str) -> str:
        """
        Get the Firebase collection path for a tenant's skill episodes.
        
        Args:
            tenant_id: Tenant identifier
            skill_name: Name of the skill
            
        Returns:
            Collection path string
        """
        return f"{self._collection_base}/{tenant_id}/{skill_name}"
    
    def _get_archive_path(self, tenant_id: str, skill_name: str) -> str:
        """
        Get the Firebase archive path for archived episodes.
        
        Args:
            tenant_id: Tenant identifier
            skill_name: Name of the skill
            
        Returns:
            Archive collection path string
        """
        return f"{self._archive_base}/{tenant_id}/{skill_name}"
    
    def _calculate_age_days(self, created_at: str) -> float:
        """Calculate age in days from ISO timestamp."""
        try:
            created_dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
            now = datetime.now(timezone.utc)
            delta = now - created_dt
            return delta.total_seconds() / 86400  # Convert to days
        except (ValueError, AttributeError):
            return 0.0
    
    def _apply_decay(self, episode: EpisodicMemory) -> float:
        """
        Calculate decayed weight for an episode.
        
        Weight decays exponentially: weight *= decay_rate ^ age_days
        
        Args:
            episode: The episode to calculate decay for
            
        Returns:
            Decayed weight value
        """
        age_days = self._calculate_age_days(episode.created_at)
        decayed_weight = episode.weight * (self._config.decay_rate ** age_days)
        return max(0.0, min(1.0, decayed_weight))  # Clamp to [0, 1]
    
    def store(self, episode: EpisodicMemory) -> str:
        """
        Store an episode to Firebase.
        
        Args:
            episode: The EpisodicMemory to store
            
        Returns:
            The episode_id of the stored episode
        """
        with self._lock:
            tenant_id = episode.prediction.tenant_id
            skill_name = episode.prediction.skill_name
            
            if not tenant_id or not skill_name:
                raise ValueError("Episode must have tenant_id and skill_name in prediction")
            
            collection_path = self._get_collection_path(tenant_id, skill_name)
            doc_data = episode.to_dict()
            
            # Store using available firebase method
            if hasattr(self._firebase, "set_document"):
                # Parse collection path - Firebase expects collection/doc format
                parts = collection_path.split("/")
                if len(parts) >= 3:
                    # system/intelligence/episodic/tenant/skill -> use full path as collection
                    self._firebase.set_document(
                        collection=collection_path,
                        doc_id=episode.episode_id,
                        data=doc_data
                    )
                else:
                    self._firebase.set_document(
                        collection=collection_path,
                        doc_id=episode.episode_id,
                        data=doc_data
                    )
            else:
                logger.warning("Firebase client missing set_document method")
                raise AttributeError("firebase_client missing set_document method")
            
            logger.debug(f"Stored episode {episode.episode_id} at {collection_path}")
            return episode.episode_id
    
    def retrieve(
        self,
        tenant_id: str,
        skill_name: str,
        domain: Optional[Domain] = None,
        min_weight: Optional[float] = None,
        limit: int = 100
    ) -> List[EpisodicMemory]:
        """
        Retrieve episodes from Firebase with filtering and decay application.
        
        Applies temporal decay on retrieval and updates retrieval metadata.
        
        Args:
            tenant_id: Tenant identifier
            skill_name: Name of the skill
            domain: Optional domain filter
            min_weight: Optional minimum weight filter (after decay)
            limit: Maximum number of episodes to return
            
        Returns:
            List of EpisodicMemory objects with decayed weights
        """
        with self._lock:
            collection_path = self._get_collection_path(tenant_id, skill_name)
            
            # Build filters
            filters = []
            if domain is not None:
                filters.append(("prediction.domain", "==", domain.value))
            
            # Query Firebase
            try:
                if hasattr(self._firebase, "query") and filters:
                    docs = self._firebase.query(
                        collection=collection_path,
                        filters=filters,
                        limit=limit * 2,  # Over-fetch to account for weight filtering
                        order_by="created_at",
                        order_direction="DESCENDING"
                    )
                elif hasattr(self._firebase, "get_collection"):
                    docs = self._firebase.get_collection(
                        collection=collection_path,
                        limit=limit * 2,
                        order_by="created_at",
                        order_direction="DESCENDING"
                    )
                else:
                    logger.warning("Firebase client missing query/get_collection methods")
                    return []
            except Exception as e:
                logger.error(f"Error querying episodes: {e}")
                return []
            
            if not docs:
                return []
            
            # Convert to episodes and apply decay
            episodes = []
            now_iso = datetime.now(timezone.utc).isoformat()
            
            for doc in docs:
                episode = self._doc_to_episode(doc)
                if episode is None:
                    continue
                
                # Apply temporal decay
                episode.weight = self._apply_decay(episode)
                
                # Filter by min_weight after decay
                if min_weight is not None and episode.weight < min_weight:
                    continue
                
                # Filter by domain if nested query didn't work
                if domain is not None and episode.prediction.domain != domain:
                    continue
                
                episodes.append(episode)
                
                # Update retrieval metadata asynchronously
                self._update_retrieval_metadata(
                    tenant_id, skill_name, episode.episode_id,
                    episode.retrieval_count + 1, now_iso
                )
                
                if len(episodes) >= limit:
                    break
            
            return episodes
    
    def _update_retrieval_metadata(
        self,
        tenant_id: str,
        skill_name: str,
        episode_id: str,
        retrieval_count: int,
        last_retrieved_at: str
    ):
        """Update retrieval count and timestamp for an episode."""
        try:
            collection_path = self._get_collection_path(tenant_id, skill_name)
            
            if hasattr(self._firebase, "update_document"):
                self._firebase.update_document(
                    collection=collection_path,
                    doc_id=episode_id,
                    data={
                        "retrieval_count": retrieval_count,
                        "last_retrieved_at": last_retrieved_at
                    }
                )
        except Exception as e:
            # Non-critical operation, log and continue
            logger.debug(f"Failed to update retrieval metadata for {episode_id}: {e}")
    
    def _list_tenants(self) -> List[str]:
        """List all tenant IDs with episodic memories.

        Tries list_subcollections first (works with both SupabaseStorage
        and FirebaseClient).  Falls back to raw Firestore SDK when the
        adapter doesn't support list_subcollections or returns empty.
        """
        try:
            if hasattr(self._firebase, 'list_subcollections'):
                tenants = self._firebase.list_subcollections(self._collection_base)
                if tenants:
                    logger.debug(f"_list_tenants: found {len(tenants)} tenants via list_subcollections")
                    return tenants

            if hasattr(self._firebase, '_get_client'):
                with self._firebase._get_client() as client:
                    episodic_ref = (
                        client.collection("system")
                        .document("intelligence")
                        .collection("episodic")
                    )
                    tenants = [doc.id for doc in episodic_ref.list_documents()]
                    logger.debug(f"_list_tenants: found {len(tenants)} tenants via list_documents()")
                    return tenants
        except Exception as e:
            logger.error(f"_list_tenants FAILED: {e}", exc_info=True)
        return []

    def _list_skills_for_tenant(self, tenant_id: str) -> List[str]:
        """List all skill subcollections for a tenant.

        Tries list_subcollections first (works with SupabaseStorage and
        FirebaseClient).  Falls back to raw Firestore SDK when unavailable.
        """
        doc_path = f"system/intelligence/episodic/{tenant_id}"
        try:
            if hasattr(self._firebase, 'list_subcollections'):
                skills = self._firebase.list_subcollections(doc_path)
                if skills:
                    logger.debug(f"_list_skills_for_tenant({tenant_id}): {len(skills)} via helper")
                    return skills

            if hasattr(self._firebase, '_get_client'):
                with self._firebase._get_client() as client:
                    tenant_ref = (
                        client.collection("system")
                        .document("intelligence")
                        .collection("episodic")
                        .document(tenant_id)
                    )
                    skills = [col.id for col in tenant_ref.collections()]
                    logger.debug(f"_list_skills_for_tenant({tenant_id}): {len(skills)} via SDK")
                    return skills
        except Exception as e:
            logger.error(f"_list_skills_for_tenant({tenant_id}) FAILED: {e}", exc_info=True)
        return []

    def decay_all(self, tenant_id: Optional[str] = None) -> Dict[str, int]:
        """
        Apply decay to all episodes and handle archival.
        
        Args:
            tenant_id: Optional tenant filter. If None, processes all tenants.
            
        Returns:
            Statistics dict with keys: decayed, to_consolidate, archived
        """
        with self._lock:
            stats = {"decayed": 0, "to_consolidate": 0, "archived": 0}

            if not hasattr(self._firebase, "get_collection"):
                logger.warning("Firebase client missing get_collection method")
                return stats

            try:
                tenants = [tenant_id] if tenant_id else self._list_tenants()
                if not tenants:
                    logger.info("No tenants found for decay")
                    return stats

                for tid in tenants:
                    skills = self._list_skills_for_tenant(tid)
                    for skill in skills:
                        skill_stats = self._decay_skill_episodes(tid, skill)
                        stats["decayed"] += skill_stats.get("decayed", 0)
                        stats["to_consolidate"] += skill_stats.get("to_consolidate", 0)
                        stats["archived"] += skill_stats.get("archived", 0)

                logger.info(
                    f"Decay complete: {stats['decayed']} decayed, "
                    f"{stats['to_consolidate']} ready for consolidation, "
                    f"{stats['archived']} archived"
                )
            except Exception as e:
                logger.error(f"Error in decay_all: {e}")

            return stats

    def _decay_skill_episodes(self, tenant_id: str, skill_name: str) -> Dict[str, int]:
        """Apply decay to all episodes for a specific tenant/skill pair."""
        stats = {"decayed": 0, "to_consolidate": 0, "archived": 0}
        collection_path = self._get_collection_path(tenant_id, skill_name)

        try:
            docs = self._firebase.get_collection(collection=collection_path)
            if not docs:
                return stats

            for doc in docs:
                episode = self._doc_to_episode(doc)
                if episode is None:
                    continue

                age_days = self._calculate_age_days(episode.created_at)

                if age_days > self._config.ttl_days:
                    self._archive_episode(episode)
                    stats["archived"] += 1
                    continue

                new_weight = self._apply_decay(episode)
                if new_weight != episode.weight:
                    self._firebase.update_document(
                        collection_path, episode.episode_id,
                        {"weight": new_weight}
                    )
                    stats["decayed"] += 1

                if new_weight < self._config.relevance_threshold:
                    stats["to_consolidate"] += 1

        except Exception as e:
            logger.error(f"Error decaying {tenant_id}/{skill_name}: {e}")

        return stats
    
    def get_for_consolidation(
        self,
        tenant_id: str,
        skill_name: str,
        limit: Optional[int] = None
    ) -> List[EpisodicMemory]:
        """
        Get episodes ready for consolidation into semantic patterns.

        An episode is ready when it has not been consolidated yet AND either:
          (a) its decayed weight is below ``relevance_threshold``, OR
          (b) it is older than 7 days (age-based fallback so episodes that
              started with low weight or had decay issues still get picked up).

        Args:
            tenant_id: Tenant identifier
            skill_name: Name of the skill
            limit: Maximum episodes to return (default: consolidation_threshold)

        Returns:
            List of episodes ready for consolidation
        """
        with self._lock:
            if limit is None:
                limit = self._config.consolidation_threshold

            collection_path = self._get_collection_path(tenant_id, skill_name)
            AGE_FALLBACK_DAYS = 7.0

            try:
                # Fetch all unconsolidated episodes.  We avoid querying for
                # ``consolidated_at == null`` because Firestore treats missing
                # fields and explicit null differently; instead we fetch all
                # and filter in memory.
                docs = None
                if hasattr(self._firebase, "get_collection"):
                    docs = self._firebase.get_collection(
                        collection=collection_path,
                        limit=limit * 5,
                        order_by="created_at",
                        order_direction="ASCENDING"
                    )

                if not docs:
                    logger.debug(f"get_for_consolidation: no docs in {collection_path}")
                    return []

                episodes = []
                for doc in docs:
                    episode = self._doc_to_episode(doc)
                    if episode is None:
                        continue

                    # Skip already consolidated (field present and non-null)
                    if episode.consolidated_at is not None:
                        continue

                    episode.weight = self._apply_decay(episode)
                    age_days = self._calculate_age_days(episode.created_at)

                    # Ready if weight below threshold OR age fallback
                    if episode.weight < self._config.relevance_threshold or age_days >= AGE_FALLBACK_DAYS:
                        episodes.append(episode)
                        if len(episodes) >= limit:
                            break

                logger.debug(
                    f"get_for_consolidation({tenant_id}/{skill_name}): "
                    f"{len(episodes)} ready out of {len(docs)} total docs"
                )
                return episodes

            except Exception as e:
                logger.error(f"get_for_consolidation ERROR: {e}", exc_info=True)
                return []
    
    def mark_consolidated(
        self,
        episode_id: str,
        tenant_id: str,
        skill_name: str
    ):
        """
        Mark an episode as consolidated into semantic memory.
        
        Args:
            episode_id: The episode ID to mark
            tenant_id: Tenant identifier
            skill_name: Name of the skill
        """
        with self._lock:
            collection_path = self._get_collection_path(tenant_id, skill_name)
            now_iso = datetime.now(timezone.utc).isoformat()
            
            try:
                if hasattr(self._firebase, "update_document"):
                    self._firebase.update_document(
                        collection=collection_path,
                        doc_id=episode_id,
                        data={"consolidated_at": now_iso}
                    )
                    logger.debug(f"Marked episode {episode_id} as consolidated")
                else:
                    logger.warning("Firebase client missing update_document method")
            except Exception as e:
                logger.error(f"Error marking episode consolidated: {e}")
    
    def _archive_episode(self, episode: EpisodicMemory):
        """
        Move an episode to the archive collection.
        
        Archives old episodes that have exceeded TTL.
        
        Args:
            episode: The episode to archive
        """
        with self._lock:
            tenant_id = episode.prediction.tenant_id
            skill_name = episode.prediction.skill_name
            
            if not tenant_id or not skill_name:
                logger.error("Cannot archive episode without tenant_id and skill_name")
                return
            
            active_path = self._get_collection_path(tenant_id, skill_name)
            archive_path = self._get_archive_path(tenant_id, skill_name)
            
            try:
                # Set archived timestamp
                episode.archived_at = datetime.now(timezone.utc).isoformat()
                doc_data = episode.to_dict()
                
                # Write to archive
                if hasattr(self._firebase, "set_document"):
                    self._firebase.set_document(
                        collection=archive_path,
                        doc_id=episode.episode_id,
                        data=doc_data
                    )
                else:
                    logger.warning("Firebase client missing set_document method")
                    return
                
                # Delete from active
                if hasattr(self._firebase, "delete_document"):
                    self._firebase.delete_document(
                        collection=active_path,
                        doc_id=episode.episode_id
                    )
                else:
                    logger.warning("Firebase client missing delete_document method")
                
                logger.info(f"Archived episode {episode.episode_id}")
                
            except Exception as e:
                logger.error(f"Error archiving episode {episode.episode_id}: {e}")
    
    def _doc_to_episode(self, doc: Dict[str, Any]) -> Optional[EpisodicMemory]:
        """
        Convert a Firebase document to an EpisodicMemory object.
        
        Handles nested Prediction and Outcome reconstruction.
        
        Args:
            doc: Firebase document dictionary
            
        Returns:
            EpisodicMemory object or None if conversion fails
        """
        if not doc:
            return None
        
        try:
            # Handle _id from Firebase metadata
            episode_id = doc.get("episode_id") or doc.get("_id")
            if not episode_id:
                logger.warning("Document missing episode_id")
                return None
            
            # Reconstruct Prediction
            prediction_data = doc.get("prediction", {})
            prediction = Prediction.from_dict(prediction_data) if prediction_data else Prediction()

            # Supabase stores skill_name / tenant_id / domain as top-level columns
            # while the Prediction JSONB blob often omits them (e.g. episodes
            # written by platform_ingestion). Backfill from the top-level
            # columns so consolidate_from_episodes doesn't see empty skill_name
            # and raise ValueError when storing a pattern.
            if not prediction.skill_name:
                prediction.skill_name = doc.get("skill_name", "")
            if not prediction.tenant_id:
                prediction.tenant_id = doc.get("tenant_id", "")

            # Only override domain when the JSONB blob didn't provide one.
            # ``Prediction.from_dict`` (via ``normalize_domain``) already
            # coerces unknown / legacy strings (e.g. "paid_media" from
            # platform_ingestion) to an enum value, but when the JSONB is
            # empty we still want to consult the top-level column so the
            # episode retains its original platform categorisation.
            jsonb_domain = prediction_data.get("domain") if isinstance(prediction_data, dict) else None
            if jsonb_domain in (None, ""):
                doc_domain = doc.get("domain")
                if doc_domain:
                    prediction.domain = normalize_domain(
                        doc_domain, default=Domain.GENERIC
                    )

            # Reconstruct Outcome
            outcome_data = doc.get("outcome", {})
            outcome = Outcome.from_dict(outcome_data) if outcome_data else Outcome()
            
            # Build EpisodicMemory
            episode = EpisodicMemory(
                episode_id=episode_id,
                prediction=prediction,
                outcome=outcome,
                weight=doc.get("weight", 1.0),
                retrieval_count=doc.get("retrieval_count", 0),
                last_retrieved_at=doc.get("last_retrieved_at"),
                created_at=doc.get("created_at", datetime.now(timezone.utc).isoformat()),
                consolidated_at=doc.get("consolidated_at"),
                archived_at=doc.get("archived_at"),
            )
            
            return episode
            
        except Exception as e:
            logger.error(f"Error converting document to episode: {e}")
            return None
    
    def get_episode(
        self,
        episode_id: str,
        tenant_id: str,
        skill_name: str
    ) -> Optional[EpisodicMemory]:
        """
        Get a single episode by ID.
        
        Args:
            episode_id: The episode ID
            tenant_id: Tenant identifier
            skill_name: Name of the skill
            
        Returns:
            EpisodicMemory or None if not found
        """
        with self._lock:
            collection_path = self._get_collection_path(tenant_id, skill_name)
            
            try:
                if hasattr(self._firebase, "get_document"):
                    doc = self._firebase.get_document(
                        collection=collection_path,
                        doc_id=episode_id
                    )
                    if doc:
                        episode = self._doc_to_episode(doc)
                        if episode:
                            episode.weight = self._apply_decay(episode)
                        return episode
                else:
                    logger.warning("Firebase client missing get_document method")
                
            except Exception as e:
                logger.error(f"Error getting episode {episode_id}: {e}")
            
            return None
    
    def delete_episode(
        self,
        episode_id: str,
        tenant_id: str,
        skill_name: str
    ) -> bool:
        """
        Delete an episode from the store.
        
        Args:
            episode_id: The episode ID to delete
            tenant_id: Tenant identifier
            skill_name: Name of the skill
            
        Returns:
            True if deleted, False otherwise
        """
        with self._lock:
            collection_path = self._get_collection_path(tenant_id, skill_name)
            
            try:
                if hasattr(self._firebase, "delete_document"):
                    self._firebase.delete_document(
                        collection=collection_path,
                        doc_id=episode_id
                    )
                    logger.debug(f"Deleted episode {episode_id}")
                    return True
                else:
                    logger.warning("Firebase client missing delete_document method")
                    
            except Exception as e:
                logger.error(f"Error deleting episode {episode_id}: {e}")
            
            return False
    
    def count_episodes(self, tenant_id: str, skill_name: str) -> int:
        """
        Count episodes for a skill (approximate).
        
        Args:
            tenant_id: Tenant identifier
            skill_name: Name of the skill
            
        Returns:
            Episode count
        """
        with self._lock:
            collection_path = self._get_collection_path(tenant_id, skill_name)
            
            try:
                if hasattr(self._firebase, "get_collection"):
                    docs = self._firebase.get_collection(
                        collection=collection_path,
                        limit=self._config.max_episodes_per_skill + 1
                    )
                    return len(docs) if docs else 0
                    
            except Exception as e:
                logger.error(f"Error counting episodes: {e}")
            
            return 0
    
    def cleanup_old_episodes(
        self,
        tenant_id: str,
        skill_name: str
    ) -> Dict[str, int]:
        """
        Archive episodes that have exceeded TTL.
        
        Args:
            tenant_id: Tenant identifier
            skill_name: Name of the skill
            
        Returns:
            Statistics dict with archived count
        """
        with self._lock:
            stats = {"archived": 0, "checked": 0}
            collection_path = self._get_collection_path(tenant_id, skill_name)
            
            try:
                if hasattr(self._firebase, "get_collection"):
                    docs = self._firebase.get_collection(
                        collection=collection_path,
                        order_by="created_at",
                        order_direction="ASCENDING"
                    )
                    
                    if not docs:
                        return stats
                    
                    for doc in docs:
                        stats["checked"] += 1
                        episode = self._doc_to_episode(doc)
                        if episode is None:
                            continue
                        
                        age_days = self._calculate_age_days(episode.created_at)
                        
                        if age_days > self._config.ttl_days:
                            self._archive_episode(episode)
                            stats["archived"] += 1
                    
            except Exception as e:
                logger.error(f"Error cleaning up old episodes: {e}")
            
            return stats


__all__ = [
    "EpisodicMemoryConfig",
    "EpisodicMemoryStore",
]
