#!/usr/bin/env python3
"""Phase 1 backfill — clean up broken state left by the consolidation bugs.

Run this ONCE after deploying Phase 1 fixes. Safe to re-run (idempotent).

Actions
-------
1. Create the pending_outcomes table if missing (via schema/supabase_tables.sql).
   This must be run manually in the Supabase SQL editor — we print the SQL.

2. Backfill episodic_memory.prediction JSONB so skill_name / tenant_id /
   domain are embedded inside the blob, matching the top-level columns.
   After Phase 1 the code reads from either location, but a one-shot
   backfill normalises data on disk and makes downstream queries cheaper.

3. De-duplicate source_episodes arrays on semantic_patterns rows. The
   previous bug appended the same IDs every consolidation cycle, which
   inflated the array unbounded.

Usage
-----
    # Preview without writing:
    python scripts/backfill_phase_1.py --dry-run

    # Execute:
    python scripts/backfill_phase_1.py

    # Scope to one tenant:
    python scripts/backfill_phase_1.py --tenant-id acme
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv  # noqa: E402

load_dotenv()

logger = logging.getLogger("backfill_phase_1")


PENDING_OUTCOMES_SQL = """
-- Run this in the Supabase SQL editor if pending_outcomes doesn't exist yet.
-- Also defined in schema/supabase_tables.sql.
CREATE TABLE IF NOT EXISTS pending_outcomes (
  prediction_id             TEXT PRIMARY KEY,
  tracking_id               TEXT,
  skill_name                TEXT,
  client_id                 TEXT NOT NULL,
  channel_id                TEXT,
  module_id                 TEXT,
  run_id                    TEXT,
  node_id                   TEXT,
  status                    TEXT DEFAULT 'pending',
  prediction                JSONB DEFAULT '{}',
  generation_score          FLOAT,
  delivery_metadata         JSONB DEFAULT '{}',
  platform_config           JSONB DEFAULT '{}',
  checkpoints               JSONB DEFAULT '[]',
  checkpoint_schedule       JSONB DEFAULT '[]',
  projection_classification TEXT,
  composite_score           FLOAT,
  created_at                TIMESTAMPTZ DEFAULT now(),
  due_24h                   TIMESTAMPTZ,
  due_7d                    TIMESTAMPTZ,
  closed_at                 TIMESTAMPTZ,
  updated_at                TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_pending_outcomes_client_status
  ON pending_outcomes(client_id, status);
CREATE INDEX IF NOT EXISTS idx_pending_outcomes_due_24h
  ON pending_outcomes(due_24h) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_pending_outcomes_due_7d
  ON pending_outcomes(due_7d) WHERE status = 'checkpoint_24h';
CREATE INDEX IF NOT EXISTS idx_pending_outcomes_closed_at
  ON pending_outcomes(closed_at DESC) WHERE status = 'closed';
""".strip()


def ensure_pending_outcomes_table(db) -> None:
    """Check the pending_outcomes table exists; surface SQL if not."""
    try:
        db.table("pending_outcomes").select("prediction_id").limit(1).execute()
        logger.info("[1/3] pending_outcomes table exists — OK")
    except Exception as e:
        logger.error(
            "[1/3] pending_outcomes table does NOT exist: %s", e
        )
        logger.error(
            "[1/3] Run the following SQL in the Supabase SQL editor:\n%s",
            PENDING_OUTCOMES_SQL,
        )
        raise SystemExit(
            "pending_outcomes table is missing. See SQL above, apply, then re-run."
        )


def backfill_episodic_prediction_fields(
    db, *, tenant_id: str | None, dry_run: bool
) -> Dict[str, int]:
    """Embed skill_name / tenant_id / domain inside the prediction JSONB.

    After Phase 1, _doc_to_episode backfills these at read time, so this
    is not strictly required for correctness. We still normalise the data
    so any downstream consumers that look only at the JSONB (e.g. the
    Airtable publisher, debugging SQL, analytics dashboards) see a
    complete picture.
    """
    stats = {"scanned": 0, "updated": 0, "skipped": 0}
    page_size = 500
    offset = 0

    while True:
        q = (
            db.table("episodic_memory")
            .select("episode_id,tenant_id,skill_name,domain,prediction")
            .range(offset, offset + page_size - 1)
        )
        if tenant_id:
            q = q.eq("tenant_id", tenant_id)
        result = q.execute()
        rows = result.data or []
        if not rows:
            break

        for row in rows:
            stats["scanned"] += 1
            prediction = row.get("prediction") or {}
            if not isinstance(prediction, dict):
                stats["skipped"] += 1
                continue

            patch: Dict[str, Any] = {}
            if not prediction.get("skill_name") and row.get("skill_name"):
                patch["skill_name"] = row["skill_name"]
            if not prediction.get("tenant_id") and row.get("tenant_id"):
                patch["tenant_id"] = row["tenant_id"]
            if not prediction.get("domain") and row.get("domain"):
                patch["domain"] = row["domain"]

            if not patch:
                continue

            if dry_run:
                logger.info(
                    "[DRY-RUN] would patch episodic %s: +%s",
                    row["episode_id"], list(patch.keys()),
                )
                stats["updated"] += 1
                continue

            merged = {**prediction, **patch}
            db.table("episodic_memory").update(
                {"prediction": merged}
            ).eq("episode_id", row["episode_id"]).execute()
            stats["updated"] += 1

        offset += page_size
        if len(rows) < page_size:
            break

    logger.info(
        "[2/3] episodic_memory backfill: scanned=%d updated=%d skipped=%d",
        stats["scanned"], stats["updated"], stats["skipped"],
    )
    return stats


def dedupe_semantic_source_episodes(
    db, *, dry_run: bool
) -> Dict[str, int]:
    """Collapse duplicate episode IDs in semantic_patterns.source_episodes."""
    stats = {"scanned": 0, "updated": 0, "duplicates_removed": 0}

    result = (
        db.table("semantic_patterns")
        .select("pattern_id,source_episodes")
        .execute()
    )
    rows = result.data or []

    for row in rows:
        stats["scanned"] += 1
        source = row.get("source_episodes") or []
        if not source:
            continue
        unique: List[str] = []
        seen = set()
        for eid in source:
            if eid in seen:
                continue
            seen.add(eid)
            unique.append(eid)

        diff = len(source) - len(unique)
        if diff == 0:
            continue

        stats["duplicates_removed"] += diff
        if dry_run:
            logger.info(
                "[DRY-RUN] pattern %s has %d duplicates (%d → %d)",
                row["pattern_id"], diff, len(source), len(unique),
            )
            stats["updated"] += 1
            continue

        db.table("semantic_patterns").update(
            {"source_episodes": unique}
        ).eq("pattern_id", row["pattern_id"]).execute()
        logger.info(
            "Deduped pattern %s: %d → %d episode IDs",
            row["pattern_id"], len(source), len(unique),
        )
        stats["updated"] += 1

    logger.info(
        "[3/3] semantic_patterns dedup: scanned=%d updated=%d duplicates_removed=%d",
        stats["scanned"], stats["updated"], stats["duplicates_removed"],
    )
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description="Phase 1 backfill")
    parser.add_argument("--dry-run", action="store_true", help="Preview only")
    parser.add_argument("--tenant-id", help="Scope episodic backfill to one tenant")
    parser.add_argument(
        "--skip-episodic", action="store_true",
        help="Skip episodic_memory backfill (step 2).",
    )
    parser.add_argument(
        "--skip-semantic", action="store_true",
        help="Skip semantic_patterns dedup (step 3).",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    if not os.environ.get("SUPABASE_URL"):
        logger.error("SUPABASE_URL is not set. Aborting.")
        return 2

    from lib.supabase_client import get_supabase
    db = get_supabase()
    logger.info("Connected to Supabase (dry_run=%s)", args.dry_run)

    ensure_pending_outcomes_table(db)

    summary: Dict[str, Any] = {"dry_run": args.dry_run}

    if not args.skip_episodic:
        summary["episodic"] = backfill_episodic_prediction_fields(
            db, tenant_id=args.tenant_id, dry_run=args.dry_run,
        )

    if not args.skip_semantic:
        summary["semantic"] = dedupe_semantic_source_episodes(
            db, dry_run=args.dry_run,
        )

    logger.info("Backfill summary: %s", json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
