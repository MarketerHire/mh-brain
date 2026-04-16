#!/usr/bin/env python3
"""Phase 2 backfill — normalise legacy domain strings on existing rows.

Phase 2 fixes normalise domain aliases (``paid_media`` → ``campaign``,
``email`` → ``content``, etc.) on the READ path via ``normalize_domain``.
This backfill updates the stored ``domain`` column on ``episodic_memory``
and ``semantic_patterns`` so the data on disk matches the enum values
the code now expects. It is safe to re-run; rows already in canonical
form are skipped.

Scope
-----
1. ``episodic_memory.domain`` — alias → canonical enum string.
2. ``episodic_memory.prediction.domain`` (JSONB) — same mapping.
3. ``episodic_memory.outcome`` — move ``metrics`` from the top level into
   ``metadata.metrics`` (Phase 2 write-side invariant). Preserves any
   existing ``metadata`` keys.
4. ``semantic_patterns.domain`` — alias → canonical enum string.

Usage
-----
    python scripts/backfill_phase_2.py --dry-run
    python scripts/backfill_phase_2.py
    python scripts/backfill_phase_2.py --tenant-id acme

Safe to run after the Phase 2 code is deployed. Order of operations:

    1. Deploy Phase 2 code (code handles both old and new shapes on read)
    2. Run this backfill (normalises existing data)
    3. Delete any Supabase views that depended on the old domain strings
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv  # noqa: E402

load_dotenv()

from lib.intelligence.types import Domain, normalize_domain  # noqa: E402

logger = logging.getLogger("backfill_phase_2")


def _canonical_domain(raw: Any) -> Optional[str]:
    """Return the canonical Domain string for ``raw``, or None to skip."""
    if not raw:
        return None
    resolved = normalize_domain(raw)
    canonical = resolved.value
    return canonical if canonical != str(raw).lower() else None


def backfill_episodic_domains(
    db, *, tenant_id: Optional[str], dry_run: bool
) -> Dict[str, int]:
    """Rewrite episodic_memory.domain (top-level + JSONB) + outcome shape."""
    stats = {"scanned": 0, "updated": 0, "skipped": 0}
    page_size = 500
    offset = 0

    while True:
        q = (
            db.table("episodic_memory")
            .select("episode_id,domain,prediction,outcome")
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
            patch: Dict[str, Any] = {}

            canonical = _canonical_domain(row.get("domain"))
            if canonical is not None:
                patch["domain"] = canonical

            prediction = row.get("prediction") or {}
            if isinstance(prediction, dict):
                jsonb_canonical = _canonical_domain(prediction.get("domain"))
                if jsonb_canonical is not None:
                    patch["prediction"] = {**prediction, "domain": jsonb_canonical}

            outcome = row.get("outcome") or {}
            if isinstance(outcome, dict) and "metrics" in outcome:
                # Move metrics into metadata.metrics without clobbering
                metadata = dict(outcome.get("metadata") or {})
                if "metrics" not in metadata:
                    metadata["metrics"] = outcome["metrics"]
                new_outcome = {k: v for k, v in outcome.items() if k != "metrics"}
                new_outcome["metadata"] = metadata
                patch["outcome"] = new_outcome

            if not patch:
                continue

            if dry_run:
                logger.info(
                    "[DRY-RUN] episode %s patches: %s",
                    row["episode_id"], list(patch.keys()),
                )
                stats["updated"] += 1
                continue

            db.table("episodic_memory").update(patch).eq(
                "episode_id", row["episode_id"]
            ).execute()
            stats["updated"] += 1

        offset += page_size
        if len(rows) < page_size:
            break

    logger.info(
        "[1/2] episodic_memory: scanned=%d updated=%d skipped=%d",
        stats["scanned"], stats["updated"], stats["skipped"],
    )
    return stats


def backfill_pattern_domains(db, *, dry_run: bool) -> Dict[str, int]:
    """Rewrite semantic_patterns.domain to canonical enum values."""
    stats = {"scanned": 0, "updated": 0}

    result = (
        db.table("semantic_patterns").select("pattern_id,domain").execute()
    )
    rows = result.data or []

    for row in rows:
        stats["scanned"] += 1
        canonical = _canonical_domain(row.get("domain"))
        if canonical is None:
            continue

        if dry_run:
            logger.info(
                "[DRY-RUN] pattern %s domain %r → %s",
                row["pattern_id"], row.get("domain"), canonical,
            )
            stats["updated"] += 1
            continue

        db.table("semantic_patterns").update(
            {"domain": canonical}
        ).eq("pattern_id", row["pattern_id"]).execute()
        stats["updated"] += 1

    logger.info(
        "[2/2] semantic_patterns: scanned=%d updated=%d",
        stats["scanned"], stats["updated"],
    )
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description="Phase 2 backfill")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--tenant-id", help="Scope episodic backfill to one tenant")
    parser.add_argument("--skip-episodic", action="store_true")
    parser.add_argument("--skip-patterns", action="store_true")
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

    summary: Dict[str, Any] = {"dry_run": args.dry_run}

    if not args.skip_episodic:
        summary["episodic"] = backfill_episodic_domains(
            db, tenant_id=args.tenant_id, dry_run=args.dry_run,
        )

    if not args.skip_patterns:
        summary["patterns"] = backfill_pattern_domains(db, dry_run=args.dry_run)

    logger.info("Backfill summary: %s", json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
