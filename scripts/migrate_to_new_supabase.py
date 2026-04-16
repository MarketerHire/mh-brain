"""
Migrate mh-brain tables from MH-OS Supabase to a dedicated Supabase project.

Connects to both projects, reads all rows from each mh-brain table in the
source (MH-OS), and batch-upserts them into the destination (new mh-brain).

Usage:
    # Dry run (counts only, no writes)
    python scripts/migrate_to_new_supabase.py --dry-run

    # Full migration
    python scripts/migrate_to_new_supabase.py

Env vars (both required):
    SOURCE_SUPABASE_URL / SOURCE_SUPABASE_KEY  — MH-OS project (current)
    DEST_SUPABASE_URL   / DEST_SUPABASE_KEY    — new mh-brain project

Falls back to MHOS_SUPABASE_URL/KEY and SUPABASE_URL/KEY respectively
if the SOURCE/DEST vars aren't set.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from typing import Any, Dict, List

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("migrate")

TABLES_TO_MIGRATE = [
    {"name": "episodic_memory", "pk": "episode_id"},
    {"name": "semantic_patterns", "pk": "pattern_id"},
    {"name": "procedural_knowledge", "pk": "knowledge_id"},
    {"name": "working_predictions", "pk": "prediction_id"},
    {"name": "guidance_cache", "pk": "skill_name,client_id"},
    {"name": "patterns", "pk": "id"},
    {"name": "client_platform_data", "pk": "id"},
    {"name": "reference_knowledge", "pk": "id"},
    {"name": "bm_watermarks", "pk": "source"},
    {"name": "shadow_state", "pk": "id"},
    {"name": "shadow_history", "pk": "id"},
    {"name": "accuracy_reports", "pk": "id"},
    {"name": "error_history", "pk": "id"},
    {"name": "channel_timing", "pk": "id"},
    {"name": "gold_standards", "pk": "id"},
    {"name": "benchmark_results", "pk": "id"},
    {"name": "predictions", "pk": "tracking_id"},
    {"name": "outcomes", "pk": "id"},
]

BATCH_SIZE = 500


def _get_clients():
    from supabase import create_client

    src_url = os.environ.get("SOURCE_SUPABASE_URL") or os.environ.get("MHOS_SUPABASE_URL", "")
    src_key = os.environ.get("SOURCE_SUPABASE_KEY") or os.environ.get("MHOS_SUPABASE_KEY", "")

    dst_url = os.environ.get("DEST_SUPABASE_URL") or os.environ.get("SUPABASE_URL", "")
    dst_key = os.environ.get("DEST_SUPABASE_KEY") or os.environ.get("SUPABASE_KEY", "")

    if not src_url or not src_key:
        logger.error("Source credentials missing (SOURCE_SUPABASE_URL/KEY or MHOS_SUPABASE_URL/KEY)")
        sys.exit(1)
    if not dst_url or not dst_key:
        logger.error("Destination credentials missing (DEST_SUPABASE_URL/KEY or SUPABASE_URL/KEY)")
        sys.exit(1)

    if src_url == dst_url:
        logger.error("Source and destination URLs are the same — aborting to prevent self-overwrite")
        sys.exit(1)

    source = create_client(src_url, src_key)
    dest = create_client(dst_url, dst_key)

    logger.info(f"Source:      {src_url}")
    logger.info(f"Destination: {dst_url}")
    return source, dest


def _count_rows(client, table: str) -> int:
    try:
        result = client.table(table).select("*", count="exact").limit(0).execute()
        return result.count or 0
    except Exception:
        return -1


def _fetch_all(client, table: str) -> List[Dict[str, Any]]:
    """Paginate through all rows in a table."""
    all_rows: List[Dict[str, Any]] = []
    offset = 0
    while True:
        result = (
            client.table(table)
            .select("*")
            .range(offset, offset + BATCH_SIZE - 1)
            .execute()
        )
        batch = result.data or []
        if not batch:
            break
        all_rows.extend(batch)
        offset += len(batch)
        if len(batch) < BATCH_SIZE:
            break
    return all_rows


def _upsert_batch(client, table: str, rows: List[Dict], pk: str):
    """Upsert rows in batches."""
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        client.table(table).upsert(batch, on_conflict=pk).execute()


def migrate(dry_run: bool = False):
    source, dest = _get_clients()

    summary: List[Dict[str, Any]] = []

    for tbl in TABLES_TO_MIGRATE:
        name = tbl["name"]
        pk = tbl["pk"]

        src_count = _count_rows(source, name)
        dst_count_before = _count_rows(dest, name)

        if src_count == -1:
            logger.warning(f"  {name}: table not found in source — skipping")
            summary.append({"table": name, "status": "skipped", "reason": "not in source"})
            continue

        if src_count == 0:
            logger.info(f"  {name}: 0 rows in source — nothing to migrate")
            summary.append({"table": name, "status": "empty", "source": 0})
            continue

        logger.info(f"  {name}: {src_count} rows in source, {dst_count_before} in dest")

        if dry_run:
            summary.append({
                "table": name,
                "status": "dry_run",
                "source": src_count,
                "dest_before": dst_count_before,
            })
            continue

        rows = _fetch_all(source, name)
        logger.info(f"  {name}: fetched {len(rows)} rows, upserting...")

        t0 = time.time()
        try:
            _upsert_batch(dest, name, rows, pk)
            elapsed = time.time() - t0
            dst_count_after = _count_rows(dest, name)
            logger.info(f"  {name}: upserted {len(rows)} rows in {elapsed:.1f}s — dest now has {dst_count_after}")
            summary.append({
                "table": name,
                "status": "migrated",
                "source": src_count,
                "dest_before": dst_count_before,
                "dest_after": dst_count_after,
                "rows_written": len(rows),
                "elapsed_s": round(elapsed, 1),
            })
        except Exception as e:
            logger.error(f"  {name}: upsert failed — {e}")
            summary.append({"table": name, "status": "error", "error": str(e)})

    logger.info("\n=== Migration Summary ===")
    for s in summary:
        logger.info(f"  {s['table']}: {s['status']} — {json.dumps({k: v for k, v in s.items() if k != 'table'})}")

    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate mh-brain tables to new Supabase")
    parser.add_argument("--dry-run", action="store_true", help="Count rows only, don't write")
    args = parser.parse_args()

    migrate(dry_run=args.dry_run)
