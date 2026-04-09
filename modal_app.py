"""
BrightMatter Modal Deployment

Deploys the BrightMatter API and cron workers to Modal cloud.

    modal deploy modal_app.py

Functions:
    api_endpoint         — FastAPI ASGI app (always on)
    worker_cron          — Event processing + consolidation (every 15 min)
    platform_data_cron   — Daily platform data pull at 1pm EST / 18:00 UTC
    fmt_data_sync_cron   — Daily FMT app data sync via MCP → BQ (2pm EST / 19:00 UTC)
    platform_backfill    — On-demand historical backfill
    weekly_eval          — Shadow eval + gold standards (Sundays 10:00 UTC)
    improvement_review   — Improvement analysis (Mondays 12:00 UTC)
    health_check         — On-demand connectivity check
"""

import modal

app = modal.App("brightmatter")

WORKSPACE_PATH = "/workspace"

bm_image = (
    modal.Image.debian_slim(python_version="3.12")
    .pip_install(
        "fastapi>=0.115.0",
        "uvicorn[standard]>=0.30.0",
        "pydantic>=2.9.0",
        "python-dotenv>=1.0.0",
        "numpy>=1.26.0",
        "supabase>=2.0.0",
        "httpx>=0.27.0",
        "google-cloud-bigquery>=3.20.0",
        "requests>=2.31.0",
        "firebase-admin>=6.0.0",
        "google-ads>=24.0.0",
    )
    .add_local_dir(".", remote_path=WORKSPACE_PATH, copy=True)
)

_required_secrets = [
    modal.Secret.from_name("bm-supabase"),
    modal.Secret.from_name("bm-api-key"),
]

_optional_secrets = []
for _name in ("bm-bigquery", "bm-airtable", "mh1-firebase", "mh1-all", "bm-mcp-fmt"):
    try:
        _optional_secrets.append(modal.Secret.from_name(_name, required_keys=[]))
    except Exception:
        pass

bm_secrets = _required_secrets + _optional_secrets


def _setup_workspace():
    """Add workspace to sys.path so lib/ imports work."""
    import sys
    if WORKSPACE_PATH not in sys.path:
        sys.path.insert(0, WORKSPACE_PATH)


# ── API Endpoint ────────────────────────────────────────────────────

@app.function(
    image=bm_image,
    secrets=bm_secrets,
    keep_warm=1,
    timeout=300,
    allow_concurrent_inputs=50,
)
@modal.asgi_app()
def api_endpoint():
    """Mount the existing FastAPI app as a Modal ASGI endpoint."""
    _setup_workspace()
    from api import app as fastapi_app
    return fastapi_app


# ── Worker Cron (every 15 min) ──────────────────────────────────────

@app.function(
    image=bm_image,
    secrets=bm_secrets,
    timeout=1800,
    schedule=modal.Period(minutes=15),
)
def worker_cron():
    """Pull events from Supabase, process through learning pipeline,
    run consolidation + checkpoints, refresh guidance cache."""
    _setup_workspace()

    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger = logging.getLogger("brightmatter.cron.worker")

    try:
        from worker import BrightMatterWorker
        w = BrightMatterWorker()
        stats = w.run_cycle()
        logger.info(f"Worker cycle complete: {stats}")
        return stats
    except Exception as e:
        logger.error(f"Worker cycle failed: {e}", exc_info=True)
        return {"error": str(e)}


# ── Platform Data Cron (daily 1pm EST = 18:00 UTC) ───────────────────

@app.function(
    image=bm_image,
    secrets=bm_secrets,
    timeout=1200,
    schedule=modal.Cron("0 18 * * *"),
)
def platform_data_cron():
    """Pull yesterday's platform data for all clients into Supabase."""
    _setup_workspace()

    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger = logging.getLogger("brightmatter.cron.platform")

    try:
        from lib.platform_ingestion import PlatformDataOrchestrator
        orch = PlatformDataOrchestrator(backfill=False)
        stats = orch.run_daily()
        logger.info(f"Platform data cron complete: {stats}")
        return stats
    except Exception as e:
        logger.error(f"Platform data cron failed: {e}", exc_info=True)
        return {"error": str(e)}


# ── FMT Data Sync Cron (daily 2pm EST = 19:00 UTC) ──────────────────

@app.function(
    image=bm_image,
    secrets=bm_secrets,
    timeout=1800,
    schedule=modal.Cron("0 19 * * *"),
)
def fmt_data_sync_cron():
    """Daily sync: FMT app data (MCP→BQ) + GTM config (MCP→BQ)."""
    _setup_workspace()

    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger = logging.getLogger("brightmatter.cron.fmt_sync")

    stats = {}
    try:
        from lib.mcp_sync import FMTDataSync
        stats["app"] = FMTDataSync().run()
        logger.info(f"App sync complete: {stats['app']}")
    except Exception as e:
        logger.error(f"App sync failed: {e}", exc_info=True)
        stats["app_error"] = str(e)

    try:
        from lib.mcp_sync import GTMSync, BQWriter
        stats["gtm"] = GTMSync().sync(BQWriter())
        logger.info(f"GTM sync complete: {stats['gtm']}")
    except Exception as e:
        logger.error(f"GTM sync failed: {e}", exc_info=True)
        stats["gtm_error"] = str(e)

    return stats


# ── Platform Backfill (on-demand) ────────────────────────────────────

@app.function(
    image=bm_image,
    secrets=bm_secrets,
    timeout=3600,
)
def platform_backfill(
    years: int = 3,
    client_filter: str = None,
    platform_filter: str = None,
):
    """On-demand historical backfill of platform data."""
    _setup_workspace()

    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger = logging.getLogger("brightmatter.backfill.platform")

    try:
        from lib.platform_ingestion import PlatformDataOrchestrator
        orch = PlatformDataOrchestrator(backfill=True)
        stats = orch.run_backfill(
            lookback_years=years,
            client_filter=client_filter,
            platform_filter=platform_filter,
        )
        logger.info(f"Platform backfill complete: {stats}")
        return stats
    except Exception as e:
        logger.error(f"Platform backfill failed: {e}", exc_info=True)
        return {"error": str(e)}


# ── Weekly Intelligence Eval (Sundays 10:00 UTC) ────────────────────

@app.function(
    image=bm_image,
    secrets=bm_secrets,
    timeout=600,
    schedule=modal.Cron("0 10 * * 0"),
)
def weekly_eval():
    """Weekly evaluation: shadow candidate assessment, gold standard
    validation, accuracy scoring, and memory health report."""
    _setup_workspace()

    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger = logging.getLogger("brightmatter.cron.weekly")
    report = {}

    try:
        from lib.intelligence import IntelligenceEngine
        engine = IntelligenceEngine()

        # Shadow evaluation
        try:
            shadow_result = engine.shadow_manager.evaluate_candidate()
            report["shadow"] = shadow_result
            logger.info(f"Shadow eval: {shadow_result.get('action', 'none')}")
        except Exception as e:
            report["shadow_error"] = str(e)

        # Gold standard validation
        try:
            gold_result = engine.gold_validator.validate_all()
            report["gold_standard"] = gold_result
            logger.info(f"Gold standard: {gold_result}")
        except Exception as e:
            report["gold_error"] = str(e)

        # Accuracy scoring
        try:
            accuracy = engine.accuracy_scorer.score_recent(lookback_days=7)
            report["accuracy"] = accuracy
            logger.info(f"Weekly accuracy: {accuracy}")
        except Exception as e:
            report["accuracy_error"] = str(e)

        # Memory health
        try:
            from lib.memory_health import check_memory_health
            health = check_memory_health(engine)
            report["memory_health"] = health
        except Exception as e:
            report["health_error"] = str(e)

    except Exception as e:
        logger.error(f"Weekly eval failed: {e}", exc_info=True)
        report["error"] = str(e)

    logger.info(f"Weekly eval complete: {list(report.keys())}")
    return report


# ── Improvement Review (Mondays 12:00 UTC) ──────────────────────────

@app.function(
    image=bm_image,
    secrets=bm_secrets,
    timeout=600,
    schedule=modal.Cron("0 12 * * 1"),
)
def improvement_review():
    """Weekly improvement analysis: scan closed outcomes for systematic
    under-performance, generate proposals, archive for review."""
    _setup_workspace()

    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger = logging.getLogger("brightmatter.cron.improvement")
    report = {}

    try:
        from lib.intelligence import IntelligenceEngine
        engine = IntelligenceEngine()

        from lib.intelligence.improvement.analyzer import ImprovementAnalyzer
        from lib.intelligence.improvement.proposer import ImprovementProposer

        analyzer = ImprovementAnalyzer(firebase_client=engine.storage)
        candidates = analyzer.analyze()
        report["candidates_found"] = len(candidates)

        if candidates:
            proposer = ImprovementProposer()
            proposals = proposer.propose(candidates)
            report["proposals_generated"] = len(proposals)

            for p in proposals[:5]:
                logger.info(
                    f"Proposal: [{p.proposal_type}] {p.title} "
                    f"(severity={p.severity:.2f})"
                )

            report["top_proposals"] = [p.to_dict() for p in proposals[:5]]
        else:
            logger.info("No improvement candidates found")

    except Exception as e:
        logger.error(f"Improvement review failed: {e}", exc_info=True)
        report["error"] = str(e)

    logger.info(f"Improvement review complete: {report.get('candidates_found', 0)} candidates")
    return report


# ── Health Check ────────────────────────────────────────────────────

@app.function(
    image=bm_image,
    secrets=bm_secrets,
    timeout=60,
)
def health_check():
    """Verify engine initialization and Supabase connectivity."""
    _setup_workspace()

    status = {"engine": False, "supabase": False}

    try:
        from lib.intelligence import IntelligenceEngine
        engine = IntelligenceEngine()
        status["engine"] = True
        status["storage"] = engine.storage is not None
    except Exception as e:
        status["engine_error"] = str(e)

    try:
        from lib.supabase_client import get_supabase
        sb = get_supabase()
        result = sb.table("events").select("id").limit(1).execute()
        status["supabase"] = True
        status["supabase_events_accessible"] = True
    except Exception as e:
        status["supabase_error"] = str(e)

    return status
