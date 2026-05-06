from datetime import datetime

from app.core.celery_app import celery
from app.core.prewarm_context import prewarm_bundle_begin, prewarm_bundle_end
from app.services.forest_intelligence_service import (
    run_vegetation_analysis,
    run_ward_vegetation_analysis,
    run_reserve_loss_analysis,
)
from app.services.admin_service import get_counties, get_wards
from app.services.gee.ee_init import warmup_earth_engine_once
from app.services.gee.uhi_analysis import UHI_MIN_YEAR
from app.services.uhi_prewarm_service import run_uhi_prewarm
from app.db.session import SessionLocal
from app.agent_debug_log import agent_debug_log


@celery.task(bind=True, autoretry_for=(Exception,), retry_backoff=5, retry_kwargs={'max_retries': 3})
def prewarm_forests_bundle(self):
    """
    Single worker run: one EE handshake, then Hansen-only prewarm for target counties,
    their wards (already filtered in admin queries), and all reserves.
    """
    db = SessionLocal()
    prewarm_bundle_begin()
    # #region agent log
    agent_debug_log("H4", "prewarm_tasks.prewarm_forests_bundle", "bundle_start", {})
    # #endregion
    try:
        warmup_earth_engine_once()
        counties = get_counties(db)
        wards = get_wards(db)
        # #region agent log
        agent_debug_log(
            "H4",
            "prewarm_tasks.prewarm_forests_bundle",
            "after_warmup_counts",
            {"counties": len(counties), "wards": len(wards)},
        )
        # #endregion
        for c in counties:
            run_vegetation_analysis(db, "county", c["id"], prewarm=True)
        for w in wards:
            run_ward_vegetation_analysis(db, w["id"], prewarm=True)
        run_reserve_loss_analysis(db, prewarm=True)
        # #region agent log
        agent_debug_log("H3", "prewarm_tasks.prewarm_forests_bundle", "bundle_complete", {})
        # #endregion
    finally:
        prewarm_bundle_end()
        db.close()


@celery.task(bind=True, autoretry_for=(Exception,), retry_backoff=5, retry_kwargs={'max_retries': 3})
def prewarm_county(self, county_id):
    db = SessionLocal()
    try:
        warmup_earth_engine_once()
        run_vegetation_analysis(db, "county", county_id, prewarm=True)
    finally:
        db.close()


@celery.task(bind=True, autoretry_for=(Exception,), retry_backoff=5, retry_kwargs={'max_retries': 3})
def prewarm_ward(self, ward_id):
    db = SessionLocal()
    try:
        warmup_earth_engine_once()
        run_ward_vegetation_analysis(db, ward_id, prewarm=True)
    finally:
        db.close()


@celery.task
def prewarm_reserves():
    db = SessionLocal()
    try:
        warmup_earth_engine_once()
        run_reserve_loss_analysis(db, prewarm=True)
    finally:
        db.close()


@celery.task(bind=True, autoretry_for=(Exception,), retry_backoff=30, retry_kwargs={"max_retries": 2})
def prewarm_uhi_bundle(self):
    """
    Warm UHI full-report Redis and county merged forest-union MODIS baselines for all pilot years.
    Skips work already cached (same defaults as HTTP prewarm). Tiles off to limit GEE load.
    """
    db = SessionLocal()
    try:
        now_y = datetime.now().year
        return run_uhi_prewarm(
            db,
            start_year=UHI_MIN_YEAR,
            end_year=now_y,
            skip_if_cached=True,
            force_refresh=False,
            include_forest_baselines=True,
            include_tiles=False,
        )
    finally:
        db.close()