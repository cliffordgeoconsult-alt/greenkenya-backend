from app.core.celery_app import celery
from app.services.forest_intelligence_service import (
    run_vegetation_analysis,
    run_ward_vegetation_analysis,
    run_subcounty_vegetation_analysis,
    run_reserve_loss_analysis
)
from app.db.session import SessionLocal


@celery.task(bind=True, autoretry_for=(Exception,), retry_backoff=5, retry_kwargs={'max_retries': 3})
def prewarm_county(self, county_id):
    db = SessionLocal()
    try:
        run_vegetation_analysis(db, "county", county_id, prewarm=True)
    finally:
        db.close()


@celery.task(bind=True, autoretry_for=(Exception,), retry_backoff=5, retry_kwargs={'max_retries': 3})
def prewarm_ward(self, ward_id):
    db = SessionLocal()
    try:
        run_ward_vegetation_analysis(db, ward_id)
    finally:
        db.close()


@celery.task(bind=True, autoretry_for=(Exception,), retry_backoff=5, retry_kwargs={'max_retries': 3})
def prewarm_subcounty(self, sub_id):
    db = SessionLocal()
    try:
        run_subcounty_vegetation_analysis(db, sub_id, prewarm=True)
    finally:
        db.close()


@celery.task
def prewarm_reserves():
    db = SessionLocal()
    try:
        run_reserve_loss_analysis(db)
    finally:
        db.close()