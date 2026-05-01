# app/jobs/radd_scheduler.py
# RADD Scheduler Job
# This module sets up a daily job to fetch and ingest RADD alerts from GFW into both the local and render databases.
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os
from apscheduler.schedulers.background import BackgroundScheduler
from app.services.radd_gfw_service import ingest_radd_alerts_gfw

LOCAL_DB = os.getenv("LOCAL_DATABASE_URL")
RENDER_DB = os.getenv("DATABASE_URL")

if not RENDER_DB:
    raise ValueError("DATABASE_URL not set")

# LOCAL DB is optional (only for local dev)
if not LOCAL_DB:
    print("⚠️ LOCAL_DATABASE_URL not set — skipping local ingestion")

RenderSession = sessionmaker(bind=create_engine(RENDER_DB))

LocalSession = None
if LOCAL_DB:
    LocalSession = sessionmaker(bind=create_engine(LOCAL_DB))

scheduler = None

def run_radd_job():
    print("Running RADD ingestion for BOTH DBs...")

    # Render
    render_db = RenderSession()
    try:
        print("Render DB...")
        ingest_radd_alerts_gfw(render_db)
    finally:
        render_db.close()

    # Local (only if exists)
    if LocalSession:
        local_db = LocalSession()
        try:
            print("Local DB...")
            ingest_radd_alerts_gfw(local_db)
        finally:
            local_db.close()

def start_scheduler():
    global scheduler

    if scheduler:
        return

    scheduler = BackgroundScheduler()

    scheduler.add_job(
        run_radd_job,
        trigger='cron',
        hour=0,
        minute=0
    )

    scheduler.start()
    print("RADD scheduler started (daily at 00:00)")