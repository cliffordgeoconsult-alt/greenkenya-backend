from celery import Celery
import os

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

celery = Celery(
    "greenmap",
    broker=REDIS_URL,
    backend=REDIS_URL
)

celery.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    worker_concurrency=int(os.getenv("CELERY_WORKER_CONCURRENCY", "1")),
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    broker_connection_retry_on_startup=True,
)

celery.autodiscover_tasks(["app.tasks"])

# 🔥 FORCE LOAD TASKS (IMPORTANT)
import app.tasks.prewarm_tasks