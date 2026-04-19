# app/api/endpoints/ai.py

from fastapi import APIRouter
from app.services.ai_service import generate_ai_insight

import hashlib
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

router = APIRouter()

# SIMPLE IN-MEMORY CACHE
ai_cache = {}

MAX_ITEMS = 20  # prevent overload


def get_hash(domain: str, data: dict):
    """
    Unique hash per domain + data
    """
    base = {"domain": domain, "data": data}
    return hashlib.md5(json.dumps(base, sort_keys=True).encode()).hexdigest()


def clean_item(item):
    return {
        "name": item.get("county") or item.get("ward") or item.get("subcounty"),
        "loss_pct": item.get("loss_pct"),
        "alerts_total": item.get("alerts_total"),
        "recent_alerts": sum(d.get("alerts", 0) for d in item.get("radd_daily", [])) if item.get("radd_daily") else 0,
        "degradation_ha": item.get("degradation_ha"),
        "regrowth_ha": item.get("regrowth_ha"),
        "vitality_pct": item.get("vitality_pct"),
        "risk": item.get("risk")
    }


def process_item(domain, item):
    key = get_hash(domain, item)

    # CACHE HIT
    if key in ai_cache:
        return {
            "name": item["name"],
            "insight": ai_cache[key],
            "cached": True
        }

    # CACHE MISS
    ai_output = generate_ai_insight(domain, [item])
    ai_cache[key] = ai_output

    return {
        "name": item["name"],
        "insight": ai_output,
        "cached": False
    }


@router.post("/interpret")
def interpret_data(payload: dict):
    """
    Improved AI endpoint (parallel + safe)

    Supports:
    - batching
    - caching
    - limits
    """

    try:
        domain = payload.get("domain", "environment")
        raw_data = payload.get("data", [])

        if not raw_data:
            return {"error": "No data provided"}

        # LIMIT DATA SIZE
        raw_data = raw_data[:MAX_ITEMS]

        cleaned_data = [clean_item(item) for item in raw_data]

        results = []

        # PARALLEL EXECUTION (BIG UPGRADE)
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(process_item, domain, item)
                for item in cleaned_data
            ]

            for future in as_completed(futures):
                try:
                    results.append(future.result())
                except Exception as e:
                    results.append({
                        "error": str(e)
                    })

        return {
            "count": len(results),
            "results": results
        }

    except Exception as e:
        return {
            "error": "AI processing failed",
            "details": str(e)
        }