# app/api/endpoints/ai.py

from fastapi import APIRouter
from app.services.ai_service import generate_ai_insight

import hashlib
import json

router = APIRouter()

# SIMPLE IN-MEMORY CACHE (upgrade later to Redis)
ai_cache = {}


def get_hash(data: dict):
    """
    Generate a unique hash for caching AI results
    """
    return hashlib.md5(json.dumps(data, sort_keys=True).encode()).hexdigest()


@router.post("/interpret")
def interpret_data(payload: dict):
    """
    Generic AI endpoint.

    Frontend sends:
    {
        "domain": "forest",
        "data": [...]
    }

    Returns per-entity AI insights
    """

    try:
        domain = payload.get("domain", "environment")
        raw_data = payload.get("data", [])

        if not raw_data:
            return {
                "error": "No data provided"
            }

        # CLEAN + REDUCE DATA
        cleaned_data = [
            {
                "name": item.get("county") or item.get("ward") or item.get("subcounty"),
                "loss_pct": item.get("loss_pct"),
                "alerts_total": item.get("alerts_total"),
                "recent_alerts": sum(d.get("alerts", 0) for d in item.get("radd_daily", [])) if item.get("radd_daily") else 0,
                "degradation_ha": item.get("degradation_ha"),
                "regrowth_ha": item.get("regrowth_ha"),
                "vitality_pct": item.get("vitality_pct"),
                "risk": item.get("risk")
            }
            for item in raw_data
        ]

        results = []

        # PER-ENTITY AI
        for item in cleaned_data:
            key = get_hash(item)

            # CACHE HIT
            if key in ai_cache:
                ai_output = ai_cache[key]

            # CACHE MISS
            else:
                ai_output = generate_ai_insight(domain, [item])
                ai_cache[key] = ai_output

            results.append({
                "name": item["name"],
                "insight": ai_output
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