# api/endpoints/subcounties.py
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.db.session import get_db
from app.services.admin_service import get_subcounties, get_subcounties_by_county

router = APIRouter()

import json


@router.get("/")
def get_subcounties_endpoint(db: Session = Depends(get_db)):
    subs = get_subcounties(db)

    features = []

    for s in subs:
        features.append({
            "type": "Feature",
            "properties": {
                "id": s["id"],
                "name": s["name"],
                "county_id": s["county_id"]   # ✅ IMPORTANT FIX
            },
            "geometry": json.loads(s["geometry"])
        })

    return {
        "type": "FeatureCollection",
        "features": features
    }


@router.get("/by-county/{county_id}")
def get_subs_by_county(county_id: str, db: Session = Depends(get_db)):
    subs = get_subcounties_by_county(db, county_id)

    features = []

    for s in subs:
        features.append({
            "type": "Feature",
            "properties": {
                "id": s["id"],
                "name": s["name"],
                "county_id": s["county_id"]   # ✅ IMPORTANT FIX
            },
            "geometry": json.loads(s["geometry"])
        })

    return {
        "type": "FeatureCollection",
        "features": features
    }