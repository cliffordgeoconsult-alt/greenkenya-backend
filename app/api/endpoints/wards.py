from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.db.session import get_db
from app.services.admin_service import get_wards, get_wards_by_county, get_wards_by_subcounty

router = APIRouter()

import json

@router.get("/")
def get_wards_endpoint(db: Session = Depends(get_db)):
    wards = get_wards(db)

    features = []

    for w in wards:
        features.append({
            "type": "Feature",
            "properties": {
                "id": w["id"],
                "name": w["name"]
            },
            "geometry": json.loads(w["geometry"])
        })

    return {
        "type": "FeatureCollection",
        "features": features
    }

@router.get("/by-county/{county_id}")

def get_wards_by_county_endpoint(county_id: str, db: Session = Depends(get_db)):
    wards = get_wards_by_county(db, county_id)

    features = []

    for w in wards:
        features.append({
            "type": "Feature",
            "properties": {
                "id": w["id"],
                "name": w["name"]
            },
            "geometry": json.loads(w["geometry"])
        })

    return {
        "type": "FeatureCollection",
        "features": features
    }

@router.get("/by-subcounty/{subcounty_id}")
def get_wards_by_sub(subcounty_id: str, db: Session = Depends(get_db)):
    wards = get_wards_by_subcounty(db, subcounty_id)

    features = []
    for w in wards:
        features.append({
            "type": "Feature",
            "properties": {
                "id": w["id"],
                "name": w["name"]
            },
            "geometry": json.loads(w["geometry"])
        })

    return {
        "type": "FeatureCollection",
        "features": features
    }