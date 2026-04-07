from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.db.session import get_db
from app.services.admin_service import get_counties

router = APIRouter()

import json

@router.get("/")
def get_counties_endpoint(db: Session = Depends(get_db)):
    counties = get_counties(db)

    features = []

    for c in counties:
        features.append({
            "type": "Feature",
            "properties": {
                "id": c["id"],
                "name": c["name"]
            },
            "geometry": json.loads(c["geometry"])
        })

    return {
        "type": "FeatureCollection",
        "features": features
    }