from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.db.session import get_db
from app.services.admin_service import get_subcounties

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
                "name": s["name"]
            },
            "geometry": json.loads(s["geometry"])
        })

    return {
        "type": "FeatureCollection",
        "features": features
    }