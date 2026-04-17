from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
from app.services.waste_detection.run_detection import run_pipeline
from app.db.session import get_db

router = APIRouter()

@router.post("/detect/{county_name}")
def detect_waste(county_name: str, db: Session = Depends(get_db)):
    
    # 1. Get county geometry from DB
    query = """
        SELECT ST_AsGeoJSON(geometry) as geom
        FROM admin_county
        WHERE name = :county_name
    """
    
    result = db.execute(text(query), {"county_name": county_name}).fetchone()

    if not result:
        return {"error": "County not found"}

    # 2. Convert to GeoJSON
    import json
    geom = json.loads(result.geom)

    # 3. Run detection using county boundary
    detections = run_pipeline(geom)

    return {
        "county": county_name,
        "detections": detections
    }