# app/api/endpoints/forests.py
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
from app.db.session import get_db
from app.models.forest import Forest
from app.services.forest_intelligence_service import (
    run_vegetation_analysis,
    run_ward_vegetation_analysis,
    run_subcounty_vegetation_analysis,
    run_national_vegetation_analysis,
    run_reserve_loss_analysis,
    run_non_reserve_forest_analysis,
    run_forest_intelligence
)
from app.services.forest_registry_service import generate_forest_registry
from app.services.radd_gfw_service import ingest_radd_alerts_gfw
from app.services.radd_hotspot_service import generate_radd_hotspots
from app.services.reserve_loader_service import load_forest_reserves
from app.services.reserve_analysis_service import compute_reserve_forests

from geoalchemy2.shape import to_shape
import json

router = APIRouter()


@router.get("/forest-analysis")
def forest_analysis(db: Session = Depends(get_db)):
    return run_vegetation_analysis(db)

@router.get("/forest-analysis/wards")
def ward_forest_analysis(db: Session = Depends(get_db)):
    return run_ward_vegetation_analysis(db)


@router.get("/forest-analysis/subcounties")
def subcounty_forest_analysis(db: Session = Depends(get_db)):
    return run_subcounty_vegetation_analysis(db)


@router.get("/forest-analysis/national")
def national_forest_analysis(db: Session = Depends(get_db)):
    return run_national_vegetation_analysis(db)

@router.get("/generate-forest-registry")
def generate_registry(db: Session = Depends(get_db)):
    return generate_forest_registry(db)


@router.get("/")
def get_forests(db: Session = Depends(get_db)):

    forests = db.query(Forest).limit(500).all()

    features = []

    for f in forests:
        # convert WKB → GeoJSON
        geom = to_shape(f.geometry)
        geojson_geom = geom.__geo_interface__

        features.append({
            "type": "Feature",
            "geometry": geojson_geom,
            "properties": {
                "id": f.forest_id,
                "area_ha": f.area_ha,
                "county": f.county,
                "baseline_year": f.baseline_year,
                "source": f.source,
                "confidence": f.confidence
            }
        })

    return {
        "type": "FeatureCollection",
        "count": len(features),
        "features": features
    }

@router.get("/load-reserves")
def load_reserves(db: Session = Depends(get_db)):
    return load_forest_reserves(
        db,
        "C:/Users/Elitebook/Desktop/greenkenya-backend/data/Kenya_Forest_Reserves_-8694555811515041942.geojson"
    )

@router.get("/compute-reserve-forests")
def compute_reserves(db: Session = Depends(get_db)):
    return compute_reserve_forests(db)

@router.get("/forest-analysis/reserves")
def reserve_loss_analysis(db: Session = Depends(get_db)):
    return run_reserve_loss_analysis(db)

@router.get("/forest-analysis/non-reserve")
def non_reserve_forest_analysis(db: Session = Depends(get_db)):
    return run_non_reserve_forest_analysis(db)

@router.get("/intelligence")
def forest_intelligence(db: Session = Depends(get_db)):
    return run_forest_intelligence(db)

@router.get("/ingest-radd")
def ingest_radd(db: Session = Depends(get_db)):
    return ingest_radd_alerts_gfw(db)

@router.get("/hotspots")
def get_hotspots(db: Session = Depends(get_db)):
    return generate_radd_hotspots(db)

@router.get("/reserves")
def get_reserves(db: Session = Depends(get_db)):

    reserves = db.execute(text("""
        SELECT 
            reserve_id,
            name,
            ST_AsGeoJSON(geometry)
        FROM forest_reserves
        LIMIT 200
    """)).fetchall()

    features = []

    for r in reserves:
        features.append({
            "type": "Feature",
            "geometry": json.loads(r[2]),
            "properties": {
                "reserve_id": r[0],
                "name": r[1]
            }
        })

    return {
        "type": "FeatureCollection",
        "count": len(features),
        "features": features
    }