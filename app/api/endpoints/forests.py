# app/api/endpoints/forests.py
import json
import ee
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
from app.services.gee.forest_analysis import get_hansen_loss_tile
from app.services.admin_service import get_counties, get_subcounties, get_wards
from app.services.forest_registry_service import generate_forest_registry
from app.services.radd_gfw_service import ingest_radd_alerts_gfw
from app.services.radd_hotspot_service import generate_radd_hotspots
from app.services.reserve_loader_service import load_forest_reserves
from app.services.reserve_analysis_service import compute_reserve_forests

from geoalchemy2.shape import to_shape


router = APIRouter()


@router.get("/forest-analysis")
def forest_analysis(
    level: str = None,
    entity_id: str = None,
    db: Session = Depends(get_db)
):
    return run_vegetation_analysis(db, level, entity_id)

@router.get("/forest-analysis/wards")
def ward_forest_analysis(
    entity_id: str = None,
    db: Session = Depends(get_db)
):
    return run_ward_vegetation_analysis(db, entity_id)

@router.get("/forest-analysis/subcounties")
def subcounty_forest_analysis(
    entity_id: str = None,
    db: Session = Depends(get_db)
):
    return run_subcounty_vegetation_analysis(db, entity_id)

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
def get_hotspots(days: int = 90, db: Session = Depends(get_db)):
    return generate_radd_hotspots(db, days)

@router.get("/reserves")
def get_reserves(
    reserve_id: str = None,
    db: Session = Depends(get_db)
):
    import json

    # SINGLE RESERVE
    if reserve_id:
        reserve = db.execute(text("""
            SELECT 
                reserve_id,
                name,
                ST_AsGeoJSON(geometry)
            FROM forest_reserves
            WHERE reserve_id = :rid
        """), {"rid": reserve_id}).fetchone()

        if not reserve:
            return {"error": "Reserve not found"}

        return {
            "type": "Feature",
            "geometry": json.loads(reserve[2]),
            "properties": {
                "reserve_id": reserve[0],
                "name": reserve[1]
            }
        }

    # ALL RESERVES
    reserves = db.execute(text("""
        SELECT 
            reserve_id,
            name,
            ST_AsGeoJSON(geometry)
        FROM forest_reserves
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

@router.get("/deforestation-tile")
def get_deforestation_tile(
    level: str,
    entity_id: str,
    year: int,
    db: Session = Depends(get_db)
):

    # initialize EE (important)
    from app.services.gee.ee_init import initialize_ee
    initialize_ee()

    # get geometry
    if level == "county":
        entities = get_counties(db)
    elif level == "subcounty":
        entities = get_subcounties(db)
    elif level == "ward":
        entities = get_wards(db)
    else:
        return {"error": "Invalid level"}

    entity = next((e for e in entities if str(e["id"]) == entity_id), None)

    if not entity:
        return {"error": "Entity not found"}

    geojson = json.loads(entity["geometry"])
    ee_geom = ee.Geometry(geojson)

    # get tile
    tile_url = get_hansen_loss_tile(ee_geom, year)

    return {
        "year": year,
        "tile_url": tile_url
    }