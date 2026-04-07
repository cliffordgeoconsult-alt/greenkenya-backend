# app/services/forest_registry_service.py
import json
import uuid
from sqlalchemy.orm import Session
from sqlalchemy import text
from shapely.geometry import shape
from geoalchemy2.shape import from_shape
import ee

from app.models.forest import Forest
from app.services.gee.ee_init import initialize_ee
from app.services.admin_service import get_counties
from app.services.gee.forest_baseline import detect_forest_baseline


def generate_forest_registry(db: Session):

    initialize_ee()

    print("Generating forest registry from BASELINE...")
    db.execute(text("TRUNCATE TABLE forests RESTART IDENTITY"))
    db.commit()

    counties = get_counties(db)

    total_inserted = 0

    for county in counties:

        print("Processing county:", county["name"])

        geojson = json.loads(county["geometry"])
        ee_geom = ee.Geometry(geojson)

        vectors = detect_forest_baseline(ee_geom)
        data = vectors.getInfo()

        print("Raw vector count:", len(data.get("features", [])))

        for i, feature in enumerate(data["features"]):

            geom = feature["geometry"]
            area = feature["properties"].get("sum", 0) / 10000

            if area < 5:
                continue

            try:
                # ✅ STRONG UNIQUE CODE (no collisions)
                forest_code = f"{county['name']}_{i}_{uuid.uuid4().hex[:8]}"

                new_forest = Forest(
                    forest_id=str(uuid.uuid4()),
                    forest_code=forest_code,
                    area_ha=round(area, 2),
                    county=county["name"],
                    geometry=from_shape(shape(geom), srid=4326),
                    baseline_year=2000,
                    source="Hansen_2023_v1_11",
                    confidence=0.9
                )

                existing = db.query(Forest).filter(
                    Forest.county == county["name"],
                    Forest.area_ha == round(area, 2)
                ).first()

                if existing:
                    continue

                db.add(new_forest)
                db.flush()

                total_inserted += 1

            except Exception as e:
                print("INSERT ERROR:", e)

                # ✅ CRITICAL FIX (reset broken session)
                db.rollback()

        db.commit()
        print("Committed for county:", county["name"])

    print(f"Inserted {total_inserted} forests")

    return {
        "message": "Baseline forest registry generated successfully",
        "total_forests": total_inserted
    }