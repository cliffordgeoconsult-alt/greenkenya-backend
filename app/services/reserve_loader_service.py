# app/services/reserve_loader_service.py
import json
import uuid
from sqlalchemy.orm import Session
from shapely.geometry import shape
from sqlalchemy import text

def load_forest_reserves(db: Session, geojson_path: str):
    with open(geojson_path) as f:
        data = json.load(f)
    inserted = 0
    for feature in data["features"]:
        props = feature.get("properties", {})
        geom = feature.get("geometry")
        name = props.get("name") or props.get("NAME") or "Unknown"
        shapely_geom = shape(geom)

        try:
            existing = db.execute(text("""
                SELECT 1 FROM forest_reserves
                WHERE name = :name
                LIMIT 1
            """), {"name": name}).fetchone()

            if existing:
                continue

            # 🔥 KEEP YOUR ORIGINAL INSERT BELOW
            db.execute(
                text("""
                    INSERT INTO forest_reserves (
                        reserve_id, name, source, area_ha, geometry
                    )
                    VALUES (
                        :id,
                        :name,
                        :source,
                        ST_Area(ST_GeomFromText(:geom, 4326)::geography) / 10000,
                        ST_GeomFromText(:geom, 4326)
                    )
                """),
                {
                    "id": str(uuid.uuid4()),
                    "name": name,
                    "source": "GeoJSON",
                    "geom": shapely_geom.wkt
                }
            )

            inserted += 1
        except Exception as e:
            print("INSERT ERROR:", e)
            db.rollback()
    db.commit()
    return {
        "message": "Forest reserves loaded",
        "total": inserted
    }