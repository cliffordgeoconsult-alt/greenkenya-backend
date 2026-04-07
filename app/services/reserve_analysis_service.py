import uuid
import ee
import json
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.services.gee.ee_init import initialize_ee


def compute_reserve_forests(db: Session):

    print("Computing reserve forest + canopy analysis...")

    initialize_ee()

    #  Hansen dataset
    hansen = ee.Image("UMD/hansen/global_forest_change_2024_v1_12")
    treecover = hansen.select("treecover2000")

    canopy30 = treecover.gte(30)
    canopy50 = treecover.gte(50)

    pixel_area = ee.Image.pixelArea()

    #  clear previous results
    db.execute(text("TRUNCATE TABLE reserve_forests RESTART IDENTITY"))
    db.commit()

    #  fetch reserves
    reserves = db.execute(text("""
        SELECT reserve_id, name, geometry, ST_AsGeoJSON(geometry)
        FROM forest_reserves
    """)).fetchall()

    print("Total reserves fetched:", len(reserves))

    total = 0

    for r in reserves:

        reserve_id = r[0]
        reserve_name = r[1]
        reserve_geom = r[2]
        reserve_geojson = r[3]

        try:

            # 1. POLYGON FOREST (PostGIS)
            result = db.execute(text("""
                WITH forest_intersections AS (
                    SELECT
                        ST_Intersection(f.geometry, r.geometry) AS geom
                    FROM forests f
                    JOIN forest_reserves r ON r.reserve_id = :rid
                    WHERE ST_Intersects(f.geometry, r.geometry)
                ),
                merged AS (
                    SELECT ST_UnaryUnion(ST_Collect(geom)) AS geom FROM forest_intersections
                )
                SELECT
                    ST_Area(merged.geom::geography) / 10000 AS forest_area,
                    ST_Area(r.geometry::geography) / 10000 AS reserve_area,
                    merged.geom AS forest_geom,
                    r.geometry AS reserve_geom
                FROM merged
                JOIN forest_reserves r ON r.reserve_id = :rid
            """), {"rid": reserve_id}).fetchone()

            if result is None or result[0] is None:
                forest_area = 0
                reserve_area = 0
                forest_geom = None
                reserve_geom = None
            else:
                forest_area = result[0] or 0
                reserve_area = result[1] or 0
                forest_geom = result[2]
                reserve_geom = result[3]
            coverage = (forest_area / reserve_area) * 100 if reserve_area > 0 else 0

            # 2. CANOPY (GEE)
            ee_geom = ee.Geometry(json.loads(reserve_geojson))

            area30 = canopy30.multiply(pixel_area).reduceRegion(
                reducer=ee.Reducer.sum(),
                geometry=ee_geom,
                scale=30,
                maxPixels=1e13
            )

            area50 = canopy50.multiply(pixel_area).reduceRegion(
                reducer=ee.Reducer.sum(),
                geometry=ee_geom,
                scale=30,
                maxPixels=1e13
            )

            c30_raw = area30.getInfo().get("treecover2000")
            c50_raw = area50.getInfo().get("treecover2000")

            canopy30_val = (c30_raw or 0) / 10000
            canopy50_val = (c50_raw or 0) / 10000
            #prevent duplicate reserve insert
            existing = db.execute(text("""
                SELECT 1 FROM reserve_forests WHERE reserve_id = :rid LIMIT 1
            """), {"rid": reserve_id}).fetchone()

            if existing:
                continue
            #  STORE (ONE ROW PER RESERVE)
            db.execute(text("""
                INSERT INTO reserve_forests (
                    id,
                    reserve_id,
                    forest_area_ha,
                    reserve_area_ha,
                    coverage_pct,
                    canopy_30_area_ha,
                    canopy_50_area_ha,
                    forest_geometry,
                    reserve_geometry
                )
                VALUES (
                    :id,
                    :rid,
                    :fa,
                    :ra,
                    :cov,
                    :c30,
                    :c50,
                    :fg,
                    :rg
                )
            """), {
                "id": str(uuid.uuid4()),
                "rid": reserve_id,
                "fa": forest_area,
                "ra": reserve_area,
                "cov": coverage,
                "c30": canopy30_val,
                "c50": canopy50_val,
                "fg": forest_geom,
                "rg": reserve_geom
            })

            total += 1

            print(
                f"{reserve_name} | "
                f"Forest: {round(forest_area,1)} ha | "
                f"C30: {round(canopy30_val,1)} | "
                f"C50: {round(canopy50_val,1)}"
            )

        except Exception as e:
            print(f"ERROR processing {reserve_name}:", e)
            db.rollback()

    db.commit()

    print("Total processed:", total)


    #  FORMAT OUTPUT 
    results = db.execute(text("""
        SELECT 
            r.name,
            rf.forest_area_ha,
            rf.canopy_30_area_ha,
            rf.canopy_50_area_ha
        FROM reserve_forests rf
        JOIN forest_reserves r 
        ON rf.reserve_id = r.reserve_id
    """)).fetchall()

    formatted = []

    for row in results:
        formatted.append({
            "reserve": row[0],
            "polygon_forest": row[1],
            "canopy_30": row[2],
            "canopy_50": row[3]
        })

    return {
        "message": "Reserve forest + canopy analysis complete",
        "total": len(formatted),
        "data": formatted[:20]  # preview (can remove later)
    }