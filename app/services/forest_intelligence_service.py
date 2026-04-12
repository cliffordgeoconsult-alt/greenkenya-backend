# app/services/forest_intelligence_service.py
import json
import ee
from sqlalchemy import text
from app.services.gee.ee_init import initialize_ee
from app.services.gee.forest_analysis import (
    county_tree_cover_area,
    county_forest_area,
    county_forest_area_by_year,
    county_loss_per_year,
    county_total_loss,
    get_loss_histogram, 
    build_yearly_loss,
    get_forest_gain_total
)
from app.services.admin_service import get_counties
from app.services.admin_service import get_subcounties
from app.services.admin_service import get_wards
from app.services.radd_analytics_service import (
    get_radd_yearly,
    get_radd_monthly_current_year
)
from app.services.radd_query_service import get_radd_loss_for_geometry

def run_vegetation_analysis(db):

    initialize_ee()

    counties = get_counties(db)[:10]  # testing first 10

    results = []

    for county in counties:

        geojson = json.loads(county["geometry"])
        ee_geom = ee.Geometry(geojson)
        radd_loss_ha = get_radd_loss_for_geometry(
            db,
            json.dumps(geojson)
        )
        radd_yearly = get_radd_yearly(db, json.dumps(geojson))
        radd_monthly = get_radd_monthly_current_year(db, json.dumps(geojson))

        tree30_stats, tree50_stats = county_tree_cover_area(ee_geom)
        forest_stats = county_forest_area(ee_geom)

        tree30 = tree30_stats.getInfo().get("treecover2000", 0)
        tree50 = tree50_stats.getInfo().get("treecover2000", 0)
        forest_m2 = forest_stats.getInfo().get("treecover2000", 0)

        # YEARLY ANALYSIS 
        stats = get_loss_histogram(ee_geom)
        yearly_data = build_yearly_loss(stats)

        # simple confidence logic
        confidence = "high"

        gain_stats = get_forest_gain_total(ee_geom)
        gain_m2 = gain_stats.getInfo().get("gain", 0)
        gain_ha = round(gain_m2 / 10000, 2)
        results.append({
            "county": county["name"],
            "canopy_30_ha": round(tree30 / 10000, 2),
            "canopy_50_ha": round(tree50 / 10000, 2),
            "forest_area_ha": round(forest_m2 / 10000, 2),
            "forest_gain_2000_2012_ha": gain_ha,
            "yearly_forest": yearly_data,
            "radd_loss_ha": round(radd_loss_ha, 2),
            "radd_yearly": radd_yearly,
            "radd_monthly": radd_monthly,
            "confidence": confidence
        })

    save_intelligence(db, results, "county")
    return results

def run_ward_vegetation_analysis(db):

    initialize_ee()

    wards = get_wards(db)[:1]

    results = []

    for ward in wards:

        geojson = json.loads(ward["geometry"])
        ee_geom = ee.Geometry(geojson)

        tree30_stats, tree50_stats = county_tree_cover_area(ee_geom)
        forest_stats = county_forest_area(ee_geom)

        tree30 = tree30_stats.getInfo().get("treecover2000", 0)
        tree50 = tree50_stats.getInfo().get("treecover2000", 0)
        forest_m2 = forest_stats.getInfo().get("treecover2000", 0)

        stats = get_loss_histogram(ee_geom)
        yearly_data = build_yearly_loss(stats)
        results.append({
            "ward": ward["name"],
            "canopy_30_ha": round(tree30 / 10000, 2),
            "canopy_50_ha": round(tree50 / 10000, 2),
            "forest_area_ha": round(forest_m2 / 10000, 2),
            "yearly_forest": yearly_data
        })

    save_intelligence(db, results, "ward")
    return results


def run_subcounty_vegetation_analysis(db):

    initialize_ee()

    subcounties = get_subcounties(db)[:1]

    results = []

    for sub in subcounties:

        geojson = json.loads(sub["geometry"])
        ee_geom = ee.Geometry(geojson)

        tree30_stats, tree50_stats = county_tree_cover_area(ee_geom)
        forest_stats = county_forest_area(ee_geom)

        tree30 = tree30_stats.getInfo().get("treecover2000", 0)
        tree50 = tree50_stats.getInfo().get("treecover2000", 0)
        forest_m2 = forest_stats.getInfo().get("treecover2000", 0)
        stats = get_loss_histogram(ee_geom)
        yearly_data = build_yearly_loss(stats)
        results.append({
            "subcounty": sub["name"],
            "canopy_30_ha": round(tree30 / 10000, 2),
            "canopy_50_ha": round(tree50 / 10000, 2),
            "forest_area_ha": round(forest_m2 / 10000, 2),
            "yearly_forest": yearly_data
        })

    save_intelligence(db, results, "subcounty")
    return results

def run_national_vegetation_analysis(db):

    initialize_ee()

    query = """
    SELECT ST_AsGeoJSON(ST_Union(geometry)) as geojson
    FROM admin_county
    """

    result = db.execute(text(query)).fetchone()

    kenya_geom = ee.Geometry(json.loads(result.geojson))

    tree30_stats, tree50_stats = county_tree_cover_area(kenya_geom)
    forest_stats = county_forest_area(kenya_geom)

    tree30 = tree30_stats.getInfo().get("treecover2000", 0)
    tree50 = tree50_stats.getInfo().get("treecover2000", 0)
    forest_m2 = forest_stats.getInfo().get("treecover2000", 0)

    stats = get_loss_histogram(kenya_geom)
    yearly_data = build_yearly_loss(stats)
    result = {
        "country": "Kenya",
        "canopy_30_ha": round(tree30 / 10000, 2),
        "canopy_50_ha": round(tree50 / 10000, 2),
        "forest_area_ha": round(forest_m2 / 10000, 2),
        "yearly_forest": yearly_data
    }

    save_intelligence(db, [result], "national")
    return result

def run_reserve_loss_analysis(db):
    initialize_ee()
    reserves = db.execute(text("""
        SELECT reserve_id, name, ST_AsGeoJSON(geometry)
        FROM forest_reserves
    """)).fetchall()

    results = []

    for r in reserves[:10]:  # test first 10

        reserve_id = r[0]
        name = r[1]
        geojson = json.loads(r[2])

        ee_geom = ee.Geometry(geojson)

        # BASELINE FOREST (30% reporting standard)
        forest_stats = county_forest_area(ee_geom)
        raw = forest_stats.getInfo().get("treecover2000")
        forest_m2 = raw or 0
        stats = get_loss_histogram(ee_geom)
        yearly_data = build_yearly_loss(stats)
        baseline_ha = round(forest_m2 / 10000, 2)
        total_loss_ha = yearly_data[-1]["loss_total_ha"] if yearly_data else 0
        loss_pct = (total_loss_ha / baseline_ha * 100) if baseline_ha > 0 else 0

        results.append({
            "reserve_id": reserve_id,
            "name": name,
            "baseline_forest_ha": baseline_ha,
            "total_loss_ha": total_loss_ha,
            "loss_pct": round(loss_pct, 2),
            "yearly_loss": yearly_data
        })

    save_intelligence(db, results, "reserve")
    return results

def run_non_reserve_forest_analysis(db):

    initialize_ee()

    forests = db.execute(text("""
        SELECT 
            f.forest_id, 
            f.forest_code,
            f.county,
            ST_AsGeoJSON(f.geometry)
        FROM forests f
        WHERE NOT EXISTS (
            SELECT 1 FROM forest_reserves r
            WHERE ST_Intersects(f.geometry, r.geometry)
        )
    """)).fetchall()

    results = []

    for f in forests[:10]:

        forest_id = f[0]
        forest_code = f[1]
        county = f[2]
        geojson = json.loads(f[3])

        ee_geom = ee.Geometry(geojson)

        # BASELINE
        forest_stats = county_forest_area(ee_geom)
        raw = forest_stats.getInfo().get("treecover2000")
        forest_m2 = raw or 0
        stats = get_loss_histogram(ee_geom)
        yearly_data = build_yearly_loss(stats)
        # ADD % LOSS (VERY IMPORTANT)
        total_loss_ha = yearly_data[-1]["loss_total_ha"] if yearly_data else 0
        baseline_ha = round(forest_m2 / 10000, 2)

        loss_pct = (total_loss_ha / baseline_ha * 100) if baseline_ha > 0 else 0

        results.append({
            "forest_id": forest_id,
            "forest_code": forest_code,
            "county": county,
            "baseline_forest_ha": baseline_ha,
            "total_loss_ha": total_loss_ha,
            "loss_pct": round(loss_pct, 2),
            "yearly_loss": yearly_data
        })

    save_intelligence(db, results, "non_reserve_forest")
    return results

def run_forest_intelligence(db):

    initialize_ee()

    forests = db.execute(text("""
        SELECT 
            forest_id,
            forest_code,
            county,
            ST_AsGeoJSON(geometry)
        FROM forests
        LIMIT 100
    """)).fetchall()

    results = []

    for f in forests:

        forest_id = f[0]
        forest_code = f[1]
        county = f[2]
        geojson = json.loads(f[3])

        ee_geom = ee.Geometry(geojson)

        # BASELINE
        forest_stats = county_forest_area(ee_geom)
        raw = forest_stats.getInfo().get("treecover2000")
        baseline_ha = (raw or 0) / 10000

        # LOSS
        stats = get_loss_histogram(ee_geom)
        yearly = build_yearly_loss(stats)

        total_loss = yearly[-1]["loss_total_ha"] if yearly else 0

        loss_pct = (total_loss / baseline_ha * 100) if baseline_ha > 0 else 0

        # RADD
        radd_yearly = get_radd_yearly(db, json.dumps(geojson))

        alerts_count = sum([y.get("loss_ha", 0) for y in radd_yearly]) if radd_yearly else 0

        # RESERVE CHECK
        reserve = db.execute(text("""
            SELECT name
            FROM forest_reserves
            WHERE ST_Intersects(
                forest_reserves.geometry,
                ST_SetSRID(ST_GeomFromGeoJSON(:geom), 4326)
            )
            LIMIT 1
        """), {"geom": json.dumps(geojson)}).fetchone()

        if reserve:
            reserve_name = reserve[0]
            is_protected = True
        else:
            reserve_name = None
            is_protected = False

        # RISK
        if loss_pct > 30 or alerts_count > 20:
            risk = "high"
        elif loss_pct > 10 or alerts_count > 5:
            risk = "medium"
        else:
            risk = "low"

        results.append({
            "forest_id": forest_id,
            "forest_code": forest_code,
            "county": county,
            "is_protected": is_protected,
            "reserve_name": reserve_name,
            "baseline_ha": round(baseline_ha, 2),
            "yearly_loss": yearly,
            "loss_ha": round(total_loss, 2),
            "loss_pct": round(loss_pct, 2),
            "alerts": alerts_count,
            "risk": risk
        })
    return results

def save_intelligence(db, results, level):

    # clear previous for this level
    inserted = 0

    for r in results:

        entity_id = (
            r.get("forest_id")
            or r.get("reserve_id")
            or r.get("county")
            or r.get("ward")
            or r.get("subcounty")
        )

        entity_id = str(entity_id) if entity_id is not None else None

        existing = db.execute(text("""
            SELECT 1 FROM forest_analysis_results
            WHERE level = :level AND entity_id = :entity_id
            LIMIT 1
        """), {
            "level": level,
            "entity_id": entity_id
        }).fetchone()

        if existing:
            continue

        db.execute(text("""
            INSERT INTO forest_analysis_results (
                id,
                level,
                entity_id,
                name,
                baseline_forest_ha,
                total_loss_ha,
                loss_pct,
                yearly_loss
            )
            VALUES (
                gen_random_uuid(),
                :level,
                :entity_id,
                :name,
                :baseline,
                :total_loss,
                :loss_pct,
                :yearly
            )
        """), {
            "level": level,
            "entity_id": entity_id,
            "name": r.get("forest_code") or r.get("name") or r.get("county") or r.get("ward") or r.get("subcounty"),
            "baseline": r.get("baseline_forest_ha", r.get("forest_area_ha", 0)),
            "total_loss": r.get("total_loss_ha", 0),
            "loss_pct": r.get("loss_pct", 0),
            "yearly": json.dumps(
                r.get("yearly_loss") or r.get("yearly_forest") or []
            )
        })

        inserted += 1
    db.commit()

    return {
    "message": f"{level} saved",
    "inserted": inserted,
    "skipped": len(results) - inserted
}