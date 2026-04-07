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
    get_dynamic_world_yearly,
    get_dynamic_world_monthly_current_year,
    get_sar_forest_yearly,
    get_sar_loss_yearly,
    get_sar_loss_monthly_current_year,
    get_forest_gain_total
)
from app.services.admin_service import get_counties
from app.services.admin_service import get_subcounties
from app.services.admin_service import get_wards

def run_vegetation_analysis(db):

    initialize_ee()

    counties = get_counties(db)[:1]  # test first 10

    results = []

    for county in counties:

        geojson = json.loads(county["geometry"])
        ee_geom = ee.Geometry(geojson)

        tree30_stats, tree50_stats = county_tree_cover_area(ee_geom)
        forest_stats = county_forest_area(ee_geom)

        tree30 = tree30_stats.getInfo().get("treecover2000", 0)
        tree50 = tree50_stats.getInfo().get("treecover2000", 0)
        forest_m2 = forest_stats.getInfo().get("treecover2000", 0)

        # YEARLY ANALYSIS 
        stats = get_loss_histogram(ee_geom)
        yearly_data = build_yearly_loss(stats)
        dw_yearly = get_dynamic_world_yearly(ee_geom)
        dw_monthly = get_dynamic_world_monthly_current_year(ee_geom)
        sar_loss_monthly = get_sar_loss_monthly_current_year(ee_geom)
        sar_yearly = get_sar_forest_yearly(ee_geom)
        sar_loss = get_sar_loss_yearly(ee_geom)

        # simple confidence logic
        confidence = "high"

        if not dw_yearly or not sar_yearly:
            confidence = "low"
        else:
            dw_latest = dw_yearly[-1]["coverage_ha"]
            sar_latest = sar_yearly[-1]["coverage_ha"]
            baseline = forest_m2 / 10000

            diff_dw = abs(dw_latest - baseline)
            diff_sar = abs(sar_latest - baseline)

            if diff_dw > 3000 or diff_sar > 3000:
                confidence = "low"
            elif diff_dw > 1500 or diff_sar > 1500:
                confidence = "medium"
            # ✅ COMBINE TREE COVER (REALISTIC)
            combined_tree_cover = None

    if dw_yearly and sar_yearly:
        dw_latest = dw_yearly[-1]["coverage_ha"]
        sar_latest = sar_yearly[-1]["coverage_ha"]

        # 🔥 WEIGHTED FUSION (REALISTIC)
        combined_tree_cover = (
            0.7 * dw_latest +
            0.3 * sar_latest
        )
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
            "dynamic_world_yearly": dw_yearly,
            "dynamic_world_monthly": dw_monthly,
            "sar_yearly": sar_yearly,
            "sar_loss": sar_loss,
            "sar_loss_monthly": sar_loss_monthly,
            "combined_tree_cover_ha": combined_tree_cover,
            "confidence": confidence
        })

    save_intelligence(db, results, "county")
    return results

def run_ward_vegetation_analysis(db):

    initialize_ee()

    wards = get_wards(db)

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

    subcounties = get_subcounties(db)

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

    for r in reserves[:50]:  # test first

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

    for f in forests[:50]:

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