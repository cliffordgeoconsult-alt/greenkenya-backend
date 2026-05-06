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
    get_forest_gain_total,
    get_dw_tree_probability,
    calculate_dw_transition,
    calculate_yearly_coverage,
    # calculate_confirmed_deforestation
)
from app.services.admin_service import get_counties
from app.services.admin_service import get_subcounties
from app.services.admin_service import get_wards
from app.services.radd_analytics_service import (
    get_radd_daily,
    get_radd_yearly,
    get_radd_monthly_current_year
)
from app.services.radd_query_service import get_radd_alerts_count
from app.services.ai_service import generate_ai_insight
from app.services.carbon_service import calculate_net_carbon
from datetime import datetime, timedelta
from app.core.cache import redis_cache

today = datetime.now().strftime('%Y-%m-%d')
this_month_start = datetime.now().strftime('%Y-%m-01')

@redis_cache("radd_daily", ttl=300)
def cached_radd_daily(db, geojson):
    return get_radd_daily(db, geojson)


@redis_cache("radd_yearly", ttl=86400)
def cached_radd_yearly(db, geojson):
    return get_radd_yearly(db, geojson)


@redis_cache("radd_monthly", ttl=3600)
def cached_radd_monthly(db, geojson):
    return get_radd_monthly_current_year(db, geojson)


@redis_cache("radd_count", ttl=300)
def cached_radd_count(db, geojson):
    return get_radd_alerts_count(db, geojson)

def calculate_risk(loss_pct, alerts_total, recent_alerts):

    if alerts_total > 10000 or recent_alerts > 500:
        return "critical"
    elif alerts_total > 2000 or recent_alerts > 100:
        return "high"
    elif alerts_total > 500:
        return "medium"
    else:
        return "low"

@redis_cache("county_analysis", ttl=3600)
def process_county_cached(county, geojson, db):

    import ee
    from app.services.gee.forest_analysis import (
        county_tree_cover_area,
        county_forest_area,
        get_loss_histogram,
        build_yearly_loss,
        get_forest_gain_total,
        calculate_dw_transition,
        calculate_yearly_coverage
    )

    ee_geom = ee.Geometry(geojson)

    tree_data = county_tree_cover_area(ee_geom)
    forest_data = county_forest_area(ee_geom)

    tree30 = tree_data["tree30"]
    tree50 = tree_data["tree50"]
    forest_m2 = forest_data["forest_m2"]

    stats = get_loss_histogram(ee_geom)
    yearly_data = build_yearly_loss(stats)

    gain_data = get_forest_gain_total(ee_geom)
    gain_m2 = gain_data["gain"]

    dw = calculate_dw_transition(ee_geom, 2020, 2025)

    coverage = calculate_yearly_coverage(
        ee_geom,
        county["name"],
        2017,
        2026
    )

    return {
        "tree30": tree30,
        "tree50": tree50,
        "forest_m2": forest_m2,
        "yearly": yearly_data,
        "gain_m2": gain_m2,
        "regrowth": dw.get("regrowth_ha", 0),
        "coverage": coverage
    }

def run_vegetation_analysis(db, level=None, entity_id=None, prewarm=False):
    initialize_ee()

    if level == "county" and entity_id:
        counties = get_counties(db)
        counties = [c for c in counties if str(c["id"]) == str(entity_id)]

        if not counties:
            return {"error": "County not found"}
    else:
        counties = get_counties(db)
    results = []

    # --- AUTO-UPDATE DATE LOGIC ---
    now = datetime.now()
    this_month_start = now.strftime('%Y-%m-01')
    today = now.strftime('%Y-%m-%d')

    for county in counties:
        geojson = json.loads(county["geometry"])
        ee_geom = ee.Geometry(geojson)
        
        # 1. RADD REAL-TIME LOSS
        alerts_count = cached_radd_count(db, json.dumps(geojson, sort_keys=True))
        radd_daily = cached_radd_daily(db, json.dumps(geojson, sort_keys=True))
        radd_yearly = cached_radd_yearly(db, json.dumps(geojson, sort_keys=True))
        radd_monthly = cached_radd_monthly(db, json.dumps(geojson, sort_keys=True))

        # 2. HANSEN BASELINE (Historical)
        tree_data = county_tree_cover_area(ee_geom)
        forest_data = county_forest_area(ee_geom)

        tree30 = tree_data["tree30"]
        tree50 = tree_data["tree50"]
        forest_m2 = forest_data["forest_m2"]

        # 3. YEARLY LOSS ANALYSIS (Hansen)
        stats = get_loss_histogram(ee_geom)
        yearly_data = build_yearly_loss(stats)

        # 4. TOTAL LOSS & GAIN
        total_loss_ha = yearly_data[-1]["loss_total_ha"] if yearly_data else 0
        baseline_ha = round(forest_m2 / 10000, 2)
        loss_pct = (total_loss_ha / baseline_ha * 100) if baseline_ha > 0 else 0

        gain_data = get_forest_gain_total(ee_geom)
        gain_m2 = gain_data["gain"]
        gain_ha = round(gain_m2 / 10000, 2)

        # --- 5. DYNAMIC WORLD (REGROWTH & VITALITY) ---
        
        # Long-term Transition (2020 - 2026 monitoring)
        # Note: 2026 DW data is pulled dynamically as it becomes available
        if prewarm:
            dw_transitions = {"regrowth_ha": 0}
        else:
            dw_transitions = calculate_dw_transition(ee_geom, 2020, 2025)
        regrowth_ha = dw_transitions.get("regrowth_ha", 0)

        
        # --- NEW: YEARLY COVERAGE ---
        if prewarm:
            yearly_coverage = []
            latest_coverage_ha = None
        else:
            yearly_coverage = calculate_yearly_coverage(
                ee_geom,
                county["name"],
                2017,
                2026
            )

            valid = [x for x in yearly_coverage if x["forest_extent_ha"] is not None]

            latest_coverage_ha = valid[-1]["forest_extent_ha"] if valid else None

        # --- COMPUTE CONFIRMED DEFORESTATION ---
        year_start = f"{datetime.now().year}-01-01"

        # confirmed_deforestation_ha = calculate_confirmed_deforestation(
        #     ee_geom,
        #     year_start,
        #     today
        # )
        # month_start = datetime.now().strftime('%Y-%m-01')

        # confirmed_deforestation_month_ha = calculate_confirmed_deforestation(
        #     ee_geom,
        #     month_start,
        #     today
        # )

        # -----------------------------------
        # CARBON CALCULATIONS
        # -----------------------------------

        # Temporary density (replace later with real lookup)
        carbon_density = 35  

        carbon_loss_tonnes = total_loss_ha * carbon_density
        carbon_gain_tonnes = regrowth_ha * carbon_density

        net_carbon = calculate_net_carbon(
            carbon_gain_tonnes,
            carbon_loss_tonnes
        )

        # 6. RISK & ALERTS
        alerts_total = sum([y["alerts"] for y in radd_yearly]) if radd_yearly else 0

        recent_alerts = sum([d["alerts"] for d in radd_daily]) if radd_daily else 0

        risk = calculate_risk(loss_pct, alerts_total, recent_alerts)

        # -----------------------------------
        # CARBON ALERT
        # -----------------------------------
        def carbon_risk_alert(density, alerts):
            if density > 80 and alerts > 100:
                return "high_carbon_under_threat"
            elif density > 50 and alerts > 50:
                return "carbon_risk"
            return "stable"

        carbon_alert = carbon_risk_alert(carbon_density, alerts_total)

        # 7. ASSEMBLE RESULTS
        results.append({
            "county": county["name"],
            "county_id": county.get("id"),
            # BASELINE
            "canopy_30_ha": round(tree30 / 10000, 2),
            "canopy_50_ha": round(tree50 / 10000, 2),
            "forest_area_ha": baseline_ha,
            "forest_gain_2000_2012_ha": gain_ha,
            
            # DYNAMIC WORLD (NEW MV FEATURES)
            "regrowth_ha": regrowth_ha, 
            "monitoring_month": this_month_start,
            "latest_coverage_ha": latest_coverage_ha,
            "coverage_timeseries": yearly_coverage,
            "coverage_note": "Approximate satellite-derived estimate",

            # HISTORICAL LOSS
            "yearly_forest": yearly_data,
            "total_loss_ha": round(total_loss_ha, 2),
            "loss_pct": round(loss_pct, 2),
            
            # RADD (REAL-TIME)
            "radd_daily": radd_daily,
            "radd_yearly": radd_yearly,
            "radd_monthly": radd_monthly,
            "alerts": alerts_count,
            "alerts_total": alerts_total,
            # "confirmed_deforestation_ha": confirmed_deforestation_ha,
            # "confirmed_deforestation_month_ha": confirmed_deforestation_month_ha,
            
            # CARBON
            # CARBON
            "net_carbon_tonnes": net_carbon["net_carbon_tonnes"],
            "carbon_status": net_carbon["carbon_status"],

            # ALERT
            "carbon_alert": carbon_alert,
            
            # STATUS
            "risk": risk,
            "confidence": "seasonally_calibrated"
        })
    
    save_intelligence(db, results, "county")
    return results

def _ward_gee_core(ward, geojson, prewarm=False):
    """Hansen + baseline forest metrics; skips Dynamic World when prewarm=True (no Redis)."""
    ee_geom = ee.Geometry(geojson)

    tree_data = county_tree_cover_area(ee_geom)
    forest_data = county_forest_area(ee_geom)

    tree30 = tree_data["tree30"]
    tree50 = tree_data["tree50"]
    forest_m2 = forest_data["forest_m2"]

    stats = get_loss_histogram(ee_geom)
    yearly_data = build_yearly_loss(stats)

    total_loss_ha = yearly_data[-1]["loss_total_ha"] if yearly_data else 0
    baseline_ha = round(forest_m2 / 10000, 2)
    loss_pct = (total_loss_ha / baseline_ha * 100) if baseline_ha > 0 else 0

    gain_data = get_forest_gain_total(ee_geom)
    gain_m2 = gain_data["gain"]

    if prewarm:
        regrowth_ha = 0
        coverage = []
        latest_coverage = None
    else:
        dw = calculate_dw_transition(ee_geom, 2020, 2025)
        regrowth_ha = dw.get("regrowth_ha", 0)
        coverage = calculate_yearly_coverage(
            ee_geom,
            None,
            2020,
            2026
        )
        latest_coverage = coverage[-1]["forest_extent_ha"] if coverage else None

    return {
        "tree30": tree30,
        "tree50": tree50,
        "forest_m2": forest_m2,
        "yearly": yearly_data,
        "total_loss_ha": total_loss_ha,
        "loss_pct": loss_pct,
        "gain_m2": gain_m2,
        "regrowth": regrowth_ha,
        "coverage": coverage,
        "latest_coverage": latest_coverage
    }


@redis_cache("ward_analysis", ttl=3600)
def process_ward_cached(ward, geojson):
    return _ward_gee_core(ward, geojson, prewarm=False)


def run_ward_vegetation_analysis(db, entity_id=None, prewarm=False):
    initialize_ee()

    if entity_id:
        wards = get_wards(db)
        wards = [w for w in wards if str(w["id"]) == str(entity_id)]

        if not wards:
            return {"error": "Ward not found"}
    else:
        wards = get_wards(db)

    results = []

    now = datetime.now()
    this_month_start = now.strftime('%Y-%m-01')
    today = now.strftime('%Y-%m-%d')

    for ward in wards:

        geojson = json.loads(ward["geometry"])
        ee_geom = ee.Geometry(geojson)

        if prewarm:
            cached = _ward_gee_core(ward, geojson, prewarm=True)
        else:
            cached = process_ward_cached(ward, geojson)

        tree30 = cached["tree30"]
        tree50 = cached["tree50"]
        forest_m2 = cached["forest_m2"]

        yearly_data = cached["yearly"]
        total_loss_ha = cached["total_loss_ha"]
        loss_pct = cached["loss_pct"]

        gain_m2 = cached["gain_m2"]
        gain_ha = round(gain_m2 / 10000, 2)

        regrowth_ha = cached["regrowth"]
        yearly_coverage = cached["coverage"]
        latest_coverage_ha = cached["latest_coverage"]
        # RADD REAL-TIME
        alerts_count = cached_radd_count(db, json.dumps(geojson, sort_keys=True))
        radd_daily = cached_radd_daily(db, json.dumps(geojson, sort_keys=True))
        radd_yearly = cached_radd_yearly(db, json.dumps(geojson, sort_keys=True))
        radd_monthly = cached_radd_monthly(db, json.dumps(geojson, sort_keys=True))

        alerts_total = sum([y["alerts"] for y in radd_yearly]) if radd_yearly else 0

        year_start = f"{datetime.now().year}-01-01"

        # confirmed_deforestation_ha = calculate_confirmed_deforestation(
        #     ee_geom,
        #     year_start,
        #     today
        # )
        # month_start = datetime.now().strftime('%Y-%m-01')

        # confirmed_deforestation_month_ha = calculate_confirmed_deforestation(
        #     ee_geom,
        #     month_start,
        #     today
        # )

        # RISK
        recent_alerts = sum([d["alerts"] for d in radd_daily]) if radd_daily else 0

        risk = calculate_risk(loss_pct, alerts_total, recent_alerts)

        results.append({
            "ward": ward["name"],
            "ward_id": ward["id"],

            # BASELINE
            "canopy_30_ha": round(tree30 / 10000, 2),
            "canopy_50_ha": round(tree50 / 10000, 2),
            "forest_area_ha": round(forest_m2 / 10000, 2),
            "forest_gain_ha": gain_ha,

            # LOSS
            "yearly_forest": yearly_data,
            "total_loss_ha": round(total_loss_ha, 2),
            "loss_pct": round(loss_pct, 2),

            # DYNAMIC WORLD (NEW MV FEATURES)
            "regrowth_ha": regrowth_ha, 
            # "vitality_pct": current_vitality_pct,
            "monitoring_month": this_month_start,
            "latest_coverage_ha": latest_coverage_ha,
            "yearly_coverage": yearly_coverage,

            # RADD
            "radd_daily": radd_daily,
            "radd_yearly": radd_yearly,
            "radd_monthly": radd_monthly,
            "alerts": alerts_count,
            "alerts_total": alerts_total,
            # "confirmed_deforestation_ha": confirmed_deforestation_ha,
            # "confirmed_deforestation_month_ha": confirmed_deforestation_month_ha,

            # STATUS
            "risk": risk
        })

    save_intelligence(db, results, "ward")
    if entity_id:
        return results[0] if results else {}
    
def run_subcounty_vegetation_analysis(db, entity_id=None, prewarm=False):
    initialize_ee()

    if entity_id:
        subcounties = get_subcounties(db)
        subcounties = [s for s in subcounties if str(s["id"]) == str(entity_id)]

        if not subcounties:
            return {"error": "Subcounty not found"}
    else:
        subcounties = get_subcounties(db)

    results = []

    now = datetime.now()
    this_month_start = now.strftime('%Y-%m-01')
    today = now.strftime('%Y-%m-%d')

    for sub in subcounties:

        geojson = json.loads(sub["geometry"])
        ee_geom = ee.Geometry(geojson)

        # BASELINE
        tree_data = county_tree_cover_area(ee_geom)
        forest_data = county_forest_area(ee_geom)

        tree30 = tree_data["tree30"]
        tree50 = tree_data["tree50"]
        forest_m2 = forest_data["forest_m2"]

        # HISTORICAL LOSS
        stats = get_loss_histogram(ee_geom)
        yearly_data = build_yearly_loss(stats)

        total_loss_ha = yearly_data[-1]["loss_total_ha"] if yearly_data else 0
        baseline_ha = round(forest_m2 / 10000, 2)
        loss_pct = (total_loss_ha / baseline_ha * 100) if baseline_ha > 0 else 0

        # Long-term Transition (2020 - 2026 monitoring)
        # Note: 2026 DW data is pulled dynamically as it becomes available
        if prewarm:
            dw_transitions = {"regrowth_ha": 0}
        else:
            dw_transitions = calculate_dw_transition(ee_geom, 2020, 2025)
        regrowth_ha = dw_transitions.get("regrowth_ha", 0)

        # # Monthly Auto-Update: Calculate current month vitality
        # current_vitality_img = get_dw_tree_probability(ee_geom, this_month_start, today)
        # vitality_stats = current_vitality_img.reduceRegion(
        #     reducer=ee.Reducer.mean(),
        #     geometry=ee_geom,
        #     scale=30,  # Scaled for performance
        #     maxPixels=1e13
        # ).getInfo()
        
        # # Convert 0-1 probability to a percentage 0-100
        # current_vitality_pct = round((vitality_stats.get('trees', 0) * 100), 2)
        
        # --- NEW: YEARLY COVERAGE ---
        if prewarm:
            yearly_coverage = []
            latest_coverage_ha = None
        else:
            yearly_coverage = calculate_yearly_coverage(
                ee_geom,
                None,
                2020,
                2026
            )
            latest_coverage_ha = (
                yearly_coverage[-1]["forest_extent_ha"] if yearly_coverage else None
            )
        # RADD (REAL-TIME)
        alerts_count = cached_radd_count(db, json.dumps(geojson, sort_keys=True))
        radd_daily = cached_radd_daily(db, json.dumps(geojson, sort_keys=True))
        radd_yearly = cached_radd_yearly(db, json.dumps(geojson, sort_keys=True))
        radd_monthly = cached_radd_monthly(db, json.dumps(geojson, sort_keys=True))

        alerts_total = sum([y["alerts"] for y in radd_yearly]) if radd_yearly else 0

        year_start = f"{datetime.now().year}-01-01"

        # confirmed_deforestation_ha = calculate_confirmed_deforestation(
        #     ee_geom,
        #     year_start,
        #     today
        # )
        # month_start = datetime.now().strftime('%Y-%m-01')

        # confirmed_deforestation_month_ha = calculate_confirmed_deforestation(
        #     ee_geom,
        #     month_start,
        #     today
        # )

        # RISK
        recent_alerts = sum([d["alerts"] for d in radd_daily]) if radd_daily else 0

        risk = calculate_risk(loss_pct, alerts_total, recent_alerts)

        results.append({
            "subcounty": sub["name"],
            "subcounty_id": sub["id"],

            # BASELINE
            "canopy_30_ha": round(tree30 / 10000, 2),
            "canopy_50_ha": round(tree50 / 10000, 2),
            "forest_area_ha": round(forest_m2 / 10000, 2),

            # LOSS
            "yearly_forest": yearly_data,
            "total_loss_ha": round(total_loss_ha, 2),
            "loss_pct": round(loss_pct, 2),

            # DYNAMIC WORLD (NEW MV FEATURES)
            "regrowth_ha": regrowth_ha, 
            # "vitality_pct": current_vitality_pct,
            "monitoring_month": this_month_start,
            "latest_coverage_ha": latest_coverage_ha,
            "yearly_coverage": yearly_coverage,

            # RADD
            "radd_daily": radd_daily,
            "radd_yearly": radd_yearly,
            "radd_monthly": radd_monthly,
            "alerts": alerts_count,
            "alerts_total": alerts_total,
            # "confirmed_deforestation_ha": confirmed_deforestation_ha,
            # "confirmed_deforestation_month_ha": confirmed_deforestation_month_ha,

            # STATUS
            "risk": risk
        })
    save_intelligence(db, results, "subcounty")

    if entity_id:
        return results[0] if results else {}

def run_national_vegetation_analysis(db):

    initialize_ee()

    result = db.execute(text("""
        SELECT ST_AsGeoJSON(ST_Union(geometry)) as geojson
        FROM admin_county
    """)).fetchone()

    kenya_geom = ee.Geometry(json.loads(result.geojson))

    # BASELINE
    tree_data = county_tree_cover_area(kenya_geom)
    forest_data = county_forest_area(kenya_geom)

    tree30 = tree_data["tree30"]
    tree50 = tree_data["tree50"]
    forest_m2 = forest_data["forest_m2"]

    # HISTORICAL LOSS
    stats = get_loss_histogram(kenya_geom)
    yearly_data = build_yearly_loss(stats)

    total_loss_ha = yearly_data[-1]["loss_total_ha"] if yearly_data else 0
    baseline_ha = round(forest_m2 / 10000, 2)
    loss_pct = (total_loss_ha / baseline_ha * 100) if baseline_ha > 0 else 0

    # Long-term Transition (2020 - 2026 monitoring)
    # Note: 2026 DW data is pulled dynamically as it becomes available
    dw_transitions = calculate_dw_transition(kenya_geom, 2020, 2025)
    regrowth_ha = dw_transitions.get("regrowth_ha", 0)

    # # # Monthly Auto-Update: Calculate current month vitality
    # # current_vitality_img = get_dw_tree_probability(kenya_geom, this_month_start, today)
    # # vitality_stats = current_vitality_img.reduceRegion(
    # #     reducer=ee.Reducer.mean(),
    # #     geometry=kenya_geom,
    # #     scale=30,  # Scaled for performance
    # #     maxPixels=1e13
    # # ).getInfo()
        
    #     # Convert 0-1 probability to a percentage 0-100
    # current_vitality_pct = round((vitality_stats.get('trees', 0) * 100), 2)
        
    # --- NEW: YEARLY COVERAGE ---
    yearly_coverage = calculate_yearly_coverage(
        kenya_geom,
        None,
        2020,
        2026
    )
        
        # Get the latest coverage (2026) for quick display
    latest_coverage_ha = yearly_coverage[-1]["forest_extent_ha"]

    # ⚡ RADD
    alerts_count = cached_radd_count(db, result.geojson)
    radd_daily = cached_radd_daily(db, result.geojson)

    radd_yearly = cached_radd_yearly(db, result.geojson)
    radd_monthly = cached_radd_monthly(db, result.geojson)

    alerts_total = sum([y["alerts"] for y in radd_yearly]) if radd_yearly else 0

    year_start = f"{datetime.now().year}-01-01"

    # # confirmed_deforestation_ha = calculate_confirmed_deforestation(
    #     kenya_geom,
    #     year_start,
    #     today
    # )
    # month_start = datetime.now().strftime('%Y-%m-01')

    # confirmed_deforestation_month_ha = calculate_confirmed_deforestation(
    #         kenya_geom,
    #         month_start,
    #         today
    #     )

    # GAIN
    gain_data = get_forest_gain_total(kenya_geom)
    gain_m2 = gain_data["gain"]
    gain_ha = round(gain_m2 / 10000, 2)

    # RISK
    recent_alerts = sum([d["alerts"] for d in radd_daily]) if radd_daily else 0
    risk = calculate_risk(loss_pct, alerts_total, recent_alerts)

    result = {
        "country": "Kenya",

        # BASELINE
        "canopy_30_ha": round(tree30 / 10000, 2),
        "canopy_50_ha": round(tree50 / 10000, 2),
        "forest_area_ha": round(forest_m2 / 10000, 2),
        "forest_gain_ha": gain_ha,

        # LOSS
        "yearly_forest": yearly_data,
        "total_loss_ha": round(total_loss_ha, 2),
        "loss_pct": round(loss_pct, 2),

        # DYNAMIC WORLD (NEW MV FEATURES)
            "regrowth_ha": regrowth_ha, 
            # "vitality_pct": current_vitality_pct,
            "monitoring_month": this_month_start,
            "latest_coverage_ha": latest_coverage_ha,
            "yearly_coverage": yearly_coverage,

        # RADD
        "radd_daily": radd_daily,
        "radd_yearly": radd_yearly,
        "radd_monthly": radd_monthly,
        "alerts": alerts_count,
        "alerts_total": alerts_total,
        # "confirmed_deforestation_ha": confirmed_deforestation_ha,
        # "confirmed_deforestation_month_ha": confirmed_deforestation_month_ha,

        # STATUS
        "risk": risk
    }

    save_intelligence(db, [result], "national")
    return result

def _reserve_gee_core(name, geojson, prewarm=False):
    ee_geom = ee.Geometry(geojson)

    tree_data = county_tree_cover_area(ee_geom)
    forest_data = county_forest_area(ee_geom)

    tree30 = tree_data["tree30"]
    tree50 = tree_data["tree50"]
    forest_m2 = forest_data["forest_m2"]

    stats = get_loss_histogram(ee_geom)
    yearly_data = build_yearly_loss(stats)

    total_loss_ha = yearly_data[-1]["loss_total_ha"] if yearly_data else 0
    baseline_ha = round(forest_m2 / 10000, 2)
    loss_pct = (total_loss_ha / baseline_ha * 100) if baseline_ha > 0 else 0

    gain_data = get_forest_gain_total(ee_geom)
    gain_m2 = gain_data["gain"]

    if prewarm:
        regrowth_ha = 0
        coverage = []
        latest_coverage = None
    else:
        dw = calculate_dw_transition(ee_geom, 2020, 2025)
        regrowth_ha = dw.get("regrowth_ha", 0)
        coverage = calculate_yearly_coverage(
            ee_geom,
            name,
            2020,
            2026
        )
        latest_coverage = coverage[-1]["forest_extent_ha"] if coverage else None

    return {
        "tree30": tree30,
        "tree50": tree50,
        "forest_m2": forest_m2,
        "yearly": yearly_data,
        "total_loss_ha": total_loss_ha,
        "loss_pct": loss_pct,
        "gain_m2": gain_m2,
        "regrowth": regrowth_ha,
        "coverage": coverage,
        "latest_coverage": latest_coverage
    }


@redis_cache("reserve_analysis_prewarm", ttl=7 * 86400)
def process_reserve_prewarm_cached(reserve_id, name, geojson):
    """Hansen + RADD path without Dynamic World; cached so forest bundle skips repeat GEE."""
    return _reserve_gee_core(name, geojson, prewarm=True)


@redis_cache("reserve_analysis", ttl=3600)
def process_reserve_cached(reserve_id, name, geojson):
    return _reserve_gee_core(name, geojson, prewarm=False)


def run_reserve_loss_analysis(db, prewarm=False, reserve_id: str | None = None):

    if reserve_id:
        reserves = db.execute(
            text("""
                SELECT reserve_id, name, ST_AsGeoJSON(geometry)
                FROM forest_reserves
                WHERE reserve_id = :rid
            """),
            {"rid": reserve_id},
        ).fetchall()
        if not reserves:
            return []
    else:
        reserves = db.execute(text("""
            SELECT reserve_id, name, ST_AsGeoJSON(geometry)
            FROM forest_reserves
        """)).fetchall()

    initialize_ee()

    results = []

    for r in reserves:

        reserve_id = r[0]
        name = r[1]
        geojson = json.loads(r[2])

        ee_geom = ee.Geometry(geojson)

        if prewarm:
            cached = process_reserve_prewarm_cached(str(reserve_id), name, geojson)
        else:
            cached = process_reserve_cached(reserve_id, name, geojson)

        tree30 = cached["tree30"]
        tree50 = cached["tree50"]
        forest_m2 = cached["forest_m2"]

        yearly_data = cached["yearly"]
        total_loss_ha = cached["total_loss_ha"]
        loss_pct = cached["loss_pct"]

        gain_m2 = cached["gain_m2"]
        gain_ha = round(gain_m2 / 10000, 2)

        regrowth_ha = cached["regrowth"]
        yearly_coverage = cached["coverage"]
        latest_coverage_ha = cached["latest_coverage"]
        # RADD
        alerts_count = cached_radd_count(db, json.dumps(geojson, sort_keys=True))
        radd_daily = cached_radd_daily(db, json.dumps(geojson, sort_keys=True))

        radd_yearly = cached_radd_yearly(db, json.dumps(geojson, sort_keys=True))
        radd_monthly = cached_radd_monthly(db, json.dumps(geojson, sort_keys=True))

        alerts_total = sum([y["alerts"] for y in radd_yearly]) if radd_yearly else 0
        # 
        year_start = f"{datetime.now().year}-01-01"

        # confirmed_deforestation_ha = calculate_confirmed_deforestation(
        #     ee_geom,
        #     year_start,
        #     today
        # )
        # month_start = datetime.now().strftime('%Y-%m-01')

        # confirmed_deforestation_month_ha = calculate_confirmed_deforestation(
        #     ee_geom,
        #     month_start,
        #     today
        # )

        # RISK
        recent_alerts = sum([d["alerts"] for d in radd_daily]) if radd_daily else 0

        risk = calculate_risk(loss_pct, alerts_total, recent_alerts)

        results.append({
            "reserve_id": reserve_id,
            "name": name,
            # BASELINE
            "canopy_30_ha": round(tree30 / 10000, 2),
            "canopy_50_ha": round(tree50 / 10000, 2),
            "forest_area_ha": round(forest_m2 / 10000, 2),
            "forest_gain_ha": gain_ha,
            # LOSS
            "yearly_forest": yearly_data,
            "total_loss_ha": round(total_loss_ha, 2),
            "loss_pct": round(loss_pct, 2),

            # DYNAMIC WORLD (NEW MV FEATURES)
            "regrowth_ha": regrowth_ha, 
            # "vitality_pct": current_vitality_pct,
            "monitoring_month": this_month_start,
            "latest_coverage_ha": latest_coverage_ha,
            "yearly_coverage": yearly_coverage,

            # RADD
            "radd_daily": radd_daily,
            "radd_yearly": radd_yearly,
            "radd_monthly": radd_monthly,
            "alerts": alerts_count,
            "alerts_total": alerts_total,
            # "confirmed_deforestation_ha": confirmed_deforestation_ha,
            # "confirmed_deforestation_month_ha": confirmed_deforestation_month_ha,
            # STATUS
            "risk": risk
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

    for f in forests:

        forest_id = f[0]
        forest_code = f[1]
        county = f[2]
        geojson = json.loads(f[3])

        ee_geom = ee.Geometry(geojson)

        # BASELINE
        tree_data = county_tree_cover_area(ee_geom)
        forest_data = county_forest_area(ee_geom)

        tree30 = tree_data["tree30"]
        tree50 = tree_data["tree50"]
        forest_m2 = forest_data["forest_m2"]

        # LOSS
        stats = get_loss_histogram(ee_geom)
        yearly_data = build_yearly_loss(stats)

        total_loss_ha = yearly_data[-1]["loss_total_ha"] if yearly_data else 0
        baseline_ha = round(forest_m2 / 10000, 2)
        loss_pct = (total_loss_ha / baseline_ha * 100) if baseline_ha > 0 else 0

        # ⚡ RADD
        alerts_count = cached_radd_count(db, json.dumps(geojson, sort_keys=True))
        radd_daily = cached_radd_daily(db, json.dumps(geojson, sort_keys=True))
        radd_yearly = cached_radd_yearly(db, json.dumps(geojson, sort_keys=True))
        radd_monthly = cached_radd_monthly(db, json.dumps(geojson, sort_keys=True))

        alerts_total = sum([y["alerts"] for y in radd_yearly]) if radd_yearly else 0

        year_start = f"{datetime.now().year}-01-01"

        # confirmed_deforestation_ha = calculate_confirmed_deforestation(
        #     ee_geom,
        #     year_start,
        #     today
        # )
        # month_start = datetime.now().strftime('%Y-%m-01')

        # confirmed_deforestation_month_ha = calculate_confirmed_deforestation(
        #     ee_geom,
        #     month_start,
        #     today
        # )

        # GAIN
        gain_data = get_forest_gain_total(ee_geom)
        gain_m2 = gain_data["gain"]
        gain_ha = round(gain_m2 / 10000, 2)

        # RISK
        recent_alerts = sum([d["alerts"] for d in radd_daily]) if radd_daily else 0

        risk = calculate_risk(loss_pct, alerts_total, recent_alerts)

        results.append({
            "forest_id": forest_id,
            "forest_code": forest_code,
            "county": county,
            # BASELINE
            "canopy_30_ha": round(tree30 / 10000, 2),
            "canopy_50_ha": round(tree50 / 10000, 2),
            "forest_area_ha": round(forest_m2 / 10000, 2),
            "forest_gain_ha": gain_ha,
            # LOSS
            "yearly_forest": yearly_data,
            "total_loss_ha": round(total_loss_ha, 2),
            "loss_pct": round(loss_pct, 2),
            # RADD
            "radd_daily": radd_daily,
            "radd_yearly": radd_yearly,
            "radd_monthly": radd_monthly,
            "alerts": alerts_count,
            "alerts_total": alerts_total,
            # "confirmed_deforestation_ha": confirmed_deforestation_ha,
            # "confirmed_deforestation_month_ha": confirmed_deforestation_month_ha,


            # STATUS
            "risk": risk
        })

    save_intelligence(db, results, "non_reserve_forest")
    return results

def run_forest_intelligence(db, prewarm=False):

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
        tree_data = county_tree_cover_area(ee_geom)
        forest_data = county_forest_area(ee_geom)

        tree30 = tree_data["tree30"]
        tree50 = tree_data["tree50"]
        forest_m2 = forest_data["forest_m2"]

        baseline_ha = round(forest_m2 / 10000, 2)

        # LOSS
        stats = get_loss_histogram(ee_geom)
        yearly = build_yearly_loss(stats)

        total_loss = yearly[-1]["loss_total_ha"] if yearly else 0
        loss_pct = (total_loss / baseline_ha * 100) if baseline_ha > 0 else 0

        # Long-term Transition (2020 - 2026 monitoring)
        # Note: 2026 DW data is pulled dynamically as it becomes available
        if prewarm:
            dw_transitions = {"regrowth_ha": 0}
        else:
            dw_transitions = calculate_dw_transition(ee_geom, 2020, 2025)
        regrowth_ha = dw_transitions.get("regrowth_ha", 0)

        # # Monthly Auto-Update: Calculate current month vitality
        # current_vitality_img = get_dw_tree_probability(ee_geom, this_month_start, today)
        # vitality_stats = current_vitality_img.reduceRegion(
        #     reducer=ee.Reducer.mean(),
        #     geometry=ee_geom,
        #     scale=30,  # Scaled for performance
        #     maxPixels=1e13
        # ).getInfo()
        
        # # Convert 0-1 probability to a percentage 0-100
        # current_vitality_pct = round((vitality_stats.get('trees', 0) * 100), 2)
        
        # --- NEW: YEARLY COVERAGE ---
        if prewarm:
            yearly_coverage = []
            latest_coverage_ha = None
        else:
            yearly_coverage = calculate_yearly_coverage(
                ee_geom,
                None,
                2020,
                2026
            )
            latest_coverage_ha = (
                yearly_coverage[-1]["forest_extent_ha"] if yearly_coverage else None
            )
        # RADD
        alerts_count = cached_radd_count(db, json.dumps(geojson, sort_keys=True))
        radd_daily = cached_radd_daily(db, json.dumps(geojson, sort_keys=True))

        radd_yearly = cached_radd_yearly(db, json.dumps(geojson, sort_keys=True))
        radd_monthly = cached_radd_monthly(db, json.dumps(geojson, sort_keys=True))

        alerts_total = sum([y["alerts"] for y in radd_yearly]) if radd_yearly else 0

        year_start = f"{datetime.now().year}-01-01"

        # confirmed_deforestation_ha = calculate_confirmed_deforestation(
        #     ee_geom,
        #     year_start,
        #     today
        # )
        # month_start = datetime.now().strftime('%Y-%m-01')

        # confirmed_deforestation_month_ha = calculate_confirmed_deforestation(
        #     ee_geom,
        #     month_start,
        #     today
        # )

        # PROTECTION CHECK
        reserve = db.execute(text("""
            SELECT name
            FROM forest_reserves
            WHERE ST_Intersects(
                forest_reserves.geometry,
                ST_SetSRID(ST_GeomFromGeoJSON(:geom), 4326)
            )
            LIMIT 1
        """), {"geom": json.dumps(geojson, sort_keys=True)}).fetchone()

        is_protected = bool(reserve)
        reserve_name = reserve[0] if reserve else None

        # GAIN
        gain_data = get_forest_gain_total(ee_geom)
        gain_m2 = gain_data["gain"]
        gain_ha = round(gain_m2 / 10000, 2)

        # RISK LOGIC
        recent_alerts = sum([d["alerts"] for d in radd_daily]) if radd_daily else 0

        risk = calculate_risk(loss_pct, alerts_total, recent_alerts)

        results.append({
            "forest_id": forest_id,
            "forest_code": forest_code,
            "county": county,
            # PROTECTION
            "is_protected": is_protected,
            "reserve_name": reserve_name,
            # BASELINE
            "canopy_30_ha": round(tree30 / 10000, 2),
            "canopy_50_ha": round(tree50 / 10000, 2),
            "forest_area_ha": baseline_ha,
            "forest_gain_ha": gain_ha,
            # LOSS
            "yearly_forest": yearly,
            "total_loss_ha": round(total_loss, 2),
            "loss_pct": round(loss_pct, 2),

            # DYNAMIC WORLD (NEW MV FEATURES)
            "regrowth_ha": regrowth_ha, 
            # "vitality_pct": current_vitality_pct,
            "monitoring_month": this_month_start,
            "latest_coverage_ha": latest_coverage_ha,
            "yearly_coverage": yearly_coverage,

            # RADD
            "radd_daily": radd_daily,
            "radd_yearly": radd_yearly,
            "radd_monthly": radd_monthly,
            "alerts": alerts_count,
            "alerts_total": alerts_total,
            # "confirmed_deforestation_ha": confirmed_deforestation_ha,
            # "confirmed_deforestation_month_ha": confirmed_deforestation_month_ha,
            # STATUS
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
            or r.get("county_id")
            or r.get("ward_id")
            or r.get("subcounty_id")
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