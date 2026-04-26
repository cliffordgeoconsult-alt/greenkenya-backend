import ee
import json
from sqlalchemy import text
from app.services.gee.ee_init import initialize_ee
from app.services.gee.forest_analysis import (
    county_loss_per_year,
    get_loss_histogram, 
    build_yearly_loss
)

from datetime import datetime

CARBON_START_YEAR = 2020

LOSS_START_YEAR = 2001
LOSS_LATEST_OFFICIAL_YEAR = 2024

CURRENT_YEAR = datetime.utcnow().year
CURRENT_OFFICIAL_YEAR = CURRENT_YEAR - 1
# -----------------------------------
# GET ALL COUNTIES
# -----------------------------------
def fetch_counties(db):
    query = """
    SELECT
        id,
        name,
        ST_AsGeoJSON(geometry) AS geojson
    FROM admin_county
    """

    rows = db.execute(text(query))

    return rows.fetchall()

# GET ALL RESERVES - Pulls reserve boundaries from forest_reserves table
def fetch_reserves(db):
    query = """
    SELECT
        reserve_id,
        name,
        area_ha,
        ST_AsGeoJSON(geometry) AS geojson
    FROM forest_reserves
    ORDER BY name
    """

    rows = db.execute(text(query))
    return rows.fetchall()

def fetch_wards(db):
    query = """
    SELECT
        id,
        name,
        county_id,
        subcounty_id,
        ST_AsGeoJSON(geometry) AS geojson
    FROM admin_ward
    ORDER BY name
    """

    rows = db.execute(text(query))
    return rows.fetchall()

def get_available_carbon_years():

    return {
        "available_years": list(
            range(CARBON_START_YEAR, CURRENT_OFFICIAL_YEAR + 1)
        ),
        "latest_official_year": CURRENT_OFFICIAL_YEAR
    }

def build_entity_loss_trend(geom, density):

    stats = get_loss_histogram(geom)
    yearly = build_yearly_loss(stats)

    results = []

    cumulative_ha = 0
    cumulative_co2e = 0

    for row in yearly:
        year = row["year"]
        loss_ha = row["loss_year_ha"]
        co2e = loss_ha * density

        cumulative_ha += loss_ha
        cumulative_co2e += co2e

        results.append({
            "year": year,
            "loss_ha": round(loss_ha, 2),
            "co2e_emitted_tonnes": round(co2e, 2),
            "cumulative_loss_ha": round(cumulative_ha, 2),
            "cumulative_co2e_tonnes": round(cumulative_co2e, 2)
        })

    return results

def get_loss_biomass_image(year):
    """
    Biomass baseline nearest to selected loss year.
    GEDI starts recent years, so old years fallback to 2020.
    """

    end_year = max(min(year, CURRENT_OFFICIAL_YEAR), 2020)
    start_year = max(end_year - 1, 2020)

    return (
        ee.ImageCollection("LARSE/GEDI/GEDI04_A_002_MONTHLY")
        .filterDate(f"{start_year}-01-01", f"{end_year}-12-31")
        .select("agbd")
        .median()
    )

# COUNTY CARBON STATS
def get_county_carbon_stats(db, year=None):

    initialize_ee()
    if year is None:
        year = CURRENT_OFFICIAL_YEAR

    if year < CARBON_START_YEAR:
        return {
            "error": f"County carbon stats begin at {CARBON_START_YEAR}"
        }

    if year > CURRENT_OFFICIAL_YEAR:
        return {
            "error": f"{year} county carbon stats not yet available. Latest completed year is {CURRENT_OFFICIAL_YEAR}."
        }

    counties = fetch_counties(db)

    # GEDI biomass (recent baseline)
    biomass_start = max(year - 1, 2020)

    biomass_img = (
        ee.ImageCollection("LARSE/GEDI/GEDI04_A_002_MONTHLY")
        .filterDate(f"{biomass_start}-01-01", f"{year}-12-31")
        .select("agbd")
        .median()
    )

    results = []

    for row in counties[:3]:   # test first

        geom = ee.Geometry(json.loads(row.geojson))

        # -----------------------------------
        # Dynamic World tree probability
        # -----------------------------------
        dw = (
            ee.ImageCollection("GOOGLE/DYNAMICWORLD/V1")
            .filterDate(f"{year}-01-01", f"{year}-12-31")
            .filterBounds(geom)
        )

        tree_prob = dw.select("trees").mean()

        # -----------------------------------
        # Two metrics
        # -----------------------------------
        dense_forest = tree_prob.gte(0.6).rename("dense")
        tree_cover = tree_prob.gte(0.3).rename("cover")

        # -----------------------------------
        # Areas
        # -----------------------------------
        dense_area = dense_forest.multiply(
            ee.Image.pixelArea()
        ).reduceRegion(
            reducer=ee.Reducer.sum(),
            geometry=geom,
            scale=10,
            maxPixels=1e13
        )

        cover_area = tree_cover.multiply(
            ee.Image.pixelArea()
        ).reduceRegion(
            reducer=ee.Reducer.sum(),
            geometry=geom,
            scale=10,
            maxPixels=1e13
        )

        dense_m2 = dense_area.getInfo().get("dense", 0)
        cover_m2 = cover_area.getInfo().get("cover", 0)

        dense_ha = dense_m2 / 10000
        cover_ha = cover_m2 / 10000

        # -----------------------------------
        # Biomass uses dense forest mask
        # -----------------------------------
        masked_biomass = biomass_img.updateMask(dense_forest)

        pixel_area_ha = ee.Image.pixelArea().divide(10000)

        biomass_per_pixel = masked_biomass.multiply(pixel_area_ha)

        biomass = biomass_per_pixel.reduceRegion(
            reducer=ee.Reducer.sum(),
            geometry=geom,
            scale=25,
            maxPixels=1e13
        )

        biomass_tonnes = biomass.getInfo().get("agbd", 0)

        # -----------------------------------
        # Carbon formulas
        # -----------------------------------
        carbon_tonnes = biomass_tonnes * 0.47
        co2e_tonnes = carbon_tonnes * 3.67

        carbon_density = (
            co2e_tonnes / dense_ha if dense_ha > 0 else 0
        )

        results.append({
            "county_id": row.id,
            "county": row.name,
            "year": year,
            "dense_forest_ha": round(dense_ha, 2),
            "tree_cover_ha": round(cover_ha, 2),
            "biomass_tonnes": round(biomass_tonnes, 2),
            "carbon_tonnes": round(carbon_tonnes, 2),
            "co2e_tonnes": round(co2e_tonnes, 2),
            "carbon_density_tco2e_ha": round(carbon_density, 2)
        })

    # -----------------------------------
    # Rank by stored CO2e
    # -----------------------------------
    results.sort(
        key=lambda x: x["co2e_tonnes"],
        reverse=True
    )

    for i, item in enumerate(results, start=1):
        item["rank"] = i

    return results

def get_county_loss_stats(db, year):

    initialize_ee()

    if year is None:
        year = LOSS_LATEST_OFFICIAL_YEAR

    if year < LOSS_START_YEAR:
        return {
            "error": f"Loss data begins at {LOSS_START_YEAR}"
        }

    if year > LOSS_LATEST_OFFICIAL_YEAR:
        return {
            "year": year,
            "status": "currently unavailable",
            "message": f"Official annual forest-loss data for {year} is not yet available. We are working on the next release.",
            "latest_available_year": LOSS_LATEST_OFFICIAL_YEAR
        }

    counties = fetch_counties(db)

    biomass_img = get_loss_biomass_image(year)

    results = []

    for row in counties[:3]:   # test first

        geom = ee.Geometry(json.loads(row.geojson))

        # -----------------------------------
        # Forest loss area from your Hansen tool
        # -----------------------------------
        loss_stats = county_loss_per_year(geom, year).getInfo()

        loss_m2 = list(loss_stats.values())[0] if loss_stats else 0
        loss_ha = loss_m2 / 10000

        hansen = ee.Image("UMD/hansen/global_forest_change_2024_v1_12")
        lossyear = hansen.select("lossyear")

        forest2000 = hansen.select("treecover2000").gte(30)

        previous_loss = lossyear.gt(0).And(lossyear.lt(year - 2000))
        forest_remaining = forest2000.updateMask(previous_loss.Not())

        loss_mask = lossyear.eq(year - 2000).And(forest_remaining)

        pixel_area_ha = ee.Image.pixelArea().divide(10000)

        biomass_per_pixel = biomass_img.updateMask(loss_mask).multiply(pixel_area_ha)

        biomass = biomass_per_pixel.reduceRegion(
            reducer=ee.Reducer.sum(),
            geometry=geom,
            scale=25,
            maxPixels=1e13
        )

        biomass_lost = biomass.getInfo().get("agbd", 0)

        carbon_lost = biomass_lost * 0.47
        co2e_emitted = carbon_lost * 3.67

        results.append({
            "county_id": row.id,
            "county": row.name,
            "year": year,
            "loss_ha": round(loss_ha, 2),
            "biomass_lost_tonnes": round(biomass_lost, 2),
            "carbon_lost_tonnes": round(carbon_lost, 2),
            "co2e_emitted_tonnes": round(co2e_emitted, 2)
        })

    results.sort(
        key=lambda x: x["co2e_emitted_tonnes"],
        reverse=True
    )

    for i, item in enumerate(results, start=1):
        item["rank"] = i

    return results

def get_county_loss_trend(db, county_id):

    initialize_ee()

    counties = fetch_counties(db)

    density_lookup = build_county_density_lookup(db)

    for row in counties:
        if str(row.id) == str(county_id):

            geom = ee.Geometry(json.loads(row.geojson))

            density = density_lookup.get(row.name.upper(), 35)

            return {
                "county_id": str(row.id),
                "county": row.name,
                "trend": build_entity_loss_trend(geom, density)
            }

    return {"error": "County not found"}

def build_county_density_lookup(db):
    stats = get_county_carbon_stats(db)

    lookup = {}

    for row in stats:
        lookup[row["county"].upper()] = row["carbon_density_tco2e_ha"]

    return lookup


def get_ward_carbon_stats(db, year=None):

    initialize_ee()

    if year is None:
        year = CURRENT_OFFICIAL_YEAR

    wards = fetch_wards(db)

    biomass_start = max(year - 1, 2020)

    biomass_img = (
        ee.ImageCollection("LARSE/GEDI/GEDI04_A_002_MONTHLY")
        .filterDate(f"{biomass_start}-01-01", f"{year}-12-31")
        .select("agbd")
        .median()
    )

    results = []

    for row in wards[:25]:   # dev speed

        geom = ee.Geometry(json.loads(row.geojson))

        dw = (
            ee.ImageCollection("GOOGLE/DYNAMICWORLD/V1")
            .filterDate(f"{year}-01-01", f"{year}-12-31")
            .filterBounds(geom)
        )

        tree_prob = dw.select("trees").mean()

        dense = tree_prob.gte(0.6).rename("dense")
        cover = tree_prob.gte(0.3).rename("cover")

        dense_area = dense.multiply(
            ee.Image.pixelArea()
        ).reduceRegion(
            reducer=ee.Reducer.sum(),
            geometry=geom,
            scale=10,
            maxPixels=1e13
        )

        cover_area = cover.multiply(
            ee.Image.pixelArea()
        ).reduceRegion(
            reducer=ee.Reducer.sum(),
            geometry=geom,
            scale=10,
            maxPixels=1e13
        )

        dense_ha = dense_area.getInfo().get("dense", 0) / 10000
        cover_ha = cover_area.getInfo().get("cover", 0) / 10000

        biomass = biomass_img.updateMask(dense).multiply(
            ee.Image.pixelArea().divide(10000)
        ).reduceRegion(
            reducer=ee.Reducer.sum(),
            geometry=geom,
            scale=25,
            maxPixels=1e13
        )

        biomass_tonnes = biomass.getInfo().get("agbd", 0)

        carbon = biomass_tonnes * 0.47
        co2e = carbon * 3.67

        density = co2e / dense_ha if dense_ha > 0 else 0

        results.append({
            "ward_id": row.id,
            "ward": row.name,
            "year": year,
            "dense_forest_ha": round(dense_ha, 2),
            "tree_cover_ha": round(cover_ha, 2),
            "biomass_tonnes": round(biomass_tonnes, 2),
            "carbon_tonnes": round(carbon, 2),
            "co2e_tonnes": round(co2e, 2),
            "carbon_density_tco2e_ha": round(density, 2)
        })

    results.sort(key=lambda x: x["co2e_tonnes"], reverse=True)

    for i, r in enumerate(results, start=1):
        r["rank"] = i

    return results

def get_ward_loss_stats(db, year):

    initialize_ee()

    if year is None:
        year = LOSS_LATEST_OFFICIAL_YEAR

    wards = fetch_wards(db)

    biomass_img = get_loss_biomass_image(year)

    results = []

    for row in wards[:25]:

        geom = ee.Geometry(json.loads(row.geojson))

        loss_stats = county_loss_per_year(geom, year).getInfo()

        loss_m2 = list(loss_stats.values())[0] if loss_stats else 0
        loss_ha = loss_m2 / 10000

        hansen = ee.Image("UMD/hansen/global_forest_change_2024_v1_12")
        lossyear = hansen.select("lossyear")

        forest2000 = hansen.select("treecover2000").gte(30)

        previous_loss = lossyear.gt(0).And(lossyear.lt(year - 2000))
        remaining = forest2000.updateMask(previous_loss.Not())

        loss_mask = lossyear.eq(year - 2000).And(remaining)

        biomass = biomass_img.updateMask(loss_mask).multiply(
            ee.Image.pixelArea().divide(10000)
        ).reduceRegion(
            reducer=ee.Reducer.sum(),
            geometry=geom,
            scale=25,
            maxPixels=1e13
        )

        biomass_lost = biomass.getInfo().get("agbd", 0)

        carbon = biomass_lost * 0.47
        co2e = carbon * 3.67

        results.append({
            "ward_id": row.id,
            "ward": row.name,
            "year": year,
            "loss_ha": round(loss_ha, 2),
            "biomass_lost_tonnes": round(biomass_lost, 2),
            "carbon_lost_tonnes": round(carbon, 2),
            "co2e_emitted_tonnes": round(co2e, 2)
        })

    results.sort(key=lambda x: x["co2e_emitted_tonnes"], reverse=True)

    for i, r in enumerate(results, start=1):
        r["rank"] = i

    return results

def get_ward_loss_trend(db, ward_id):

    wards = fetch_wards(db)

    for row in wards:
        if str(row.id) == str(ward_id):

            geom = ee.Geometry(json.loads(row.geojson))

            return {
                "ward_id": str(row.id),
                "ward": row.name,
                "trend": build_entity_loss_trend(geom, 35)
            }

    return {"error": "Ward not found"}

# RESERVE CARBON STATS
def get_reserve_carbon_stats(db, year=None):

    initialize_ee()

    if year is None:
        year = CURRENT_OFFICIAL_YEAR

    if year < CARBON_START_YEAR:
        return {
            "error": f"Reserve carbon stats begin at {CARBON_START_YEAR}"
        }

    if year > CURRENT_OFFICIAL_YEAR:
        return {
            "error": f"{year} reserve carbon stats not yet available. Latest completed year is {CURRENT_OFFICIAL_YEAR}."
        }

    reserves = fetch_reserves(db)

    biomass_start = max(year - 1, 2020)

    biomass_img = (
        ee.ImageCollection("LARSE/GEDI/GEDI04_A_002_MONTHLY")
        .filterDate(f"{biomass_start}-01-01", f"{year}-12-31")
        .select("agbd")
        .median()
    )

    results = []

    for row in reserves[:20]:   # dev mode fast load

        geom = ee.Geometry(json.loads(row.geojson))

        dw = (
            ee.ImageCollection("GOOGLE/DYNAMICWORLD/V1")
            .filterDate(f"{year}-01-01", f"{year}-12-31")
            .filterBounds(geom)
        )

        tree_prob = dw.select("trees").mean()

        dense_forest = tree_prob.gte(0.6).rename("dense")
        tree_cover = tree_prob.gte(0.3).rename("cover")

        dense_area = dense_forest.multiply(
            ee.Image.pixelArea()
        ).reduceRegion(
            reducer=ee.Reducer.sum(),
            geometry=geom,
            scale=10,
            maxPixels=1e13
        )

        cover_area = tree_cover.multiply(
            ee.Image.pixelArea()
        ).reduceRegion(
            reducer=ee.Reducer.sum(),
            geometry=geom,
            scale=10,
            maxPixels=1e13
        )

        dense_m2 = dense_area.getInfo().get("dense", 0)
        cover_m2 = cover_area.getInfo().get("cover", 0)

        dense_ha = dense_m2 / 10000
        cover_ha = cover_m2 / 10000

        masked_biomass = biomass_img.updateMask(dense_forest)

        pixel_area_ha = ee.Image.pixelArea().divide(10000)

        biomass_per_pixel = masked_biomass.multiply(pixel_area_ha)

        biomass = biomass_per_pixel.reduceRegion(
            reducer=ee.Reducer.sum(),
            geometry=geom,
            scale=25,
            maxPixels=1e13
        )

        biomass_tonnes = biomass.getInfo().get("agbd", 0)

        carbon_tonnes = biomass_tonnes * 0.47
        co2e_tonnes = carbon_tonnes * 3.67

        density = co2e_tonnes / dense_ha if dense_ha > 0 else 0

        results.append({
            "reserve_id": str(row.reserve_id),
            "reserve": row.name,
            "year": year,
            "dense_forest_ha": round(dense_ha, 2),
            "tree_cover_ha": round(cover_ha, 2),
            "biomass_tonnes": round(biomass_tonnes, 2),
            "carbon_tonnes": round(carbon_tonnes, 2),
            "co2e_tonnes": round(co2e_tonnes, 2),
            "carbon_density_tco2e_ha": round(density, 2)
        })

    results.sort(
        key=lambda x: x["co2e_tonnes"],
        reverse=True
    )

    for i, item in enumerate(results, start=1):
        item["rank"] = i

    return results

# RESERVE LOSS STATS
def get_reserve_loss_stats(db, year):

    initialize_ee()

    if year is None:
        year = LOSS_LATEST_OFFICIAL_YEAR

    if year < LOSS_START_YEAR:
        return {
            "error": f"Loss data begins at {LOSS_START_YEAR}"
        }

    if year > LOSS_LATEST_OFFICIAL_YEAR:
        return {
            "year": year,
            "status": "currently unavailable",
            "message": f"Official annual forest-loss data for {year} is not yet available. We are working on the next release.",
            "latest_available_year": LOSS_LATEST_OFFICIAL_YEAR
        }

    reserves = fetch_reserves(db)

    biomass_img = get_loss_biomass_image(year)

    results = []

    for row in reserves[:20]:   # dev mode speed

        geom = ee.Geometry(json.loads(row.geojson))

        # Hansen loss mask
        loss_stats = county_loss_per_year(geom, year).getInfo()

        loss_m2 = list(loss_stats.values())[0] if loss_stats else 0
        loss_ha = loss_m2 / 10000

        hansen = ee.Image("UMD/hansen/global_forest_change_2024_v1_12")
        lossyear = hansen.select("lossyear")

        forest2000 = hansen.select("treecover2000").gte(30)

        previous_loss = lossyear.gt(0).And(lossyear.lt(year - 2000))
        forest_remaining = forest2000.updateMask(previous_loss.Not())

        loss_mask = lossyear.eq(year - 2000).And(forest_remaining)

        # biomass lost
        pixel_area_ha = ee.Image.pixelArea().divide(10000)

        biomass_per_pixel = biomass_img.updateMask(loss_mask).multiply(pixel_area_ha)

        biomass = biomass_per_pixel.reduceRegion(
            reducer=ee.Reducer.sum(),
            geometry=geom,
            scale=25,
            maxPixels=1e13
        )

        biomass_lost = biomass.getInfo().get("agbd", 0)

        carbon_lost = biomass_lost * 0.47
        co2e_emitted = carbon_lost * 3.67

        results.append({
            "reserve_id": str(row.reserve_id),
            "reserve": row.name,
            "year": year,
            "loss_ha": round(loss_ha, 2),
            "biomass_lost_tonnes": round(biomass_lost, 2),
            "carbon_lost_tonnes": round(carbon_lost, 2),
            "co2e_emitted_tonnes": round(co2e_emitted, 2)
        })

    results.sort(
        key=lambda x: x["co2e_emitted_tonnes"],
        reverse=True
    )

    for i, item in enumerate(results, start=1):
        item["rank"] = i

    return results

def get_reserve_loss_trend(db, reserve_id):

    initialize_ee()

    reserves = fetch_reserves(db)

    for row in reserves:
        if str(row.reserve_id) == str(reserve_id):

            geom = ee.Geometry(json.loads(row.geojson))

            # use current reserve carbon density
            density = 35

            return {
                "reserve_id": str(row.reserve_id),
                "reserve": row.name,
                "trend": build_entity_loss_trend(geom, density)
            }

    return {"error": "Reserve not found"}

def get_national_loss_trend(db):

    initialize_ee()

    counties = fetch_counties(db)

    density_lookup = build_county_density_lookup(db)

    results = []
    yearly_totals = {}

    cumulative_ha = 0
    cumulative_co2e = 0

    for county in counties[:3]:   # test first

        county_name = county.name.upper()
        geom = ee.Geometry(json.loads(county.geojson))

        density = density_lookup.get(county_name, 35)

        stats = get_loss_histogram(geom)
        yearly = build_yearly_loss(stats)

        for row in yearly:

            year = row["year"]
            loss_ha = row["loss_year_ha"]
            co2e = loss_ha * density

            if year not in yearly_totals:
                yearly_totals[year] = {
                    "loss_ha": 0,
                    "co2e": 0
                }

            yearly_totals[year]["loss_ha"] += loss_ha
            yearly_totals[year]["co2e"] += co2e

    for year in sorted(yearly_totals.keys()):

        loss_ha = yearly_totals[year]["loss_ha"]
        co2e = yearly_totals[year]["co2e"]

        cumulative_ha += loss_ha
        cumulative_co2e += co2e

        results.append({
            "year": year,
            "loss_ha": round(loss_ha, 2),
            "co2e_emitted_tonnes": round(co2e, 2),
            "cumulative_loss_ha": round(cumulative_ha, 2),
            "cumulative_co2e_tonnes": round(cumulative_co2e, 2)
        })

    return results

def get_national_carbon_map(year=None):

    initialize_ee()
    if year is None:
        year = CURRENT_OFFICIAL_YEAR

    if year < CARBON_START_YEAR:
        return {
            "error": f"Carbon maps begin at {CARBON_START_YEAR}"
        }

    if year > CURRENT_OFFICIAL_YEAR:
        return {
            "error": f"{year} carbon map not yet available. Latest completed year is {CURRENT_OFFICIAL_YEAR}."
        }

    kenya = (
        ee.FeatureCollection("FAO/GAUL/2015/level0")
        .filter(ee.Filter.eq("ADM0_NAME", "Kenya"))
        .geometry()
    )

    # -----------------------------------
    # Biomass baseline nearest year
    # -----------------------------------
    biomass_start = max(year - 1, 2020)

    biomass = (
        ee.ImageCollection("LARSE/GEDI/GEDI04_A_002_MONTHLY")
        .filterDate(f"{biomass_start}-01-01", f"{year}-12-31")
        .select("agbd")
        .median()
        .clip(kenya)
    )

    # -----------------------------------
    # Dynamic World annual tree mask
    # -----------------------------------
    dw = (
        ee.ImageCollection("GOOGLE/DYNAMICWORLD/V1")
        .filterDate(f"{year}-01-01", f"{year}-12-31")
        .filterBounds(kenya)
    )

    tree_prob = (
        dw.select("trees")
        .mean()
        .focal_mean(radius=500, units="meters")
    )

    carbon = (
        tree_prob
        .pow(1.05)
        .multiply(52)
        .add(
            biomass
            .focal_mean(radius=2500, units="meters")
            .multiply(0.12)
        )
        .updateMask(tree_prob.gte(0.18))
        .reproject(crs="EPSG:4326", scale=250)
    )

    vis = {
        "min": 3,
        "max": 70,
        "palette": [
            "#ffffe5",
            "#d9f0a3",
            "#78c679",
            "#41ab5d",
            "#238443",
            "#005a32",
            "#003320"
        ]
    }

    map_id = carbon.visualize(**vis).getMapId()

    return {
        "title": "Kenya Above-Ground Carbon Stock Map",
        "year": year,
        "status": "GreenMap baseline estimate" if year <= CURRENT_OFFICIAL_YEAR else "provisional estimate",
        "source": "GEDI + Dynamic World",
        "unit": "modelled above-ground biomass intensity",
        "update_frequency": "annual",
        "tile_url": map_id["tile_fetcher"].url_format
    }