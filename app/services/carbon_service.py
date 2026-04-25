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


# -----------------------------------
# COUNTY CARBON STATS
# -----------------------------------
def get_county_carbon_stats(db):

    initialize_ee()

    counties = fetch_counties(db)

    # GEDI biomass (recent baseline)
    biomass_img = (
        ee.ImageCollection("LARSE/GEDI/GEDI04_A_002_MONTHLY")
        .filterDate("2023-01-01", "2024-12-31")
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
            .filterDate("2024-01-01", "2024-12-31")
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

    counties = fetch_counties(db)

    # same biomass baseline
    biomass_img = (
        ee.ImageCollection("LARSE/GEDI/GEDI04_A_002_MONTHLY")
        .filterDate("2023-01-01", "2024-12-31")
        .select("agbd")
        .median()
    )

    results = []

    for row in counties[:3]:   # test first

        geom = ee.Geometry(json.loads(row.geojson))

        # -----------------------------------
        # Forest loss area from your Hansen tool
        # -----------------------------------
        loss_stats = county_loss_per_year(geom, year).getInfo()

        loss_m2 = list(loss_stats.values())[0] if loss_stats else 0
        loss_ha = loss_m2 / 10000

        # -----------------------------------
        # Estimate biomass on lost pixels
        # -----------------------------------
        hansen = ee.Image("UMD/hansen/global_forest_change_2024_v1_12")
        lossyear = hansen.select("lossyear")

        loss_mask = lossyear.eq(year - 2000)

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

def build_county_density_lookup(db):
    stats = get_county_carbon_stats(db)

    lookup = {}

    for row in stats:
        lookup[row["county"].upper()] = row["carbon_density_tco2e_ha"]

    return lookup

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

def get_available_carbon_years():

    return {
        "available_years": list(
            range(CARBON_START_YEAR, CURRENT_OFFICIAL_YEAR + 1)
        ),
        "latest_official_year": CURRENT_OFFICIAL_YEAR
    }


def get_national_carbon_map(year):

    initialize_ee()

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

    tree_prob = dw.select("trees").mean()

    forest = tree_prob.gte(0.3)

    carbon = (
        tree_prob
        .pow(1.4)
        .multiply(65)
        .add(
            biomass.focal_mean(radius=3000, units="meters").multiply(0.35)
        )
        .updateMask(tree_prob.gte(0.08))
        .reproject(crs="EPSG:4326", scale=100)
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