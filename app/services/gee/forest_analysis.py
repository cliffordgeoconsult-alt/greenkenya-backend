# app/services/gee/forest_analysis.py
import ee
from datetime import datetime

COUNTY_MONITORING_RULES = {
    # URBAN / HIGHLAND
    "NAIROBI": {"start_month": 7, "end_month": 9, "tree": 0.58, "built": 0.45, "patch": 8},
    "KIAMBU": {"start_month": 7, "end_month": 9, "tree": 0.72, "built": 0.18, "patch": 55},
    "NYERI": {"start_month": 7, "end_month": 9, "tree": 0.70, "built": 0.20, "patch": 50},

    # COASTAL
    "MOMBASA": {"start_month": 6, "end_month": 9, "tree": 0.75, "built": 0.15, "patch": 50},
    "KILIFI": {"start_month": 6, "end_month": 9, "tree": 0.68, "built": 0.20, "patch": 45},
    "KWALE": {"start_month": 6, "end_month": 9, "tree": 0.68, "built": 0.20, "patch": 45},

    # DRYLAND / ASAL
    "TURKANA": {"start_month": 1, "end_month": 3, "tree": 0.52, "built": 0.20, "patch": 35},
    "GARISSA": {"start_month": 1, "end_month": 3, "tree": 0.52, "built": 0.20, "patch": 35},
    "WAJIR": {"start_month": 1, "end_month": 3, "tree": 0.50, "built": 0.20, "patch": 35},
    "MANDERA": {"start_month": 1, "end_month": 3, "tree": 0.50, "built": 0.20, "patch": 35},

    # DEFAULT ALL OTHERS
    "DEFAULT": {"start_month": 7, "end_month": 9, "tree": 0.68, "built": 0.20, "patch": 45}
}
def get_county_rule(county_name):
    county_name = (county_name or "").upper().strip()
    return COUNTY_MONITORING_RULES.get(
        county_name,
        COUNTY_MONITORING_RULES["DEFAULT"]
    )

# For COUNTY / REPORTING (scientific standard)
def get_reporting_forest_mask():
    hansen = ee.Image("UMD/hansen/global_forest_change_2024_v1_12")
    treecover = hansen.select("treecover2000")

    return treecover.gte(30)


# For GREENMAP FOREST ENTITIES (keep your strict logic)
def get_true_forest_mask():
    hansen = ee.Image("UMD/hansen/global_forest_change_2024_v1_12")
    treecover = hansen.select("treecover2000")

    baseline = treecover.gte(50)
    connected = baseline.connectedPixelCount(100, True)

    return baseline.updateMask(connected.gte(55))


def county_tree_cover_area(county_geometry):
    hansen = ee.Image("UMD/hansen/global_forest_change_2024_v1_12")
    treecover = hansen.select("treecover2000")

    canopy30 = treecover.gte(30)
    canopy50 = treecover.gte(50)

    pixel_area = ee.Image.pixelArea()

    area30 = canopy30.multiply(pixel_area)
    area50 = canopy50.multiply(pixel_area)

    stats30 = area30.reduceRegion(
        reducer=ee.Reducer.sum(),
        geometry=county_geometry,
        scale=30,
        maxPixels=1e13
    )

    stats50 = area50.reduceRegion(
        reducer=ee.Reducer.sum(),
        geometry=county_geometry,
        scale=30,
        maxPixels=1e13
    )

    return stats30, stats50


def county_forest_area(county_geometry):
    forest = get_reporting_forest_mask()
    area = forest.multiply(ee.Image.pixelArea())

    stats = area.reduceRegion(
        reducer=ee.Reducer.sum(),
        geometry=county_geometry,
        scale=30,
        maxPixels=1e13
    )

    return stats


def county_forest_area_by_year(county_geometry, year):
    hansen = ee.Image("UMD/hansen/global_forest_change_2024_v1_12")
    lossyear = hansen.select("lossyear")

    forest2000 = get_reporting_forest_mask()

    loss = lossyear.gt(0).And(lossyear.lte(year - 2000))
    loss = loss.updateMask(forest2000)

    loss_mask = loss.selfMask()
    remaining = forest2000.updateMask(loss_mask.Not())

    area = remaining.multiply(ee.Image.pixelArea())

    stats = area.reduceRegion(
        reducer=ee.Reducer.sum(),
        geometry=county_geometry,
        scale=30,
        maxPixels=1e13
    )

    return stats

def county_loss_per_year(county_geometry, year):

    hansen = ee.Image("UMD/hansen/global_forest_change_2024_v1_12")
    lossyear = hansen.select("lossyear")

    forest2000 = get_reporting_forest_mask()

    # cumulative loss BEFORE this year
    previous_loss = lossyear.gt(0).And(lossyear.lt(year - 2000))

    # forest remaining BEFORE this year
    forest_remaining = forest2000.updateMask(previous_loss.Not())

    # loss in THIS year only on remaining forest
    loss = lossyear.eq(year - 2000).And(forest_remaining)

    area = loss.multiply(ee.Image.pixelArea())

    stats = area.reduceRegion(
        reducer=ee.Reducer.sum(),
        geometry=county_geometry,
        scale=30,
        maxPixels=1e13
    )

    return stats

def county_total_loss(county_geometry, year):

    hansen = ee.Image("UMD/hansen/global_forest_change_2024_v1_12")
    lossyear = hansen.select("lossyear")

    forest2000 = get_reporting_forest_mask()

    # cumulative loss up to year
    loss = lossyear.gt(0).And(lossyear.lte(year - 2000)).And(forest2000)

    area = loss.multiply(ee.Image.pixelArea())

    stats = area.reduceRegion(
        reducer=ee.Reducer.sum(),
        geometry=county_geometry,
        scale=30,
        maxPixels=1e13
    )

    return stats

def get_loss_histogram(geometry):
    hansen = ee.Image("UMD/hansen/global_forest_change_2024_v1_12")
    lossyear = hansen.select("lossyear")

    pixel_area = ee.Image.pixelArea()

    # mask only where loss occurred
    forest2000 = get_reporting_forest_mask()

    loss = lossyear.updateMask(
        lossyear.gt(0).And(forest2000)
    )

    # group by lossyear
    stats = pixel_area.addBands(loss).reduceRegion(
        reducer=ee.Reducer.sum().group(
            groupField=1,
            groupName='lossyear'
        ),
        geometry=geometry,
        scale=30,
        maxPixels=1e13
    )

    return stats

def build_yearly_loss(stats):
    data = stats.getInfo()
    groups = data.get("groups", []) if data else []

    yearly_map = {g["lossyear"]: g["sum"] for g in groups}

    yearly = []
    cumulative = 0

    for year in range(2001, 2025):
        loss_m2 = yearly_map.get(year - 2000, 0)

        cumulative += loss_m2

        yearly.append({
            "year": year,
            "loss_year_ha": round(loss_m2 / 10000, 2),
            "loss_total_ha": round(cumulative / 10000, 2)
        })

    return yearly

def get_hansen_loss_tile(geometry, year):

    hansen = ee.Image("UMD/hansen/global_forest_change_2024_v1_12")
    lossyear = hansen.select("lossyear")

    # Use your existing forest mask (IMPORTANT)
    forest = get_reporting_forest_mask()

    # isolate loss for that year
    loss = lossyear.eq(year - 2000).And(forest).clip(geometry)

    # style (red loss)
    vis = {
        "palette": ["#ff0000"],
        "min": 1,
        "max": 1
    }

    # generate tile
    map_id = loss.selfMask().visualize(**vis).getMapId()

    return map_id["tile_fetcher"].url_format

def get_dw_coverage_tile(geometry, year):

    # 1. TIME WINDOW
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"

    # Handle current year (LIVE UPDATE)
    if year == datetime.now().year:
        end_date = datetime.now().strftime('%Y-%m-%d')

    # 2. GET DYNAMIC WORLD
    dw = ee.ImageCollection("GOOGLE/DYNAMICWORLD/V1") \
        .filterBounds(geometry) \
        .filterDate(start_date, end_date) \
        .select("trees")

    # Smooth (IMPORTANT → avoids flicker)
    tree_prob = dw.mean().clip(geometry)

    # 3. FOREST THRESHOLD (same as analytics)
    forest = tree_prob.gt(0.7)

    # 4. VISUALIZATION (clean green forest)
    vis = {
        "min": 0,
        "max": 1,
        "palette": [
            "#000000",   # non forest
            "#00ff00"    # forest (bright green)
        ]
    }

    map_id = forest.selfMask().visualize(**vis).getMapId()

    return map_id["tile_fetcher"].url_format

def get_forest_gain_total(geometry):

    hansen = ee.Image("UMD/hansen/global_forest_change_2024_v1_12")

    gain = hansen.select("gain")

    # constrain to forest region (important)
    forest_mask = hansen.select("treecover2000").gte(30)

    gain = gain.And(forest_mask)

    area = gain.multiply(ee.Image.pixelArea())

    stats = area.reduceRegion(
        reducer=ee.Reducer.sum(),
        geometry=geometry,
        scale=30,
        maxPixels=1e13
    )

    return stats

def get_dw_tree_probability(geometry, start_date, end_date):

    dw = (
        ee.ImageCollection("GOOGLE/DYNAMICWORLD/V1")
        .filterBounds(geometry)
        .filterDate(start_date, end_date)
        .select('trees')
    )

    # CHECK IF EMPTY
    count = dw.size()

    return ee.Image(
        ee.Algorithms.If(
            count.gt(0),
            dw.mean().clip(geometry),
            ee.Image(0).rename("trees")  # SAFE fallback
        )
    )

def calculate_dw_transition(geometry, start_year=2020, end_year=2025):
    """
    Compare baseline (2020) to current to detect REGROWTH or DEGRADATION.
    """
    baseline = get_dw_tree_probability(geometry, f'{start_year}-01-01', f'{start_year}-12-31')
    current = get_dw_tree_probability(geometry, '2025-01-01', '2025-12-31')
    
    # Regrowth: Probability increased significantly
    regrowth = current.subtract(baseline).gt(0.2).selfMask()
    
    # Degradation: Probability decreased significantly
    degradation = baseline.subtract(current).gt(0.2).selfMask()
    
    pixel_area = ee.Image.pixelArea()
    
    # 🔥 FINAL SAFE VERSION (NO IF, NO CRASH)
    regrowth_area = regrowth.multiply(pixel_area).unmask(0)

    stats = regrowth_area.reduceRegion(
        reducer=ee.Reducer.sum(),
        geometry=geometry,
        scale=10,
        maxPixels=1e13
    ).getInfo()

    regrowth_ha = round((list(stats.values())[0] if stats else 0) / 10000, 2)

    return {
        "regrowth_ha": regrowth_ha
    }

    return {
        "regrowth_ha": round((stats.get("trees", 0) or 0) / 10000, 2)
    }


def smooth_forest_coverage(series):
    """
    3-year rolling median smoothing
    """

    if not series or len(series) < 3:
        return series

    smoothed = []

    for i in range(len(series)):

        # edges stay same
        if i == 0 or i == len(series) - 1:
            smoothed.append(series[i])
            continue

        window = [
            series[i - 1]["forest_extent_ha"],
            series[i]["forest_extent_ha"],
            series[i + 1]["forest_extent_ha"]
        ]

        window = [v for v in window if v is not None]

        if not window:
            smoothed.append(series[i])
            continue

        median_val = sorted(window)[len(window)//2]

        smoothed.append({
            "year": series[i]["year"],
            "forest_extent_ha": round(median_val, 2),
            "tree_cover_ha": series[i]["tree_cover_ha"],
            "method": "smoothed"
        })

    return smoothed

def calculate_yearly_coverage(geometry, county_name=None, start_year=2020, end_year=2026):

    now = datetime.now()
    current_year = now.year
    pixel_area = ee.Image.pixelArea()

    results = []

    for year in range(start_year, end_year + 1):

        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"

        # current year = partial live year
        if year == current_year:
            end_date = now.strftime("%Y-%m-%d")

        dw = (
            ee.ImageCollection("GOOGLE/DYNAMICWORLD/V1")
            .filterBounds(geometry)
            .filterDate(start_date, end_date)
        )

        count = dw.size().getInfo()

        if count == 0:
            results.append({
                "year": year,
                "forest_extent_ha": None,
                "tree_cover_ha": None
            })
            continue

        # yearly median probabilities
        tree_prob = dw.select("trees").median()
        built_prob = dw.select("built").median()
        crop_prob = dw.select("crops").median()
        bare_prob = dw.select("bare").median()
        water_prob = dw.select("water").median()

        # -------------------------
        # DENSE FOREST
        # -------------------------
        dense = (
            tree_prob.gte(0.65)
            .And(built_prob.lte(0.15))
            .And(crop_prob.lte(0.40))
            .And(bare_prob.lte(0.20))
            .And(water_prob.lte(0.10))
        )

        dense_patch = dense.selfMask().connectedPixelCount(100, True)

        dense = dense.And(dense_patch.gte(50)).selfMask()

        # -------------------------
        # TREE COVER
        # -------------------------
        cover = (
            tree_prob.gte(0.35)
            .And(built_prob.lte(0.35))
            .And(water_prob.lte(0.20))
        )

        cover_patch = cover.selfMask().connectedPixelCount(100, True)

        cover = cover.And(cover_patch.gte(10)).selfMask()

        # -------------------------
        # AREA
        # -------------------------
        dense_stats = dense.multiply(pixel_area).reduceRegion(
            reducer=ee.Reducer.sum(),
            geometry=geometry,
            scale=10,
            maxPixels=1e13
        ).getInfo()

        cover_stats = cover.multiply(pixel_area).reduceRegion(
            reducer=ee.Reducer.sum(),
            geometry=geometry,
            scale=10,
            maxPixels=1e13
        ).getInfo()

        dense_area = (list(dense_stats.values())[0] if dense_stats else 0) / 10000
        cover_area = (list(cover_stats.values())[0] if cover_stats else 0) / 10000

        results.append({
            "year": year,
            "forest_extent_ha": round(dense_area, 2),
            "tree_cover_ha": round(cover_area, 2)
        })

    smoothed = smooth_forest_coverage(results)
    return smoothed

def calculate_degradation(geometry, start_year=2020, end_year=2025):
    """
    Detects degradation by finding pixels where the tree probability 
    dropped significantly (0.2+ decrease) but stayed above 0.1 (not total loss).
    """
    baseline = get_dw_tree_probability(geometry, f'{start_year}-01-01', f'{start_year}-12-31')
    current = get_dw_tree_probability(geometry, f'{end_year}-01-01', f'{end_year}-12-31')
    
    # Degradation formula: (Baseline - Current) > 0.2
    # This captures thinning canopy, selective logging, or charcoal burning sites.
    degradation_mask = baseline.subtract(current).gt(0.35) \
        .And(current.gt(0.1)) \
        .And(baseline.gt(0.5)) \
        .selfMask()
    
    pixel_area = ee.Image.pixelArea()
    area = degradation_mask.multiply(pixel_area).unmask(0)

    stats = area.reduceRegion(
        reducer=ee.Reducer.sum(),
        geometry=geometry,
        scale=10,
        maxPixels=1e13
    ).getInfo()

    return round((list(stats.values())[0] if stats else 0) / 10000, 2)

    return round((stats.get('trees') or 0) / 10000, 2)

def calculate_confirmed_deforestation(geometry, start_date, end_date):

    # 1. Forest baseline (scientific)
    forest = get_reporting_forest_mask()

    # 2. RADD alerts (time-bound)
    alerts = (
        ee.ImageCollection("projects/glad/alert/UpdResult")
        .filterBounds(geometry)
        .filterDate(start_date, end_date)
    )

    # 3. Select correct band dynamically
    def extract_conf(img):
        bands = img.bandNames()
        return ee.Image(
            ee.Algorithms.If(
                bands.contains("conf26"),
                img.select("conf26"),
                ee.Algorithms.If(
                    bands.contains("conf25"),
                    img.select("conf25"),
                    ee.Image(0)
                )
            )
        )

    # 🔥 CRITICAL FIX — HANDLE EMPTY COLLECTION
    count = alerts.size()

    alert_img = ee.Image(
        ee.Algorithms.If(
            count.gt(0),
            alerts.map(extract_conf).max(),
            ee.Image(0)  # fallback when no alerts exist
        )
    )

    # 4. High confidence only
    confirmed = alert_img.gte(2)

    # 5. INTERSECTION (THIS IS THE MAGIC)
    deforestation = confirmed.And(forest)

    # 6. AREA
    area = deforestation.multiply(ee.Image.pixelArea())

    stats = area.reduceRegion(
        reducer=ee.Reducer.sum(),
        geometry=geometry,
        scale=10,
        maxPixels=1e13
    ).getInfo()

    return round(
        (list(stats.values())[0] if stats else 0) / 10000,
        2
    )