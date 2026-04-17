# app/services/gee/forest_analysis.py
import ee
from datetime import datetime

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
    loss = lossyear.eq(year - 2000).And(forest)

    # style (red loss)
    vis = {
        "palette": ["#ff0000"],
        "min": 1,
        "max": 1
    }

    # generate tile
    map_id = loss.selfMask().visualize(**vis).getMapId()

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
    """
    Returns a smoothed tree probability map for a specific period.
    This reduces the 'flicker' noise common in Dynamic World.
    """
    dw = ee.ImageCollection("GOOGLE/DYNAMICWORLD/V1") \
        .filterBounds(geometry) \
        .filterDate(start_date, end_date) \
        .select('trees')
    
    # Use mean or median to smooth out cloud shadows/sensor noise
    return dw.mean().clip(geometry)

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
    
    regrowth_stats = regrowth.multiply(pixel_area).reduceRegion(
        reducer=ee.Reducer.sum(),
        geometry=geometry,
        scale=10,
        maxPixels=1e13
    )
    
    return {
        "regrowth_ha": round(ee.Number(regrowth_stats.get('trees')).getInfo() / 10000, 2),
    }

def calculate_yearly_coverage(geometry, start_year=2020, end_year=2026):
    """
    Calculates the total forested area (ha) for each year.
    """
    coverage_history = []
    pixel_area = ee.Image.pixelArea()

    for year in range(start_year, end_year + 1):
        # Define the year window
        start_date = f'{year}-01-01'
        end_date = f'{year}-12-31'
        
        # If it's the current year (2026), only go up to today
        if year == 2026:
            end_date = datetime.now().strftime('%Y-%m-%d')

        # Get smoothed tree probability for that specific year
        tree_prob = get_dw_tree_probability(geometry, start_date, end_date)
        
        # Threshold: 0.5 probability counts as 'Forest Cover'
        forest_mask = tree_prob.gt(0.7).selfMask()
        
        stats = forest_mask.multiply(pixel_area).reduceRegion(
            reducer=ee.Reducer.sum(),
            geometry=geometry,
            scale=30,
            maxPixels=1e13
        ).getInfo()

        area_ha = round((stats.get('trees') or 0) / 10000, 2)
        
        coverage_history.append({
            "year": year,
            "forest_extent_ha": area_ha
        })

    return coverage_history

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
    stats = degradation_mask.multiply(pixel_area).reduceRegion(
        reducer=ee.Reducer.sum(),
        geometry=geometry,
        scale=10,
        maxPixels=1e13
    ).getInfo()

    return round((stats.get('trees') or 0) / 10000, 2)

def calculate_confirmed_deforestation(geometry, start_date, end_date):

    # 1. Forest baseline (scientific)
    forest = get_reporting_forest_mask()

    # 2. RADD alerts (time-bound)
    alerts = ee.ImageCollection("projects/glad/alert/UpdResult") \
        .filterBounds(geometry) \
        .filterDate(start_date, end_date)

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

    alert_img = alerts.map(extract_conf).max()

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