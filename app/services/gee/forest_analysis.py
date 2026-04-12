# app/services/gee/forest_analysis.py
import ee

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