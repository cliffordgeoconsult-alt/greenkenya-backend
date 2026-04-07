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

    # 🔥 cumulative loss BEFORE this year
    previous_loss = lossyear.gt(0).And(lossyear.lt(year - 2000))

    # 🔥 forest remaining BEFORE this year
    forest_remaining = forest2000.updateMask(previous_loss.Not())

    # 🔥 loss in THIS year only on remaining forest
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

def get_dynamic_world_yearly(geometry):

    import datetime

    dw = ee.ImageCollection("GOOGLE/DYNAMICWORLD/V1")

    current_year = datetime.datetime.now().year
    start_year = 2016
    end_year = min(current_year, 2025)

    years = list(range(start_year, end_year + 1))

    def compute_year(year):
        year = ee.Number(year)

        start = ee.Date.fromYMD(year, 1, 1)
        end = ee.Date.fromYMD(year, 12, 31)

        collection = dw.filterDate(start, end).filterBounds(geometry)

        def compute():
            trees = collection.select("trees").mean()
            built = collection.select("built").mean()

            # 🔥 STRICT TREE + REMOVE BUILT
            tree_mask = trees.gte(0.65).And(built.lt(0.2))

            return tree_mask.multiply(ee.Image.pixelArea())
        area = ee.Image(
            ee.Algorithms.If(
                collection.size().gte(2),
                compute(),
                ee.Image.constant(0)
            )
        )

        stats = area.reduceRegion(
            reducer=ee.Reducer.sum(),
            geometry=geometry,
            scale=10,
            maxPixels=1e13
        )

        return ee.Feature(None, {
            "year": year,
            "area": ee.Algorithms.If(
                stats.size().gt(0),
                stats.values().get(0),
                0
            )
        })

    fc = ee.FeatureCollection([compute_year(y) for y in years])
    result = fc.getInfo()

    return [
        {
            "year": int(f["properties"]["year"]),
            "coverage_ha": round((f["properties"]["area"] or 0) / 10000, 2)
        }
        for f in result["features"]
    ]
def get_dynamic_world_monthly_current_year(geometry):

    import datetime

    dw = ee.ImageCollection("GOOGLE/DYNAMICWORLD/V1")
    current_year = datetime.datetime.now().year

    # 🔥 STEP 1: GET YEARLY VALUE (ANCHOR)
    yearly_data = get_dynamic_world_yearly(geometry)

    year_value = next(
        (y["coverage_ha"] for y in yearly_data if y["year"] == current_year),
        None
    )

    # fallback if current year not available
    if not year_value and yearly_data:
        year_value = yearly_data[-1]["coverage_ha"]

    if not year_value:
        year_value = 0

    current_date = datetime.datetime.now()
    current_month = current_date.month

    months = list(range(1, current_month + 1))

    def compute_month(month):
        month = ee.Number(month)

        start = ee.Date.fromYMD(current_year, month, 1)
        end = start.advance(1, 'month')

        collection = dw.filterDate(start, end).filterBounds(geometry)

        size = collection.size()
        valid = size.gte(2)

        def compute():
            # 🔥 USE SIGNAL, NOT HARD CLASSIFICATION
            vegetation = collection.map(
                lambda img: img.select("trees").add(img.select("shrub_and_scrub"))
            )

            mean_signal = vegetation.mean()

            stats = mean_signal.reduceRegion(
                reducer=ee.Reducer.mean(),
                geometry=geometry,
                scale=10,
                maxPixels=1e13
            )

            return stats.values().get(0)

        def compute_sar():

            s1 = ee.ImageCollection("COPERNICUS/S1_GRD") \
                .filter(ee.Filter.eq('instrumentMode', 'IW')) \
                .filter(ee.Filter.listContains('transmitterReceiverPolarisation', 'VV')) \
                .select('VV') \
                .filterDate(start, end) \
                .filterBounds(geometry)

            median = s1.median()

            # 🔥 Normalize SAR to 0–1 scale
            sar_norm = median.unitScale(-25, -5)

            stats = sar_norm.reduceRegion(
                reducer=ee.Reducer.mean(),
                geometry=geometry,
                scale=30,
                maxPixels=1e13
            )

            return stats.values().get(0)

        signal = ee.Algorithms.If(valid, compute(), compute_sar())

        return ee.Feature(None, {
            "month": month,
            "signal": signal
        })

    fc = ee.FeatureCollection([compute_month(m) for m in months])

    result = fc.getInfo()

    # 🔥 STEP 2: NORMALIZE SIGNAL → REAL AREA
    signals = [
        f["properties"]["signal"] or 0
        for f in result["features"]
    ]

    max_signal = max(signals) if max(signals) > 0 else 1

    output = []

    for f in result["features"]:
        month = int(f["properties"]["month"])
        signal = f["properties"]["signal"] or 0

        # 🔥 SCALE USING YEARLY VALUE
        coverage = (signal / max_signal) * year_value

        output.append({
            "month": month,
            "coverage_ha": round(coverage, 2)
        })

    return output

def get_sar_forest_yearly(geometry):

    import datetime

    s1 = ee.ImageCollection("COPERNICUS/S1_GRD") \
        .filter(ee.Filter.eq('instrumentMode', 'IW')) \
        .filter(ee.Filter.listContains('transmitterReceiverPolarisation', 'VV')) \
        .select('VV')

    current_year = datetime.datetime.now().year
    years = list(range(2016, current_year + 1))

    def compute_year(year):
        year = ee.Number(year)

        start = ee.Date.fromYMD(year, 1, 1)
        end = ee.Date.fromYMD(year, 12, 31)

        collection = s1.filterDate(start, end).filterBounds(geometry)

        def compute():
            median = collection.median()

            # 🔥 STRICTER SAR RANGE
            forest = median.gt(-13).And(median.lt(-9))

            # 🔥 STRONG URBAN MASK
            dw = ee.ImageCollection("GOOGLE/DYNAMICWORLD/V1") \
                .filterDate(start, end) \
                .filterBounds(geometry)

            built = dw.select("built").mean().gte(0.2)
            bare = dw.select("bare").mean().gte(0.2)

            # 🔥 REMOVE URBAN + BARE LAND
            forest = forest.And(built.Not()).And(bare.Not())

            # 🔥 EXTRA: ONLY WHERE TREES EXIST
            trees = dw.select("trees").mean().gte(0.3)
            forest = forest.And(trees)

            return forest.multiply(ee.Image.pixelArea())
        area = ee.Image(
            ee.Algorithms.If(
                collection.size().gte(10),
                compute(),
                ee.Image.constant(0)
            )
        )

        stats = area.reduceRegion(
            reducer=ee.Reducer.sum(),
            geometry=geometry,
            scale=30,
            maxPixels=1e13
        )

        return ee.Feature(None, {
            "year": year,
            "area": ee.Algorithms.If(
                stats.size().gt(0),
                stats.values().get(0),
                0
            )
        })

    fc = ee.FeatureCollection([compute_year(y) for y in years])
    result = fc.getInfo()

    return [
        {
            "year": int(f["properties"]["year"]),
            "coverage_ha": round((f["properties"]["area"] or 0) / 10000, 2)
        }
        for f in result["features"]
    ]
def get_forest_degradation_sar(geometry):

    s1 = ee.ImageCollection("COPERNICUS/S1_GRD") \
        .filter(ee.Filter.eq('instrumentMode', 'IW')) \
        .filter(ee.Filter.listContains('transmitterReceiverPolarisation', 'VV')) \
        .select('VV')

    # baseline (2017)
    baseline = s1.filterDate('2017-01-01', '2017-12-31').median()

    # current (latest year)
    current = s1.filterDate('2025-01-01', '2025-12-31').median()

    change = current.subtract(baseline)

    # degradation = drop in signal
    degraded = change.lt(-2)

    area = degraded.multiply(ee.Image.pixelArea())

    stats = area.reduceRegion(
        reducer=ee.Reducer.sum(),
        geometry=geometry,
        scale=30,
        maxPixels=1e13
    )

    return stats

def get_sar_loss_yearly(geometry):

    import datetime

    s1 = ee.ImageCollection("COPERNICUS/S1_GRD") \
        .filter(ee.Filter.eq('instrumentMode', 'IW')) \
        .filter(ee.Filter.listContains('transmitterReceiverPolarisation', 'VV')) \
        .select('VV')

    current_year = datetime.datetime.now().year
    years = list(range(2017, current_year + 1))

    def compute_year(year):

        prev = s1.filterDate(f"{year-1}-01-01", f"{year-1}-12-31") \
            .filterBounds(geometry).median()

        curr = s1.filterDate(f"{year}-01-01", f"{year}-12-31") \
            .filterBounds(geometry).median()

        # ✅ CHANGE
        change = curr.subtract(prev)

        # ✅ STRONG LOSS ONLY
        hansen = ee.Image("UMD/hansen/global_forest_change_2024_v1_12")
        forest_mask = hansen.select("treecover2000").gte(30)

        loss = change.lt(-4).And(forest_mask)

        area = loss.multiply(ee.Image.pixelArea())

        stats = area.reduceRegion(
            reducer=ee.Reducer.sum(),
            geometry=geometry,
            scale=30,
            maxPixels=1e13
        )

        return ee.Feature(None, {
            "year": year,
            "area": ee.Algorithms.If(
                stats.size().gt(0),
                stats.values().get(0),
                0
            )
        })

    fc = ee.FeatureCollection([compute_year(y) for y in years])
    result = fc.getInfo()

    output = []
    cumulative = 0

    for f in result["features"]:
        year = int(f["properties"]["year"])
        area = f["properties"]["area"] or 0

        loss_ha = area / 10000
        cumulative += loss_ha

        output.append({
            "year": year,
            "loss_year_ha": round(loss_ha, 2),
            "loss_total_ha": round(cumulative, 2)
        })

    return output
def get_sar_loss_monthly_current_year(geometry):

    import datetime

    s1 = ee.ImageCollection("COPERNICUS/S1_GRD") \
        .filter(ee.Filter.eq('instrumentMode', 'IW')) \
        .filter(ee.Filter.listContains('transmitterReceiverPolarisation', 'VV')) \
        .select('VV')

    current_date = datetime.datetime.now()
    current_year = current_date.year
    current_month = current_date.month

    months = list(range(1, current_month + 1))

    def compute_month(month):

        month = ee.Number(month)

        start = ee.Date.fromYMD(current_year, month, 1)
        end = start.advance(1, 'month')

        prev_start = start.advance(-1, 'month')
        prev_end = start

        current = s1.filterDate(start, end).filterBounds(geometry)
        previous = s1.filterDate(prev_start, prev_end).filterBounds(geometry)

        current_size = current.size()
        prev_size = previous.size()

        valid = current_size.gte(3).And(prev_size.gte(3))

        def compute():
            curr = current.median()
            prev = previous.median()

            change = curr.subtract(prev)

            # 🔥 STRONG LOSS ONLY (reduce noise)
            loss = change.lt(-4)

            # 🔥 Hansen forest constraint
            hansen = ee.Image("UMD/hansen/global_forest_change_2024_v1_12")
            forest_mask = hansen.select("treecover2000").gte(30)

            loss = loss.And(forest_mask)

            # 🔥 REMOVE SMALL SPECKLE (CRITICAL)
            connected = loss.connectedPixelCount(8, True)
            loss = loss.updateMask(connected.gte(5))

            hansen = ee.Image("UMD/hansen/global_forest_change_2024_v1_12")
            forest_mask = hansen.select("treecover2000").gte(30)

            loss = loss.And(forest_mask)

            area = loss.multiply(ee.Image.pixelArea())

            stats = area.reduceRegion(
                reducer=ee.Reducer.sum(),
                geometry=geometry,
                scale=30,
                maxPixels=1e13
            )

            return ee.Dictionary({
                "area": ee.Algorithms.If(
                    stats.size().gt(0),
                    stats.values().get(0),
                    0
                )
            })

        # ✅ SAFE RETURN (NO None)
        result = ee.Algorithms.If(
            valid,
            compute(),
            ee.Dictionary({"area": -1})  # 👈 FLAG INVALID
        )

        return ee.Feature(None, {
            "month": month,
            "area": ee.Dictionary(result).get("area")
        })

    fc = ee.FeatureCollection([compute_month(m) for m in months])

    result = fc.getInfo()

    output = []
    cumulative = 0

    for f in result["features"]:
        month = int(f["properties"]["month"])
        area = f["properties"]["area"]

        # 🔥 SKIP INVALID MONTHS
        if area is None or area == -1:
            continue

        loss_ha = area / 10000
        cumulative += loss_ha

        output.append({
            "month": month,
            "loss_month_ha": round(loss_ha, 2),
            "loss_total_ha": round(cumulative, 2)
        })

    return output

def get_forest_gain_total(geometry):

    hansen = ee.Image("UMD/hansen/global_forest_change_2024_v1_12")

    gain = hansen.select("gain")

    # 🔥 constrain to forest region (important)
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