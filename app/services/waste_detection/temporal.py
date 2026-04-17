import ee

def get_temporal_change(aoi):
    past = (
        ee.ImageCollection("COPERNICUS/S2_SR")
        .filterBounds(aoi)
        .filterDate("2025-01-01", "2025-02-01")
        .median()
    )

    recent = (
        ee.ImageCollection("COPERNICUS/S2_SR")
        .filterBounds(aoi)
        .filterDate("2025-03-01", "2025-04-01")
        .median()
    )

    ndvi_past = past.normalizedDifference(["B8", "B4"])
    ndvi_recent = recent.normalizedDifference(["B8", "B4"])

    change = ndvi_recent.subtract(ndvi_past)

    return change.lt(-0.2)