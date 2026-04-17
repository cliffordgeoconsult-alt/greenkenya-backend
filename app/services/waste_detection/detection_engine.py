import ee
from .spectral_indices import add_indices
from .waste_mask import build_waste_mask
from .temporal import get_temporal_change

def detect_waste_v2(aoi):
    image = (
        ee.ImageCollection("COPERNICUS/S2_SR")
        .filterBounds(aoi)
        .filterDate("2025-03-01", "2025-04-01")
        .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 20))
        .median()
    )

    image = add_indices(image)

    waste_mask = build_waste_mask(image)
    change_mask = get_temporal_change(aoi)

    final_mask = waste_mask.And(change_mask)

    return final_mask