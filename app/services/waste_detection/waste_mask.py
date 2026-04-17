import ee

def build_waste_mask(image):
    low_vegetation = image.select("NDVI").lt(0.2)
    urban_like = image.select("NDBI").gt(0.1)
    not_water = image.select("NDWI").lt(0)
    bright_surface = image.select("BRIGHTNESS").gt(1500)

    waste_mask = (
        low_vegetation
        .And(urban_like)
        .And(not_water)
        .And(bright_surface)
    )

    return waste_mask.selfMask()