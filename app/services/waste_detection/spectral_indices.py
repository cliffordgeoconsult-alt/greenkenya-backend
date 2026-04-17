import ee

def add_indices(image):
    ndvi = image.normalizedDifference(["B8", "B4"]).rename("NDVI")
    ndbi = image.normalizedDifference(["B11", "B8"]).rename("NDBI")
    ndwi = image.normalizedDifference(["B3", "B8"]).rename("NDWI")
    brightness = image.select("B2").rename("BRIGHTNESS")

    return image.addBands([ndvi, ndbi, ndwi, brightness])