# app/services/gee/forest_baseline.py
import ee

def detect_forest_baseline(geometry):

    hansen = ee.Image("UMD/hansen/global_forest_change_2023_v1_11")
    tree_cover = hansen.select("treecover2000")

    # Forest definition (>=50% canopy)
    baseline = tree_cover.gte(50)

    # Remove small patches (~5ha ≈ 55 pixels)
    connected = baseline.connectedPixelCount(100, True)
    forest = baseline.updateMask(connected.gte(78))

    # Added area band for correct polygon area calculation
    label = forest.rename("label")
    area_band = forest.multiply(ee.Image.pixelArea()).rename("area")

    image = label.addBands(area_band)

    polygons = image.reduceToVectors(
        geometry=geometry,
        scale=30,
        geometryType="polygon",
        reducer=ee.Reducer.sum(),  # Enables "sum" property (area)
        maxPixels=1e13,
        tileScale=4
    )

    return polygons