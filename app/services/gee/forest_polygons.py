# app/services/gee/forest_polygons.py
# A service to get forest polygons for Kenya using the Hansen Global Forest Change dataset.
import ee

def get_forest_polygons_kenya():
    # Load Hansen
    hansen = ee.Image("UMD/hansen/global_forest_change_2024_v1_12")
    treecover = hansen.select("treecover2000")

    # Forest definition
    baseline = treecover.gte(50)

    # Remove small patches (5ha ≈ 55 pixels)
    connected = baseline.connectedPixelCount(100, True)
    forest = baseline.updateMask(connected.gte(55))

    # Convert to polygons
    vectors = forest.selfMask().reduceToVectors(
        scale=30,
        geometryType='polygon',
        eightConnected=True,
        maxPixels=1e13,
        tileScale=4,
        bestEffort=True
    )
    return vectors