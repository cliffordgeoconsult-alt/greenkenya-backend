# app/services/gee/forest_statistics.py - computes forest area statistics for a given county geometry
import ee
def compute_county_forest_area(county_geom):
    hansen = ee.Image("UMD/hansen/global_forest_change_2024_v1_12")
    tree_cover = hansen.select("treecover2000")
    forest = tree_cover.gte(50)
    area_image = forest.multiply(ee.Image.pixelArea())
    stats = area_image.reduceRegion(
        reducer=ee.Reducer.sum(),
        geometry=county_geom,
        scale=30,
        maxPixels=1e13
    )
    forest_area = stats.getInfo()
    return forest_area