from app.services.gee.ee_init import initialize_ee
import ee
from .detection_engine import detect_waste_v2
from .clustering import extract_clusters

def run_pipeline(aoi_geojson):
    initialize_ee()   

    aoi = ee.Geometry(aoi_geojson)

    mask = detect_waste_v2(aoi)

    clusters = extract_clusters(mask, aoi)

    return clusters.getInfo()