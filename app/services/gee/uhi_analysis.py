# app/services/gee/uhi_analysis.py
"""Urban heat / LST metrics from real EO only (MODIS LST, MODIS NDVI, Dynamic World built)."""
import json
from datetime import datetime

import ee

from app.core.cache import redis_cache

UHI_MIN_YEAR = 2000

DATA_SOURCES = [
    "MODIS/061/MOD11A2 (LST day/night, annual median, QC-masked)",
    "MODIS/061/MOD13Q2 (NDVI, annual median)",
    "GOOGLE/DYNAMICWORLD/V1 (built probability, annual mean)",
]

METHODOLOGY_SUMMARY = (
    "Annual median of MOD11A2 day/night LST (°C) after QC mask; "
    "annual median MOD13Q2 NDVI; mean Dynamic World 'built' probability. "
    "Ward fields lst_*_excess_vs_county_mean_c are ward minus county zonal means "
    "for the same year (relative heat vs county average, not rural baseline)."
)


def _year_end_date(year: int) -> str:
    if year >= datetime.now().year:
        return datetime.now().strftime("%Y-%m-%d")
    return f"{year}-12-31"


def _modis_lst_annual_median_c(geometry: ee.Geometry, year: int, day: bool) -> ee.Image:
    start = f"{year}-01-01"
    end = _year_end_date(year)
    col = (
        ee.ImageCollection("MODIS/061/MOD11A2")
        .filterDate(start, end)
        .filterBounds(geometry)
    )
    lst_band = "LST_Day_1km" if day else "LST_Night_1km"
    qc_band = "QC_Day" if day else "QC_Night"

    def prep(img: ee.Image) -> ee.Image:
        qc = img.select(qc_band)
        mask = qc.bitwiseAnd(3).eq(0)
        lst_c = img.select(lst_band).multiply(0.02).subtract(273.15)
        return lst_c.updateMask(mask).copyProperties(img, ["system:time_start"])

    return col.map(prep).median()


def _modis_ndvi_annual_median(geometry: ee.Geometry, year: int) -> ee.Image:
    start = f"{year}-01-01"
    end = _year_end_date(year)
    col = (
        ee.ImageCollection("MODIS/061/MOD13Q2")
        .filterDate(start, end)
        .filterBounds(geometry)
        .select("NDVI")
    )
    return col.median().multiply(0.0001).clamp(-0.2, 1.0)


def _dynamic_world_built_mean(geometry: ee.Geometry, year: int) -> ee.Image:
    start = f"{year}-01-01"
    end = _year_end_date(year)
    dw = (
        ee.ImageCollection("GOOGLE/DYNAMICWORLD/V1")
        .filterBounds(geometry)
        .filterDate(start, end)
        .select("built")
    )
    return dw.mean()


@redis_cache("uhi_zonal", ttl=43200)
def compute_uhi_zonal_metrics(geojson_str: str, year: int) -> dict:
    """
    One reduceRegion (single GEE round-trip) for LST day/night, NDVI, built mean.
    Returns only keys with real values; omits fabricated fields.
    """
    y = int(year)
    now_y = datetime.now().year
    if y < UHI_MIN_YEAR or y > now_y:
        return {"error": f"year must be between {UHI_MIN_YEAR} and {now_y}"}

    geo = json.loads(geojson_str)
    geometry = ee.Geometry(geo)

    lst_day = _modis_lst_annual_median_c(geometry, y, True)
    lst_night = _modis_lst_annual_median_c(geometry, y, False)
    ndvi = _modis_ndvi_annual_median(geometry, y)
    built = _dynamic_world_built_mean(geometry, y)

    scale_m = 1000
    crs_out = "EPSG:4326"
    lst_day_r = lst_day.reproject(crs=crs_out, scale=scale_m)
    lst_night_r = lst_night.reproject(crs=crs_out, scale=scale_m)
    ndvi_r = ndvi.reproject(crs=crs_out, scale=scale_m)
    built_r = built.reproject(crs=crs_out, scale=scale_m)

    stack = (
        lst_day_r.rename("lst_day_c")
        .addBands(lst_night_r.rename("lst_night_c"))
        .addBands(ndvi_r.rename("ndvi"))
        .addBands(built_r.rename("built_mean"))
    )

    raw = stack.reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=geometry,
        scale=scale_m,
        maxPixels=1e13,
    ).getInfo()

    out = {"year": y}
    if raw.get("lst_day_c") is not None:
        out["lst_day_mean_c"] = round(float(raw["lst_day_c"]), 3)
    if raw.get("lst_night_c") is not None:
        out["lst_night_mean_c"] = round(float(raw["lst_night_c"]), 3)
    if raw.get("ndvi") is not None:
        out["ndvi_mean"] = round(float(raw["ndvi"]), 4)
    if raw.get("built_mean") is not None:
        out["built_probability_mean"] = round(float(raw["built_mean"]), 4)

    if len(out) <= 1:
        out["error"] = "no_valid_pixels_in_geometry"
    return out
