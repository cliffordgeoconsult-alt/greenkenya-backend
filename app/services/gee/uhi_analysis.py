# app/services/gee/uhi_analysis.py
"""Urban heat / LST metrics from real EO (MODIS LST, MODIS NDVI, Dynamic World, ERA5-land)."""
import calendar
import json
from datetime import datetime
from typing import Any

import ee

from app.core.cache import redis_cache

UHI_MIN_YEAR = 2000

DATA_SOURCES = [
    "MODIS/061/MOD11A2 (LST day/night)",
    "MODIS/061/MOD13A2 (NDVI 1 km)",
    "GOOGLE/DYNAMICWORLD/V1 (built probability)",
    "ECMWF/ERA5_LAND/MONTHLY_AGGR (2 m temperature, livability index)",
]

METHODOLOGY_SUMMARY = (
    "LST: annual median MOD11A2 day (primary for UHI) and night, QC-masked. "
    "NDVI: annual median MOD13A2. Built: mean Dynamic World 'built'. "
    "UHI intensity vs forest: mean day LST(entity) − mean day LST(forest reserves ∩ county), when forest exists. "
    "Built-up % is mean built probability × 100. "
    "Livability: share of months where ERA5-Land zonal-mean 2 m T is in [18, 26] °C (climate-scale, ~11 km). "
    "Cooling slope: OLS of day LST ~ NDVI across wards in the county (correlation, not causal). "
    "Heat risk: weighted index of LST, built, NDVI (0–100). "
    "Hotspots: grid cells (3 km) with day LST ≥ p75 of cell means inside the geometry."
)


def _year_end_date(year: int) -> str:
    if year >= datetime.now().year:
        return datetime.now().strftime("%Y-%m-%d")
    return f"{year}-12-31"


def _month_bounds(year: int, month: int) -> tuple[str, str]:
    last = calendar.monthrange(year, month)[1]
    return f"{year}-{month:02d}-01", f"{year}-{month:02d}-{last:02d}"


def _modis_lst_for_range_c(
    geometry: ee.Geometry, start: str, end: str, day: bool
) -> ee.Image:
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


def _modis_lst_annual_median_c(geometry: ee.Geometry, year: int, day: bool) -> ee.Image:
    return _modis_lst_for_range_c(
        geometry, f"{year}-01-01", _year_end_date(year), day
    )


def _modis_ndvi_for_range(geometry: ee.Geometry, start: str, end: str) -> ee.Image:
    col = (
        ee.ImageCollection("MODIS/061/MOD13A2")
        .filterDate(start, end)
        .filterBounds(geometry)
        .select("NDVI")
    )
    return col.median().multiply(0.0001).clamp(-0.2, 1.0)


def _modis_ndvi_annual_median(geometry: ee.Geometry, year: int) -> ee.Image:
    return _modis_ndvi_for_range(
        geometry, f"{year}-01-01", _year_end_date(year)
    )


def _dynamic_world_built_mean_range(geometry: ee.Geometry, start: str, end: str) -> ee.Image:
    dw = (
        ee.ImageCollection("GOOGLE/DYNAMICWORLD/V1")
        .filterBounds(geometry)
        .filterDate(start, end)
        .select("built")
    )
    return dw.mean()


def _dynamic_world_built_mean(geometry: ee.Geometry, year: int) -> ee.Image:
    return _dynamic_world_built_mean_range(
        geometry, f"{year}-01-01", _year_end_date(year)
    )


@redis_cache("uhi_zonal_v3", ttl=43200)
def compute_uhi_zonal_metrics(geojson_str: str, year: int) -> dict:
    """Day LST mean/min/max, night mean, NDVI mean, built mean. Two reduceRegion calls, one cache."""
    y = int(year)
    now_y = datetime.now().year
    if y < UHI_MIN_YEAR or y > now_y:
        return {"error": f"year must be between {UHI_MIN_YEAR} and {now_y}"}

    g = ee.Geometry(json.loads(geojson_str))
    crs_out = "EPSG:4326"
    scale_m = 1000

    ld = (
        _modis_lst_annual_median_c(g, y, True)
        .rename("lst_day_c")
        .reproject(crs=crs_out, scale=scale_m)
    )
    comb = ee.Reducer.mean().combine(
        reducer2=ee.Reducer.minMax(),
        outputPrefix="",
        sharedInputs=True,
    )
    r1 = ld.reduceRegion(comb, g, scale_m, maxPixels=1e13).getInfo()

    ln = (
        _modis_lst_annual_median_c(g, y, False)
        .rename("lst_night_c")
        .reproject(crs=crs_out, scale=scale_m)
    )
    nd = _modis_ndvi_annual_median(g, y).rename("ndvi").reproject(
        crs=crs_out, scale=scale_m
    )
    bu = _dynamic_world_built_mean(g, y).rename("built_mean").reproject(
        crs=crs_out, scale=scale_m
    )
    st = ln.addBands(nd).addBands(bu)
    r2 = st.reduceRegion(ee.Reducer.mean(), g, scale_m, maxPixels=1e13).getInfo()

    out: dict[str, Any] = {"year": y}

    if r1.get("lst_day_c_mean") is not None:
        out["lst_day_mean_c"] = round(float(r1["lst_day_c_mean"]), 3)
    elif r1.get("mean") is not None:
        out["lst_day_mean_c"] = round(float(r1["mean"]), 3)
    if r1.get("lst_day_c_min") is not None:
        out["lst_day_min_c"] = round(float(r1["lst_day_c_min"]), 3)
    if r1.get("lst_day_c_max") is not None:
        out["lst_day_max_c"] = round(float(r1["lst_day_c_max"]), 3)

    if r2.get("lst_night_c") is not None:
        out["lst_night_mean_c"] = round(float(r2["lst_night_c"]), 3)
    if r2.get("ndvi") is not None:
        out["ndvi_mean"] = round(float(r2["ndvi"]), 4)
    if r2.get("built_mean") is not None:
        out["built_probability_mean"] = round(float(r2["built_mean"]), 4)

    if len(out) <= 1:
        out["error"] = "no_valid_pixels_in_geometry"
    return out


@redis_cache("uhi_forest_baseline_v1", ttl=86400)
def compute_forest_baseline_lst_day(forest_geojson_str: str, year: int) -> dict:
    y = int(year)
    now_y = datetime.now().year
    if y < UHI_MIN_YEAR or y > now_y:
        return {"error": f"year must be between {UHI_MIN_YEAR} and {now_y}"}
    fg = ee.Geometry(json.loads(forest_geojson_str))
    ld = (
        _modis_lst_annual_median_c(fg, y, True)
        .rename("lst_f")
        .reproject(crs="EPSG:4326", scale=1000)
    )
    raw = ld.reduceRegion(ee.Reducer.mean(), fg, 1000, maxPixels=1e13).getInfo()
    if raw.get("lst_f") is not None:
        return {"lst_day_forest_mean_c": round(float(raw["lst_f"]), 3)}
    return {"error": "no_forest_lst"}


@redis_cache("uhi_era5_livability_v1", ttl=86400)
def compute_era5_livability_percent(geojson_str: str, year: int) -> dict:
    y = int(year)
    now_y = datetime.now().year
    if y < UHI_MIN_YEAR or y > now_y:
        return {"error": f"year must be between {UHI_MIN_YEAR} and {now_y}"}
    g = ee.Geometry(json.loads(geojson_str))
    col = (
        ee.ImageCollection("ECMWF/ERA5_LAND/MONTHLY_AGGR")
        .filterBounds(g)
        .filterDate(f"{y}-01-01", _year_end_date(y))
        .select("temperature_2m")
    )
    n = int(col.size().getInfo() or 0)
    if n == 0:
        return {"error": "no_era5_months"}

    months_in_range = 0
    month_values: list[dict[str, Any]] = []
    for i in range(n):
        im = ee.Image(col.toList(n).get(i))
        d = im.reduceRegion(
            ee.Reducer.mean(),
            g,
            11000,
            maxPixels=1e13,
        ).getInfo()
        key = (
            "temperature_2m"
            if d.get("temperature_2m") is not None
            else next((k for k in d if k and k != "error"), None)
        )
        if not key or d[key] is None:
            continue
        t_k = float(d[key])
        t = t_k - 273.15
        month_values.append({"temperature_2m_mean_c": round(t, 2)})
        if 18.0 <= t <= 26.0:
            months_in_range += 1

    denom = len(month_values) or 1
    return {
        "livability_months_in_optimal_range": months_in_range,
        "livability_months_total": len(month_values),
        "livability_percent": round(100.0 * months_in_range / denom, 1),
        "era5_monthly_means_c": month_values,
    }


@redis_cache("uhi_monthly_v1", ttl=21600)
def compute_uhi_monthly_metrics(geojson_str: str, year: int, month: int) -> dict:
    y, m = int(year), int(month)
    now = datetime.now()
    if y < UHI_MIN_YEAR or y > now.year or m < 1 or m > 12:
        return {"error": "invalid year or month"}
    if y == now.year and m > now.month:
        return {"error": "future_month"}

    start, end = _month_bounds(y, m)
    g = ee.Geometry(json.loads(geojson_str))
    crs_out = "EPSG:4326"
    scale_m = 1000

    ld = (
        _modis_lst_for_range_c(g, start, end, True)
        .rename("lst_day_c")
        .reproject(crs=crs_out, scale=scale_m)
    )
    comb = ee.Reducer.mean().combine(
        reducer2=ee.Reducer.minMax(),
        outputPrefix="",
        sharedInputs=True,
    )
    r1 = ld.reduceRegion(comb, g, scale_m, maxPixels=1e13).getInfo()

    nd = (
        _modis_ndvi_for_range(g, start, end)
        .rename("ndvi")
        .reproject(crs=crs_out, scale=scale_m)
    )
    bu = (
        _dynamic_world_built_mean_range(g, start, end)
        .rename("built_mean")
        .reproject(crs=crs_out, scale=scale_m)
    )
    r2 = nd.addBands(bu).reduceRegion(
        ee.Reducer.mean(), g, scale_m, maxPixels=1e13
    ).getInfo()

    out: dict[str, Any] = {"year": y, "month": m}
    if r1.get("lst_day_c_mean") is not None:
        out["lst_day_mean_c"] = round(float(r1["lst_day_c_mean"]), 3)
    if r1.get("lst_day_c_min") is not None:
        out["lst_day_min_c"] = round(float(r1["lst_day_c_min"]), 3)
    if r1.get("lst_day_c_max") is not None:
        out["lst_day_max_c"] = round(float(r1["lst_day_c_max"]), 3)
    if r2.get("ndvi") is not None:
        out["ndvi_mean"] = round(float(r2["ndvi"]), 4)
    if r2.get("built_mean") is not None:
        out["built_probability_mean"] = round(float(r2["built_mean"]), 4)
    if len(out) <= 2:
        out["error"] = "no_valid_pixels_in_geometry"
    return out


def get_uhi_lst_day_tile_url(geojson_str: str, year: int) -> dict:
    y = int(year)
    now_y = datetime.now().year
    if y < UHI_MIN_YEAR or y > now_y:
        return {"error": f"year must be between {UHI_MIN_YEAR} and {now_y}"}
    g = ee.Geometry(json.loads(geojson_str))
    lst = _modis_lst_annual_median_c(g, y, True).clip(g)
    vis = {
        "min": 20,
        "max": 45,
        "palette": [
            "#313695",
            "#4575b4",
            "#74add1",
            "#abd9e9",
            "#fee090",
            "#fdae61",
            "#f46d43",
            "#d73027",
            "#a50026",
        ],
    }
    mid = lst.visualize(**vis).getMapId()
    return {
        "tile_url": mid["tile_fetcher"].url_format,
        "band": "lst_day_c_annual_median",
        "year": y,
        "visualization": vis,
    }


def get_uhi_lst_night_tile_url(geojson_str: str, year: int) -> dict:
    y = int(year)
    now_y = datetime.now().year
    if y < UHI_MIN_YEAR or y > now_y:
        return {"error": f"year must be between {UHI_MIN_YEAR} and {now_y}"}
    g = ee.Geometry(json.loads(geojson_str))
    lst = _modis_lst_annual_median_c(g, y, False).clip(g)
    vis = {
        "min": 10,
        "max": 30,
        "palette": ["#0c2c84", "#225ea8", "#41b6c4", "#ffffcc", "#f16913", "#d7191c"],
    }
    mid = lst.visualize(**vis).getMapId()
    return {
        "tile_url": mid["tile_fetcher"].url_format,
        "band": "lst_night_c_annual_median",
        "year": y,
        "visualization": vis,
    }


@redis_cache("uhi_hotspots_v1", ttl=43200)
def compute_uhi_hotspots(geojson_str: str, year: int, grid_m: float = 3000) -> dict:
    y = int(year)
    now_y = datetime.now().year
    if y < UHI_MIN_YEAR or y > now_y:
        return {"error": f"year must be between {UHI_MIN_YEAR} and {now_y}"}

    geometry = ee.Geometry(json.loads(geojson_str))
    lst = _modis_lst_annual_median_c(geometry, y, True)
    proj = ee.Projection("EPSG:3857")
    grid = geometry.coveringGrid(proj, grid_m)

    reduced = lst.reduceRegions(
        collection=grid,
        reducer=ee.Reducer.mean(),
        scale=1000,
        tileScale=2,
    )

    fc_sample = reduced.getInfo()
    feats = fc_sample.get("features", [])
    if not feats:
        return {"hotspots": [], "priority_zones": []}

    means = []
    for f in feats:
        m = f["properties"].get("mean")
        if m is not None:
            means.append((float(m), f))

    if not means:
        return {"hotspots": [], "priority_zones": []}

    means.sort(key=lambda x: x[0])
    p75_idx = min(len(means) - 1, int(round(0.75 * (len(means) - 1))))
    threshold = means[p75_idx][0]

    hotspots = []
    for val, f in means:
        if val < threshold:
            continue
        geom = f.get("geometry")
        if not geom:
            continue
        hotspots.append({"mean_lst_day_c": round(val, 2), "geometry": geom})

    hotspots.sort(key=lambda h: h["mean_lst_day_c"], reverse=True)
    priority = []
    for i, h in enumerate(hotspots[:15], start=1):
        priority.append(
            {
                "rank": i,
                "priority": "CRITICAL"
                if i <= 3
                else "HIGH"
                if i <= 8
                else "ELEVATED",
                "mean_lst_day_c": h["mean_lst_day_c"],
                "geometry": h["geometry"],
            }
        )

    return {
        "hotspots": hotspots[:50],
        "priority_zones": priority,
        "threshold_lst_day_c_p75": round(threshold, 2),
        "grid_cell_m": grid_m,
    }
