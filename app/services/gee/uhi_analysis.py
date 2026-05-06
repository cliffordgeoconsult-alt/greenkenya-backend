# app/services/gee/uhi_analysis.py
"""Urban heat / LST metrics from real EO (MODIS LST, MODIS NDVI, Dynamic World, ERA5-land)."""
import calendar
import json
from datetime import datetime
from typing import Any, Optional

import ee

from app.core.cache import redis_cache

UHI_MIN_YEAR = 2000

# GOOGLE/DYNAMICWORLD/V1 begins 2015-06-27 — no meaningful annual DW composite before 2015.
DYNAMIC_WORLD_MIN_YEAR = 2015

DYNAMIC_WORLD_UNAVAILABLE_MSG = (
    "Built-up and green-cover metrics are not available for this year "
    "(Dynamic World catalog begins mid-2015)."
)


def dynamic_world_metrics_available(year: int) -> bool:
    return int(year) >= DYNAMIC_WORLD_MIN_YEAR


DATA_SOURCES = [
    "MODIS/061/MOD11A2 (LST day/night)",
    "MODIS/061/MOD13A2 (NDVI 1 km)",
    "GOOGLE/DYNAMICWORLD/V1 (built + vegetated class probabilities)",
    "ECMWF/ERA5_LAND/MONTHLY_AGGR (2 m temperature, livability index)",
]

METHODOLOGY_SUMMARY = (
    "LST: annual median MOD11A2 day (primary for UHI) and night, QC-masked. "
    "NDVI: annual median MOD13A2. Built: mean Dynamic World 'built' (not available before 2015). "
    "Green cover: mean sum of Dynamic World trees, grass, flooded_vegetation, crops, shrub_and_scrub probabilities (×100 as %); unavailable before 2015. "
    "UHI intensity vs forest: mean day LST(entity) − mean day LST(forest reserves ∩ county), when forest exists. "
    "Built-up % is mean built probability × 100. "
    "Livability: share of months where ERA5-Land zonal-mean 2 m T is in [18, 26] °C (climate-scale, ~11 km). "
    "Cooling slope: OLS of day LST ~ NDVI across wards in the county (correlation, not causal). "
    "Heat risk: weighted index of LST, built, NDVI (0–100). "
    "Hotspots: 3 km grid cells with day LST ≥ p75 of cell means; each cell reports LST, NDVI, built, green cover."
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
    # Dynamic World starts ~2015; empty collection → .mean() has no bands and breaks addBands.
    return ee.Image(
        ee.Algorithms.If(
            dw.size().gt(0),
            dw.mean(),
            ee.Image(0).rename("built"),
        )
    )


def _dynamic_world_built_mean(geometry: ee.Geometry, year: int) -> ee.Image:
    return _dynamic_world_built_mean_range(
        geometry, f"{year}-01-01", _year_end_date(year)
    )


_DW_GREEN_BANDS = [
    "trees",
    "grass",
    "flooded_vegetation",
    "crops",
    "shrub_and_scrub",
]


def _sum_dynamic_world_green(mean_img: ee.Image) -> ee.Image:
    green = mean_img.select("trees")
    for b in _DW_GREEN_BANDS[1:]:
        green = green.add(mean_img.select(b))
    return green.rename("green_mean")


def _dynamic_world_green_cover_mean_range(
    geometry: ee.Geometry, start: str, end: str
) -> ee.Image:
    dw = (
        ee.ImageCollection("GOOGLE/DYNAMICWORLD/V1")
        .filterBounds(geometry)
        .filterDate(start, end)
        .select(_DW_GREEN_BANDS)
    )
    mean_img = dw.mean()
    # No DW scenes (years before ~2015, or edge geometry/date) → empty bands; avoid .select crash.
    return ee.Image(
        ee.Algorithms.If(
            dw.size().gt(0),
            _sum_dynamic_world_green(mean_img),
            ee.Image(0).rename("green_mean"),
        )
    )


def _dynamic_world_green_cover_mean(geometry: ee.Geometry, year: int) -> ee.Image:
    return _dynamic_world_green_cover_mean_range(
        geometry, f"{year}-01-01", _year_end_date(year)
    )


@redis_cache("uhi_zonal_v6", ttl=43200)
def compute_uhi_zonal_metrics(geojson_str: str, year: int) -> dict:
    """Day LST mean/min/max, night mean, NDVI mean, built mean. Two reduceRegion calls, one cache."""
    y = int(year)
    now_y = datetime.now().year
    if y < UHI_MIN_YEAR or y > now_y:
        return {"error": f"year must be between {UHI_MIN_YEAR} and {now_y}"}

    g = ee.Geometry(json.loads(geojson_str))
    crs_out = "EPSG:4326"
    scale_m = 1000
    dw_avail = dynamic_world_metrics_available(y)

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
    if dw_avail:
        bu = _dynamic_world_built_mean(g, y).rename("built_mean").reproject(
            crs=crs_out, scale=scale_m
        )
        gr = _dynamic_world_green_cover_mean(g, y).rename("green_mean").reproject(
            crs=crs_out, scale=scale_m
        )
        st = ln.addBands(nd).addBands(bu).addBands(gr)
    else:
        st = ln.addBands(nd)
    r2 = st.reduceRegion(ee.Reducer.mean(), g, scale_m, maxPixels=1e13).getInfo()

    out: dict[str, Any] = {
        "year": y,
        "dynamic_world_available": dw_avail,
    }
    if not dw_avail:
        out["built_probability_mean"] = None
        out["green_cover_percent"] = None
        out["dynamic_world_message"] = DYNAMIC_WORLD_UNAVAILABLE_MSG

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
    if dw_avail:
        if r2.get("built_mean") is not None:
            out["built_probability_mean"] = round(float(r2["built_mean"]), 4)
        if r2.get("green_mean") is not None:
            out["green_cover_percent"] = round(float(r2["green_mean"]) * 100.0, 1)

    if not any(
        out.get(k) is not None
        for k in ("lst_day_mean_c", "lst_night_mean_c", "ndvi_mean")
    ):
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


@redis_cache("uhi_monthly_v2", ttl=21600)
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
    dw_avail = dynamic_world_metrics_available(y)
    if dw_avail:
        bu = (
            _dynamic_world_built_mean_range(g, start, end)
            .rename("built_mean")
            .reproject(crs=crs_out, scale=scale_m)
        )
        r2 = nd.addBands(bu).reduceRegion(
            ee.Reducer.mean(), g, scale_m, maxPixels=1e13
        ).getInfo()
    else:
        r2 = nd.reduceRegion(
            ee.Reducer.mean(), g, scale_m, maxPixels=1e13
        ).getInfo()

    out: dict[str, Any] = {
        "year": y,
        "month": m,
        "dynamic_world_available": dw_avail,
    }
    if not dw_avail:
        out["built_probability_mean"] = None
        out["dynamic_world_message"] = DYNAMIC_WORLD_UNAVAILABLE_MSG
    if r1.get("lst_day_c_mean") is not None:
        out["lst_day_mean_c"] = round(float(r1["lst_day_c_mean"]), 3)
    if r1.get("lst_day_c_min") is not None:
        out["lst_day_min_c"] = round(float(r1["lst_day_c_min"]), 3)
    if r1.get("lst_day_c_max") is not None:
        out["lst_day_max_c"] = round(float(r1["lst_day_c_max"]), 3)
    if r2.get("ndvi") is not None:
        out["ndvi_mean"] = round(float(r2["ndvi"]), 4)
    if dw_avail and r2.get("built_mean") is not None:
        out["built_probability_mean"] = round(float(r2["built_mean"]), 4)
    if out.get("lst_day_mean_c") is None and out.get("ndvi_mean") is None:
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


def _props_cell_metrics(props: dict) -> tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """Parse reduceRegions mean() output for lst, ndvi, built, green bands."""
    lst = props.get("lst_day_c")
    if lst is None:
        lst = props.get("lst_day_c_mean")
    if lst is None:
        lst = props.get("mean")
    ndvi = props.get("ndvi")
    if ndvi is None:
        ndvi = props.get("ndvi_mean")
    built = props.get("built_mean")
    if built is None:
        built = props.get("built_mean_mean")
    green = props.get("green_mean")
    if green is None:
        green = props.get("green_mean_mean")
    return (
        float(lst) if lst is not None else None,
        float(ndvi) if ndvi is not None else None,
        float(built) if built is not None else None,
        float(green) if green is not None else None,
    )


def _enriched_hotspot_dict(
    lst_c: float,
    ndvi: Optional[float],
    built_p: Optional[float],
    green_frac: Optional[float],
    geom: dict,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "mean_lst_day_c": round(lst_c, 2),
        "geometry": geom,
    }
    if ndvi is not None:
        row["mean_ndvi"] = round(ndvi, 4)
    if built_p is not None:
        row["built_up_percent"] = round(built_p * 100.0, 1)
    if green_frac is not None:
        row["green_cover_percent"] = round(green_frac * 100.0, 1)
    return row


@redis_cache("uhi_hotspots_v3", ttl=43200)
def compute_uhi_hotspots(
    geojson_str: str,
    year: int,
    grid_m: float = 3000,
    max_priority_zones: int = 10,
) -> dict:
    y = int(year)
    now_y = datetime.now().year
    if y < UHI_MIN_YEAR or y > now_y:
        return {"error": f"year must be between {UHI_MIN_YEAR} and {now_y}"}

    geometry = ee.Geometry(json.loads(geojson_str))
    crs_out = "EPSG:4326"
    scale_m = 1000
    dw_avail = dynamic_world_metrics_available(y)
    lst = (
        _modis_lst_annual_median_c(geometry, y, True)
        .rename("lst_day_c")
        .reproject(crs=crs_out, scale=scale_m)
    )
    ndvi = _modis_ndvi_annual_median(geometry, y).rename("ndvi").reproject(
        crs=crs_out, scale=scale_m
    )
    if dw_avail:
        built = _dynamic_world_built_mean(geometry, y).rename("built_mean").reproject(
            crs=crs_out, scale=scale_m
        )
        green = _dynamic_world_green_cover_mean(geometry, y).rename(
            "green_mean"
        ).reproject(crs=crs_out, scale=scale_m)
        stack = lst.addBands(ndvi).addBands(built).addBands(green)
    else:
        stack = lst.addBands(ndvi)

    proj = ee.Projection("EPSG:3857")
    grid = geometry.coveringGrid(proj, grid_m)

    reduced = stack.reduceRegions(
        collection=grid,
        reducer=ee.Reducer.mean(),
        scale=scale_m,
        tileScale=2,
    )

    fc_sample = reduced.getInfo()
    feats = fc_sample.get("features", [])
    if not feats:
        return {"hotspots": [], "priority_zones": []}

    means: list[tuple[float, dict[str, Any]]] = []
    for f in feats:
        props = f.get("properties") or {}
        lst_v, _, _, _ = _props_cell_metrics(props)
        if lst_v is not None:
            means.append((lst_v, f))

    if not means:
        return {"hotspots": [], "priority_zones": []}

    means.sort(key=lambda x: x[0])
    p75_idx = min(len(means) - 1, int(round(0.75 * (len(means) - 1))))
    threshold = means[p75_idx][0]

    hotspots: list[dict[str, Any]] = []
    for val, f in means:
        if val < threshold:
            continue
        geom = f.get("geometry")
        if not geom:
            continue
        props = f.get("properties") or {}
        _, ndvi_v, bu_v, gr_v = _props_cell_metrics(props)
        hotspots.append(_enriched_hotspot_dict(val, ndvi_v, bu_v, gr_v, geom))

    hotspots.sort(key=lambda h: h["mean_lst_day_c"], reverse=True)
    cap_p = max(1, min(50, int(max_priority_zones)))
    priority: list[dict[str, Any]] = []
    for i, h in enumerate(hotspots[:cap_p], start=1):
        priority.append(
            {
                "rank": i,
                "priority": "CRITICAL"
                if i <= 3
                else "HIGH"
                if i <= 7
                else "ELEVATED",
                "mean_lst_day_c": h["mean_lst_day_c"],
                "mean_ndvi": h.get("mean_ndvi"),
                "built_up_percent": h.get("built_up_percent"),
                "green_cover_percent": h.get("green_cover_percent"),
                "geometry": h["geometry"],
            }
        )

    out_hs: dict[str, Any] = {
        "hotspots": hotspots[:50],
        "priority_zones": priority,
        "threshold_lst_day_c_p75": round(threshold, 2),
        "grid_cell_m": grid_m,
        "dynamic_world_available": dw_avail,
    }
    if not dw_avail:
        out_hs["dynamic_world_message"] = DYNAMIC_WORLD_UNAVAILABLE_MSG
    return out_hs
