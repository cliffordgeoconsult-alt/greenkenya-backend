# app/services/uhi_report_service.py
"""Assembled UHI intelligence reports (real metrics only)."""
from datetime import datetime
from typing import Any, Optional

import numpy as np
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.redis_client import cache_get, cache_set, make_cache_key
from app.services.admin_service import get_uhi_counties, get_uhi_wards
from app.services.gee.ee_init import initialize_ee
from app.services.gee.uhi_analysis import (
    compute_era5_livability_percent,
    compute_forest_baseline_lst_day,
    compute_uhi_hotspots,
    compute_uhi_monthly_metrics,
    compute_uhi_zonal_metrics,
    DATA_SOURCES,
    METHODOLOGY_SUMMARY,
    UHI_MIN_YEAR,
)

# Long-horizon trend baseline (MODIS Era consistent; Dynamic World from ~2015).
TREND_BASE_YEAR = 2001

# UHI reference: matches compute_forest_baseline_lst_day (MOD11A2 day, annual median composite).
UHI_BASELINE_NONE = "none"
UHI_BASELINE_FOREST_RESERVES_UNION = "forest_reserves_union_modis_day_lst"

# Full assembled API payloads (county/ward report) — avoids recomputing heavy GEE chains.
FULL_COUNTY_REPORT_TTL_SEC = 7 * 24 * 3600
FULL_WARD_REPORT_TTL_SEC = 7 * 24 * 3600


def _full_county_report_cache_key(county_id: str, year: int) -> str:
    return make_cache_key("uhi:full_county_report:v2", (str(county_id), int(year)), {})


def _full_ward_report_cache_key(ward_id: str, year: int) -> str:
    return make_cache_key("uhi:full_ward_report:v2", (str(ward_id), int(year)), {})


def _norm_geojson(geojson_string: str) -> str:
    import json

    return json.dumps(json.loads(geojson_string), sort_keys=True)


def _forest_union_geojson(db: Session, county_id: str) -> Optional[str]:
    row = db.execute(
        text("""
            SELECT ST_AsGeoJSON(
                ST_UnaryUnion(ST_Collect(r.geometry))
            ) AS gj
            FROM forest_reserves r
            INNER JOIN admin_county c
              ON c.id = CAST(:cid AS uuid)
              AND ST_Intersects(r.geometry, c.geometry)
        """),
        {"cid": county_id},
    ).fetchone()
    if not row or row[0] is None:
        return None
    return row[0]


def _forest_reserves_intersecting_county(
    db: Session, county_id: str
) -> list[dict[str, Any]]:
    rows = db.execute(
        text("""
            SELECT r.reserve_id, r.name
            FROM forest_reserves r
            INNER JOIN admin_county c
              ON c.id = CAST(:cid AS uuid)
              AND ST_Intersects(r.geometry, c.geometry)
            ORDER BY r.name NULLS LAST, r.reserve_id
        """),
        {"cid": county_id},
    ).fetchall()
    out: list[dict[str, Any]] = []
    for rid, name in rows:
        label = (name or "").strip() or "Unknown"
        out.append({"reserve_id": str(rid) if rid is not None else "", "name": label})
    return out


def _uhi_baseline_description(
    year: int, reserves: list[dict[str, Any]], reference_lst_c: float
) -> str:
    n = len(reserves)
    names = [r["name"] for r in reserves if r.get("name")]
    preview_n = 12
    head = names[:preview_n]
    tail = ""
    if len(names) > preview_n:
        tail = f" (+{len(names) - preview_n} more)"
    names_bit = f": {', '.join(head)}{tail}" if head else ""

    return (
        f"UHI intensity uses MODIS/061/MOD11A2 daytime land-surface temperature "
        f"(annual median composite for {year}): mean over this boundary minus "
        f"{reference_lst_c:.1f} °C, the mean over the merged PostGIS geometry of "
        f"{n} forest reserve record{'s' if n != 1 else ''} in our database that intersect "
        f"the parent county boundary{names_bit}."
    )


def _risk_level(score: int) -> str:
    if score >= 90:
        return "CRITICAL"
    if score >= 70:
        return "HIGH"
    if score >= 45:
        return "MODERATE"
    return "LOW"


def _heat_risk_score(
    lst_day: float, built_p: Optional[float], ndvi: float
) -> int:
    t = max(0.0, min(1.0, (lst_day - 22.0) / (40.0 - 22.0)))
    v = max(0.0, min(1.0, 1.0 - ndvi))
    if built_p is not None:
        b = max(0.0, min(1.0, built_p))
        s = 100.0 * (0.45 * t + 0.35 * b + 0.20 * v)
    else:
        s = 100.0 * (0.55 * t + 0.45 * v)
    return int(max(0, min(100, round(s))))


_HUMANE_GENERIC = (
    "Part of this profile is still being assembled from live Earth-engine pulls—check back soon."
)


def _humane_building_message(kind: str) -> str:
    # Do not use dict.get(..., recursive_call): Python evaluates the default eagerly.
    messages = {
        "no_pixels": (
            "We could not summarize satellite pixels for this boundary right now—"
            "coverage or masks left nothing to aggregate. Try again later; we are improving coverage checks."
        ),
        "incomplete": (
            "Some layers are still catching up for this year (for example built-up before 2015). "
            "Core temperature and vegetation metrics are shown where available."
        ),
        "generic": _HUMANE_GENERIC,
    }
    return messages.get(kind, _HUMANE_GENERIC)


def _insights_and_recommendations(
    *,
    lst_day: Optional[float],
    ndvi: Optional[float],
    built_pct: Optional[float],
    uhi_intensity: Optional[float],
    risk_level: Optional[str],
    risk_score: Optional[int],
    liv_pct: Optional[float],
    cooling: Optional[float],
    trend_temp: Optional[float],
    trend_ndvi: Optional[float],
    trend_built: Optional[float],
    excess_vs_county: Optional[float],
    era5_ok: bool,
    dw_ok: bool,
    forest_ok: bool,
) -> tuple[list[str], list[str]]:
    insights: list[str] = []
    recs: list[str] = []

    if excess_vs_county is not None:
        if excess_vs_county > 0.5:
            insights.append(
                f"This area runs about {excess_vs_county:.1f} °C warmer than the county mean by day (MODIS), "
                "which concentrates heat stress locally."
            )
            recs.append(
                "Prioritize shade, street trees, and cool surfaces in these warmer pockets first."
            )
        elif excess_vs_county < -0.5:
            insights.append(
                f"Cooler than the county average by about {abs(excess_vs_county):.1f} °C—relatively favorable for daytime heat."
            )

    if lst_day is not None and lst_day >= 33:
        insights.append(
            f"Daytime land-surface temperature is high (≈{lst_day:.1f} °C annual median), which amplifies felt heat in dense areas."
        )
    if ndvi is not None and ndvi < 0.35:
        insights.append(
            f"Vegetation index is limited (NDVI ≈{ndvi:.2f}), so there is little canopy cooling compared with greener zones."
        )
        recs.append(
            "Expand continuous tree canopy and vegetated corridors—small patches help less than connected cover."
        )
    if dw_ok and built_pct is not None and built_pct > 40:
        insights.append(
            f"Built-up signal is strong (~{built_pct:.0f}% mean built probability), which typically holds more heat overnight."
        )
    if not dw_ok:
        insights.append(
            "Built-up percentage is not available for this year from Dynamic World (catalog starts ~2015)—compare recent years for urbanization trend."
        )
    if uhi_intensity is not None and uhi_intensity > 3:
        insights.append(
            f"Urban heat island intensity vs the mapped forest-reserve reference (MOD11A2 day LST) "
            f"is about {uhi_intensity:.1f} °C—this boundary reads noticeably hotter than the merged reserve geometry."
        )
    if risk_level in ("HIGH", "CRITICAL"):
        insights.append(
            f"Composite heat risk is {risk_level.lower()} (score {risk_score}). "
            "That combines hot land surface, sparse vegetation"
            + (
                ", and dense built fabric."
                if dw_ok
                else ", with vegetation stress where built-up data is missing."
            )
        )
        recs.append(
            "Coordinate heat alerts with urban greening and cool-roof programs in the hottest ranked cells."
        )
    elif risk_level == "LOW":
        insights.append("Overall heat-risk components look comparatively moderate this year.")

    if liv_pct is not None:
        insights.append(
            f"Climate livability index (ERA5-Land, coarse grid): about {liv_pct:.0f}% of months had mean 2 m temperature in the 18–26 °C comfort band."
        )
    elif not era5_ok:
        insights.append(
            "Livability from ERA5-Land was not available—often a transient Earth Engine or geometry edge case."
        )

    if cooling is not None:
        insights.append(
            f"Across wards in this county, regression suggests roughly {cooling:+.2f} °C day LST change per +10% vegetation proxy (correlational)."
        )

    if trend_temp is not None:
        if trend_temp > 1.0:
            insights.append(
                f"Since {TREND_BASE_YEAR}, median day LST rose about {trend_temp:.1f} °C—worth tracking alongside greening efforts."
            )
        elif trend_temp < -1.0:
            insights.append(
                f"Since {TREND_BASE_YEAR}, median day LST fell about {abs(trend_temp):.1f} °C in this zone."
            )
    if trend_ndvi is not None and abs(trend_ndvi) >= 0.05:
        insights.append(
            f"NDVI change since {TREND_BASE_YEAR} is about {trend_ndvi:+.3f}—{'greening' if trend_ndvi > 0 else 'browning'} signal in the MODIS record."
        )
    if trend_built is not None and abs(trend_built) >= 0.03:
        insights.append(
            f"Mean built probability changed by about {trend_built:+.3f} since {TREND_BASE_YEAR} (where both years have Dynamic World)."
        )

    if not forest_ok:
        insights.append(
            "UHI intensity vs a forest-reserve reference is unavailable—no forest reserve polygons "
            "in our database intersect the parent county boundary."
        )

    if not recs:
        recs.append(
            "Keep monitoring seasonal NDVI and night-time LST—cooling interventions show up fastest in night metrics and vegetation continuity."
        )
    return insights, recs


def build_uhi_year_snapshot(
    db: Session,
    *,
    geojson_norm: str,
    county_id: str,
    year: int,
    level: str,
    entity_id: str,
    name: str,
    county_name: Optional[str] = None,
    ward_county_excess_c: Optional[float] = None,
) -> dict[str, Any]:
    """Nested year payload (MODIS + DW + ERA5 + forest UHI); humane partials when layers fail."""
    y = int(year)
    m = compute_uhi_zonal_metrics(geojson_norm, y)

    base: dict[str, Any] = {
        "year": y,
        "level": level,
        "entity_id": entity_id,
        "name": name,
        "status": "complete",
        "message": None,
    }
    if county_name is not None:
        base["county_name"] = county_name
    if level == "ward":
        base["county_id"] = county_id

    if m.get("error"):
        base["status"] = "partial"
        base["message"] = _humane_building_message(
            "no_pixels" if m["error"] == "no_valid_pixels_in_geometry" else "generic"
        )
        base["temperature"] = None
        base["uhi"] = None
        base["vegetation"] = {"ndvi": None, "green_cover_percent": None}
        base["urbanization"] = None
        base["livability"] = None
        base["risk"] = None
        base["trend"] = None
        base["hotspots"] = []
        base["insights"] = [base["message"]]
        base["recommendations"] = [
            "Retry after a few minutes; if this persists, the boundary may need a topology check."
        ]
        return base

    lst_mean = m.get("lst_day_mean_c")
    lst_min = m.get("lst_day_min_c")
    lst_max = m.get("lst_day_max_c")
    ndvi = m.get("ndvi_mean")
    built_p = m.get("built_probability_mean")

    if lst_mean is None:
        base["status"] = "partial"
        base["message"] = _humane_building_message("incomplete")
        base["temperature"] = None
        base["uhi"] = None
        base["vegetation"] = {
            "ndvi": ndvi,
            "green_cover_percent": m.get("green_cover_percent"),
            "cooling_effect_per_10pct": None,
        }
        base["urbanization"] = {
            "built_up_percent": round(float(built_p) * 100, 1)
            if built_p is not None
            else None
        }
        base["livability"] = {"percent_optimal": None}
        base["risk"] = None
        base["trend"] = None
        base["hotspots"] = []
        base["insights"] = [base["message"]]
        base["recommendations"] = [
            "Once LST composite fills in, risk and livability scores will populate automatically."
        ]
        return base

    lst_day_f = float(lst_mean)
    ndvi_f = float(ndvi) if ndvi is not None else None

    dw_ok = built_p is not None
    built_pct = round(float(built_p) * 100, 1) if dw_ok else None

    forest_reserves = _forest_reserves_intersecting_county(db, str(county_id))
    fgj = _forest_union_geojson(db, str(county_id))
    uhi_intensity: Optional[float] = None
    baseline_code = UHI_BASELINE_NONE
    baseline_description: Optional[str] = None
    reference_lst_c: Optional[float] = None
    forest_ok = bool(fgj)
    if fgj:
        fb = compute_forest_baseline_lst_day(_norm_geojson(fgj), y)
        ref = fb.get("lst_day_forest_mean_c")
        if ref is not None:
            reference_lst_c = float(ref)
            uhi_intensity = round(lst_day_f - reference_lst_c, 2)
            baseline_code = UHI_BASELINE_FOREST_RESERVES_UNION
            baseline_description = _uhi_baseline_description(
                y, forest_reserves, reference_lst_c
            )

    cool_reg = county_vegetation_cooling_slope(db, str(county_id), y)
    cooling_val = cool_reg.get("cooling_effect_c_per_10pct_ndvi_proxy")

    era = compute_era5_livability_percent(geojson_norm, y)
    era5_ok = not era.get("error")
    liv_pct = era.get("livability_percent") if era5_ok else None

    risk_s = None
    risk_lv = None
    if ndvi_f is not None:
        risk_s = _heat_risk_score(lst_day_f, float(built_p) if built_p is not None else None, ndvi_f)
        risk_lv = _risk_level(risk_s)

    base_m = compute_uhi_zonal_metrics(geojson_norm, TREND_BASE_YEAR)
    trend: dict[str, Any] = {}
    if not base_m.get("error"):
        if base_m.get("lst_day_mean_c") is not None:
            trend["temp_change_since_2001"] = round(
                lst_day_f - float(base_m["lst_day_mean_c"]), 2
            )
        if (
            ndvi_f is not None
            and base_m.get("ndvi_mean") is not None
        ):
            trend["ndvi_change"] = round(
                ndvi_f - float(base_m["ndvi_mean"]), 4
            )
        if (
            built_p is not None
            and base_m.get("built_probability_mean") is not None
        ):
            trend["built_change"] = round(
                float(built_p) - float(base_m["built_probability_mean"]), 4
            )

    hs = compute_uhi_hotspots(geojson_norm, y, max_priority_zones=10)
    hotspots_out: list[dict[str, Any]] = []
    if not hs.get("error"):
        for p in hs.get("priority_zones") or []:
            hotspots_out.append(
                {
                    "rank": p.get("rank"),
                    "priority": p.get("priority"),
                    "mean_lst_day_c": p.get("mean_lst_day_c"),
                    "mean_ndvi": p.get("mean_ndvi"),
                    "built_up_percent": p.get("built_up_percent"),
                    "green_cover_percent": p.get("green_cover_percent"),
                    "geometry": p.get("geometry"),
                }
            )
        if not hotspots_out:
            for h in (hs.get("hotspots") or [])[:25]:
                hotspots_out.append(
                    {
                        "mean_lst_day_c": h.get("mean_lst_day_c"),
                        "geometry": h.get("geometry"),
                    }
                )

    partial_note = None
    dw_catalog_year = m.get("dynamic_world_available")
    if not era5_ok or ndvi_f is None:
        base["status"] = "partial"
        partial_note = _humane_building_message("incomplete")
    elif dw_catalog_year is True and built_p is None:
        base["status"] = "partial"
        partial_note = _humane_building_message("incomplete")

    insights, recs = _insights_and_recommendations(
        lst_day=lst_day_f,
        ndvi=ndvi_f,
        built_pct=built_pct,
        uhi_intensity=uhi_intensity,
        risk_level=risk_lv,
        risk_score=risk_s,
        liv_pct=liv_pct,
        cooling=cooling_val,
        trend_temp=trend.get("temp_change_since_2001"),
        trend_ndvi=trend.get("ndvi_change"),
        trend_built=trend.get("built_change"),
        excess_vs_county=ward_county_excess_c,
        era5_ok=era5_ok,
        dw_ok=dw_ok,
        forest_ok=forest_ok,
    )
    if partial_note:
        insights.insert(0, partial_note)
    if dw_catalog_year is False and m.get("dynamic_world_message"):
        insights.append(m["dynamic_world_message"])

    base["message"] = partial_note
    base["temperature"] = {
        "mean": round(lst_day_f, 1),
        "min": round(float(lst_min), 1) if lst_min is not None else None,
        "max": round(float(lst_max), 1) if lst_max is not None else None,
    }
    uhi_block: dict[str, Any] = {
        "intensity": uhi_intensity,
        "baseline": baseline_code,
        "forest_reserves": forest_reserves,
    }
    if baseline_description is not None:
        uhi_block["baseline_description"] = baseline_description
    if reference_lst_c is not None:
        uhi_block["reference_lst_day_mean_c"] = round(reference_lst_c, 3)
    base["uhi"] = uhi_block
    gcp = m.get("green_cover_percent")
    veg_block: dict[str, Any] = {
        "ndvi": round(ndvi_f, 4) if ndvi_f is not None else None,
        "green_cover_percent": gcp,
        "cooling_effect_per_10pct": cooling_val,
    }
    if dw_catalog_year is False:
        veg_block["green_cover_percent"] = None
        veg_block["green_cover_status"] = "unavailable"
        if m.get("dynamic_world_message"):
            veg_block["green_cover_message"] = m["dynamic_world_message"]
    base["vegetation"] = veg_block

    urb_block: dict[str, Any] = {"built_up_percent": built_pct}
    if dw_catalog_year is False:
        urb_block["built_up_percent"] = None
        urb_block["built_up_status"] = "unavailable"
        if m.get("dynamic_world_message"):
            urb_block["built_up_message"] = m["dynamic_world_message"]
    base["urbanization"] = urb_block
    base["livability"] = {"percent_optimal": liv_pct}
    base["risk"] = (
        {"heat_risk_score": risk_s, "level": risk_lv}
        if risk_s is not None and risk_lv is not None
        else None
    )
    base["trend"] = trend if trend else {}
    base["hotspots"] = hotspots_out
    base["insights"] = insights
    base["recommendations"] = recs

    return base


def county_uhi_year_snapshot(db: Session, county_id: str, year: int) -> dict[str, Any]:
    initialize_ee()
    counties = get_uhi_counties(db)
    county = next((c for c in counties if str(c["id"]) == str(county_id)), None)
    if not county:
        return {
            "year": year,
            "level": "county",
            "entity_id": str(county_id),
            "status": "partial",
            "message": "This county is not in the UHI pilot list yet—we are expanding coverage gradually.",
            "temperature": None,
            "uhi": None,
            "vegetation": None,
            "urbanization": None,
            "livability": None,
            "risk": None,
            "trend": {},
            "hotspots": [],
            "insights": ["County not found or not in UHI pilot list."],
            "recommendations": ["Pick a pilot county from GET /uhi/counties."],
        }
    g = _norm_geojson(county["geometry"])
    return build_uhi_year_snapshot(
        db,
        geojson_norm=g,
        county_id=str(county_id),
        year=year,
        level="county",
        entity_id=str(county["id"]),
        name=county["name"],
    )


def ward_uhi_year_snapshot(db: Session, ward_id: str, year: int) -> dict[str, Any]:
    initialize_ee()
    wards = get_uhi_wards(db)
    ward = next((w for w in wards if str(w["id"]) == str(ward_id)), None)
    if not ward:
        return {
            "year": year,
            "level": "ward",
            "entity_id": str(ward_id),
            "status": "partial",
            "message": "This ward is not in the pilot dataset yet.",
            "temperature": None,
            "uhi": None,
            "vegetation": None,
            "urbanization": None,
            "livability": None,
            "risk": None,
            "trend": {},
            "hotspots": [],
            "insights": ["Ward not found or not in UHI pilot counties."],
            "recommendations": ["Select a ward from GET /uhi/wards."],
        }
    counties = get_uhi_counties(db)
    county = next(
        (c for c in counties if str(c["id"]) == str(ward["county_id"])),
        None,
    )
    if not county:
        return {
            "year": year,
            "level": "ward",
            "entity_id": str(ward_id),
            "status": "partial",
            "message": "Parent county is missing from the pilot list.",
            "temperature": None,
            "uhi": None,
            "vegetation": None,
            "urbanization": None,
            "livability": None,
            "risk": None,
            "trend": {},
            "hotspots": [],
            "insights": ["Parent county not in pilot list."],
            "recommendations": [],
        }

    g = _norm_geojson(ward["geometry"])
    excess: Optional[float] = None
    cg = _norm_geojson(county["geometry"])
    wm = compute_uhi_zonal_metrics(g, year)
    cm = compute_uhi_zonal_metrics(cg, year)
    if (
        not wm.get("error")
        and not cm.get("error")
        and wm.get("lst_day_mean_c") is not None
        and cm.get("lst_day_mean_c") is not None
    ):
        excess = round(
            float(wm["lst_day_mean_c"]) - float(cm["lst_day_mean_c"]),
            3,
        )

    snap = build_uhi_year_snapshot(
        db,
        geojson_norm=g,
        county_id=str(ward["county_id"]),
        year=year,
        level="ward",
        entity_id=str(ward["id"]),
        name=ward["name"],
        county_name=county["name"],
        ward_county_excess_c=excess,
    )
    if excess is not None:
        snap["comparison_to_county"] = {
            "lst_day_excess_vs_county_mean_c": excess,
        }
    return snap


def county_vegetation_cooling_slope(
    db: Session, county_id: str, year: int
) -> dict[str, Any]:
    """OLS slope: LST_day ~ NDVI across UHI pilot wards in county (cached per ward)."""
    wards = [w for w in get_uhi_wards(db) if str(w["county_id"]) == str(county_id)]
    xs: list[float] = []
    ys: list[float] = []
    for w in wards:
        m = compute_uhi_zonal_metrics(_norm_geojson(w["geometry"]), year)
        if m.get("error"):
            continue
        if m.get("ndvi_mean") is None or m.get("lst_day_mean_c") is None:
            continue
        xs.append(float(m["ndvi_mean"]))
        ys.append(float(m["lst_day_mean_c"]))
    if len(xs) < 3:
        return {
            "cooling_effect_c_per_10pct_ndvi_proxy": None,
            "regression_n_wards": len(xs),
            "note": "Need ≥3 wards with valid LST and NDVI for county regression.",
        }
    coef = np.polyfit(np.array(xs), np.array(ys), 1)
    slope = float(coef[0])
    # +0.1 NDVI (≈ +10% green proxy) → slope * 0.1 °C change in LST
    delta = round(slope * 0.1, 3)
    return {
        "cooling_effect_c_per_10pct_ndvi_proxy": delta,
        "lst_per_unit_ndvi_slope": round(slope, 3),
        "regression_n_wards": len(xs),
        "note": "10% vegetation mapped to +0.1 NDVI as reporting proxy; ecological correlation only.",
    }


def _yearly_built_green_series(
    geojson_str: str, end_year: int, lookback: int = 8
) -> list[dict[str, Any]]:
    ys = []
    start = max(UHI_MIN_YEAR, end_year - lookback + 1)
    for y in range(start, end_year + 1):
        m = compute_uhi_zonal_metrics(geojson_str, y)
        if m.get("error"):
            continue
        row: dict[str, Any] = {"year": y}
        if m.get("ndvi_mean") is not None:
            row["ndvi_mean"] = m["ndvi_mean"]
        if m.get("built_probability_mean") is not None:
            row["built_up_percent"] = round(float(m["built_probability_mean"]) * 100, 2)
        if m.get("green_cover_percent") is not None:
            row["green_cover_percent"] = m["green_cover_percent"]
        ys.append(row)
    return ys


def _zone_dedupe_key(geometry: dict) -> tuple[float, float]:
    ring = geometry.get("coordinates") or [[]]
    pts = ring[0] if ring else []
    if not pts:
        return (0.0, 0.0)
    xs = [float(p[0]) for p in pts if isinstance(p, (list, tuple)) and len(p) >= 2]
    ys = [float(p[1]) for p in pts if isinstance(p, (list, tuple)) and len(p) >= 2]
    if not xs:
        return (0.0, 0.0)
    cx = sum(xs) / len(xs)
    cy = sum(ys) / len(ys)
    q = 750.0
    return (round(cx / q) * q, round(cy / q) * q)


def merge_county_priority_zones(
    county_hs: dict[str, Any],
    db: Session,
    county_id: str,
    year: int,
    worst_ward_rows: list[dict[str, Any]],
    max_zones: int = 10,
) -> list[dict[str, Any]]:
    """County grid hotspots plus top cells from worst wards; dedupe by coarse footprint."""
    candidates: list[dict[str, Any]] = []
    if not county_hs.get("error"):
        for p in county_hs.get("priority_zones") or []:
            row = {k: v for k, v in p.items() if k != "rank"}
            row["source"] = "county_grid"
            candidates.append(row)
    for wrow in worst_ward_rows[:10]:
        wid = wrow.get("ward_id")
        wname = wrow.get("name")
        if not wid:
            continue
        wgeom = None
        for w in get_uhi_wards(db):
            if str(w["county_id"]) != str(county_id) or str(w["id"]) != str(wid):
                continue
            wgeom = w["geometry"]
            break
        if not wgeom:
            continue
        whs = compute_uhi_hotspots(
            _norm_geojson(wgeom), year, max_priority_zones=5
        )
        if whs.get("error"):
            continue
        for p in (whs.get("priority_zones") or [])[:2]:
            row = {k: v for k, v in p.items() if k != "rank"}
            row["source"] = "ward_hotspot"
            row["source_ward_id"] = str(wid)
            row["source_ward_name"] = wname
            candidates.append(row)

    candidates.sort(
        key=lambda x: float(x.get("mean_lst_day_c") or 0.0), reverse=True
    )
    deduped: list[dict[str, Any]] = []
    keys: set[tuple[float, float]] = set()
    for c in candidates:
        g = c.get("geometry")
        if not g:
            continue
        key = _zone_dedupe_key(g)
        if key in keys:
            continue
        keys.add(key)
        deduped.append(c)
        if len(deduped) >= max_zones:
            break
    for i, z in enumerate(deduped, start=1):
        z["rank"] = i
        z["priority"] = (
            "CRITICAL" if i <= 3 else "HIGH" if i <= 7 else "ELEVATED"
        )
    return deduped


def county_wards_metrics_table(
    db: Session, county_id: str, year: int
) -> dict[str, Any]:
    """Per-ward zonal metrics (MODIS + Dynamic World) for one pilot county."""
    counties = get_uhi_counties(db)
    county = next((c for c in counties if str(c["id"]) == str(county_id)), None)
    if not county:
        return {
            "year": year,
            "county_id": str(county_id),
            "error": "county_not_in_uhi_pilot",
            "wards": [],
            "worst_wards_top_10": [],
        }
    forest_reserves = _forest_reserves_intersecting_county(db, str(county_id))
    fgj = _forest_union_geojson(db, str(county_id))
    forest_baseline: Optional[float] = None
    wards_baseline_description: Optional[str] = None
    if fgj:
        fb = compute_forest_baseline_lst_day(_norm_geojson(fgj), year)
        if fb.get("lst_day_forest_mean_c") is not None:
            forest_baseline = float(fb["lst_day_forest_mean_c"])
            wards_baseline_description = _uhi_baseline_description(
                int(year), forest_reserves, forest_baseline
            )

    rows: list[dict[str, Any]] = []
    for w in get_uhi_wards(db):
        if str(w["county_id"]) != str(county_id):
            continue
        m = compute_uhi_zonal_metrics(_norm_geojson(w["geometry"]), year)
        if m.get("error"):
            rows.append(
                {
                    "ward_id": str(w["id"]),
                    "name": w["name"],
                    "county_id": str(county_id),
                    "year": year,
                    "status": "no_data",
                    "message": m.get("error"),
                }
            )
            continue
        lst = m.get("lst_day_mean_c")
        ndvi = m.get("ndvi_mean")
        built = m.get("built_probability_mean")
        green_pct = m.get("green_cover_percent")
        built_pct = round(float(built) * 100, 1) if built is not None else None
        uhi_i = None
        if forest_baseline is not None and lst is not None:
            uhi_i = round(float(lst) - forest_baseline, 2)
        risk_s = None
        risk_lv = None
        if ndvi is not None and lst is not None:
            risk_s = _heat_risk_score(
                float(lst),
                float(built) if built is not None else None,
                float(ndvi),
            )
            risk_lv = _risk_level(risk_s)
        row_data: dict[str, Any] = {
            "ward_id": str(w["id"]),
            "name": w["name"],
            "county_id": str(county_id),
            "year": year,
            "status": "complete",
            "lst_day_mean_c": lst,
            "lst_night_mean_c": m.get("lst_night_mean_c"),
            "ndvi_mean": m.get("ndvi_mean"),
            "green_cover_percent": green_pct,
            "built_up_percent": built_pct,
            "uhi_intensity_vs_forest_c": uhi_i,
            "heat_risk_score": risk_s,
            "heat_risk_level": risk_lv,
        }
        if m.get("dynamic_world_available") is False:
            row_data["built_up_status"] = "unavailable"
            row_data["green_cover_status"] = "unavailable"
            if m.get("dynamic_world_message"):
                row_data["dynamic_world_message"] = m["dynamic_world_message"]
        rows.append(row_data)
    complete = [r for r in rows if r.get("status") == "complete"]
    worst = sorted(
        complete,
        key=lambda x: (
            x.get("heat_risk_score") is None,
            -(x.get("heat_risk_score") or 0),
        ),
    )[:10]
    return {
        "year": year,
        "county_id": str(county_id),
        "county_name": county["name"],
        "uhi_reference": {
            "baseline": (
                UHI_BASELINE_FOREST_RESERVES_UNION
                if forest_baseline is not None
                else UHI_BASELINE_NONE
            ),
            "baseline_description": wards_baseline_description,
            "reference_lst_day_mean_c": round(forest_baseline, 3)
            if forest_baseline is not None
            else None,
            "forest_reserves": forest_reserves,
        },
        "wards": rows,
        "worst_wards_top_10": worst,
    }


def _compute_ward_uhi_report(db: Session, ward_id: str, year: int) -> dict[str, Any]:
    initialize_ee()
    snap = ward_uhi_year_snapshot(db, ward_id, year)

    wards = get_uhi_wards(db)
    ward = next((w for w in wards if str(w["id"]) == str(ward_id)), None)
    if not ward:
        return {**snap, "data_sources": DATA_SOURCES, "methodology": METHODOLOGY_SUMMARY}

    counties = get_uhi_counties(db)
    county = next(
        (c for c in counties if str(c["id"]) == str(ward["county_id"])), None
    )
    if not county:
        return {**snap, "data_sources": DATA_SOURCES, "methodology": METHODOLOGY_SUMMARY}

    g = _norm_geojson(ward["geometry"])
    m = compute_uhi_zonal_metrics(g, year)
    cool = county_vegetation_cooling_slope(db, str(ward["county_id"]), year)
    era = compute_era5_livability_percent(g, year)
    yearly = _yearly_built_green_series(g, year)
    hs = compute_uhi_hotspots(g, year, max_priority_zones=10)

    months_block: Optional[list[dict[str, Any]]] = None
    if year == datetime.now().year:
        months_block = [
            compute_uhi_monthly_metrics(g, year, mo)
            for mo in range(1, datetime.now().month + 1)
        ]

    out: dict[str, Any] = {**snap}
    if isinstance(out.get("temperature"), dict) and m.get("lst_night_mean_c") is not None:
        out["temperature"]["night_mean"] = m["lst_night_mean_c"]

    veg = dict(out.get("vegetation") or {})
    veg["cooling_regression"] = cool
    out["vegetation"] = veg

    liv = dict(out.get("livability") or {})
    liv["method"] = "era5_land_monthly_2m_t_share_in_18_26C"
    if era.get("error"):
        liv["error"] = era["error"]
    else:
        liv["months_in_optimal"] = era.get("livability_months_in_optimal_range")
        liv["months_total"] = era.get("livability_months_total")
        liv["era5_monthly_means_c"] = era.get("era5_monthly_means_c")
    out["livability"] = liv

    out["yearly_greening_built"] = yearly
    out["monthly"] = months_block
    if not hs.get("error"):
        pz = list(hs.get("priority_zones") or [])
        out["priority_zones"] = pz
        out["hotspots"] = pz
    if m.get("built_probability_mean") is not None:
        ur = dict(out.get("urbanization") or {})
        ur["built_probability_mean"] = m["built_probability_mean"]
        out["urbanization"] = ur
    out["data_sources"] = DATA_SOURCES
    out["methodology"] = METHODOLOGY_SUMMARY
    return out


def ward_uhi_report(
    db: Session, ward_id: str, year: int, *, force_refresh: bool = False
) -> dict[str, Any]:
    key = _full_ward_report_cache_key(ward_id, year)
    if not force_refresh:
        hit = cache_get(key)
        if hit is not None:
            return hit
    out = _compute_ward_uhi_report(db, ward_id, year)
    cache_set(key, out, FULL_WARD_REPORT_TTL_SEC)
    return out


def _compute_county_uhi_report(db: Session, county_id: str, year: int) -> dict[str, Any]:
    initialize_ee()
    snap = county_uhi_year_snapshot(db, county_id, year)

    counties = get_uhi_counties(db)
    county = next((c for c in counties if str(c["id"]) == str(county_id)), None)
    if not county:
        return {**snap, "data_sources": DATA_SOURCES, "methodology": METHODOLOGY_SUMMARY}

    g = _norm_geojson(county["geometry"])
    m = compute_uhi_zonal_metrics(g, year)
    cool = county_vegetation_cooling_slope(db, str(county_id), year)
    era = compute_era5_livability_percent(g, year)
    yearly = _yearly_built_green_series(g, year)
    hs = compute_uhi_hotspots(g, year, max_priority_zones=10)

    months_block: Optional[list[dict[str, Any]]] = None
    if year == datetime.now().year:
        months_block = [
            compute_uhi_monthly_metrics(g, year, mo)
            for mo in range(1, datetime.now().month + 1)
        ]

    out: dict[str, Any] = {**snap}
    if isinstance(out.get("temperature"), dict) and m.get("lst_night_mean_c") is not None:
        out["temperature"]["night_mean"] = m["lst_night_mean_c"]

    veg = dict(out.get("vegetation") or {})
    veg["cooling_regression"] = cool
    out["vegetation"] = veg

    liv = dict(out.get("livability") or {})
    liv["method"] = "era5_land_monthly_2m_t_share_in_18_26C"
    if era.get("error"):
        liv["error"] = era["error"]
    else:
        liv["months_in_optimal"] = era.get("livability_months_in_optimal_range")
        liv["months_total"] = era.get("livability_months_total")
        liv["era5_monthly_means_c"] = era.get("era5_monthly_means_c")
    out["livability"] = liv

    out["yearly_greening_built"] = yearly
    out["monthly"] = months_block
    ward_tbl = county_wards_metrics_table(db, str(county_id), year)
    merged: list[dict[str, Any]] = []
    if not ward_tbl.get("error"):
        out["wards"] = ward_tbl["wards"]
        out["worst_wards_top_10"] = ward_tbl["worst_wards_top_10"]
        merged = merge_county_priority_zones(
            hs,
            db,
            str(county_id),
            year,
            list(ward_tbl["worst_wards_top_10"]),
        )
    else:
        out["wards"] = []
        out["worst_wards_top_10"] = []
    if merged:
        out["priority_zones"] = merged
        out["hotspots"] = merged
    elif not hs.get("error"):
        pz = list(hs.get("priority_zones") or [])
        out["priority_zones"] = pz
        out["hotspots"] = pz
    if m.get("built_probability_mean") is not None:
        ur = dict(out.get("urbanization") or {})
        ur["built_probability_mean"] = m["built_probability_mean"]
        out["urbanization"] = ur
    out["data_sources"] = DATA_SOURCES
    out["methodology"] = METHODOLOGY_SUMMARY
    return out


def county_uhi_report(
    db: Session, county_id: str, year: int, *, force_refresh: bool = False
) -> dict[str, Any]:
    key = _full_county_report_cache_key(county_id, year)
    if not force_refresh:
        hit = cache_get(key)
        if hit is not None:
            return hit
    out = _compute_county_uhi_report(db, county_id, year)
    cache_set(key, out, FULL_COUNTY_REPORT_TTL_SEC)
    return out
