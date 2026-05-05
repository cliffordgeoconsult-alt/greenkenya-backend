# app/services/uhi_report_service.py
"""Assembled UHI intelligence reports (real metrics only)."""
from datetime import datetime
from typing import Any, Optional

import numpy as np
from sqlalchemy import text
from sqlalchemy.orm import Session

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


def _risk_level(score: int) -> str:
    if score >= 90:
        return "CRITICAL"
    if score >= 70:
        return "HIGH"
    if score >= 45:
        return "MODERATE"
    return "LOW"


def _heat_risk_score(lst_day: float, built_p: float, ndvi: float) -> int:
    t = max(0.0, min(1.0, (lst_day - 22.0) / (40.0 - 22.0)))
    b = max(0.0, min(1.0, built_p))
    v = max(0.0, min(1.0, 1.0 - ndvi))
    s = 100.0 * (0.45 * t + 0.35 * b + 0.20 * v)
    return int(max(0, min(100, round(s))))


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
        ys.append(row)
    return ys


def ward_uhi_report(db: Session, ward_id: str, year: int) -> dict[str, Any]:
    initialize_ee()
    wards = get_uhi_wards(db)
    ward = next((w for w in wards if str(w["id"]) == str(ward_id)), None)
    if not ward:
        return {"error": "Ward not found or not in UHI pilot counties"}
    counties = get_uhi_counties(db)
    county = next(
        (c for c in counties if str(c["id"]) == str(ward["county_id"])), None
    )
    if not county:
        return {"error": "Parent county not in pilot list"}

    g = _norm_geojson(ward["geometry"])
    m = compute_uhi_zonal_metrics(g, year)
    if m.get("error"):
        return {
            "error": m["error"],
            "entity_id": str(ward_id),
            "data_sources": DATA_SOURCES,
            "methodology": METHODOLOGY_SUMMARY,
        }
    if (
        m.get("lst_day_mean_c") is None
        or m.get("ndvi_mean") is None
        or m.get("built_probability_mean") is None
    ):
        return {
            "error": "incomplete_zonal_metrics",
            "entity_id": str(ward_id),
            "data_sources": DATA_SOURCES,
            "methodology": METHODOLOGY_SUMMARY,
        }

    lst_day = float(m["lst_day_mean_c"])
    ndvi = float(m["ndvi_mean"])
    built_p = float(m["built_probability_mean"])
    built_pct = round(built_p * 100, 2)

    fgj = _forest_union_geojson(db, str(ward["county_id"]))
    uhi_block: dict[str, Any] = {"intensity": None, "baseline_type": "none"}
    if fgj:
        fb = compute_forest_baseline_lst_day(_norm_geojson(fgj), year)
        if fb.get("lst_day_forest_mean_c") is not None:
            uhi_block["intensity"] = round(
                lst_day - float(fb["lst_day_forest_mean_c"]), 2
            )
            uhi_block["baseline_type"] = "forest"
            uhi_block["baseline_description"] = (
                "Mean day LST over union of forest reserves intersecting the county."
            )
    else:
        uhi_block["baseline_note"] = (
            "No intersecting forest reserve geometry in database for this county."
        )

    cool = county_vegetation_cooling_slope(db, str(ward["county_id"]), year)
    cooling_val = cool.get("cooling_effect_c_per_10pct_ndvi_proxy")

    risk_s = _heat_risk_score(lst_day, built_p, ndvi)

    era = compute_era5_livability_percent(g, year)
    liv_pct = era.get("livability_percent") if not era.get("error") else None
    livability: dict[str, Any] = {
        "percent_optimal": liv_pct,
        "method": "era5_land_monthly_2m_t_share_in_18_26C",
    }
    if era.get("error"):
        livability["error"] = era["error"]
    else:
        livability["months_in_optimal"] = era.get("livability_months_in_optimal_range")
        livability["months_total"] = era.get("livability_months_total")
        livability["era5_monthly_means_c"] = era.get("era5_monthly_means_c")

    base_2000 = compute_uhi_zonal_metrics(g, UHI_MIN_YEAR)
    trend: dict[str, Any] = {}
    if not base_2000.get("error") and m.get("lst_day_mean_c") is not None:
        if base_2000.get("lst_day_mean_c") is not None:
            trend["temp_change_since_2000"] = round(
                float(m["lst_day_mean_c"]) - float(base_2000["lst_day_mean_c"]),
                2,
            )
        if base_2000.get("ndvi_mean") is not None and m.get("ndvi_mean") is not None:
            trend["ndvi_change_since_2000"] = round(
                float(m["ndvi_mean"]) - float(base_2000["ndvi_mean"]),
                4,
            )

    hs = compute_uhi_hotspots(g, year)
    hotspots = hs.get("hotspots", []) if not hs.get("error") else []
    priority = hs.get("priority_zones", []) if not hs.get("error") else []

    yearly = _yearly_built_green_series(g, year)

    months_block: Optional[list[dict[str, Any]]] = None
    if year == datetime.now().year:
        months_block = []
        for mo in range(1, datetime.now().month + 1):
            months_block.append(compute_uhi_monthly_metrics(g, year, mo))

    return {
        "year": year,
        "level": "ward",
        "entity_id": str(ward["id"]),
        "name": ward["name"],
        "county_id": str(ward["county_id"]),
        "county_name": county["name"],
        "temperature": {
            "avg": m.get("lst_day_mean_c"),
            "max": m.get("lst_day_max_c"),
            "min": m.get("lst_day_min_c"),
            "night_avg": m.get("lst_night_mean_c"),
        },
        "uhi": uhi_block,
        "vegetation": {
            "ndvi": m.get("ndvi_mean"),
            "cooling_effect_per_10pct": cooling_val,
            "cooling_regression": cool,
        },
        "urbanization": {
            "built_up_percent": built_pct,
            "built_probability_mean": built_p,
        },
        "livability": livability,
        "risk": {
            "heat_risk_score": risk_s,
            "level": _risk_level(risk_s),
        },
        "trend": trend,
        "yearly_greening_built": yearly,
        "hotspots": hotspots,
        "priority_zones": priority,
        "monthly": months_block,
        "data_sources": DATA_SOURCES,
        "methodology": METHODOLOGY_SUMMARY,
    }


def county_uhi_report(db: Session, county_id: str, year: int) -> dict[str, Any]:
    initialize_ee()
    counties = get_uhi_counties(db)
    county = next((c for c in counties if str(c["id"]) == str(county_id)), None)
    if not county:
        return {"error": "County not found or not in UHI pilot list"}

    g = _norm_geojson(county["geometry"])
    m = compute_uhi_zonal_metrics(g, year)
    if m.get("error"):
        return {
            "error": m["error"],
            "entity_id": str(county_id),
            "data_sources": DATA_SOURCES,
            "methodology": METHODOLOGY_SUMMARY,
        }
    if (
        m.get("lst_day_mean_c") is None
        or m.get("ndvi_mean") is None
        or m.get("built_probability_mean") is None
    ):
        return {
            "error": "incomplete_zonal_metrics",
            "entity_id": str(county_id),
            "data_sources": DATA_SOURCES,
            "methodology": METHODOLOGY_SUMMARY,
        }

    lst_day = float(m["lst_day_mean_c"])
    ndvi = float(m["ndvi_mean"])
    built_p = float(m["built_probability_mean"])
    built_pct = round(built_p * 100, 2)

    fgj = _forest_union_geojson(db, str(county_id))
    uhi_block: dict[str, Any] = {
        "intensity": None,
        "baseline_type": "none",
    }
    if fgj:
        fb = compute_forest_baseline_lst_day(_norm_geojson(fgj), year)
        if fb.get("lst_day_forest_mean_c") is not None:
            uhi_block["intensity"] = round(
                lst_day - float(fb["lst_day_forest_mean_c"]), 2
            )
            uhi_block["baseline_type"] = "forest"
            uhi_block["baseline_description"] = (
                "Mean day LST over union of forest reserves intersecting the county."
            )
    else:
        uhi_block["baseline_note"] = (
            "No intersecting forest reserve geometry in database for this county."
        )

    cool = county_vegetation_cooling_slope(db, str(county_id), year)
    cooling_val = cool.get("cooling_effect_c_per_10pct_ndvi_proxy")
    risk_s = _heat_risk_score(lst_day, built_p, ndvi)
    era = compute_era5_livability_percent(g, year)
    liv_pct = era.get("livability_percent") if not era.get("error") else None
    livability_c: dict[str, Any] = {
        "percent_optimal": liv_pct,
        "method": "era5_land_monthly_2m_t_share_in_18_26C",
    }
    if era.get("error"):
        livability_c["error"] = era["error"]
    else:
        livability_c["months_in_optimal"] = era.get("livability_months_in_optimal_range")
        livability_c["months_total"] = era.get("livability_months_total")
        livability_c["era5_monthly_means_c"] = era.get("era5_monthly_means_c")

    base_2000 = compute_uhi_zonal_metrics(g, UHI_MIN_YEAR)
    trend: dict[str, Any] = {}
    if not base_2000.get("error"):
        if base_2000.get("lst_day_mean_c") is not None:
            trend["temp_change_since_2000"] = round(
                float(m["lst_day_mean_c"]) - float(base_2000["lst_day_mean_c"]),
                2,
            )
        if base_2000.get("ndvi_mean") is not None and m.get("ndvi_mean") is not None:
            trend["ndvi_change_since_2000"] = round(
                float(m["ndvi_mean"]) - float(base_2000["ndvi_mean"]),
                4,
            )

    hs = compute_uhi_hotspots(g, year)
    hotspots = hs.get("hotspots", []) if not hs.get("error") else []
    priority = hs.get("priority_zones", []) if not hs.get("error") else []

    yearly = _yearly_built_green_series(g, year)

    months_block: Optional[list[dict[str, Any]]] = None
    if year == datetime.now().year:
        months_block = []
        for mo in range(1, datetime.now().month + 1):
            months_block.append(compute_uhi_monthly_metrics(g, year, mo))

    return {
        "year": year,
        "level": "county",
        "entity_id": str(county["id"]),
        "name": county["name"],
        "temperature": {
            "avg": m.get("lst_day_mean_c"),
            "max": m.get("lst_day_max_c"),
            "min": m.get("lst_day_min_c"),
            "night_avg": m.get("lst_night_mean_c"),
        },
        "uhi": uhi_block,
        "vegetation": {
            "ndvi": m.get("ndvi_mean"),
            "cooling_effect_per_10pct": cooling_val,
            "cooling_regression": cool,
        },
        "urbanization": {
            "built_up_percent": built_pct,
            "built_probability_mean": built_p,
        },
        "livability": livability_c,
        "risk": {
            "heat_risk_score": risk_s,
            "level": _risk_level(risk_s),
        },
        "trend": trend,
        "yearly_greening_built": yearly,
        "hotspots": hotspots,
        "priority_zones": priority,
        "monthly": months_block,
        "data_sources": DATA_SOURCES,
        "methodology": METHODOLOGY_SUMMARY,
    }
