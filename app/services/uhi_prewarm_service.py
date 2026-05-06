# app/services/uhi_prewarm_service.py
"""Batch warm UHI Redis caches for pilot counties, wards, and intersecting forest reserves."""
from __future__ import annotations

import traceback
from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from app.core.redis_client import cache_get, make_cache_key
from app.services import uhi_service
from app.services.admin_service import (
    get_forest_reserves_intersecting_uhi_counties,
    get_uhi_counties,
    get_uhi_wards,
)
from app.services.gee.ee_init import initialize_ee
from app.services.gee.uhi_analysis import (
    compute_forest_baseline_lst_day,
    get_uhi_lst_day_tile_url,
    get_uhi_lst_night_tile_url,
    UHI_MIN_YEAR,
)
from app.services.uhi_report_service import (
    _full_county_report_cache_key,
    _full_ward_report_cache_key,
    _norm_geojson,
    county_uhi_report,
    ward_uhi_report,
)


def _forest_baseline_cache_key(geojson_norm: str, year: int) -> str:
    return make_cache_key("uhi_forest_baseline_v1", (geojson_norm, year), {})


def run_uhi_prewarm(
    db: Session,
    *,
    start_year: int,
    end_year: int,
    skip_if_cached: bool = True,
    force_refresh: bool = False,
    include_forest_baselines: bool = True,
    include_tiles: bool = False,
) -> dict[str, Any]:
    """
    Populate Redis for UHI pilot scope.
    - Full county & ward reports: one cached payload each (reuses inner GEE caches).
    - Forest reserves intersecting UHI counties: forest-baseline LST per reserve/year.
    - Optional LST tile mapIds (heavy); skipped by default.
    """
    now_y = datetime.now().year
    y0 = max(UHI_MIN_YEAR, int(start_year))
    y1 = min(now_y, int(end_year))
    if y1 < y0:
        return {"error": "end_year must be >= start_year", "years": []}

    years = list(range(y0, y1 + 1))
    initialize_ee()

    stats: dict[str, Any] = {
        "years": years,
        "county_reports_computed": 0,
        "county_reports_skipped": 0,
        "ward_reports_computed": 0,
        "ward_reports_skipped": 0,
        "forest_baselines_computed": 0,
        "forest_baselines_skipped": 0,
        "tiles_computed": 0,
        "tiles_skipped": 0,
        "errors": [],
    }

    counties = get_uhi_counties(db)
    wards = get_uhi_wards(db)

    # --- County full reports (includes ward metrics table + merged hotspots for that year)
    for c in counties:
        cid = str(c["id"])
        for y in years:
            ck = _full_county_report_cache_key(cid, y)
            try:
                if (
                    skip_if_cached
                    and not force_refresh
                    and cache_get(ck) is not None
                ):
                    stats["county_reports_skipped"] += 1
                    continue
                county_uhi_report(db, cid, y, force_refresh=force_refresh)
                stats["county_reports_computed"] += 1
            except Exception:
                stats["errors"].append(
                    {
                        "step": "county_report",
                        "county_id": cid,
                        "year": y,
                        "detail": traceback.format_exc(limit=6),
                    }
                )

    # --- Ward full reports (ward-level priority zones, etc.)
    for w in wards:
        wid = str(w["id"])
        for y in years:
            wk = _full_ward_report_cache_key(wid, y)
            try:
                if (
                    skip_if_cached
                    and not force_refresh
                    and cache_get(wk) is not None
                ):
                    stats["ward_reports_skipped"] += 1
                    continue
                ward_uhi_report(db, wid, y, force_refresh=force_refresh)
                stats["ward_reports_computed"] += 1
            except Exception:
                stats["errors"].append(
                    {
                        "step": "ward_report",
                        "ward_id": wid,
                        "year": y,
                        "detail": traceback.format_exc(limit=6),
                    }
                )

    # --- Per-reserve forest baseline LST (same cache key as compute_forest_baseline_lst_day)
    if include_forest_baselines:
        reserves = get_forest_reserves_intersecting_uhi_counties(db)
        stats["forest_reserve_count"] = len(reserves)
        for r in reserves:
            gj = _norm_geojson(r["geometry"])
            for y in years:
                fk = _forest_baseline_cache_key(gj, y)
                try:
                    if (
                        skip_if_cached
                        and not force_refresh
                        and cache_get(fk) is not None
                    ):
                        stats["forest_baselines_skipped"] += 1
                        continue
                    compute_forest_baseline_lst_day(gj, y)
                    stats["forest_baselines_computed"] += 1
                except Exception:
                    stats["errors"].append(
                        {
                            "step": "forest_baseline",
                            "reserve_id": r["id"],
                            "year": y,
                            "detail": traceback.format_exc(limit=6),
                        }
                    )

    # --- Map tiles (optional; getMapId cached per geometry+year)
    if include_tiles:
        for c in counties:
            cid = str(c["id"])
            g = uhi_service.get_uhi_geometry_normalized(db, "county", cid)
            if not g:
                continue
            for y in years:
                try:
                    dk = make_cache_key("uhi_tile_lst_day_v1", (g, y), {})
                    nk = make_cache_key("uhi_tile_lst_night_v1", (g, y), {})
                    if skip_if_cached and not force_refresh:
                        if cache_get(dk) is not None and cache_get(nk) is not None:
                            stats["tiles_skipped"] += 2
                            continue
                    get_uhi_lst_day_tile_url(g, y)
                    get_uhi_lst_night_tile_url(g, y)
                    stats["tiles_computed"] += 2
                except Exception:
                    stats["errors"].append(
                        {
                            "step": "county_tiles",
                            "county_id": cid,
                            "year": y,
                            "detail": traceback.format_exc(limit=6),
                        }
                    )
        for w in wards:
            wid = str(w["id"])
            g = uhi_service.get_uhi_geometry_normalized(db, "ward", wid)
            if not g:
                continue
            for y in years:
                try:
                    dk = make_cache_key("uhi_tile_lst_day_v1", (g, y), {})
                    nk = make_cache_key("uhi_tile_lst_night_v1", (g, y), {})
                    if skip_if_cached and not force_refresh:
                        if cache_get(dk) is not None and cache_get(nk) is not None:
                            stats["tiles_skipped"] += 2
                            continue
                    get_uhi_lst_day_tile_url(g, y)
                    get_uhi_lst_night_tile_url(g, y)
                    stats["tiles_computed"] += 2
                except Exception:
                    stats["errors"].append(
                        {
                            "step": "ward_tiles",
                            "ward_id": wid,
                            "year": y,
                            "detail": traceback.format_exc(limit=6),
                        }
                    )

    stats["errors"] = stats["errors"][:80]
    stats["ok"] = len(stats["errors"]) == 0
    return stats


def uhi_prewarm_status(
    db: Session,
    *,
    start_year: int,
    end_year: int,
) -> dict[str, Any]:
    """Lightweight check: which full reports / forest baselines are already cached (no GEE)."""
    now_y = datetime.now().year
    y0 = max(UHI_MIN_YEAR, int(start_year))
    y1 = min(now_y, int(end_year))
    if y1 < y0:
        return {"error": "end_year must be >= start_year"}

    years = list(range(y0, y1 + 1))
    counties = get_uhi_counties(db)
    wards = get_uhi_wards(db)
    reserves = get_forest_reserves_intersecting_uhi_counties(db)

    county_cached = 0
    county_total = len(counties) * len(years)
    for c in counties:
        cid = str(c["id"])
        for y in years:
            if cache_get(_full_county_report_cache_key(cid, y)) is not None:
                county_cached += 1

    ward_cached = 0
    ward_total = len(wards) * len(years)
    for w in wards:
        wid = str(w["id"])
        for y in years:
            if cache_get(_full_ward_report_cache_key(wid, y)) is not None:
                ward_cached += 1

    forest_cached = 0
    forest_total = len(reserves) * len(years)
    for r in reserves:
        gj = _norm_geojson(r["geometry"])
        for y in years:
            if cache_get(_forest_baseline_cache_key(gj, y)) is not None:
                forest_cached += 1

    return {
        "years": years,
        "pilot_counties": len(counties),
        "pilot_wards": len(wards),
        "forest_reserves_intersecting_pilot": len(reserves),
        "full_county_reports": {
            "cached": county_cached,
            "total": county_total,
            "complete": county_cached >= county_total and county_total > 0,
        },
        "full_ward_reports": {
            "cached": ward_cached,
            "total": ward_total,
            "complete": ward_cached >= ward_total and ward_total > 0,
        },
        "forest_baselines": {
            "cached": forest_cached,
            "total": forest_total,
            "complete": forest_cached >= forest_total and forest_total > 0,
        },
    }
