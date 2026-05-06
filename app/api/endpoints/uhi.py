# app/api/endpoints/uhi.py
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.services import uhi_service
from app.services.gee.ee_init import initialize_ee
from app.services.gee.uhi_analysis import (
    get_uhi_lst_day_tile_url,
    get_uhi_lst_night_tile_url,
)
from app.services.uhi_prewarm_service import run_uhi_prewarm, uhi_prewarm_status
from app.services.uhi_report_service import (
    county_uhi_report,
    county_wards_metrics_table,
    ward_uhi_report,
)

router = APIRouter()


def _year_or_default(year: Optional[int]) -> int:
    if year is None:
        return datetime.now().year - 1
    return year


@router.get("/prewarm/status")
def uhi_prewarm_status_endpoint(
    start_year: int = Query(2000, ge=2000),
    end_year: Optional[int] = Query(None, ge=2000),
    db: Session = Depends(get_db),
):
    """Check Redis coverage for full county/ward reports and forest baselines (no Earth Engine)."""
    now_y = datetime.now().year
    ey = end_year if end_year is not None else now_y
    return uhi_prewarm_status(db, start_year=start_year, end_year=ey)


@router.api_route("/prewarm", methods=["GET", "POST"])
def uhi_prewarm_run(
    start_year: int = Query(2000, ge=2000),
    end_year: Optional[int] = Query(None, ge=2000),
    skip_if_cached: bool = Query(
        True,
        description="If true, skip items already present in Redis (fast re-run).",
    ),
    force_refresh: bool = Query(
        False,
        description="If true, recompute and overwrite full report caches (and baselines/tiles when selected).",
    ),
    include_forest_baselines: bool = Query(True),
    include_tiles: bool = Query(
        False,
        description="Also warm LST day/night map tiles (many extra Earth Engine getMapId calls).",
    ),
    db: Session = Depends(get_db),
):
    """
    Warm UHI caches for all pilot counties, their wards, and forest reserves intersecting those counties.
    Uses full-report Redis so repeat runs are cheap when skip_if_cached=true. May take several minutes.
    """
    initialize_ee()
    now_y = datetime.now().year
    ey = end_year if end_year is not None else now_y
    return run_uhi_prewarm(
        db,
        start_year=start_year,
        end_year=ey,
        skip_if_cached=skip_if_cached,
        force_refresh=force_refresh,
        include_forest_baselines=include_forest_baselines,
        include_tiles=include_tiles,
    )


@router.get("/counties")
def uhi_counties(db: Session = Depends(get_db)):
    """Pilot counties for urban heat (no GEE call)."""
    return {"counties": uhi_service.list_uhi_counties(db)}


@router.get("/wards")
def uhi_wards(
    county_id: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """Wards in pilot counties; optional filter by county_id."""
    return {"wards": uhi_service.list_uhi_wards(db, county_id)}


@router.get("/county/{county_id}/report")
def uhi_county_report(
    county_id: str,
    year: Optional[int] = Query(None, ge=2000, le=2100),
    db: Session = Depends(get_db),
):
    """Full UHI intelligence: ward tables, worst wards, merged priority zones (county grid + worst wards), trends."""
    return county_uhi_report(db, county_id, _year_or_default(year))


@router.get("/county/{county_id}/wards/metrics")
def uhi_county_wards_metrics(
    county_id: str,
    year: Optional[int] = Query(None, ge=2000, le=2100),
    db: Session = Depends(get_db),
):
    """Per-ward LST, green cover (Dynamic World), built-up, UHI vs forest, heat risk; includes worst_wards_top_10."""
    initialize_ee()
    return county_wards_metrics_table(db, county_id, _year_or_default(year))


@router.get("/ward/{ward_id}/report")
def uhi_ward_report(
    ward_id: str,
    year: Optional[int] = Query(None, ge=2000, le=2100),
    db: Session = Depends(get_db),
):
    """Full UHI intelligence for a ward (includes county cooling regression)."""
    return ward_uhi_report(db, ward_id, _year_or_default(year))


@router.get("/tiles/lst-day")
def uhi_tile_lst_day(
    level: str = Query(..., description="county or ward"),
    entity_id: str = Query(...),
    year: Optional[int] = Query(None, ge=2000, le=2100),
    db: Session = Depends(get_db),
):
    """XYZ map tiles for annual median day LST (°C)."""
    initialize_ee()
    g = uhi_service.get_uhi_geometry_normalized(db, level, entity_id)
    if not g:
        raise HTTPException(status_code=404, detail="Entity not found or not in UHI pilot")
    return get_uhi_lst_day_tile_url(g, _year_or_default(year))


@router.get("/tiles/lst-night")
def uhi_tile_lst_night(
    level: str = Query(..., description="county or ward"),
    entity_id: str = Query(...),
    year: Optional[int] = Query(None, ge=2000, le=2100),
    db: Session = Depends(get_db),
):
    """XYZ map tiles for annual median night LST (°C)."""
    initialize_ee()
    g = uhi_service.get_uhi_geometry_normalized(db, level, entity_id)
    if not g:
        raise HTTPException(status_code=404, detail="Entity not found or not in UHI pilot")
    return get_uhi_lst_night_tile_url(g, _year_or_default(year))


@router.get("/county/{county_id}")
def uhi_county_metrics(
    county_id: str,
    year: Optional[int] = Query(None, ge=2000, le=2100),
    db: Session = Depends(get_db),
):
    """Zonal UHI-related metrics for county geometry (one cached GEE reduce per county/year)."""
    return uhi_service.county_uhi_metrics(db, county_id, _year_or_default(year))


@router.get("/ward/{ward_id}")
def uhi_ward_metrics(
    ward_id: str,
    year: Optional[int] = Query(None, ge=2000, le=2100),
    db: Session = Depends(get_db),
):
    """Zonal metrics for ward; includes excess LST vs county mean when computable."""
    return uhi_service.ward_uhi_metrics(db, ward_id, _year_or_default(year))


@router.get("/ward/{ward_id}/timeseries")
def uhi_ward_timeseries(
    ward_id: str,
    start_year: int = Query(..., ge=2000),
    end_year: int = Query(..., ge=2000),
    db: Session = Depends(get_db),
):
    """Year-by-year metrics; each year uses Redis cache after first computation."""
    if end_year < start_year:
        return {"error": "end_year must be >= start_year"}
    now_y = datetime.now().year
    if end_year > now_y:
        end_year = now_y
    series = []
    for y in range(start_year, end_year + 1):
        series.append(uhi_service.ward_uhi_metrics(db, ward_id, y))
    return {"ward_id": ward_id, "series": series}


@router.get("/county/{county_id}/timeseries")
def uhi_county_timeseries(
    county_id: str,
    start_year: int = Query(..., ge=2000),
    end_year: int = Query(..., ge=2000),
    db: Session = Depends(get_db),
):
    if end_year < start_year:
        return {"error": "end_year must be >= start_year"}
    now_y = datetime.now().year
    if end_year > now_y:
        end_year = now_y
    series = []
    for y in range(start_year, end_year + 1):
        series.append(uhi_service.county_uhi_metrics(db, county_id, y))
    return {"county_id": county_id, "series": series}
