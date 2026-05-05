# app/api/endpoints/uhi.py
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.services import uhi_service

router = APIRouter()


def _year_or_default(year: Optional[int]) -> int:
    if year is None:
        return datetime.now().year - 1
    return year


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
