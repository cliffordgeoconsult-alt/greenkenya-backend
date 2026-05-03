# app/api/endpoints/carbon.py
from typing import Optional

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from datetime import datetime, UTC
from app.db.session import get_db
from uuid import UUID
from app.services.carbon_service import (
    get_county_loss_stats,
    get_county_loss_trend,
    get_single_county_carbon,
    get_single_county_loss,
    get_national_loss_trend,
    get_national_carbon_map,
    get_available_carbon_years,
    get_single_ward_carbon,
    get_ward_loss_stats,
    get_single_ward_loss,
    get_ward_loss_trend,
    get_single_reserve_carbon,
    get_reserve_loss_stats,
    get_single_reserve_loss,
    get_reserve_loss_trend
)

router = APIRouter()

def resolve_year(year: Optional[int]) -> int:
    return year if year is not None else datetime.now(UTC).year - 1
# COUNTY CARBON
# /counties
# /counties?year=2024
@router.get("/counties")
def county_carbon(
    year: Optional[int] = None,
    db: Session = Depends(get_db)
):
    year = resolve_year(year)

    result = db.execute(
        """
        SELECT *
        FROM carbon_stats
        WHERE entity_type = 'county'
        AND year = :year
        ORDER BY co2e_tonnes DESC
        """,
        {"year": year}
    )
    return [dict(row._mapping) for row in result]

@router.get("/counties/{county_id}")
def county_carbon_single(
    county_id: UUID,
    year: Optional[int] = None,
    db: Session = Depends(get_db)
):
    year = resolve_year(year)

    result = db.execute("""
    SELECT *
    FROM carbon_stats
    WHERE entity_type = 'county'
    AND entity_id = :id
    AND year = :year
    """, {"id": str(county_id), "year": year})

    row = result.fetchone()
    if not row:
        return {"error": "Data not available for this year"}

    return dict(row._mapping)

# COUNTY LOSS
# /loss/counties
# /loss/counties?year=2022
@router.get("/loss/counties")
def county_loss(
    year: Optional[int] = None,
    db: Session = Depends(get_db)
):
    year = resolve_year(year)
    result = db.execute(
        """
        SELECT *
        FROM loss_stats
        WHERE entity_type = 'county'
        AND year = :year
        ORDER BY co2e_emitted_tonnes DESC
        """,
        {"year": year}
    )
    return [dict(row._mapping) for row in result]

@router.get("/loss/counties/{county_id}")
def county_loss_single(
    county_id: UUID,
    year: Optional[int] = None,
    db: Session = Depends(get_db)
):
    year = resolve_year(year)

    result = db.execute("""
    SELECT *
    FROM loss_stats
    WHERE entity_type = 'county'
    AND entity_id = :id
    AND year = :year
    """, {"id": str(county_id), "year": year})

    row = result.fetchone()
    if not row:
        return {"error": "Data not available for this year"}

    return dict(row._mapping)

# NATIONAL LOSS TREND
@router.get("/loss/trend/national")
def national_loss_trend(db: Session = Depends(get_db)):
    return get_national_loss_trend(db)


# AVAILABLE YEARS
@router.get("/map/years")
def carbon_years():
    return get_available_carbon_years()

# NATIONAL CARBON MAP
# /map/national
# /map/national?year=2025
@router.get("/map/national")
def national_carbon_map(
    year: Optional[int] = None
):
    return get_national_carbon_map(year)

# RESERVE CARBON
# /reserves
# /reserves?year=2025
@router.get("/reserves")
def reserve_carbon(
    year: Optional[int] = None,
    db: Session = Depends(get_db)
):
    year = resolve_year(year)
    result = db.execute(
        """
        SELECT *
        FROM carbon_stats
        WHERE entity_type = 'reserve'
        AND year = :year
        ORDER BY co2e_tonnes DESC
        """,
        {"year": year}
    )
    return [dict(row._mapping) for row in result]

@router.get("/reserves/{reserve_id}")
def reserve_carbon_single(
    reserve_id: UUID,
    year: Optional[int] = None,
    db: Session = Depends(get_db)
):
    year = resolve_year(year)
    result = db.execute("""
    SELECT *
    FROM carbon_stats
    WHERE entity_type = 'reserve'
    AND entity_id = :id
    AND year = :year
    """, {"id": str(reserve_id), "year": year})

    row = result.fetchone()
    if not row:
        return {"error": "Data not available for this year"}

    return dict(row._mapping)

# RESERVE LOSS
# /loss/reserves
# /loss/reserves?year=2024
@router.get("/loss/reserves")
def reserve_loss(
    year: Optional[int] = None,
    db: Session = Depends(get_db)
):
    year = resolve_year(year)

    result = db.execute(
        """
        SELECT *
        FROM loss_stats
        WHERE entity_type = 'reserve'
        AND year = :year
        ORDER BY co2e_emitted_tonnes DESC
        """,
        {"year": year}
    )
    return [dict(row._mapping) for row in result]

@router.get("/loss/reserves/{reserve_id}")
def reserve_loss_single(
    reserve_id: UUID,
    year: Optional[int] = None,
    db: Session = Depends(get_db)
):
    year = resolve_year(year)
    result = db.execute("""
    SELECT *
    FROM loss_stats
    WHERE entity_type = 'reserve'
    AND entity_id = :id
    AND year = :year
    """, {"id": str(reserve_id), "year": year})

    row = result.fetchone()
    if not row:
        return {"error": "Data not available for this year"}

    return dict(row._mapping)

@router.get("/loss/reserves/{reserve_id}/trend")
def reserve_loss_trend(
    reserve_id: str,
    db: Session = Depends(get_db)
):
    return get_reserve_loss_trend(db, reserve_id)


@router.get("/loss/counties/{county_id}/trend")
def county_loss_trend(
    county_id: str,
    db: Session = Depends(get_db)
):
    return get_county_loss_trend(db, county_id)

@router.get("/wards")
def ward_carbon(
    year: Optional[int] = None,
    db: Session = Depends(get_db)
):
    year = resolve_year(year)
    result = db.execute(
        """
        SELECT *
        FROM carbon_stats
        WHERE entity_type = 'ward'
        AND year = :year
        ORDER BY co2e_tonnes DESC
        """,
        {"year": year}
    )
    return [dict(row._mapping) for row in result]

@router.get("/wards/{ward_id}")
def ward_carbon_single(
    ward_id: UUID,
    year: Optional[int] = None,
    db: Session = Depends(get_db)
):
    year = resolve_year(year)
    result = db.execute("""
    SELECT *
    FROM carbon_stats
    WHERE entity_type = 'ward'
    AND entity_id = :id
    AND year = :year
    """, {"id": str(ward_id), "year": year})

    row = result.fetchone()
    if not row:
        return {"error": "Data not available for this year"}

    return dict(row._mapping)

@router.get("/loss/wards")
def ward_loss(
    year: Optional[int] = None,
    db: Session = Depends(get_db)
):
    year = resolve_year(year)
    result = db.execute(
        """
        SELECT *
        FROM loss_stats
        WHERE entity_type = 'ward'
        AND year = :year
        ORDER BY co2e_emitted_tonnes DESC
        """,
        {"year": year}
    )
    return [dict(row._mapping) for row in result]

@router.get("/loss/wards/{ward_id}")
def ward_loss_single(
    ward_id: UUID,
    year: Optional[int] = None,
    db: Session = Depends(get_db)
):
    year = resolve_year(year)
    result = db.execute("""
    SELECT *
    FROM loss_stats
    WHERE entity_type = 'ward'
    AND entity_id = :id
    AND year = :year
    """, {"id": str(ward_id), "year": year})

    row = result.fetchone()
    if not row:
        return {"error": "Data not available for this year"}

    return dict(row._mapping)

@router.get("/loss/wards/{ward_id}/trend")
def ward_loss_trend(
    ward_id: str,
    db: Session = Depends(get_db)
):
    return get_ward_loss_trend(db, ward_id)