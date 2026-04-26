# app/api/endpoints/carbon.py
from typing import Optional

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.services.carbon_service import (
    get_county_carbon_stats,
    get_county_loss_stats,
    get_county_loss_trend,
    get_national_loss_trend,
    get_national_carbon_map,
    get_available_carbon_years,
    get_ward_carbon_stats,
    get_ward_loss_stats,
    get_ward_loss_trend,
    get_reserve_carbon_stats,
    get_reserve_loss_stats,
    get_reserve_loss_trend
)

router = APIRouter()

# COUNTY CARBON
# /counties
# /counties?year=2024
@router.get("/counties")
def county_carbon(
    year: Optional[int] = None,
    db: Session = Depends(get_db)
):
    return get_county_carbon_stats(db, year)


# COUNTY LOSS
# /loss/counties
# /loss/counties?year=2022
@router.get("/loss/counties")
def county_loss(
    year: Optional[int] = None,
    db: Session = Depends(get_db)
):
    return get_county_loss_stats(db, year)


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
    return get_reserve_carbon_stats(db, year)

# RESERVE LOSS
# /loss/reserves
# /loss/reserves?year=2024
@router.get("/loss/reserves")
def reserve_loss(
    year: Optional[int] = None,
    db: Session = Depends(get_db)
):
    return get_reserve_loss_stats(db, year)

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
    return get_ward_carbon_stats(db, year)


@router.get("/loss/wards")
def ward_loss(
    year: Optional[int] = None,
    db: Session = Depends(get_db)
):
    return get_ward_loss_stats(db, year)


@router.get("/loss/wards/{ward_id}/trend")
def ward_loss_trend(
    ward_id: str,
    db: Session = Depends(get_db)
):
    return get_ward_loss_trend(db, ward_id)