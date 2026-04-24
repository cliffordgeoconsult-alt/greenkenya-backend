from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.services.carbon_service import (
    get_county_carbon_stats,
    get_county_loss_stats,
    get_national_loss_trend,
    get_national_carbon_map,
    get_available_carbon_years
)

router = APIRouter()


@router.get("/counties")
def county_carbon(db: Session = Depends(get_db)):
    return get_county_carbon_stats(db)

@router.get("/loss/counties/{year}")
def county_loss(year: int, db: Session = Depends(get_db)):
    return get_county_loss_stats(db, year)

@router.get("/loss/trend/national")
def national_loss_trend(db: Session = Depends(get_db)):
    return get_national_loss_trend(db)

@router.get("/map/years")
def carbon_years():
    return get_available_carbon_years()


@router.get("/map/national/{year}")
def national_carbon_map(year: int):
    return get_national_carbon_map(year)