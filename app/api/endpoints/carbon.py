# app/api/endpoints/carbon.py
from typing import Optional
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime, UTC
from app.db.session import get_db
from uuid import UUID

from app.services.carbon_service import (
    get_county_loss_trend,
    get_national_loss_trend,
    get_national_carbon_map,
    get_available_carbon_years,
    get_ward_loss_trend,
    get_reserve_loss_trend
)

router = APIRouter()

def resolve_year(year: Optional[int]) -> int:
    return year if year is not None else datetime.now(UTC).year - 1

# =========================
# COUNTY CARBON
# =========================
@router.get("/counties")
def county_carbon(year: Optional[int] = None, db: Session = Depends(get_db)):
    year = resolve_year(year)

    try:
        result = db.execute(text("""
            SELECT *
            FROM carbon_stats
            WHERE entity_type = 'county'
            AND year = :year
            ORDER BY co2e_tonnes DESC
        """), {"year": year})
    except Exception as e:
        print("🔥 ERROR county_carbon:", str(e))
        raise

    return [dict(row._mapping) for row in result]


@router.get("/counties/{county_id}")
def county_carbon_single(county_id: UUID, year: Optional[int] = None, db: Session = Depends(get_db)):
    year = resolve_year(year)

    try:
        result = db.execute(text("""
            SELECT *
            FROM carbon_stats
            WHERE entity_type = 'county'
            AND entity_id = :id
            AND year = :year
        """), {"id": str(county_id), "year": year})
    except Exception as e:
        print("🔥 ERROR county_carbon_single:", str(e))
        raise

    row = result.fetchone()
    return dict(row._mapping) if row else {"error": "Data not available"}


# =========================
# COUNTY LOSS
# =========================
@router.get("/loss/counties")
def county_loss(year: Optional[int] = None, db: Session = Depends(get_db)):
    year = resolve_year(year)

    try:
        result = db.execute(text("""
            SELECT *
            FROM loss_stats
            WHERE entity_type = 'county'
            AND year = :year
            ORDER BY co2e_emitted_tonnes DESC
        """), {"year": year})
    except Exception as e:
        print("🔥 ERROR county_loss:", str(e))
        raise

    return [dict(row._mapping) for row in result]


@router.get("/loss/counties/{county_id}")
def county_loss_single(county_id: UUID, year: Optional[int] = None, db: Session = Depends(get_db)):
    year = resolve_year(year)

    try:
        result = db.execute(text("""
            SELECT *
            FROM loss_stats
            WHERE entity_type = 'county'
            AND entity_id = :id
            AND year = :year
        """), {"id": str(county_id), "year": year})
    except Exception as e:
        print("🔥 ERROR county_loss_single:", str(e))
        raise

    row = result.fetchone()
    return dict(row._mapping) if row else {"error": "Data not available"}


# =========================
# RESERVES
# =========================
@router.get("/reserves")
def reserve_carbon(year: Optional[int] = None, db: Session = Depends(get_db)):
    year = resolve_year(year)

    try:
        result = db.execute(text("""
            SELECT *
            FROM carbon_stats
            WHERE entity_type = 'reserve'
            AND year = :year
            ORDER BY co2e_tonnes DESC
        """), {"year": year})
    except Exception as e:
        print("🔥 ERROR reserve_carbon:", str(e))
        raise

    return [dict(row._mapping) for row in result]


@router.get("/reserves/{reserve_id}")
def reserve_carbon_single(reserve_id: UUID, year: Optional[int] = None, db: Session = Depends(get_db)):
    year = resolve_year(year)

    try:
        result = db.execute(text("""
            SELECT *
            FROM carbon_stats
            WHERE entity_type = 'reserve'
            AND entity_id = :id
            AND year = :year
        """), {"id": str(reserve_id), "year": year})
    except Exception as e:
        print("🔥 ERROR reserve_carbon_single:", str(e))
        raise

    row = result.fetchone()
    return dict(row._mapping) if row else {"error": "Data not available"}


@router.get("/loss/reserves")
def reserve_loss(year: Optional[int] = None, db: Session = Depends(get_db)):
    year = resolve_year(year)

    try:
        result = db.execute(text("""
            SELECT *
            FROM loss_stats
            WHERE entity_type = 'reserve'
            AND year = :year
            ORDER BY co2e_emitted_tonnes DESC
        """), {"year": year})
    except Exception as e:
        print("🔥 ERROR reserve_loss:", str(e))
        raise

    return [dict(row._mapping) for row in result]


@router.get("/loss/reserves/{reserve_id}")
def reserve_loss_single(reserve_id: UUID, year: Optional[int] = None, db: Session = Depends(get_db)):
    year = resolve_year(year)

    try:
        result = db.execute(text("""
            SELECT *
            FROM loss_stats
            WHERE entity_type = 'reserve'
            AND entity_id = :id
            AND year = :year
        """), {"id": str(reserve_id), "year": year})
    except Exception as e:
        print("🔥 ERROR reserve_loss_single:", str(e))
        raise

    row = result.fetchone()
    return dict(row._mapping) if row else {"error": "Data not available"}


# =========================
# WARDS
# =========================
@router.get("/wards")
def ward_carbon(year: Optional[int] = None, db: Session = Depends(get_db)):
    year = resolve_year(year)

    try:
        result = db.execute(text("""
            SELECT *
            FROM carbon_stats
            WHERE entity_type = 'ward'
            AND year = :year
            ORDER BY co2e_tonnes DESC
        """), {"year": year})
    except Exception as e:
        print("🔥 ERROR ward_carbon:", str(e))
        raise

    return [dict(row._mapping) for row in result]


@router.get("/wards/{ward_id}")
def ward_carbon_single(ward_id: UUID, year: Optional[int] = None, db: Session = Depends(get_db)):
    year = resolve_year(year)

    try:
        result = db.execute(text("""
            SELECT *
            FROM carbon_stats
            WHERE entity_type = 'ward'
            AND entity_id = :id
            AND year = :year
        """), {"id": str(ward_id), "year": year})
    except Exception as e:
        print("🔥 ERROR ward_carbon_single:", str(e))
        raise

    row = result.fetchone()
    return dict(row._mapping) if row else {"error": "Data not available"}


@router.get("/loss/wards")
def ward_loss(year: Optional[int] = None, db: Session = Depends(get_db)):
    year = resolve_year(year)

    try:
        result = db.execute(text("""
            SELECT *
            FROM loss_stats
            WHERE entity_type = 'ward'
            AND year = :year
            ORDER BY co2e_emitted_tonnes DESC
        """), {"year": year})
    except Exception as e:
        print("🔥 ERROR ward_loss:", str(e))
        raise

    return [dict(row._mapping) for row in result]


@router.get("/loss/wards/{ward_id}")
def ward_loss_single(ward_id: UUID, year: Optional[int] = None, db: Session = Depends(get_db)):
    year = resolve_year(year)

    try:
        result = db.execute(text("""
            SELECT *
            FROM loss_stats
            WHERE entity_type = 'ward'
            AND entity_id = :id
            AND year = :year
        """), {"id": str(ward_id), "year": year})
    except Exception as e:
        print("🔥 ERROR ward_loss_single:", str(e))
        raise

    row = result.fetchone()
    return dict(row._mapping) if row else {"error": "Data not available"}


# =========================
# TRENDS (UNCHANGED)
# =========================
@router.get("/loss/trend/national")
def national_loss_trend(db: Session = Depends(get_db)):
    return get_national_loss_trend(db)

@router.get("/loss/counties/{county_id}/trend")
def county_loss_trend(county_id: str, db: Session = Depends(get_db)):
    return get_county_loss_trend(db, county_id)

@router.get("/loss/reserves/{reserve_id}/trend")
def reserve_loss_trend(reserve_id: str, db: Session = Depends(get_db)):
    return get_reserve_loss_trend(db, reserve_id)

@router.get("/loss/wards/{ward_id}/trend")
def ward_loss_trend(ward_id: str, db: Session = Depends(get_db)):
    return get_ward_loss_trend(db, ward_id)

# =========================
# MAP / YEARS
# =========================
@router.get("/map/years")
def carbon_years():
    return get_available_carbon_years()

@router.get("/map/national")
def national_carbon_map(year: Optional[int] = None):
    return get_national_carbon_map(year)