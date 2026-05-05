# app/services/uhi_service.py
import json
from typing import Optional

from sqlalchemy.orm import Session

from app.services.admin_service import get_uhi_counties, get_uhi_wards
from app.services.uhi_report_service import (
    county_uhi_year_snapshot,
    ward_uhi_year_snapshot,
)


def _norm_geojson(geojson_string: str) -> str:
    return json.dumps(json.loads(geojson_string), sort_keys=True)


def list_uhi_counties(db: Session) -> list:
    return [
        {"id": str(c["id"]), "name": c["name"]}
        for c in get_uhi_counties(db)
    ]


def get_uhi_geometry_normalized(db: Session, level: str, entity_id: str) -> Optional[str]:
    """Return sort-keys-normalized GeoJSON string for county or ward in pilot list."""
    level = (level or "").lower().strip()
    if level == "county":
        counties = get_uhi_counties(db)
        c = next((x for x in counties if str(x["id"]) == str(entity_id)), None)
        if not c:
            return None
        return _norm_geojson(c["geometry"])
    if level == "ward":
        wards = get_uhi_wards(db)
        w = next((x for x in wards if str(x["id"]) == str(entity_id)), None)
        if not w:
            return None
        return _norm_geojson(w["geometry"])
    return None


def list_uhi_wards(db: Session, county_id: Optional[str] = None) -> list:
    wards = get_uhi_wards(db)
    if county_id:
        wards = [w for w in wards if str(w["county_id"]) == str(county_id)]
    return [
        {
            "id": str(w["id"]),
            "name": w["name"],
            "county_id": str(w["county_id"]),
        }
        for w in wards
    ]


def county_uhi_metrics(db: Session, county_id: str, year: int) -> dict:
    """One year of nested UHI intelligence (same shape as /report core fields)."""
    return county_uhi_year_snapshot(db, county_id, year)


def ward_uhi_metrics(db: Session, ward_id: str, year: int) -> dict:
    """One year of nested UHI intelligence; includes ward-vs-county LST delta when computable."""
    return ward_uhi_year_snapshot(db, ward_id, year)
