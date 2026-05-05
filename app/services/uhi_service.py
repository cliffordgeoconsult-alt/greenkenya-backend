# app/services/uhi_service.py
import json
from typing import Any, Optional

from sqlalchemy.orm import Session

from app.services.admin_service import get_uhi_counties, get_uhi_wards
from app.services.gee.ee_init import initialize_ee
from app.services.gee.uhi_analysis import (
    compute_uhi_zonal_metrics,
    DATA_SOURCES,
    METHODOLOGY_SUMMARY,
)


def _norm_geojson(geojson_string: str) -> str:
    return json.dumps(json.loads(geojson_string), sort_keys=True)


def _envelope(
    level: str,
    entity: dict,
    metrics: dict,
    extra: Optional[dict] = None,
) -> dict:
    if metrics.get("error"):
        return {
            "level": level,
            "entity_id": str(entity.get("id")),
            "name": entity.get("name"),
            "county_id": str(entity["county_id"]) if entity.get("county_id") else None,
            "error": metrics["error"],
            "data_sources": DATA_SOURCES,
            "methodology": METHODOLOGY_SUMMARY,
        }
    row: dict[str, Any] = {
        "level": level,
        "entity_id": str(entity["id"]),
        "name": entity["name"],
        "data_sources": DATA_SOURCES,
        "methodology": METHODOLOGY_SUMMARY,
    }
    if entity.get("county_id") is not None:
        row["county_id"] = str(entity["county_id"])
    row.update({k: v for k, v in metrics.items() if k != "error"})
    if extra:
        row.update(extra)
    return row


def list_uhi_counties(db: Session) -> list:
    return [
        {"id": str(c["id"]), "name": c["name"]}
        for c in get_uhi_counties(db)
    ]


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
    initialize_ee()
    counties = get_uhi_counties(db)
    county = next((c for c in counties if str(c["id"]) == str(county_id)), None)
    if not county:
        return {"error": "County not found or not in UHI pilot list"}
    g = _norm_geojson(county["geometry"])
    metrics = compute_uhi_zonal_metrics(g, year)
    return _envelope("county", county, metrics)


def ward_uhi_metrics(db: Session, ward_id: str, year: int) -> dict:
    initialize_ee()
    wards = get_uhi_wards(db)
    ward = next((w for w in wards if str(w["id"]) == str(ward_id)), None)
    if not ward:
        return {"error": "Ward not found or not in UHI pilot counties"}
    counties = get_uhi_counties(db)
    county = next(
        (c for c in counties if str(c["id"]) == str(ward["county_id"])),
        None,
    )
    ent = {
        "id": ward["id"],
        "name": ward["name"],
        "county_id": ward["county_id"],
    }
    g = _norm_geojson(ward["geometry"])
    metrics = compute_uhi_zonal_metrics(g, year)
    extra: dict = {}
    if county and not metrics.get("error"):
        cg = _norm_geojson(county["geometry"])
        cmetrics = compute_uhi_zonal_metrics(cg, year)
        if not cmetrics.get("error"):
            if metrics.get("lst_day_mean_c") is not None and cmetrics.get(
                "lst_day_mean_c"
            ) is not None:
                extra["lst_day_excess_vs_county_mean_c"] = round(
                    metrics["lst_day_mean_c"] - cmetrics["lst_day_mean_c"],
                    3,
                )
            if metrics.get("lst_night_mean_c") is not None and cmetrics.get(
                "lst_night_mean_c"
            ) is not None:
                extra["lst_night_excess_vs_county_mean_c"] = round(
                    metrics["lst_night_mean_c"] - cmetrics["lst_night_mean_c"],
                    3,
                )
    return _envelope("ward", ent, metrics, extra if extra else None)
