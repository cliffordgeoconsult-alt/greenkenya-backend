# app/services/admin_service.py
from sqlalchemy.orm import Session
from sqlalchemy import text

def get_counties(db: Session):

    query = """
    SELECT
        id,
        name,
        ST_AsGeoJSON(geometry) as geojson
    FROM admin_county
    """

    result = db.execute(text(query))

    counties = []

    for row in result:
        counties.append({
            "id": row.id,
            "name": row.name,
            "geometry": row.geojson
        })

    return counties

def get_wards(db: Session):

    query = """
    SELECT
        id,
        name,
        ST_AsGeoJSON(geometry) as geojson
    FROM admin_ward
    """

    result = db.execute(text(query))

    return [
        {"id": row.id, "name": row.name, "geometry": row.geojson}
        for row in result
    ]


def get_subcounties(db: Session):

    query = """
    SELECT
        id,
        name,
        ST_AsGeoJSON(geometry) as geojson
    FROM admin_subcounty
    """

    result = db.execute(text(query))

    return [
        {"id": row.id, "name": row.name, "geometry": row.geojson}
        for row in result
    ]

def get_wards_by_county(db: Session, county_id: str):

    query = """
    SELECT
        w.id,
        w.name,
        ST_AsGeoJSON(
            ST_Intersection(w.geometry, c.geometry)
        ) as geojson
    FROM admin_ward w
    JOIN admin_county c
    ON w.county_id = c.id
    WHERE c.id = :county_id
    """

    result = db.execute(text(query), {"county_id": county_id})

    return [
        {"id": row.id, "name": row.name, "geometry": row.geojson}
        for row in result
    ]

def get_subcounties_by_county(db: Session, county_id: str):
    query = """
    SELECT 
        s.id,
        s.name,
        ST_AsGeoJSON(
            ST_Intersection(s.geometry, c.geometry)
        ) as geojson
    FROM admin_subcounty s
    JOIN admin_county c
    ON s.county_id = c.id
    WHERE c.id = :county_id
    """

    result = db.execute(text(query), {"county_id": county_id})

    return [
        {"id": row.id, "name": row.name, "geometry": row.geojson}
        for row in result
    ]

def get_wards_by_subcounty(db: Session, subcounty_id: str):
    query = """
    SELECT id, name, ST_AsGeoJSON(geometry) as geojson
    FROM admin_ward
    WHERE subcounty_id = :subcounty_id
    """
    result = db.execute(text(query), {"subcounty_id": subcounty_id})

    return [
        {"id": row.id, "name": row.name, "geometry": row.geojson}
        for row in result
    ]