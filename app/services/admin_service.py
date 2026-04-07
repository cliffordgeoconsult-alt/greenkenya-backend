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