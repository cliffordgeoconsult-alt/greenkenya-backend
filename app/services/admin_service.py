from sqlalchemy.orm import Session
from sqlalchemy import text


def get_counties(db: Session):

    query = """
    SELECT
        id,
        name,
        ST_AsGeoJSON(geometry) as geojson
    FROM counties
    """

    result = db.execute(text(query))

    import json

    features = []

    for row in result:
        features.append({
            "type": "Feature",
            "properties": {
                "id": row.id,
                "name": row.name
            },
            "geometry": json.loads(row.geojson)
        })

    return {
        "type": "FeatureCollection",
        "features": features
    }

def get_wards(db: Session):

    query = """
    SELECT
        id,
        name,
        ST_AsGeoJSON(geometry) as geojson
    FROM wards
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
    FROM subcounties
    """

    result = db.execute(text(query))

    return [
        {"id": row.id, "name": row.name, "geometry": row.geojson}
        for row in result
    ]