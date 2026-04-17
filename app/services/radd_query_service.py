# app/services/radd_query_service.py
from sqlalchemy.orm import Session
from sqlalchemy import text

def get_radd_alerts_count(db: Session, geometry_geojson):

    query = text("""
        SELECT COUNT(*) as alerts
        FROM radd_alerts
        WHERE ST_Intersects(
            geometry,
            ST_SetSRID(ST_GeomFromGeoJSON(:geom), 4326)
        )
    """)

    result = db.execute(query, {
        "geom": geometry_geojson
    }).fetchone()

    return result[0] if result else 0