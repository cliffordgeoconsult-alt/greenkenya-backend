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

def get_alert_persistence(db: Session, geometry_geojson, days=7):

    query = text("""
        WITH nearby AS (
            SELECT a1.id, COUNT(a2.id) as detections
            FROM radd_alerts a1
            JOIN radd_alerts a2
            ON ST_DWithin(a1.geometry, a2.geometry, 0.0001)
            AND ABS(EXTRACT(EPOCH FROM (a1.alert_date - a2.alert_date))) <= (:days * 86400)
            WHERE ST_Intersects(
                a1.geometry,
                ST_SetSRID(ST_GeomFromGeoJSON(:geom), 4326)
            )
            GROUP BY a1.id
        )
        SELECT AVG(detections) FROM nearby
    """)

    result = db.execute(query, {
        "geom": geometry_geojson,
        "days": days
    }).fetchone()

    return result[0] if result else 0