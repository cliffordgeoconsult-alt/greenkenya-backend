# app/services/radd_query_service.py
from sqlalchemy.orm import Session
from sqlalchemy import text


def get_radd_loss_for_geometry(db: Session, geometry_geojson):
    """
    Returns total RADD loss (ha) inside a geometry
    """

    query = text("""
        SELECT COALESCE(SUM(loss_ha), 0) as total_loss
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