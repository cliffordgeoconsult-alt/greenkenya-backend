from sqlalchemy import text
from sqlalchemy.orm import Session

def get_radd_yearly(db: Session, geom_geojson):
    query = text("""
        SELECT 
            EXTRACT(YEAR FROM alert_date) as year,
            COUNT(*) as alerts
        FROM radd_alerts
        WHERE ST_Intersects(
            geometry,
            ST_SetSRID(ST_GeomFromGeoJSON(:geom), 4326)
        )
        GROUP BY year
        ORDER BY year
    """)

    rows = db.execute(query, {"geom": geom_geojson}).fetchall()

    return [
        {"year": int(r[0]), "alerts": int(r[1])}
        for r in rows
    ]


def get_radd_monthly_current_year(db: Session, geom_geojson):
    query = text("""
        SELECT 
            EXTRACT(MONTH FROM alert_date) as month,
            COUNT(*) as alerts
        FROM radd_alerts
        WHERE 
            EXTRACT(YEAR FROM alert_date) = EXTRACT(YEAR FROM CURRENT_DATE)
        AND ST_Intersects(
            geometry,
            ST_SetSRID(ST_GeomFromGeoJSON(:geom), 4326)
        )
        GROUP BY month
        ORDER BY month
    """)

    rows = db.execute(query, {"geom": geom_geojson}).fetchall()

    return [
        {"month": int(r[0]), "alerts": int(r[1])}
        for r in rows
    ]


def get_radd_daily(db: Session, geom_geojson, days=30):
    query = text("""
        SELECT 
            DATE(alert_date) as date,
            COUNT(*) as alerts
        FROM radd_alerts
        WHERE 
            alert_date >= NOW() - (:days || ' days')::interval
        AND ST_Intersects(
            geometry,
            ST_SetSRID(ST_GeomFromGeoJSON(:geom), 4326)
        )
        GROUP BY date
        ORDER BY date
    """)

    rows = db.execute(query, {"geom": geom_geojson, "days": days}).fetchall()

    return [
        {"date": str(r[0]), "alerts": int(r[1])}
        for r in rows
    ]