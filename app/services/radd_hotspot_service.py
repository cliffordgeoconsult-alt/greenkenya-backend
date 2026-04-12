# app/services/radd_hotspot_service.py
from sqlalchemy import text

def generate_radd_hotspots(db):

    hotspots = db.execute(text("""
        WITH clustered AS (
            SELECT
                ST_ClusterDBSCAN(geometry, eps := 0.05, minpoints := 3)
                geometry,
                loss_ha,
                alert_date
            FROM radd_alerts
            WHERE alert_date >= NOW() - INTERVAL '90 days'
        )

        SELECT
            cluster_id,
            COUNT(*) AS alerts_count,
            SUM(loss_ha) AS total_loss,
            ST_AsGeoJSON(ST_ConvexHull(ST_Collect(geometry))) AS geometry
        FROM clustered
        WHERE cluster_id IS NOT NULL
        GROUP BY cluster_id
    """)).fetchall()

    results = []

    for h in hotspots:

        cluster_id = h[0]
        alerts_count = h[1]
        total_loss = h[2]
        geometry = h[3]

        # severity logic
        if alerts_count >= 30:
            severity = "high"
        elif alerts_count >= 10:
            severity = "medium"
        else:
            severity = "low"

        results.append({
            "cluster_id": int(cluster_id),
            "alerts_count": int(alerts_count),
            "total_loss_ha": round(total_loss or 0, 2),
            "severity": severity,
            "geometry": geometry
        })

    return results