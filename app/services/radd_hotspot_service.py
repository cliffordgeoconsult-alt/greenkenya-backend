# app/services/radd_hotspot_service.py
# This module generates hotspots based on RADD alerts using PostGIS clustering. 
# It defines a function `generate_radd_hotspots` that performs the following steps:
# 1. Filters RADD alerts from the last N days (default 90).
# 2. Clusters the alerts using DBSCAN based on their geographic proximity.
# 3. For each cluster, it calculates the total number of alerts, total loss, and the time range of the alerts.
# 4. It also enriches the cluster information by checking for intersections with forests, reserves, and administrative boundaries (county, subcounty, ward).
# The resulting hotspots are returned as a list of dictionaries, each containing the cluster information and its geographic geometry in GeoJSON format. 
from sqlalchemy import text
import json

def generate_radd_hotspots(db, days=3650):

    hotspots = db.execute(text("""
        WITH filtered AS (
            SELECT *
            FROM radd_alerts
            WHERE alert_date >= NOW() - INTERVAL '30 days'
        ),

        clustered AS (
            SELECT
                ST_ClusterDBSCAN(
                    geometry,
                    eps := 0.02,
                    minpoints := 5
                ) OVER () AS cluster_id,
                geometry,
                loss_ha,
                alert_date
            FROM filtered
        ),

        clusters AS (
            SELECT
                cluster_id,
                COUNT(*) AS alerts_count,
                MIN(alert_date) AS start_date,
                MAX(alert_date) AS end_date,
                ST_Envelope(ST_Collect(geometry)) AS geom
            FROM clustered
            WHERE cluster_id IS NOT NULL
            GROUP BY cluster_id
        )

        SELECT
            c.cluster_id,
            c.alerts_count,
            c.start_date,
            c.end_date,
            ST_AsGeoJSON(c.geom) AS geometry,

            -- FOREST
            f.forest_code,

            -- RESERVE
            r.name AS reserve_name,

            -- COUNTY
            ac.name AS county,

            -- SUBCOUNTY
            sc.name AS subcounty,

            -- WARD
            w.name AS ward

        FROM clusters c

        LEFT JOIN forests f
        ON ST_Intersects(f.geometry, c.geom)

        LEFT JOIN forest_reserves r
        ON ST_Intersects(r.geometry, c.geom)

        LEFT JOIN admin_county ac
        ON ST_Intersects(ac.geometry, c.geom)

        LEFT JOIN admin_subcounty sc
        ON ST_Intersects(sc.geometry, c.geom)

        LEFT JOIN admin_ward w
        ON ST_Intersects(w.geometry, c.geom)
    """), {"days": days}).fetchall()

    results = []

    for h in hotspots:

        alerts_count = h[1]

        # severity
        if alerts_count >= 30:
            severity = "high"
        elif alerts_count >= 10:
            severity = "medium"
        else:
            severity = "low"

        results.append({
            "cluster_id": int(h[0]),
            "alerts_count": int(h[1]),
            "severity": severity,
            "start_date": str(h[3]),
            "end_date": str(h[4]),
            "forest_code": h[6],
            "reserve_name": h[7],
            "county": h[8],
            "subcounty": h[9],
            "ward": h[10],

            "geometry": json.loads(h[5])
        })

    return results