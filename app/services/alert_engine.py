from sqlalchemy import text
from datetime import datetime
from app.services.radd_query_service import get_alert_persistence

def interpret_persistence(persistence):

    if persistence < 1.5:
        return {
            "level": "Early Stage",
            "confidence": "low",
            "explanation": "There are a few isolated detections of possible forest disturbance, but no strong evidence yet of sustained deforestation."
        }

    elif persistence < 3:
        return {
            "level": "Ongoing Activity",
            "confidence": "moderate",
            "explanation": "Repeated detections suggest ongoing forest disturbance, indicating likely active deforestation in the area."
        }

    else:
        return {
            "level": "Confirmed Deforestation",
            "confidence": "high",
            "explanation": "Strong repeated detections confirm sustained forest loss activity, indicating active deforestation."
        }
    
def get_alerts(
    db,
    level: str = None,
    entity_id: str = None
):

    alerts = []

    # =========================
    # 1. RESERVE THREATS
    # =========================
    if level in (None, "reserve"):
        reserve_alerts = db.execute(text("""
            SELECT 
                r.reserve_id,
                r.name,
                COUNT(a.id) as alerts,
                MIN(a.alert_date) as first_seen,
                MAX(a.alert_date) as last_seen,
                ST_AsGeoJSON(r.geometry)as geom,
                ST_X(ST_Centroid(ST_Collect(a.geometry))) as lon,
                ST_Y(ST_Centroid(ST_Collect(a.geometry))) as lat
            FROM forest_reserves r
            JOIN radd_alerts a
            ON ST_Intersects(r.geometry, a.geometry)
            WHERE a.alert_date >= NOW() - INTERVAL '7 days'
            AND (:entity_id IS NULL OR r.reserve_id = :entity_id)
            GROUP BY r.reserve_id, r.name, r.geometry
            HAVING COUNT(a.id) > 10
            ORDER BY alerts DESC
        """), {
            "entity_id": entity_id
        }).fetchall()

        for r in reserve_alerts:

            reserve_id = r[0]
            name = r[1]
            alerts_count = int(r[2])
            first_seen = r[3]
            last_seen = r[4]
            geom = r[5]
            lon = r[6]
            lat = r[7]

            persistence = get_alert_persistence(db, geom, days=7)

            interpretation = interpret_persistence(persistence)

            hotspots = db.execute(text("""
                SELECT 
                    ST_X(ST_Centroid(ST_Collect(a.geometry))) as lon,
                    ST_Y(ST_Centroid(ST_Collect(a.geometry))) as lat,
                    COUNT(*) as alerts
                FROM radd_alerts a
                JOIN forest_reserves r 
                ON ST_Intersects(r.geometry, a.geometry)
                WHERE r.reserve_id = :reserve_id
                AND a.alert_date >= NOW() - INTERVAL '7 days'
                GROUP BY ST_SnapToGrid(a.geometry, 0.05)
                ORDER BY alerts DESC
                LIMIT 10
            """), {"reserve_id": reserve_id}).fetchall()

            alerts.append({
                "type": "reserve_threat",
                "entity": name,
                "alerts": alerts_count,
                "persistence": round(persistence, 2),
                "confidence": interpretation["confidence"],
                "level": interpretation["level"],
                "coordinates": {
                    "lat": lat,
                    "lon": lon
                },
                "hotspots": [
                    {"lat": float(h[1]), "lon": float(h[0]), "alerts": h[2]}
                    for h in hotspots
                ],
                "message": f"{interpretation['level']} ({interpretation['confidence']} confidence): {interpretation['explanation']}",
                "time_range": {
                    "first_detected": first_seen.isoformat(),
                    "last_detected": last_seen.isoformat()
                }
            })

    # =========================
    # 2. COUNTY SPIKES
    # =========================
    if level in (None, "county"):
        spikes = db.execute(text("""
            WITH this_week AS (
                SELECT 
                    c.name,
                    COUNT(a.id) as alerts,
                    MIN(a.alert_date) as first_seen,
                    MAX(a.alert_date) as last_seen,
                    ST_AsGeoJSON(c.geometry) as geom,
                    ST_X(ST_Centroid(ST_Collect(a.geometry))) as lon,
                    ST_Y(ST_Centroid(ST_Collect(a.geometry))) as lat
                FROM admin_county c
                JOIN radd_alerts a
                ON ST_Intersects(c.geometry, a.geometry)
                WHERE a.alert_date >= NOW() - INTERVAL '7 days'
                AND (:entity_id IS NULL OR c.id = :entity_id)
                GROUP BY c.id, c.name, c.geometry
            ),
            last_week AS (
                SELECT 
                    c.name,
                    COUNT(a.id) as alerts
                FROM admin_county c
                JOIN radd_alerts a
                ON ST_Intersects(c.geometry, a.geometry)
                WHERE a.alert_date BETWEEN 
                    NOW() - INTERVAL '14 days' 
                    AND NOW() - INTERVAL '7 days'
                GROUP BY c.name
            )
        SELECT 
                t.name,
                t.alerts as current,
                COALESCE(l.alerts, 0) as previous,
                t.first_seen,
                t.last_seen,
                t.geom,
                t.lon,
                t.lat
            FROM this_week t
            LEFT JOIN last_week l ON t.name = l.name
            WHERE t.alerts > (COALESCE(l.alerts, 0) * 2)
            AND t.alerts > 20
        """), {
            "entity_id": entity_id
        }).fetchall()
        

        for s in spikes:

            name = s[0]
            current = int(s[1])
            previous = int(s[2])
            first_seen = s[3]
            last_seen = s[4]
            geom = s[5]
            lon = float(s[6])
            lat = float(s[7])

            persistence = get_alert_persistence(db, geom, days=7)

            growth = (current / previous) if previous > 0 else 999

            interpretation = interpret_persistence(persistence)

            alerts.append({
                "type": "county_spike",
                "entity": name,
                "current": current,
                "previous": previous,
                "growth": round(growth, 2),
                "persistence": round(persistence, 2),
                "confidence": interpretation["confidence"],
                "level": interpretation["level"],
                "coordinates": {
                    "lat": lat,
                    "lon": lon
                },
                "message": f"{interpretation['level']} ({interpretation['confidence']} confidence): {interpretation['explanation']}",
                "time_range": {
                    "first_detected": first_seen.isoformat(),
                    "last_detected": last_seen.isoformat()
                }
            })
        county_alerts = db.execute(text("""
            SELECT 
                c.id,
                c.name,
                COUNT(a.id) as alerts,
                MIN(a.alert_date) as first_seen,
                MAX(a.alert_date) as last_seen,
                ST_AsGeoJSON(c.geometry) as geom,
                ST_X(ST_Centroid(ST_Collect(a.geometry))) as lon,
                ST_Y(ST_Centroid(ST_Collect(a.geometry))) as lat
            FROM admin_county c
            JOIN radd_alerts a
            ON ST_Intersects(c.geometry, a.geometry)
            WHERE a.alert_date >= NOW() - INTERVAL '7 days'
            AND (:entity_id IS NULL OR c.id = :entity_id)
            GROUP BY c.id, c.name, c.geometry
            HAVING COUNT(a.id) > 5
        """), {
            "entity_id": entity_id
        }).fetchall()

        for c in county_alerts:
            county_id = c[0]
            name = c[1]
            alerts_count = int(c[2])
            first_seen = c[3]
            last_seen = c[4]
            geom = c[5]
            lon = float(c[6])
            lat = float(c[7])

            persistence = get_alert_persistence(db, geom, days=7)

            interpretation = interpret_persistence(persistence)

            hotspots = db.execute(text("""
                SELECT 
                    ST_X(ST_Centroid(ST_Collect(a.geometry))) as lon,
                    ST_Y(ST_Centroid(ST_Collect(a.geometry))) as lat,
                    COUNT(*) as alerts
                FROM radd_alerts a
                JOIN admin_county c 
                ON ST_Intersects(c.geometry, a.geometry)
                WHERE c.id = :county_id
                AND a.alert_date >= NOW() - INTERVAL '7 days'
                GROUP BY ST_SnapToGrid(a.geometry, 0.05)
                ORDER BY alerts DESC
                LIMIT 10
            """), {"county_id": county_id}).fetchall()

            # 🔽 Get wards inside this county
            wards = db.execute(text("""
                SELECT 
                    w.name,
                    COUNT(a.id) as alerts
                FROM admin_ward w
                JOIN radd_alerts a
                ON ST_Intersects(w.geometry, a.geometry)
                JOIN admin_subcounty sc ON w.subcounty_id = sc.id
                WHERE sc.county_id = :county_id
                AND a.alert_date >= NOW() - INTERVAL '7 days'
                GROUP BY w.id, w.name
                HAVING COUNT(a.id) > 2
                ORDER BY alerts DESC
            """), {"county_id": county_id}).fetchall()


            # 🔽 Get subcounties
            subcounties = db.execute(text("""
                SELECT 
                    sc.name,
                    COUNT(a.id) as alerts
                FROM admin_subcounty sc
                JOIN radd_alerts a
                ON ST_Intersects(sc.geometry, a.geometry)
                WHERE sc.county_id = :county_id
                AND a.alert_date >= NOW() - INTERVAL '7 days'
                GROUP BY sc.id, sc.name
                HAVING COUNT(a.id) > 5
                ORDER BY alerts DESC
            """), {"county_id": county_id}).fetchall()


            # 🔽 Get reserves
            reserves = db.execute(text("""
                SELECT 
                    r.name,
                    COUNT(a.id) as alerts
                FROM forest_reserves r
                JOIN radd_alerts a
                ON ST_Intersects(r.geometry, a.geometry)
                JOIN admin_county c ON ST_Intersects(r.geometry, c.geometry)
                WHERE c.id = :county_id
                AND a.alert_date >= NOW() - INTERVAL '7 days'
                GROUP BY r.reserve_id, r.name
                HAVING COUNT(a.id) > 2
            """), {"county_id": county_id}).fetchall()

            alerts.append({
                "type": "county_alert",  
                "entity": name,
                "breakdown": {
                    "wards": [
                        {"name": w[0], "alerts": int(w[1])}
                        for w in wards
                    ],
                    "subcounties": [
                        {"name": s[0], "alerts": int(s[1])}
                        for s in subcounties
                    ],
                    "reserves": [
                        {"name": r[0], "alerts": int(r[1])}
                        for r in reserves
                    ]
                },
                "alerts": alerts_count,
                "persistence": round(persistence, 2),
                "confidence": interpretation["confidence"],
                "level": interpretation["level"],
                "coordinates": {
                    "lat": lat,
                    "lon": lon
                },
                "hotspots": [
                    {
                        "lat": float(h[1]),
                        "lon": float(h[0]),
                        "alerts": int(h[2])
                    }
                    for h in hotspots
                ],
                "message": f"{interpretation['level']} ({interpretation['confidence']} confidence): {interpretation['explanation']}",
                "time_range": {
                    "first_detected": first_seen.isoformat(),
                    "last_detected": last_seen.isoformat()
                }
            })
        

    # 3. SUBCOUNTY ALERTS
    if level in (None, "subcounty"):
        sub_alerts = db.execute(text("""
            WITH base AS (
                SELECT 
                    sc.id,
                    sc.name,
                    a.id AS alert_id,
                    a.alert_date,
                    sc.geometry
                FROM admin_subcounty sc
                JOIN radd_alerts a
                ON ST_Intersects(sc.geometry, a.geometry)
                WHERE a.alert_date >= NOW() - INTERVAL '7 days'
                AND (:entity_id IS NULL OR sc.id = :entity_id)
            ),

            persistence_calc AS (
                SELECT 
                    b1.id,
                    b1.name,
                    b1.alert_date,
                    COUNT(b2.alert_id)::float AS detections
                FROM base b1
                JOIN base b2
                ON ST_DWithin(b1.geometry, b2.geometry, 0.0001)
                AND ABS(EXTRACT(EPOCH FROM (b1.alert_date - b2.alert_date))) <= 604800
                GROUP BY b1.id, b1.name, b1.alert_date
            ),

            aggregated AS (
                SELECT
                    id,
                    name,
                    COUNT(*) as alerts,
                    AVG(detections) as persistence,
                    MIN(alert_date) as first_seen,
                    MAX(alert_date) as last_seen
                FROM persistence_calc
                GROUP BY id, name
            )

            SELECT 
                a.id,
                a.name,
                a.alerts,
                a.persistence,
                a.first_seen,
                a.last_seen,
                ST_AsGeoJSON(sc.geometry) as geom,
                ST_X(ST_Centroid(ST_Collect(a.geometry))) as lon,
                ST_Y(ST_Centroid(ST_Collect(a.geometry))) as lat
            FROM aggregated a
            JOIN admin_subcounty sc ON sc.id = a.id
            WHERE a.alerts > 10
            """), {
                "entity_id": entity_id
            }).fetchall()

        for s in sub_alerts:

            subcounty_id = s[0]
            name = s[1]
            alerts_count = int(s[2])
            persistence = float(s[3])
            first_seen = s[4]
            last_seen = s[5]
            geom = s[6]
            lon = float(s[7])
            lat = float(s[8])

            interpretation = interpret_persistence(persistence)

            hotspots = db.execute(text("""
                SELECT 
                    ST_X(ST_Centroid(ST_Collect(a.geometry))) as lon,
                    ST_Y(ST_Centroid(ST_Collect(a.geometry))) as lat,
                    COUNT(*) as alerts
                FROM radd_alerts a
                JOIN admin_subcounty sc 
                ON ST_Intersects(sc.geometry, a.geometry)
                WHERE sc.id = :subcounty_id
                AND a.alert_date >= NOW() - INTERVAL '7 days'
                GROUP BY ST_SnapToGrid(a.geometry, 0.05)
                ORDER BY alerts DESC
                LIMIT 10
            """), {"subcounty_id": subcounty_id}).fetchall()

            wards = db.execute(text("""
                SELECT 
                    w.name,
                    COUNT(a.id) as alerts
                FROM admin_ward w
                JOIN radd_alerts a
                ON ST_Intersects(w.geometry, a.geometry)
                WHERE w.subcounty_id = :subcounty_id
                AND a.alert_date >= NOW() - INTERVAL '7 days'
                GROUP BY w.id, w.name
                HAVING COUNT(a.id) > 2
            """), {"subcounty_id": subcounty_id}).fetchall()

            alerts.append({
                "type": "subcounty_alert",
                "entity": name,
                "breakdown": {
                    "wards": [
                        {"name": w[0], "alerts": int(w[1])}
                        for w in wards
                    ]
                },
                "alerts": alerts_count,
                "persistence": round(persistence, 2),
                "confidence": interpretation["confidence"],
                "level": interpretation["level"],
                "coordinates": {
                    "lat": lat,
                    "lon": lon
                },
                "hotspots": [
                    {"lat": float(h[1]), "lon": float(h[0]), "alerts": int(h[2])}
                    for h in hotspots
                ],
                "message": f"{interpretation['level']} ({interpretation['confidence']} confidence): {interpretation['explanation']}",
                "time_range": {
                    "first_detected": first_seen.isoformat(),
                    "last_detected": last_seen.isoformat()
                }
            })

    # 4. WARD ALERTS
    if level in (None, "ward"):
        ward_alerts = db.execute(text("""
            SELECT 
                w.id,
                w.name,
                COUNT(a.id) as alerts,
                MIN(a.alert_date) as first_seen,
                MAX(a.alert_date) as last_seen,
                ST_AsGeoJSON(w.geometry) as geom,
                ST_X(ST_Centroid(ST_Collect(a.geometry))) as lon,
                ST_Y(ST_Centroid(ST_Collect(a.geometry))) as lat
            FROM admin_ward w
            JOIN radd_alerts a
            ON ST_Intersects(w.geometry, a.geometry)
            WHERE a.alert_date >= NOW() - INTERVAL '7 days'
            AND (:entity_id IS NULL OR w.id = :entity_id)
            GROUP BY w.id, w.name, w.geometry
            HAVING COUNT(a.id) > 5
        """), {
            "entity_id": entity_id
        }).fetchall()

        for w in ward_alerts:

            ward_id = w[0]
            name = w[1]
            alerts_count = int(w[2])
            first_seen = w[3]
            last_seen = w[4]
            geom = w[5]
            lon = float(w[6])
            lat = float(w[7])

            persistence = get_alert_persistence(db, geom, days=7)

            interpretation = interpret_persistence(persistence)

            hotspots = db.execute(text("""
                SELECT
                    ST_X(ST_Centroid(ST_Collect(a.geometry))) as lon,
                    ST_Y(ST_Centroid(ST_Collect(a.geometry))) as lat,
                    COUNT(*) as alerts
                FROM radd_alerts a
                JOIN admin_ward w 
                ON ST_Intersects(w.geometry, a.geometry)
                WHERE w.id = :ward_id
                AND a.alert_date >= NOW() - INTERVAL '7 days'
                GROUP BY ST_SnapToGrid(a.geometry, 0.05)
                ORDER BY alerts DESC
                LIMIT 10
            """), {"ward_id": ward_id}).fetchall()

            alerts.append({
                "type": "ward_alert",
                "entity": name,
                "alerts": alerts_count,
                "persistence": round(persistence, 2),
                "confidence": interpretation["confidence"],
                "level": interpretation["level"],
                "coordinates": {
                    "lat": lat,
                    "lon": lon
                },
                "hotspots": [
                    {"lat": float(h[1]), "lon": float(h[0]), "alerts": int(h[2])}
                    for h in hotspots
                ],
                "message": f"{interpretation['level']} ({interpretation['confidence']} confidence): {interpretation['explanation']}",
                "time_range": {
                    "first_detected": first_seen.isoformat(),
                    "last_detected": last_seen.isoformat()
                }
            })

    return {
        "alerts": alerts,
        "count": len(alerts),
        "generated_at": datetime.utcnow().isoformat()
    }