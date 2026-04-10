import ee
import os
import json
from sqlalchemy import text
from sqlalchemy.orm import Session
import uuid
from datetime import datetime

# =========================
# EE INIT (SERVICE ACCOUNT)
# =========================
SERVICE_ACCOUNT = "greenmap-kenya@greenmap-kenya-483110.iam.gserviceaccount.com"

def initialize_ee():
    try:
        service_account_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")

        if service_account_json:
            service_account_info = json.loads(service_account_json)

            credentials = ee.ServiceAccountCredentials(
                service_account_info["client_email"],
                key_data=json.dumps(service_account_info)
            )
        else:
            credentials = ee.ServiceAccountCredentials(
                SERVICE_ACCOUNT,
                "service-account.json"
            )

        ee.Initialize(credentials)
        print("✅ EE initialized")

    except Exception as e:
        print("❌ EE init failed:", str(e))


# =========================
# FETCH RADD ALERTS
# =========================
def fetch_radd_alerts_gee(geojson):
    initialize_ee()

    geom = ee.Geometry(json.loads(geojson))

    collection = ee.ImageCollection("projects/glad/alert/UpdResult") \
        .filterBounds(geom) \
        .filterDate("2024-01-01", "2026-12-31")

    image = collection.mosaic()

    # ✅ INCLUDE ALL IMPORTANT BANDS
    alerts = image.select([
        "conf25", "conf26",
        "alertDate25", "alertDate26"
    ])

    # 🔥 only keep alert pixels
    mask = image.select("conf26").gt(0).Or(image.select("conf25").gt(0))

    vectors = alerts.updateMask(mask).reduceToVectors(
        geometry=geom,
        scale=30,
        geometryType="centroid",
        reducer=ee.Reducer.first(),   # 🔥 keeps properties
        maxPixels=1e13
    )

    features = vectors.getInfo()["features"]

    print("🔥 GEE ALERTS:", len(features))

    return features

# =========================
# INGEST INTO DB
# =========================
def ingest_radd_alerts_gfw(db: Session):

    # 🔥 USE YOUR REAL TABLE NAME
    result = db.execute(text("""
        SELECT ST_AsGeoJSON(ST_Union(geometry)) as geojson
        FROM admin_county
    """)).fetchone()

    if not result or not result.geojson:
        return {"message": "No Kenya geometry", "inserted": 0}

    alerts = fetch_radd_alerts_gee(result.geojson)

    if not alerts:
        return {"message": "No alerts fetched", "inserted": 0}

    inserted = 0

    for f in alerts:
        try:
            coords = f["geometry"]["coordinates"]
            lon, lat = coords

            props = f.get("properties", {})

            # ✅ CONFIDENCE
            conf = props.get("conf26") or props.get("conf25") or 0.5

            if conf == 0:
                continue

            # ✅ REAL DATE EXTRACTION
            alert_date_val = props.get("alertDate26") or props.get("alertDate25")

            if alert_date_val:
                alert_date = datetime.utcfromtimestamp(alert_date_val / 1000)
            else:
                alert_date = datetime.utcnow()

            db.execute(text("""
                INSERT INTO radd_alerts (
                    id, alert_date, loss_ha, confidence, geometry
                )
                VALUES (
                    :id,
                    :date,
                    :loss,
                    :confidence,
                    ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)
                )
            """), {
                "id": str(uuid.uuid4()),
                "date": alert_date,
                "loss": 0.09,
                "confidence": conf / 100 if conf > 1 else conf,  # normalize
                "lon": lon,
                "lat": lat
            })

            inserted += 1

        except Exception as e:
            print("⚠️ SKIP:", e)
    # ✅ COMMIT OUTSIDE LOOP
    db.commit()

    return {
        "message": "RADD ingestion complete",
        "inserted": inserted
    }