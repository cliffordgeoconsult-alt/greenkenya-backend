# app/services/radd_gfw_service.py
import ee
import os
import json
from sqlalchemy import text
from sqlalchemy.orm import Session
import uuid
from datetime import datetime

SERVICE_ACCOUNT = "greenmap-kenya@greenmap-kenya-483110.iam.gserviceaccount.com"


# EE INIT (ONCE ONLY)
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
        print(" EE initialized")

    except Exception as e:
        print(" EE init failed:", str(e))


# FETCH RADD ALERTS
def fetch_radd_alerts_gee(geojson):

    geom = ee.Geometry(json.loads(geojson))

    collection = ee.ImageCollection("projects/glad/alert/UpdResult") \
        .filterBounds(geom) \
        .filterDate("2024-01-01", "2026-12-31")

    size = collection.size().getInfo()
    print(" COLLECTION SIZE:", size)

    if size == 0:
        return []

    image = collection.mosaic()

    band_names = image.bandNames().getInfo()

    bands_to_use = []

    if "conf26" in band_names:
        bands_to_use += ["conf26", "alertDate26"]

    if "conf25" in band_names:
        bands_to_use += ["conf25", "alertDate25"]

    if not bands_to_use:
        return []

    alerts = image.select(bands_to_use)

    mask = ee.Image.constant(0)

    if "conf26" in band_names:
        mask = mask.Or(image.select("conf26").gt(0))

    if "conf25" in band_names:
        mask = mask.Or(image.select("conf25").gt(0))

    vectors = alerts.updateMask(mask).reduceToVectors(
        geometry=geom,
        scale=30,
        geometryType="centroid",
        reducer=ee.Reducer.first(),
        maxPixels=1e13
    )

    features = vectors.limit(200).getInfo()["features"]

    print(" GEE ALERTS:", len(features))

    return features


# INGEST INTO DB
def ingest_radd_alerts_gfw(db: Session):

    initialize_ee()  # ONLY ONCE

    counties = db.execute(text("""
        SELECT name, ST_AsGeoJSON(geometry)
        FROM admin_county
    """)).fetchall()

    # LOAD EXISTING POINTS ONCE (FAST)
    existing_points = db.execute(text("""
        SELECT ST_X(geometry), ST_Y(geometry)
        FROM radd_alerts
    """)).fetchall()

    existing_set = set((round(x, 5), round(y, 5)) for x, y in existing_points)

    total_inserted = 0

    for county in counties[:5]:

        county_name = county[0]
        geojson = county[1]

        print(f"\n Processing county: {county_name}")

        alerts = fetch_radd_alerts_gee(geojson)

        if not alerts:
            continue

        batch = []

        for f in alerts:
            try:
                lon, lat = f["geometry"]["coordinates"]

                point_key = (round(lon, 5), round(lat, 5))
                if point_key in existing_set:
                    continue

                props = f.get("properties", {})

                conf = props.get("conf26") or props.get("conf25") or 0.5
                if conf == 0:
                    continue

                alert_date_val = props.get("alertDate26") or props.get("alertDate25")

                if alert_date_val:
                    alert_date = datetime.utcfromtimestamp(alert_date_val / 1000)
                else:
                    alert_date = datetime.utcnow()

                batch.append({
                    "id": str(uuid.uuid4()),
                    "date": alert_date,
                    "loss": 0.09,
                    "confidence": conf / 100 if conf > 1 else conf,
                    "lon": lon,
                    "lat": lat
                })

                existing_set.add(point_key)  # prevent duplicates in same run

            except Exception:
                continue

        if not batch:
            print(" No new alerts")
            continue

        inserted = len(batch)
        total_inserted += inserted

        print(f" INSERTING {inserted} alerts for {county_name}")

        db.execute(text("""
            INSERT INTO radd_alerts (id, alert_date, loss_ha, confidence, geometry)
            VALUES (
                :id,
                :date,
                :loss,
                :confidence,
                ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)
            )
        """), batch)

        db.commit()

    return {
        "message": "RADD ingestion complete",
        "total_inserted": total_inserted
    }