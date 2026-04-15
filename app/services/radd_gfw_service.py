import ee
import os
import json
from sqlalchemy import text
from sqlalchemy.orm import Session
import uuid
from datetime import datetime, timedelta

SERVICE_ACCOUNT = "greenmap-kenya@greenmap-kenya-483110.iam.gserviceaccount.com"


# =========================
# EE INIT
# =========================
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

    geom = ee.Geometry(json.loads(geojson))

    collection = ee.ImageCollection("projects/glad/alert/UpdResult") \
        .filterBounds(geom) \
        .filterDate("2025-01-01", "2026-12-31")

    size = collection.size().getInfo()
    print("🔥 COLLECTION SIZE:", size)

    if size == 0:
        return []

    all_features = []
    images = collection.toList(size)

    for i in range(size):
        image = ee.Image(images.get(i))
        band_names = image.bandNames().getInfo()

        bands_to_use = []

        if "conf26" in band_names:
            bands_to_use += ["conf26", "alertDate26"]

        if "conf25" in band_names:
            bands_to_use += ["conf25", "alertDate25"]

        if not bands_to_use:
            continue

        alerts = image.select(bands_to_use)

        mask = ee.Image.constant(0)

        if "conf26" in band_names:
            mask = mask.Or(image.select("conf26").gt(0))

        if "conf25" in band_names:
            mask = mask.Or(image.select("conf25").gt(0))

        vectors = alerts.updateMask(mask).reduceToVectors(
            geometry=geom,
            scale=10,
            geometryType="centroid",
            reducer=ee.Reducer.first(),
            maxPixels=1e13
        )

        features = vectors.getInfo().get("features", [])
        all_features.extend(features)

    print("🔥 TOTAL ALERTS (ALL IMAGES):", len(all_features))

    return all_features


# =========================
# INGEST INTO DB (FINAL FIXED)
# =========================
def ingest_radd_alerts_gfw(db: Session):

    initialize_ee()

    counties = db.execute(text("""
        SELECT name, ST_AsGeoJSON(geometry)
        FROM admin_county
    """)).fetchall()

    total_inserted = 0

    for county in counties:

        county_name = county[0]
        geojson = county[1]

        print(f"\n🔥 Processing county: {county_name}")

        alerts = fetch_radd_alerts_gee(geojson)

        if not alerts:
            print("⚠️ No alerts found")
            continue

        batch = []

        total_seen = 0
        skipped_conf = 0
        skipped_date = 0

        for f in alerts:
            try:
                total_seen += 1

                lon, lat = f["geometry"]["coordinates"]
                props = f.get("properties", {})

                alert_date = None
                conf = 0

                # =========================
                # PRIORITY: 2026 FIRST
                # =========================
                if props.get("alertDate26") and props.get("conf26"):
                    try:
                        days = int(props["alertDate26"])
                        if 1 <= days <= 366:
                            alert_date = datetime(2026, 1, 1) + timedelta(days=days - 1)
                            conf = props.get("conf26")
                    except:
                        pass

                # =========================
                # FALLBACK: 2025
                # =========================
                elif props.get("alertDate25") and props.get("conf25"):
                    try:
                        days = int(props["alertDate25"])
                        if 1 <= days <= 366:
                            alert_date = datetime(2025, 1, 1) + timedelta(days=days - 1)
                            conf = props.get("conf25")
                    except:
                        pass

                # =========================
                # SKIP IF NO DATE
                # =========================
                if alert_date is None:
                    skipped_date += 1
                    continue

                # =========================
                # FILTER NOISE
                # =========================
                if conf == 0:
                    skipped_conf += 1
                    continue

                # =========================
                # LOSS MODEL (CALIBRATED)
                # =========================
                loss_ha = 0.08

                is_confirmed = str(int(conf)).startswith('3')
                actual_confidence = 0.975 if is_confirmed else 0.85

                batch.append({
                    "id": str(uuid.uuid4()),
                    "date": alert_date,
                    "loss": loss_ha,
                    "confidence": actual_confidence,
                    "lon": lon,
                    "lat": lat
                })

            except Exception:
                continue

        # =========================
        # DEBUG LOGGING
        # =========================
        print(f"""
📊 COUNTY SUMMARY: {county_name}
TOTAL FEATURES: {total_seen}
SKIPPED (CONF=0): {skipped_conf}
SKIPPED (NO DATE): {skipped_date}
TO INSERT: {len(batch)}
""")

        if not batch:
            print("⚠️ No new alerts to insert")
            continue

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

        total_inserted += len(batch)

        print(f"🔥 INSERTED {len(batch)} alerts for {county_name}")

    return {
        "message": "RADD ingestion complete",
        "total_inserted": total_inserted
    }