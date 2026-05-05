# app/services/gee/ee_init.py
import ee
import os
import json

SERVICE_ACCOUNT = "greenmap-kenya@greenmap-kenya-483110.iam.gserviceaccount.com"

_ee_initialized = False


def initialize_ee():
    global _ee_initialized
    if _ee_initialized:
        return
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
        _ee_initialized = True
        print(" EE initialized")

    except Exception as e:
        print(" EE init failed:", str(e))


def warmup_earth_engine_once():
    """
    One lightweight GEE round-trip after Initialize (auth + client warmup).
    Use at the start of batched forest prewarm so workers pay the handshake once.
    """
    initialize_ee()
    ee.Number(1).getInfo()