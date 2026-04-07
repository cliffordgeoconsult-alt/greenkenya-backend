import ee
import os
import json

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
        print(" EE initialized")

    except Exception as e:
        print(" EE init failed:", str(e))