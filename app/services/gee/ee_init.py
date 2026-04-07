
import ee
SERVICE_ACCOUNT = "greenmap-kenya@greenmap-kenya-483110.iam.gserviceaccount.com"
KEY_PATH = "service-account.json"
def initialize_ee():
    credentials = ee.ServiceAccountCredentials(
        SERVICE_ACCOUNT,
        KEY_PATH
    )
    ee.Initialize(credentials)