import requests

# =========================
# STEP 1 — GET TOKEN
# =========================

auth_url = "https://data-api.globalforestwatch.org/auth/token"

auth_headers = {
    "Content-Type": "application/x-www-form-urlencoded"
}

auth_data = {
    "grant_type": "password",
    "username": "omojaarnorld7@gmail.com",        # 🔥 replace
    "password": "@Samarnorld1808",     # 🔥 replace
    "scope": "",
    "client_id": "",
    "client_secret": ""
}

auth_res = requests.post(auth_url, headers=auth_headers, data=auth_data)

print("\n=== TOKEN RESPONSE ===")
print("STATUS:", auth_res.status_code)
print("BODY:", auth_res.text)

if auth_res.status_code != 200:
    print("❌ Failed to authenticate")
    exit()

# Extract token
token = auth_res.json()["data"]["access_token"]

print("\n✅ TOKEN ACQUIRED")

# =========================
# STEP 2 — USE EXISTING API KEY
# =========================

api_key = "e2f5bc73-b340-4293-a1ae-dacbe0c371ab"

print("\n✅ USING EXISTING API KEY:", api_key)

# =========================
# STEP 3 — TEST API (DATASETS)
# =========================

datasets_url = "https://data-api.globalforestwatch.org/datasets"

datasets_headers = {
    "x-api-key": api_key,
    "origin": "localhost"
}

datasets_res = requests.get(datasets_url, headers=datasets_headers)

print("\n=== DATASETS TEST ===")
print("STATUS:", datasets_res.status_code)

try:
    data = datasets_res.json()
    print("DATA SAMPLE:", str(data)[:500])  # print first part
except:
    print("RAW:", datasets_res.text)


print("\n🔥 DONE — YOU ARE CONNECTED TO GFW API")