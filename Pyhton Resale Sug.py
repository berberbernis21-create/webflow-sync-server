import requests

WEBFLOW_TOKEN = "1748644312288ac0d26cc6925b2476809a55786de20804d457212344a66ae047"
COLLECTION_ID = "61bdff45aeb6f0a0627748be"

URL = f"https://api.webflow.com/v2/collections/{COLLECTION_ID}"

headers = {
    "Authorization": f"Bearer {WEBFLOW_TOKEN}",
    "accept": "application/json"
}

resp = requests.get(URL, headers=headers)
resp.raise_for_status()

data = resp.json()

print("\n==============================")
print("COLLECTION NAME:", data.get("name"))
print("COLLECTION ID  :", data.get("id"))
print("==============================\n")

fields = data.get("fields", [])

print(f"TOTAL FIELDS: {len(fields)}\n")

for field in fields:
    print("--------------------------------")
    print("DISPLAY NAME :", field.get("displayName"))
    print("SLUG         :", field.get("slug"))
    print("TYPE         :", field.get("type"))
    print("REQUIRED     :", field.get("isRequired"))
