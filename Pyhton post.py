import requests
import json

WEBFLOW_TOKEN = "YOUR_WEBFLOW_TOKEN"
SKU_COLLECTION_ID = "61bdff45aeb6f0ebdf7748c0"

url = f"https://api.webflow.com/v2/collections/{SKU_COLLECTION_ID}/items?limit=5"

headers = {
    "Authorization": f"1748644312288ac0d26cc6925b2476809a55786de20804d457212344a66ae047",
    "accept": "application/json"
}

resp = requests.get(url, headers=headers)
resp.raise_for_status()

data = resp.json()
items = data.get("items", [])

print(f"\nTOTAL ITEMS RETURNED: {len(items)}\n")

for item in items:
    field_data = item.get("fieldData", {})

    print("=" * 50)
    print("SKU ITEM ID:", item["id"])
    print("NAME:", field_data.get("name"))
    print("\nFIELD KEYS:")
    for k in field_data.keys():
        print(" -", k)

    print("\nPRICE FIELD:")
    print(json.dumps(field_data.get("price"), indent=2))

    print("\nMAIN IMAGE:")
    print(json.dumps(field_data.get("main-image"), indent=2))

    print("\nMORE IMAGES:")
    print(json.dumps(field_data.get("more-images"), indent=2))

    print("\nWEIGHT:", field_data.get("weight"))
    print("DIMENSIONS:",
          "L=", field_data.get("length"),
          "W=", field_data.get("width"),
          "H=", field_data.get("height"))
    print("=" * 50)
