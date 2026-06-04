"""
Re-sync misclassified brooches via webflow-sync-server /sync-by-ids (forceReclassify).

This is the correct path: Shopify tags/metafields, Google Merchant, Webflow Luxury CMS,
Render cache, and duplicate-placement email all run on the server.

Requires deployed webflow-sync-server (Render). Do not use move_brooches_furniture_to_luxury.py.
"""
import json
import sys

import requests

SYNC_SERVER = "https://webflow-sync-server.onrender.com"
SHOPIFY_STORE = "lost-and-found-luxury-resale"

BROOCH_TITLES = [
    "Luxury Forstner 10kt Flower Brooch",
    "Luxury Pearl & Rhinestone Brooch",
]


def fetch_shopify_products():
    products = []
    page = 1
    while True:
        url = f"https://{SHOPIFY_STORE}.myshopify.com/products.json?limit=250&page={page}"
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        batch = resp.json().get("products") or []
        if not batch:
            break
        products.extend(batch)
        if len(batch) < 250:
            break
        page += 1
    return products


def resolve_ids(titles, products):
    by_title = {p["title"].strip().lower(): str(p["id"]) for p in products}
    resolved = []
    missing = []
    for title in titles:
        pid = by_title.get(title.strip().lower())
        if pid:
            resolved.append({"title": title, "shopifyProductId": pid})
        else:
            missing.append(title)
    return resolved, missing


def main():
    print("Fetching Shopify catalog...")
    products = fetch_shopify_products()
    resolved, missing = resolve_ids(BROOCH_TITLES, products)

    if missing:
        print("Could not find these titles in Shopify (check exact title spelling):")
        for title in missing:
            print(f"  - {title}")
            # Fuzzy hint: partial match on forstner / rhinestone brooch
            needle = title.split()[0].lower()
            hints = [
                p
                for p in products
                if needle in (p.get("title") or "").lower()
                or "forstner" in (p.get("title") or "").lower()
                and "brooch" in (p.get("title") or "").lower()
                or "rhinestone brooch" in (p.get("title") or "").lower()
            ]
            for p in hints[:5]:
                print(f"    near match: {p['id']}  {p.get('title')}")
        if not resolved:
            sys.exit(1)

    ids = [r["shopifyProductId"] for r in resolved]
    print(f"\nSyncing {len(ids)} brooches via {SYNC_SERVER}/sync-by-ids (forceReclassify)...")
    for row in resolved:
        print(f"  {row['shopifyProductId']}  {row['title']}")

    resp = requests.post(
        f"{SYNC_SERVER}/sync-by-ids",
        json={"shopifyProductIds": ids, "forceReclassify": True},
        timeout=600,
    )
    print(f"\nHTTP {resp.status_code}")
    try:
        data = resp.json()
        print(json.dumps(data, indent=2))
    except Exception:
        print(resp.text)
        sys.exit(1)

    if resp.status_code >= 400:
        sys.exit(1)

    for row in data.get("results") or []:
        print(
            f"  -> {row.get('shopifyProductId')}: {row.get('operation')} "
            f"webflowId={row.get('webflowId')}"
        )

    print("\nDone. In Shopify confirm: Luxury Goods, type/category Jewelry (not Furniture & Home / Bedroom).")


if __name__ == "__main__":
    main()
