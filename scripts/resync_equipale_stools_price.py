"""Re-sync Mexican Equipale stools after SKU price/markdown fix (Shopify $299 vs Webflow $499)."""
import json
import sys

import requests

SYNC_SERVER = "https://webflow-sync-server.onrender.com"
SHOPIFY_ID = "9331494682883"  # Mexican Equipale Counter Stools


def main():
    print(f"Clearing cache for {SHOPIFY_ID}...")
    r0 = requests.post(
        f"{SYNC_SERVER}/clear-cache",
        json={"shopifyProductIds": [SHOPIFY_ID]},
        timeout=60,
    )
    print("clear-cache:", r0.status_code, r0.text[:300])

    print(f"Syncing {SHOPIFY_ID} (forceReclassify)...")
    r1 = requests.post(
        f"{SYNC_SERVER}/sync-by-ids",
        json={"shopifyProductIds": [SHOPIFY_ID], "forceReclassify": True},
        timeout=600,
    )
    print("sync-by-ids:", r1.status_code)
    try:
        body = r1.json()
        print(json.dumps(body, indent=2)[:4000])
    except Exception:
        print(r1.text[:2000])
        sys.exit(1)

    op = (body.get("results") or [{}])[0].get("operation")
    if op not in ("update", "sold"):
        print(f"WARNING: expected update/sold, got {op!r} — deploy server.js fix first if still skip")
        sys.exit(1)
    print("Done. Check Webflow SKU: price $299, compare-at $499.")


if __name__ == "__main__":
    main()
