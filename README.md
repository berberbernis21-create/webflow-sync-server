# Lost & Found Webflow Sync Server

Dual-pipeline sync: **Luxury / Accessories** and **Furniture & Home**. Each vertical has its own Webflow collection and SOLD behavior.

- **POST `/sync-all`** — Pull all Shopify products, detect vertical (luxury vs furniture), sync to the correct Webflow collection, write back metafields.

## Environment variables

**Shopify (shared)**  
`SHOPIFY_STORE`, `SHOPIFY_ACCESS_TOKEN`

**Luxury (existing)**  
`WEBFLOW_TOKEN`, `WEBFLOW_COLLECTION_ID`

**Furniture & Home (RESALE)**  
`RESALE_TOKEN`, `RESALE_Products_Collection_ID`, `RESALE_SKUs_Collection_ID`, `RESALE_WEBFLOW_SITE_ID`

## Behavior

- **Vertical detection:** Product title, vendor, tags, product type → `luxury` or `furniture`.
- **Luxury:** Syncs to Luxury Webflow collection. SOLD → "Recently Sold" + hidden. Category from luxury keywords.
- **Furniture:** Syncs to Furniture Webflow collection. SOLD → `sold: true`, item stays visible. Category from furniture keywords (fallback: Accessories). Dimensions (weight + optional metafields) and `dimensions_status` (present | missing) written when applicable.
- **Shopify write-back:** `custom.vertical`, `custom.category`, `custom.dimensions_status` (furniture), vendor.
- **No duplicates:** Cache stores `webflowId` and `vertical` per Shopify product; lookup uses the correct collection.
- **Disappeared products:** Marked SOLD in the same collection (using cached vertical).

---

Legacy: POST `/webflow-sync` to create a Webflow CMS item (single-item). JSON example:
```json
{
  "name": "Test Bag",
  "price": 600,
  "brand": "Fendi",
  "description": "Gorgeous micro baguette",
  "shopifyProductId": "12345",
  "shopifyUrl": "https://shopify.com/products/xyz",
  "featuredImage": "https://...jpg",
  "images": [
    { "url": "https://...1.jpg", "alt": "" },
    { "url": "https://...2.jpg", "alt": "" }
  ]
}
```

