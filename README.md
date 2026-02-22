# Lost & Found Webflow Sync Server

Dual-pipeline sync: **Luxury / Accessories** and **Furniture & Home**. Each vertical has its own Webflow collection and SOLD behavior.

- **POST `/sync-all`** — Pull all Shopify products, detect vertical (luxury vs furniture), sync to the correct Webflow collection, write back metafields.

## Environment variables

**Logging (optional)**  
`LOG_LEVEL` — `info` (default) | `warn` | `error`. Use `error` for production cron jobs to cut most I/O and speed up frequent syncs; use `info` for debugging.

**Performance (optional)**  
`SYNC_CONCURRENCY` — Number of products to process in parallel (default `3`, max `15`). Lower values reduce Webflow API load.  
`WEBFLOW_MIN_DELAY_MS` — Minimum ms between Webflow API requests (default `1000` ≈ 60/min). Use `600` for CMS/eCommerce plans (120/min).  
`WEBFLOW_429_MAX_RETRIES` — Max retries on 429 rate limit (default `3`). Waits per `Retry-After` header.

**Shopify (shared)**  
`SHOPIFY_STORE`, `SHOPIFY_ACCESS_TOKEN`

**Luxury (existing)**  
`WEBFLOW_TOKEN`, `WEBFLOW_COLLECTION_ID`

**Furniture & Home (RESALE)**  
`RESALE_TOKEN`, `RESALE_Products_Collection_ID`, `RESALE_SKUs_Collection_ID`, `RESALE_WEBFLOW_SITE_ID`

**Furniture categories (required for “Pick Categories” to be set)**  
Webflow ecommerce expects `category` to be an ItemRef (the 24-character hex ID of the category item from your Webflow Categories collection). Set one env var per category; the key is derived from the display name (spaces → `_`, `/` → `_`, uppercase, non‑alphanumeric removed):

| Display name    | Env variable                     |
|-----------------|-----------------------------------|
| Living Room     | `FURNITURE_CATEGORY_LIVING_ROOM`  |
| Dining Room     | `FURNITURE_CATEGORY_DINING_ROOM`  |
| Office Den      | `FURNITURE_CATEGORY_OFFICE_DEN`   |
| Rugs            | `FURNITURE_CATEGORY_RUGS`         |
| Art / Mirrors   | `FURNITURE_CATEGORY_ART_MIRRORS`  |
| Bedroom         | `FURNITURE_CATEGORY_BEDROOM`      |
| Accessories     | `FURNITURE_CATEGORY_ACCESSORIES`  |
| Outdoor / Patio | `FURNITURE_CATEGORY_OUTDOOR_PATIO`|
| Lighting        | `FURNITURE_CATEGORY_LIGHTING`     |

Example: `FURNITURE_CATEGORY_LIVING_ROOM=507f1f77bcf86cd799439011` (use the real ID from Webflow). If a variable is missing, that category is not sent and the product will show no category in Webflow.

**Automatic category lookup:** The script now loads category IDs from Webflow at the start of each sync: it fetches your site's Collections, finds the **Categories** collection, and builds a map from category name to item ID. So if your Webflow Categories have items named exactly **Living Room**, **Dining Room**, **Office Den**, **Rugs**, **Art / Mirrors**, **Bedroom**, **Accessories**, **Outdoor / Patio**, and **Lighting**, you don't need to set any of the env vars above—categories will be assigned automatically. Env vars are only a fallback if the API fails or a name doesn't match.

## Behavior

- **Vertical detection:** Product title, description (body), vendor, tags, product type → `luxury` or `furniture` (keyword/fuzzy matching so e.g. "silk scarf" in the description can classify as luxury).
- **Luxury:** Syncs to Luxury Webflow collection. SOLD → "Recently Sold" + hidden. Category from luxury keywords.
- **Furniture:** Syncs to Furniture Webflow collection. SOLD → `sold: true`, item stays visible. Category from furniture keywords (fallback: Accessories). Dimensions (weight + optional metafields) and `dimensions_status` (present | missing) written when applicable.
- **Shopify write-back:** `custom.department` (parent: "Furniture & Home" or "Luxury Goods"), `custom.category` (child: e.g. Living Room, Handbags), `custom.vertical`, `custom.dimensions_status` (furniture), vendor. Use these metafields in Shopify collection rules (e.g. Department is equal to Luxury Goods, Luxury Goods is equal to Handbags).
- **No duplicates:** Cache stores `webflowId` and `vertical` per Shopify product; lookup uses the correct collection. If the same item would appear in multiple places we archive the duplicate: (1) **Cache said Furniture, we now detect Luxury** → archive from Furniture, re-sync to Luxury, send email, **throw**. (2) **No cache but we're creating in Luxury** → we first check the Furniture collection for the same Shopify product ID; if found (e.g. item was added before or cache was lost), we archive it from Furniture, send email, then create in Luxury. So items like bags/clutches that were wrongly in Furniture get archived and removed when you run a full sync.
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

