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

**LLM vertical classifier (required for sync)**  
`OPENAI_API_KEY` — OpenAI API key for GPT-based LUXURY vs HOME_INTERIOR classification.  
`OPENAI_VERTICAL_MODEL` — (optional) Model name, default `gpt-4o-mini`.  
`LLM_VERTICAL_SECOND_PASS` — (optional) Set to `true` or `1` to run a second validation pass; if it disagrees with the first, result is forced to HOME_INTERIOR.

**Sold retention (optional)** — **Furniture (ecommerce) only.** After listings have been sold for **`SOLD_RETENTION_DAYS`** (default **4**), each `/sync-all` run **deletes** the ecommerce product from Webflow; if DELETE is not supported, it **archives** as fallback (same helper as duplicate cleanup). **Luxury** is never part of retention: sold items stay in the **Recently Sold** category (and hidden from the main grid) via normal sync — no CMS sweep.  
`SOLD_RETENTION_DAYS` — Default `4` (furniture only).  
`SOLD_RETENTION_DISABLE` — Set to `1` or `true` to turn off furniture retention (no delete/archive sweep).  
`FURNITURE_SOLD_SINCE_FIELD_SLUG` — (optional) DateTime field slug on **furniture ecommerce** products. Default **`date-sold`**. The sync **always fills** this when marking sold (or when `sold` is true and the date is empty); cleared when back in stock. **Ongoing** retention uses **only** this Webflow datetime (not `soldMarkedAt` in cache). One-time backfill can still fall back to Webflow `lastUpdated` / etc. if the field is empty. `LUXURY_SOLD_SINCE_FIELD_SLUG` — optional; if set, written when luxury items are marked sold (not used for removing luxury listings).

**One-time sold backfill (first `/sync-all` after deploy)** — **Furniture only.** While `DATA_DIR/sold_retention_backfill_2026-04-02.done` does **not** exist, the server **deletes** (archive fallback) **sold** furniture listings whose **anchor date** is **on or before** the cutoff (default **2026-04-02** end of day UTC). Anchor = `FURNITURE_SOLD_SINCE_FIELD_SLUG` (`date-sold`) when set and parseable, else Webflow ecommerce product `lastUpdated` / `lastPublished` / `createdOn` (not `soldMarkedAt` in cache). Items with **no** parseable anchor are skipped (`sold_retention.backfill_skip_no_anchor`). After the pass, it writes the marker file; **delete that file** to run another backfill.  
`SOLD_BACKFILL_BEFORE_DATE` — Optional `YYYY-MM-DD` (default `2026-04-02`).  
`SOLD_BACKFILL_DISABLE` — Set to `1` or `true` to skip the one-time backfill only.  
`SOLD_BACKFILL_DONE_FILE` — Optional override path for the marker file.  
After the marker exists, only the normal **`SOLD_RETENTION_DAYS`** (default 4) furniture rule runs.

**Webflow sold sweep (each `/sync-all`)** — For every Webflow item that has a **Shopify product id** and is not already sold/archived: if that product is in the Shopify crawl with **first variant inventory ≤ 0**, mark sold in Webflow. If the id is **missing** from the crawl, the server **GETs** `/products/{id}.json`: **404** → mark sold; **active** with **qty ≤ 0** → mark sold; **active** with stock → skip. This is in addition to the “disappeared from cache” path for ids that used to be synced.

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

- **Vertical detection:** LLM-based (GPT): product title, description, vendor, tags, product type → `LUXURY` or `HOME_INTERIOR` (mapped to `luxury` / `furniture`). Uses semantic understanding; confidence < 0.65 or parse failure → HOME_INTERIOR. Strong furniture indicators force HOME_INTERIOR unless clearly wearable/jewelry. Optional second-pass validation can override LUXURY to HOME_INTERIOR on disagreement.
- **Luxury:** Syncs to Luxury Webflow collection. SOLD → "Recently Sold" + hidden. Category from luxury keywords.
- **Furniture:** Syncs to Furniture Webflow collection. SOLD → `sold: true`, item stays visible. Category from furniture keywords (fallback: Accessories). Dimensions (weight + optional metafields) and `dimensions_status` (present | missing) written when applicable.
- **Inventory → sold:** When the **first variant** goes from in stock to **0** (or negative), Webflow is marked sold on the next sync (`shouldMarkSoldTransition` + `shopifyHash` includes qty). String quantities from Shopify are normalized. If Webflow is wrong but cache says 0, `repair_sold` still PATCHes.
- **Shopify write-back:** `custom.department` (parent: "Furniture & Home" or "Luxury Goods"), `custom.category` (child: e.g. Living Room, Handbags), `custom.vertical`, `custom.dimensions_status` (furniture), vendor. Use these metafields in Shopify collection rules (e.g. Department is equal to Luxury Goods, Luxury Goods is equal to Handbags).
- **No duplicates:** Cache stores `webflowId` and `vertical` per Shopify product; lookup uses the correct collection. If the same item would appear in multiple places we archive the duplicate: (1) **Cache said Furniture, we now detect Luxury** → archive from Furniture, re-sync to Luxury, send email, **throw**. (2) **No cache but we're creating in Luxury** → we first check the Furniture collection for the same Shopify product ID; if found (e.g. item was added before or cache was lost), we archive it from Furniture, send email, then create in Luxury. So items like bags/clutches that were wrongly in Furniture get archived and removed when you run a full sync.
- **Disappeared products:** Marked SOLD in the same collection (using cached vertical).
- **Qty 0 vs Webflow:** If Shopify inventory is 0 but Webflow is not in sold state (e.g. cache had `lastQty: 0` so the server used to hit `skip_unchanged`), the sync now **repairs** sold state (`sync_product.repair_sold`) instead of skipping.
- **Long-sold furniture removal:** One-time **April 2 (configurable) backfill** deletes old **sold furniture** (archive fallback); **luxury** is unchanged (Recently Sold). After the marker file exists, **furniture** sold listings older than `SOLD_RETENTION_DAYS` (default **4**) are removed the same way on each `/sync-all`.
- **Webflow sold sweep:** Any listing with a Shopify id is aligned to Shopify: **0 qty** or **product gone/archived/draft** → mark sold (see env section above).

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

