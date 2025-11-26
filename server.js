import express from "express";
import axios from "axios";
import cors from "cors";
import dotenv from "dotenv";
import fs from "fs";
import { CATEGORY_KEYWORDS } from "./categoryKeywords.js";
import { detectBrandFromProduct } from "./brand.js";

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json());

/* ======================================================
   PATHS / CACHE SETUP
====================================================== */
const DATA_DIR = "./data";
const CACHE_FILE = `${DATA_DIR}/lastSync.json`;

function ensureDataDir() {
  if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
  }
}

function loadCache() {
  try {
    ensureDataDir();
    if (!fs.existsSync(CACHE_FILE)) return {};
    const raw = fs.readFileSync(CACHE_FILE, "utf8");
    return JSON.parse(raw);
  } catch (err) {
    console.error("⚠️ Failed to load cache:", err.toString());
    return {};
  }
}

function saveCache(cache) {
  try {
    ensureDataDir();
    fs.writeFileSync(CACHE_FILE, JSON.stringify(cache, null, 2), "utf8");
  } catch (err) {
    console.error("⚠️ Failed to save cache:", err.toString());
  }
}

/**
 * Cache entry helper:
 * - New format: { hash, webflowId, lastQty }
 * - Legacy: hash only (we normalize it)
 */
function getCacheEntry(cache, idStr) {
  const entry = cache[idStr];
  if (!entry) return null;

  if (entry && typeof entry === "object" && entry.hash) {
    return {
      hash: entry.hash,
      webflowId: entry.webflowId || null,
      lastQty:
        typeof entry.lastQty === "number" ? entry.lastQty : null,
    };
  }

  // Legacy format: value is just the hash
  return { hash: entry, webflowId: null, lastQty: null };
}

/**
 * Direct Webflow item lookup by ID (bypasses pagination).
 */
async function getWebflowItemById(itemId) {
  if (!itemId) return null;

  try {
    const url = `https://api.webflow.com/v2/collections/${process.env.WEBFLOW_COLLECTION_ID}/items/${itemId}`;
    const response = await axios.get(url, {
      headers: {
        Authorization: `Bearer ${process.env.WEBFLOW_TOKEN}`,
        accept: "application/json",
      },
    });
    return response.data;
  } catch (err) {
    if (err.response?.status === 404) {
      console.warn(`⚠️ Webflow item ${itemId} not found by ID.`);
      return null;
    }
    console.error("⚠️ getWebflowItemById error:", err.toString());
    return null;
  }
}

/* ======================================================
   SHOPIFY — AUTO PUBLISH TO SALES CHANNELS
====================================================== */

const SHOPIFY_GRAPHQL_URL = `https://${process.env.SHOPIFY_STORE}.myshopify.com/admin/api/2024-01/graphql.json`;

let cachedPublicationIds = null;

// 1. Get publication IDs once and cache them
async function getPublicationIds() {
  if (cachedPublicationIds) return cachedPublicationIds;

  const query = `
    {
      publications(first: 10) {
        edges {
          node {
            id
            name
          }
        }
      }
    }
  `;

  const resp = await axios.post(
    SHOPIFY_GRAPHQL_URL,
    { query },
    {
      headers: {
        "X-Shopify-Access-Token": process.env.SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json",
      },
    }
  );

  const all = resp.data?.data?.publications?.edges || [];

  const publishTargets = all.filter((edge) => {
    const n = edge.node.name.toLowerCase();
    return (
      n.includes("online store") ||
      n.includes("facebook") ||
      n.includes("instagram") ||
      n.includes("buy button") ||
      n.includes("shop") // shop app
    );
  });

  const ids = publishTargets.map((e) => e.node.id);
  cachedPublicationIds = ids;

  console.log("🟢 Loaded publication IDs:", publishTargets.map((p) => p.node.name));
  return ids;
}

// 2. Publish product to all selected sales channels
async function publishToSalesChannels(productId) {
  const pubIds = await getPublicationIds();

  for (const publicationId of pubIds) {
    const mutation = `
      mutation {
        publishablePublish(
          id: "gid://shopify/Product/${productId}"
          input: { publicationId: "${publicationId}" }
        ) {
          userErrors { message }
        }
      }
    `;

    try {
      await axios.post(
        SHOPIFY_GRAPHQL_URL,
        { query: mutation },
        {
          headers: {
            "X-Shopify-Access-Token": process.env.SHOPIFY_ACCESS_TOKEN,
            "Content-Type": "application/json",
          },
        }
      );

      console.log(`🌐 Published ${productId} → ${publicationId}`);
    } catch (err) {
      console.error("⚠️ Publishing error:", err.response?.data || err.toString());
    }
  }
}

/* ======================================================
   HASH FOR CHANGE DETECTION
====================================================== */
function shopifyHash(product) {
  return {
    title: product.title,
    vendor: product.vendor,
    body_html: product.body_html,
    price: product.variants?.[0]?.price || null,
    qty: product.variants?.[0]?.inventory_quantity ?? null,
    images: (product.images || []).map((i) => i.src),
    slug: product.handle,
  };
}

/* ======================================================
   CATEGORY DETECTOR
====================================================== */
function detectCategory(title) {
  if (!title) return "Other";
  const normalized = title.toLowerCase();
  for (const [category, keywords] of Object.entries(CATEGORY_KEYWORDS)) {
    for (const kw of keywords) {
      if (normalized.includes(kw.toLowerCase())) return category;
    }
  }
  return "Other";
}

/* ======================================================
   SHOPIFY — FETCH ALL PRODUCTS
====================================================== */
async function fetchAllShopifyProducts() {
  const store = process.env.SHOPIFY_STORE;
  const token = process.env.SHOPIFY_ACCESS_TOKEN;

  let allProducts = [];
  let lastId = 0;

  while (true) {
    const baseUrl = `https://${store}.myshopify.com/admin/api/2024-01/products.json`;
    const url =
      lastId === 0
        ? `${baseUrl}?limit=250`
        : `${baseUrl}?limit=250&since_id=${lastId}`;

    console.log("🛒 Fetching Shopify products:", url);

    const response = await axios.get(url, {
      headers: {
        "X-Shopify-Access-Token": token,
        "Content-Type": "application/json",
      },
    });

    const products = response.data.products || [];
    if (!products.length) break;

    allProducts.push(...products);
    lastId = products[products.length - 1].id;

    if (products.length < 250) break;
  }

  console.log(`📦 Total Shopify products fetched: ${allProducts.length}`);
  return allProducts;
}

/* ======================================================
   WEBFLOW — FIND EXISTING ITEM BY SHOPIFY PRODUCT ID
====================================================== */
async function findExistingWebflowItem(shopifyProductId) {
  let page = 1;

  while (true) {
    const url = `https://api.webflow.com/v2/collections/${process.env.WEBFLOW_COLLECTION_ID}/items?page=${page}&limit=100`;

    const response = await axios.get(url, {
      headers: {
        Authorization: `Bearer ${process.env.WEBFLOW_TOKEN}`,
        accept: "application/json",
      },
    });

    const items = response.data.items || [];

    for (const item of items) {
      if (item.fieldData?.["shopify-product-id"] === String(shopifyProductId)) {
        return item;
      }
    }

    if (!response.data.pagination?.nextPage) break;
    page = response.data.pagination.nextPage;
  }

  return null;
}

/* ======================================================
   ⭐ MARK AS SOLD IN WEBFLOW
====================================================== */
async function markAsSold(existing) {
  if (!existing) return;

  const soldPayload = {
    fieldData: {
      ...existing.fieldData,
      category: "Recently Sold",
      "show-on-webflow": false,
    },
  };

  await axios.patch(
    `https://api.webflow.com/v2/collections/${process.env.WEBFLOW_COLLECTION_ID}/items/${existing.id}`,
    soldPayload,
    {
      headers: {
        Authorization: `Bearer ${process.env.WEBFLOW_TOKEN}`,
        "Content-Type": "application/json",
      },
    }
  );

  console.log(`💀 Marked SOLD in Webflow → ${existing.id}`);
}

/* ======================================================
   CORE SYNC LOGIC
====================================================== */
async function syncSingleProduct(product, cache) {
  const shopifyProductId = product.id;
  const idStr = String(shopifyProductId);

  const cacheEntry = getCacheEntry(cache, idStr);
  const previousHash = cacheEntry?.hash || null;

  const name = product.title;
  const description = product.body_html;
  const price = product.variants?.[0]?.price || null;
  const slug = product.handle;
  const shopifyUrl = `https://${process.env.SHOPIFY_STORE}.myshopify.com/products/${slug}`;

  /* BRAND DETECTION */
  let detectedBrand = detectBrandFromProduct(product.title, product.vendor);
  if (!detectedBrand) detectedBrand = product.vendor || null;

  if (detectedBrand && detectedBrand !== product.vendor) {
    try {
      await axios.put(
        `https://${process.env.SHOPIFY_STORE}.myshopify.com/admin/api/2024-01/products/${product.id}.json`,
        { product: { id: product.id, vendor: detectedBrand } },
        {
          headers: {
            "X-Shopify-Access-Token": process.env.SHOPIFY_ACCESS_TOKEN,
            "Content-Type": "application/json",
          },
        }
      );
      console.log(`🏷️ Shopify vendor updated → ${detectedBrand}`);
    } catch {
      // ignore vendor update errors
    }
  }

  const brand = detectedBrand;

  /* IMAGES */
  const allImages = (product.images || []).map((img) => img.src);
  const featuredImage = product.image?.src || allImages[0] || null;
  const gallery = allImages.filter((url) => url !== featuredImage);

  /* SOLD & QTY LOGIC */
  const variant = product.variants?.[0];
  const qty =
    typeof variant?.inventory_quantity === "number"
      ? variant.inventory_quantity
      : null;

  const previousQty =
    typeof cacheEntry?.lastQty === "number" ? cacheEntry.lastQty : null;

  const soldByInventoryNow = qty !== null && qty <= 0;
  const normalizedTitle = (name || "").toLowerCase();
  const soldByTitle =
    normalizedTitle.includes("sold") || normalizedTitle.includes("reserved");

  // For display/category, keep using both signals
  const recentlySold = soldByInventoryNow || soldByTitle;

  // For Webflow "mark as sold" side-effect, trigger only on transition:
  // previously > 0 (or unknown) → now <= 0
  const justSoldByInventory =
    soldByInventoryNow && (previousQty === null || previousQty > 0);

  let category = detectCategory(name);
  let showOnWebflow = !recentlySold;
  if (recentlySold) category = "Recently Sold";

  console.log(
    `🔁 Product ${shopifyProductId} | "${name}" | brand=${brand} | qty=${qty} | sold=${recentlySold} | prevQty=${previousQty} | justSold=${justSoldByInventory}`
  );

  /* SHOPIFY METAFIELD UPDATE */
  try {
    await axios.put(
      `https://${process.env.SHOPIFY_STORE}.myshopify.com/admin/api/2024-01/products/${product.id}.json`,
      {
        product: {
          id: product.id,
          metafields: [
            {
              namespace: "custom",
              key: "category",
              type: "single_line_text_field",
              value: category,
            },
          ],
        },
      },
      {
        headers: {
          "X-Shopify-Access-Token": process.env.SHOPIFY_ACCESS_TOKEN,
          "Content-Type": "application/json",
        },
      }
    );
  } catch {
    // ignore metafield errors
  }

  /* AUTO PUBLISH TO SHOPIFY SALES CHANNELS */
  try {
    await publishToSalesChannels(product.id);
  } catch (err) {
    console.error("⚠️ Failed to publish:", err.toString());
  }

  /* WEBFLOW PAYLOAD */
  const fieldDataBase = {
    name,
    brand,
    price,
    description,
    "shopify-product-id": idStr,
    "shopify-url": shopifyUrl,
    category,
    "featured-image": featuredImage ? { url: featuredImage } : null,
    "image-1": gallery[0] ? { url: gallery[0] } : null,
    "image-2": gallery[1] ? { url: gallery[1] } : null,
    "image-3": gallery[2] ? { url: gallery[2] } : null,
    "image-4": gallery[3] ? { url: gallery[3] } : null,
    "image-5": gallery[4] ? { url: gallery[4] } : null,
    "show-on-webflow": showOnWebflow,
  };

  const currentHash = shopifyHash(product);

  /* --- RESOLVE EXISTING ITEM --- */

  let existing = null;

  // 1) If cache knows Webflow ID, try direct fetch first
  if (cacheEntry?.webflowId) {
    existing = await getWebflowItemById(cacheEntry.webflowId);
  }

  // 2) Fallback to scan by Shopify Product ID
  if (!existing) {
    existing = await findExistingWebflowItem(shopifyProductId);
  }

  // If cache says we've seen this product, but Webflow item is missing,
  // do NOT create a new item (this prevents duplicates).
  if (cacheEntry && !existing) {
    console.error(
      `❌ SKIP: Shopify ${idStr} has cache entry but no Webflow item found. Avoiding duplicate create.`
    );

    // Still update cache with latest qty + hash so transitions remain correct
    cache[idStr] = {
      hash: currentHash,
      webflowId: cacheEntry.webflowId || null,
      lastQty: qty,
    };

    return { operation: "skip-missing-webflow", id: null };
  }

  // Helper to write cache consistently
  const setCacheEntry = (webflowIdValue) => {
    cache[idStr] = {
      hash: currentHash,
      webflowId: webflowIdValue || cacheEntry?.webflowId || existing?.id || null,
      lastQty: qty,
    };
  };

  /* IF JUST SOLD → HANDLE IT (ONLY ON TRANSITION) */
  if (justSoldByInventory && existing) {
    await markAsSold(existing);
    setCacheEntry(existing.id);
    return { operation: "sold", id: existing.id };
  }

  /* CREATE NEW ITEM
     Only allowed when:
       - There's no existing Webflow item
       - AND no cache entry (truly first time seen)
  */
  if (!existing && !cacheEntry) {
    const createPayload = { ...fieldDataBase, slug };
    const resp = await axios.post(
      `https://api.webflow.com/v2/collections/${process.env.WEBFLOW_COLLECTION_ID}/items`,
      { fieldData: createPayload },
      {
        headers: {
          Authorization: `Bearer ${process.env.WEBFLOW_TOKEN}`,
          "Content-Type": "application/json",
        },
      }
    );

    const newId = resp.data.id;

    cache[idStr] = {
      hash: currentHash,
      webflowId: newId,
      lastQty: qty,
    };

    console.log(`🆕 Created Webflow item ${newId} for Shopify ${idStr}`);
    return { operation: "create", id: newId };
  }

  /* UPDATE IF CHANGED (NON-SOLD CASES) */
  const hasChanged =
    !previousHash ||
    JSON.stringify(previousHash) !== JSON.stringify(currentHash);

  if (!hasChanged) {
    // No changes, but still update lastQty so transitions work next time
    setCacheEntry(existing?.id || cacheEntry?.webflowId || null);
    return { operation: "skip", id: existing?.id || null };
  }

  await axios.patch(
    `https://api.webflow.com/v2/collections/${process.env.WEBFLOW_COLLECTION_ID}/items/${existing.id}`,
    { fieldData: { ...fieldDataBase, slug: existing.fieldData.slug } },
    {
      headers: {
        Authorization: `Bearer ${process.env.WEBFLOW_TOKEN}`,
        "Content-Type": "application/json",
      },
    }
  );

  setCacheEntry(existing.id);

  console.log(`✏️ Updated Webflow item ${existing.id} for Shopify ${idStr}`);
  return { operation: "update", id: existing.id };
}

/* ======================================================
   ROUTES
====================================================== */
app.get("/", (req, res) => {
  res.send(
    "Lost & Found – Full Shopify → Webflow Sync (Brand Normalized, Duplicate-Safe, Sold-by-Transition)"
  );
});

app.post("/sync-all", async (req, res) => {
  try {
    console.log("🔄 FULL SYNC STARTED…");

    const products = await fetchAllShopifyProducts();
    const cache = loadCache();

    let created = 0;
    let updated = 0;
    let skipped = 0;
    let sold = 0;

    /* Detect disappeared (= SOLD) Shopify items */
    const previousIds = Object.keys(cache);
    const currentIds = products.map((p) => String(p.id));

    const disappeared = previousIds.filter((id) => !currentIds.includes(id));

    for (const goneId of disappeared) {
      const entry = getCacheEntry(cache, goneId);

      let existing = null;

      // Prefer direct lookup via cached Webflow ID if we have it
      if (entry?.webflowId) {
        existing = await getWebflowItemById(entry.webflowId);
      }

      // Fallback: old scan by Shopify ID
      if (!existing) {
        existing = await findExistingWebflowItem(goneId);
      }

      if (existing) {
        await markAsSold(existing);
        sold++;
      }

      delete cache[goneId];
    }

    /* Process current products */
    for (const product of products) {
      try {
        const result = await syncSingleProduct(product, cache);

        if (result.operation === "create") created++;
        else if (result.operation === "update") updated++;
        else if (result.operation === "sold") sold++;
        else skipped++;
      } catch (err) {
        console.error("⚠️ Error syncing:", product.id, err.toString());
      }
    }

    saveCache(cache);

    res.json({
      status: "ok",
      total: products.length,
      created,
      updated,
      skipped,
      sold,
    });
  } catch (err) {
    console.error("❌ /sync-all error:", err.toString());
    res.status(500).json({ error: err.toString() });
  }
});

/* ======================================================
   SERVER
====================================================== */
const PORT = process.env.PORT || 4000;
app.listen(PORT, () => {
  console.log(`🔥 L&F Sync Server running on port ${PORT}`);
});
