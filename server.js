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
 * Normalize old cache entries (legacy hash-only)
 */
function getCacheEntry(cache, idStr) {
  const entry = cache[idStr];
  if (!entry) return null;

  if (typeof entry === "object" && entry.hash) {
    return entry; // new format
  }

  return { hash: entry, webflowId: null, lastQty: null };
}

/* ======================================================
   WEBFLOW DIRECT LOOKUP
====================================================== */
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
   SHOPIFY — AUTO PUBLISH TO CHANNELS
====================================================== */

const SHOPIFY_GRAPHQL_URL = `https://${process.env.SHOPIFY_STORE}.myshopify.com/admin/api/2024-01/graphql.json`;

let cachedPublicationIds = null;

async function getPublicationIds() {
  if (cachedPublicationIds) return cachedPublicationIds;

  const query = `
    {
      publications(first: 20) {
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

  cachedPublicationIds = all
    .filter((e) => {
      const n = e.node.name.toLowerCase();
      return (
        n.includes("online") ||
        n.includes("instagram") ||
        n.includes("facebook") ||
        n.includes("shop") ||
        n.includes("buy button")
      );
    })
    .map((e) => e.node.id);

  return cachedPublicationIds;
}

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
    } catch (err) {
      console.error("⚠️ Publishing error:", err.response?.data || err.toString());
    }
  }
}

/* ======================================================
   SHOPIFY — WRITE CATEGORY METAFIELD (custom.category)
====================================================== */
async function updateShopifyCategoryMetafield(productId, categoryValue) {
  const mutation = `
    mutation MetafieldsSet($metafields: [MetafieldsSetInput!]!) {
      metafieldsSet(metafields: $metafields) {
        metafields {
          key
          value
        }
        userErrors {
          field
          message
        }
      }
    }
  `;

  const variables = {
    metafields: [
      {
        ownerId: `gid://shopify/Product/${productId}`,
        key: "category",
        namespace: "custom",
        type: "single_line_text_field",
        value: categoryValue,
      },
    ],
  };

  await axios.post(
    SHOPIFY_GRAPHQL_URL,
    { query: mutation, variables },
    {
      headers: {
        "X-Shopify-Access-Token": process.env.SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json",
      },
    }
  );
}
/* ======================================================
   SHOPIFY — WRITE BRAND INTO VENDOR FIELD
====================================================== */
async function updateShopifyVendor(productId, brandValue) {
  const mutation = `
    mutation UpdateProductVendor($input: ProductInput!) {
      productUpdate(input: $input) {
        product {
          id
          vendor
        }
        userErrors {
          field
          message
        }
      }
    }
  `;

  const variables = {
    input: {
      id: `gid://shopify/Product/${productId}`,
      vendor: brandValue || "Unknown",
    },
  };

  await axios.post(
    SHOPIFY_GRAPHQL_URL,
    { query: mutation, variables },
    {
      headers: {
        "X-Shopify-Access-Token": process.env.SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json",
      }
    }
  );
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
   MAP CATEGORY TO SHOPIFY METAFIELD VALUES
   (Shopify dropdown requires exact matches)
====================================================== */
function mapCategoryForShopify(category) {
  const map = {
    Handbags: "Handbags",
    Totes: "Totes",
    Crossbody: "Crossbody",
    Wallets: "Wallets",
    Backpacks: "Backpacks",
    Luggage: "Luggage",
    Scarves: "Scarves",
    Belts: "Belts",
    Accessories: "Accessories",
    "Small Bags": "Small Bags",

    // EVERYTHING ELSE must map to "Other " WITH TRAILING SPACE
    default: "Other ",
  };

  return map[category] || map.default;
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
    const url =
      lastId === 0
        ? `https://${store}.myshopify.com/admin/api/2024-01/products.json?limit=250`
        : `https://${store}.myshopify.com/admin/api/2024-01/products.json?limit=250&since_id=${lastId}`;

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

  return allProducts;
}

/* ======================================================
   WEBFLOW — STRONG MATCHER (ID / URL / SLUG)
   Logs EVERYTHING and returns FIRST match only
====================================================== */
async function findExistingWebflowItem(shopifyProductId, shopifyUrl, slug) {
  const shopifyUrlNorm = shopifyUrl ? String(shopifyUrl).trim() : null;
  const slugNorm = slug ? String(slug).trim() : null;

  console.log("\n=======================================");
  console.log("🔍 START MATCH SCAN FOR SHOPIFY PRODUCT");
  console.log("shopifyProductId =", shopifyProductId);
  console.log("shopifyUrl       =", shopifyUrlNorm);
  console.log("slug             =", slugNorm);
  console.log("=======================================\n");

  let offset = 0;
  const limit = 100;

  while (true) {
    const url = `https://api.webflow.com/v2/collections/${process.env.WEBFLOW_COLLECTION_ID}/items?limit=${limit}&offset=${offset}`;

    let response;
    try {
      response = await axios.get(url, {
        headers: {
          Authorization: `Bearer ${process.env.WEBFLOW_TOKEN}`,
          accept: "application/json",
        },
      });
    } catch (err) {
      console.error(
        "❌ Webflow scan error (offset",
        offset,
        ")",
        err.response?.data || err.toString()
      );
      return null;
    }

    const items = response.data.items || [];
    console.log(`📄 Scanning Webflow offset ${offset} (${items.length} items)`);

    for (const item of items) {
      const fd = item.fieldData || {};

      const wfIdRaw = fd["shopify-product-id"] || null;
      const wfUrlRaw = fd["shopify-url"] || null;
      const wfSlugRaw = fd["slug"] || null;

      const wfId = wfIdRaw ? String(wfIdRaw) : null;
      const wfUrl = wfUrlRaw ? String(wfUrlRaw).trim() : null;
      const wfSlug = wfSlugRaw ? String(wfSlugRaw).trim() : null;

      const idMatch = wfId && String(wfId) === String(shopifyProductId);
      const urlMatch = wfUrl && shopifyUrlNorm && wfUrl === shopifyUrlNorm;
      const slugMatch = wfSlug && slugNorm && wfSlug === slugNorm;

      console.log(
        `🔎 CHECK: shopifyProductId=${shopifyProductId}, webflowShopifyId=${wfId || "null"}, webflowItemId=${
          item.id
        }, shopifyUrl=${shopifyUrlNorm || "null"}, webflowShopifyUrl=${
          wfUrl || "null"
        }, slug=${slugNorm || "null"}, webflowSlug=${wfSlug || "null"}, idMatch=${
          idMatch
        }, urlMatch=${urlMatch}, slugMatch=${slugMatch}`
      );

      if (idMatch || urlMatch || slugMatch) {
        console.log(`🎯 MATCH FOUND → Webflow itemId=${item.id}`);
        return item; // Return the first match
      }
    }

    // Stop when no more items
    if (items.length < limit) break;

    offset += limit; // Move to next batch of 100
  }

  console.log("❌ NO MATCH FOUND IN WEBFLOW FOR shopifyProductId =", shopifyProductId);
  return null;
}

/* ======================================================
   MARK AS SOLD (once)
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
}

/* ======================================================
   ⭐ CORE SYNC LOGIC — NO DUPLICATES EVER ⭐
====================================================== */
async function syncSingleProduct(product, cache) {
  const shopifyProductId = String(product.id);
  const cacheEntry = getCacheEntry(cache, shopifyProductId);

  const previousQty = cacheEntry?.lastQty ?? null;

  let name = product.title;
  let description = product.body_html;
  let price = product.variants?.[0]?.price || null;
  let qty = product.variants?.[0]?.inventory_quantity ?? null;
  let slug = product.handle;

  let detectedBrand = detectBrandFromProduct(product.title, product.vendor);
  if (!detectedBrand) detectedBrand = product.vendor || null;
  const brand = detectedBrand;

  const allImages = (product.images || []).map((img) => img.src);
  const featuredImage = allImages[0] || null;
  const gallery = allImages.slice(1);

  const soldNow = qty !== null && qty <= 0;

  // Detect category for Webflow
const detectedCategory = detectCategory(name);

// Webflow category uses detected category (free text)
let category = detectedCategory;

// Shopify category must match dropdown EXACTLY
const shopifyCategory = mapCategoryForShopify(detectedCategory);

let showOnWebflow = !soldNow;
if (soldNow) category = "Recently Sold";


  const shopifyUrl = `https://${process.env.SHOPIFY_STORE}.myshopify.com/products/${slug}`;

  // 🔎 PRE-OP LOGGING FOR THIS PRODUCT
  console.log("\n=======================================");
  console.log("🧾 SYNC PRODUCT");
  console.log("shopifyProductId          =", shopifyProductId);
  console.log("cache.webflowId           =", cacheEntry?.webflowId || "null");
  console.log("previousQty               =", previousQty);
  console.log("currentQty                =", qty);
  console.log("category                  =", category);
  console.log("soldNow                   =", soldNow);
  console.log("shopifyUrl                =", shopifyUrl);
  console.log("slug                      =", slug);
  console.log("=======================================\n");

  // Write category back to Shopify metafield custom.category
  await updateShopifyCategoryMetafield(shopifyProductId, shopifyCategory);

  // Write brand back to Shopify vendor field
  await updateShopifyVendor(shopifyProductId, brand);
    const currentHash = shopifyHash(product);
  /* ======================================================
     🔍 FIND EXISTING IN WEBFLOW (cache → scan)
  ======================================================= */

  let existing = null;

  // 1. Fast lookup using cached Webflow ID
  if (cacheEntry?.webflowId) {
    console.log("⚡ Trying cache.webflowId =", cacheEntry.webflowId);
    existing = await getWebflowItemById(cacheEntry.webflowId);
    if (!existing) {
      console.log("⚠️ Cached Webflow ID not found, falling back to scan.");
    }
  }

  // 2. Fallback matcher (ID / URL / slug)
  if (!existing) {
    existing = await findExistingWebflowItem(shopifyProductId, shopifyUrl, slug);
  }

  /* ======================================================
     🔒 HARD RULE:
     If a Webflow item exists, we NEVER create a new one.
  ======================================================= */

  if (existing) {
    console.log("✅ EXISTING WEBFLOW ITEM LINKED:", existing.id);

    // If newly sold
    const newlySold =
      (previousQty === null || previousQty > 0) && qty !== null && qty <= 0;

    if (newlySold) {
      console.log("🟠 Newly sold, marking as Recently Sold in Webflow.");
      await markAsSold(existing);

      cache[shopifyProductId] = {
        hash: currentHash,
        webflowId: existing.id,
        lastQty: qty,
      };

      return { operation: "sold", id: existing.id };
    }

    // UPDATE ONLY IF HASH CHANGED
    const previousHash = cacheEntry?.hash || null;
    const changed =
      !previousHash ||
      JSON.stringify(currentHash) !== JSON.stringify(previousHash);

    if (changed) {
      console.log("✏️ Changes detected, updating Webflow item:", existing.id);
      await axios.patch(
        `https://api.webflow.com/v2/collections/${process.env.WEBFLOW_COLLECTION_ID}/items/${existing.id}`,
        {
          fieldData: {
            name,
            brand,
            price,
            description,
            "shopify-product-id": shopifyProductId,
            "shopify-url": shopifyUrl,
            category,
            "featured-image": featuredImage ? { url: featuredImage } : null,
            "image-1": gallery[0] ? { url: gallery[0] } : null,
            "image-2": gallery[1] ? { url: gallery[1] } : null,
            "image-3": gallery[2] ? { url: gallery[2] } : null,
            "image-4": gallery[3] ? { url: gallery[3] } : null,
            "image-5": gallery[4] ? { url: gallery[4] } : null,
            "show-on-webflow": showOnWebflow,
            slug: existing.fieldData.slug, // preserve existing slug
          },
        },
        {
          headers: {
            Authorization: `Bearer ${process.env.WEBFLOW_TOKEN}`,
            "Content-Type": "application/json",
          },
        }
      );

      cache[shopifyProductId] = {
        hash: currentHash,
        webflowId: existing.id,
        lastQty: qty,
      };

      return { operation: "update", id: existing.id };
    }

    console.log("⏭️ No changes detected, skipping update for Webflow item:", existing.id);

    // SKIP if no changes
    cache[shopifyProductId] = {
      hash: currentHash,
      webflowId: existing.id,
      lastQty: qty,
    };
    return { operation: "skip", id: existing.id };
  }

  /* ======================================================
     NO EXISTING ITEM FOUND IN WEBFLOW
     → Creation allowed ONLY if NO CACHE ENTRY exists.
  ======================================================= */

  if (!cacheEntry) {
    console.log("🆕 NO CACHE + NO MATCH → Creating new Webflow item.");

    const resp = await axios.post(
      `https://api.webflow.com/v2/collections/${process.env.WEBFLOW_COLLECTION_ID}/items`,
      {
        fieldData: {
          name,
          brand,
          price,
          description,
          "shopify-product-id": shopifyProductId,
          "shopify-url": shopifyUrl,
          category,
          "featured-image": featuredImage ? { url: featuredImage } : null,
          "image-1": gallery[0] ? { url: gallery[0] } : null,
          "image-2": gallery[1] ? { url: gallery[1] } : null,
          "image-3": gallery[2] ? { url: gallery[2] } : null,
          "image-4": gallery[3] ? { url: gallery[3] } : null,
          "image-5": gallery[4] ? { url: gallery[4] } : null,
          "show-on-webflow": showOnWebflow,
          slug,
        },
      },
      {
        headers: {
          Authorization: `Bearer ${process.env.WEBFLOW_TOKEN}`,
          "Content-Type": "application/json",
        },
      }
    );

    const newId = resp.data.id;
    console.log("✅ CREATED NEW WEBFLOW ITEM:", newId);

    cache[shopifyProductId] = {
      hash: currentHash,
      webflowId: newId,
      lastQty: qty,
    };

    return { operation: "create", id: newId };
  }

  // CACHE EXISTS BUT WEBFLOW MISSING → NEVER CREATE (duplicate protection)
  console.log(
    "🚫 CACHE EXISTS BUT NO WEBFLOW MATCH FOUND → NOT CREATING (skip-missing-webflow)"
  );
  return { operation: "skip-missing-webflow", id: null };
}

/* ======================================================
   ROUTES
====================================================== */
app.get("/", (req, res) => {
  res.send(
    "Lost & Found — Clean Sync Server (No Duplicates, Sold Logic Fixed, Deep Scan Matcher + Logging)"
  );
});

app.post("/sync-all", async (req, res) => {
  try {
    const products = await fetchAllShopifyProducts();
    const cache = loadCache();

    let created = 0,
      updated = 0,
      skipped = 0,
      sold = 0;

    // detect disappeared Shopify items (Option A behaviour)
    const previousIds = Object.keys(cache);
    const currentIds = products.map((p) => String(p.id));
    const disappeared = previousIds.filter((id) => !currentIds.includes(id));

    console.log("\n=======================================");
    console.log("🧹 CHECKING DISAPPEARED SHOPIFY PRODUCTS");
    console.log("previousIds:", previousIds.length);
    console.log("currentIds :", currentIds.length);
    console.log("disappeared:", disappeared.length);
    console.log("=======================================\n");

    for (const goneId of disappeared) {
      const entry = getCacheEntry(cache, goneId);
      let existing = null;

      console.log(
        `🕳️ DISAPPEARED: shopifyProductId=${goneId}, cache.webflowId=${entry?.webflowId || "null"}`
      );

      if (entry?.webflowId) {
        existing = await getWebflowItemById(entry.webflowId);
      }
      if (!existing) {
        // we don't know slug/url here → pass nulls, matcher will still log
        existing = await findExistingWebflowItem(goneId, null, null);
      }

      if (existing) {
        console.log("🟠 Marking disappeared product as Recently Sold in Webflow:", existing.id);
        await markAsSold(existing);
        sold++;
      } else {
        console.log("⚪ No Webflow item found for disappeared product", goneId);
      }

      delete cache[goneId];
    }

    for (const product of products) {
      const result = await syncSingleProduct(product, cache);

      if (result.operation === "create") created++;
      else if (result.operation === "update") updated++;
      else if (result.operation === "sold") sold++;
      else skipped++;
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
    console.error("❌ sync-all error:", err.toString());
    res.status(500).json({ error: err.toString() });
  }
});

/* ======================================================
   SERVER
====================================================== */
const PORT = process.env.PORT || 4000;
app.listen(PORT, () => {
  console.log(`🔥 Sync server running on ${PORT}`);
});



