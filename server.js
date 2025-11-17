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
   SANITIZATION HELPERS
====================================================== */
function sanitizeText(input) {
  if (!input) return "";
  return input
    .normalize("NFKD")                 // split accents into ASCII base letters
    .replace(/[^\x00-\x7F]/g, "")      // remove all non-ASCII characters
    .replace(/–|—/g, "-")              // normalize dashes
    .replace(/\//g, " and ")           // replace slashes
    .replace(/&/g, "and")              // replace ampersands
    .replace(/\s+/g, " ")              // collapse spaces
    .trim();
}

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

function changedSinceLastSync(product, cache) {
  const id = String(product.id);
  const previous = cache[id];
  const current = shopifyHash(product);
  if (!previous) return true;
  return JSON.stringify(previous) !== JSON.stringify(current);
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

  if (!store || !token) throw new Error("Missing SHOPIFY_STORE or SHOPIFY_ACCESS_TOKEN");

  let allProducts = [];
  let lastId = 0;

  while (true) {
    const baseUrl = `https://${store}.myshopify.com/admin/api/2024-01/products.json`;
    const url =
      lastId === 0 ? `${baseUrl}?limit=250` : `${baseUrl}?limit=250&since_id=${lastId}`;

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
   WEBFLOW — FIND EXISTING ITEM
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
   CORE SYNC LOGIC
====================================================== */
async function syncSingleProduct(product, cache) {
  const shopifyProductId = product.id;
  const idStr = String(shopifyProductId);

  // ✅ sanitize the only two fields Webflow chokes on
  const rawBrand = detectBrandFromProduct(product.title, product.vendor);
  const brand = sanitizeText(rawBrand || product.vendor);

  const name = sanitizeText(product.title);

  // ❌ leave description untouched (HTML preserved)
  const description = product.body_html;

  const price = product.variants?.[0]?.price || null;
  const slug = product.handle;
  const shopifyUrl = `https://${process.env.SHOPIFY_STORE}.myshopify.com/products/${slug}`;

  /* ======================================================
     UPDATE SHOPIFY VENDOR (raw, not sanitized)
  ======================================================= */
  if (rawBrand && rawBrand !== product.vendor) {
    try {
      await axios.put(
        `https://${process.env.SHOPIFY_STORE}.myshopify.com/admin/api/2024-01/products/${product.id}.json`,
        {
          product: { id: product.id, vendor: rawBrand },
        },
        {
          headers: {
            "X-Shopify-Access-Token": process.env.SHOPIFY_ACCESS_TOKEN,
            "Content-Type": "application/json",
          },
        }
      );
      console.log(`🏷️ Vendor updated → ${rawBrand}`);
    } catch (err) {
      console.error("⚠️ Failed to update Shopify vendor:", err.response?.data || err.toString());
    }
  }

  /* ======================================================
     IMAGES
  ======================================================= */
  const allImages = (product.images || []).map((i) => i.src);
  const featuredImage = product.image?.src || allImages[0] || null;
  const gallery = allImages.filter((u) => u !== featuredImage);

  /* ======================================================
     SOLD LOGIC
  ======================================================= */
  const variant = product.variants?.[0];
  const qty = typeof variant?.inventory_quantity === "number" ? variant.inventory_quantity : null;

  const soldByTitle =
    name.toLowerCase().includes("sold") || name.toLowerCase().includes("reserved");
  const soldByInventory = qty !== null && qty <= 0;

  const recentlySold = soldByTitle || soldByInventory;

  let category = detectCategory(name);
  let showOnWebflow = !recentlySold;
  if (recentlySold) category = "Recently Sold";

  console.log(
    `🔁 Product ${shopifyProductId} | "${name}" | brand=${brand} | qty=${qty} | sold=${recentlySold}`
  );

  /* ======================================================
     UPDATE SHOPIFY CATEGORY METAFIELD
  ======================================================= */
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
    console.log(`🛍️ Category updated → ${category}`);
  } catch (err) {
    console.error("⚠️ Failed to update category:", err.response?.data || err.toString());
  }

  /* ======================================================
     WEBFLOW PAYLOAD
  ======================================================= */
  const fieldDataBase = {
    name,
    brand,
    price,
    description,
    category,
    "shopify-product-id": idStr,
    "shopify-url": shopifyUrl,

    "featured-image": featuredImage ? { url: featuredImage } : null,
    "image-1": gallery[0] ? { url: gallery[0] } : null,
    "image-2": gallery[1] ? { url: gallery[1] } : null,
    "image-3": gallery[2] ? { url: gallery[2] } : null,
    "image-4": gallery[3] ? { url: gallery[3] } : null,
    "image-5": gallery[4] ? { url: gallery[4] } : null,

    "show-on-webflow": showOnWebflow,
    "featured-item-on-homepage": false,
  };

  /* ======================================================
     CREATE OR UPDATE WEBFLOW ITEM
  ======================================================= */
  const existing = await findExistingWebflowItem(shopifyProductId);
  const currentHash = shopifyHash(product);

  if (!existing) {
    console.log("🆕 Creating Webflow item…");

    const createPayload = { ...fieldDataBase, slug };

    const createResp = await axios.post(
      `https://api.webflow.com/v2/collections/${process.env.WEBFLOW_COLLECTION_ID}/items`,
      { fieldData: createPayload },
      {
        headers: {
          Authorization: `Bearer ${process.env.WEBFLOW_TOKEN}`,
          "Content-Type": "application/json",
        },
      }
    );

    cache[idStr] = currentHash;
    return { operation: "create", id: createResp.data.id };
  }

  const previousHash = cache[idStr];
  const hasChanged = !previousHash || JSON.stringify(previousHash) !== JSON.stringify(currentHash);

  if (!hasChanged) {
    console.log(`⏩ SKIPPED → ${shopifyProductId} (no change)`);
    return { operation: "skip", id: existing.id };
  }

  console.log(`✏️ Updating Webflow item: ${existing.id}`);

  const updatePayload = {
    ...fieldDataBase,
    slug: existing.fieldData.slug,
  };

  await axios.patch(
    `https://api.webflow.com/v2/collections/${process.env.WEBFLOW_COLLECTION_ID}/items/${existing.id}`,
    { fieldData: updatePayload },
    {
      headers: {
        Authorization: `Bearer ${process.env.WEBFLOW_TOKEN}`,
        "Content-Type": "application/json",
      },
    }
  );

  cache[idStr] = currentHash;
  return { operation: "update", id: existing.id };
}

/* ======================================================
   ROUTES
====================================================== */
app.get("/", (req, res) => {
  res.send("Lost & Found – Full Shopify → Webflow Sync (with normalized brands)");
});

app.post("/sync-all", async (req, res) => {
  try {
    console.log("🔄 FULL SYNC STARTED…");

    const products = await fetchAllShopifyProducts();
    const cache = loadCache();

    let created = 0;
    let updated = 0;
    let skipped = 0;

    for (const product of products) {
      try {
        const result = await syncSingleProduct(product, cache);
        if (result.operation === "create") created++;
        else if (result.operation === "update") updated++;
        else skipped++;
      } catch (err) {
        console.error("⚠️ Error syncing:", product.id, err.toString());
      }
    }

    saveCache(cache);

    console.log("✅ COMPLETE:", { total: products.length, created, updated, skipped });

    res.json({
      status: "ok",
      total: products.length,
      created,
      updated,
      skipped,
    });
  } catch (err) {
    console.error("🔥 /sync-all ERROR:", err);
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
