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

/* ======================================================
   SLUGIFY HELPER (for brands, NOT products)
====================================================== */
function slugify(str = "") {
  return str
    .toString()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
}

/* ======================================================
   LOAD WEBFLOW BRAND COLLECTION
====================================================== */
let brandReferenceMap = null;

async function loadBrandReferenceMap() {
  if (brandReferenceMap) return brandReferenceMap;

  const map = {};
  let page = 1;
  const BRAND_COLLECTION_ID = "6923887d5ff23fcb91ef9ef1";

  while (true) {
    const url = `https://api.webflow.com/v2/collections/${BRAND_COLLECTION_ID}/items?page=${page}&limit=100`;

    const resp = await axios.get(url, {
      headers: {
        Authorization: `Bearer ${process.env.WEBFLOW_TOKEN}`,
        accept: "application/json"
      }
    });

    const items = resp.data.items || [];

    for (const item of items) {
      const slug = item.fieldData.slug;
      map[slug] = item.id;
    }

    if (!resp.data.pagination?.nextPage) break;
    page = resp.data.pagination.nextPage;
  }

  console.log("🟢 Loaded Brand Reference Map:", Object.keys(map).length);
  brandReferenceMap = map;
  return map;
}

/* ======================================================
   WEBFLOW — FIND EXISTING ITEM
   Primary: match by Shopify Product ID
   Secondary: match by slug = Shopify handle
====================================================== */
async function findExistingWebflowItem(shopifyProductId, productSlug) {
  const idStr = String(shopifyProductId);
  let matchById = null;
  let matchBySlug = null;

  let page = 1;
  while (true) {
    const url = `https://api.webflow.com/v2/collections/${process.env.WEBFLOW_COLLECTION_ID}/items?page=${page}&limit=100`;

    const response = await axios.get(url, {
      headers: {
        Authorization: `Bearer ${process.env.WEBFLOW_TOKEN}`,
        accept: "application/json"
      }
    });

    const items = response.data.items || [];

    for (const item of items) {
      const fd = item.fieldData || {};

      // Primary: match by Shopify Product ID
      if (fd["shopify-product-id"] === idStr) {
        matchById = item;
        break;
      }

      // Secondary: match by slug (handle) if we don't have an ID match yet
      if (!matchById && productSlug && fd.slug === productSlug && !matchBySlug) {
        matchBySlug = item;
      }
    }

    if (matchById) break;

    if (!response.data.pagination?.nextPage) break;
    page = response.data.pagination.nextPage;
  }

  if (matchById) {
    return matchById;
  }

  if (matchBySlug) {
    console.log(
      `🧷 Fallback matched by slug "${productSlug}" for Shopify product ${idStr}`
    );
    return matchBySlug;
  }

  return null;
}

/* ======================================================
   SHOPIFY — AUTO PUBLISH TO SALES CHANNELS
====================================================== */

const SHOPIFY_GRAPHQL_URL = `https://${process.env.SHOPIFY_STORE}.myshopify.com/admin/api/2024-01/graphql.json`;

let cachedPublicationIds = null;

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
        "Content-Type": "application/json"
      }
    }
  );

  const all = resp.data?.data?.publications?.edges || [];

  const publishTargets = all.filter(edge => {
    const n = edge.node.name.toLowerCase();
    return (
      n.includes("online store") ||
      n.includes("facebook") ||
      n.includes("instagram") ||
      n.includes("buy button") ||
      n.includes("shop")
    );
  });

  const ids = publishTargets.map(e => e.node.id);
  cachedPublicationIds = ids;

  console.log("🟢 Loaded publication IDs:", publishTargets.map(p => p.node.name));

  return ids;
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
            "Content-Type": "application/json"
          }
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
    slug: product.handle
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
        "Content-Type": "application/json"
      }
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
   ⭐ MARK AS SOLD
====================================================== */
async function markAsSold(existing) {
  if (!existing) return;

  const soldPayload = {
    fieldData: {
      ...existing.fieldData,
      category: "Recently Sold",
      "show-on-webflow": false
    }
  };

  await axios.patch(
    `https://api.webflow.com/v2/collections/${process.env.WEBFLOW_COLLECTION_ID}/items/${existing.id}`,
    soldPayload,
    {
      headers: {
        Authorization: `Bearer ${process.env.WEBFLOW_TOKEN}`,
        "Content-Type": "application/json"
      }
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

  const name = product.title;
  const description = product.body_html;
  const price = product.variants?.[0]?.price || null;

  // Use Shopify handle as THE slug for the Webflow item
  const productSlug = product.handle;
  const shopifyUrl = `https://${process.env.SHOPIFY_STORE}.myshopify.com/products/${productSlug}`;

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
            "Content-Type": "application/json"
          }
        }
      );
      console.log(`🏷️ Shopify vendor updated → ${detectedBrand}`);
    } catch {}
  }

  const brand = detectedBrand;

  /* BRAND LINK MAPPING */
  const brandMap = await loadBrandReferenceMap();
  const brandSlug = brand ? slugify(brand) : null;
  const brandLinkId = brandSlug ? brandMap[brandSlug] : null;

  /* IMAGES */
  const allImages = (product.images || []).map((img) => img.src);
  const featuredImage = product.image?.src || allImages[0] || null;
  const gallery = allImages.filter((url) => url !== featuredImage);

  /* SOLD LOGIC */
  const variant = product.variants?.[0];
  const qty =
    typeof variant?.inventory_quantity === "number"
      ? variant.inventory_quantity
      : null;

  const soldByInventory = qty !== null && qty <= 0;
  const normalizedTitle = (name || "").toLowerCase();
  const soldByTitle =
    normalizedTitle.includes("sold") ||
    normalizedTitle.includes("reserved");
  const recentlySold = soldByInventory || soldByTitle;

  let category = detectCategory(name);
  let showOnWebflow = !recentlySold;
  if (recentlySold) category = "Recently Sold";

  console.log(
    `🔁 Product ${shopifyProductId} | "${name}" | brand=${brand} | link=${brandLinkId} | qty=${qty} | sold=${recentlySold}`
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
              value: category
            }
          ]
        }
      },
      {
        headers: {
          "X-Shopify-Access-Token": process.env.SHOPIFY_ACCESS_TOKEN,
          "Content-Type": "application/json"
        }
      }
    );
  } catch {}

  /* AUTO PUBLISH */
  try {
    await publishToSalesChannels(product.id);
  } catch (err) {
    console.error("⚠️ Failed to publish:", err.toString());
  }

  /* BASE WEBFLOW PAYLOAD */
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
    "brand-link": brandLinkId || null
  };

  // 🔍 Find existing by product ID, then by slug/handle
  const existing = await findExistingWebflowItem(shopifyProductId, productSlug);
  const currentHash = shopifyHash(product);

  /* SOLD → UPDATE */
  if (recentlySold && existing) {
    await markAsSold(existing);
    cache[idStr] = currentHash;
    return { operation: "sold", id: existing.id };
  }

  /* CREATE IF NOT FOUND (USE HANDLE AS SLUG) */
  if (!existing) {
    const createPayload = { ...fieldDataBase, slug: productSlug };
    const resp = await axios.post(
      `https://api.webflow.com/v2/collections/${process.env.WEBFLOW_COLLECTION_ID}/items`,
      { fieldData: createPayload },
      {
        headers: {
          Authorization: `Bearer ${process.env.WEBFLOW_TOKEN}`,
          "Content-Type": "application/json"
        }
      }
    );

    cache[idStr] = currentHash;
    console.log(`🆕 Created Webflow item for Shopify product ${idStr}`);
    return { operation: "create", id: resp.data.id };
  }

  /* UPDATE IF CHANGED */
  const previousHash = cache[idStr];
  const hasChanged =
    !previousHash ||
    JSON.stringify(previousHash) !== JSON.stringify(currentHash);

  if (!hasChanged) {
    return { operation: "skip", id: existing.id };
  }

  await axios.patch(
    `https://api.webflow.com/v2/collections/${process.env.WEBFLOW_COLLECTION_ID}/items/${existing.id}`,
    {
      fieldData: {
        ...fieldDataBase,
        // 🔒 Preserve existing slug so Webflow doesn't treat it as a new item
        slug: existing.fieldData.slug
      }
    },
    {
      headers: {
        Authorization: `Bearer ${process.env.WEBFLOW_TOKEN}`,
        "Content-Type": "application/json"
      }
    }
  );

  cache[idStr] = currentHash;
  console.log(`✏️ Updated Webflow item ${existing.id} for Shopify product ${idStr}`);
  return { operation: "update", id: existing.id };
}

/* ======================================================
   ROUTES
====================================================== */
app.get("/", (req, res) => {
  res.send(
    "Lost & Found – Full Shopify → Webflow Sync (Brand Linked, Clean, Duplicate-Proof, Slug-Fallback)"
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

    /* DETECT DISAPPEARED SHOPIFY ITEMS */
    const previousIds = Object.keys(cache);
    const currentIds = products.map((p) => String(p.id));

    const disappeared = previousIds.filter(
      (id) => !currentIds.includes(id)
    );

    for (const goneId of disappeared) {
      const existing = await findExistingWebflowItem(goneId, null);
      if (existing) {
        await markAsSold(existing);
        sold++;
      }
      delete cache[goneId];
    }

    /* PROCESS PRODUCTS */
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
      sold
    });
  } catch (err) {
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
