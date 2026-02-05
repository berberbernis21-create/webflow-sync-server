import express from "express";
import axios from "axios";
import cors from "cors";
import dotenv from "dotenv";
import fs from "fs";
import { CATEGORY_KEYWORDS } from "./categoryKeywords.js";
import { CATEGORY_KEYWORDS_FURNITURE } from "./categoryKeywordsFurniture.js";
import { detectBrandFromProduct } from "./brand.js";
import { detectVertical } from "./vertical.js";

dotenv.config();

/* ======================================================
   [WEBFLOW] CENTRALIZED LOGGING — Render stdout, no external deps
====================================================== */
const WEBFLOW_LOG_PREFIX = "[WEBFLOW]";
function webflowLog(level, payload) {
  const msg = typeof payload === "object" ? `${WEBFLOW_LOG_PREFIX} ${JSON.stringify(payload)}` : `${WEBFLOW_LOG_PREFIX} ${payload}`;
  if (level === "warn") console.warn(msg);
  else if (level === "error") console.error(msg);
  else console.log(msg);
}

function webflowRequestLog(method, url, body) {
  webflowLog("info", { event: "request", method, url, body: body ?? null });
}

function webflowFailureLog(method, url, status, responseBody, requestBody) {
  webflowLog("error", {
    event: "failure",
    method,
    url,
    status,
    responseBody: responseBody ?? null,
    requestBody: requestBody ?? null,
  });
}

// Axios interceptors: only for api.webflow.com
const WEBFLOW_ORIGIN = "https://api.webflow.com";
axios.interceptors.request.use((config) => {
  if (config.url && String(config.url).startsWith(WEBFLOW_ORIGIN)) {
    webflowRequestLog(config.method?.toUpperCase() ?? "GET", config.url, config.data);
  }
  return config;
});
axios.interceptors.response.use(
  (response) => response,
  (err) => {
    const url = err.config?.url;
    if (url && String(url).startsWith(WEBFLOW_ORIGIN)) {
      webflowFailureLog(
        err.config?.method?.toUpperCase() ?? "?",
        url,
        err.response?.status,
        err.response?.data,
        err.config?.data
      );
    }
    return Promise.reject(err);
  }
);

/* ======================================================
   DUAL-PIPELINE: WEBFLOW ENV CONFIG
   Luxury = existing. Furniture = new env vars.
====================================================== */
function getWebflowConfig(vertical) {
  if (vertical === "furniture") {
    return {
      collectionId: process.env.RESALE_Products_Collection_ID,
      skuCollectionId: process.env.RESALE_SKUs_Collection_ID,
      token: process.env.RESALE_TOKEN,
      siteId: process.env.RESALE_WEBFLOW_SITE_ID,
    };
  }
  return {
    collectionId: process.env.WEBFLOW_COLLECTION_ID,
    skuCollectionId: null,
    token: process.env.WEBFLOW_TOKEN,
    siteId: null,
  };
}

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
 * Normalize old cache entries (legacy hash-only or without vertical)
 */
function getCacheEntry(cache, idStr) {
  const entry = cache[idStr];
  if (!entry) return null;

  if (typeof entry === "object" && entry.hash) {
    return { ...entry, vertical: entry.vertical ?? "luxury" };
  }

  return { hash: entry, webflowId: null, lastQty: null, vertical: "luxury" };
}

/* ======================================================
   WEBFLOW DIRECT LOOKUP (parameterized by collection)
   CMS only. For ecommerce (Furniture) use getWebflowEcommerceProductById.
====================================================== */
async function getWebflowItemById(itemId, config) {
  if (!itemId || !config?.collectionId || !config?.token) return null;
  try {
    const url = `https://api.webflow.com/v2/collections/${config.collectionId}/items/${itemId}`;
    const response = await axios.get(url, {
      headers: {
        Authorization: `Bearer ${config.token}`,
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
   WEBFLOW ECOMMERCE API (Furniture — uses site_id, not collection_id)
   Required: ecommerce:write scope. Products collection is ecommerce.
====================================================== */
async function getWebflowEcommerceProductById(siteId, productId, token) {
  if (!siteId || !productId || !token) return null;
  try {
    const url = `https://api.webflow.com/v2/sites/${siteId}/products/${productId}`;
    const response = await axios.get(url, {
      headers: { Authorization: `Bearer ${token}`, accept: "application/json" },
    });
    const data = response.data;
    // API returns { product, skus }; normalize so caller always has .id and .skus
    const product = data.product ?? data;
    const skus = data.skus ?? product.skus ?? [];
    return { ...product, id: product.id ?? productId, skus };
  } catch (err) {
    if (err.response?.status === 404) return null;
    console.error("⚠️ getWebflowEcommerceProductById error:", err.message, err.response?.data);
    return null;
  }
}

async function findExistingWebflowEcommerceProduct(shopifyProductId, slug, config) {
  if (!config?.siteId || !config?.token) return null;
  let offset = 0;
  const limit = 100;
  const slugNorm = slug ? String(slug).trim() : null;
  while (true) {
    const url = `https://api.webflow.com/v2/sites/${config.siteId}/products?limit=${limit}&offset=${offset}`;
    let response;
    try {
      response = await axios.get(url, {
        headers: { Authorization: `Bearer ${config.token}`, accept: "application/json" },
      });
    } catch (err) {
      console.error("❌ Webflow ecommerce list error:", err.response?.data || err.message);
      return null;
    }
    const raw = response.data.products ?? response.data.items ?? [];
    const listItems = Array.isArray(raw) ? raw : [];
    for (const listItem of listItems) {
      const product = listItem.product ?? listItem;
      const skus = listItem.skus ?? product.skus ?? [];
      const fd = product.fieldData || {};
      const wfId = fd["shopify-product-id"] ? String(fd["shopify-product-id"]) : null;
      const wfSlug = (fd["slug"] || fd["shopify-slug-2"]) ? String(fd.slug || fd["shopify-slug-2"]).trim() : null;
      if (wfId && String(wfId) === String(shopifyProductId)) return { ...product, skus };
      if (slugNorm && wfSlug && wfSlug === slugNorm) return { ...product, skus };
    }
    if (listItems.length < limit) break;
    offset += limit;
  }
  return null;
}

const WEBFLOW_ITEM_REF_REGEX = /^[a-f0-9]{24}$/i;

async function createWebflowEcommerceProduct(siteId, productFieldData, skuFieldData, token) {
  const url = `https://api.webflow.com/v2/sites/${siteId}/products`;
  const productData = { ...productFieldData };
  if ("category" in productData && (typeof productData.category !== "string" || !WEBFLOW_ITEM_REF_REGEX.test(productData.category))) {
    delete productData.category;
  }
  const payload = {
    product: { fieldData: productData },
    sku: { fieldData: skuFieldData },
    publishStatus: "staging",
  };
  const response = await axios.post(url, payload, {
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
  });
  return response.data;
}

async function updateWebflowEcommerceProduct(siteId, productId, fieldData, token, existingProduct = null) {
  const url = `https://api.webflow.com/v2/sites/${siteId}/products/${productId}`;
  const data = { ...fieldData };
  if ("category" in data && (typeof data.category !== "string" || !WEBFLOW_ITEM_REF_REGEX.test(data.category))) {
    delete data.category;
  }
  let skuFieldData = existingProduct?.skus?.[0]?.fieldData;
  if (skuFieldData == null || typeof skuFieldData !== "object") {
    webflowLog("info", { event: "product.patch.skuRefetch", productId, reason: "existingProduct had no skus or sku.fieldData" });
    const full = await getWebflowEcommerceProductById(siteId, productId, token);
    skuFieldData = full?.skus?.[0]?.fieldData ?? {};
  }
  const body = {
    product: { fieldData: data },
    sku: { fieldData: skuFieldData },
  };
  webflowLog("info", { event: "product.patch.calling", method: "PATCH", url, productId, bodyKeys: ["product", "sku"] });
  await axios.patch(url, body, {
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
  });
}

async function updateWebflowEcommerceSku(siteId, productId, skuId, fieldData, token) {
  if (!siteId || !productId || !skuId || !token) {
    webflowLog("warn", { event: "sku.patch.skipped", reason: "missing_params", siteId: !!siteId, productId: !!productId, skuId: !!skuId, token: !!token });
    return;
  }
  if (fieldData == null || typeof fieldData !== "object") {
    webflowLog("warn", { event: "sku.patch.skipped", reason: "invalid_fieldData", productId, skuId });
    return;
  }
  const url = `https://api.webflow.com/v2/sites/${siteId}/products/${productId}/skus/${skuId}`;
  const body = { sku: { fieldData } };
  webflowLog("info", { event: "sku.patch.calling", method: "PATCH", url, productId, skuId, bodyKeys: ["sku"] });
  await axios.patch(url, body, {
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
  });
}

/** Sync default SKU for ecommerce product (price, images, weight, dimensions). */
async function syncFurnitureEcommerceSku(product, webflowProductId, config) {
  webflowLog("info", { event: "syncFurnitureEcommerceSku.entry", shopifyProductId: product?.id, webflowProductId, hasSiteId: !!config?.siteId, hasToken: !!config?.token });
  if (!config?.siteId || !config?.token) {
    webflowLog("warn", { event: "syncFurnitureEcommerceSku.skipped", reason: "missing_config", webflowProductId });
    return;
  }
  const full = await getWebflowEcommerceProductById(config.siteId, webflowProductId, config.token);
  const skus = full?.skus ?? [];
  const defaultSku = skus[0];
  if (!defaultSku?.id) {
    webflowLog("warn", { event: "syncFurnitureEcommerceSku.skipped", reason: "no_default_sku", webflowProductId, skusCount: skus.length });
    console.warn("[Furniture] No default SKU found on product", webflowProductId);
    return;
  }
  const price = product.variants?.[0]?.price;
  const priceCents = price != null && price !== "" ? Math.round(parseFloat(price) * 100) : null;
  const dimensions = getDimensionsFromProduct(product);
  const allImages = (product.images || []).map((img) => img.src);
  const mainImageUrl = allImages[0] || null;
  const moreImagesUrls = allImages.slice(1);
  const fieldData = {
    price: priceCents != null ? { value: priceCents, unit: "USD" } : null,
    ...skuDimensionFields(dimensions),
    "main-image": mainImageUrl ? { url: mainImageUrl } : null,
    "more-images": moreImagesUrls.length > 0 ? moreImagesUrls.slice(0, 10).map((url) => (url ? { url } : null)).filter(Boolean) : null,
  };
  await updateWebflowEcommerceSku(config.siteId, webflowProductId, defaultSku.id, { ...defaultSku.fieldData, ...fieldData }, config.token);
  webflowLog("info", { event: "syncFurnitureEcommerceSku.exit", webflowProductId, skuId: defaultSku.id });
}

/** Archive an ecommerce product (soft-delete) so it no longer appears in the furniture store. */
async function archiveWebflowEcommerceProduct(siteId, productId, token) {
  if (!siteId || !productId || !token) return;
  const full = await getWebflowEcommerceProductById(siteId, productId, token);
  if (!full) return;
  const productFieldData = full.fieldData || {};
  const skuFieldData = full?.skus?.[0]?.fieldData ?? {};
  const url = `https://api.webflow.com/v2/sites/${siteId}/products/${productId}`;
  const body = {
    product: { fieldData: productFieldData, isArchived: true },
    sku: { fieldData: skuFieldData },
  };
  webflowLog("info", { event: "archive.ecommerce", productId, message: "Archiving mistaken furniture product" });
  await axios.patch(url, body, {
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
  });
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
   SHOPIFY — WRITE METAFIELDS (category, vertical, dimensions_status)
====================================================== */
async function updateShopifyMetafields(productId, { category, vertical, dimensionsStatus }) {
  const ownerId = `gid://shopify/Product/${productId}`;
  const metafields = [
    {
      ownerId,
      key: "category",
      namespace: "custom",
      type: "single_line_text_field",
      value: category ?? "",
    },
    {
      ownerId,
      key: "vertical",
      namespace: "custom",
      type: "single_line_text_field",
      value: vertical ?? "luxury",
    },
  ];
  if (dimensionsStatus != null) {
    metafields.push({
      ownerId,
      key: "dimensions_status",
      namespace: "custom",
      type: "single_line_text_field",
      value: dimensionsStatus,
    });
  }

  const mutation = `
    mutation MetafieldsSet($metafields: [MetafieldsSetInput!]!) {
      metafieldsSet(metafields: $metafields) {
        metafields { key value }
        userErrors { field message }
      }
    }
  `;
  await axios.post(
    SHOPIFY_GRAPHQL_URL,
    { query: mutation, variables: { metafields } },
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
   FURNITURE CATEGORY — detect + map to Shopify display
   Fallback: Accessories when no keyword matches.
   Uses word-boundary matching so "art" doesn't match "smart", "print" doesn't match "blueprint", "desk" doesn't match "desktop".
====================================================== */
function matchFurnitureKeyword(normalized, keyword) {
  const k = keyword.trim().toLowerCase();
  if (!k) return false;
  const escaped = k.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const pattern = "\\b" + escaped + "\\b";
  try {
    return new RegExp(pattern, "i").test(normalized);
  } catch {
    return normalized.includes(k);
  }
}

function detectCategoryFurniture(title) {
  if (!title) return "Accessories";
  const normalized = title.toLowerCase();
  for (const [category, keywords] of Object.entries(CATEGORY_KEYWORDS_FURNITURE)) {
    for (const kw of keywords) {
      if (matchFurnitureKeyword(normalized, kw)) return category;
    }
  }
  return "Accessories";
}

const FURNITURE_CATEGORY_TO_SHOPIFY = {
  LivingRoom: "Living Room",
  DiningRoom: "Dining Room",
  OfficeDen: "Office Den",
  Rugs: "Rugs",
  ArtMirrors: "Art / Mirrors",
  Bedroom: "Bedroom",
  Accessories: "Accessories",
  OutdoorPatio: "Outdoor / Patio",
  Lighting: "Lighting",
};

function mapFurnitureCategoryForShopify(category) {
  return FURNITURE_CATEGORY_TO_SHOPIFY[category] ?? "Accessories";
}

/** Ecommerce category must be an ItemRef (Webflow collection item ID). Return ID only if configured via env and looks like an ID. */
function resolveFurnitureCategoryRef(displayCategory) {
  if (!displayCategory || typeof displayCategory !== "string") return null;
  const key = displayCategory.replace(/\s*\/\s*/g, "_").replace(/\s+/g, "_").toUpperCase().replace(/[^A-Z0-9_]/g, "");
  const envKey = `FURNITURE_CATEGORY_${key}`;
  const id = process.env[envKey];
  const trimmed = id && String(id).trim();
  if (!trimmed) return null;
  if (!/^[a-f0-9]{24}$/i.test(trimmed)) return null;
  return trimmed;
}

/* ======================================================
   DIMENSIONS — extract from Shopify (Furniture)
   Native: weight, width, height, length on variant.
   Metafields: custom.width, custom.height, custom.length.
   If missing → dimensions_status = "missing".
====================================================== */
function getDimensionsFromProduct(product) {
  const v = product.variants?.[0];
  if (!v) return { weight: null, width: null, height: null, length: null };
  const weight = v.weight != null && v.weight > 0 ? Number(v.weight) : null;
  let width = null,
    height = null,
    length = null;
  const metafields = Array.isArray(product.metafields) ? product.metafields : [];
  for (const m of metafields) {
    if (m.namespace === "custom" && m.key === "width" && m.value) width = parseFloat(m.value);
    if (m.namespace === "custom" && m.key === "height" && m.value) height = parseFloat(m.value);
    if (m.namespace === "custom" && m.key === "length" && m.value) length = parseFloat(m.value);
  }
  return { weight, width, height, length };
}

function hasAnyDimensions(dims) {
  return (
    (dims.weight != null && dims.weight > 0) ||
    (dims.width != null && !Number.isNaN(dims.width)) ||
    (dims.height != null && !Number.isNaN(dims.height)) ||
    (dims.length != null && !Number.isNaN(dims.length))
  );
}

/** Format dimensions for description (incl. weight). Furniture & handbags: at start; other luxury: at end. */
function formatDimensionsForDescription(dims) {
  if (!dims || !hasAnyDimensions(dims)) return "";
  const parts = [];
  if (dims.width != null && !Number.isNaN(dims.width)) parts.push(`Width: ${dims.width}"`);
  if (dims.length != null && !Number.isNaN(dims.length)) parts.push(`Depth: ${dims.length}"`);
  if (dims.height != null && !Number.isNaN(dims.height)) parts.push(`Height: ${dims.height}"`);
  if (dims.weight != null && !Number.isNaN(dims.weight) && dims.weight > 0) parts.push(`Weight: ${dims.weight} lb`);
  return parts.length ? `Dimensions: ${parts.join(" × ")}.` : "";
}

/** Webflow SKU dimension fields must be numbers; omit keys when value is null/NaN. */
function skuDimensionFields(dimensions) {
  const d = dimensions || {};
  const out = {};
  if (d.weight != null && !Number.isNaN(d.weight) && d.weight > 0) out.weight = Number(d.weight);
  if (d.width != null && !Number.isNaN(d.width)) out.width = Number(d.width);
  if (d.height != null && !Number.isNaN(d.height)) out.height = Number(d.height);
  if (d.length != null && !Number.isNaN(d.length)) out.length = Number(d.length);
  return out;
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
   Uses config (collectionId + token) for target collection.
====================================================== */
async function findExistingWebflowItem(shopifyProductId, shopifyUrl, slug, config) {
  if (!config?.collectionId || !config?.token) return null;
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
    const url = `https://api.webflow.com/v2/collections/${config.collectionId}/items?limit=${limit}&offset=${offset}`;

    let response;
    try {
      response = await axios.get(url, {
        headers: {
          Authorization: `Bearer ${config.token}`,
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
      const wfSlug2Raw = fd["shopify-slug-2"] || null;
      const wfId = wfIdRaw ? String(wfIdRaw) : null;
      const wfUrl = wfUrlRaw ? String(wfUrlRaw).trim() : null;
      const wfSlug = (wfSlugRaw ? String(wfSlugRaw).trim() : null) || (wfSlug2Raw ? String(wfSlug2Raw).trim() : null);
      const idMatch = wfId && String(wfId) === String(shopifyProductId);
      const urlMatch = wfUrl && shopifyUrlNorm && wfUrl === shopifyUrlNorm;
      const slugMatch = wfSlug && slugNorm && wfSlug === slugNorm;

      console.log(
        `🔎 CHECK: shopifyProductId=${shopifyProductId}, webflowShopifyId=${wfId || "null"}, webflowItemId=${item.id}, idMatch=${idMatch}, urlMatch=${urlMatch}, slugMatch=${slugMatch}`
      );

      if (idMatch || urlMatch || slugMatch) {
        console.log(`🎯 MATCH FOUND → Webflow itemId=${item.id}`);
        return item;
      }
    }

    if (items.length < limit) break;
    offset += limit;
  }

  console.log("❌ NO MATCH FOUND IN WEBFLOW FOR shopifyProductId =", shopifyProductId);
  return null;
}

/* ======================================================
   FURNITURE SKU — find by product reference (required for PATCH)
====================================================== */
async function findExistingSkuByProductId(skuCollectionId, webflowProductId, token) {
  if (!skuCollectionId || !token || !webflowProductId) return null;
  let offset = 0;
  const limit = 100;
  while (true) {
    const url = `https://api.webflow.com/v2/collections/${skuCollectionId}/items?limit=${limit}&offset=${offset}`;
    let response;
    try {
      response = await axios.get(url, {
        headers: { Authorization: `Bearer ${token}`, accept: "application/json" },
      });
    } catch (err) {
      console.error("⚠️ SKU list error:", err.response?.data || err.message);
      return null;
    }
    const items = response.data.items || [];
    for (const item of items) {
      const productRef = item.fieldData?.product;
      if (productRef && String(productRef) === String(webflowProductId)) {
        return item;
      }
    }
    if (items.length < limit) break;
    offset += limit;
  }
  return null;
}

/* ======================================================
   FURNITURE SKU — sync default SKU (price, images, weight, dimensions)
   Products collection = metadata only. SKU = commerce + images.
   Every SKU PATCH/CREATE MUST include "product": "<WEBFLOW_PRODUCT_ID>".
====================================================== */
async function syncFurnitureSku(product, webflowProductId, config) {
  if (!config?.skuCollectionId || !config?.token || !webflowProductId) return;
  const price = product.variants?.[0]?.price;
  const priceDollars = price != null && price !== "" ? parseFloat(price) : null;
  const priceCents =
    priceDollars != null && !Number.isNaN(priceDollars) ? Math.round(priceDollars * 100) : null;
  const dimensions = getDimensionsFromProduct(product);
  const allImages = (product.images || []).map((img) => img.src);
  const mainImageUrl = allImages[0] || null;
  const moreImagesUrls = allImages.slice(1);

  const skuFieldData = {
    product: webflowProductId,
    price: priceCents != null ? { value: priceCents, unit: "USD" } : null,
    ...skuDimensionFields(dimensions),
    "main-image": mainImageUrl ? { url: mainImageUrl } : null,
    "more-images":
      moreImagesUrls.length > 0
        ? moreImagesUrls.slice(0, 10).map((url) => (url ? { url } : null)).filter(Boolean)
        : null,
  };

  const existingSku = await findExistingSkuByProductId(
    config.skuCollectionId,
    webflowProductId,
    config.token
  );

  const headers = {
    Authorization: `Bearer ${config.token}`,
    "Content-Type": "application/json",
  };

  if (existingSku) {
    console.log("✏️ PATCH Furniture SKU:", existingSku.id);
    const fd = existingSku.fieldData || {};
    await axios.patch(
      `https://api.webflow.com/v2/collections/${config.skuCollectionId}/items/${existingSku.id}`,
      {
        fieldData: {
          ...skuFieldData,
          slug: fd.slug ?? existingSku.slug,
        },
      },
      { headers }
    );
  } else {
    const name = product.title ? `${product.title} - Default` : "Default SKU";
    const slug = product.handle ? `${product.handle}-default-sku` : `sku-${webflowProductId}`;
    console.log("🆕 CREATE Furniture SKU for product:", webflowProductId);
    await axios.post(
      `https://api.webflow.com/v2/collections/${config.skuCollectionId}/items`,
      { fieldData: { ...skuFieldData, name, slug } },
      { headers }
    );
  }
}

/* ======================================================
   MARK AS SOLD — per vertical
   Luxury: CMS PATCH. Furniture: ecommerce PATCH (siteId).
====================================================== */
async function markAsSold(existing, vertical, config) {
  if (!existing || !config?.token) return;
  const base = { ...(existing.fieldData || {}) };
  const fieldData =
    vertical === "furniture"
      ? { ...base, sold: true }
      : { ...base, category: "Recently Sold", "show-on-webflow": false };

  if (vertical === "furniture" && config.siteId) {
    // Ecommerce PATCH requires { product: { fieldData }, sku: { fieldData } }; reuse shared updater
    await updateWebflowEcommerceProduct(config.siteId, existing.id, fieldData, config.token, existing);
    return;
  }
  await axios.patch(
    `https://api.webflow.com/v2/collections/${config.collectionId}/items/${existing.id}`,
    { fieldData },
    {
      headers: {
        Authorization: `Bearer ${config.token}`,
        "Content-Type": "application/json",
      },
    }
  );
}

/* ======================================================
   ⭐ CORE SYNC LOGIC — DUAL PIPELINE, NO DUPLICATES ⭐
====================================================== */
async function syncSingleProduct(product, cache) {
  const shopifyProductId = String(product.id);
  const cacheEntry = getCacheEntry(cache, shopifyProductId);

  // Vertical detection first (before any Webflow or category logic)
  const detectedVertical = detectVertical(product);
  // Prefer cache, but allow correction: if cache says furniture and we now detect luxury (e.g. backpack), use luxury
  const vertical =
    cacheEntry?.vertical === "furniture" && detectedVertical === "luxury"
      ? "luxury"
      : (cacheEntry?.vertical ?? detectedVertical);
  const verticalCorrected = cacheEntry?.vertical === "furniture" && detectedVertical === "luxury";
  webflowLog("info", {
    event: "vertical.resolved",
    shopifyProductId,
    detectedVertical,
    cacheVertical: cacheEntry?.vertical ?? null,
    vertical,
    corrected: verticalCorrected,
  });

  // When we correct furniture → luxury, remove the mistaken product from Webflow (archive) and clear cache so we create in luxury
  if (verticalCorrected && cacheEntry?.webflowId && vertical === "luxury") {
    const furnitureConfig = getWebflowConfig("furniture");
    if (furnitureConfig?.siteId && furnitureConfig?.token) {
      try {
        await archiveWebflowEcommerceProduct(furnitureConfig.siteId, cacheEntry.webflowId, furnitureConfig.token);
        webflowLog("info", { event: "vertical.corrected.archived", shopifyProductId, webflowId: cacheEntry.webflowId });
      } catch (err) {
        webflowLog("error", { event: "vertical.corrected.archive_failed", shopifyProductId, webflowId: cacheEntry.webflowId, message: err.message });
      }
    }
    delete cache[shopifyProductId];
    return syncSingleProduct(product, cache);
  }

  const config = getWebflowConfig(vertical);

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

  // Category and Shopify value by vertical
  let category;
  let shopifyCategoryValue;
  if (vertical === "furniture") {
    const detFurn = detectCategoryFurniture(name);
    category = mapFurnitureCategoryForShopify(detFurn);
    shopifyCategoryValue = category;
  } else {
    const detLux = detectCategory(name);
    category = soldNow ? "Recently Sold" : detLux;
    shopifyCategoryValue = mapCategoryForShopify(detLux);
  }

  const showOnWebflow = vertical === "luxury" ? !soldNow : true;
  const shopifyUrl = `https://${process.env.SHOPIFY_STORE}.myshopify.com/products/${slug}`;

  // Dimensions: furniture uses for status + SKU; luxury uses for description when present
  let dimensionsStatus = null;
  let dimensions = getDimensionsFromProduct(product);
  if (vertical === "furniture") {
    dimensionsStatus = hasAnyDimensions(dimensions) ? "present" : "missing";
  }
  if (hasAnyDimensions(dimensions)) {
    const dimStr = formatDimensionsForDescription(dimensions);
    if (dimStr) {
      const body = (description || "").trim();
      const dimensionsFirst = vertical === "furniture" || (vertical === "luxury" && category === "Handbags");
      description = dimensionsFirst
        ? (dimStr + "\n\n" + body).trim()
        : (body + "\n\n" + dimStr).trim();
    }
  }

  // Write back to Shopify: vertical, category, dimensions_status (furniture), vendor
  await updateShopifyMetafields(shopifyProductId, {
    category: shopifyCategoryValue,
    vertical: detectedVertical,
    dimensionsStatus: vertical === "furniture" ? dimensionsStatus : undefined,
  });
  await updateShopifyVendor(shopifyProductId, brand);

  const currentHash = shopifyHash(product);

  console.log("\n=======================================");
  console.log("🧾 SYNC PRODUCT");
  console.log("shopifyProductId          =", shopifyProductId);
  console.log("vertical                  =", vertical);
  console.log("cache.webflowId           =", cacheEntry?.webflowId || "null");
  console.log("previousQty               =", previousQty);
  console.log("currentQty                =", qty);
  console.log("category                  =", category);
  console.log("soldNow                   =", soldNow);
  console.log("shopifyUrl                =", shopifyUrl);
  console.log("slug                      =", slug);
  if (vertical === "furniture") console.log("dimensions_status          =", dimensionsStatus);
  console.log("=======================================\n");

  // Find existing: ecommerce API for Furniture (siteId), CMS for Luxury (collectionId)
  let existing = null;
  if (vertical === "furniture" && config.siteId) {
    if (cacheEntry?.webflowId) {
      console.log("⚡ Trying cache.webflowId (ecommerce) =", cacheEntry.webflowId);
      existing = await getWebflowEcommerceProductById(config.siteId, cacheEntry.webflowId, config.token);
      if (!existing) console.log("⚠️ Cached Webflow product not found, falling back to scan.");
    }
    if (!existing) {
      existing = await findExistingWebflowEcommerceProduct(shopifyProductId, slug, config);
    }
  } else {
    if (cacheEntry?.webflowId) {
      console.log("⚡ Trying cache.webflowId =", cacheEntry.webflowId);
      existing = await getWebflowItemById(cacheEntry.webflowId, config);
      if (!existing) console.log("⚠️ Cached Webflow ID not found, falling back to scan.");
    }
    if (!existing) {
      existing = await findExistingWebflowItem(shopifyProductId, shopifyUrl, slug, config);
    }
  }

  if (existing) {
    console.log("✅ EXISTING WEBFLOW ITEM LINKED:", existing.id);

    const newlySold =
      (previousQty === null || previousQty > 0) && qty !== null && qty <= 0;

    if (newlySold) {
      console.log("🟠 Newly sold, marking per vertical in Webflow.");
      await markAsSold(existing, vertical, config);
      if (vertical === "furniture" && config.siteId) {
        await syncFurnitureEcommerceSku(product, existing.id, config);
      }
      cache[shopifyProductId] = {
        hash: currentHash,
        webflowId: existing.id,
        lastQty: qty,
        vertical,
      };
      webflowLog("info", { event: "cache.mutated", shopifyProductId, op: "sold", webflowId: existing.id, vertical });
      return { operation: "sold", id: existing.id };
    }

    const previousHash = cacheEntry?.hash || null;
    const changed =
      !previousHash ||
      JSON.stringify(currentHash) !== JSON.stringify(previousHash);

    if (changed) {
      console.log("✏️ Changes detected, updating Webflow item:", existing.id);
      const fieldData = buildWebflowFieldData({
        vertical,
        name,
        brand,
        price,
        description,
        shopifyProductId,
        shopifyUrl,
        shopifySlug: slug,
        productType: product.product_type,
        category,
        featuredImage,
        gallery,
        showOnWebflow,
        soldNow,
        dimensions,
        dimensionsStatus,
        existingSlug: existing.fieldData?.slug,
        newSlug: slug,
      });
      if (vertical === "furniture" && config.siteId) {
        await updateWebflowEcommerceProduct(config.siteId, existing.id, fieldData, config.token, existing);
        await syncFurnitureEcommerceSku(product, existing.id, config);
      } else {
        await axios.patch(
          `https://api.webflow.com/v2/collections/${config.collectionId}/items/${existing.id}`,
          { fieldData },
          {
            headers: {
              Authorization: `Bearer ${config.token}`,
              "Content-Type": "application/json",
            },
          }
        );
      }
      cache[shopifyProductId] = {
        hash: currentHash,
        webflowId: existing.id,
        lastQty: qty,
        vertical,
      };
      webflowLog("info", { event: "cache.mutated", shopifyProductId, op: "update", webflowId: existing.id, vertical });
      return { operation: "update", id: existing.id };
    }

    console.log("⏭️ No changes detected, skipping update for Webflow item:", existing.id);
    cache[shopifyProductId] = {
      hash: currentHash,
      webflowId: existing.id,
      lastQty: qty,
      vertical,
    };
    webflowLog("info", { event: "cache.mutated", shopifyProductId, op: "skip", webflowId: existing.id, vertical });
    return { operation: "skip", id: existing.id };
  }

  if (!cacheEntry) {
    console.log("\n" + "=".repeat(60));
    console.log("🆕 CREATE PATH — NO CACHE + NO MATCH → Creating new Webflow item.");
    console.log("=".repeat(60));

    const createConfig = getWebflowConfig(detectedVertical);
    const productFieldData = buildWebflowFieldData({
      vertical: detectedVertical,
      name,
      brand,
      price,
      description,
      shopifyProductId,
      shopifyUrl,
      shopifySlug: slug,
      productType: product.product_type,
      category,
      featuredImage,
      gallery,
      showOnWebflow,
      soldNow,
      dimensions,
      dimensionsStatus,
      existingSlug: null,
      newSlug: slug,
    });

    let newId;

    if (detectedVertical === "furniture" && createConfig.siteId) {
      const priceCents = price != null && price !== "" ? Math.round(parseFloat(price) * 100) : null;
      const dims = dimensions || getDimensionsFromProduct(product);
      const skuFieldData = {
        name: name ? `${name} - Default` : "Default SKU",
        slug: slug ? `${slug}-default-sku` : `sku-${shopifyProductId}`,
        price: priceCents != null ? { value: priceCents, unit: "USD" } : null,
        ...skuDimensionFields(dims),
        "main-image": featuredImage ? { url: featuredImage } : null,
        "more-images": (gallery || []).slice(0, 10).map((url) => (url ? { url } : null)).filter(Boolean),
      };
      console.log("[CREATE] ecommerce: POST /v2/sites/" + createConfig.siteId + "/products");
      console.log("[CREATE] productFieldData:", JSON.stringify(productFieldData, null, 2));
      console.log("[CREATE] skuFieldData:", JSON.stringify(skuFieldData, null, 2));
      try {
        const createResp = await createWebflowEcommerceProduct(
          createConfig.siteId,
          productFieldData,
          skuFieldData,
          createConfig.token
        );
        newId = createResp.product?.id;
        if (!newId) throw new Error("No product.id in ecommerce create response");
        console.log("[CREATE] ✅ CREATED NEW ECOMMERCE PRODUCT id =", newId);
      } catch (createErr) {
        console.error("[CREATE] ❌ Ecommerce create failed:", createErr.message);
        console.error("[CREATE] status =", createErr.response?.status);
        console.error("[CREATE] body   =", JSON.stringify(createErr.response?.data, null, 2));
        throw createErr;
      }
    } else {
      console.log("[CREATE] CMS: POST /v2/collections/" + createConfig.collectionId + "/items");
      console.log("[CREATE] fieldData:", JSON.stringify(productFieldData, null, 2));
      let resp;
      try {
        resp = await axios.post(
          `https://api.webflow.com/v2/collections/${createConfig.collectionId}/items`,
          { fieldData: productFieldData },
          {
            headers: {
              Authorization: `Bearer ${createConfig.token}`,
              "Content-Type": "application/json",
            },
          }
        );
      } catch (createErr) {
        console.error("[CREATE] ❌ POST failed:", createErr.message);
        console.error("[CREATE] status  =", createErr.response?.status);
        console.error("[CREATE] body    =", JSON.stringify(createErr.response?.data, null, 2));
        throw createErr;
      }
      newId = resp.data.id;
      console.log("[CREATE] ✅ CREATED NEW WEBFLOW ITEM id =", newId);
    }
    console.log("=".repeat(60) + "\n");
    cache[shopifyProductId] = {
      hash: currentHash,
      webflowId: newId,
      lastQty: qty,
      vertical: detectedVertical,
    };
    webflowLog("info", { event: "cache.mutated", shopifyProductId, op: "create", webflowId: newId, vertical: detectedVertical });
    return { operation: "create", id: newId };
  }

  webflowLog("info", { event: "path.skipped", reason: "skip-missing-webflow", shopifyProductId, message: "CACHE EXISTS BUT NO WEBFLOW MATCH FOUND → NOT CREATING" });
  console.log(
    "🚫 CACHE EXISTS BUT NO WEBFLOW MATCH FOUND → NOT CREATING (skip-missing-webflow)"
  );
  return { operation: "skip-missing-webflow", id: null };
}

/* ======================================================
   BUILD WEBFLOW fieldData BY VERTICAL
   Luxury: featured-image, image-1..5, show-on-webflow, brand, price, shopify-url.
   Furniture: slugs match CMS — name, slug, description, category, sold,
   shopify-product-id, shopify-slug-2, main-description-2, ec-product-type, shippable.
   (Images live on SKU; no dimension fields on this collection.)
====================================================== */
function buildWebflowFieldData(opts) {
  const {
    vertical,
    name,
    brand,
    price,
    description,
    shopifyProductId,
    shopifyUrl,
    shopifySlug,
    productType,
    category,
    featuredImage,
    gallery,
    showOnWebflow,
    soldNow,
    dimensions,
    dimensionsStatus,
    existingSlug,
    newSlug,
  } = opts;

  const slug = existingSlug ?? newSlug ?? "";

  if (vertical === "furniture") {
    // Ecommerce product category must be an ItemRef (collection item ID), never a display string.
    const categoryRef = resolveFurnitureCategoryRef(category);
    const out = {
      name,
      slug,
      description: description ?? "",
      sold: !!soldNow,
      "shopify-product-id": shopifyProductId,
      "shopify-slug-2": shopifySlug ?? newSlug ?? "",
      "main-description-2": description ?? null,
      "ec-product-type": productType ?? null,
      shippable: true,
    };
    if (categoryRef != null) out.category = categoryRef;
    return out;
  }

  const base = {
    name,
    brand,
    price,
    description,
    "shopify-product-id": shopifyProductId,
    "shopify-url": shopifyUrl,
    category,
    slug,
  };
  return {
    ...base,
    "featured-image": featuredImage ? { url: featuredImage } : null,
    "image-1": gallery?.[0] ? { url: gallery[0] } : null,
    "image-2": gallery?.[1] ? { url: gallery[1] } : null,
    "image-3": gallery?.[2] ? { url: gallery[2] } : null,
    "image-4": gallery?.[3] ? { url: gallery[3] } : null,
    "image-5": gallery?.[4] ? { url: gallery[4] } : null,
    "show-on-webflow": showOnWebflow,
  };
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
  webflowLog("info", { event: "sync-all.entry", message: "sync-all started" });
  try {
    const products = await fetchAllShopifyProducts();
    const cache = loadCache();
    webflowLog("info", { event: "sync-all.loaded", productCount: products?.length ?? 0, cacheKeys: Object.keys(cache).length });

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
      const vertical = entry?.vertical ?? "luxury";
      const config = getWebflowConfig(vertical);
      let existing = null;

      console.log(
        `🕳️ DISAPPEARED: shopifyProductId=${goneId}, cache.webflowId=${entry?.webflowId || "null"}, vertical=${vertical}`
      );

      if (entry?.webflowId) {
        if (vertical === "furniture" && config.siteId) {
          existing = await getWebflowEcommerceProductById(config.siteId, entry.webflowId, config.token);
        } else {
          existing = await getWebflowItemById(entry.webflowId, config);
        }
      }
      if (!existing) {
        if (vertical === "furniture" && config.siteId) {
          existing = await findExistingWebflowEcommerceProduct(goneId, null, config);
        } else {
          existing = await findExistingWebflowItem(goneId, null, null, config);
        }
      }

      if (existing) {
        console.log("🟠 Marking disappeared product as SOLD in Webflow:", existing.id, "vertical:", vertical);
        await markAsSold(existing, vertical, config);
        sold++;
      } else {
        console.log("⚪ No Webflow item found for disappeared product", goneId);
      }

      delete cache[goneId];
      webflowLog("info", { event: "cache.mutated", shopifyProductId: goneId, op: "deleted", reason: "disappeared" });
    }

    for (const product of products) {
      const result = await syncSingleProduct(product, cache);

      if (result.operation === "create") created++;
      else if (result.operation === "update") updated++;
      else if (result.operation === "sold") sold++;
      else skipped++;
    }

    saveCache(cache);

    webflowLog("info", { event: "sync-all.exit", created, updated, skipped, sold, total: products.length });
    res.json({
      status: "ok",
      total: products.length,
      created,
      updated,
      skipped,
      sold,
    });
  } catch (err) {
    const status = err.response?.status;
    const body = err.response?.data;
    console.error("❌ sync-all error:", err.message);
    if (status) console.error("   status:", status);
    if (body) console.error("   response:", JSON.stringify(body));
    webflowLog("error", {
      event: "sync-all.failure",
      message: err.message,
      status: status ?? null,
      responseBody: body ?? null,
      url: err.config?.url ?? null,
      method: err.config?.method ?? null,
    });
    const detail = body?.message || body?.err || err.message;
    res.status(status && status >= 400 ? status : 500).json({
      error: err.message,
      ...(detail && detail !== err.message && { detail: String(detail) }),
      ...(body && typeof body === "object" && { webflowResponse: body }),
    });
  }
});

/* ======================================================
   SERVER
====================================================== */
const PORT = process.env.PORT || 4000;
app.listen(PORT, () => {
  console.log(`🔥 Sync server running on ${PORT}`);
});



