import express from "express";
import axios from "axios";
import cors from "cors";
import crypto from "crypto";
import dotenv from "dotenv";
import fs from "fs";
import nodemailer from "nodemailer";
import { CATEGORY_KEYWORDS } from "./categoryKeywords.js";
import { CATEGORY_KEYWORDS_FURNITURE, CATEGORY_KEYWORDS_FURNITURE_WEAK } from "./categoryKeywordsFurniture.js";
import { detectBrandFromProduct } from "./brand.js";
import { detectBrandFromProductFurniture } from "./brandFurniture.js";
import { detectVertical } from "./vertical.js";

dotenv.config();

/* ======================================================
   DUPLICATE PLACEMENT — Email alert (Gmail SMTP)
   Env: GMAIL_SMTP_USER, GMAIL_SMTP_PASSWORD, REPORT_EMAIL_TO (comma-separated)
====================================================== */
/** @param {Set<string>} [duplicateEmailSentFor] - Per-run dedupe: only one email per shopifyProductId per run. We also persist sent IDs so we never send again for the same item unless a significant change happens. */
async function sendDuplicatePlacementEmail(conflictLog, duplicateEmailSentFor) {
  const { productTitle, shopifyProductId, previousVertical, detectedVertical, webflowIdArchived, sweep } = conflictLog;
  const id = String(shopifyProductId ?? "");
  if (duplicateEmailSentFor && duplicateEmailSentFor.has(id)) {
    webflowLog("info", { event: "duplicate_placement.email_skipped", reason: "already_sent_this_run", shopifyProductId: id });
    return;
  }
  const sentPreviously = loadDuplicatePlacementSentIds();
  if (sentPreviously.has(id)) {
    webflowLog("info", { event: "duplicate_placement.email_skipped", reason: "already_sent_previous_run", shopifyProductId: id });
    return;
  }
  const to = process.env.REPORT_EMAIL_TO;
  if (!to || !process.env.GMAIL_SMTP_USER || !process.env.GMAIL_SMTP_PASSWORD) {
    webflowLog("warn", { event: "duplicate_placement.email_skipped", reason: "missing_env", REPORT_EMAIL_TO: !!to });
    return;
  }
  const recipients = to.split(",").map((e) => e.trim()).filter(Boolean);
  const subject = `[Webflow Sync] Duplicate placement — item is archived`;
  const intro = sweep
    ? "This item was found in the Furniture collection but is classified as Luxury (e.g. bag, clutch, scarf). It is archived; we created it in Luxury."
    : "This item was previously synced to one collection but is now detected as belonging to another. It is archived.";
  const body = [
    intro,
    "",
    "Details (from sync logs):",
    `  Product title: ${productTitle || "(none)"}`,
    `  Shopify product ID: ${shopifyProductId}`,
    `  Was in: ${previousVertical}`,
    `  Now detected as: ${detectedVertical}`,
    `  Webflow item ID (archived): ${webflowIdArchived || "n/a"}`,
    "",
    "This item is archived. You should go into the backend and delete it (so it does not remain in the wrong collection).",
    "If a significant change happens again for this item, we will send another email.",
    "",
    "Please:",
    "  1. Go into the backend and delete the archived item if it still appears.",
    "  2. Adjust the product (name, tags, or product type) in Shopify if needed so it classifies consistently in the future.",
    "",
    "— Lost & Found Webflow Sync",
  ].join("\n");

  try {
    const transporter = nodemailer.createTransport({
      host: "smtp.gmail.com",
      port: 587,
      secure: false,
      auth: {
        user: process.env.GMAIL_SMTP_USER,
        pass: process.env.GMAIL_SMTP_PASSWORD,
      },
    });
    await transporter.sendMail({
      from: process.env.GMAIL_SMTP_USER,
      to: recipients,
      subject,
      text: body,
    });
    webflowLog("info", { event: "duplicate_placement.email_sent", to: recipients, shopifyProductId });
    if (duplicateEmailSentFor) duplicateEmailSentFor.add(id);
    saveDuplicatePlacementSentId(id);
  } catch (err) {
    webflowLog("error", { event: "duplicate_placement.email_failed", shopifyProductId, message: err.message });
  }
}

/* ======================================================
   [WEBFLOW] CENTRALIZED LOGGING — Single-line JSON, timestamp, optional requestId/elapsedMs
   LOG_LEVEL: "error" | "warn" | "info" — defaults to "info". "error" cuts most I/O to speed up frequent syncs.
====================================================== */
const LOG_LEVEL = (process.env.LOG_LEVEL || "info").toLowerCase();
const LOG_ERROR = LOG_LEVEL === "error" || LOG_LEVEL === "warn" || LOG_LEVEL === "info";
const LOG_WARN = LOG_LEVEL === "warn" || LOG_LEVEL === "info";
const LOG_INFO = LOG_LEVEL === "info";
const LOG_REQUESTS = LOG_INFO; // skip per-request logs unless info

let syncRequestId = null;
let syncStartTime = null;

function webflowLog(level, payload) {
  if (level === "error" && !LOG_ERROR) return;
  if (level === "warn" && !LOG_WARN) return;
  if (level === "info" && !LOG_INFO) return;
  const base = {
    ts: new Date().toISOString(),
    level,
    ...(syncRequestId != null && { requestId: syncRequestId }),
    ...(syncStartTime != null && { elapsedMs: Date.now() - syncStartTime }),
    ...(typeof payload === "object" && payload !== null ? payload : { message: String(payload) }),
  };
  const msg = JSON.stringify(base);
  if (level === "warn") console.warn(msg);
  else if (level === "error") console.error(msg);
  else console.log(msg);
}

function webflowRequestLog(method, url, body) {
  if (!LOG_REQUESTS) return;
  // avoid logging full request body — it can be huge and slows down sync
  webflowLog("info", { event: "request", method, url });
}

function webflowFailureLog(method, url, status, responseBody, requestBody) {
  if (!LOG_ERROR) return;
  const truncate = (o, max = 500) => {
    if (o == null) return null;
    const s = typeof o === "string" ? o : JSON.stringify(o);
    return s.length <= max ? s : s.slice(0, max) + "...";
  };
  webflowLog("error", {
    event: "failure",
    method,
    url,
    status,
    responseBody: truncate(responseBody),
    requestBody: truncate(requestBody),
  });
}

// Axios interceptors: only for api.webflow.com
const WEBFLOW_ORIGIN = "https://api.webflow.com";
const WEBFLOW_MIN_DELAY_MS = Math.max(500, parseInt(process.env.WEBFLOW_MIN_DELAY_MS || "1000", 10)); // ~60 req/min; use 600 for CMS (120/min)
const WEBFLOW_429_MAX_RETRIES = Math.min(5, Math.max(1, parseInt(process.env.WEBFLOW_429_MAX_RETRIES || "3", 10)));

let lastWebflowRequestTime = 0;

axios.interceptors.request.use(async (config) => {
  if (!config.url || !String(config.url).startsWith(WEBFLOW_ORIGIN)) return config;
  const now = Date.now();
  const elapsed = now - lastWebflowRequestTime;
  if (elapsed < WEBFLOW_MIN_DELAY_MS) {
    await new Promise((r) => setTimeout(r, WEBFLOW_MIN_DELAY_MS - elapsed));
  }
  lastWebflowRequestTime = Date.now();
  if (LOG_REQUESTS) {
    webflowRequestLog(config.method?.toUpperCase() ?? "GET", config.url, config.data);
  }
  return config;
});
axios.interceptors.response.use(
  (response) => response,
  async (err) => {
    const config = err.config;
    const url = config?.url;
    if (!url || !String(url).startsWith(WEBFLOW_ORIGIN)) return Promise.reject(err);

    const status = err.response?.status;
    const retryCount = config.__webflowRetryCount ?? 0;

    if (status === 429 && retryCount < WEBFLOW_429_MAX_RETRIES) {
      const retryAfter = err.response?.headers?.["retry-after"];
      const waitSec = retryAfter != null ? parseInt(String(retryAfter), 10) : 60;
      const waitMs = Math.min(120000, Math.max(5000, (Number.isNaN(waitSec) ? 60 : waitSec) * 1000));
      webflowLog("warn", {
        event: "webflow.429_retry",
        url: config.url,
        retryCount: retryCount + 1,
        maxRetries: WEBFLOW_429_MAX_RETRIES,
        waitMs,
        retryAfter: retryAfter ?? "default 60s",
      });
      await new Promise((r) => setTimeout(r, waitMs));
      config.__webflowRetryCount = retryCount + 1;
      return axios(config);
    }

    webflowFailureLog(
      config?.method?.toUpperCase() ?? "?",
      url,
      status,
      err.response?.data,
      config?.data
    );
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
const DUPLICATE_EMAIL_SENT_FILE = `${DATA_DIR}/duplicate_placement_emails_sent.json`;

function ensureDataDir() {
  if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
  }
}

/** IDs we've already sent a duplicate-placement email for (across runs). If they don't fix it, we won't email again unless a significant change happens. */
function loadDuplicatePlacementSentIds() {
  try {
    if (!fs.existsSync(DUPLICATE_EMAIL_SENT_FILE)) return new Set();
    const raw = fs.readFileSync(DUPLICATE_EMAIL_SENT_FILE, "utf8");
    const arr = JSON.parse(raw);
    return new Set(Array.isArray(arr) ? arr : []);
  } catch (err) {
    return new Set();
  }
}

function saveDuplicatePlacementSentId(id) {
  try {
    ensureDataDir();
    const set = loadDuplicatePlacementSentIds();
    set.add(String(id));
    fs.writeFileSync(DUPLICATE_EMAIL_SENT_FILE, JSON.stringify([...set], null, 2), "utf8");
  } catch (err) {
    webflowLog("error", { event: "duplicate_placement_sent.save_failed", message: err.message });
  }
}

function loadCache() {
  try {
    ensureDataDir();
    if (!fs.existsSync(CACHE_FILE)) return {};
    const raw = fs.readFileSync(CACHE_FILE, "utf8");
    return JSON.parse(raw);
  } catch (err) {
    webflowLog("error", { event: "cache.load_failed", message: err.message });
    return {};
  }
}

function saveCache(cache) {
  try {
    ensureDataDir();
    fs.writeFileSync(CACHE_FILE, JSON.stringify(cache, null, 2), "utf8");
  } catch (err) {
    webflowLog("error", { event: "cache.save_failed", message: err.message });
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
    webflowLog("error", { event: "getWebflowEcommerceProductById.error", siteId, productId, message: err.message, responseData: err.response?.data });
    return null;
  }
}

/** Run-scoped index: Furniture ecommerce products. Populated at sync start for O(1) lookup. */
let furnitureProductIndex = null;

async function findExistingWebflowEcommerceProduct(shopifyProductId, slug, config, productNameForFallback = null) {
  if (!config?.siteId || !config?.token) return null;
  const slugNorm = slug ? String(slug).trim() : null;

  // Use pre-loaded index when available
  if (furnitureProductIndex) {
    const byId = furnitureProductIndex.byShopifyId?.get(String(shopifyProductId));
    if (byId) return byId;
    if (slugNorm) {
      const bySlug = furnitureProductIndex.bySlug?.get(slugNorm);
      if (bySlug) return bySlug;
    }
    if (productNameForFallback && furnitureProductIndex.byName) {
      const nameKey = normalizeProductNameForIndex(productNameForFallback);
      const byName = furnitureProductIndex.byName.get(nameKey);
      if (byName) {
        webflowLog("info", { event: "furniture_find_by_name", shopifyProductId, webflowId: byName.id, name: productNameForFallback });
        return byName;
      }
    }
    return null;
  }

  let offset = 0;
  const limit = 100;
  while (true) {
    const url = `https://api.webflow.com/v2/sites/${config.siteId}/products?limit=${limit}&offset=${offset}`;
    let response;
    try {
      response = await axios.get(url, {
        headers: { Authorization: `Bearer ${config.token}`, accept: "application/json" },
      });
    } catch (err) {
      webflowLog("error", { event: "ecommerce_list.error", siteId: config.siteId, message: err.message, responseData: err.response?.data });
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
  const productData = sanitizeCategoryForWebflow({ ...productFieldData });
  const skuData = sanitizeCategoryForWebflow({ ...skuFieldData });
  const payload = {
    product: { fieldData: productData },
    sku: { fieldData: skuData },
    publishStatus: "staging",
  };
  const response = await axios.post(url, payload, {
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
  });
  return response.data;
}

function sanitizeCategoryForWebflow(fieldData) {
  if (!fieldData || typeof fieldData !== "object") return fieldData;
  const out = { ...fieldData };
  const cat = out.category;
  const validRef = typeof cat === "string" && WEBFLOW_ITEM_REF_REGEX.test(cat);
  if ("category" in out && !validRef) {
    delete out.category;
  }
  return out;
}

async function updateWebflowEcommerceProduct(siteId, productId, fieldData, token, existingProduct = null) {
  const url = `https://api.webflow.com/v2/sites/${siteId}/products/${productId}`;
  let data = sanitizeCategoryForWebflow({ ...fieldData });
  let skuFieldData = existingProduct?.skus?.[0]?.fieldData;
  if (skuFieldData == null || typeof skuFieldData !== "object") {
    webflowLog("info", { event: "product.patch.skuRefetch", productId, reason: "existingProduct had no skus or sku.fieldData" });
    const full = await getWebflowEcommerceProductById(siteId, productId, token);
    skuFieldData = full?.skus?.[0]?.fieldData ?? {};
  }
  skuFieldData = sanitizeCategoryForWebflow({ ...skuFieldData });
  const body = {
    product: { fieldData: data },
    sku: { fieldData: skuFieldData },
  };
  body.product.fieldData = sanitizeCategoryForWebflow(body.product.fieldData);
  body.sku.fieldData = sanitizeCategoryForWebflow(body.sku.fieldData);
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

/** Try to delete an ecommerce product; if the API doesn't support delete (404/405), archive instead. */
async function deleteOrArchiveWebflowEcommerceProduct(siteId, productId, token) {
  if (!siteId || !productId || !token) return;
  const url = `https://api.webflow.com/v2/sites/${siteId}/products/${productId}`;
  try {
    await axios.delete(url, {
      headers: { Authorization: `Bearer ${token}`, accept: "application/json" },
    });
    webflowLog("info", { event: "delete.ecommerce_product", productId, message: "Duplicate deleted from Furniture" });
  } catch (err) {
    const status = err.response?.status;
    if (status === 404 || status === 405 || status === 501) {
      webflowLog("info", { event: "delete.ecommerce_not_supported", productId, status, message: "Falling back to archive" });
      await archiveWebflowEcommerceProduct(siteId, productId, token);
    } else {
      throw err;
    }
  }
}

/** Delete a CMS collection item (e.g. remove from Luxury when product is actually furniture). */
async function deleteWebflowCollectionItem(collectionId, itemId, token) {
  if (!collectionId || !itemId || !token) return;
  const url = `https://api.webflow.com/v2/collections/${collectionId}/items/${itemId}`;
  webflowLog("info", { event: "delete.cms_item", collectionId, itemId, message: "Removing duplicate from other vertical" });
  await axios.delete(url, {
    headers: { Authorization: `Bearer ${token}`, accept: "application/json" },
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
      webflowLog("error", { event: "publishing.error", message: err.message, responseData: err.response?.data });
    }
  }
}

/* ======================================================
   SHOPIFY — WRITE METAFIELDS (department, category, vertical, dimensions_status)
   Department = parent (Furniture & Home | Luxury Goods). Category = child (Living Room, Handbags, etc.).
   Furniture & Home and Luxury Goods are mutually exclusive: furniture gets furniture_and_home = category, luxury_goods = "";
   luxury gets furniture_and_home = "", luxury_goods = category. Collection rules use these.
   Override Furniture & Home metafield: FURNITURE_AND_HOME_METAFIELD_NAMESPACE, FURNITURE_AND_HOME_METAFIELD_KEY
====================================================== */
const FURNITURE_AND_HOME_NAMESPACE = (process.env.FURNITURE_AND_HOME_METAFIELD_NAMESPACE || "custom").trim() || "custom";
const FURNITURE_AND_HOME_KEY = (process.env.FURNITURE_AND_HOME_METAFIELD_KEY || "furniture_category").trim() || "furniture_category";

async function updateShopifyMetafields(productId, { department, category, vertical, dimensionsStatus }) {
  const ownerId = `gid://shopify/Product/${productId}`;
  const dept = department ?? "";
  const cat = category ?? "";
  const vert = vertical ?? "luxury";
  const isFurniture = dept === "Furniture & Home";
  // "category" metafield only accepts luxury options (Handbags, Totes, etc.). Furniture uses furniture_and_home only.
  const metafields = [
    { ownerId, key: "department", namespace: "custom", type: "single_line_text_field", value: dept },
    ...(!isFurniture ? [{ ownerId, key: "category", namespace: "custom", type: "single_line_text_field", value: cat || "Other " }] : []),
    { ownerId, key: "vertical", namespace: "custom", type: "single_line_text_field", value: vert },
    { ownerId, key: "product_type_group", namespace: "custom", type: "single_line_text_field", value: dept },
    // Furniture & Home: Living Room, Bedroom, Accessories, etc. (use env vars if your store's definition differs)
    ...(isFurniture ? [{ ownerId, key: FURNITURE_AND_HOME_KEY, namespace: FURNITURE_AND_HOME_NAMESPACE, type: "single_line_text_field", value: cat || "Accessories" }] : []),
    // luxury_goods: Handbags, Other , etc. (Luxury Goods dropdown)
    ...(!isFurniture ? [{ ownerId, key: "luxury_goods", namespace: "custom", type: "single_line_text_field", value: cat || "Other " }] : []),
  ].filter((m) => m.value != null && String(m.value).trim() !== "");
  if (dimensionsStatus != null && String(dimensionsStatus).trim() !== "") {
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
  const res = await axios.post(
    SHOPIFY_GRAPHQL_URL,
    { query: mutation, variables: { metafields } },
    {
      headers: {
        "X-Shopify-Access-Token": process.env.SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json",
      },
    }
  );
  const errors = res.data?.data?.metafieldsSet?.userErrors ?? [];
  if (errors.length > 0) {
    const msg = errors.map((e) => `${e.field}: ${e.message}`).join("; ");
    webflowLog("error", {
      event: "metafields_set.user_errors",
      productId,
      userErrors: errors,
      furnitureMetafield: isFurniture ? { namespace: FURNITURE_AND_HOME_NAMESPACE, key: FURNITURE_AND_HOME_KEY, value: cat || "Accessories" } : null,
      hint: "If Furniture & Home fails: check Shopify Settings → Custom data → Products for the metafield's exact namespace and key, then set FURNITURE_AND_HOME_METAFIELD_NAMESPACE and FURNITURE_AND_HOME_METAFIELD_KEY.",
    });
    throw new Error(`Shopify metafieldsSet failed: ${msg}`);
  }
}
/* ======================================================
   SHOPIFY — WRITE our logic TO Shopify (vendor, productType, tags)
   We decide vertical/category; we WRITE that to Shopify so Shopify matches Webflow. Tags = department + category.
====================================================== */
const SYNC_DEPARTMENT_TAGS = ["Furniture & Home", "Luxury Goods"];
const SYNC_CATEGORY_TAGS = [
  "Living Room", "Dining Room", "Office Den", "Rugs", "Art / Mirrors", "Bedroom", "Accessories", "Outdoor / Patio", "Lighting",
  "Handbags", "Totes", "Crossbody", "Wallets", "Backpacks", "Luggage", "Scarves", "Belts", "Small Bags", "Other ", "Other",
  "Recently Sold",
];
function mergeProductTagsForSync(existingTags, department, category) {
  const existing = Array.isArray(existingTags) ? existingTags : (typeof existingTags === "string" ? existingTags.split(",").map((s) => s.trim()).filter(Boolean) : []);
  const toRemove = new Set([...SYNC_DEPARTMENT_TAGS, ...SYNC_CATEGORY_TAGS].map((t) => t.trim()).filter(Boolean));
  const kept = existing.filter((t) => !toRemove.has(String(t).trim()));
  const toAdd = [department, category].filter((v) => v != null && String(v).trim() !== "");
  const combined = [...kept];
  for (const tag of toAdd) {
    const t = String(tag).trim();
    if (t && !combined.includes(t)) combined.push(t);
  }
  return combined;
}

async function updateShopifyVendorAndType(productId, brandValue, productType, existingTags, department, category, descriptionHtml) {
  const mutation = `
    mutation UpdateProduct($input: ProductInput!) {
      productUpdate(input: $input) {
        product {
          id
          vendor
          productType
          tags
          descriptionHtml
        }
        userErrors {
          field
          message
        }
      }
    }
  `;

  const input = {
    id: `gid://shopify/Product/${productId}`,
    vendor: brandValue || "Unknown",
  };
  if (productType != null && String(productType).trim() !== "") {
    input.productType = String(productType).trim();
  }
  if (department != null && category != null) {
    input.tags = mergeProductTagsForSync(existingTags ?? [], department, category);
  }
  // Only update description if explicitly provided (not null and not empty)
  if (descriptionHtml != null && String(descriptionHtml).trim() !== "") {
    input.descriptionHtml = String(descriptionHtml).trim();
  }
  const variables = { input };

  const res = await axios.post(
    SHOPIFY_GRAPHQL_URL,
    { query: mutation, variables },
    {
      headers: {
        "X-Shopify-Access-Token": process.env.SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json",
      }
    }
  );
  const errors = res.data?.data?.productUpdate?.userErrors ?? [];
  if (errors.length > 0) {
    const msg = errors.map((e) => `${e.field}: ${e.message}`).join("; ");
    webflowLog("error", { event: "product_update.user_errors", productId, userErrors: errors });
    throw new Error(`Shopify productUpdate failed: ${msg}`);
  }
}

/* ======================================================
   SHOPIFY — REPLACE "CONDITION" WITH "TITLE" (Furniture only)
   Replaces "Condition" option with "Title: Default Title" for Furniture & Home products.
   This satisfies Shopify's requirement for at least one option while being eBay-compatible.
====================================================== */
async function removeConditionOptionIfFurniture(product) {
  const productId = String(product.id);
  
  // Check if product has options
  if (!product.options || !Array.isArray(product.options) || product.options.length === 0) {
    webflowLog("info", { event: "condition_option.skip", shopifyProductId: productId, reason: "no_options" });
    return;
  }

  // Find "Condition" option
  const conditionOption = product.options.find(opt => 
    opt && opt.name && String(opt.name).toLowerCase() === "condition"
  );

  if (!conditionOption || !conditionOption.id) {
    webflowLog("info", { event: "condition_option.skip", shopifyProductId: productId, reason: "condition_not_found" });
    return;
  }

  // First, query the product to get the ProductOptionValue IDs
  const queryProduct = `
    query getProduct($id: ID!) {
      product(id: $id) {
        options {
          id
          name
          optionValues {
            id
            name
          }
        }
      }
    }
  `;

  let optionValueIds = [];
  try {
    const queryRes = await axios.post(
      SHOPIFY_GRAPHQL_URL,
      { 
        query: queryProduct, 
        variables: { id: `gid://shopify/Product/${productId}` } 
      },
      {
        headers: {
          "X-Shopify-Access-Token": process.env.SHOPIFY_ACCESS_TOKEN,
          "Content-Type": "application/json",
        },
      }
    );

    const productData = queryRes.data?.data?.product;
    const conditionOptionData = productData?.options?.find(opt => 
      opt.name && String(opt.name).toLowerCase() === "condition"
    );

    if (conditionOptionData?.optionValues) {
      optionValueIds = conditionOptionData.optionValues.map(ov => ov.id);
    }
  } catch (err) {
    webflowLog("error", {
      event: "condition_option.query_error",
      shopifyProductId: productId,
      message: err.message
    });
    return;
  }

  if (optionValueIds.length === 0) {
    webflowLog("warn", {
      event: "condition_option.no_values",
      shopifyProductId: productId,
      optionId: conditionOption.id
    });
    return;
  }

  // Build the optionValuesToUpdate array with actual IDs
  const optionValuesToUpdate = optionValueIds.map(id => ({
    id,
    name: "Default Title"
  }));

  // Strategy: Use productOptionUpdate to rename option and update all its values
  const mutation = `
    mutation productOptionUpdate(
      $productId: ID!,
      $option: OptionUpdateInput!,
      $optionValuesToUpdate: [OptionValueUpdateInput!]
    ) {
      productOptionUpdate(
        productId: $productId,
        option: $option,
        optionValuesToUpdate: $optionValuesToUpdate
      ) {
        product {
          id
          options {
            id
            name
            values
          }
        }
        userErrors {
          field
          message
          code
        }
      }
    }
  `;

  const variables = {
    productId: `gid://shopify/Product/${productId}`,
    option: {
      id: `gid://shopify/ProductOption/${conditionOption.id}`,
      name: "Title"
    },
    optionValuesToUpdate
  };

  try {
    const res = await axios.post(
      SHOPIFY_GRAPHQL_URL,
      { query: mutation, variables },
      {
        headers: {
          "X-Shopify-Access-Token": process.env.SHOPIFY_ACCESS_TOKEN,
          "Content-Type": "application/json",
        },
      }
    );

    const data = res.data?.data?.productOptionUpdate;
    const userErrors = data?.userErrors ?? [];

    if (userErrors.length > 0) {
      webflowLog("warn", {
        event: "condition_option.update_errors",
        shopifyProductId: productId,
        optionId: conditionOption.id,
        userErrors
      });
      return;
    }

    const updatedProduct = data?.product;
    if (updatedProduct) {
      webflowLog("info", {
        event: "condition_option.replaced_with_title",
        shopifyProductId: productId,
        oldOptionId: conditionOption.id,
        newOptions: updatedProduct.options
      });
    } else {
      webflowLog("info", {
        event: "condition_option.update_success_no_return",
        shopifyProductId: productId,
        optionId: conditionOption.id,
        message: "Update likely succeeded but Shopify didn't return product data"
      });
    }
  } catch (err) {
    // Log but don't throw - don't fail the entire sync for this
    webflowLog("error", {
      event: "condition_option.update_error",
      shopifyProductId: productId,
      optionId: conditionOption.id,
      message: err.message,
      status: err.response?.status,
      responseData: err.response?.data
    });
  }
}

/* ======================================================
   HASH FOR CHANGE DETECTION
   Includes dimensions (variant + metafields + tags) so dimension changes trigger an update.
   body_html is normalized (collapse whitespace) so Shopify formatting drift doesn't cause false "changed".
====================================================== */
function normalizeHtmlForHash(html) {
  if (html == null || typeof html !== "string") return html;
  return html.replace(/\s+/g, " ").trim();
}

function shopifyHash(product) {
  const dimensions = getDimensionsFromProduct(product);
  return {
    title: product.title,
    vendor: product.vendor,
    body_html: normalizeHtmlForHash(product.body_html),
    price: product.variants?.[0]?.price || null,
    qty: product.variants?.[0]?.inventory_quantity ?? null,
    images: (product.images || []).map((i) => i.src),
    slug: product.handle,
    dimensions: { width: dimensions.width, height: dimensions.height, length: dimensions.length, weight: dimensions.weight },
  };
}

/* ======================================================
   CATEGORY DETECTOR
====================================================== */
function detectCategory(title) {
  if (!title) return "Other ";
  const normalized = title.toLowerCase();
  for (const [category, keywords] of Object.entries(CATEGORY_KEYWORDS)) {
    for (const kw of keywords) {
      if (normalized.includes(kw.toLowerCase())) return category;
    }
  }
  return "Other ";
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

/**
 * Apply dimension-based rules for tables. Returns overridden category or null if no override.
 * Rules (height/depth/width in inches):
 * - Dining: Height ≥ 28 AND Depth ≥ 30 AND Width ≥ 40 → Dining Room
 * - Not dining: Height ≤ 22 OR Depth ≤ 24 → Living Room (disqualifies Dining)
 */
function applyTableDimensionRules(dims, name, descAndTags) {
  const h = dims?.height != null && !Number.isNaN(dims.height) ? Number(dims.height) : null;
  const w = dims?.width != null && !Number.isNaN(dims.width) ? Number(dims.width) : null;
  const d = dims?.length != null && !Number.isNaN(dims.length) ? Number(dims.length) : null; // length = depth
  if (h == null || w == null || d == null) return null;
  const text = (name + " " + (descAndTags || "")).toLowerCase();
  if (!/\btable\b/.test(text)) return null; // only for table-like products

  if (h <= 22 || d <= 24) return "LivingRoom"; // cannot be dining
  if (h >= 28 && d >= 30 && w >= 40) return "DiningRoom"; // dining table dimensions
  return null;
}

function detectCategoryFurniture(title, descriptionHtml, tags, dimensions) {
  const stripHtml = (html) => (html || "").replace(/<[^>]*>/g, " ").replace(/\s+/g, " ").trim();
  const descText = stripHtml(descriptionHtml || "").trim();
  const tagsStr = Array.isArray(tags) ? tags.join(" ") : typeof tags === "string" ? tags : "";
  const name = ((title || "").trim()).toLowerCase();
  const descAndTags = descText ? [descText, tagsStr].filter(Boolean).join(" ").toLowerCase() : "";
  const hasDesc = !!descText;

  if (!name && !descAndTags) return "Accessories";

  // Dimension-based override for tables (height/depth/width rules)
  const dimOverride = applyTableDimensionRules(dimensions, name, descAndTags);
  if (dimOverride != null) return dimOverride;

  const scores = {};

  for (const [category, keywords] of Object.entries(CATEGORY_KEYWORDS_FURNITURE)) {
    scores[category] = 0;
    for (const kw of keywords) {
      const isMultiWord = kw.trim().includes(" ");
      const inName = matchFurnitureKeyword(name, kw);
      const inDesc = hasDesc && matchFurnitureKeyword(descAndTags, kw);
      if (inName) scores[category] += isMultiWord ? 3 : 2;
      else if (inDesc) scores[category] += isMultiWord ? 1.5 : 1;
    }
  }

  if (CATEGORY_KEYWORDS_FURNITURE_WEAK) {
    for (const [category, weakKws] of Object.entries(CATEGORY_KEYWORDS_FURNITURE_WEAK)) {
      for (const kw of weakKws) {
        const inName = matchFurnitureKeyword(name, kw);
        const inDesc = hasDesc && matchFurnitureKeyword(descAndTags, kw);
        if (inName || inDesc) scores[category] = (scores[category] || 0) + 0.5;
      }
    }
  }

  let bestCategory = "Accessories";
  let bestScore = 0;
  for (const [category, score] of Object.entries(scores)) {
    if (score > bestScore) {
      bestScore = score;
      bestCategory = category;
    }
  }
  return bestScore > 0 ? bestCategory : "Accessories";
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

/* ======================================================
   DEPARTMENT & METAFIELDS FROM TYPE (authoritative)
   Do NOT invent categorization. Type is correct; derive Department and metafield from Type only.
   Priority: Department → department-specific metafield → tags. Never populate both metafields.
====================================================== */
function normalizeTypeForMatch(s) {
  if (s == null || typeof s !== "string") return "";
  return s.trim().toLowerCase().replace(/\s+/g, " ");
}

// Type signals: if Type contains these → Furniture & Home
const FURNITURE_TYPE_SIGNALS = [
  "furniture", "table", "tables", "seating", "case goods", "lighting", "decor", "mirrors", "mirror",
  "rugs", "rug", "home accessories", "living room", "dining room", "office", "bedroom",
  "art", "painting", "paintings", "outdoor", "patio", "lamps", "lamp",
];

// Type signals: if Type contains these → Luxury Goods
const LUXURY_TYPE_SIGNALS = [
  "handbag", "handbags", "bag", "bags", "wallet", "wallets", "belt", "belts",
  "scarf", "scarves", "fashion accessories", "wearable", "tote", "totes",
  "crossbody", "backpack", "backpacks", "luggage", "clutch", "small bag",
];

/** Returns "Furniture & Home" | "Luxury Goods" | null. null = use fallback (detectVertical). */
function getDepartmentFromType(productType) {
  const n = normalizeTypeForMatch(productType);
  if (!n) return null;
  for (const s of FURNITURE_TYPE_SIGNALS) {
    if (n.includes(s)) return "Furniture & Home";
  }
  for (const s of LUXURY_TYPE_SIGNALS) {
    if (n.includes(s)) return "Luxury Goods";
  }
  return null;
}

/** True when product_type is only a generic luxury bucket (accessories/wearable) with no specific bag/scarf/belt etc. */
function isGenericLuxuryTypeOnly(productType) {
  const n = normalizeTypeForMatch(productType);
  if (!n) return false;
  const generic = ["accessories", "wearable", "fashion accessories"];
  const specific = ["handbag", "handbags", "bag", "bags", "wallet", "wallets", "belt", "belts", "scarf", "scarves", "tote", "totes", "crossbody", "backpack", "backpacks", "luggage", "clutch", "small bag"];
  const hasGeneric = generic.some((s) => n.includes(s));
  const hasSpecific = specific.some((s) => n.includes(s));
  return hasGeneric && !hasSpecific;
}

/** True when title or description clearly indicate art or furniture (painting, canvas, art, mirror, rug, etc.). */
function hasFurnitureOrArtSignals(product) {
  const title = (product.title || "").toLowerCase();
  const desc = (product.body_html || "").replace(/<[^>]*>/g, " ").toLowerCase();
  const combined = [title, desc].join(" ");
  const signals = ["painting", "paintings", "canvas", "acrylic on canvas", "art", "mirror", "mirrors", "rug", "rugs", "furniture", "decor", "lamp", "lamps", "seating", "case goods", "living room", "dining room", "bedroom", "outdoor", "patio"];
  return signals.some((w) => combined.includes(w));
}

/** True when product is obviously luxury (jewelry, earring, bracelet, pouch, or clearly a luxury accessory).
 *  IMPORTANT: Brand alone is NOT enough — we require accessory/jewelry cues so furniture/housewares from luxury brands stay in Furniture & Home.
 */
function isClearlyLuxury(product) {
  const title = (product.title || "").toLowerCase();
  const tagsStr = getProductTagsArray(product).join(" ").toLowerCase();
  const desc = (product.body_html || "").replace(/<[^>]*>/g, " ").toLowerCase();
  const combined = [title, tagsStr, desc].join(" ");
  const clearlyLuxuryWords = [
    "jewelry", "earring", "earrings", "bracelet", "bracelets", "necklace", "necklaces",
    "pouch", "designer accessories", "statement jewelry", "costume jewelry", "luxury-collection"
  ];
  if (clearlyLuxuryWords.some((w) => combined.includes(w))) return true;
  // Brand-based override ONLY when it looks like a wearable/accessory (bag, clutch, scarf, wallet, belt, etc.).
  const isLuxuryBrand = detectBrandFromProduct(product.title, product.vendor);
  if (isLuxuryBrand) {
    const accessoryWords = [
      "bag", "bags", "handbag", "handbags", "tote", "totes", "crossbody",
      "wallet", "wallets", "clutch", "clutches", "backpack", "backpacks",
      "scarf", "scarves", "belt", "belts", "pouch", "small bag"
    ];
    if (accessoryWords.some((w) => combined.includes(w))) return true;
  }
  return false;
}

// Existing taxonomy: Furniture & Home metafield values (exact)
const FURNITURE_TAXONOMY = ["Living Room", "Dining Room", "Office Den", "Rugs", "Art / Mirrors", "Bedroom", "Accessories", "Outdoor / Patio", "Lighting"];
// Type → Furniture & Home metafield value (normalized type key → taxonomy value)
const TYPE_TO_FURNITURE_CATEGORY = {
  "living room": "Living Room", "dining room": "Dining Room", "office den": "Office Den", "office": "Office Den",
  "rugs": "Rugs", "rug": "Rugs", "art / mirrors": "Art / Mirrors", "art mirrors": "Art / Mirrors", "art": "Art / Mirrors", "mirrors": "Art / Mirrors", "mirror": "Art / Mirrors", "painting": "Art / Mirrors", "paintings": "Art / Mirrors",
  "bedroom": "Bedroom", "accessories": "Accessories", "outdoor / patio": "Outdoor / Patio", "outdoor": "Outdoor / Patio", "patio": "Outdoor / Patio",
  "lighting": "Lighting", "lamps": "Lighting", "lamp": "Lighting", "table": "Living Room", "tables": "Living Room", "seating": "Living Room", "decor": "Accessories", "case goods": "Living Room", "home accessories": "Accessories",
};

// Existing taxonomy: Luxury Goods metafield values (exact)
const LUXURY_TAXONOMY = ["Handbags", "Totes", "Crossbody", "Small Bags", "Backpacks", "Wallets", "Luggage", "Scarves", "Belts", "Accessories", "Other ", "Recently Sold"];
const TYPE_TO_LUXURY_CATEGORY = {
  "handbag": "Handbags", "handbags": "Handbags", "tote": "Totes", "totes": "Totes", "crossbody": "Crossbody",
  "small bag": "Small Bags", "backpack": "Backpacks", "backpacks": "Backpacks", "wallet": "Wallets", "wallets": "Wallets",
  "luggage": "Luggage", "scarf": "Scarves", "scarves": "Scarves", "belt": "Belts", "belts": "Belts",
  "clutch": "Small Bags", "bag": "Handbags", "bags": "Handbags", "fashion accessories": "Accessories", "wearable": "Accessories", "accessories": "Accessories",
};

function getFurnitureCategoryFromType(productType) {
  const n = normalizeTypeForMatch(productType);
  if (!n) return "Accessories";
  return TYPE_TO_FURNITURE_CATEGORY[n] ?? "Accessories";
}

function getLuxuryCategoryFromType(productType, soldNow) {
  if (soldNow) return "Recently Sold";
  const n = normalizeTypeForMatch(productType);
  if (!n) return "Other ";
  return TYPE_TO_LUXURY_CATEGORY[n] ?? "Other ";
}

/** Word-boundary match for keyword in text. */
function matchWordBoundary(text, keyword) {
  if (!text || !keyword) return false;
  const k = String(keyword).trim().toLowerCase();
  if (!k) return false;
  const escaped = k.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  try {
    return new RegExp("\\b" + escaped + "\\b", "i").test(String(text));
  } catch {
    return String(text).toLowerCase().includes(k);
  }
}

/** Shoe keywords — explicitly route to Other (no Handbags/Totes/etc. category for footwear). */
const SHOE_KEYWORDS = [
  "sneakers", "sneaker", "pumps", "pump", "heels", "heel", "flats", "flat",
  "boots", "boot", "sandals", "sandal", "loafers", "loafer", "mules", "mule",
  "slides", "slide", "ballerina", "oxfords", "oxford", "footwear", "shoes", "shoe",
];

/** True if title/description indicate footwear — checked before product_type so shoes with type "Accessories" become Other. */
function isShoeProduct(title, descriptionHtml) {
  const stripHtml = (html) => (html || "").replace(/<[^>]*>/g, " ").replace(/\s+/g, " ").trim();
  const text = [(title || "").trim(), stripHtml(descriptionHtml || "")].filter(Boolean).join(" ").toLowerCase();
  return text && SHOE_KEYWORDS.some((kw) => matchWordBoundary(text, kw));
}

/** Detect luxury category from title/description when product_type is empty or unmatched. Title-first: match on title before description so accessory mentions (e.g. "comes with clutch") don't override the main product. */
function detectLuxuryCategoryFromTitle(title, descriptionHtml) {
  const stripHtml = (html) => (html || "").replace(/<[^>]*>/g, " ").replace(/\s+/g, " ").trim();
  const titleText = (title || "").trim().toLowerCase();
  const descText = stripHtml(descriptionHtml || "").trim().toLowerCase();
  const combined = [titleText, descText].filter(Boolean).join(" ");
  if (!combined) return null;
  if (SHOE_KEYWORDS.some((kw) => matchWordBoundary(combined, kw))) return null;
  const tryMatch = (text) => {
    if (!text) return null;
    for (const [category, keywords] of Object.entries(CATEGORY_KEYWORDS)) {
      if (!Array.isArray(keywords)) continue;
      for (const kw of keywords) {
        if (matchWordBoundary(text, kw)) return category;
      }
    }
    return null;
  };
  return tryMatch(titleText) ?? tryMatch(descText);
}

/** In-memory map: display name (and slug) -> Webflow category item ID. Filled by loadFurnitureCategoryMap(). */
let furnitureCategoryMapCache = null;

/** Fetch Categories collection from Webflow and build name/slug -> item ID map so we don't need env vars. */
async function loadFurnitureCategoryMap() {
  if (furnitureCategoryMapCache) return furnitureCategoryMapCache;
  const siteId = process.env.RESALE_WEBFLOW_SITE_ID;
  const token = process.env.RESALE_TOKEN;
  if (!siteId || !token) {
    webflowLog("info", { event: "furniture_categories.skip", reason: "missing RESALE_WEBFLOW_SITE_ID or RESALE_TOKEN" });
    return {};
  }
  try {
    const collResp = await axios.get(`https://api.webflow.com/v2/sites/${siteId}/collections`, {
      headers: { Authorization: `Bearer ${token}`, accept: "application/json" },
    });
    const collections = collResp.data?.collections ?? [];
    const categoriesColl = collections.find(
      (c) => (c.displayName && c.displayName.toLowerCase() === "categories") || (c.slug && c.slug.toLowerCase() === "category")
    );
    if (!categoriesColl?.id) {
      webflowLog("info", { event: "furniture_categories.skip", reason: "no Categories collection found" });
      return {};
    }
    const map = {};
    let offset = 0;
    const limit = 100;
    while (true) {
      const itemsResp = await axios.get(
        `https://api.webflow.com/v2/collections/${categoriesColl.id}/items`,
        { params: { limit, offset }, headers: { Authorization: `Bearer ${token}`, accept: "application/json" } }
      );
      const items = itemsResp.data?.items ?? [];
      for (const item of items) {
        const id = item.id;
        if (!id || !/^[a-f0-9]{24}$/i.test(id)) continue;
        const name = item.fieldData?.name ?? item.name;
        if (name && typeof name === "string") map[name.trim()] = id;
        const slug = item.fieldData?.slug ?? item.slug;
        if (slug && typeof slug === "string") map[slug.trim()] = id;
      }
      if (items.length < limit) break;
      offset += limit;
    }
    furnitureCategoryMapCache = map;
    webflowLog("info", { event: "furniture_categories.loaded", count: Object.keys(map).length });
    return map;
  } catch (err) {
    webflowLog("warn", { event: "furniture_categories.load_failed", message: err.message });
    return {};
  }
}

/** Pre-load Luxury CMS items once per sync → O(1) lookup instead of N×page scans. */
async function loadLuxuryItemIndex() {
  const config = getWebflowConfig("luxury");
  if (!config?.collectionId || !config?.token) return;
  const byShopifyId = new Map();
  const bySlug = new Map();
  const byUrl = new Map();
  let offset = 0;
  const limit = 100;
  while (true) {
    const url = `https://api.webflow.com/v2/collections/${config.collectionId}/items?limit=${limit}&offset=${offset}`;
    const resp = await axios.get(url, {
      headers: { Authorization: `Bearer ${config.token}`, accept: "application/json" },
    });
    const items = resp.data?.items ?? [];
    for (const item of items) {
      const fd = item.fieldData || {};
      const wfId = fd["shopify-product-id"] ? String(fd["shopify-product-id"]) : null;
      const wfUrl = fd["shopify-url"] ? String(fd["shopify-url"]).trim() : null;
      const wfSlug = (fd["slug"] || fd["shopify-slug-2"]) ? String(fd["slug"] || fd["shopify-slug-2"]).trim() : null;
      if (wfId) byShopifyId.set(wfId, item);
      if (wfUrl) byUrl.set(wfUrl, item);
      if (wfSlug) bySlug.set(wfSlug, item);
    }
    if (items.length < limit) break;
    offset += limit;
  }
  luxuryItemIndex = { byShopifyId, bySlug, byUrl };
  webflowLog("info", { event: "luxury_item_index.loaded", count: byShopifyId.size });
}

/** Normalize product name for index (so we can find by name and avoid duplicate creates). */
function normalizeProductNameForIndex(name) {
  return (name ?? "").trim().toLowerCase().replace(/\s+/g, " ");
}

/** Pre-load Furniture ecommerce products once per sync → O(1) lookup. */
async function loadFurnitureProductIndex() {
  const config = getWebflowConfig("furniture");
  if (!config?.siteId || !config?.token) return;
  const byShopifyId = new Map();
  const bySlug = new Map();
  const byName = new Map();
  let offset = 0;
  const limit = 100;
  while (true) {
    const url = `https://api.webflow.com/v2/sites/${config.siteId}/products?limit=${limit}&offset=${offset}`;
    const resp = await axios.get(url, {
      headers: { Authorization: `Bearer ${config.token}`, accept: "application/json" },
    });
    const raw = resp.data?.products ?? resp.data?.items ?? [];
    const list = Array.isArray(raw) ? raw : [];
    for (const listItem of list) {
      const product = listItem.product ?? listItem;
      const skus = listItem.skus ?? product.skus ?? [];
      const fd = product.fieldData || {};
      const wfId = fd["shopify-product-id"] ? String(fd["shopify-product-id"]) : null;
      const wfSlug = (fd["slug"] || fd["shopify-slug-2"]) ? String(fd["slug"] || fd["shopify-slug-2"]).trim() : null;
      const wfName = fd.name ?? product.name;
      const entry = { ...product, skus };
      if (wfId) byShopifyId.set(wfId, entry);
      if (wfSlug) bySlug.set(wfSlug, entry);
      const nameKey = normalizeProductNameForIndex(wfName);
      if (nameKey && !byName.has(nameKey)) byName.set(nameKey, entry);
    }
    if (list.length < limit) break;
    offset += limit;
  }
  furnitureProductIndex = { byShopifyId, bySlug, byName };
  webflowLog("info", { event: "furniture_product_index.loaded", count: byShopifyId.size, byName: byName.size });
}

/** Pre-load Furniture SKUs by product ID once per sync → O(1) lookup. */
async function loadFurnitureSkuIndex() {
  const config = getWebflowConfig("furniture");
  if (!config?.skuCollectionId || !config?.token) return;
  const byProductId = new Map();
  let offset = 0;
  const limit = 100;
  while (true) {
    const url = `https://api.webflow.com/v2/collections/${config.skuCollectionId}/items?limit=${limit}&offset=${offset}`;
    const resp = await axios.get(url, {
      headers: { Authorization: `Bearer ${config.token}`, accept: "application/json" },
    });
    const items = resp.data?.items ?? [];
    for (const item of items) {
      const productRef = item.fieldData?.product;
      if (productRef) byProductId.set(String(productRef), item);
    }
    if (items.length < limit) break;
    offset += limit;
  }
  furnitureSkuIndex = byProductId;
  webflowLog("info", { event: "furniture_sku_index.loaded", count: byProductId.size });
}

/** Ecommerce category must be an ItemRef (Webflow collection item ID). Uses Webflow Categories if loaded; else env vars. */
function resolveFurnitureCategoryRef(displayCategory) {
  if (!displayCategory || typeof displayCategory !== "string") return null;
  if (furnitureCategoryMapCache && furnitureCategoryMapCache[displayCategory]) {
    return furnitureCategoryMapCache[displayCategory];
  }
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
   Tags: "Width: 48", "Height: 18", "Depth: 18", "Weight: 10" (ecommerce tags; used as fallback).
   If missing → dimensions_status = "missing".
====================================================== */
function getProductTagsArray(product) {
  const t = product.tags;
  if (Array.isArray(t)) return t;
  if (typeof t === "string") return t.split(",").map((s) => s.trim()).filter(Boolean);
  return [];
}

/** Parse dimensions from Shopify product tags, e.g. "Width: 48", "Height: 18", "Depth: 18", "Weight: 10". */
function parseDimensionsFromTags(product) {
  const tags = getProductTagsArray(product);
  let width = null, height = null, length = null, weight = null;
  const num = "(\\d+(?:\\.\\d+)?)";
  for (const tag of tags) {
    const t = String(tag).trim();
    const wMatch = t.match(new RegExp("^Width:\\s*" + num + "$", "i"));
    if (wMatch) width = parseFloat(wMatch[1]);
    const hMatch = t.match(new RegExp("^Height:\\s*" + num + "$", "i"));
    if (hMatch) height = parseFloat(hMatch[1]);
    const dMatch = t.match(new RegExp("^Depth:\\s*" + num + "$", "i"));
    if (dMatch) length = parseFloat(dMatch[1]);
    const wtMatch = t.match(new RegExp("^Weight:\\s*" + num + "$", "i"));
    if (wtMatch) weight = parseFloat(wtMatch[1]);
  }
  return { width, height, length, weight };
}

function getDimensionsFromProduct(product) {
  const v = product.variants?.[0];
  let weight = v?.weight != null && v.weight > 0 ? Number(v.weight) : null;
  let width = null, height = null, length = null;
  const metafields = Array.isArray(product.metafields) ? product.metafields : [];
  for (const m of metafields) {
    if (m.namespace === "custom" && m.key === "width" && m.value) width = parseFloat(m.value);
    if (m.namespace === "custom" && m.key === "height" && m.value) height = parseFloat(m.value);
    if (m.namespace === "custom" && m.key === "length" && m.value) length = parseFloat(m.value);
  }
  const fromTags = parseDimensionsFromTags(product);
  if (width == null && fromTags.width != null && !Number.isNaN(fromTags.width)) width = fromTags.width;
  if (height == null && fromTags.height != null && !Number.isNaN(fromTags.height)) height = fromTags.height;
  if (length == null && fromTags.length != null && !Number.isNaN(fromTags.length)) length = fromTags.length;
  if (weight == null && fromTags.weight != null && !Number.isNaN(fromTags.weight) && fromTags.weight > 0) weight = fromTags.weight;
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

/** Format dimensions for description. Dimensions on one line, weight on the next. Uses <br> for HTML line breaks. Only called when hasAnyDimensions. */
function formatDimensionsForDescription(dims) {
  if (!dims || !hasAnyDimensions(dims)) return "";
  const sizeParts = [];
  if (dims.width != null && !Number.isNaN(dims.width)) sizeParts.push(`Width: ${dims.width}"`);
  if (dims.length != null && !Number.isNaN(dims.length)) sizeParts.push(`Depth: ${dims.length}"`);
  if (dims.height != null && !Number.isNaN(dims.height)) sizeParts.push(`Height: ${dims.height}"`);
  const hasWeight = dims.weight != null && !Number.isNaN(dims.weight) && dims.weight > 0;
  const sizeLine = sizeParts.length ? `Dimensions: ${sizeParts.join(" × ")}.` : "";
  const weightLine = hasWeight ? `Weight: ${dims.weight} lb.` : "";
  if (sizeLine && weightLine) return `${sizeLine}<br>${weightLine}`;
  return sizeLine || weightLine || "";
}

/** Strip existing dimensions/weight block(s) from description to prevent duplication.
 * Must match BOTH formats we emit:
 * 1) Dimensions: ... Weight: ... (furniture with size + weight)
 * 2) Weight: N lb. (handbags with weight only - was not matched before, causing repeated appends)
 * Also strips any number of trailing duplicate blocks from prior bug.
 */
function stripExistingDimensions(descriptionHtml) {
  if (!descriptionHtml || typeof descriptionHtml !== "string") return "";
  let s = descriptionHtml;
  // Strip trailing blocks in a loop (handles multiple duplicates from prior bug)
  for (let prev = ""; prev !== s; ) {
    prev = s;
    // Pattern 1: <br><br>Dimensions: ... (optional Weight: ...)
    s = s.replace(/(<br\s*\/?>\s*){2,}Dimensions:[\s\S]*?Weight:\s*[\d.]+?\s*lb\.?$/i, "").trim();
    s = s.replace(/(<br\s*\/?>\s*){2,}Dimensions:[\s\S]*?$/i, "").trim();
    // Pattern 2: <br><br>Weight: N lb. (weight-only — handbags)
    s = s.replace(/(<br\s*\/?>\s*){2,}Weight:\s*[\d.]+?\s*lb\.?$/i, "").trim();
    s = s.replace(/(<br\s*\/?>\s*)+Weight:\s*[\d.]+?\s*lb\.?$/i, "").trim();
  }
  return s.trim();
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
   SHOPIFY — FETCH SINGLE PRODUCT (confirm status before touching Webflow)
   Returns { status: 'active'|'archived'|'draft'|'gone' } or null on request failure (don't assume).
====================================================== */
async function fetchShopifyProductStatus(productId) {
  const store = process.env.SHOPIFY_STORE;
  const token = process.env.SHOPIFY_ACCESS_TOKEN;
  if (!store || !token) return null;
  const url = `https://${store}.myshopify.com/admin/api/2024-01/products/${productId}.json`;
  try {
    const response = await axios.get(url, {
      headers: {
        "X-Shopify-Access-Token": token,
        "Content-Type": "application/json",
      },
    });
    const product = response.data?.product;
    if (!product) return null;
    const status = (product.status || "").toLowerCase();
    if (status === "active") return { status: "active", product };
    if (status === "archived") return { status: "archived", product };
    if (status === "draft") return { status: "draft", product };
    return { status: status || "unknown", product };
  } catch (err) {
    if (err.response?.status === 404) return { status: "gone" };
    webflowLog("info", {
      event: "shopify_fetch_one.failed",
      shopifyProductId: productId,
      status: err.response?.status,
      message: err.message,
    });
    return null; // couldn't confirm — do not touch Webflow
  }
}

/* ======================================================
   WEBFLOW — STRONG MATCHER (ID / URL / SLUG)
   Uses config (collectionId + token) for target collection.
====================================================== */
/** Run-scoped index: Luxury CMS items. Populated at sync start for O(1) lookup. */
let luxuryItemIndex = null;

async function findExistingWebflowItem(shopifyProductId, shopifyUrl, slug, config) {
  if (!config?.collectionId || !config?.token) return null;
  const shopifyUrlNorm = shopifyUrl ? String(shopifyUrl).trim() : null;
  const slugNorm = slug ? String(slug).trim() : null;

  webflowLog("info", { event: "match_scan.start", shopifyProductId, shopifyUrl: shopifyUrlNorm, slug: slugNorm });

  // Use pre-loaded index when available (avoids N×page API calls per product)
  if (luxuryItemIndex) {
    const byId = luxuryItemIndex.byShopifyId?.get(String(shopifyProductId));
    if (byId) {
      webflowLog("info", { event: "match_scan.found", shopifyProductId, webflowItemId: byId.id, source: "index" });
      return byId;
    }
    if (shopifyUrlNorm) {
      const byUrl = luxuryItemIndex.byUrl?.get(shopifyUrlNorm);
      if (byUrl) {
        webflowLog("info", { event: "match_scan.found", shopifyProductId, webflowItemId: byUrl.id, source: "index" });
        return byUrl;
      }
    }
    if (slugNorm) {
      const bySlug = luxuryItemIndex.bySlug?.get(slugNorm);
      if (bySlug) {
        webflowLog("info", { event: "match_scan.found", shopifyProductId, webflowItemId: bySlug.id, source: "index" });
        return bySlug;
      }
    }
    webflowLog("info", { event: "match_scan.not_found", shopifyProductId, source: "index" });
    return null;
  }

  let offset = 0;
  const limit = 100;
  while (true) {
    const url = `https://api.webflow.com/v2/collections/${config.collectionId}/items?limit=${limit}&offset=${offset}`;
    let response;
    try {
      response = await axios.get(url, {
        headers: { Authorization: `Bearer ${config.token}`, accept: "application/json" },
      });
    } catch (err) {
      webflowLog("error", { event: "match_scan.error", shopifyProductId, offset, message: err.message, responseData: err.response?.data });
      return null;
    }
    const items = response.data.items || [];
    webflowLog("info", { event: "match_scan.page", shopifyProductId, offset, itemCount: items.length });
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
      if (idMatch || urlMatch || slugMatch) {
        webflowLog("info", { event: "match_scan.found", shopifyProductId, webflowItemId: item.id });
        return item;
      }
    }
    if (items.length < limit) break;
    offset += limit;
  }
  webflowLog("info", { event: "match_scan.not_found", shopifyProductId });
  return null;
}

/* ======================================================
   FURNITURE SKU — find by product reference (required for PATCH)
====================================================== */
/** Run-scoped index: Furniture SKUs by product ID. Populated at sync start for O(1) lookup. */
let furnitureSkuIndex = null;

async function findExistingSkuByProductId(skuCollectionId, webflowProductId, token) {
  if (!skuCollectionId || !token || !webflowProductId) return null;

  // Use pre-loaded index when available
  if (furnitureSkuIndex) {
    return furnitureSkuIndex.get(String(webflowProductId)) ?? null;
  }

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
      if (productRef && String(productRef) === String(webflowProductId)) return item;
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
    webflowLog("info", { event: "furniture_sku.patch", shopifyProductId: product.id, webflowProductId, skuId: existingSku.id });
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
    webflowLog("info", { event: "furniture_sku.create", shopifyProductId: product.id, webflowProductId });
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
async function syncSingleProduct(product, cache, options = {}) {
  const shopifyProductId = String(product.id);
  const cacheEntry = getCacheEntry(cache, shopifyProductId);
  const duplicateEmailSentFor = options.duplicateEmailSentFor ?? null;

  // Department/vertical from Type first (authoritative). Override: if Type says Furniture but product is clearly luxury (jewelry, earring, bracelet, pouch, luxury brand), use luxury.
  // Also: if Type is generic "Accessories" (Luxury bucket) but title/description are clearly art or furniture (painting, canvas, art, etc.), keep in Furniture & Home.
  const productTypeForVertical = (product.product_type ?? "").trim();
  let departmentFromType = getDepartmentFromType(productTypeForVertical);
  if (departmentFromType === "Luxury Goods" && isGenericLuxuryTypeOnly(productTypeForVertical) && hasFurnitureOrArtSignals(product)) {
    departmentFromType = "Furniture & Home";
  }
  const detectedVerticalRaw = detectVertical(product);
  const detectedVertical = detectedVerticalRaw === "luxury" && hasFurnitureOrArtSignals(product) ? "furniture" : detectedVerticalRaw;
  let vertical;
  if (departmentFromType === "Furniture & Home" && isClearlyLuxury(product)) {
    vertical = "luxury";
  } else if (departmentFromType != null) {
    vertical = departmentFromType === "Furniture & Home" ? "furniture" : "luxury";
  } else {
    vertical =
      cacheEntry?.vertical === "furniture" && detectedVertical === "luxury"
        ? "luxury"
        : (cacheEntry?.vertical ?? detectedVertical);
  }
  const verticalCorrected = cacheEntry?.vertical === "furniture" && detectedVertical === "luxury";
  webflowLog("info", {
    event: "vertical.resolved",
    shopifyProductId,
    fromType: departmentFromType != null,
    detectedVertical,
    cacheVertical: cacheEntry?.vertical ?? null,
    vertical,
    corrected: verticalCorrected,
  });

  // When we correct furniture → luxury, remove the mistaken product from Webflow (archive) and clear cache so we create in luxury.
  // If it's already archived, don't re-archive and don't send duplicate email.
  if (verticalCorrected && cacheEntry?.webflowId && vertical === "luxury") {
    const furnitureConfig = getWebflowConfig("furniture");
    let alreadyArchived = false;
    if (furnitureConfig?.siteId && furnitureConfig?.token) {
      const full = await getWebflowEcommerceProductById(furnitureConfig.siteId, cacheEntry.webflowId, furnitureConfig.token);
      alreadyArchived = full?.isArchived === true;
      if (alreadyArchived) {
        webflowLog("info", { event: "vertical.corrected.skipped_already_archived", shopifyProductId, webflowId: cacheEntry.webflowId });
        saveDuplicatePlacementSentId(shopifyProductId);
      } else {
        try {
          await deleteOrArchiveWebflowEcommerceProduct(furnitureConfig.siteId, cacheEntry.webflowId, furnitureConfig.token);
          webflowLog("info", { event: "vertical.corrected.removed", shopifyProductId, webflowId: cacheEntry.webflowId });
        } catch (err) {
          webflowLog("error", { event: "vertical.corrected.archive_failed", shopifyProductId, webflowId: cacheEntry.webflowId, message: err.message });
        }
      }
    }
    const duplicateLog = {
      productTitle: product.title || "",
      shopifyProductId,
      previousVertical: "furniture",
      detectedVertical: "luxury",
      webflowIdArchived: cacheEntry.webflowId,
    };
    delete cache[shopifyProductId];
    const result = await syncSingleProduct(product, cache, options);
    return { ...result, duplicateCorrected: !alreadyArchived, duplicateLog };
  }

  // Cleanup: ensure this product exists only in the current vertical. Remove from the other vertical if found.
  const slugForCleanup = product.handle || "";
  const shopifyUrlForCleanup = `https://${process.env.SHOPIFY_STORE || ""}.myshopify.com/products/${slugForCleanup}`;

  if (vertical === "luxury") {
    const furnitureConfig = getWebflowConfig("furniture");
    if (furnitureConfig?.siteId && furnitureConfig?.token) {
      const existingInFurniture = await findExistingWebflowEcommerceProduct(shopifyProductId, slugForCleanup, furnitureConfig);
      if (existingInFurniture) {
        const full = await getWebflowEcommerceProductById(furnitureConfig.siteId, existingInFurniture.id, furnitureConfig.token);
        const alreadyArchived = full?.isArchived === true;
        if (alreadyArchived) {
          webflowLog("info", { event: "cleanup.skipped_already_archived", shopifyProductId, webflowId: existingInFurniture.id });
          saveDuplicatePlacementSentId(shopifyProductId);
        } else {
          webflowLog("info", {
            event: "cleanup.found_in_other_vertical",
            shopifyProductId,
            currentVertical: "luxury",
            otherVertical: "furniture",
            webflowId: existingInFurniture.id,
            productTitle: product.title,
          });
          try {
            await deleteOrArchiveWebflowEcommerceProduct(furnitureConfig.siteId, existingInFurniture.id, furnitureConfig.token);
            webflowLog("info", { event: "cleanup.removed_from_furniture", shopifyProductId, webflowId: existingInFurniture.id });
            await sendDuplicatePlacementEmail(
              {
                productTitle: product.title || "",
                shopifyProductId,
                previousVertical: "furniture",
                detectedVertical: "luxury",
                webflowIdArchived: existingInFurniture.id,
                sweep: true,
              },
              duplicateEmailSentFor
            );
          } catch (err) {
            webflowLog("error", { event: "cleanup.remove_furniture_failed", shopifyProductId, webflowId: existingInFurniture.id, message: err.message });
          }
        }
      }
    }
  }

  if (vertical === "furniture") {
    const luxuryConfig = getWebflowConfig("luxury");
    if (luxuryConfig?.collectionId && luxuryConfig?.token) {
      const existingInLuxury = await findExistingWebflowItem(shopifyProductId, shopifyUrlForCleanup, slugForCleanup, luxuryConfig);
      if (existingInLuxury) {
        webflowLog("info", {
          event: "cleanup.found_in_other_vertical",
          shopifyProductId,
          currentVertical: "furniture",
          otherVertical: "luxury",
          webflowId: existingInLuxury.id,
          productTitle: product.title,
        });
        try {
          await deleteWebflowCollectionItem(luxuryConfig.collectionId, existingInLuxury.id, luxuryConfig.token);
          webflowLog("info", { event: "cleanup.deleted_from_luxury", shopifyProductId, webflowId: existingInLuxury.id });
          await sendDuplicatePlacementEmail(
            {
              productTitle: product.title || "",
              shopifyProductId,
              previousVertical: "luxury",
              detectedVertical: "furniture",
              webflowIdArchived: existingInLuxury.id,
              sweep: true,
            },
            duplicateEmailSentFor
          );
        } catch (err) {
          webflowLog("error", { event: "cleanup.delete_luxury_failed", shopifyProductId, webflowId: existingInLuxury.id, message: err.message });
        }
      }
    }
  }

  const config = getWebflowConfig(vertical);

  // Use name + description FROM Shopify to decide; then write our decision back to Shopify and update the correct Webflow collection (Luxury or Furniture).
  const previousQty = cacheEntry?.lastQty ?? null;
  let name = product.title;
  let description = product.body_html;
  let price = product.variants?.[0]?.price || null;
  let qty = product.variants?.[0]?.inventory_quantity ?? null;
  let slug = product.handle;

  let detectedBrand =
    vertical === "furniture"
      ? detectBrandFromProductFurniture(product.title, product.body_html, product.vendor)
      : detectBrandFromProduct(product.title, product.vendor);
  const brand = detectedBrand || "Unknown";

  const allImages = (product.images || []).map((img) => img.src);
  const featuredImage = allImages[0] || null;
  const gallery = allImages.slice(1);

  const soldNow = qty !== null && qty <= 0;

  // Single source of truth: resolved VERTICAL. Department and category are derived from it everywhere (Webflow + Shopify).
  const productType = (product.product_type ?? "").trim();
  const department = vertical === "furniture" ? "Furniture & Home" : "Luxury Goods";
  let dimensionsStatus = null;
  const dimensions = getDimensionsFromProduct(product);
  const categoryForMetafield =
    vertical === "furniture"
      ? mapFurnitureCategoryForShopify(detectCategoryFurniture(name, description, getProductTagsArray(product), dimensions))
      : (() => {
          if (isShoeProduct(name, description)) return "Other ";
          const fromTitle = detectLuxuryCategoryFromTitle(name, description);
          if (fromTitle != null) return fromTitle;
          return getLuxuryCategoryFromType(productType, soldNow) ?? "Other ";
        })();
  const shopifyDepartment = department;
  const shopifyCategoryValue = categoryForMetafield;
  const category = shopifyCategoryValue;

  const showOnWebflow = vertical === "luxury" ? !soldNow : true;
  const shopifyUrl = `https://${process.env.SHOPIFY_STORE}.myshopify.com/products/${slug}`;

  // Dimensions status + append to description when present
  const originalDescription = product.body_html || "";
  let descriptionChanged = false;
  
  if (vertical === "furniture") {
    dimensionsStatus = hasAnyDimensions(dimensions) ? "present" : "missing";
  }
  if (hasAnyDimensions(dimensions)) {
    const dimStr = formatDimensionsForDescription(dimensions);
    if (dimStr) {
      // Strip any existing dimensions block to prevent duplication
      const body = stripExistingDimensions(description || "").trim();
      // Both luxury and furniture: dimensions at end, on own line(s), weight underneath
      const newDescription = (body + "<br><br>" + dimStr).trim();
      
      // Only update if description actually changed
      if (newDescription !== originalDescription) {
        description = newDescription;
        descriptionChanged = true;
      }
    }
  }

  // Remove "Condition" option only for products we're actually syncing as Furniture.
  if (vertical === "furniture") {
    await removeConditionOptionIfFurniture(product);
  }

  // Write metafields + vendor/type/tags to Shopify so Shopify matches the vertical we're syncing to Webflow.
  if (shopifyCategoryValue !== "Recently Sold") {
    await updateShopifyMetafields(shopifyProductId, {
      department: shopifyDepartment,
      category: shopifyCategoryValue,
      vertical: vertical === "furniture" ? "furniture" : "luxury",
      dimensionsStatus: vertical === "furniture" ? dimensionsStatus : undefined,
    });
    // Only pass description if it changed
    await updateShopifyVendorAndType(
      shopifyProductId, 
      brand, 
      shopifyCategoryValue, 
      getProductTagsArray(product), 
      shopifyDepartment, 
      shopifyCategoryValue, 
      descriptionChanged ? description : null
    );
  }

  const currentHash = shopifyHash(product);

  // RULE: Only touch Webflow when (1) item removed from Shopify, or (2) item changed in Shopify from previous run.
  // Otherwise we do not call Webflow at all. When we do go to Webflow we retrieve the item, compare to what we'd send, and only update if there is a difference.
  const previousHash = cacheEntry?.hash || null;
  const hashUnchanged = previousHash && JSON.stringify(currentHash) === JSON.stringify(previousHash);
  const newlySoldCheck = (previousQty === null || previousQty > 0) && qty !== null && qty <= 0;
  if (cacheEntry?.webflowId && hashUnchanged && !newlySoldCheck) {
    webflowLog("info", { event: "sync_product.skip_early_no_webflow", shopifyProductId, productTitle: name, webflowId: cacheEntry.webflowId, reason: "shopify_unchanged" });
    cache[shopifyProductId] = {
      hash: currentHash,
      webflowId: cacheEntry.webflowId,
      lastQty: qty,
      vertical,
    };
    webflowLog("info", { event: "cache.mutated", shopifyProductId, op: "skip", webflowId: cacheEntry.webflowId, vertical });
    return { operation: "skip", id: cacheEntry.webflowId };
  }

  webflowLog("info", {
    event: "sync_product.entry",
    shopifyProductId,
    productTitle: name,
    vertical,
    cacheWebflowId: cacheEntry?.webflowId ?? null,
    previousQty,
    currentQty: qty,
    department: shopifyDepartment,
    category,
    categoryChild: shopifyCategoryValue,
    soldNow,
    shopifyUrl,
    slug,
    ...(vertical === "furniture" && { dimensionsStatus }),
  });

  // Retrieve from Webflow only when we might need to update (hash changed or no cache). Then compare; only PATCH if different.
  let existing = null;
  if (vertical === "furniture" && config.siteId) {
    if (cacheEntry?.webflowId) {
      webflowLog("info", { event: "sync_product.try_cache", shopifyProductId, cacheWebflowId: cacheEntry.webflowId, target: "ecommerce" });
      existing = await getWebflowEcommerceProductById(config.siteId, cacheEntry.webflowId, config.token);
      if (!existing) webflowLog("info", { event: "sync_product.cache_miss", shopifyProductId, reason: "ecommerce_not_found" });
    }
    if (!existing) {
      existing = await findExistingWebflowEcommerceProduct(shopifyProductId, slug, config, name);
    }
  } else {
    if (cacheEntry?.webflowId) {
      webflowLog("info", { event: "sync_product.try_cache", shopifyProductId, cacheWebflowId: cacheEntry.webflowId, target: "cms" });
      existing = await getWebflowItemById(cacheEntry.webflowId, config);
      if (!existing) webflowLog("info", { event: "sync_product.cache_miss", shopifyProductId, reason: "cms_not_found" });
    }
    if (!existing) {
      existing = await findExistingWebflowItem(shopifyProductId, shopifyUrl, slug, config);
    }
  }

  if (existing) {
    webflowLog("info", { event: "sync_product.linked", shopifyProductId, productTitle: name, webflowId: existing.id });

    const newlySold =
      (previousQty === null || previousQty > 0) && qty !== null && qty <= 0;

    if (newlySold) {
      webflowLog("info", { event: "sync_product.newly_sold", shopifyProductId, productTitle: name, webflowId: existing.id, vertical });
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
      const existingFD = existing.fieldData || {};
      if (fieldDataEffectivelyEqual(fieldData, existingFD)) {
        webflowLog("info", { event: "sync_product.skip_webflow_unchanged", shopifyProductId, productTitle: name, webflowId: existing.id, message: "Retrieved from Webflow; same as Shopify would send; not touching" });
        cache[shopifyProductId] = {
          hash: currentHash,
          webflowId: existing.id,
          lastQty: qty,
          vertical,
        };
        webflowLog("info", { event: "cache.mutated", shopifyProductId, op: "skip", webflowId: existing.id, vertical });
        return { operation: "skip", id: existing.id };
      }
      webflowLog("info", { event: "sync_product.updating", shopifyProductId, productTitle: name, webflowId: existing.id, reason: "shopify_changed_and_webflow_differs" });
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

    webflowLog("info", { event: "sync_product.skip_no_changes", shopifyProductId, productTitle: name, webflowId: existing.id });
    cache[shopifyProductId] = {
      hash: currentHash,
      webflowId: existing.id,
      lastQty: qty,
      vertical,
    };
    webflowLog("info", { event: "cache.mutated", shopifyProductId, op: "skip", webflowId: existing.id, vertical });
    return { operation: "skip", id: existing.id };
  }

  // Create when no existing item found — either no cache, or cache pointed to deleted Webflow item
  if (!existing) {
    // Don't recreate sold items (qty 0). If you deleted them from Webflow, we won't push them back.
    if (soldNow) {
      webflowLog("info", {
        event: "sync_product.skip_create_sold",
        shopifyProductId,
        productTitle: name,
        reason: "qty 0 — not recreating deleted sold items",
      });
      return { operation: "skip", id: null };
    }

    webflowLog("info", {
      event: "sync_product.create_path",
      shopifyProductId,
      productTitle: name,
      message: cacheEntry ? "CACHE MISS (Webflow item deleted) → Creating new" : "NO CACHE + NO MATCH → Creating new Webflow item",
    });

    // Sweep: if we're creating in Luxury, check if this product wrongly exists in Furniture (e.g. no cache / cache lost). Archive it so it doesn't stay in both places.
    if (detectedVertical === "luxury") {
      const furnitureConfig = getWebflowConfig("furniture");
      if (furnitureConfig?.siteId && furnitureConfig?.token) {
        const existingInFurniture = await findExistingWebflowEcommerceProduct(shopifyProductId, slug, furnitureConfig, name);
        if (existingInFurniture) {
          const full = await getWebflowEcommerceProductById(furnitureConfig.siteId, existingInFurniture.id, furnitureConfig.token);
          const alreadyArchived = full?.isArchived === true;
          if (alreadyArchived) {
            webflowLog("info", { event: "sweep.skipped_already_archived", shopifyProductId, webflowId: existingInFurniture.id });
            saveDuplicatePlacementSentId(shopifyProductId);
          } else {
            webflowLog("info", {
              event: "sweep.found_in_furniture",
              shopifyProductId,
              webflowId: existingInFurniture.id,
              productTitle: name,
              message: "Archiving from Furniture before creating in Luxury",
            });
            try {
              await deleteOrArchiveWebflowEcommerceProduct(furnitureConfig.siteId, existingInFurniture.id, furnitureConfig.token);
              webflowLog("info", { event: "sweep.removed_from_furniture", shopifyProductId, webflowId: existingInFurniture.id });
              await sendDuplicatePlacementEmail(
                {
                  productTitle: name,
                  shopifyProductId,
                  previousVertical: "furniture",
                  detectedVertical: "luxury",
                  webflowIdArchived: existingInFurniture.id,
                  sweep: true,
                },
                duplicateEmailSentFor
              );
            } catch (err) {
              webflowLog("error", { event: "sweep.remove_furniture_failed", shopifyProductId, webflowId: existingInFurniture.id, message: err.message });
            }
          }
        }
      }
    }

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
      webflowLog("info", { event: "create.ecommerce.start", shopifyProductId, productTitle: name, siteId: createConfig.siteId });
      try {
        const createResp = await createWebflowEcommerceProduct(
          createConfig.siteId,
          productFieldData,
          skuFieldData,
          createConfig.token
        );
        newId = createResp.product?.id;
        if (!newId) throw new Error("No product.id in ecommerce create response");
        webflowLog("info", { event: "create.ecommerce.ok", shopifyProductId, productTitle: name, webflowId: newId });
      } catch (createErr) {
        webflowLog("error", {
          event: "create.ecommerce.failed",
          shopifyProductId,
          productTitle: name,
          message: createErr.message,
          status: createErr.response?.status,
          responseBody: createErr.response?.data,
        });
        throw createErr;
      }
    } else {
      webflowLog("info", { event: "create.cms.start", shopifyProductId, productTitle: name, collectionId: createConfig.collectionId });
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
        webflowLog("error", {
          event: "create.cms.failed",
          shopifyProductId,
          productTitle: name,
          message: createErr.message,
          status: createErr.response?.status,
          responseBody: createErr.response?.data,
        });
        throw createErr;
      }
      newId = resp.data.id;
      webflowLog("info", { event: "create.cms.ok", shopifyProductId, productTitle: name, webflowId: newId });
    }
    cache[shopifyProductId] = {
      hash: currentHash,
      webflowId: newId,
      lastQty: qty,
      vertical: detectedVertical,
    };
    webflowLog("info", { event: "cache.mutated", shopifyProductId, op: "create", webflowId: newId, vertical: detectedVertical });
    return { operation: "create", id: newId };
  }

  // Should not reach: existing was non-null but we didn't update/skip/sold
  return { operation: "skip", id: null };
}

/* ======================================================
   COMPARE FIELD DATA — skip PATCH when nothing actually changed
   Avoids touching Webflow (no Modified bump, no unpublished changes).
====================================================== */
function fieldDataEffectivelyEqual(newFD, existingFD) {
  if (!newFD || typeof newFD !== "object") return !existingFD;
  if (!existingFD || typeof existingFD !== "object") return false;
  const strNorm = (v) => (v == null ? "" : String(v).replace(/\s+/g, " ").trim());
  const priceNorm = (v) => {
    if (v == null) return null;
    const num = parseFloat(String(v).replace(/[^0-9.-]/g, ""));
    return Number.isNaN(num) ? String(v) : num;
  };
  for (const key of Object.keys(newFD)) {
    let n = newFD[key];
    let e = existingFD[key];
    if (n === e) continue;
    if (n == null && e == null) continue;
    if (typeof n === "object" && n !== null && typeof e === "object" && e !== null) {
      if (n.url != null && e.url != null && n.url === e.url) continue;
      if (JSON.stringify(n) === JSON.stringify(e)) continue;
    }
    if (key === "description" || key === "body_html") {
      if (strNorm(n) === strNorm(e)) continue;
    }
    if (key === "price" && priceNorm(n) === priceNorm(e)) continue;
    if (["name", "brand", "slug", "shopify-product-id", "shopify-url"].includes(key) && strNorm(n) === strNorm(e)) continue;
    if (key === "category") {
      const nRef = typeof n === "string" && WEBFLOW_ITEM_REF_REGEX.test(n);
      const eRef = typeof e === "string" && WEBFLOW_ITEM_REF_REGEX.test(e);
      if (nRef && eRef && n === e) continue;
      if ((nRef && typeof e === "string" && e.trim()) || (eRef && typeof n === "string" && n.trim())) continue;
    }
    if (String(n) === String(e)) continue;
    return false;
  }
  return true;
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
    if (categoryRef == null) {
      const envKey = `FURNITURE_CATEGORY_${category.replace(/\s*\/\s*/g, "_").replace(/\s+/g, "_").toUpperCase().replace(/[^A-Z0-9_]/g, "")}`;
      webflowLog("warn", { event: "build_field_data.category_unassigned", category, envKey, message: `Set ${envKey} to Webflow category item ID` });
    }
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
    if (categoryRef != null && WEBFLOW_ITEM_REF_REGEX.test(String(categoryRef))) out.category = categoryRef;
    return out;
  }

  // Webflow rejects "Other " (trailing space); use "Other" for Luxury CMS. Never send Furniture-only categories (Dining Room, etc.) to Luxury.
  const luxuryCategory = category && FURNITURE_TAXONOMY.includes(category) ? "Other " : (category ?? "Other ");
  const webflowCategory = (luxuryCategory && luxuryCategory.trimEnd() === "Other") ? "Other" : (luxuryCategory ?? "");
  const base = {
    name,
    brand,
    price,
    description,
    "shopify-product-id": shopifyProductId,
    "shopify-url": shopifyUrl,
    category: webflowCategory,
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
  syncRequestId = crypto.randomUUID().slice(0, 8);
  syncStartTime = Date.now();
  webflowLog("info", { event: "sync-all.entry", message: "sync-all started" });
  try {
    const products = await fetchAllShopifyProducts();
    webflowLog("info", { event: "sync-all.fetched_shopify", productCount: products?.length ?? 0 });
    const cache = loadCache();
    webflowLog("info", { event: "sync-all.loaded", productCount: products?.length ?? 0, cacheKeys: Object.keys(cache).length });

    await loadFurnitureCategoryMap();

    // Pre-load Webflow indexes once → O(1) lookups instead of N×page API scans per product
    luxuryItemIndex = null;
    furnitureProductIndex = null;
    furnitureSkuIndex = null;
    await Promise.all([
      loadLuxuryItemIndex(),
      loadFurnitureProductIndex(),
      loadFurnitureSkuIndex(),
    ]);

    let created = 0,
      updated = 0,
      skipped = 0,
      sold = 0;

    // Disappeared: in cache but not in this run's product list. Only touch Webflow when we've confirmed in Shopify that the product is not active.
    const previousIds = Object.keys(cache);
    const currentIds = products.map((p) => String(p.id));
    const disappeared = previousIds.filter((id) => !currentIds.includes(id));

    const disappearedConcurrency = Math.min(10, Math.max(3, disappeared.length));
    webflowLog("info", {
      event: "sync-all.disappeared_check",
      previousIds: previousIds.length,
      currentIds: currentIds.length,
      disappeared: disappeared.length,
      disappearedConcurrency,
    });

    // Run Shopify status checks in parallel (capped) so many disappeared don't slow the run.
    const confirmedById = {};
    for (let i = 0; i < disappeared.length; i += disappearedConcurrency) {
      const chunk = disappeared.slice(i, i + disappearedConcurrency);
      const results = await Promise.all(chunk.map((id) => fetchShopifyProductStatus(id)));
      chunk.forEach((id, idx) => {
        confirmedById[id] = results[idx];
      });
    }

    for (const goneId of disappeared) {
      const confirmed = confirmedById[goneId];
      if (confirmed === null || confirmed === undefined) {
        webflowLog("info", {
          event: "sync-all.disappeared_skip_unconfirmed",
          shopifyProductId: goneId,
          reason: "fetch_failed_or_unknown",
        });
        continue;
      }
      if (confirmed.status === "active") {
        webflowLog("info", {
          event: "sync-all.disappeared_skip_still_active",
          shopifyProductId: goneId,
          reason: "product_still_active_in_shopify",
        });
        continue;
      }
      if (confirmed.status !== "gone" && confirmed.status !== "archived" && confirmed.status !== "draft") {
        webflowLog("info", {
          event: "sync-all.disappeared_skip_unconfirmed",
          shopifyProductId: goneId,
          reason: "shopify_status_unknown",
          status: confirmed.status,
        });
        continue;
      }
      const entry = getCacheEntry(cache, goneId);
      const vertical = entry?.vertical ?? "luxury";
      const config = getWebflowConfig(vertical);
      let existing = null;

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
        webflowLog("info", {
          event: "sync-all.disappeared_mark_sold_confirmed",
          shopifyProductId: goneId,
          webflowId: existing.id,
          vertical,
          shopifyStatus: confirmed.status,
        });
        await markAsSold(existing, vertical, config);
        sold++;
      } else {
        webflowLog("info", { event: "sync-all.disappeared_no_webflow", shopifyProductId: goneId });
      }

      delete cache[goneId];
      webflowLog("info", { event: "cache.mutated", shopifyProductId: goneId, op: "deleted", reason: "disappeared_confirmed" });
    }

    const duplicateEmailSentFor = new Set();
    const concurrency = Math.min(Math.max(1, parseInt(process.env.SYNC_CONCURRENCY || "3", 10) || 1), 15);

    for (let i = 0; i < products.length; i += concurrency) {
      const chunk = products.slice(i, i + concurrency);
      const results = await Promise.all(chunk.map((p) => syncSingleProduct(p, cache, { duplicateEmailSentFor })));

      for (const result of results) {
        if (result.duplicateCorrected && result.duplicateLog) {
          webflowLog("info", {
            event: "sync-all.duplicate_placement",
            message: "Item was in Furniture but detected as Luxury; archived and re-synced to Luxury. Run continues so cache is saved.",
            ...result.duplicateLog,
          });
          await sendDuplicatePlacementEmail(result.duplicateLog, duplicateEmailSentFor);
          // Do NOT throw: we already created/updated in Luxury and updated cache. Throwing prevented saveCache(), so next run had no cache and the item stayed in limbo (re-archiving / duplicate emails every run).
        }
        if (result.operation === "create") created++;
        else if (result.operation === "update") updated++;
        else if (result.operation === "sold") sold++;
        else skipped++;
      }
    }

    saveCache(cache);

    const durationMs = Date.now() - syncStartTime;
    webflowLog("info", {
      event: "sync-all.exit",
      created,
      updated,
      skipped,
      sold,
      total: products.length,
      durationMs,
    });
    res.json({
      status: "ok",
      total: products.length,
      created,
      updated,
      skipped,
      sold,
      durationMs,
    });
  } catch (err) {
    const status = err.response?.status;
    const body = err.response?.data;
    webflowLog("error", {
      event: "sync-all.error",
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
  } finally {
    syncRequestId = null;
    luxuryItemIndex = null;
    furnitureProductIndex = null;
    furnitureSkuIndex = null;
    syncStartTime = null;
  }
});

/* ======================================================
   SERVER
====================================================== */
const PORT = process.env.PORT || 4000;
app.listen(PORT, () => {
  console.log(`🔥 Sync server running on ${PORT}`);
});



