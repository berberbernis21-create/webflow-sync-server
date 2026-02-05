import express from "express";
import axios from "axios";
import cors from "cors";
import dotenv from "dotenv";
import fs from "fs";
import nodemailer from "nodemailer";
import { CATEGORY_KEYWORDS } from "./categoryKeywords.js";
import { CATEGORY_KEYWORDS_FURNITURE } from "./categoryKeywordsFurniture.js";
import { detectBrandFromProduct } from "./brand.js";
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
    console.error("⚠️ Failed to save duplicate-placement-sent list:", err.toString());
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
      console.error("⚠️ Publishing error:", err.response?.data || err.toString());
    }
  }
}

/* ======================================================
   SHOPIFY — WRITE METAFIELDS (department, category, vertical, dimensions_status)
   Department = parent (Furniture & Home | Luxury Goods). Category = child (Living Room, Handbags, etc.).
   Furniture & Home and Luxury Goods are mutually exclusive: furniture gets furniture_and_home = category, luxury_goods = "";
   luxury gets furniture_and_home = "", luxury_goods = category. Collection rules use these.
====================================================== */
async function updateShopifyMetafields(productId, { department, category, vertical, dimensionsStatus }) {
  const ownerId = `gid://shopify/Product/${productId}`;
  const dept = department ?? "";
  const cat = category ?? "";
  const vert = vertical ?? "luxury";
  const isFurniture = dept === "Furniture & Home";
  const metafields = [
    {
      ownerId,
      key: "department",
      namespace: "custom",
      type: "single_line_text_field",
      value: dept,
    },
    {
      ownerId,
      key: "category",
      namespace: "custom",
      type: "single_line_text_field",
      value: cat,
    },
    {
      ownerId,
      key: "vertical",
      namespace: "custom",
      type: "single_line_text_field",
      value: vert,
    },
    {
      ownerId,
      key: "product_type_group",
      namespace: "custom",
      type: "single_line_text_field",
      value: dept,
    },
    // Only for furniture: "Furniture & Home is equal to Rugs/Living Room/..." — empty for luxury
    {
      ownerId,
      key: "furniture_and_home",
      namespace: "custom",
      type: "single_line_text_field",
      value: isFurniture ? cat : "",
    },
    // Only for luxury: "Luxury Goods is equal to Handbags/..." — empty for furniture (mutual exclusivity)
    {
      ownerId,
      key: "luxury_goods",
      namespace: "custom",
      type: "single_line_text_field",
      value: isFurniture ? "" : cat,
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
   SHOPIFY — WRITE VENDOR AND PRODUCT TYPE (Type field in admin)
   productType = assigned category so collection rules and Type display match (e.g. Rugs, Handbags, Art / Mirrors).
====================================================== */
/** Tags we set for department/category — strip before re-adding. Same for Luxury and Furniture. */
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

async function updateShopifyVendorAndType(productId, brandValue, productType, existingTags, department, category) {
  const mutation = `
    mutation UpdateProduct($input: ProductInput!) {
      productUpdate(input: $input) {
        product {
          id
          vendor
          productType
          tags
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

  const variables = { input };

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
   Includes dimensions (variant + metafields + tags) so dimension changes trigger an update.
====================================================== */
function shopifyHash(product) {
  const dimensions = getDimensionsFromProduct(product);
  return {
    title: product.title,
    vendor: product.vendor,
    body_html: product.body_html,
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

function detectCategoryFurniture(title, descriptionHtml) {
  const stripHtml = (html) => (html || "").replace(/<[^>]*>/g, " ").replace(/\s+/g, " ").trim();
  const descText = stripHtml(descriptionHtml || "");
  const combined = [title || "", descText].filter(Boolean).join(" ").toLowerCase();
  if (!combined) return "Accessories";
  for (const [category, keywords] of Object.entries(CATEGORY_KEYWORDS_FURNITURE)) {
    for (const kw of keywords) {
      if (matchFurnitureKeyword(combined, kw)) return category;
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

/** Format dimensions for description. Size (W×D×H) on one line; weight on the next line (no × before weight). */
function formatDimensionsForDescription(dims) {
  if (!dims || !hasAnyDimensions(dims)) return "";
  const sizeParts = [];
  if (dims.width != null && !Number.isNaN(dims.width)) sizeParts.push(`Width: ${dims.width}"`);
  if (dims.length != null && !Number.isNaN(dims.length)) sizeParts.push(`Depth: ${dims.length}"`);
  if (dims.height != null && !Number.isNaN(dims.height)) sizeParts.push(`Height: ${dims.height}"`);
  const hasWeight = dims.weight != null && !Number.isNaN(dims.weight) && dims.weight > 0;
  const sizeLine = sizeParts.length ? `Dimensions: ${sizeParts.join(" × ")}.` : "";
  const weightLine = hasWeight ? `Weight: ${dims.weight} lb.` : "";
  if (sizeLine && weightLine) return `${sizeLine}\n${weightLine}`;
  return sizeLine || weightLine || "";
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
async function syncSingleProduct(product, cache, options = {}) {
  const shopifyProductId = String(product.id);
  const cacheEntry = getCacheEntry(cache, shopifyProductId);
  const duplicateEmailSentFor = options.duplicateEmailSentFor ?? null;

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
    const detFurn = detectCategoryFurniture(name, description);
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

  // Write back to Shopify: department (parent), category (child), vertical, dimensions_status (furniture), vendor
  const shopifyDepartment = vertical === "furniture" ? "Furniture & Home" : "Luxury Goods";
  await updateShopifyMetafields(shopifyProductId, {
    department: shopifyDepartment,
    category: shopifyCategoryValue,
    vertical: detectedVertical,
    dimensionsStatus: vertical === "furniture" ? dimensionsStatus : undefined,
  });
  await updateShopifyVendorAndType(shopifyProductId, brand, shopifyCategoryValue, getProductTagsArray(product), shopifyDepartment, shopifyCategoryValue);

  const currentHash = shopifyHash(product);

  console.log("\n=======================================");
  console.log("🧾 SYNC PRODUCT");
  console.log("shopifyProductId          =", shopifyProductId);
  console.log("vertical                  =", vertical);
  console.log("cache.webflowId           =", cacheEntry?.webflowId || "null");
  console.log("previousQty               =", previousQty);
  console.log("currentQty                =", qty);
  console.log("department                =", shopifyDepartment);
  console.log("category                  =", category, "(child:", shopifyCategoryValue + ")");
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

    // Sweep: if we're creating in Luxury, check if this product wrongly exists in Furniture (e.g. no cache / cache lost). Archive it so it doesn't stay in both places.
    if (detectedVertical === "luxury") {
      const furnitureConfig = getWebflowConfig("furniture");
      if (furnitureConfig?.siteId && furnitureConfig?.token) {
        const existingInFurniture = await findExistingWebflowEcommerceProduct(shopifyProductId, slug, furnitureConfig);
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
    if (categoryRef == null) {
      const envKey = `FURNITURE_CATEGORY_${category.replace(/\s*\/\s*/g, "_").replace(/\s+/g, "_").toUpperCase().replace(/[^A-Z0-9_]/g, "")}`;
      console.log("category not assigned (set " + envKey + " to Webflow category item ID for: " + category + ")");
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

    await loadFurnitureCategoryMap();

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

    const duplicateEmailSentFor = new Set();
    for (const product of products) {
      const result = await syncSingleProduct(product, cache, { duplicateEmailSentFor });

      if (result.duplicateCorrected && result.duplicateLog) {
        webflowLog("error", {
          event: "sync-all.duplicate_placement",
          message: "Item was in multiple places; archived duplicate and re-synced. Throwing so run fails and email was sent.",
          ...result.duplicateLog,
        });
        await sendDuplicatePlacementEmail(result.duplicateLog, duplicateEmailSentFor);
        const errMsg = `Duplicate placement: "${result.duplicateLog.productTitle}" (Shopify ID ${result.duplicateLog.shopifyProductId}) is archived. We re-synced to Luxury. Please go into the backend and delete the archived item if it still appears.`;
        throw new Error(errMsg);
      }

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



