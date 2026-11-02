/**
 * Durable variant SKU copy for Find Item.
 * Sold products leave Shopify, and the Webflow name indexes do not keep the
 * variant SKU. This file lives in DATA_DIR with the server's other JSON stores.
 */
import fs from "fs";
import path from "path";
import { normalizeListingSkuKey, pickListingSkuMatch } from "./listingLookupQuery.js";
import { stripNoLongerAvailableSuffix } from "./listingTitleDisplay.js";

export const LISTING_SKU_STORE_FILENAME = "listing_sku_variants.json";

const DIM_KEYS = ["width", "depth", "height", "weight"];

export function normalizeStoredListingName(name) {
  return stripNoLongerAvailableSuffix(name).toLowerCase().replace(/\s+/g, " ").trim();
}

export function normalizeStoredListingSlug(raw) {
  let s = String(raw || "").trim();
  if (!s) return "";
  try {
    if (/^https?:\/\//i.test(s) || s.includes("/")) {
      const href = /^https?:\/\//i.test(s) ? s : `https://${s.replace(/^\/+/, "")}`;
      const u = new URL(href);
      const parts = u.pathname.split("/").filter(Boolean);
      const pi = parts.findIndex((p) => p === "product" || p === "products" || p === "shop");
      const slug = parts[pi >= 0 && parts[pi + 1] ? pi + 1 : parts.length - 1] || "";
      s = decodeURIComponent(slug);
    }
  } catch {
    /* keep the raw token */
  }
  return s.replace(/\/+$/, "").trim().toLowerCase();
}

function hasValue(value) {
  if (value == null) return false;
  if (typeof value === "string") return value.trim() !== "";
  if (Array.isArray(value)) return value.length > 0;
  return true;
}

function positiveNumber(value) {
  if (value == null || value === "") return null;
  const n = typeof value === "number" ? value : Number(String(value).replace(/[^0-9.]+/g, ""));
  return Number.isFinite(n) && n > 0 ? n : null;
}

function pickProductUrl(prev, next, source) {
  const incoming = String(next.productUrl || "").trim();
  const existing = String(prev.productUrl || "").trim();
  if (!incoming) return existing;
  if (!existing) return incoming;
  if (
    source === "shopify_sync" &&
    /lostandfoundhandbags\.com/i.test(existing) &&
    /lostandfoundresale\.com/i.test(incoming)
  ) {
    return existing;
  }
  return incoming;
}

function mergeRecord(prev, next, source) {
  const sku = String(next.sku || prev.sku || "").trim();
  const skuKey = normalizeListingSkuKey(sku);
  if (!skuKey) return null;
  const merged = {
    skuKey,
    sku,
    variantId: hasValue(next.variantId) ? String(next.variantId).trim() : String(prev.variantId || "").trim(),
    productId: hasValue(next.productId) ? String(next.productId).trim() : String(prev.productId || "").trim(),
    title: hasValue(next.title) ? String(next.title).trim() : String(prev.title || "").trim(),
    price: hasValue(next.price) ? String(next.price).trim() : String(prev.price || "").trim(),
    productUrl: pickProductUrl(prev, next, source),
    shopifyOnlineUrl: hasValue(next.shopifyOnlineUrl)
      ? String(next.shopifyOnlineUrl).trim()
      : String(prev.shopifyOnlineUrl || "").trim(),
    handle: hasValue(next.handle) ? String(next.handle).trim() : String(prev.handle || "").trim(),
    description: hasValue(next.description) ? String(next.description) : String(prev.description || ""),
    images: hasValue(next.images) ? next.images.filter(Boolean).map((url) => String(url)) : prev.images || [],
    vendor: hasValue(next.vendor) ? String(next.vendor).trim() : String(prev.vendor || "").trim(),
    productType: hasValue(next.productType) ? String(next.productType).trim() : String(prev.productType || "").trim(),
    luxuryGoodsCategory: hasValue(next.luxuryGoodsCategory)
      ? String(next.luxuryGoodsCategory).trim()
      : String(prev.luxuryGoodsCategory || "").trim(),
    tags: hasValue(next.tags) ? next.tags : prev.tags || [],
    metafields: hasValue(next.metafields) ? next.metafields : prev.metafields || [],
    freightClass: hasValue(next.freightClass)
      ? next.freightClass
      : hasValue(next.freight_class)
        ? next.freight_class
        : prev.freightClass ?? null,
    vertical:
      hasValue(next.vertical) && next.vertical !== "shopify"
        ? String(next.vertical)
        : prev.vertical && prev.vertical !== "shopify"
          ? prev.vertical
          : hasValue(next.vertical)
            ? String(next.vertical)
            : prev.vertical || "shopify",
    sold: typeof next.sold === "boolean" ? next.sold : prev.sold === true,
  };
  for (const key of DIM_KEYS) {
    const incoming = positiveNumber(next[key]);
    merged[key] = incoming != null ? incoming : positiveNumber(prev[key]);
  }
  return merged;
}

function comparable(record) {
  if (!record) return "";
  const { updatedAt, ...rest } = record;
  return JSON.stringify(rest);
}

/** Listing object GET /api/listing already turns into the freight form. */
export function listingFromStoredSkuRecord(record, { markSold = false } = {}) {
  if (!record) return null;
  const images = Array.isArray(record.images) ? record.images.filter(Boolean) : [];
  return {
    title: record.title || "",
    sku: record.sku || "",
    variantId: record.variantId || "",
    productId: record.productId || "",
    shopifyProductId: record.productId || null,
    price: record.price != null && record.price !== "" ? String(record.price) : "",
    description: record.description || "",
    images,
    handle: record.handle || null,
    productUrl: record.productUrl || null,
    shopifyOnlineUrl: record.shopifyOnlineUrl || null,
    vertical: record.vertical || "shopify",
    vendor: record.vendor || null,
    luxuryGoodsCategory: record.luxuryGoodsCategory || null,
    productType: record.productType || null,
    tags: Array.isArray(record.tags) ? record.tags : [],
    weight: record.weight ?? null,
    width: record.width ?? null,
    depth: record.depth ?? null,
    height: record.height ?? null,
    freight_class: record.freightClass ?? null,
    metafields: Array.isArray(record.metafields) ? record.metafields : [],
    sold: record.sold === true || markSold,
  };
}

/**
 * Shopify variant search first. A miss still returns the stored row.
 * liveError is set when Shopify itself failed; a normal miss leaves it null.
 */
export async function resolveListingSkuFromLiveThenStore(rawSku, { searchLive, findStored }) {
  const key = normalizeListingSkuKey(rawSku);
  if (!key) return { listing: null, source: null, liveError: null };
  let live = null;
  let liveError = null;
  try {
    live = await searchLive(rawSku);
  } catch (err) {
    liveError = err;
  }
  if (live) return { listing: live, source: "shopify", liveError: null };
  const stored = typeof findStored === "function" ? findStored(rawSku) : null;
  if (stored) return { listing: stored, source: "stored_sku", liveError };
  return { listing: null, source: null, liveError };
}

export function createListingSkuStore({ filePath, writeDelayMs = 400, onError } = {}) {
  if (!filePath) throw new Error("listing SKU store requires filePath");
  const bySku = new Map();
  const byName = new Map();
  const bySlug = new Map();
  let loaded = false;
  let readable = true;
  let timer = null;
  let dirty = false;

  function fail(err, event) {
    if (typeof onError === "function") onError(err, event);
  }

  function indexRecord(record) {
    if (!record?.skuKey) return;
    const nameKey = normalizeStoredListingName(record.title);
    if (nameKey) byName.set(nameKey, record.skuKey);
    for (const raw of [record.handle, record.productUrl]) {
      const slugKey = normalizeStoredListingSlug(raw);
      if (slugKey) bySlug.set(slugKey, record.skuKey);
    }
  }

  function reindex() {
    byName.clear();
    bySlug.clear();
    for (const record of bySku.values()) indexRecord(record);
  }

  function load() {
    if (loaded) return;
    loaded = true;
    if (!fs.existsSync(filePath)) return;
    try {
      const parsed = JSON.parse(fs.readFileSync(filePath, "utf8"));
      const bag = parsed?.records && typeof parsed.records === "object" ? parsed.records : {};
      for (const record of Object.values(bag)) {
        const skuKey = normalizeListingSkuKey(record?.sku || record?.skuKey);
        if (!skuKey || !record) continue;
        bySku.set(skuKey, { ...record, skuKey });
      }
      reindex();
    } catch (err) {
      readable = false;
      fail(err, "listing_sku_store.load_failed");
    }
  }

  function save() {
    if (timer) {
      clearTimeout(timer);
      timer = null;
    }
    if (!dirty) return;
    if (!readable && bySku.size === 0) return;
    try {
      fs.mkdirSync(path.dirname(filePath), { recursive: true });
      const records = {};
      for (const [key, record] of bySku.entries()) records[key] = record;
      fs.writeFileSync(filePath, JSON.stringify({ version: 1, records }, null, 2), "utf8");
      dirty = false;
      readable = true;
    } catch (err) {
      fail(err, "listing_sku_store.save_failed");
    }
  }

  function scheduleSave(immediate) {
    dirty = true;
    if (immediate || writeDelayMs === 0) {
      save();
      return;
    }
    if (timer) return;
    timer = setTimeout(() => {
      timer = null;
      save();
    }, writeDelayMs);
    if (typeof timer.unref === "function") timer.unref();
  }

  function apply(partial, source) {
    const skuKey = normalizeListingSkuKey(partial?.sku);
    if (!skuKey) return false;
    const prev = bySku.get(skuKey) || {};
    const merged = mergeRecord(prev, partial, source);
    if (!merged || comparable(prev) === comparable(merged)) return false;
    merged.updatedAt = new Date().toISOString();
    bySku.set(skuKey, merged);
    indexRecord(merged);
    return true;
  }

  function upsert(partial, options = {}) {
    load();
    const changed = apply(partial, options.source || "");
    if (changed) scheduleSave(options.immediate === true);
    return bySku.get(normalizeListingSkuKey(partial?.sku)) || null;
  }

  function upsertMany(records, options = {}) {
    load();
    let changed = 0;
    for (const record of records || []) {
      if (apply(record, options.source || "")) changed += 1;
    }
    if (changed) scheduleSave(options.immediate === true || changed >= 25);
    return changed;
  }

  function updateByProductId(productId, partial, options = {}) {
    load();
    const id = String(productId || "").trim();
    if (!id) return 0;
    const skus = [];
    for (const record of bySku.values()) {
      if (String(record.productId || "") === id && record.sku) skus.push(record.sku);
    }
    let changed = 0;
    for (const sku of skus) {
      if (apply({ ...partial, sku }, options.source || "")) changed += 1;
    }
    if (changed) scheduleSave(options.immediate === true);
    return changed;
  }

  function findBySku(query) {
    load();
    const key = normalizeListingSkuKey(query);
    if (!key) return null;
    const exact = bySku.get(key);
    if (exact) return exact;
    const candidates = [];
    for (const record of bySku.values()) candidates.push(record);
    return pickListingSkuMatch(candidates, query) || null;
  }

  function findByName(name) {
    load();
    const nameKey = normalizeStoredListingName(name);
    if (!nameKey) return null;
    const skuKey = byName.get(nameKey);
    return skuKey ? bySku.get(skuKey) || null : null;
  }

  function findBySlug(slugOrUrl) {
    load();
    const slugKey = normalizeStoredListingSlug(slugOrUrl);
    if (!slugKey) return null;
    const skuKey = bySlug.get(slugKey);
    return skuKey ? bySku.get(skuKey) || null : null;
  }

  function flush() {
    if (!loaded) load();
    save();
  }

  return { filePath, upsert, upsertMany, updateByProductId, findBySku, findByName, findBySlug, flush, load };
}
