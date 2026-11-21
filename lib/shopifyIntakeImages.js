/**
 * Intake → Shopify image handoff (before Webflow / Google).
 *
 * Order: Traxia e-com Save (no Traxia photos) → product lands in Shopify →
 * this module attaches Intake photos via Admin API → stamps Pricing Engine →
 * then existing sync continues to Webflow / Google.
 *
 * Products that already have display images are left alone on bulk sync
 * (later Intake photos append via /intake-images/photo-added).
 *
 * Env:
 *   INTAKE_API_BASE          default https://lostfound-pricing-api.onrender.com
 *   INTAKE_EMAIL + INTAKE_PASSWORD  OR  INTAKE_SESSION_COOKIE
 *   INTAKE_SHOPIFY_IMAGES_ENABLED   default on when creds present; set 0/false to disable
 *   INTAKE_SHOPIFY_IMAGES_MAX       max photos to attach (default 25)
 *   INTAKE_SYNC_HOOK_SECRET  optional; sent as X-Intake-Sync-Secret when stamping
 *   SHOPIFY_STORE + SHOPIFY_ACCESS_TOKEN  (existing)
 */

import axios from "axios";

const DEFAULT_INTAKE_API_BASE = "https://lostfound-pricing-api.onrender.com";
const INTAKE_ITEM_ID_RE =
  /(?:lost-and-found-intake|field-buyer)\/item\.html\?[^#\s"']*?\bid=([0-9a-f-]{36})\b|(?:\/api\/lost-and-found-intake\/items\/)([0-9a-f-]{36})\b|\bintake[_-]?id[=:]([0-9a-f-]{36})\b|\bintake-([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\b/i;
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

let cachedCookie = null;
let cachedCookieAt = 0;
const COOKIE_TTL_MS = 6 * 60 * 60 * 1000;

function envFlag(name, defaultOnWhenUnset = false) {
  const raw = process.env[name];
  if (raw == null || String(raw).trim() === "") return defaultOnWhenUnset;
  const v = String(raw).trim().toLowerCase();
  return !(v === "0" || v === "false" || v === "off" || v === "no");
}

export function intakeShopifyImagesConfigured() {
  const cookie = String(process.env.INTAKE_SESSION_COOKIE || "").trim();
  const email = String(process.env.INTAKE_EMAIL || process.env.INTAKE_USERNAME || "").trim();
  const password = String(process.env.INTAKE_PASSWORD || "").trim();
  return Boolean(cookie || (email && password));
}

export function intakeShopifyImagesEnabled() {
  const hasCreds = intakeShopifyImagesConfigured();
  if (!hasCreds) return false;
  return envFlag("INTAKE_SHOPIFY_IMAGES_ENABLED", true);
}

export function getIntakeApiBase() {
  return String(process.env.INTAKE_API_BASE || DEFAULT_INTAKE_API_BASE)
    .trim()
    .replace(/\/+$/, "");
}

export function normalizeFullItemName(name) {
  return String(name || "")
    .replace(/\u00a0/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function namesEqualFull(a, b) {
  return normalizeFullItemName(a).toLowerCase() === normalizeFullItemName(b).toLowerCase();
}

export function parseIntakeItemIdFromText(text) {
  const s = String(text || "");
  if (!s) return "";
  const m = s.match(INTAKE_ITEM_ID_RE);
  if (m) {
    for (let i = 1; i < m.length; i++) {
      const hit = String(m[i] || "").trim();
      if (hit && UUID_RE.test(hit)) return hit;
    }
  }
  try {
    const u = new URL(s);
    const id = u.searchParams.get("id");
    if (id && UUID_RE.test(id) && /item\.html/i.test(u.pathname)) return id.trim();
  } catch (_err) {
    /* not a URL */
  }
  return "";
}

/** Scan common Shopify product fields for Intake id (External Link URL, intake-* tag, metafield). */
export function extractIntakeItemIdFromShopifyProduct(product) {
  if (!product || typeof product !== "object") return "";
  const chunks = [];
  chunks.push(product.body_html, product.handle, product.tags, product.vendor, product.product_type);
  if (Array.isArray(product.metafields)) {
    for (const mf of product.metafields) {
      chunks.push(mf?.value, mf?.key, mf?.namespace);
    }
  }
  if (Array.isArray(product.note_attributes)) {
    for (const na of product.note_attributes) {
      chunks.push(na?.value, na?.name);
    }
  }
  if (product.image?.src) chunks.push(product.image.src);
  if (Array.isArray(product.images)) {
    for (const img of product.images) chunks.push(img?.src, img?.alt);
  }
  for (const c of chunks) {
    const id = parseIntakeItemIdFromText(c);
    if (id) return id;
  }
  return "";
}

export function shopifyProductHasDisplayImages(product) {
  const imgs = product?.images;
  if (!Array.isArray(imgs) || imgs.length === 0) return false;
  return imgs.some((i) => i?.src && String(i.src).trim());
}

function shopifyHeaders() {
  const token = String(process.env.SHOPIFY_ACCESS_TOKEN || "").trim();
  return {
    "X-Shopify-Access-Token": token,
    "Content-Type": "application/json",
  };
}

function shopifyRestBase() {
  const store = String(process.env.SHOPIFY_STORE || "").trim();
  if (!store) return "";
  return `https://${store}.myshopify.com/admin/api/2024-01`;
}

function cookieHeaderFromSetCookie(setCookie) {
  const list = Array.isArray(setCookie) ? setCookie : setCookie ? [setCookie] : [];
  const parts = [];
  for (const raw of list) {
    const first = String(raw || "").split(";")[0].trim();
    if (first) parts.push(first);
  }
  return parts.join("; ");
}

async function ensureIntakeSessionCookie(log) {
  const forced = String(process.env.INTAKE_SESSION_COOKIE || "").trim();
  if (forced) return forced;
  if (cachedCookie && Date.now() - cachedCookieAt < COOKIE_TTL_MS) return cachedCookie;

  const email = String(process.env.INTAKE_EMAIL || process.env.INTAKE_USERNAME || "").trim();
  const password = String(process.env.INTAKE_PASSWORD || "").trim();
  if (!email || !password) {
    throw new Error("Intake auth missing (set INTAKE_EMAIL + INTAKE_PASSWORD or INTAKE_SESSION_COOKIE)");
  }

  const base = getIntakeApiBase();
  const res = await axios.post(
    `${base}/api/auth/login`,
    { email, password },
    { timeout: 30000, validateStatus: () => true }
  );
  if (res.status >= 400) {
    throw new Error(`Intake login failed HTTP ${res.status}`);
  }
  const setCookie = res.headers?.["set-cookie"] || res.headers?.["Set-Cookie"];
  const cookie = cookieHeaderFromSetCookie(setCookie);
  if (!cookie) {
    throw new Error("Intake login returned no session cookie");
  }
  cachedCookie = cookie;
  cachedCookieAt = Date.now();
  log?.("info", { event: "intake.auth.ok", base });
  return cookie;
}

function absoluteIntakeUrl(pathOrUrl) {
  const s = String(pathOrUrl || "").trim();
  if (!s) return "";
  if (/^https?:\/\//i.test(s)) return s;
  const base = getIntakeApiBase();
  if (s.startsWith("/")) return `${base}${s}`;
  return `${base}/${s}`;
}

async function intakeGet(path, { cookie, params } = {}) {
  const base = getIntakeApiBase();
  const url = path.startsWith("http") ? path : `${base}${path.startsWith("/") ? "" : "/"}${path}`;
  const res = await axios.get(url, {
    params,
    timeout: 45000,
    headers: cookie ? { Cookie: cookie } : {},
    validateStatus: () => true,
    responseType: "json",
  });
  return res;
}

export async function fetchIntakeItemById(itemId, { log } = {}) {
  const id = String(itemId || "").trim();
  if (!id || !UUID_RE.test(id)) return null;
  const cookie = await ensureIntakeSessionCookie(log);
  const res = await intakeGet(`/api/lost-and-found-intake/items/${id}`, { cookie });
  if (res.status === 404) return null;
  if (res.status >= 400) {
    throw new Error(`Intake get item HTTP ${res.status}`);
  }
  return res.data?.item || res.data || null;
}

/**
 * Exact full-name match against Intake list search.
 * Prefer unique exact name; never pick a near-miss (Chair IV vs Chair V).
 */
export async function fetchIntakeItemByFullName(fullName, { log } = {}) {
  const name = normalizeFullItemName(fullName);
  if (!name) return null;
  const cookie = await ensureIntakeSessionCookie(log);
  const res = await intakeGet(`/api/lost-and-found-intake/items`, {
    cookie,
    params: { q: name, page: 1, pageSize: 50, photos: "with" },
  });
  if (res.status >= 400) {
    throw new Error(`Intake search HTTP ${res.status}`);
  }
  const items =
    res.data?.items ||
    res.data?.results ||
    (Array.isArray(res.data) ? res.data : []);
  const exact = (Array.isArray(items) ? items : []).filter((it) => namesEqualFull(it?.name, name));
  if (!exact.length) {
    log?.("info", {
      event: "intake.lookup.by_name.miss",
      name: name.slice(0, 120),
      hitCount: Array.isArray(items) ? items.length : 0,
    });
    return null;
  }
  if (exact.length > 1) {
    log?.("warn", {
      event: "intake.lookup.by_name.ambiguous",
      name: name.slice(0, 120),
      ids: exact.map((it) => it.id).slice(0, 8),
    });
    // Prefer the one with photos; if still tied, first.
    const withPhotos = exact.filter((it) => Array.isArray(it.photos) && it.photos.length);
    const pick = withPhotos[0] || exact[0];
    // Re-fetch full item so photo URLs are signed.
    return fetchIntakeItemById(pick.id, { log });
  }
  const only = exact[0];
  if (Array.isArray(only.photos) && only.photos.length && only.photos[0]?.url) {
    return only;
  }
  return fetchIntakeItemById(only.id, { log });
}

export function orderedIntakePhotoUrls(item) {
  const photos = Array.isArray(item?.photos) ? [...item.photos] : [];
  photos.sort((a, b) => {
    const ao = Number(a?.sortOrder ?? a?.sort_order ?? 0);
    const bo = Number(b?.sortOrder ?? b?.sort_order ?? 0);
    if (ao !== bo) return ao - bo;
    return String(a?.id || "").localeCompare(String(b?.id || ""));
  });
  const primaryId = String(item?.primaryPhotoId || item?.primary_photo_id || "").trim();
  if (primaryId) {
    const idx = photos.findIndex((p) => String(p?.id) === primaryId);
    if (idx > 0) {
      const [p] = photos.splice(idx, 1);
      photos.unshift(p);
    }
  }
  const max = Math.min(
    50,
    Math.max(1, parseInt(process.env.INTAKE_SHOPIFY_IMAGES_MAX || "25", 10) || 25)
  );
  const urls = [];
  for (const p of photos) {
    const abs = absoluteIntakeUrl(p?.url || p?.thumbUrl || "");
    if (abs && /\/media\/view\//i.test(abs)) {
      urls.push(abs);
    } else if (abs) {
      urls.push(abs);
    }
    if (urls.length >= max) break;
  }
  return urls;
}

/** Normalize caller-supplied image URL list (signed media/view URLs preferred). */
export function normalizeCallerImageUrls(imageUrls) {
  const max = Math.min(
    50,
    Math.max(1, parseInt(process.env.INTAKE_SHOPIFY_IMAGES_MAX || "25", 10) || 25)
  );
  const out = [];
  const seen = new Set();
  for (const raw of imageUrls || []) {
    const abs = absoluteIntakeUrl(raw);
    if (!abs || seen.has(abs)) continue;
    seen.add(abs);
    out.push(abs);
    if (out.length >= max) break;
  }
  return out;
}

/**
 * Upload a list of public image URLs onto a Shopify product (download+attachment, src fallback).
 * Used when Traxia/Intake passes imageUrls so the server does not need an Intake item lookup.
 */
export async function uploadCallerImageUrlsToShopifyProduct(
  product,
  imageUrls,
  { log } = {}
) {
  const urls = normalizeCallerImageUrls(imageUrls);
  if (!product?.id || !urls.length) {
    return { product, uploaded: 0, status: "skipped_no_photos", createdSrcs: [] };
  }
  if (shopifyProductHasDisplayImages(product)) {
    return {
      product,
      uploaded: 0,
      status: "skipped_has_images",
      createdSrcs: (product.images || []).map((i) => i?.src).filter(Boolean),
    };
  }

  let cookie = null;
  try {
    if (intakeShopifyImagesConfigured()) {
      cookie = await ensureIntakeSessionCookie(log);
    }
  } catch (_err) {
    cookie = null;
  }

  let uploaded = 0;
  const createdSrcs = [];
  for (let i = 0; i < urls.length; i++) {
    const src = urls[i];
    try {
      const { buffer, filename } = await downloadImageBuffer(src, {
        cookie: /\/media\/view\//i.test(src) ? undefined : cookie,
      });
      const image = await createShopifyProductImage(product.id, {
        attachment: buffer.toString("base64"),
        filename,
        position: i + 1,
      });
      if (image?.src) createdSrcs.push(image.src);
      uploaded += 1;
    } catch (oneErr) {
      try {
        const image = await createShopifyProductImage(product.id, {
          src,
          position: i + 1,
        });
        if (image?.src) createdSrcs.push(image.src);
        uploaded += 1;
      } catch (twoErr) {
        log?.("warn", {
          event: "shopify.intake_image.upload_one_failed",
          shopifyProductId: product.id,
          index: i,
          message: twoErr?.message || oneErr?.message || String(twoErr),
        });
      }
    }
  }

  if (!uploaded) {
    return {
      product,
      uploaded: 0,
      status: "upload_failed",
      reason: "zero_images_uploaded",
      createdSrcs,
    };
  }

  const next = {
    ...product,
    images: createdSrcs.length
      ? createdSrcs.map((src, i) => ({ src, position: i + 1 }))
      : product.images,
  };
  log?.("info", {
    event: "shopify.intake_image.uploaded_from_urls",
    shopifyProductId: product.id,
    uploaded,
    productTitle: product.title,
  });
  return { product: next, uploaded, status: "uploaded", createdSrcs };
}

async function downloadImageBuffer(url, { cookie } = {}) {
  const res = await axios.get(url, {
    responseType: "arraybuffer",
    timeout: 60000,
    headers: cookie ? { Cookie: cookie } : {},
    validateStatus: () => true,
  });
  if (res.status >= 400) {
    throw new Error(`Download image HTTP ${res.status}`);
  }
  const buf = Buffer.from(res.data);
  if (!buf.length) throw new Error("Empty image download");
  const ctype = String(res.headers?.["content-type"] || "").toLowerCase();
  let filename = "intake.jpg";
  if (ctype.includes("png")) filename = "intake.png";
  else if (ctype.includes("webp")) filename = "intake.webp";
  else if (ctype.includes("gif")) filename = "intake.gif";
  return { buffer: buf, filename, contentType: ctype };
}

async function createShopifyProductImage(productId, { src, attachment, filename, position }) {
  const base = shopifyRestBase();
  if (!base) throw new Error("SHOPIFY_STORE missing");
  const image = {};
  if (attachment) {
    image.attachment = attachment;
    if (filename) image.filename = filename;
  } else if (src) {
    image.src = src;
  } else {
    throw new Error("image src or attachment required");
  }
  if (position != null) image.position = position;
  const res = await axios.post(
    `${base}/products/${productId}/images.json`,
    { image },
    { headers: shopifyHeaders(), timeout: 90000, validateStatus: () => true }
  );
  if (res.status >= 400) {
    const msg =
      res.data?.errors ||
      res.data?.error ||
      (typeof res.data === "string" ? res.data : JSON.stringify(res.data || {}).slice(0, 200));
    throw new Error(`Shopify image create HTTP ${res.status}: ${msg}`);
  }
  return res.data?.image || null;
}

async function fetchShopifyProductById(productId, { log } = {}) {
  const id = String(productId || "").trim();
  const base = shopifyRestBase();
  if (!base || !id) return null;
  const res = await axios.get(`${base}/products/${id}.json`, {
    headers: shopifyHeaders(),
    timeout: 30000,
    validateStatus: () => true,
  });
  if (res.status === 404) return null;
  if (res.status >= 400) {
    log?.("warn", {
      event: "shopify.get_product.failed",
      shopifyProductId: id,
      status: res.status,
    });
    return null;
  }
  return res.data?.product || null;
}

/**
 * Persist Shopify image sync note/flag on the Pricing Engine Intake item.
 */
export async function stampIntakeShopifyImageSync(
  intakeItemId,
  {
    status,
    shopifyProductId = null,
    uploaded = null,
    photoId = null,
    detail = null,
    log = null,
  } = {}
) {
  const id = String(intakeItemId || "").trim();
  if (!id || !UUID_RE.test(id)) {
    return { ok: false, reason: "bad_intake_id" };
  }
  if (!intakeShopifyImagesConfigured()) {
    return { ok: false, reason: "missing_intake_creds" };
  }
  try {
    const cookie = await ensureIntakeSessionCookie(log);
    const headers = {
      "Content-Type": "application/json",
      Accept: "application/json",
      Cookie: cookie,
    };
    const secret = String(process.env.INTAKE_SYNC_HOOK_SECRET || "").trim();
    if (secret) headers["X-Intake-Sync-Secret"] = secret;
    const body = {
      status,
      shopifyProductId: shopifyProductId != null ? String(shopifyProductId) : null,
      uploaded,
      photoId,
      detail,
    };
    const res = await axios.post(
      `${getIntakeApiBase()}/api/lost-and-found-intake/items/${id}/shopify-image-sync`,
      body,
      { headers, timeout: 30000, validateStatus: () => true }
    );
    if (res.status >= 400) {
      log?.("warn", {
        event: "intake.shopify_sync_stamp.failed",
        intakeItemId: id,
        status: res.status,
        message: res.data?.error || JSON.stringify(res.data || {}).slice(0, 160),
      });
      return { ok: false, reason: `http_${res.status}`, data: res.data };
    }
    log?.("info", {
      event: "intake.shopify_sync_stamp.ok",
      intakeItemId: id,
      shopifySyncStatus: status,
      shopifyProductId: shopifyProductId || null,
    });
    return { ok: true, data: res.data };
  } catch (err) {
    log?.("warn", {
      event: "intake.shopify_sync_stamp.error",
      intakeItemId: id,
      message: err?.message || String(err),
    });
    return { ok: false, reason: err?.message || "stamp_error" };
  }
}

function stampStatusFromImageGate(result) {
  if (!result) return null;
  if (result.status === "uploaded" || result.status === "skipped_has_images") return "synced";
  if (result.status === "upload_failed") return "failed";
  if (
    result.status === "skipped_no_photos" ||
    result.status === "blocked_waiting" ||
    result.blocked
  ) {
    return "pending";
  }
  return null;
}

function stampDetailFromImageGate(result) {
  if (!result) return null;
  if (result.status === "uploaded") {
    return `Shopify images synced (${result.uploaded || 0})`;
  }
  if (result.status === "skipped_has_images") {
    return "Shopify already had display images";
  }
  if (result.status === "skipped_no_photos") {
    return "Shopify image sync pending — Intake has no photos yet";
  }
  if (result.status === "upload_failed") {
    return `Shopify image sync failed: ${result.reason || "upload_failed"}`;
  }
  if (result.blocked) {
    return `Shopify image sync pending: ${result.reason || "waiting"}`;
  }
  return result.reason || null;
}

export async function stampFromImageGateResult(result, { log } = {}) {
  const intakeItemId = result?.intakeItemId;
  if (!intakeItemId) return { ok: false, reason: "no_intake_id" };
  const status = stampStatusFromImageGate(result);
  if (!status) return { ok: false, reason: "no_stamp_status" };
  return stampIntakeShopifyImageSync(intakeItemId, {
    status,
    shopifyProductId: result.shopifyProductId || result.product?.id || null,
    uploaded: result.uploaded,
    detail: stampDetailFromImageGate(result),
    log,
  });
}

/**
 * REST title search with exact full-name preference (never silently take a near miss).
 */
export async function fetchShopifyProductByFullName(fullName, { log } = {}) {
  const title = normalizeFullItemName(fullName);
  const base = shopifyRestBase();
  if (!base || !title) return null;
  const url = `${base}/products.json?title=${encodeURIComponent(title)}&limit=10`;
  const res = await axios.get(url, {
    headers: shopifyHeaders(),
    timeout: 30000,
    validateStatus: () => true,
  });
  if (res.status >= 400) {
    log?.("warn", {
      event: "shopify.poll_by_name.failed",
      title: title.slice(0, 120),
      status: res.status,
    });
    return null;
  }
  const products = Array.isArray(res.data?.products) ? res.data.products : [];
  const exact = products.find((p) => namesEqualFull(p?.title, title));
  if (!exact) {
    log?.("info", {
      event: "shopify.poll_by_name.miss",
      title: title.slice(0, 120),
      hitCount: products.length,
    });
    return null;
  }
  return exact;
}

export async function pollShopifyProductByFullName(
  fullName,
  { log, maxWaitMs = 120000, intervalMs = 5000, backoff = true } = {}
) {
  const started = Date.now();
  let attempt = 0;
  let delayMs = Math.max(500, Number(intervalMs) || 5000);
  const maxDelayMs = Math.max(
    delayMs,
    parseInt(process.env.SHOPIFY_TITLE_POLL_MAX_DELAY_MS || "15000", 10) || 15_000
  );
  while (Date.now() - started <= maxWaitMs) {
    attempt += 1;
    const hit = await fetchShopifyProductByFullName(fullName, { log });
    if (hit?.id) {
      log?.("info", {
        event: "shopify.poll_by_name.hit",
        title: normalizeFullItemName(fullName).slice(0, 120),
        shopifyProductId: hit.id,
        attempt,
        waitedMs: Date.now() - started,
      });
      return hit;
    }
    if (Date.now() - started + delayMs > maxWaitMs) break;
    await new Promise((r) => setTimeout(r, delayMs));
    if (backoff) {
      delayMs = Math.min(maxDelayMs, Math.round(delayMs * 1.6));
    }
  }
  log?.("info", {
    event: "shopify.poll_by_name.timeout",
    title: normalizeFullItemName(fullName).slice(0, 120),
    attempt,
    waitedMs: Date.now() - started,
  });
  return null;
}

/**
 * Attach Intake photos to a Shopify product that has none.
 * @returns {{ product, status, uploaded, blocked, intakeItemId, reason }}
 *   status: skipped_has_images | skipped_disabled | skipped_no_intake | skipped_no_photos |
 *           uploaded | upload_failed | blocked_waiting
 */
export async function ensureShopifyProductHasIntakeImages(
  product,
  { log, intakeItemId: intakeItemIdHint = null, imageUrls: imageUrlsHint = null } = {}
) {
  const empty = {
    product,
    status: "skipped_disabled",
    uploaded: 0,
    blocked: false,
    intakeItemId: null,
    reason: null,
  };

  if (!product?.id) return { ...empty, status: "skipped_disabled", reason: "no_product" };

  // REST product list often omits metafields — fetch so External Link / intake id can surface.
  if (!Array.isArray(product.metafields)) {
    try {
      const base = shopifyRestBase();
      if (base) {
        const mfRes = await axios.get(`${base}/products/${product.id}/metafields.json`, {
          headers: shopifyHeaders(),
          timeout: 20000,
          validateStatus: () => true,
        });
        if (mfRes.status < 400 && Array.isArray(mfRes.data?.metafields)) {
          product.metafields = mfRes.data.metafields;
        }
      }
    } catch (_err) {
      /* name / tag fallback still available */
    }
  }

  let intakeItemId =
    (intakeItemIdHint && UUID_RE.test(String(intakeItemIdHint).trim())
      ? String(intakeItemIdHint).trim()
      : "") || extractIntakeItemIdFromShopifyProduct(product);

  if (shopifyProductHasDisplayImages(product)) {
    if (!intakeItemId && intakeShopifyImagesConfigured()) {
      try {
        const byName = await fetchIntakeItemByFullName(product.title, { log });
        intakeItemId = byName?.id || null;
      } catch (_err) {
        /* optional resolve for stamp only */
      }
    }
    const skipped = {
      ...empty,
      status: "skipped_has_images",
      intakeItemId: intakeItemId || null,
      shopifyProductId: product.id,
    };
    if (intakeItemId) await stampFromImageGateResult(skipped, { log });
    return skipped;
  }

  // Caller-supplied signed URLs: upload without Intake session login.
  const callerUrls = normalizeCallerImageUrls(imageUrlsHint);
  if (callerUrls.length) {
    const store = String(process.env.SHOPIFY_STORE || "").trim();
    const token = String(process.env.SHOPIFY_ACCESS_TOKEN || "").trim();
    if (!store || !token) {
      return { ...empty, status: "skipped_disabled", reason: "missing_shopify_creds" };
    }
    const uploadedFromUrls = await uploadCallerImageUrlsToShopifyProduct(product, callerUrls, {
      log,
    });
    if (uploadedFromUrls.status === "uploaded") {
      const ok = {
        product: uploadedFromUrls.product,
        status: "uploaded",
        uploaded: uploadedFromUrls.uploaded,
        blocked: false,
        intakeItemId: intakeItemId || null,
        shopifyProductId: product.id,
        reason: null,
        source: "caller_image_urls",
      };
      if (intakeItemId) await stampFromImageGateResult(ok, { log });
      return ok;
    }
    // Fall through to Intake lookup if URL upload failed and creds exist.
  }

  if (!intakeShopifyImagesEnabled()) {
    return {
      ...empty,
      status: "skipped_disabled",
      reason: intakeShopifyImagesConfigured() ? "disabled" : "missing_intake_creds",
    };
  }

  let item = null;
  try {
    if (intakeItemId) {
      item = await fetchIntakeItemById(intakeItemId, { log });
    }
    if (!item) {
      item = await fetchIntakeItemByFullName(product.title, { log });
      intakeItemId = item?.id || intakeItemId || null;
    }
  } catch (err) {
    log?.("warn", {
      event: "intake.lookup.failed",
      shopifyProductId: product.id,
      productTitle: product.title,
      message: err?.message || String(err),
    });
    const failed = {
      ...empty,
      status: "upload_failed",
      blocked: true,
      intakeItemId: intakeItemId || null,
      shopifyProductId: product.id,
      reason: err?.message || "intake_lookup_failed",
    };
    if (intakeItemId) await stampFromImageGateResult(failed, { log });
    return failed;
  }

  if (!item) {
    // Legacy / non-Intake catalog: do not block Webflow/Google.
    return { ...empty, status: "skipped_no_intake", reason: "no_intake_match" };
  }

  const urls = orderedIntakePhotoUrls(item);
  if (!urls.length) {
    log?.("info", {
      event: "intake.images.waiting_photos",
      shopifyProductId: product.id,
      intakeItemId: item.id,
      productTitle: product.title,
    });
    const waiting = {
      ...empty,
      status: "skipped_no_photos",
      blocked: true,
      intakeItemId: item.id,
      shopifyProductId: product.id,
      reason: "intake_has_no_photos_yet",
    };
    await stampFromImageGateResult(waiting, { log });
    return waiting;
  }

  let cookie = null;
  try {
    cookie = await ensureIntakeSessionCookie(log);
  } catch (_err) {
    cookie = null;
  }

  let uploaded = 0;
  const createdSrcs = [];
  try {
    for (let i = 0; i < urls.length; i++) {
      const src = urls[i];
      try {
        const { buffer, filename } = await downloadImageBuffer(src, {
          cookie: /\/media\/view\//i.test(src) ? undefined : cookie,
        });
        const image = await createShopifyProductImage(product.id, {
          attachment: buffer.toString("base64"),
          filename,
          position: i + 1,
        });
        if (image?.src) createdSrcs.push(image.src);
        uploaded += 1;
      } catch (oneErr) {
        // Fallback: let Shopify fetch the public signed view URL.
        try {
          const image = await createShopifyProductImage(product.id, {
            src,
            position: i + 1,
          });
          if (image?.src) createdSrcs.push(image.src);
          uploaded += 1;
        } catch (twoErr) {
          log?.("warn", {
            event: "shopify.intake_image.upload_one_failed",
            shopifyProductId: product.id,
            index: i,
            message: twoErr?.message || oneErr?.message || String(twoErr),
          });
        }
      }
    }
  } catch (err) {
    log?.("error", {
      event: "shopify.intake_image.upload_failed",
      shopifyProductId: product.id,
      intakeItemId: item.id,
      message: err?.message || String(err),
    });
    const failed = {
      product,
      status: "upload_failed",
      uploaded,
      blocked: true,
      intakeItemId: item.id,
      shopifyProductId: product.id,
      reason: err?.message || "upload_failed",
    };
    await stampFromImageGateResult(failed, { log });
    return failed;
  }

  if (!uploaded) {
    const failed = {
      product,
      status: "upload_failed",
      uploaded: 0,
      blocked: true,
      intakeItemId: item.id,
      shopifyProductId: product.id,
      reason: "zero_images_uploaded",
    };
    await stampFromImageGateResult(failed, { log });
    return failed;
  }

  const next = {
    ...product,
    images: createdSrcs.length
      ? createdSrcs.map((src, i) => ({ src, position: i + 1 }))
      : product.images,
  };
  log?.("info", {
    event: "shopify.intake_image.uploaded",
    shopifyProductId: product.id,
    intakeItemId: item.id,
    uploaded,
    productTitle: product.title,
  });
  const ok = {
    product: next,
    status: "uploaded",
    uploaded,
    blocked: false,
    intakeItemId: item.id,
    shopifyProductId: product.id,
    reason: null,
  };
  await stampFromImageGateResult(ok, { log });
  return ok;
}

/**
 * Poll Shopify by full name, then attach Intake images.
 * Used after Traxia e-com Save when the product may not be in the sync crawl yet.
 */
export async function ensureIntakeImagesForFullName(
  fullName,
  {
    log,
    intakeItemId = null,
    imageUrls = null,
    maxWaitMs = 180000,
    intervalMs = 5000,
  } = {}
) {
  const name = normalizeFullItemName(fullName);
  if (!name) {
    return { ok: false, reason: "missing_name", product: null };
  }
  const hasCallerUrls = normalizeCallerImageUrls(imageUrls).length > 0;
  const shopifyReady =
    String(process.env.SHOPIFY_STORE || "").trim() &&
    String(process.env.SHOPIFY_ACCESS_TOKEN || "").trim();
  // Allow URL-only handoff without Intake login; otherwise require Intake creds.
  if (!hasCallerUrls && !intakeShopifyImagesEnabled()) {
    return {
      ok: false,
      reason: intakeShopifyImagesConfigured() ? "disabled" : "missing_intake_creds",
      product: null,
    };
  }
  if (hasCallerUrls && !shopifyReady) {
    return { ok: false, reason: "missing_shopify_creds", product: null };
  }

  if (intakeItemId) {
    await stampIntakeShopifyImageSync(intakeItemId, {
      status: "pending",
      detail: "Waiting for Shopify product (Traxia e-com Save) then Intake→Shopify images",
      log,
    });
  }

  const shopifyProduct = await pollShopifyProductByFullName(name, {
    log,
    maxWaitMs,
    intervalMs,
    backoff: true,
  });
  if (!shopifyProduct?.id) {
    if (intakeItemId) {
      await stampIntakeShopifyImageSync(intakeItemId, {
        status: "pending",
        detail: "Shopify product not found yet by full name — will retry on sync",
        log,
      });
    }
    return { ok: false, reason: "shopify_not_found", product: null, name, intakeItemId };
  }

  const result = await ensureShopifyProductHasIntakeImages(shopifyProduct, {
    log,
    intakeItemId,
    imageUrls,
  });
  return {
    ok: result.status === "uploaded" || result.status === "skipped_has_images",
    ...result,
    name,
    shopifyProductId: shopifyProduct.id,
    intakeItemId: result.intakeItemId || intakeItemId || null,
  };
}

/**
 * Later Intake photo → append onto existing Shopify product + update sync note.
 * If the product is not live yet, leave pending for ensure-by-name / sync gate.
 */
export async function handleIntakePhotoAdded(
  {
    intakeItemId,
    photoId = null,
    imageUrl = null,
    name = null,
    shopifyProductId = null,
  } = {},
  { log, maxWaitMs = 0 } = {}
) {
  const id = String(intakeItemId || "").trim();
  if (!id || !UUID_RE.test(id)) {
    return { ok: false, reason: "missing_intake_item_id" };
  }
  if (!intakeShopifyImagesEnabled()) {
    return {
      ok: false,
      reason: intakeShopifyImagesConfigured() ? "disabled" : "missing_intake_creds",
    };
  }

  await stampIntakeShopifyImageSync(id, {
    status: "pending",
    shopifyProductId,
    photoId,
    detail: "Needs Shopify image sync (new Intake photo)",
    log,
  });

  let product = null;
  const pid = String(shopifyProductId || "").trim();
  if (pid) {
    product = await fetchShopifyProductById(pid, { log });
  }

  let item = null;
  try {
    item = await fetchIntakeItemById(id, { log });
  } catch (err) {
    return { ok: false, reason: err?.message || "intake_lookup_failed", intakeItemId: id };
  }
  if (!item) {
    return { ok: false, reason: "intake_not_found", intakeItemId: id };
  }

  const knownPid = pid || String(item.shopifyProductId || item.shopify_product_id || "").trim();
  if (!product && knownPid) {
    product = await fetchShopifyProductById(knownPid, { log });
  }

  const fullName = normalizeFullItemName(name || item.name || "");
  if (!product && fullName) {
    if (maxWaitMs > 0) {
      product = await pollShopifyProductByFullName(fullName, { log, maxWaitMs });
    } else {
      product = await fetchShopifyProductByFullName(fullName, { log });
    }
  }

  if (!product?.id) {
    return {
      ok: false,
      deferred: true,
      reason: "shopify_not_found",
      intakeItemId: id,
      name: fullName || null,
    };
  }

  // First-time empty product: attach full gallery (same as Traxia kickoff).
  if (!shopifyProductHasDisplayImages(product)) {
    const result = await ensureShopifyProductHasIntakeImages(product, {
      log,
      intakeItemId: id,
    });
    return {
      ok: result.status === "uploaded" || result.status === "skipped_has_images",
      ...result,
      name: fullName || product.title,
      shopifyProductId: product.id,
    };
  }

  // Product already has images — append this one new photo.
  let src = absoluteIntakeUrl(imageUrl || "");
  if (!src) {
    const urls = orderedIntakePhotoUrls(item);
    src = urls[urls.length - 1] || "";
  }
  if (!src) {
    const failed = {
      ok: false,
      status: "upload_failed",
      reason: "missing_image_url",
      intakeItemId: id,
      shopifyProductId: product.id,
    };
    await stampIntakeShopifyImageSync(id, {
      status: "failed",
      shopifyProductId: product.id,
      photoId,
      detail: "Shopify image sync failed: missing image URL",
      log,
    });
    return failed;
  }

  let cookie = null;
  try {
    cookie = await ensureIntakeSessionCookie(log);
  } catch (_err) {
    cookie = null;
  }

  const position = (Array.isArray(product.images) ? product.images.length : 0) + 1;
  try {
    try {
      const { buffer, filename } = await downloadImageBuffer(src, {
        cookie: /\/media\/view\//i.test(src) ? undefined : cookie,
      });
      await createShopifyProductImage(product.id, {
        attachment: buffer.toString("base64"),
        filename,
        position,
      });
    } catch (_oneErr) {
      await createShopifyProductImage(product.id, { src, position });
    }
  } catch (err) {
    log?.("warn", {
      event: "shopify.intake_image.append_failed",
      shopifyProductId: product.id,
      intakeItemId: id,
      photoId,
      message: err?.message || String(err),
    });
    await stampIntakeShopifyImageSync(id, {
      status: "failed",
      shopifyProductId: product.id,
      photoId,
      detail: `Shopify image sync failed: ${err?.message || "append_failed"}`,
      log,
    });
    return {
      ok: false,
      status: "upload_failed",
      reason: err?.message || "append_failed",
      intakeItemId: id,
      shopifyProductId: product.id,
    };
  }

  await stampIntakeShopifyImageSync(id, {
    status: "synced",
    shopifyProductId: product.id,
    photoId,
    uploaded: 1,
    detail: photoId
      ? `Shopify images synced (photo ${photoId})`
      : "Shopify images synced (appended)",
    log,
  });
  log?.("info", {
    event: "shopify.intake_image.appended",
    shopifyProductId: product.id,
    intakeItemId: id,
    photoId,
  });
  return {
    ok: true,
    status: "uploaded",
    uploaded: 1,
    intakeItemId: id,
    shopifyProductId: product.id,
    name: fullName || product.title,
  };
}
