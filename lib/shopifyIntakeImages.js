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
 *   SHOPIFY_TITLE_POLL_INTERVAL_MS / SHOPIFY_TITLE_POLL_MS  title poll cadence (default 5000)
 *   SHOPIFY_TITLE_POLL_MAX_DELAY_MS  backoff cap (default 5000 = steady ~5s)
 */

import axios from "axios";
import { createHash } from "crypto";
import sharp from "sharp";
import {
  getEcommerceClassificationFromTags,
  getEcommerceVerticalOverrideFromTags,
} from "../ecommerceTags.js";
import { parseDimsFromTitle } from "./freightPalletize.js";
import {
  installShopifyThrottle,
  runWithShopifyPriority,
  shopifyRetryDelayMs,
} from "./shopifyThrottle.js";

installShopifyThrottle(axios);

/** Webhook coalesce stays inside this ceiling. The Intake write does not wait for it. */
export const SHOPIFY_WEBHOOK_COALESCE_CEILING_MS = 3000;

const DEFAULT_INTAKE_API_BASE = "https://lostfound-pricing-api.onrender.com";
const INTAKE_ITEM_ID_RE =
  /(?:lost-and-found-intake|field-buyer)\/item\.html\?[^#\s"']*?\bid=([0-9a-f-]{36})\b|(?:\/api\/lost-and-found-intake\/items\/)([0-9a-f-]{36})\b|\bintake[_-]?id[=:]([0-9a-f-]{36})\b|\bintake-([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\b/i;
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
/** REST Product Image accepts png/gif/jpg — keep attachments under this for reliable JSON posts. */
const SHOPIFY_ATTACH_MAX_BYTES = 5 * 1024 * 1024;

let cachedCookie = null;
let cachedCookieAt = 0;
const COOKIE_TTL_MS = 6 * 60 * 60 * 1000;
/** In-flight title polls keyed by normalized name — share one poll; stop after first hit. */
const titlePollInflight = new Map();
/** Recent title→product hits so later ensure/photo-added skip re-polling. */
const titlePollHitCache = new Map();
const TITLE_HIT_CACHE_TTL_MS = 30 * 60 * 1000;

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

/** Shopify often returns `errors` as a nested object — never coerce to "[object Object]". */
export function formatShopifyErrorBody(data) {
  if (data == null || data === "") return "";
  if (typeof data === "string") return data.slice(0, 400);
  try {
    return JSON.stringify(data).slice(0, 400);
  } catch (_err) {
    return String(data).slice(0, 400);
  }
}

export function sniffImageKind(buffer) {
  const buf = Buffer.isBuffer(buffer) ? buffer : Buffer.from(buffer || []);
  if (buf.length >= 3 && buf[0] === 0xff && buf[1] === 0xd8 && buf[2] === 0xff) return "jpeg";
  if (
    buf.length >= 8 &&
    buf[0] === 0x89 &&
    buf[1] === 0x50 &&
    buf[2] === 0x4e &&
    buf[3] === 0x47
  ) {
    return "png";
  }
  if (
    buf.length >= 6 &&
    buf[0] === 0x47 &&
    buf[1] === 0x49 &&
    buf[2] === 0x46
  ) {
    return "gif";
  }
  if (
    buf.length >= 12 &&
    buf.toString("ascii", 0, 4) === "RIFF" &&
    buf.toString("ascii", 8, 12) === "WEBP"
  ) {
    return "webp";
  }
  return "unknown";
}

/**
 * Shopify REST Product Image only accepts png/gif/jpg. Convert WebP/unknown and
 * recompress oversized buffers so attachment uploads stop 422'ing.
 */
export async function prepareShopifyAttachment(buffer) {
  const input = Buffer.isBuffer(buffer) ? buffer : Buffer.from(buffer || []);
  if (!input.length) throw new Error("Empty image buffer");
  const kind = sniffImageKind(input);
  if (
    (kind === "jpeg" || kind === "png" || kind === "gif") &&
    input.length <= SHOPIFY_ATTACH_MAX_BYTES
  ) {
    const filename =
      kind === "png" ? "intake.png" : kind === "gif" ? "intake.gif" : "intake.jpg";
    return { buffer: input, filename, kind, converted: false };
  }

  let quality = 85;
  let out = await sharp(input, { failOn: "none" })
    .rotate()
    .jpeg({ quality, mozjpeg: true })
    .toBuffer();
  while (out.length > SHOPIFY_ATTACH_MAX_BYTES && quality > 55) {
    quality -= 10;
    out = await sharp(input, { failOn: "none" })
      .rotate()
      .jpeg({ quality, mozjpeg: true })
      .toBuffer();
  }
  if (out.length > SHOPIFY_ATTACH_MAX_BYTES) {
    out = await sharp(input, { failOn: "none" })
      .rotate()
      .resize({
        width: 2400,
        height: 2400,
        fit: "inside",
        withoutEnlargement: true,
      })
      .jpeg({ quality: 78, mozjpeg: true })
      .toBuffer();
  }
  return { buffer: out, filename: "intake.jpg", kind: "jpeg", converted: true };
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

/**
 * lite: the same item fields and photos without the audit history and without
 * the repair passes (the full reply is 0.7–2.9 MB). The listing path reads
 * only item fields, so it asks for lite.
 */
export async function fetchIntakeItemById(itemId, { log, lite = false } = {}) {
  const id = String(itemId || "").trim();
  if (!id || !UUID_RE.test(id)) return null;
  const cookie = await ensureIntakeSessionCookie(log);
  const res = await intakeGet(`/api/lost-and-found-intake/items/${id}`, {
    cookie,
    ...(lite ? { params: { lite: 1 } } : {}),
  });
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

/** Intake photos in display order. primaryFirst matches Shopify featured-image pushes. */
export function orderedIntakePhotos(item, { primaryFirst = true } = {}) {
  const photos = Array.isArray(item?.photos) ? [...item.photos] : [];
  photos.sort((a, b) => {
    const ao = Number(a?.sortOrder ?? a?.sort_order ?? 0);
    const bo = Number(b?.sortOrder ?? b?.sort_order ?? 0);
    if (ao !== bo) return ao - bo;
    return String(a?.id || "").localeCompare(String(b?.id || ""));
  });
  if (primaryFirst) {
    const primaryId = String(item?.primaryPhotoId || item?.primary_photo_id || "").trim();
    if (primaryId) {
      const idx = photos.findIndex((p) => String(p?.id) === primaryId);
      if (idx > 0) {
        const [p] = photos.splice(idx, 1);
        photos.unshift(p);
      }
    }
  }
  const max = Math.min(
    50,
    Math.max(1, parseInt(process.env.INTAKE_SHOPIFY_IMAGES_MAX || "25", 10) || 25)
  );
  return photos.slice(0, max);
}

export function orderedIntakePhotoUrls(item, { primaryFirst = true } = {}) {
  const urls = [];
  for (const p of orderedIntakePhotos(item, { primaryFirst })) {
    const abs = absoluteIntakeUrl(p?.url || p?.thumbUrl || "");
    if (abs && /\/media\/view\//i.test(abs)) {
      urls.push(abs);
    } else if (abs) {
      urls.push(abs);
    }
  }
  return urls;
}

export function orderedIntakePhotoIds(item, { primaryFirst = false } = {}) {
  return orderedIntakePhotos(item, { primaryFirst })
    .map((p) => String(p?.id || "").trim())
    .filter(Boolean);
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
      const image = await uploadIntakeUrlToShopifyProduct(product.id, src, {
        cookie,
        position: i + 1,
        log,
      });
      if (image?.src) createdSrcs.push(image.src);
      uploaded += 1;
    } catch (oneErr) {
      log?.("warn", {
        event: "shopify.intake_image.upload_one_failed",
        shopifyProductId: product.id,
        index: i,
        message: oneErr?.message || String(oneErr),
      });
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
  const tryOnce = async (useCookie) => {
    const res = await axios.get(url, {
      responseType: "arraybuffer",
      timeout: 60000,
      headers: useCookie && cookie ? { Cookie: cookie } : {},
      validateStatus: () => true,
      maxRedirects: 5,
    });
    return res;
  };

  // Signed media/view URLs are cookie-free; only send session if the signed GET fails.
  let res = await tryOnce(false);
  if (res.status >= 400 && cookie) {
    res = await tryOnce(true);
  }
  if (res.status >= 400) {
    throw new Error(`Download image HTTP ${res.status}`);
  }
  const buf = Buffer.from(res.data);
  if (!buf.length) throw new Error("Empty image download");
  const ctype = String(res.headers?.["content-type"] || "").toLowerCase();
  // Prefer magic bytes — Shopify REST rejects WebP; never label WebP as a shopify filename.
  const kind = sniffImageKind(buf);
  let filename = "intake.jpg";
  if (kind === "png") filename = "intake.png";
  else if (kind === "gif") filename = "intake.gif";
  else if (kind === "jpeg") filename = "intake.jpg";
  else if (ctype.includes("png")) filename = "intake.png";
  else if (ctype.includes("gif")) filename = "intake.gif";
  // Never pass through webp/heic/unknown filenames — prepareShopifyAttachment converts bytes.
  return { buffer: buf, filename, contentType: ctype, kind };
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
  // Only set position when caller asks — occupied positions often 422 on append.
  if (position != null) image.position = Number(position);
  const res = await axios.post(
    `${base}/products/${productId}/images.json`,
    { image },
    {
      headers: shopifyHeaders(),
      timeout: 90000,
      validateStatus: () => true,
      maxBodyLength: 25 * 1024 * 1024,
      maxContentLength: 25 * 1024 * 1024,
    }
  );
  if (res.status >= 400) {
    const msg = formatShopifyErrorBody(
      res.data?.errors ?? res.data?.error ?? res.data
    );
    throw new Error(`Shopify image create HTTP ${res.status}: ${msg || res.statusText || "unknown"}`);
  }
  return res.data?.image || null;
}

/**
 * Download → prepare (jpeg/png ≤5MB) → attachment upload.
 * Appends omit position (avoids 422 when slot already taken). Empty-gallery callers may pass position.
 */
async function uploadIntakeUrlToShopifyProduct(
  productId,
  src,
  { cookie = null, position = null, log = null } = {}
) {
  // Always download here (with cookie fallback) so Shopify gets a clean jpeg/png attachment.
  // Relying on image.src alone often 422s: Shopify's fetchers cannot always reach signed Intake URLs.
  const downloaded = await downloadImageBuffer(src, { cookie });
  const prepared = await prepareShopifyAttachment(downloaded.buffer);
  const attachPayload = {
    attachment: prepared.buffer.toString("base64"),
    filename: prepared.filename,
  };
  try {
    return await createShopifyProductImage(productId, {
      ...attachPayload,
      ...(position != null ? { position } : {}),
    });
  } catch (attachErr) {
    let lastAttachErr = attachErr;
    log?.("warn", {
      event: "shopify.intake_image.attachment_failed",
      shopifyProductId: productId,
      message: attachErr?.message || String(attachErr),
      bytes: prepared.buffer.length,
      filename: prepared.filename,
      kind: prepared.kind,
      converted: prepared.converted === true,
      position: position != null ? position : null,
    });
    // Retry once without position if a conflicting slot may have caused the 422.
    if (position != null) {
      try {
        return await createShopifyProductImage(productId, attachPayload);
      } catch (retryErr) {
        lastAttachErr = retryErr;
        log?.("warn", {
          event: "shopify.intake_image.attachment_retry_no_position_failed",
          shopifyProductId: productId,
          message: retryErr?.message || String(retryErr),
        });
      }
    }
    // Last resort: Shopify fetches the public signed URL itself (omit position on retry).
    try {
      return await createShopifyProductImage(productId, { src });
    } catch (srcErr) {
      throw new Error(
        `${lastAttachErr?.message || "attachment_failed"}; src: ${
          srcErr?.message || "src_failed"
        }`
      );
    }
  }
}

async function stripNoImagesTag(productId, { log } = {}) {
  const id = String(productId || "").trim();
  const base = shopifyRestBase();
  if (!base || !id) return { ok: false, reason: "missing" };
  try {
    const product = await fetchShopifyProductById(id, { log });
    if (!product) return { ok: false, reason: "not_found" };
    const tags = String(product.tags || "")
      .split(",")
      .map((t) => t.trim())
      .filter(Boolean);
    const next = tags.filter((t) => t.toLowerCase() !== "no-images");
    if (next.length === tags.length) return { ok: true, changed: false };
    const res = await axios.put(
      `${base}/products/${id}.json`,
      { product: { id, tags: next.join(", ") } },
      { headers: shopifyHeaders(), timeout: 30000, validateStatus: () => true }
    );
    if (res.status >= 400) {
      log?.("warn", {
        event: "shopify.strip_no_images_tag.failed",
        shopifyProductId: id,
        status: res.status,
        message: formatShopifyErrorBody(res.data?.errors ?? res.data),
      });
      return { ok: false, reason: `http_${res.status}` };
    }
    log?.("info", {
      event: "shopify.strip_no_images_tag.ok",
      shopifyProductId: id,
    });
    return { ok: true, changed: true };
  } catch (err) {
    log?.("warn", {
      event: "shopify.strip_no_images_tag.error",
      shopifyProductId: id,
      message: err?.message || String(err),
    });
    return { ok: false, reason: err?.message || "strip_error" };
  }
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
/** Prefer first non-empty variant SKU, then barcode (Traxia-style). */
export function extractShopifyVariantSku(product) {
  const variants = Array.isArray(product?.variants) ? product.variants : [];
  for (const v of variants) {
    const sku = String(v?.sku || "").trim();
    if (sku) return sku.slice(0, 64);
  }
  for (const v of variants) {
    const barcode = String(v?.barcode || "").trim();
    if (barcode) return barcode.slice(0, 64);
  }
  return null;
}

function extractShopifyVariantPrice(product) {
  const variants = Array.isArray(product?.variants) ? product.variants : [];
  for (const v of variants) {
    const raw = v?.price;
    if (raw == null || String(raw).trim() === "") continue;
    const n = Number(raw);
    if (Number.isFinite(n) && n >= 0) return n;
  }
  return null;
}

function positiveMeasure(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return null;
  return Math.round(n * 100) / 100;
}

function variantWeightLbs(variant) {
  if (!variant || typeof variant !== "object") return null;
  const grams = positiveMeasure(variant.grams);
  if (grams) return positiveMeasure(grams / 453.59237);
  const weight = positiveMeasure(variant.weight);
  if (!weight) return null;
  const unit = String(variant.weight_unit || variant.weightUnit || "lb").toLowerCase();
  if (unit === "g" || unit === "grams") return positiveMeasure(weight / 453.59237);
  if (unit === "kg" || unit === "kilograms") return positiveMeasure(weight * 2.20462262);
  if (unit === "oz" || unit === "ounces") return positiveMeasure(weight / 16);
  return weight;
}

/**
 * Real Traxia/Shopify measurements.
 * User-written title W×D×H wins over Width:/Depth:/Height: tags (stops a tag remix
 * like 75/42/250 from overwriting title dims 42×23×75). Tags fill only missing axes.
 * Variant grams/weight fill weight when no Weight tag exists.
 * Zero defaults are omitted so they cannot wipe Intake.
 */
export function extractShopifyListingFacts(product) {
  const facts = { width: null, depth: null, height: null, weightLbs: null };
  // Only measurements stated in the title. A two-number "25 x 35H" painting has
  // width and height — never invent a depth here for Intake storage.
  const titleDims = parseDimsFromTitle(product?.title || "");
  if (titleDims) {
    if (titleDims.width != null) facts.width = positiveMeasure(titleDims.width);
    if (titleDims.depth != null) facts.depth = positiveMeasure(titleDims.depth);
    if (titleDims.height != null) facts.height = positiveMeasure(titleDims.height);
  }
  const tags = parseTagList(product?.tags);
  const axis = { width: "width", depth: "depth", height: "height", length: "depth", weight: "weightLbs" };
  for (const tag of tags) {
    const match = String(tag).match(/^(width|depth|height|length|weight)\s*:\s*([0-9]+(?:\.[0-9]+)?)\b/i);
    if (!match) continue;
    const key = axis[match[1].toLowerCase()];
    const number = positiveMeasure(match[2]);
    if (!key || number == null || facts[key] != null) continue;
    facts[key] = number;
  }
  if (facts.weightLbs == null) {
    const variants = Array.isArray(product?.variants) ? product.variants : [];
    for (const variant of variants) {
      const pounds = variantWeightLbs(variant);
      if (pounds) {
        facts.weightLbs = pounds;
        break;
      }
    }
  }
  return facts;
}

/** Collapse rapid stamps for the same Intake item into one POST. */
const stampInflight = new Map(); // id -> Promise
const stampPending = new Map(); // id -> latest args

function preferNewerShopifyStamp(prev = {}, next = {}) {
  const prevAt = Date.parse(prev.shopifyUpdatedAt || "") || 0;
  const nextAt = Date.parse(next.shopifyUpdatedAt || "") || 0;
  if (prevAt && nextAt && nextAt < prevAt) return prev;
  return { ...prev, ...next };
}

export async function stampIntakeShopifyImageSync(
  intakeItemId,
  opts = {}
) {
  const id = String(intakeItemId || "").trim();
  if (!id || !UUID_RE.test(id)) {
    return { ok: false, reason: "bad_intake_id" };
  }
  if (!intakeShopifyImagesConfigured()) {
    return { ok: false, reason: "missing_intake_creds" };
  }
  if (stampInflight.has(id)) {
    const prev = stampPending.get(id) || {};
    stampPending.set(id, preferNewerShopifyStamp(prev, { ...opts, intakeItemId: id }));
    opts.log?.("info", {
      event: "intake.shopify_sync_stamp.coalesced",
      intakeItemId: id,
      reason: "in_flight",
    });
    return {
      ok: true,
      coalesced: true,
      reason: "in_flight_collapsed",
      data: { ok: true, resyncQueued: false },
    };
  }
  const run = (async () => {
    try {
      return await stampIntakeShopifyImageSyncNow(id, opts);
    } finally {
      stampInflight.delete(id);
      const pending = stampPending.get(id);
      if (pending) {
        stampPending.delete(id);
        const { intakeItemId: _iid, ...rest } = pending;
        void stampIntakeShopifyImageSync(id, rest).catch(() => {});
      }
    }
  })();
  stampInflight.set(id, run);
  return run;
}

async function stampIntakeShopifyImageSyncNow(
  intakeItemId,
  {
    status = null,
    shopifyProductId = null,
    traxiaSku = null,
    sku = null,
    name = null,
    retailPrice = null,
    price = null,
    uploaded = null,
    photoId = null,
    detail = null,
    resyncListing = null,
    bodyHtml = null,
    description = null,
    notes = null,
    conditionIssues = null,
    dimWidth = null,
    dimDepth = null,
    dimHeight = null,
    weightLbs = null,
    shopifyLinkState = null,
    lastShopifyProductId = null,
    historyLine = null,
    shopifyUpdatedAt = null,
    photoIds = null,
    bodyFingerprint = null,
    bodyIsIntakeBuild = null,
    log = null,
  } = {}
) {
  const id = String(intakeItemId || "").trim();
  try {
    const cookie = await ensureIntakeSessionCookie(log);
    const headers = {
      "Content-Type": "application/json",
      Accept: "application/json",
      Cookie: cookie,
    };
    const secret = String(process.env.INTAKE_SYNC_HOOK_SECRET || "").trim();
    if (secret) headers["X-Intake-Sync-Secret"] = secret;
    const skuVal = String(traxiaSku || sku || "").trim() || null;
    const nameVal = normalizeFullItemName(name);
    const priceVal =
      retailPrice != null && String(retailPrice).trim() !== ""
        ? Number(retailPrice)
        : price != null && String(price).trim() !== ""
          ? Number(price)
          : null;
    const body = {
      shopifyProductId: shopifyProductId != null ? String(shopifyProductId) : null,
      traxiaSku: skuVal,
      sku: skuVal,
      uploaded,
      photoId,
      detail,
    };
    if (status != null && String(status).trim() !== "") {
      body.status = String(status).trim();
    }
    if (nameVal) body.name = nameVal;
    if (Number.isFinite(priceVal) && priceVal >= 0) {
      body.retailPrice = priceVal;
      body.price = priceVal;
    }
    // Only real measurements. Zero Traxia defaults are left off the stamp.
    const dimFields = { dimWidth, dimDepth, dimHeight, weightLbs };
    for (const [key, value] of Object.entries(dimFields)) {
      const n = Number(value);
      if (Number.isFinite(n) && n > 0) body[key] = Math.round(n * 100) / 100;
    }
    // Pass Shopify Description for Intake-side filter (warehouse notes ignored;
    // customer damage/condition may land in Extra/notes; Helper prose restored on resync).
    if (bodyHtml != null) body.bodyHtml = String(bodyHtml);
    else if (description != null) body.description = String(description);
    if (notes != null) body.notes = String(notes);
    if (conditionIssues != null) body.conditionIssues = String(conditionIssues);
    if (resyncListing != null) body.resyncListing = Boolean(resyncListing);
    // What Shopify's description is now, in the same form the listing-update
    // reply uses, so Pricing Engine compares like with like.
    if (bodyFingerprint != null) body.bodyFingerprint = String(bodyFingerprint);
    if (bodyIsIntakeBuild != null) body.bodyIsIntakeBuild = Boolean(bodyIsIntakeBuild);
    if (shopifyLinkState) body.shopifyLinkState = String(shopifyLinkState);
    if (lastShopifyProductId) body.lastShopifyProductId = String(lastShopifyProductId);
    if (historyLine) body.historyLine = String(historyLine);
    if (shopifyUpdatedAt) body.shopifyUpdatedAt = String(shopifyUpdatedAt);
    if (Array.isArray(photoIds) && photoIds.length) {
      body.photoIds = photoIds.map((pid) => String(pid));
    }
    body.allowOpenAi = false;
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

export function intakeLockedProductId(item) {
  return String(item?.shopifyProductId || item?.shopify_product_id || "").trim();
}

export function intakeLockedSku(item) {
  return String(item?.traxiaSku || item?.traxia_sku || item?.sku || "").trim();
}

/**
 * Find once, then lock.
 * A stored product id wins. SKU is next. Exact title is only for a row that
 * does not already have a product id or SKU. A title match cannot steal a row
 * that is already locked to a different product.
 */
export function selectIntakeItemForShopifyProduct({
  shopifyProductId,
  sku = "",
  byProductId = null,
  bySku = null,
  byExternalId = null,
  byExactTitle = null,
} = {}) {
  const pid = String(shopifyProductId || "").trim();
  const wantSku = String(sku || "").trim().toLowerCase();
  if (byProductId?.id && pid && intakeLockedProductId(byProductId) === pid) {
    return { item: byProductId, via: "shopify_product_id", locked: true };
  }
  if (bySku?.id) {
    const existing = intakeLockedProductId(bySku);
    const rowSku = intakeLockedSku(bySku).toLowerCase();
    const skuOk = !wantSku || !rowSku || rowSku === wantSku;
    if (skuOk && (!existing || existing === pid)) {
      return { item: bySku, via: "traxia_sku", locked: true };
    }
  }
  const candidates = [
    [byExternalId, "external_link"],
    [byExactTitle, "exact_title"],
  ];
  for (const [item, via] of candidates) {
    if (!item?.id) continue;
    const existing = intakeLockedProductId(item);
    const rowSku = intakeLockedSku(item).toLowerCase();
    const state = shopifyLinkState(item);
    if (state === "ambiguous") continue;
    if (state === "unlinked") {
      return { item, via: "exact_name_relink", locked: false, wake: true };
    }
    if (existing && existing !== pid) continue;
    if (rowSku && wantSku && rowSku !== wantSku) continue;
    if (via === "exact_title" && (existing || rowSku)) {
      if (existing === pid || (wantSku && rowSku === wantSku)) {
        return { item, via, locked: true };
      }
      continue;
    }
    return { item, via, locked: false };
  }
  return { item: null, via: "none", locked: false };
}

/** linked | unlinked | ambiguous | missing. A stored id with no flag is still a live lock. */
export function shopifyLinkState(item) {
  const raw = String(item?.shopifyLinkState || item?.shopify_link_state || "")
    .trim()
    .toLowerCase();
  if (raw === "unlinked" || raw === "ambiguous" || raw === "linked") return raw;
  return intakeLockedProductId(item) ? "linked" : "missing";
}

export function itemIsLiveLocked(item) {
  return shopifyLinkState(item) === "linked" && Boolean(intakeLockedProductId(item));
}

/** Full title, case-insensitive, optional trailing " - AS IS" ignored. Not a fuzzy match. */
export function titlesMatchForRelink(a, b) {
  const strip = (value) =>
    normalizeFullItemName(value)
      .replace(/\s*[-–—]\s*AS\s*-?\s*IS\s*$/i, "")
      .replace(/\s+/g, " ")
      .trim()
      .toLowerCase();
  const left = strip(a);
  const right = strip(b);
  return Boolean(left) && left === right;
}

/** Draft stays linked so a later wake-up can republish the same id. Archived, unpublished, and gone do not. */
export function shopifyStatusUnlinks(status) {
  const value = String(status || "").trim().toLowerCase();
  return value === "archived" || value === "unpublished" || value === "gone";
}

/**
 * The Shopify product disappeared. Keep the Intake row, photos, SKU, and listing copy.
 * The old product id stays as history and the link flag is unlinked.
 */
export function planShopifyProductGone(item, { shopifyProductId, reason = "deleted" } = {}) {
  const id = String(shopifyProductId || intakeLockedProductId(item) || "").trim();
  const photos = Array.isArray(item?.photos) ? item.photos : null;
  return {
    action: "unlink",
    deleteItem: false,
    clearDescription: false,
    clearPhotos: false,
    clearSku: false,
    shopifyLinkState: "unlinked",
    status: "unlinked",
    shopifyProductId: id || null,
    lastShopifyProductId: id || null,
    traxiaSku: intakeLockedSku(item) || null,
    description: item?.description ?? null,
    photoCount: photos ? photos.length : item?.photoCount ?? null,
    history: `Shopify product ${id || "unknown"} went away (${reason}). Intake kept; link marked unlinked.`,
  };
}

/**
 * A product id we have not stored may relink only an unlinked or never-linked row
 * with the exact title. A live lock is never stolen. Two matches are left alone.
 */
export function planShopifyRelink({ shopifyProductId, title, sku = "", candidates = [] } = {}) {
  const pid = String(shopifyProductId || "").trim();
  const rows = Array.isArray(candidates) ? candidates : [];
  const already = rows.find((item) => itemIsLiveLocked(item) && intakeLockedProductId(item) === pid);
  if (already) return { action: "already_linked", relink: false, wake: false, item: already };
  const named = rows.filter((item) => titlesMatchForRelink(item?.name, title));
  const ignoredLocked = named.filter(
    (item) => itemIsLiveLocked(item) && intakeLockedProductId(item) !== pid
  ).length;
  const eligible = named.filter((item) => {
    const state = shopifyLinkState(item);
    return state === "unlinked" || state === "missing";
  });
  if (eligible.length > 1) {
    return {
      action: "ambiguous",
      relink: false,
      wake: false,
      matchIds: eligible.map((item) => item.id),
      ignoredLocked,
      history: "Shopify relink skipped: more than one Intake item has this exact name.",
    };
  }
  if (eligible.length === 1) {
    const item = eligible[0];
    return {
      action: "relink",
      relink: true,
      wake: true,
      item,
      shopifyProductId: pid,
      traxiaSku: String(sku || intakeLockedSku(item) || "").trim() || null,
      shopifyLinkState: "linked",
      status: "pending",
      history: "Shopify product relinked",
      ignoredLocked,
    };
  }
  return { action: "none", relink: false, wake: false, ignoredLocked };
}

/** Staff search: one exact Shopify title, or a plain no-match / too-many answer. No fuzzy match and no create. */
export function planManualShopifyLink({ title, products = [] } = {}) {
  const exact = (Array.isArray(products) ? products : []).filter((product) =>
    titlesMatchForRelink(product?.title, title)
  );
  if (exact.length === 0) {
    return {
      ok: false,
      linked: false,
      reason: "no_match",
      matchCount: 0,
      message: "No exact match found for that name.",
    };
  }
  if (exact.length > 1) {
    return {
      ok: false,
      linked: false,
      reason: "ambiguous",
      matchCount: exact.length,
      matches: exact.map((product) => ({
        id: String(product.id || ""),
        title: product.title || "",
        status: product.status || "",
      })),
      message: `Shopify link: no exact name match (${exact.length} candidates)`,
    };
  }
  const product = exact[0];
  return {
    ok: true,
    linked: true,
    reason: "exact",
    matchCount: 1,
    wake: true,
    shopifyProductId: String(product.id || "").trim(),
    traxiaSku: extractShopifyVariantSku(product),
    product,
    message: "Linked to the one exact title match.",
  };
}

/** True when Intake condition has a shopper flaw the Shopify body does not say yet. */
export function listingMissesCustomerFlaw(shopifyBody, notes, conditionIssues) {
  const flaw = stripShippingSentences(
    appendIntakeNotesToDescription("", notes, conditionIssues, { name: "Painting" })
  );
  if (!flaw) return false;
  const plain = stripListingDimJunk(shopifyBody).replace(/\s+/g, " ").toLowerCase();
  return !plain.includes(flaw.toLowerCase().replace(/\.$/, ""));
}

function listingPlainForCompare(text) {
  return stripShippingSentences(stripListingDimJunk(text)).replace(/\s+/g, " ").trim().toLowerCase();
}

/** The whole Shopify body is only a raw staff flaw or an AS IS paste. */
export function isRawFlawListingBody(text) {
  const plain = stripListingDimJunk(text).replace(/\s+/g, " ").trim();
  if (!plain) return false;
  const sentences = plain.split(/(?<=[.!?])\s+/).map((part) => part.trim()).filter(Boolean);
  if (!sentences.length) return false;
  return sentences.every(
    (sentence) =>
      /^AS IS\s*:/i.test(sentence) ||
      isGeneratedFlawSentence(sentence) ||
      isRawFlawFragmentSentence(sentence)
  );
}

/** Current Shopify prose is not the Intake prose we would write. */
export function listingBodyNeedsWrite(currentBody, desiredProse) {
  if (isRawFlawListingBody(currentBody)) {
    const desiredPlain = stripListingDimJunk(desiredProse).replace(/\s+/g, " ").trim();
    if (desiredPlain && !isRawFlawListingBody(desiredProse)) return true;
  }
  return listingPlainForCompare(currentBody) !== listingPlainForCompare(desiredProse);
}

/**
 * Prose compare strips the Dimensions/Weight footer, so a weight or size change
 * looks like a no-op and the tags update alone. The footer is its own write.
 */
export function listingBodyHtmlForUpdate(currentBody, prose, dims, { clearedBody = false } = {}) {
  const text = String(prose || "");
  if (!text.trim() || isDimsOnlyListingText(text)) return "";
  const listingDims = resolveListingDims(dims, currentBody);
  const html = composeShopifyBodyHtml(text, listingDims);
  if (!html) return "";
  if (
    listingBodyNeedsWrite(currentBody || "", text) ||
    clearedBody ||
    listingFooterNeedsWrite(currentBody || "", listingDims)
  ) {
    return html;
  }
  return "";
}

/** Body still contains a flaw sentence the current note no longer supports. */
export function listingCarriesStaleFlaw(shopifyBody, notes, conditionIssues, item) {
  const desired = appendIntakeNotesToDescription(shopifyBody, notes, conditionIssues, item);
  return listingBodyNeedsWrite(shopifyBody, desired);
}

/** Extra Information on a Shopify product, when the payload actually carries it. */
export function readLiveTraxiaExtra(product) {
  if (product && typeof product === "object") {
    if (Object.prototype.hasOwnProperty.call(product, "extraInformation") || Object.prototype.hasOwnProperty.call(product, "extra_information")) {
      return {
        known: true,
        text: String(product.extraInformation ?? product.extra_information ?? "").trim(),
      };
    }
    const tagged = parseTagList(product.tags).find((tag) => /^extra information\s*:/i.test(String(tag || "")));
    if (tagged) {
      return { known: true, text: String(tagged).replace(/^extra information\s*:/i, "").trim() };
    }
  }
  return { known: false, text: "" };
}

function conditionAfterClearedExtra(existing) {
  const kept = [];
  for (const clause of splitNoteClauses(existing)) {
    const cleaned = cleanNoteClause(clause);
    if (!cleaned) continue;
    if (naturalFlawSentence(cleaned) || positiveConditionSentence(cleaned) || isCustomerSilentNote(cleaned)) continue;
    if (/\b(?:smallt|smalli|smalt)\b/i.test(cleaned)) continue;
    if (/^(?:sold\s+)?as[\s-]is\b/i.test(cleaned)) continue;
    if (/^(?:this piece is in\s+)?(?:in\s+)?excellent\s+condition\.?$/i.test(cleaned)) continue;
    if (isGeneratedFlawSentence(cleaned.endsWith(".") ? cleaned : `${cleaned}.`)) continue;
    kept.push(cleaned);
  }
  return kept.join("\n").trim();
}

/**
 * The live Extra Information decides the flaw. A known empty note drops the
 * stored shorthand and asks for a body rewrite. An unknown note keeps the store.
 */
const FLAW_CUE_RE =
  /\b(?:AS\s*IS|noticeable wear|there is a\b|scratch(?:es|ed)?|crack(?:s|ed)?|chip(?:s|ped)?|stain(?:s|ed)?|dent(?:s|ed)?|scuff(?:s|ed)?|blemish(?:es)?)\b/i;

export function planLiveFlawSync({
  shopifyBody = "",
  liveNote = "",
  liveKnown = true,
  storedCondition = "",
  item = null,
} = {}) {
  const stored = String(storedCondition || "").trim();
  const known = liveKnown !== false;
  const note = known ? String(liveNote || "").trim() : stored;
  const condition = known && !note ? conditionAfterClearedExtra(stored) : note || stored;
  const itemForBuild = item && typeof item === "object" ? item : { name: "Painting" };
  // No note to weave: skip a full marketing finalize. Openings/size weaves would
  // rewrite the body and look like a missing flaw. Only a body that still shows
  // a customer flaw cue (cleared Traxia note) asks for a rewrite.
  if (!String(note || "").trim() && !String(condition || "").trim()) {
    const plain = stripListingDimJunk(shopifyBody);
    if (!FLAW_CUE_RE.test(plain)) {
      return { condition: "", prose: shopifyBody, flawMissing: false, bodyHtml: "" };
    }
    const prose = appendIntakeNotesToDescription(shopifyBody, "", "", itemForBuild);
    const flawMissing = listingBodyNeedsWrite(shopifyBody, prose);
    return { condition: "", prose, flawMissing, bodyHtml: flawMissing ? prose : "" };
  }
  const prose = appendIntakeNotesToDescription(shopifyBody, known ? note : "", condition, itemForBuild);
  const flawMissing = listingBodyNeedsWrite(shopifyBody, prose);
  return { condition, prose, flawMissing, bodyHtml: flawMissing ? prose : "" };
}

export function skuRemoteImageUrls(fieldData = {}) {
  const urls = [];
  const push = (value) => {
    const url = typeof value === "string" ? value : value?.url;
    const text = String(url || "").trim();
    if (text && !urls.includes(text)) urls.push(text);
  };
  push(fieldData["main-image"]);
  const more = fieldData["more-images"];
  if (Array.isArray(more)) more.forEach(push);
  return urls;
}

/** Keep only image URLs that already answered. A retry can pass a fresh URL list. */
export function skuFieldDataWithReadyImages(fieldData = {}, readyUrls = null, freshUrls = null) {
  const source = Array.isArray(freshUrls) ? freshUrls.map((url) => String(url || "").trim()).filter(Boolean) : skuRemoteImageUrls(fieldData);
  const ready = readyUrls == null ? source : source.filter((url) => readyUrls.includes(url));
  const next = { ...fieldData };
  delete next["main-image"];
  delete next["more-images"];
  if (!ready.length) return next;
  next["main-image"] = { url: ready[0] };
  if (ready.length > 1) next["more-images"] = ready.slice(1, 11).map((url) => ({ url }));
  return next;
}

export function shopifyCdnImageIsReady(status) {
  return Number(status) === 200;
}

async function fetchIntakeIdentity(shopifyProductId, sku, log) {
  const pid = String(shopifyProductId || "").trim();
  const skuVal = String(sku || "").trim();
  if (!pid && !skuVal) return { byProductId: null, bySku: null };
  try {
    const cookie = await ensureIntakeSessionCookie(log);
    const res = await intakeGet("/api/lost-and-found-intake/items/by-shopify-identity", {
      cookie,
      params: {
        ...(pid ? { shopifyProductId: pid } : {}),
        ...(skuVal ? { traxiaSku: skuVal } : {}),
      },
    });
    if (res.status >= 400) {
      log?.("warn", {
        event: "intake.lookup.by_identity.failed",
        shopifyProductId: pid,
        status: res.status,
      });
      return { byProductId: null, bySku: null };
    }
    return {
      byProductId: res.data?.byProductId || null,
      bySku: res.data?.bySku || null,
    };
  } catch (err) {
    log?.("warn", {
      event: "intake.lookup.by_identity.error",
      shopifyProductId: pid,
      message: err?.message || String(err),
    });
    return { byProductId: null, bySku: null };
  }
}

/**
 * Traxia name/SKU → Shopify (via Traxia e-com) → Intake feedback loop.
 * First find may use External Link or exact title. After that, product id
 * then SKU. Stamps Intake name + traxiaSku + shopifyProductId.
 */
const intakeMirrorRecent = new Map();

export function intakeMirrorFactHash(product) {
  const facts = extractShopifyListingFacts(product);
  const bodyHash = createHash("sha256").update(String(product?.body_html || "")).digest("hex").slice(0, 16);
  return JSON.stringify({
    id: String(product?.id || ""),
    title: normalizeFullItemName(product?.title),
    sku: extractShopifyVariantSku(product),
    price: extractShopifyVariantPrice(product),
    width: facts.width,
    depth: facts.depth,
    height: facts.height,
    weightLbs: facts.weightLbs,
    body: bodyHash,
    status: String(product?.status || "").toLowerCase(),
  });
}

export function intakeMirrorIsFresh(productId, hash, maxAgeMs = 15000) {
  const row = intakeMirrorRecent.get(String(productId));
  if (!row) return false;
  if (Date.now() - row.at > maxAgeMs) return false;
  return row.hash === hash;
}

function rememberIntakeMirror(productId, hash) {
  intakeMirrorRecent.set(String(productId), { hash, at: Date.now() });
}

const TRAILING_DIM_RE =
  /(?:^|[\s\-–—])(\d+(?:\.\d+)?(?:\s*['’′]\s*\d+(?:\.\d+)?)?\s*["”″]?(?:\s*[xX×]\s*\d+(?:\.\d+)?(?:\s*['’′]\s*\d+(?:\.\d+)?)?\s*["”″]?){1,2}\s*H?)\s*$/i;
const TEST_JUNK_RE = /\s+BERNIS\s+TEST\b.*?(?=\s*[-–—]\s*\d|\s*$)/i;
const DELETE_NOTE_RE = /\s*\(?\s*delete this part of name\b[^)]*\)?/gi;

function stripTestJunk(name) {
  return String(name || "")
    .replace(TEST_JUNK_RE, "")
    .replace(DELETE_NOTE_RE, "")
    .replace(/\s{2,}/g, " ")
    .trim();
}

function peelTrailingDims(name) {
  let text = String(name || "").trim();
  const found = [];
  for (let i = 0; i < 6; i += 1) {
    const match = text.match(TRAILING_DIM_RE);
    if (!match) break;
    const nxt = text.slice(0, match.index).replace(/[\s\-–—]+$/g, "").trim();
    if (nxt === text) break;
    found.push(match[1]);
    text = nxt;
  }
  found.reverse();
  return { base: text, suffixes: found };
}

function compactDimToken(token) {
  const raw = String(token || "").trim();
  if (!raw) return "";
  const trailingH = /h\s*$/i.test(raw);
  const body = raw.replace(/h\s*$/i, "");
  const parts = body.split(/\s*[xX×]\s*/).map((part) => {
    const match = String(part).match(/(\d+(?:\.\d+)?)/);
    if (!match) return "";
    const n = Number(match[1]);
    if (!Number.isFinite(n)) return match[1];
    if (Math.abs(n - Math.round(n)) < 1e-9) return String(Math.round(n));
    return String(Math.round(n * 100) / 100);
  }).filter(Boolean);
  if (parts.length < 2) return raw.replace(/\s+/g, "").replace(/×/g, "X").replace(/x/g, "X");
  return `${parts.join("X")}${trailingH ? "H" : ""}`;
}

function sizeTokenPattern() {
  return /\d+(?:\.\d+)?(?:\s*[xX×]\s*\d+(?:\.\d+)?){1,2}\s*H?/gi;
}

export function collapseDuplicateDimSuffix(name) {
  const text = String(name || "").trim();
  const matches = [...text.matchAll(sizeTokenPattern())];
  if (matches.length < 2) return text;
  const last = matches[matches.length - 1];
  const tail = text.slice(last.index + last[0].length);
  if (!/^(?:\s*[-–—]\s*AS\s*-?\s*IS)?\s*$/i.test(tail)) return text;
  const prefix = text.slice(0, last.index).replace(/[\s\-–—]+$/g, "");
  return `${prefix}${tail}`.replace(/[ \t]{2,}/g, " ").trim();
}

export function refreshTitleDimensions(title, dims = {}) {
  const num = (value) => {
    const n = Number(value);
    if (!Number.isFinite(n) || n <= 0) return null;
    if (Math.abs(n - Math.round(n)) < 1e-9) return String(Math.round(n));
    return String(Math.round(n * 100) / 100);
  };
  let text = collapseDuplicateDimSuffix(title);
  if (!text) return "";
  const w = num(dims.width ?? dims.dimWidth);
  const d = num(dims.depth ?? dims.dimDepth ?? dims.dimLength);
  const h = num(dims.height ?? dims.dimHeight);
  if (!(w && d && h)) return text;
  const matches = [...text.matchAll(sizeTokenPattern())];
  if (!matches.length) return text;
  const tokenMatch = matches[matches.length - 1];
  const token = tokenMatch[0];
  const numbers = token.match(/\d+(?:\.\d+)?/g) || [];
  const hasH = /H\s*$/i.test(token);
  let newNums = null;
  if (numbers.length >= 3) newNums = [w, d, h];
  else if (numbers.length === 2 && hasH) newNums = [w, h];
  else if (numbers.length === 2) newNums = [w, d];
  if (!newNums) return text;
  let index = 0;
  const newToken = token.replace(/\d+(?:\.\d+)?/g, (part) => (index < newNums.length ? newNums[index++] : part));
  return text.slice(0, tokenMatch.index) + newToken + text.slice(tokenMatch.index + token.length);
}

const SAVE_NAME_KEYS = new Set(["name"]);
const SAVE_BODY_KEYS = new Set([
  "description",
  "notes",
  "conditionIssues",
  "condition_issues",
  "conditionExcellent",
  "condition_excellent",
  "conditionVeryGood",
  "condition_very_good",
  "conditionGood",
  "condition_good",
]);
const SAVE_DIM_KEYS = new Set([
  "dimHeight", "dim_height", "dimWidth", "dim_width", "dimDepth", "dim_depth",
  "dimLength", "dim_length", "weightLbs", "weight_lbs",
]);
const SAVE_PRICE_KEYS = new Set(["retailPrice", "retail_price", "resalePrice", "resale_price", "price"]);
const SAVE_PHOTO_KEYS = new Set(["photos", "photoUrls", "images"]);
const SAVE_TAG_KEYS = new Set([
  "ecommerceTags", "ecommerce_tags", "merchType", "merch_type",
  "websiteCategory", "website_category", "category", "brand",
]);

export function saveSyncDelta(changes) {
  const keys = new Set(
    Object.keys(changes || {}).filter((key) => !String(key).startsWith("_"))
  );
  const fields = [];
  const take = (group, name) => {
    for (const key of group) {
      if (keys.has(key)) {
        fields.push(name);
        return;
      }
    }
  };
  take(SAVE_NAME_KEYS, "name");
  take(SAVE_BODY_KEYS, "body");
  take(SAVE_DIM_KEYS, "dims");
  take(SAVE_PRICE_KEYS, "price");
  take(SAVE_PHOTO_KEYS, "photos");
  take(SAVE_TAG_KEYS, "tags");
  return { sync: fields.length > 0, fields };
}

export function listingDeltaPayload(item = {}, fields = []) {
  const wanted = [];
  for (const field of fields || []) {
    if (!wanted.includes(field)) wanted.push(field);
  }
  if (!wanted.length) return null;
  const name = String(item.name || "");
  // Name / dims change → from-scratch body + ecommerce tags. Title sticks exactly
  // (never remangle W×D×H from Width/Depth/Height tags into e.g. 75X42X250).
  if (wanted.includes("name")) {
    if (!wanted.includes("body")) wanted.push("body");
    if (!wanted.includes("tags")) wanted.push("tags");
  }
  if (wanted.includes("dims")) {
    if (!wanted.includes("body")) wanted.push("body");
    if (!wanted.includes("tags")) wanted.push("tags");
  }
  const payload = {
    intakeItemId: item.id || null,
    shopifyProductId: item.shopifyProductId || item.shopify_product_id || null,
    allowOpenAi: false,
    syncFields: wanted.slice(),
  };
  const dims = {
    width: item.dimWidth ?? item.dim_width,
    depth: item.dimDepth ?? item.dim_depth ?? item.dimLength ?? item.dim_length,
    height: item.dimHeight ?? item.dim_height,
    weightLbs: item.weightLbs ?? item.weight_lbs,
  };
  if (wanted.includes("name")) {
    payload.name = name;
  }
  if (wanted.includes("dims")) payload.dims = dims;
  if (wanted.includes("body") || wanted.includes("dims")) {
    const notes = String(item.notes || "").trim();
    const issues = String(item.conditionIssues || item.condition_issues || "").trim();
    payload.notes = notes;
    payload.conditionIssues = issues;
    // From-scratch finalize only — never concatenate prior Shopify dual paste.
    payload.description = appendIntakeNotesToDescription(item.description || "", notes, issues, item);
    payload.replaceBody = true;
    payload.rewriteBody = true;
    if (!notes && !issues) payload.clearedNote = true;
  }
  if (wanted.includes("price")) {
    const price = item.retailPrice ?? item.retail_price ?? item.price;
    if (price != null && String(price).trim() !== "") {
      payload.price = price;
      payload.retailPrice = price;
    }
  }
  if (wanted.includes("tags")) {
    payload.ecommerceTags = item.ecommerceTags || item.ecommerce_tags || "";
  }
  if (wanted.includes("photos")) payload.syncPhotos = true;
  return payload;
}

export function planSaveSync(changes, item) {
  const delta = saveSyncDelta(changes);
  if (!delta.sync) return { sync: false, payload: null };
  return { sync: true, payload: listingDeltaPayload(item, delta.fields) };
}

export function shopifyProductFieldsFromPatch(patch = {}) {
  if (!patch || patch.nameOnly) {
    const out = {};
    if (patch?.title) out.title = String(patch.title);
    return out;
  }
  const out = {};
  if (patch.title) out.title = String(patch.title);
  if (patch.bodyHtml != null && String(patch.bodyHtml).trim()) out.body_html = patch.bodyHtml;
  if (Array.isArray(patch.images) && patch.images.length) out.images = patch.images;
  if (patch.tags) out.tags = patch.tags;
  if (patch.price != null && String(patch.price).trim() !== "") out.price = patch.price;
  return out;
}

function readSnapshot(value) {
  if (!value) return {};
  if (typeof value === "object") return value;
  try {
    const data = JSON.parse(String(value));
    return data && typeof data === "object" ? data : {};
  } catch (_err) {
    return {};
  }
}

export function shopifyWritebackDecision({
  snapshot,
  updatedAt,
  title,
  body,
  tags,
  price,
  intakeTitle = "",
} = {}) {
  const snap = snapshot && typeof snapshot === "object" ? snapshot : readSnapshot(snapshot);
  const incoming = {
    title: title != null && String(title).trim() ? snapshotHash(title) : "",
    body: body != null && String(body).trim() ? snapshotHash(body) : "",
    tags: tags != null && String(tags).trim() ? snapshotHash(Array.isArray(tags) ? tags.join(", ") : tags) : "",
    price: price != null && String(price).trim() ? snapshotHash(price) : "",
  };
  const intakeName = String(intakeTitle || "").trim();
  if (!snap || !Object.keys(snap).length) {
    // Legacy / first-link items: adopt Shopify so Traxia names reach Intake.
    // Clash rules apply only after a baseline snapshot exists.
    return { apply: true, reason: "bootstrap" };
  }
  const plainBody = String(body || "")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  const payloadAt = Date.parse(updatedAt || "");
  const snapAt = Date.parse(snap.at || "");
  const stale = Number.isFinite(payloadAt) && Number.isFinite(snapAt) && payloadAt <= snapAt;
  // A cleared Shopify description is a real edit even when the title, tags,
  // and price still match the snapshot, and even when the titles differ.
  if (!plainBody && snap.body && !stale) {
    return { apply: true, reason: "body_cleared", bodyOnly: true };
  }
  const keys = ["title", "body", "tags", "price"].filter((key) => incoming[key] && snap[key]);
  if (keys.includes("title") && keys.every((key) => incoming[key] === snap[key])) {
    return { apply: false, reason: "echo" };
  }
  if (snap.title && incoming.title) {
    const authority = decideSyncAuthority({
      snapshotHash: snap.title,
      intakeHash: snapshotHash(intakeName),
      shopifyHash: incoming.title,
    });
    if (
      authority.authority === "intake" &&
      ["clash_intake_wins", "intake_one_sided", "no_snapshot_intake_wins"].includes(authority.reason)
    ) {
      return {
        apply: false,
        reason: authority.reason,
        pushIntake: authority.reason === "clash_intake_wins",
      };
    }
  }
  if (stale) {
    return { apply: false, reason: "stale" };
  }
  return { apply: true, reason: "newer" };
}

export function convergeShopifyEvents(events = []) {
  let name = "";
  let snapshot = {};
  for (const event of events) {
    const decision = shopifyWritebackDecision({
      snapshot,
      updatedAt: event?.updatedAt || event?.updated_at,
      title: event?.title,
      body: event?.body,
      tags: event?.tags,
      price: event?.price,
      // Treat the last applied title as Intake's current name for clash checks.
      intakeTitle: name,
    });
    if (!decision.apply) continue;
    name = String(event?.title || "");
    snapshot = {
      ...snapshot,
      at: String(event?.updatedAt || event?.updated_at || ""),
      title: snapshotHash(name),
      body: snapshotHash(event?.body || ""),
      tags: snapshotHash(event?.tags || ""),
      price: snapshotHash(event?.price || ""),
    };
  }
  return name;
}

function dropStaleDeltaFields(payload, fields, snapshot, jobAt, snapAt) {
  if (!Number.isFinite(jobAt) || !Number.isFinite(snapAt) || jobAt >= snapAt) return fields;
  let kept = fields.slice();
  if (kept.includes("name") && snapshot.title && snapshot.title === snapshotHash(payload.name || "")) {
    delete payload.name;
    delete payload.nameOnly;
    kept = kept.filter((field) => field !== "name");
  }
  if (kept.includes("body") && snapshot.body && payload.description != null && snapshot.body === snapshotHash(payload.description)) {
    delete payload.description;
    delete payload.notes;
    delete payload.conditionIssues;
    kept = kept.filter((field) => field !== "body");
  }
  if (kept.includes("price") && snapshot.price) {
    const price = payload.price != null ? payload.price : payload.retailPrice;
    if (price != null && snapshot.price === snapshotHash(price)) {
      delete payload.price;
      delete payload.retailPrice;
      kept = kept.filter((field) => field !== "price");
    }
  }
  payload.syncFields = kept;
  if (kept.length === 1 && kept[0] === "name" && payload.name) {
    payload.nameOnly = true;
    delete payload.description;
    delete payload.notes;
    delete payload.conditionIssues;
    delete payload.dims;
    delete payload.price;
    delete payload.retailPrice;
  } else if (!kept.includes("name")) {
    delete payload.nameOnly;
  }
  return kept;
}

export function resolveQueuedListingJob(job = {}, item) {
  if (!item) return { action: "drop", reason: "missing_item", name: null, payload: null };
  const fields = Array.isArray(job.syncFields) ? job.syncFields : Array.isArray(job.fields) ? job.fields : null;
  const dims = {
    width: item.dimWidth ?? item.dim_width,
    depth: item.dimDepth ?? item.dim_depth ?? item.dimLength ?? item.dim_length,
    height: item.dimHeight ?? item.dim_height,
  };
  let payload;
  let name;
  if (fields) {
    payload = listingDeltaPayload(item, fields);
    if (!payload) return { action: "noop", reason: "no_changes", name: null, payload: null };
    name = payload.name || String(item.name || "");
  } else {
    name = job.__corrective ? collapseDuplicateDimSuffix(String(item.name || "")) : String(item.name || "");
    const notes = String(item.notes || "").trim();
    const issues = String(item.conditionIssues || item.condition_issues || "").trim();
    payload = {
      intakeItemId: item.id,
      shopifyProductId: item.shopifyProductId || item.shopify_product_id || job.shopifyProductId || null,
      name,
      // From-scratch only — discard dual Shopify / stacked Intake paste.
      description: appendIntakeNotesToDescription(item.description || "", notes, issues, item),
      notes,
      conditionIssues: issues,
      ecommerceTags: item.ecommerceTags || item.ecommerce_tags || "",
      dimWidth: dims.width,
      dimDepth: dims.depth,
      dimHeight: dims.height,
      retailPrice: item.retailPrice ?? item.retail_price,
      allowOpenAi: false,
      replaceBody: true,
      rewriteBody: true,
      queuedAt: job.queuedAt || job.queued_at || null,
    };
    if (!notes && !issues) payload.clearedNote = true;
  }
  if (job.__corrective && payload.name) payload.name = collapseDuplicateDimSuffix(payload.name);
  // Coalesce / corrective follow-ups always SET body from Intake finalize.
  if (job.__rebuildFromCurrent || job.__corrective || job.forceCleanBody || job.__forceCleanBody) {
    payload.replaceBody = true;
    payload.rewriteBody = true;
    payload.forceRefresh = true;
  }
  const snapshot = readSnapshot(item.shopifySyncSnapshot || item.shopify_sync_snapshot);
  const jobAt = Date.parse(job.queuedAt || job.queued_at || "");
  const snapAt = Date.parse(snapshot.at || "");
  if (fields) {
    const kept = dropStaleDeltaFields(payload, fields, snapshot, jobAt, snapAt);
    if (!kept.length) return { action: "noop", reason: "stale_fields", name: item.name, payload: null };
    name = payload.name || item.name;
  } else if (Number.isFinite(jobAt) && Number.isFinite(snapAt) && jobAt < snapAt && snapshot.title === snapshotHash(item.name || "")) {
    // From-scratch body restores / corrective rebuilds must still apply after coalesce.
    const forceBody =
      job.__rebuildFromCurrent ||
      job.__corrective ||
      job.forceCleanBody ||
      job.__forceCleanBody ||
      job.replaceBody ||
      job.rewriteBody ||
      job.forceRefresh;
    if (!forceBody) {
      return { action: "noop", reason: "stale_job", name: item.name, payload: null };
    }
  }
  return { action: "write", reason: "current", name, payload };
}

export function resolveQueuedImageJob(job = {}, item) {
  if (!item) return { action: "drop", reason: "missing_item", imageUrls: [] };
  const imageUrls = orderedIntakePhotoUrls(item);
  const snapshot = readSnapshot(item.shopifySyncSnapshot || item.shopify_sync_snapshot);
  const jobAt = Date.parse(job.queuedAt || job.queued_at || "");
  const snapAt = Date.parse(snapshot.at || "");
  if (
    Number.isFinite(jobAt) &&
    Number.isFinite(snapAt) &&
    jobAt < snapAt &&
    snapshot.images &&
    snapshot.images === snapshotHash(imageUrls.join("|"))
  ) {
    return { action: "noop", reason: "stale_job", imageUrls };
  }
  return { action: "write", reason: "current", imageUrls };
}

export function assessCleanProduct(state = {}) {
  const title = String(state.title || state.name || "");
  const violations = [];
  if (collapseDuplicateDimSuffix(title) !== title.trim()) violations.push("title_repeated_dims");
  const body = String(state.body || "");
  const plain = body.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
  // One minimum everywhere: product sentences only, not the pickup close or the size line.
  if (listingProseIsThin(body)) violations.push("body_thin");
  const identity = { name: title, brand: state.brand || "", category: state.category || "" };
  if (
    /AS\s*IS\s*:/i.test(body) ||
    descriptionHasStackedWear(plain) ||
    descriptionHasStackedProductProse(plain, identity)
  ) {
    violations.push("body_as_is_paste");
  }
  if (/\b(?:aeo|geo|seo|condition note)\s*:/i.test(body)) violations.push("body_label");
  const note = String(state.note || "");
  if (note.trim() && /\b(?:smallt|smalli|smalt)\b/i.test(body)) violations.push("body_raw_token");
  if ((body.match(/Dimensions\s*:/gi) || []).length > 1) violations.push("body_repeated_footer");
  if (/\bMeasuring\b/i.test(body) && /Dimensions\s*:/i.test(body)) violations.push("body_repeated_size");
  if (/\bthere is a\b/i.test(body) && !/\b(?:crack|scratch|chip|stain|tear|dent)\b/i.test(note)) {
    violations.push("body_unsupported_flaw");
  }
  const tags = Array.isArray(state.tags) ? state.tags : String(state.tags || "").split(/[,;\n]+/);
  if (tags.some((tag) => /^(?:used|active|draft|sold|archived|inactive|no-images)$/i.test(String(tag || "").trim()) || /^location\s*:/i.test(String(tag || "").trim()))) {
    violations.push("tags_junk");
  }
  const dims = state.dims || {};
  for (const [axis, label] of [["width", "Width"], ["depth", "Depth"], ["height", "Height"]]) {
    if (dims[axis] == null || dims[axis] === "") continue;
    const match = tags.join(" ").match(new RegExp(`\\b${label}\\s*:\\s*([0-9.]+)`, "i"));
    if (match && Math.abs(Number(match[1]) - Number(dims[axis])) > 0.001) {
      violations.push("tags_dims");
      break;
    }
  }
  if (state.intakePhotoCount && !state.imageCount) violations.push("images_missing");
  if (state.imageOrder && state.desiredOrder && state.imageOrder.join("|") !== state.desiredOrder.join("|")) {
    violations.push("images_order");
  }
  return { clean: violations.length === 0, violations };
}

export function planSettledCleanliness(state = {}, { corrective = false } = {}) {
  const assessment = assessCleanProduct(state);
  if (assessment.clean) return { action: "none", violations: [] };
  // Thin copy alone is not dirt. A rebuild reads the same Intake row and cannot
  // lengthen it, so no corrective pass is spent and no error is stamped. Pricing
  // Engine asks the model once per change for a thin item (plan_settled_cleanliness).
  if (assessment.violations.every((v) => v === "body_thin")) {
    return { action: "none", violations: [], thin: true };
  }
  const hasBodyPaste = assessment.violations.includes("body_as_is_paste");
  // body_as_is_paste / dual-paragraph garbage must always rebuild clean — never stop and leave it.
  const bodyOnlyFixable = assessment.violations.every((v) =>
    ["body_as_is_paste", "body_repeated_footer", "body_repeated_size", "body_thin"].includes(v)
  );
  if (corrective && !hasBodyPaste && !bodyOnlyFixable) {
    return {
      action: "stop",
      violations: assessment.violations,
      note: "Shopify sync error: listing still needs a cleanup after one rebuild",
    };
  }
  return {
    action: "rebuild",
    violations: assessment.violations,
    forceCleanBody: hasBodyPaste,
  };
}

export function intakeMirrorPostBody(product, { item = null } = {}) {
  const facts = extractShopifyListingFacts(product);
  const dims = {
    width: facts.width ?? positiveMeasure(item?.dimWidth ?? item?.dim_width),
    depth: facts.depth ?? positiveMeasure(item?.dimDepth ?? item?.dim_depth ?? item?.dimLength ?? item?.dim_length),
    height: facts.height ?? positiveMeasure(item?.dimHeight ?? item?.dim_height),
    weightLbs: facts.weightLbs ?? positiveMeasure(item?.weightLbs ?? item?.weight_lbs ?? item?.weight),
  };
  // Shopify/Traxia title sticks exactly — never remangle from Width/Depth/Height tags.
  const title = normalizeFullItemName(product?.title) || normalizeFullItemName(item?.name) || "";
  const sku = extractShopifyVariantSku(product);
  const price = extractShopifyVariantPrice(product);
  const body = {
    shopifyProductId: product?.id != null ? String(product.id) : null,
    traxiaSku: sku,
    sku,
    name: title || null,
    allowOpenAi: false,
  };
  if (price != null) {
    body.retailPrice = price;
    body.price = price;
  }
  if (facts.width) body.dimWidth = facts.width;
  if (facts.depth) body.dimDepth = facts.depth;
  if (facts.height) body.dimHeight = facts.height;
  if (facts.weightLbs) body.weightLbs = facts.weightLbs;
  const mirroredBody = product?.body_html != null ? String(product.body_html) : "";
  const plainMirror = mirroredBody.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
  const intakeDesc = item?.description || item?.ecommerceDescription || "";
  const bodyClass = classifyShopifyBodyForIntake(mirroredBody, intakeDesc);
  // Never mirror a stacked / AS IS polluted Shopify body into Intake.
  if (plainMirror && bodyClass.applyDescription) {
    body.bodyHtml = mirroredBody;
  }
  return { body, dims, title, bodyClass };
}

export function rebuildListingFromCurrentState({ description, notes, conditionIssues, item, dims, title } = {}) {
  const prose = appendIntakeNotesToDescription(description, notes, conditionIssues, item);
  // Item carries websiteCategory / units when mirror dims only have numbers.
  const body = composeShopifyBodyHtml(prose, dims, item);
  return {
    // Keep the written title; dims refresh the footer/tags, not the name token.
    title: normalizeFullItemName(title || item?.name || "") || "",
    prose,
    body,
    llmCalled: false,
  };
}

export async function syncWebhookFactsToIntake(product, { post, resolveItem } = {}) {
  const item = resolveItem ? await resolveItem(product) : null;
  if (!item?.id) return { ok: false, reason: "no_intake_match", llmCalled: false };
  const planned = intakeMirrorPostBody(product, { item });
  const writeback = shopifyWritebackDecision({
    snapshot: item.shopifySyncSnapshot || item.shopify_sync_snapshot,
    updatedAt: product?.updated_at || product?.updatedAt,
    title: planned.title,
    body: product?.body_html,
    tags: product?.tags,
    price: planned.body?.retailPrice,
    intakeTitle: item.name || item.title || "",
  });
  if (!writeback.apply) {
    return {
      ok: true,
      skipped: true,
      reason: writeback.reason,
      pushIntake: Boolean(writeback.pushIntake),
      llmCalled: false,
      url: null,
      body: null,
    };
  }
  const url = `${getIntakeApiBase()}/api/lost-and-found-intake/items/${item.id}/shopify-image-sync`;
  const posted = post ? await post(url, planned.body) : null;
  const rebuilt = rebuildListingFromCurrentState({
    description: item.description || "",
    notes: item.notes,
    conditionIssues: item.conditionIssues || item.condition_issues,
    item,
    dims: planned.dims,
    title: planned.title,
  });
  return { ok: true, url, body: planned.body, posted, rebuilt, llmCalled: false };
}

async function fetchUnlinkedByName(name, log) {
  const title = normalizeFullItemName(name);
  if (!title) return [];
  try {
    const cookie = await ensureIntakeSessionCookie(log);
    const res = await intakeGet("/api/lost-and-found-intake/items/unlinked-by-name", {
      cookie,
      params: { name: title },
    });
    if (res.status >= 400) return [];
    return Array.isArray(res.data?.matches) ? res.data.matches : [];
  } catch (err) {
    log?.("warn", { event: "intake.unlinked_by_name.error", message: err?.message || String(err) });
    return [];
  }
}

export async function markIntakeShopifyProductUnlinked(product, { log, reason = "deleted" } = {}) {
  const productId = String(product?.id || "").trim();
  if (!productId) return { ok: false, reason: "missing_product" };
  if (!intakeShopifyImagesConfigured()) return { ok: false, reason: "missing_intake_creds" };
  const identity = await fetchIntakeIdentity(productId, extractShopifyVariantSku(product), log);
  const item = identity.byProductId;
  if (!item?.id) return { ok: false, reason: "no_intake_match", shopifyProductId: productId };
  const plan = planShopifyProductGone(item, { shopifyProductId: productId, reason });
  const stamped = await stampIntakeShopifyImageSync(item.id, {
    status: "unlinked",
    shopifyProductId: plan.shopifyProductId,
    traxiaSku: plan.traxiaSku,
    shopifyLinkState: "unlinked",
    lastShopifyProductId: plan.lastShopifyProductId,
    historyLine: plan.history,
    detail: plan.history,
    log,
  });
  return { ok: Boolean(stamped.ok), action: "unlink", stamp: stamped, plan };
}

export async function fetchShopifyProductsByExactTitle(fullName, { log } = {}) {
  const title = normalizeFullItemName(fullName);
  const base = shopifyRestBase();
  if (!base || !title) return [];
  const url = `${base}/products.json?title=${encodeURIComponent(title)}&limit=10`;
  const res = await axios.get(url, {
    headers: shopifyHeaders(),
    timeout: 30000,
    validateStatus: () => true,
  });
  if (res.status >= 400) {
    log?.("warn", { event: "shopify.exact_title.failed", status: res.status });
    return [];
  }
  const products = Array.isArray(res.data?.products) ? res.data.products : [];
  return products.filter((product) => titlesMatchForRelink(product?.title, title));
}

const INTAKE_DIM_KEYS = [
  "dimWidth",
  "dim_width",
  "dimDepth",
  "dim_depth",
  "dimLength",
  "dim_length",
  "dimHeight",
  "dim_height",
  "weightLbs",
  "weight_lbs",
];

/** True when the Intake row we were given has dimension fields at all (a null value still counts). */
export function intakeRowCarriesDims(item) {
  if (!item || typeof item !== "object") return false;
  return INTAKE_DIM_KEYS.some((key) => Object.prototype.hasOwnProperty.call(item, key));
}

/**
 * The description a from-scratch rebuild writes for this Intake row: the
 * Intake description with Extra Information woven in. This calls the same
 * builder the listing-update write calls; no build logic lives here.
 */
export function intakeRowBuildProse(item = null) {
  if (!item || typeof item !== "object") return "";
  const base = String(item.description || item.ecommerceDescription || item.listingDescription || "");
  const notes = String((item.publicNotes != null ? item.publicNotes : item.notes) || "");
  const condition = String(item.conditionIssues || item.condition_issues || "");
  // No description and no note: Pricing Engine sends no description at all.
  // A note alone still builds its flaw sentence.
  if (!base.trim() && !notes.trim() && !condition.trim()) return "";
  // Same inputs the listing-update write passes into appendIntakeNotesToDescription
  // (category/units/dims), so the mirror echo check matches the published body.
  return appendIntakeNotesToDescription(
    base,
    notes,
    condition,
    listingBuildItemFromRow(item)
  );
}

/** Item shape shared by listing-update finalize and the mirror Intake-build compare. */
export function listingBuildItemFromRow(item = null, overrides = {}) {
  const src = item && typeof item === "object" ? item : {};
  const width = src.dimWidth ?? src.dim_width ?? overrides.dimWidth;
  const depth =
    src.dimDepth ?? src.dim_depth ?? src.dimLength ?? src.dim_length ?? overrides.dimDepth;
  const height = src.dimHeight ?? src.dim_height ?? overrides.dimHeight;
  const weightLbs = src.weightLbs ?? src.weight_lbs ?? overrides.weightLbs;
  const websiteCategory =
    src.websiteCategory ?? src.website_category ?? overrides.websiteCategory ?? "";
  const category = src.category ?? overrides.category;
  return {
    name: overrides.name ?? src.name ?? "",
    brand: overrides.brand ?? src.brand ?? "",
    merchType: normalizeMerchType(
      overrides.merchType ?? src.merchType ?? src.merch_type
    ),
    ecommerceTags: String(
      overrides.ecommerceTags ?? src.ecommerceTags ?? src.ecommerce_tags ?? ""
    ),
    websiteCategory,
    website_category: websiteCategory,
    category,
    dimWidth: width,
    dim_width: width,
    dimDepth: depth,
    dim_depth: depth,
    dimLength: depth,
    dim_length: depth,
    dimHeight: height,
    dim_height: height,
    weightLbs,
    weight_lbs: weightLbs,
    dimWidthUnit: src.dimWidthUnit ?? src.dim_width_unit ?? overrides.dimWidthUnit ?? "",
    dimDepthUnit:
      src.dimDepthUnit ??
      src.dim_depth_unit ??
      src.dimLengthUnit ??
      src.dim_length_unit ??
      overrides.dimDepthUnit ??
      "",
    dimHeightUnit: src.dimHeightUnit ?? src.dim_height_unit ?? overrides.dimHeightUnit ?? "",
    dims: {
      width,
      depth,
      height,
      weightLbs,
      websiteCategory,
      category,
      dimWidthUnit: src.dimWidthUnit ?? src.dim_width_unit ?? "",
      dimDepthUnit: src.dimDepthUnit ?? src.dim_depth_unit ?? src.dimLengthUnit ?? "",
      dimHeightUnit: src.dimHeightUnit ?? src.dim_height_unit ?? "",
    },
  };
}

/**
 * True when the Shopify description already is what a from-scratch rebuild
 * from this Intake row writes. An empty Shopify description is never that.
 * The Dimensions footer and the shipping line are not compared here; a size
 * change is its own reason for a rewrite.
 */
export function shopifyBodyIsIntakeBuild(shopifyBody, item = null) {
  const current = stripListingDimJunk(shopifyBody || "").replace(/\s+/g, " ").trim();
  if (!current) return false;
  const prose = intakeRowBuildProse(item);
  if (!String(prose || "").trim() || isDimsOnlyListingText(prose)) return false;
  return !listingBodyNeedsWrite(shopifyBody, prose);
}

/**
 * True when the row is known to have no description and no note of any kind.
 * A rebuild from such a row sends no description, so an empty Shopify
 * description stays empty. A row whose description field was not sent is unknown.
 */
export function intakeRowBuildsNothing(item = null) {
  if (!item || typeof item !== "object") return false;
  if (!Object.prototype.hasOwnProperty.call(item, "description")) return false;
  const filled = (value) => String(value ?? "").trim() !== "";
  return ![
    item.description,
    item.ecommerceDescription,
    item.listingDescription,
    item.notes,
    item.publicNotes,
    item.conditionIssues,
    item.condition_issues,
  ].some(filled);
}

export async function mirrorShopifyProductToIntake(product, { log } = {}) {
  if (!product?.id) return { ok: false, reason: "missing_product" };
  if (!intakeShopifyImagesConfigured()) {
    return { ok: false, reason: "missing_intake_creds" };
  }
  if (shopifyStatusUnlinks(product.status)) {
    return markIntakeShopifyProductUnlinked(product, { log, reason: String(product.status) });
  }
  const factHash = intakeMirrorFactHash(product);
  if (intakeMirrorIsFresh(product.id, factHash)) {
    return { ok: true, skipped: true, reason: "already_mirrored", shopifyProductId: String(product.id) };
  }

  const title = normalizeFullItemName(product.title);
  const sku = extractShopifyVariantSku(product);
  const shopifyProductId = String(product.id);
  const identity = await fetchIntakeIdentity(shopifyProductId, sku, log);
  let choice = selectIntakeItemForShopifyProduct({
    shopifyProductId,
    sku,
    byProductId: identity.byProductId,
    bySku: identity.bySku,
  });
  // Title is only the first find, and only when neither product id nor SKU is stored.
  if (!choice.item) {
    let byExternal = null;
    const externalId = extractIntakeItemIdFromShopifyProduct(product);
    if (externalId) {
      try {
        byExternal = await fetchIntakeItemById(externalId, { log });
      } catch (err) {
        log?.("warn", {
          event: "shopify.intake_mirror.lookup_failed",
          shopifyProductId,
          message: err?.message || String(err),
        });
      }
    }
    let byTitle = null;
    if (!byExternal?.id && title) {
      try {
        byTitle = await fetchIntakeItemByFullName(title, { log });
      } catch (err) {
        log?.("warn", {
          event: "shopify.intake_mirror.lookup_failed",
          shopifyProductId,
          message: err?.message || String(err),
        });
      }
    }
    choice = selectIntakeItemForShopifyProduct({
      shopifyProductId,
      sku,
      byProductId: identity.byProductId,
      bySku: identity.bySku,
      byExternalId: byExternal,
      byExactTitle: byTitle,
    });
  }
  if (!choice.item && title) {
    const unlinked = await fetchUnlinkedByName(title, log);
    const relink = planShopifyRelink({ shopifyProductId, title, sku, candidates: unlinked });
    if (relink.action === "ambiguous") {
      for (const matchId of relink.matchIds || []) {
        await stampIntakeShopifyImageSync(matchId, {
          shopifyLinkState: "ambiguous",
          historyLine: relink.history,
          detail: relink.history,
          log,
        });
      }
      return { ok: false, reason: "ambiguous_name", shopifyProductId, name: title || null };
    }
    if (relink.relink && relink.item?.id) {
      choice = { item: relink.item, via: "exact_name_relink", wake: true };
    }
  }
  const intakeItem = choice.item;
  const intakeItemId = intakeItem?.id ? String(intakeItem.id) : "";

  if (!intakeItemId) {
    log?.("info", {
      event: "shopify.intake_mirror.skip",
      shopifyProductId,
      title: title.slice(0, 120),
      reason: "no_intake_match",
    });
    return { ok: false, reason: "no_intake_match", shopifyProductId, name: title || null };
  }

  const variantPrice = extractShopifyVariantPrice(product);
  const listingFacts = extractShopifyListingFacts(product);
  const shopifyBody = product.body_html != null ? String(product.body_html) : "";
  const bodyClass = classifyShopifyBodyForIntake(
    shopifyBody,
    intakeItem?.description || intakeItem?.ecommerceDescription || ""
  );
  // Name/price stamps alone must not trigger listing-update → Shopify → webhook → stamp loop.
  // A customer flaw that is not in the Shopify body yet still needs one listing pass.
  const liveExtra = readLiveTraxiaExtra(product);
  const storedCondition = intakeItem?.conditionIssues || intakeItem?.condition_issues || "";
  const storedNotes = intakeItem?.notes || "";
  // Extra Information does not ride along in the Shopify description. When the
  // product or a tag carries the field, that value (including "") is the live note.
  // Otherwise the Intake note is the live source and the old condition is only
  // the stored copy of a previous Traxia note.
  const flawPlan = planLiveFlawSync({
    shopifyBody,
    liveNote: liveExtra.known ? liveExtra.text : storedNotes,
    liveKnown: liveExtra.known || !storedCondition,
    storedCondition,
    item: intakeItem,
  });
  const flawMissing = flawPlan.flawMissing || listingMissesCustomerFlaw(
    shopifyBody,
    liveExtra.known ? liveExtra.text : storedNotes,
    liveExtra.known ? flawPlan.condition : storedCondition
  );
  const planned = intakeMirrorPostBody(product, { item: intakeItem });
  const writeback = shopifyWritebackDecision({
    snapshot: intakeItem?.shopifySyncSnapshot || intakeItem?.shopify_sync_snapshot,
    updatedAt: product.updated_at || product.updatedAt,
    title: planned.title || title,
    body: shopifyBody,
    tags: product.tags,
    price: variantPrice,
    intakeTitle: intakeItem?.name || intakeItem?.title || "",
  });
  // "echo" is left to Pricing Engine. It holds the whole Intake row and the
  // record of what was last written, so it can tell an echo from a stamp that
  // also carries a new size, SKU, note or link. Older and clashing events are
  // still dropped here.
  if (!writeback.apply && writeback.reason !== "echo") {
    log?.("info", {
      event: "shopify.intake_mirror.skipped",
      shopifyProductId,
      intakeItemId,
      reason: writeback.reason,
      updatedAt: product.updated_at || product.updatedAt || null,
    });
    return {
      ok: true,
      skipped: true,
      reason: writeback.reason,
      intakeItemId,
      shopifyProductId,
      name: intakeItem?.name || null,
    };
  }
  const relinked = choice.via === "exact_name_relink" || choice.wake === true;
  // Real Shopify/Traxia name or dim change → one from-scratch body + tags rewrite.
  // Echo of our own listing-update matches Intake already → no resync (breaks RH bed loop).
  const intakeName = normalizeFullItemName(intakeItem?.name || "");
  const mirroredTitle = normalizeFullItemName(planned.title || title || "");
  const nameChanged = Boolean(mirroredTitle && intakeName && mirroredTitle !== intakeName);
  const bodyReason = Boolean(bodyClass.restoreListing) || flawMissing;
  const productShowsDims = ["width", "depth", "height", "weightLbs"].some(
    (key) => listingFacts[key] != null
  );
  // The identity lookup of an older Pricing Engine returns no dimension, tag
  // or brand fields. Read the row (without its history) only when a rewrite
  // is in question, instead of guessing from fields that were never sent.
  let listingRow = intakeItem;
  if (!intakeRowCarriesDims(intakeItem) && (bodyReason || productShowsDims)) {
    try {
      const full = await fetchIntakeItemById(intakeItemId, { log, lite: true });
      if (full?.id && String(full.id) === intakeItemId) listingRow = { ...intakeItem, ...full };
    } catch (err) {
      log?.("warn", {
        event: "shopify.intake_mirror.row_read_failed",
        shopifyProductId,
        intakeItemId,
        message: err?.message || String(err),
      });
    }
  }
  // A dimension is "changed" only against a value Intake actually reported.
  // A field that was not sent is unknown, never a change.
  const dimsKnown = intakeRowCarriesDims(listingRow);
  const dimAxes = [
    ["width", listingRow?.dimWidth ?? listingRow?.dim_width],
    ["depth", listingRow?.dimDepth ?? listingRow?.dim_depth ?? listingRow?.dimLength ?? listingRow?.dim_length],
    ["height", listingRow?.dimHeight ?? listingRow?.dim_height],
    ["weightLbs", listingRow?.weightLbs ?? listingRow?.weight_lbs],
  ];
  const dimsChanged =
    dimsKnown &&
    dimAxes.some(([key, current]) => {
      const next = listingFacts[key];
      if (next == null) return false;
      const prev = positiveMeasure(current);
      return prev == null || Math.abs(prev - next) > 0.001;
    });
  if (!dimsKnown && productShowsDims) {
    log?.("warn", {
      event: "shopify.intake_mirror.dims_unknown",
      shopifyProductId,
      intakeItemId,
    });
  }
  // A resync rewrites the Shopify description from the Intake row as it is
  // after this stamp (a cleared or silent Traxia note is applied by the stamp).
  // Extra Information is the current condition source: empty or non-condition
  // text replaces a prior flaw; when the description already matches that
  // rewrite, asking again only feeds webhook → stamp → listing-update → webhook.
  const noteCleared = Boolean(liveExtra.known && !liveExtra.text);
  const liveSilentNote = Boolean(
    liveExtra.known && liveExtra.text && isCustomerSilentNote(liveExtra.text)
  );
  // Stamp/rebuild when published condition content changes. Adding internal
  // text onto an existing flaw keeps the same buyer-facing clauses.
  const livePublishChanged = Boolean(
    liveExtra.known &&
      buyerFacingClauses(liveExtra.text || "", "").join("\n") !==
        buyerFacingClauses(storedNotes || "", storedCondition || "").join("\n")
  );
  const passConditionUpdate = noteCleared || liveSilentNote || livePublishChanged;
  const rowAfterStamp = passConditionUpdate
    ? {
        ...listingRow,
        conditionIssues: flawPlan.condition || "",
        condition_issues: flawPlan.condition || "",
        ...(liveSilentNote || noteCleared
          ? { notes: liveExtra.text || "", publicNotes: liveExtra.text || "" }
          : {}),
      }
    : listingRow;
  const bodyIsIntakeBuild = shopifyBodyIsIntakeBuild(shopifyBody, rowAfterStamp);
  const plainShopify = shopifyBody.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
  // An Intake row with no description and no note builds an empty description.
  // Shopify is empty too, so it already is the build. (With a note there is a
  // flaw sentence to write, and that still asks.)
  const emptyIsIntakeBuild = !plainShopify && intakeRowBuildsNothing(rowAfterStamp);
  const bodyIsBuild = bodyIsIntakeBuild || emptyIsIntakeBuild;
  const wantResync = (bodyReason && !bodyIsBuild) || nameChanged || dimsChanged;
  if (bodyReason && bodyIsBuild) {
    log?.("info", {
      event: "shopify.intake_mirror.resync_not_needed",
      shopifyProductId,
      intakeItemId,
      reason: bodyIsIntakeBuild ? "body_is_intake_build" : "nothing_to_build",
      restoreListing: Boolean(bodyClass.restoreListing),
      flawMissing,
    });
  }
  // Empty body restores from Intake finalize. Polluted dual/AS IS bodies must not overwrite Intake.
  const mayMirrorBody = Boolean(plainShopify) && bodyClass.applyDescription === true;
  // A description that already is the Intake build has nothing to tell Intake.
  // It goes along only when the stamp also carries a real change.
  const sendBody =
    mayMirrorBody &&
    !(bodyIsIntakeBuild && !nameChanged && !dimsChanged && !relinked && !passConditionUpdate);
  const stamped = await stampIntakeShopifyImageSync(intakeItemId, {
    // Mirror-only: do not force image sync status. A relink stays pending until the wake-up write finishes.
    status: relinked ? "pending" : null,
    shopifyProductId,
    traxiaSku: sku,
    name: writeback.bodyOnly ? null : planned.title || title || null,
    retailPrice: writeback.bodyOnly ? null : variantPrice,
    dimWidth: writeback.bodyOnly ? null : listingFacts.width,
    dimDepth: writeback.bodyOnly ? null : listingFacts.depth,
    dimHeight: writeback.bodyOnly ? null : listingFacts.height,
    weightLbs: writeback.bodyOnly ? null : listingFacts.weightLbs,
    shopifyLinkState: relinked ? "linked" : null,
    historyLine: relinked ? "Shopify product relinked" : null,
    shopifyUpdatedAt: product.updated_at || product.updatedAt || null,
    // Explicit null, so a body from an earlier coalesced stamp does not ride along.
    bodyHtml: sendBody ? shopifyBody : null,
    // Same function as the listing-update reply, so the two can be compared ("" = empty description).
    bodyFingerprint: shopifyBodyFingerprint(shopifyBody),
    bodyIsIntakeBuild,
    ...(passConditionUpdate ? { conditionIssues: flawPlan.condition || "" } : {}),
    resyncListing: wantResync || relinked,
    detail: title
      ? `Mirrored Shopify title/SKU/price/body → Intake (${shopifyProductId}; body=${bodyClass.kind})`
      : `Mirrored Shopify product id/SKU/price/body → Intake (${shopifyProductId}; body=${bodyClass.kind})`,
    log,
  });

  if (stamped.ok) rememberIntakeMirror(shopifyProductId, factHash);
  if (title) {
    titlePollHitCache.set(title.toLowerCase(), { product, at: Date.now() });
  }

  log?.("info", {
    event: stamped.ok ? "shopify.intake_mirror.ok" : "shopify.intake_mirror.failed",
    shopifyProductId,
    intakeItemId,
    traxiaSku: sku || null,
    retailPrice: variantPrice,
    name: title.slice(0, 120) || null,
    shopifyBodyKind: bodyClass.kind,
    restoreListing: wantResync,
    matchVia: choice.via,
    flawMissing,
  });
  return {
    ok: Boolean(stamped.ok),
    intakeItemId,
    shopifyProductId,
    traxiaSku: sku,
    retailPrice: variantPrice,
    name: title || null,
    shopifyBodyKind: bodyClass.kind,
    stamp: stamped,
  };
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
  const product = result.product || null;
  return stampIntakeShopifyImageSync(intakeItemId, {
    status,
    shopifyProductId: result.shopifyProductId || product?.id || null,
    traxiaSku: extractShopifyVariantSku(product),
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

/** Default Shopify title-search poll cadence (~2.5s). Override via SHOPIFY_TITLE_POLL(_INTERVAL)_MS. */
export function shopifyTitlePollIntervalMs(fallback = 2500) {
  const raw =
    process.env.SHOPIFY_TITLE_POLL_INTERVAL_MS ||
    process.env.SHOPIFY_TITLE_POLL_MS ||
    "";
  const n = parseInt(String(raw).trim() || String(fallback), 10);
  if (!Number.isFinite(n) || n < 500) return Math.max(500, Number(fallback) || 2500);
  return Math.min(30_000, n);
}

/** Backoff cap for title poll. Default matches interval so cadence stays ~2–3s. */
export function shopifyTitlePollMaxDelayMs(intervalMs = 2500) {
  const base = Math.max(500, Number(intervalMs) || shopifyTitlePollIntervalMs());
  const raw = process.env.SHOPIFY_TITLE_POLL_MAX_DELAY_MS || "";
  const n = parseInt(String(raw).trim() || String(base), 10);
  if (!Number.isFinite(n) || n < 500) return base;
  // Cap at 3s by default so title poll stays tight until first hit.
  const envCap = process.env.SHOPIFY_TITLE_POLL_MAX_DELAY_MS ? n : Math.min(n, 3000);
  return Math.max(base, Math.min(60_000, envCap));
}

export async function pollShopifyProductByFullName(
  fullName,
  { log, maxWaitMs = 120000, intervalMs, backoff = false } = {}
) {
  const title = normalizeFullItemName(fullName);
  const key = title.toLowerCase();
  if (!key) return null;

  const cached = titlePollHitCache.get(key);
  if (cached?.product?.id && Date.now() - cached.at < TITLE_HIT_CACHE_TTL_MS) {
    log?.("info", {
      event: "shopify.poll_by_name.cache_hit",
      title: title.slice(0, 120),
      shopifyProductId: cached.product.id,
    });
    return cached.product;
  }

  if (titlePollInflight.has(key)) {
    log?.("info", {
      event: "shopify.poll_by_name.join_inflight",
      title: title.slice(0, 120),
    });
    return titlePollInflight.get(key);
  }

  const run = (async () => {
    const started = Date.now();
    let attempt = 0;
    let delayMs = Math.max(500, Number(intervalMs) || shopifyTitlePollIntervalMs());
    const maxDelayMs = shopifyTitlePollMaxDelayMs(delayMs);
    while (Date.now() - started <= maxWaitMs) {
      attempt += 1;
      const hit = await fetchShopifyProductByFullName(title, { log });
      if (hit?.id) {
        titlePollHitCache.set(key, { product: hit, at: Date.now() });
        log?.("info", {
          event: "shopify.poll_by_name.hit",
          title: title.slice(0, 120),
          shopifyProductId: hit.id,
          attempt,
          waitedMs: Date.now() - started,
          delayMs,
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
      title: title.slice(0, 120),
      attempt,
      waitedMs: Date.now() - started,
    });
    return null;
  })().finally(() => {
    titlePollInflight.delete(key);
  });

  titlePollInflight.set(key, run);
  return run;
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
  // A draft product is quiet, not gone. Intake photos still upload to that id.
  if (draftBlocksImageUpload(product.status)) {
    return { ...empty, status: "skipped_disabled", reason: "draft_blocked" };
  }

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
    // Stale Traxia tag often survives after media lands — clear it whenever we see images.
    await stripNoImagesTag(product.id, { log });
    // Existing media: latest-wins gallery order (Intake push, Shopify adopt, or noop).
    try {
      const desired = normalizeCallerImageUrls(imageUrlsHint);
      const reconciled = await reconcileExistingGalleryOrder(product, {
        intakeItemId: intakeItemId || null,
        imageUrlsHint: desired.length ? desired : null,
        log,
      });
      const skipped = {
        ...empty,
        status: "skipped_has_images",
        intakeItemId: reconciled?.intakeItemId || intakeItemId || null,
        shopifyProductId: product.id,
        reordered: Boolean(reconciled?.reordered),
        moved: reconciled?.moved || 0,
        uploaded: reconciled?.uploaded || 0,
        imageOrderAction: reconciled?.action || null,
      };
      if (skipped.intakeItemId && !reconciled?.stamped) {
        await stampIntakeShopifyImageSync(skipped.intakeItemId, {
          status: "synced",
          shopifyProductId: product.id,
          detail: reconciled?.detail || "Shopify already had display images",
          log,
        });
      }
      return skipped;
    } catch (err) {
      log?.("warn", {
        event: "shopify.intake_image.existing_gallery_reorder_failed",
        shopifyProductId: product.id,
        message: err?.message || String(err),
      });
      const skipped = {
        ...empty,
        status: "skipped_has_images",
        intakeItemId: intakeItemId || null,
        shopifyProductId: product.id,
      };
      if (intakeItemId) await stampFromImageGateResult(skipped, { log });
      return skipped;
    }
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
      await stripNoImagesTag(product.id, { log });
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
  if (!intakePhotosShouldUpload(product, urls.length)) {
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
        const image = await uploadIntakeUrlToShopifyProduct(product.id, src, {
          cookie,
          position: i + 1,
          log,
        });
        if (image?.src) createdSrcs.push(image.src);
        uploaded += 1;
      } catch (oneErr) {
        log?.("warn", {
          event: "shopify.intake_image.upload_one_failed",
          shopifyProductId: product.id,
          index: i,
          message: oneErr?.message || String(oneErr),
        });
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
  await stripNoImagesTag(product.id, { log });
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
    intervalMs,
    skipListingUpdate = false,
  } = {}
) {
  const name = normalizeFullItemName(fullName);
  if (!name) {
    return { ok: false, reason: "missing_name", product: null };
  }
  const pollEveryMs = Math.max(500, Number(intervalMs) || shopifyTitlePollIntervalMs());
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

  // Prefer already-stamped Shopify product id — never keep title-polling once known.
  let intakeItem = null;
  let knownPid = null;
  if (intakeItemId) {
    try {
      intakeItem = await fetchIntakeItemById(intakeItemId, { log });
      knownPid = String(
        intakeItem?.shopifyProductId || intakeItem?.shopify_product_id || ""
      ).trim() || null;
    } catch (_err) {
      intakeItem = null;
    }
  }

  let shopifyProduct = null;
  if (knownPid) {
    shopifyProduct = await fetchShopifyProductById(knownPid, { log });
    if (shopifyProduct?.id) {
      log?.("info", {
        event: "shopify.ensure_by_name.used_stamped_id",
        intakeItemId,
        shopifyProductId: shopifyProduct.id,
      });
    }
  }

  if (!shopifyProduct?.id) {
    if (intakeItemId) {
      await stampIntakeShopifyImageSync(intakeItemId, {
        status: "pending",
        shopifyProductId: knownPid,
        detail: `Waiting for Shopify product (poll ~${Math.round(pollEveryMs / 1000)}s) then Intake→Shopify images`,
        log,
      });
    }
    // Steady ~5s cadence until first hit — then cache/inflight stop further polls.
    shopifyProduct = await pollShopifyProductByFullName(name, {
      log,
      maxWaitMs,
      intervalMs: pollEveryMs,
      backoff: false,
    });
  }

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

  // Stamp product id (+ variant SKU) immediately on first find so photo-added uses id not title poll.
  const foundSku = extractShopifyVariantSku(shopifyProduct);
  if (intakeItemId) {
    await stampIntakeShopifyImageSync(intakeItemId, {
      status: "pending",
      shopifyProductId: shopifyProduct.id,
      traxiaSku: foundSku,
      detail: `Shopify product found (${shopifyProduct.id}); attaching Intake images`,
      log,
    });
  }

  const result = await ensureShopifyProductHasIntakeImages(shopifyProduct, {
    log,
    intakeItemId,
    imageUrls,
  });
  const resolvedIntakeId = result.intakeItemId || intakeItemId || null;
  // After images: push Intake listing prose + ONE dims footer (never dims-only).
  // Online sync may skip listing when PE already queued one — but only if Shopify
  // already has real body copy. Otherwise PE's early listing-update often deferred
  // (product_not_found) and skip would leave Draft products with empty Description.
  let listingIntakeId = resolvedIntakeId;
  const shopifyBodyThin =
    listingProseIsThin(shopifyProduct.body_html || "") ||
    isInternalOpsListingNote(shopifyProduct.body_html || "") ||
    isDimsOnlyListingText(shopifyProduct.body_html || "");
  if (skipListingUpdate && !shopifyBodyThin) {
    result.listingUpdate = { ok: true, status: "skipped_by_caller" };
  } else try {
    if (!intakeItem && listingIntakeId) {
      intakeItem = await fetchIntakeItemById(listingIntakeId, { log });
    }
    if (!intakeItem) {
      intakeItem = await fetchIntakeItemByFullName(name, { log });
      if (intakeItem?.id) listingIntakeId = String(intakeItem.id);
    }
    if (
      (result.status === "uploaded" || result.status === "skipped_has_images") &&
      (intakeItem || listingIntakeId)
    ) {
      if (intakeItem && listingIntakeId && String(intakeItem.id) !== String(listingIntakeId)) {
        intakeItem = await fetchIntakeItemById(listingIntakeId, { log });
      }
      if (skipListingUpdate && shopifyBodyThin) {
        log?.("info", {
          event: "shopify.ensure_by_name.listing_despite_skip",
          intakeItemId: listingIntakeId || resolvedIntakeId,
          shopifyProductId: shopifyProduct.id,
          reason: "shopify_body_thin_or_empty",
        });
      }
      const intakeDesc =
        (intakeItem &&
          (intakeItem.description ||
            intakeItem.ecommerceDescription ||
            intakeItem.listingDescription)) ||
        "";
      // Push existing Helper prose when present; only OpenAI-refresh if Intake is thin too.
      const needForceRefresh =
        Boolean(shopifyBodyThin) && listingProseIsThin(intakeDesc);
      const listing = await handleIntakeListingUpdate(
        {
          intakeItemId: listingIntakeId || (intakeItem?.id ? String(intakeItem.id) : null),
          shopifyProductId: String(shopifyProduct.id),
          name: (intakeItem && intakeItem.name) || name,
          description: intakeDesc,
          notes:
            (intakeItem &&
              (intakeItem.notes || intakeItem.conditionIssues || "")) ||
            "",
          ecommerceTags:
            (intakeItem && (intakeItem.ecommerceTags || intakeItem.tags)) || "",
          merchType:
            (intakeItem && (intakeItem.merchType || intakeItem.vertical)) ||
            "furniture",
          websiteCategory: (intakeItem && intakeItem.websiteCategory) || "",
          category: (intakeItem && intakeItem.category) || "",
          brand: (intakeItem && intakeItem.brand) || "",
          seoTitle:
            (intakeItem && (intakeItem.seoTitle || intakeItem.seo_title)) || undefined,
          seoDescription:
            (intakeItem &&
              (intakeItem.seoDescription || intakeItem.seo_description)) ||
            undefined,
          aeoDescription:
            (intakeItem &&
              (intakeItem.aeoDescription || intakeItem.aeo_description)) ||
            undefined,
          geoDescription:
            (intakeItem &&
              (intakeItem.geoDescription || intakeItem.geo_description)) ||
            undefined,
          forceRefresh: needForceRefresh,
          dims: intakeItem
            ? {
                width: intakeItem.dimWidth ?? intakeItem.width,
                depth:
                  intakeItem.dimDepth ??
                  intakeItem.dimLength ??
                  intakeItem.depth,
                height: intakeItem.dimHeight ?? intakeItem.height,
                weightLbs: intakeItem.weightLbs ?? intakeItem.weight,
              }
            : undefined,
        },
        { log }
      );
      result.listingUpdate = listing;
    }
  } catch (err) {
    log?.("warn", {
      event: "shopify.ensure_by_name.listing_update_failed",
      intakeItemId: listingIntakeId || resolvedIntakeId,
      message: err?.message || String(err),
    });
  }
  return {
    ok: result.status === "uploaded" || result.status === "skipped_has_images",
    ...result,
    name,
    shopifyProductId: shopifyProduct.id,
    intakeItemId: resolvedIntakeId,
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

  // Persist id (+ SKU) as soon as product is resolved so later photos skip title poll.
  const photoSku = extractShopifyVariantSku(product);
  await stampIntakeShopifyImageSync(id, {
    status: "pending",
    shopifyProductId: product.id,
    traxiaSku: photoSku,
    photoId,
    detail: "Shopify product resolved; syncing Intake photo",
    log,
  });

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

  // Product already has images — reconcile full Intake order (append + reorder)
  // so new photos never scramble primary/order. Do NOT short-circuit on existing media.
  const desired = orderedIntakePhotoUrls(item);
  if (!desired.length) {
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

  const reordered = await handleIntakeImagesReorder(
    {
      intakeItemId: id,
      shopifyProductId: String(product.id),
      name: fullName || product.title,
      imageUrls: desired,
    },
    { log }
  );

  // Listing copy in parallel with image reconcile — never leave dims-only body.
  try {
    await handleIntakeListingUpdate(
      {
        intakeItemId: id,
        shopifyProductId: String(product.id),
        name: fullName || item.name || product.title,
        description:
          item.description ||
          item.ecommerceDescription ||
          item.listingDescription ||
          "",
        notes: item.notes || item.conditionIssues || "",
        ecommerceTags: item.ecommerceTags || item.tags || "",
        merchType: item.merchType || item.vertical || "furniture",
        websiteCategory: item.websiteCategory || "",
        category: item.category || "",
        brand: item.brand || "",
        dims: {
          width: item.dimWidth ?? item.width,
          depth: item.dimDepth ?? item.dimLength ?? item.depth,
          height: item.dimHeight ?? item.height,
          weightLbs: item.weightLbs ?? item.weight,
        },
      },
      { log }
    );
  } catch (err) {
    log?.("warn", {
      event: "shopify.intake_image.photo_added_listing_failed",
      shopifyProductId: product.id,
      intakeItemId: id,
      message: err?.message || String(err),
    });
  }

  await stripNoImagesTag(product.id, { log });
  log?.("info", {
    event: "shopify.intake_image.photo_added_reconciled",
    shopifyProductId: product.id,
    intakeItemId: id,
    photoId,
    status: reordered?.status,
    uploaded: reordered?.uploaded,
    moved: reordered?.moved,
  });
  return {
    ok: Boolean(reordered?.ok),
    status: reordered?.status || "uploaded",
    uploaded: reordered?.uploaded || 0,
    moved: reordered?.moved || 0,
    intakeItemId: id,
    shopifyProductId: product.id,
    name: fullName || product.title,
    listingUpdate: true,
  };
}

/** 8x8 average-hash bits for fuzzy image matching (Intake ↔ Shopify CDN). */
export async function imageAverageHash(buffer) {
  const raw = await sharp(buffer)
    .resize(8, 8, { fit: "fill" })
    .grayscale()
    .raw()
    .toBuffer();
  let sum = 0;
  for (let i = 0; i < raw.length; i++) sum += raw[i];
  const avg = sum / Math.max(1, raw.length);
  let bits = 0n;
  for (let i = 0; i < raw.length; i++) {
    if (raw[i] >= avg) bits |= 1n << BigInt(i);
  }
  return bits;
}

export function hammingDistance64(a, b) {
  let x = BigInt(a) ^ BigInt(b);
  let n = 0;
  while (x) {
    n += Number(x & 1n);
    x >>= 1n;
  }
  return n;
}

async function updateShopifyImagePosition(productId, imageId, position) {
  const base = shopifyRestBase();
  if (!base) throw new Error("SHOPIFY_STORE missing");
  const res = await axios.put(
    `${base}/products/${productId}/images/${imageId}.json`,
    { image: { id: Number(imageId), position: Number(position) } },
    { headers: shopifyHeaders(), timeout: 30000, validateStatus: () => true }
  );
  if (res.status >= 400) {
    const msg = formatShopifyErrorBody(res.data?.errors ?? res.data?.error ?? res.data);
    throw new Error(`Shopify image position HTTP ${res.status}: ${msg || res.statusText || "unknown"}`);
  }
  return res.data?.image || null;
}

async function deleteShopifyProductImage(productId, imageId, { log } = {}) {
  const base = shopifyRestBase();
  if (!base) throw new Error("SHOPIFY_STORE missing");
  const res = await axios.delete(
    `${base}/products/${productId}/images/${imageId}.json`,
    { headers: shopifyHeaders(), timeout: 30000, validateStatus: () => true }
  );
  if (res.status >= 400) {
    const msg = formatShopifyErrorBody(res.data?.errors ?? res.data?.error ?? res.data);
    throw new Error(`Shopify image delete HTTP ${res.status}: ${msg || res.statusText || "unknown"}`);
  }
  log?.("info", {
    event: "shopify.intake_image.deleted",
    shopifyProductId: productId,
    imageId,
  });
  return true;
}

function mediaNumericId(gid) {
  const match = String(gid || "").match(/(\d+)\s*$/);
  return match ? match[1] : "";
}

async function shopifyAdminGraphql(query, variables) {
  const store = String(process.env.SHOPIFY_STORE || "").trim();
  const token = String(process.env.SHOPIFY_ACCESS_TOKEN || "").trim();
  if (!store || !token) throw new Error("SHOPIFY_STORE missing");
  const res = await axios.post(
    `https://${store}.myshopify.com/admin/api/2024-01/graphql.json`,
    { query, variables },
    { headers: shopifyHeaders(), timeout: 30000, validateStatus: () => true }
  );
  if (res.status >= 400) {
    throw new Error(`Shopify GraphQL HTTP ${res.status}`);
  }
  const errs = res.data?.errors;
  if (Array.isArray(errs) && errs.length) {
    throw new Error(errs.map((e) => e.message || "graphql error").join("; "));
  }
  return res.data?.data || {};
}

async function fetchShopifyMediaImages(productId) {
  const data = await shopifyAdminGraphql(
    `query ProductMedia($id: ID!) {
      product(id: $id) {
        media(first: 50) {
          nodes {
            id
            ... on MediaImage { image { url } }
          }
        }
      }
    }`,
    { id: `gid://shopify/Product/${productId}` }
  );
  return (data?.product?.media?.nodes || [])
    .filter((node) => node?.id && node?.image?.url)
    .map((node) => ({ id: String(node.id), src: node.image.url }));
}

/**
 * Moves that productReorderMedia applies in order (remove, then insert at newPosition).
 * Desired ids that are not already on the product are ignored. Unmatched Shopify
 * images stay after the reordered prefix — this never deletes or uploads.
 */
export function planShopifyMediaMoves(currentIds, desiredIds) {
  const current = [];
  const seenCur = new Set();
  for (const raw of currentIds || []) {
    const id = String(raw || "");
    if (!id || seenCur.has(id)) continue;
    seenCur.add(id);
    current.push(id);
  }
  const wanted = [];
  const seenWant = new Set();
  for (const raw of desiredIds || []) {
    const id = String(raw || "");
    if (!id || seenWant.has(id) || !seenCur.has(id)) continue;
    seenWant.add(id);
    wanted.push(id);
  }
  const finalOrder = wanted.concat(current.filter((id) => !seenWant.has(id)));
  const order = current.slice();
  const moves = [];
  for (let target = 0; target < finalOrder.length; target += 1) {
    const id = finalOrder[target];
    const from = order.indexOf(id);
    if (from < 0 || from === target) continue;
    order.splice(from, 1);
    order.splice(target, 0, id);
    moves.push({ id, newPosition: String(target) });
  }
  return { moves, order };
}

/** A second pass with the same order has nothing to write. */
export function shopifyImageReorderNeedsWrite(currentIds, desiredIds) {
  return planShopifyMediaMoves(currentIds, desiredIds).moves.length > 0;
}

/**
 * Latest-wins gallery order. Clash or missing snapshot → Intake order stands.
 * intakeOrderHash / shopifyImpliedIntakeHash are hashes of the same identity
 * space (usually Intake photo ids in display order).
 */
export function decideSyncAuthority({ snapshotHash = "", intakeHash = "", shopifyHash = "" } = {}) {
  const snap = String(snapshotHash || "").trim();
  const intake = String(intakeHash || "").trim();
  const shopify = String(shopifyHash || "").trim();
  if (intake && shopify && intake === shopify) {
    return {
      authority: "noop",
      reason: !snap || intake === snap ? "aligned" : "aligned_update_snapshot",
    };
  }
  if (!snap) return { authority: "intake", reason: "no_snapshot_intake_wins" };
  const intakeChanged = Boolean(intake) && intake !== snap;
  const shopifyChanged = Boolean(shopify) && shopify !== snap;
  if (shopifyChanged && !intakeChanged) {
    return { authority: "shopify", reason: "shopify_one_sided" };
  }
  if (intakeChanged && !shopifyChanged) {
    return { authority: "intake", reason: "intake_one_sided" };
  }
  if (intakeChanged && shopifyChanged) {
    return { authority: "intake", reason: "clash_intake_wins" };
  }
  return { authority: "noop", reason: "unchanged" };
}

export function planImageOrderSync({
  snapshotImagesHash = "",
  intakeOrderHash = "",
  shopifyImpliedIntakeHash = "",
} = {}) {
  const decision = decideSyncAuthority({
    snapshotHash: snapshotImagesHash,
    intakeHash: intakeOrderHash,
    shopifyHash: shopifyImpliedIntakeHash,
  });
  if (decision.authority === "shopify") {
    return { action: "adopt_shopify", reason: decision.reason };
  }
  if (decision.authority === "intake") {
    return { action: "push_intake", reason: decision.reason };
  }
  return { action: "noop", reason: decision.reason };
}

/**
 * Reorder Intake photo ids to match Shopify media positions via pairing indexes.
 * pairs[i] = Shopify media index for Intake photo i (null = unmatched, stays after).
 */
export function intakePhotoIdsFromShopifyOrder(intakePhotoIds = [], pairs = [], shopifyPositions = []) {
  const ids = (Array.isArray(intakePhotoIds) ? intakePhotoIds : []).map((id) => String(id || ""));
  const scored = [];
  const unmatched = [];
  for (let i = 0; i < ids.length; i += 1) {
    const mediaIdx = Array.isArray(pairs) ? pairs[i] : null;
    if (mediaIdx == null || mediaIdx < 0) {
      unmatched.push(ids[i]);
      continue;
    }
    const position = Number(
      Array.isArray(shopifyPositions) && shopifyPositions[mediaIdx] != null
        ? shopifyPositions[mediaIdx]
        : mediaIdx
    );
    scored.push({ id: ids[i], position: Number.isFinite(position) ? position : mediaIdx });
  }
  scored.sort((a, b) => a.position - b.position || a.id.localeCompare(b.id));
  return scored.map((row) => row.id).concat(unmatched.filter(Boolean));
}

/** Shopify image position is already the Intake position. Do not PUT. */
export function imagePositionNeedsWrite(image, position) {
  const current = Number(image?.position);
  const want = Number(position);
  return !(Number.isFinite(current) && Number.isFinite(want) && current === want);
}

/** Draft, active, or missing status never blocks an Intake photo upload. */
export function draftBlocksImageUpload(_status) {
  return false;
}

export function snapshotHash(value) {
  const text = String(value || "").replace(/\s+/g, " ").trim().toLowerCase();
  return createHash("sha256").update(text).digest("hex").slice(0, 16);
}

const BODY_ENTITIES = { amp: "&", lt: "<", gt: ">", quot: '"', apos: "'", nbsp: " " };

/** What a shopper reads in a Shopify description: no tags, entities decoded, whitespace collapsed. */
export function shopifyBodyPlainText(html) {
  return String(html || "")
    .replace(/<[^>]*>/g, " ")
    .replace(/&(#x[0-9a-f]+|#\d+|[a-z]+);/gi, (match, code) => {
      const key = String(code).toLowerCase();
      if (key.startsWith("#")) {
        const point = key.startsWith("#x") ? parseInt(key.slice(2), 16) : parseInt(key.slice(1), 10);
        if (!Number.isFinite(point) || point <= 0 || point > 0x10ffff) return " ";
        return String.fromCodePoint(point);
      }
      return Object.prototype.hasOwnProperty.call(BODY_ENTITIES, key) ? BODY_ENTITIES[key] : match;
    })
    .replace(/[\s\u00a0]+/g, " ")
    .trim();
}

/**
 * Fingerprint of the description text. The listing-update reply and the
 * webhook mirror both use this one function, so comparing them does not depend
 * on Shopify handing the HTML back byte for byte. "" means an empty description.
 */
export function shopifyBodyFingerprint(html) {
  const text = shopifyBodyPlainText(html);
  return text ? snapshotHash(text) : "";
}

/** Last write stored on the Intake item. Later events compare this locally. */
export function buildSyncSnapshot({
  intakeItemId = "",
  shopifyProductId = "",
  sku = "",
  body = "",
  title = "",
  tags = "",
  price = "",
  imageOrder = [],
  publishStatus = "",
  at = "",
} = {}) {
  const images = Array.isArray(imageOrder) ? imageOrder : [];
  return {
    v: 1,
    item: String(intakeItemId || ""),
    pid: String(shopifyProductId || ""),
    sku: String(sku || ""),
    body: snapshotHash(body),
    title: snapshotHash(title),
    tags: snapshotHash(Array.isArray(tags) ? tags.join(", ") : tags),
    price: snapshotHash(price),
    images: snapshotHash(images.join("|")),
    pub: String(publishStatus || "").toLowerCase(),
    at: at || new Date().toISOString(),
  };
}

export function shopifyWriteNeeded({ stored, desired, explicit = false } = {}) {
  if (explicit) {
    return {
      write: true,
      reason: "intake_wakeup",
      blockedByDraft: false,
      blockedByAlreadySynced: false,
      shopifyGet: false,
    };
  }
  if (!stored) return { write: true, reason: "no_snapshot", shopifyGet: false };
  const next = buildSyncSnapshot(desired || {});
  if (desired && desired.imageOrder == null) next.images = stored.images || "";
  if (desired && desired.publishStatus == null) next.pub = stored.pub || "";
  for (const key of ["pid", "body", "title", "tags", "price", "images", "pub"]) {
    if (String(next[key] || "") !== String(stored[key] || "")) {
      return { write: true, reason: `${key}_changed`, shopifyGet: false };
    }
  }
  return { write: false, reason: "snapshot_unchanged", shopifyGet: false };
}

/** Traxia e-commerce off: draft the same product and keep the Intake lock. */
export function planEcommerceOff({ shopifyProductId, sku, intakeItemId, body }) {
  return {
    intakeFirst: true,
    shopifyProductId: String(shopifyProductId || ""),
    sku: String(sku || ""),
    intakeItemId: String(intakeItemId || ""),
    clearLock: false,
    keepPhotos: true,
    keepIntakeData: true,
    shopifyStatus: "draft",
    publish: false,
    body: String(body || ""),
  };
}

/** A later Intake action republishes and rewrites that same product id. */
export function planIntakeWakeup({
  shopifyProductId,
  sku,
  intakeItemId,
  shopifyStatus = "draft",
  alreadySynced = true,
} = {}) {
  return {
    intakeFirst: true,
    shopifyProductId: String(shopifyProductId || ""),
    sku: String(sku || ""),
    intakeItemId: String(intakeItemId || ""),
    previousStatus: String(shopifyStatus || ""),
    alreadySynced: Boolean(alreadySynced),
    shopifyStatus: "active",
    writeImages: true,
    writeBody: true,
    writeTitle: true,
    writeTags: true,
    blockedByDraft: false,
    blockedByAlreadySynced: false,
    sameProduct: true,
    clearLock: false,
  };
}

/**
 * Greedy unique pairing of Intake hashes to Shopify media hashes.
 * Returns media indexes aligned to the Intake list (null = no match).
 * fullSet (same count, every hash present) pairs even a loose distance so an
 * existing gallery is reordered instead of uploaded again.
 */
export function pairIntakePhotosToShopifyMedia(
  desiredHashes,
  mediaHashes,
  { fullSet = false, maxHamming = 12 } = {}
) {
  const desired = Array.isArray(desiredHashes) ? desiredHashes : [];
  const media = Array.isArray(mediaHashes) ? mediaHashes : [];
  const pairs = new Array(desired.length).fill(null);
  const edges = [];
  for (let i = 0; i < desired.length; i += 1) {
    if (desired[i] == null) continue;
    for (let j = 0; j < media.length; j += 1) {
      if (media[j] == null) continue;
      edges.push({
        i,
        j,
        dist: hammingDistance64(desired[i], media[j]),
      });
    }
  }
  edges.sort((a, b) => a.dist - b.dist || a.i - b.i || a.j - b.j);
  const usedI = new Set();
  const usedJ = new Set();
  for (const edge of edges) {
    if (usedI.has(edge.i) || usedJ.has(edge.j)) continue;
    if (!fullSet && edge.dist > maxHamming) continue;
    usedI.add(edge.i);
    usedJ.add(edge.j);
    pairs[edge.i] = edge.j;
  }
  return pairs;
}

/**
 * Intake order → Shopify media ids.
 * Hashes are average hashes of the prepared Intake file and the Shopify CDN file.
 * A missing hash is a failed download or a fingerprint that could not be computed.
 * When the counts match and exactly one photo could not be paired, the unused
 * Shopify image is that photo. Nothing is uploaded or deleted.
 */
export function pairReorderToMedia({
  desiredHashes = [],
  mediaHashes = [],
  mediaIds = [],
  maxHamming = 12,
} = {}) {
  const desired = Array.isArray(desiredHashes) ? desiredHashes.slice() : [];
  const media = Array.isArray(mediaHashes) ? mediaHashes.slice() : [];
  const alignedIds = Array.isArray(mediaIds) ? mediaIds.slice() : [];
  const countMatch =
    desired.length > 0 &&
    desired.length === media.length &&
    desired.length === alignedIds.length;
  const allKnown =
    countMatch &&
    desired.every((hash) => hash != null) &&
    media.every((hash) => hash != null);

  const fillSingleGap = (pairs) => {
    if (!countMatch) return pairs;
    const next = pairs.slice();
    const unmatched = [];
    next.forEach((idx, i) => {
      if (idx == null) unmatched.push(i);
    });
    const used = new Set(next.filter((idx) => idx != null));
    const unused = [];
    for (let j = 0; j < media.length; j += 1) {
      if (!used.has(j)) unused.push(j);
    }
    if (unmatched.length === 1 && unused.length === 1) {
      next[unmatched[0]] = unused[0];
    }
    return next;
  };

  let pairIdx = pairIntakePhotosToShopifyMedia(desired, media, {
    fullSet: allKnown,
    maxHamming,
  });
  pairIdx = fillSingleGap(pairIdx);
  const stillOpen = pairIdx.some((idx) => idx == null);
  const oneMissing =
    countMatch && desired.filter((hash) => hash == null).length === 1;
  if (stillOpen && oneMissing) {
    pairIdx = fillSingleGap(
      pairIntakePhotosToShopifyMedia(desired, media, {
        fullSet: false,
        maxHamming: 64,
      })
    );
  }

  const desiredIds = [];
  const seen = new Set();
  for (const idx of pairIdx) {
    if (idx == null) continue;
    const id = alignedIds[idx];
    if (!id || seen.has(id)) continue;
    seen.add(id);
    desiredIds.push(String(id));
  }
  const currentIds = alignedIds.filter(Boolean).map((id) => String(id));
  const { moves, order } = planShopifyMediaMoves(currentIds, desiredIds);
  const unmatched = pairIdx.filter((idx) => idx == null).length;
  return {
    pairIdx,
    desiredIds,
    moves,
    order,
    unmatched,
    resolved: desired.length > 0 && unmatched === 0 && desiredIds.length === desired.length,
  };
}

/** Intake photos belong on a product whose Shopify media is empty. */
export function intakePhotosShouldUpload(product, intakePhotoCount) {
  return Number(intakePhotoCount) > 0 && !shopifyProductHasDisplayImages(product);
}

/**
 * Orphans we may delete. An unread image (hash null) is kept. If nothing
 * matched and nothing uploaded, the current gallery stays — a body write
 * never deletes images, and a failed reconcile must not empty the media.
 */
export function galleryImagesToDelete(fingerprints = [], { uploaded = 0, moved = 0 } = {}) {
  const list = Array.isArray(fingerprints) ? fingerprints : [];
  const uploadedN = Number(uploaded) || 0;
  const movedN = Number(moved) || 0;
  const unknown = list.filter((cand) => cand && !cand.used && cand.hash == null);
  const orphans = list.filter((cand) => cand && !cand.used && cand.hash != null);
  if (unknown.length && !uploadedN && !movedN) return [];
  const kept = list.filter((cand) => cand && (cand.used || cand.hash == null)).length;
  if (!kept && !uploadedN && !movedN) return [];
  return orphans;
}

/** A product PUT that includes images replaces the gallery. Listing writes omit it. */
export function listingProductOmitsImages(product) {
  if (!product || typeof product !== "object") return product;
  const next = { ...product };
  delete next.images;
  return next;
}

export function gallerySyncOutcome({
  desired = 0,
  moved = 0,
  uploaded = 0,
  deleted = 0,
  failedMoves = 0,
  failedUploads = 0,
  failedDeletes = 0,
} = {}) {
  const failed = Number(failedMoves || 0) + Number(failedUploads || 0) + Number(failedDeletes || 0);
  const pending = failed > 0;
  return {
    status: pending ? "pending" : "synced",
    retry: pending,
    reorderOk: !pending,
    moved: Number(moved || 0),
    uploaded: Number(uploaded || 0),
    deleted: Number(deleted || 0),
    desired: Number(desired || 0),
    failed,
  };
}

/**
 * A reorder that could not pair every Intake photo did not confirm the gallery.
 * Leave it pending and retry a bounded number of times. A real move failure still retries.
 * Zero moves after a complete pairing is a true "already matched".
 */
export function reorderOnlySyncDecision({
  desired = 0,
  unmatched = 0,
  moved = 0,
  failedMoves = 0,
} = {}) {
  const pairedAll = Number(desired) > 0 && Number(unmatched) === 0;
  const outcome = gallerySyncOutcome({ desired, moved, failedMoves });
  if (pairedAll || Number(failedMoves) > 0) {
    return { ...outcome, orderVerified: pairedAll && outcome.status === "synced" };
  }
  // Still unresolved. Retry a bounded number of times; the caller threads attempt.
  return {
    ...outcome,
    status: "pending",
    retry: true,
    reorderOk: false,
    failed: Math.max(Number(unmatched) || 0, 1),
    orderVerified: false,
  };
}

let galleryRetryHandler = null;
export function setGalleryRetryHandler(handler) {
  galleryRetryHandler = typeof handler === "function" ? handler : null;
}

export function scheduleGalleryRetry(job = {}) {
  const attempt = Number(job.attempt || 0) + 1;
  if (attempt > 4) return { queued: false, reason: "retry_cap", attempt };
  const next = {
    intakeItemId: job.intakeItemId,
    shopifyProductId: job.shopifyProductId,
    reorderOnly: Boolean(job.reorderOnly),
    attempt,
    queuedAt: job.queuedAt || new Date().toISOString(),
    rebuildFromCurrent: true,
    log: job.log,
  };
  if (galleryRetryHandler) {
    galleryRetryHandler(next);
    return { queued: true, attempt };
  }
  const delay = Math.min(SHOPIFY_WEBHOOK_COALESCE_CEILING_MS, 800 * attempt);
  setTimeout(() => {
    void handleIntakeImagesReorder(
      {
        intakeItemId: next.intakeItemId,
        shopifyProductId: next.shopifyProductId,
        reorderOnly: Boolean(next.reorderOnly),
        rebuildFromCurrent: true,
        queuedAt: next.queuedAt,
        attempt: next.attempt,
      },
      { log: next.log }
    ).catch(() => {});
  }, delay);
  return { queued: true, attempt };
}

export async function finishGallerySync({
  intakeItemId,
  productId,
  outcome,
  detail,
  log,
  retryJob = null,
  stamp = null,
  photoIds = null,
} = {}) {
  const write = stamp || stampIntakeShopifyImageSync;
  if (intakeItemId && outcome) {
    const payload = {
      status: outcome.status,
      shopifyProductId: productId,
      uploaded: outcome.uploaded || 0,
      detail,
      log,
    };
    // Persist agreed Intake photo order so a Shopify echo webhook is not
    // treated as a fresh Shopify-side reorder.
    if (Array.isArray(photoIds) && photoIds.length) {
      payload.photoIds = photoIds;
    } else if (intakeItemId && outcome.status === "synced") {
      try {
        const item = await fetchIntakeItemById(intakeItemId, { log });
        const ids = orderedIntakePhotoIds(item, { primaryFirst: false });
        if (ids.length) payload.photoIds = ids;
      } catch (_err) {
        /* optional snapshot assist */
      }
    }
    await write(intakeItemId, payload);
  }
  if (outcome?.retry && retryJob) scheduleGalleryRetry(retryJob);
  return outcome;
}

export function listingFailureShouldRetry(result) {
  const reason = `${result?.reason || ""} ${result?.message || ""}`;
  return /429|exceeded 2 calls per second/i.test(reason);
}

let listingRetryHandler = null;
export function setListingRetryHandler(handler) {
  listingRetryHandler = typeof handler === "function" ? handler : null;
}

export function scheduleListingUpdateRetry(payload = {}, { log } = {}) {
  const attempt = Number(payload?.__retryAttempt || 0) + 1;
  if (attempt > 4) return { queued: false, reason: "retry_cap", attempt };
  const job = {
    intakeItemId: payload.intakeItemId || payload.itemId || null,
    shopifyProductId: payload.shopifyProductId || null,
    __retryAttempt: attempt,
    __rebuildFromCurrent: true,
    __corrective: payload.__corrective === true,
    syncFields: payload.syncFields || null,
    nameOnly: payload.nameOnly === true,
    queuedAt: payload.queuedAt || new Date().toISOString(),
    allowOpenAi: false,
  };
  if (listingRetryHandler) {
    listingRetryHandler(job);
    return { queued: true, attempt };
  }
  const delay = Math.min(
    SHOPIFY_WEBHOOK_COALESCE_CEILING_MS,
    shopifyRetryDelayMs({ status: 429, headers: {} }, attempt)
  );
  setTimeout(() => {
    void handleIntakeListingUpdate(job, { log }).catch(() => {});
  }, delay);
  return { queued: true, attempt };
}

async function reorderShopifyMediaOnce(productId, moves) {
  if (!moves.length) return { ok: true, moved: 0 };
  const data = await shopifyAdminGraphql(
    `mutation ReorderProductMedia($id: ID!, $moves: [MoveInput!]!) {
      productReorderMedia(id: $id, moves: $moves) {
        job { id }
        mediaUserErrors { field message code }
      }
    }`,
    {
      id: `gid://shopify/Product/${productId}`,
      moves: moves.map((move) => ({
        id: move.id,
        newPosition: String(move.newPosition),
      })),
    }
  );
  const payload = data?.productReorderMedia || {};
  const userErrors = payload.mediaUserErrors || [];
  if (userErrors.length) {
    throw new Error(userErrors.map((err) => err.message || err.code || "reorder failed").join("; "));
  }
  return { ok: true, moved: moves.length, jobId: payload.job?.id || null };
}

/**
 * Reorder images Shopify already has. One productReorderMedia call.
 * Does not upload, delete, or write the product title, body, tags, or price.
 */
async function completeReorderOnly({
  productId,
  intakeItemId,
  desiredFingerprints,
  shopifyImages,
  log,
  attempt = 0,
}) {
  let media = [];
  try {
    media = await fetchShopifyMediaImages(productId);
  } catch (err) {
    log?.("warn", {
      event: "shopify.intake_image.reorder_media_query_failed",
      shopifyProductId: productId,
      message: err?.message || String(err),
    });
    media = [];
  }
  if (!media.length) {
    media = (shopifyImages || [])
      .filter((img) => img?.id && img?.src)
      .sort((a, b) => Number(a.position || 0) - Number(b.position || 0))
      .map((img) => ({
        id: `gid://shopify/MediaImage/${img.id}`,
        src: img.src,
      }));
  }

  const baseResult = {
    ok: true,
    status: "reordered",
    shopifyProductId: productId,
    intakeItemId: intakeItemId || null,
    name: null,
    desired: (desiredFingerprints || []).length,
    moved: 0,
    uploaded: 0,
    deleted: 0,
    imageIds: [],
  };

  if (!media.length) {
    const hasIntakePhotos = (desiredFingerprints || []).some((row) => row?.src);
    log?.("info", {
      event: "shopify.intake_image.reorder_only_no_media",
      shopifyProductId: productId,
      intakeItemId: intakeItemId || null,
      needsUpload: hasIntakePhotos,
    });
    return {
      ...baseResult,
      status: "reorder_only_no_shopify_images",
      needsUpload: hasIntakePhotos,
    };
  }

  const shopifyHashed = [];
  for (const entry of media) {
    try {
      const downloaded = await downloadImageBuffer(entry.src, { cookie: null });
      const hash = await imageAverageHash(downloaded.buffer);
      shopifyHashed.push({ id: entry.id, hash });
    } catch (err) {
      log?.("warn", {
        event: "shopify.intake_image.reorder_shopify_fingerprint_failed",
        shopifyProductId: productId,
        imageId: entry.id,
        message: err?.message || String(err),
      });
      shopifyHashed.push({ id: entry.id, hash: null });
    }
  }

  // Prepared hash first: that is the file Shopify was given. The raw original
  // is only a fallback. Shopify's CDN file is a recompress of the prepared one.
  const desiredHashes = (desiredFingerprints || []).map(
    (row) => (row?.hash != null ? row.hash : row?.rawHash ?? null)
  );
  const pairing = pairReorderToMedia({
    desiredHashes,
    mediaHashes: shopifyHashed.map((row) => row.hash),
    mediaIds: shopifyHashed.map((row) => row.id),
  });
  const { moves, order } = pairing;

  let moved = 0;
  let failedMoves = 0;
  if (moves.length) {
    try {
      const gql = await reorderShopifyMediaOnce(productId, moves);
      moved = Number(gql.moved || moves.length);
      if (moved < moves.length) failedMoves = moves.length - moved;
    } catch (err) {
      log?.("warn", {
        event: "shopify.intake_image.reorder_graphql_failed",
        shopifyProductId: productId,
        message: err?.message || String(err),
      });
      moved = 0;
      for (const move of moves) {
        const imageId = mediaNumericId(move.id);
        if (!imageId) {
          failedMoves += 1;
          continue;
        }
        try {
          await updateShopifyImagePosition(
            productId,
            imageId,
            Number(move.newPosition) + 1
          );
          moved += 1;
        } catch (restErr) {
          failedMoves += 1;
          log?.("warn", {
            event: "shopify.intake_image.reorder_position_failed",
            shopifyProductId: productId,
            imageId,
            message: restErr?.message || String(restErr),
          });
        }
      }
    }
  }

  const outcome = reorderOnlySyncDecision({
    desired: desiredHashes.length,
    unmatched: pairing.resolved ? 0 : pairing.unmatched,
    moved,
    failedMoves,
  });
  const detail = outcome.status === "pending" && !outcome.retry
    ? "Shopify gallery order not confirmed (photos could not be matched) — still pending"
    : outcome.retry
      ? `Shopify gallery incomplete (${outcome.moved} moved, ${outcome.failed} failed) — will retry`
      : moved
        ? `Shopify gallery order updated (${moved} moved, no upload)`
        : "Shopify gallery order already matched (reorder only, no upload)";
  await finishGallerySync({
    intakeItemId,
    productId,
    outcome,
    detail,
    log,
    retryJob: outcome.retry
      ? {
          intakeItemId,
          shopifyProductId: productId,
          imageUrls: (desiredFingerprints || []).map((row) => row.src).filter(Boolean),
          reorderOnly: true,
          attempt: Number(attempt) || 0,
        }
      : null,
  });
  log?.(outcome.reorderOk ? "info" : "warn", {
    event: outcome.reorderOk ? "shopify.intake_image.reorder_ok" : "shopify.intake_image.reorder_incomplete",
    shopifyProductId: productId,
    intakeItemId: intakeItemId || null,
    desired: desiredHashes.length,
    moved: outcome.moved,
    failed: outcome.failed,
    uploaded: 0,
    deleted: 0,
    reorderOnly: true,
  });
  return {
    ...baseResult,
    ok: outcome.reorderOk,
    status: outcome.status === "pending" ? "reorder_pending" : "reordered",
    moved: outcome.moved,
    retry: outcome.retry,
    imageIds: order,
  };
}

/**
 * Latest-wins gallery order for a product that already has images.
 * Explicit Intake imageUrls → push to Shopify. Otherwise compare snapshot:
 * Shopify-only change adopts into Intake; Intake-only / clash / no snapshot pushes Intake.
 */
export async function reconcileExistingGalleryOrder(
  product,
  { intakeItemId = null, imageUrlsHint = null, log = null } = {}
) {
  const productId = String(product?.id || "").trim();
  const hint = normalizeCallerImageUrls(imageUrlsHint);
  if (hint.length) {
    const reordered = await handleIntakeImagesReorder(
      {
        intakeItemId,
        shopifyProductId: productId,
        name: product?.title || null,
        imageUrls: hint,
        reorderOnly: true,
      },
      { log }
    );
    const alreadyStamped =
      reordered?.status === "reordered" || reordered?.status === "reorder_pending";
    return {
      action: "push_intake",
      reason: "intake_urls_hint",
      reordered: Boolean(reordered?.ok),
      moved: reordered?.moved || 0,
      uploaded: reordered?.uploaded || 0,
      intakeItemId: reordered?.intakeItemId || intakeItemId,
      stamped: alreadyStamped,
      detail: reordered?.status === "reorder_pending"
        ? "Shopify gallery order not confirmed"
        : reordered?.ok
          ? `Shopify gallery order reconciled (${reordered?.moved || 0} moved, ${reordered?.uploaded || 0} uploaded)`
          : "Shopify already had display images",
    };
  }

  let item = null;
  const id = String(intakeItemId || "").trim() || null;
  if (id) {
    try {
      item = await fetchIntakeItemById(id, { log });
    } catch (_err) {
      item = null;
    }
  }
  if (!item) {
    return {
      action: "noop",
      reason: "no_intake_item",
      intakeItemId: id,
      detail: "Shopify already had display images",
    };
  }

  const intakeIds = orderedIntakePhotoIds(item, { primaryFirst: false });
  const intakeHash = snapshotHash(intakeIds.join("|"));
  const snap = readSnapshot(item.shopifySyncSnapshot || item.shopify_sync_snapshot);
  const snapImages = String(snap.images || "");

  const shopifyImages = Array.isArray(product?.images)
    ? [...product.images]
        .filter((img) => img?.id && img?.src)
        .sort((a, b) => Number(a.position || 0) - Number(b.position || 0))
    : [];

  let shopifyImpliedIds = intakeIds.slice();
  if (intakeIds.length && shopifyImages.length) {
    let cookie = null;
    try {
      cookie = await ensureIntakeSessionCookie(log);
    } catch (_err) {
      cookie = null;
    }
    const urls = orderedIntakePhotoUrls(item, { primaryFirst: false });
    const desiredHashes = [];
    for (const src of urls) {
      try {
        const downloaded = await downloadImageBuffer(src, { cookie });
        const prepared = await prepareShopifyAttachment(downloaded.buffer);
        desiredHashes.push(await imageAverageHash(prepared.buffer));
      } catch (_err) {
        desiredHashes.push(null);
      }
    }
    const mediaHashes = [];
    const positions = [];
    for (const img of shopifyImages) {
      positions.push(Number(img.position || mediaHashes.length + 1));
      try {
        const downloaded = await downloadImageBuffer(img.src, { cookie: null });
        mediaHashes.push(await imageAverageHash(downloaded.buffer));
      } catch (_err) {
        mediaHashes.push(null);
      }
    }
    const fullSet =
      desiredHashes.length > 0 &&
      desiredHashes.length === mediaHashes.length &&
      desiredHashes.every((hash) => hash != null) &&
      mediaHashes.every((hash) => hash != null);
    const pairs = pairIntakePhotosToShopifyMedia(desiredHashes, mediaHashes, {
      fullSet,
      maxHamming: 12,
    });
    const pairsReliable =
      pairs.length > 0 &&
      pairs.every((idx) => idx != null) &&
      desiredHashes.length === mediaHashes.length;
    if (!pairsReliable) {
      const reordered = await handleIntakeImagesReorder(
        {
          intakeItemId: item.id,
          shopifyProductId: productId,
          name: product?.title || item.name || null,
          reorderOnly: true,
        },
        { log }
      );
      const alreadyStamped =
        reordered?.status === "reordered" || reordered?.status === "reorder_pending";
      return {
        action: "push_intake",
        reason: "pairing_incomplete",
        reordered: Boolean(reordered?.ok),
        moved: reordered?.moved || 0,
        uploaded: reordered?.uploaded || 0,
        intakeItemId: item.id,
        stamped: alreadyStamped,
        detail: reordered?.status === "reorder_pending"
          ? "Shopify gallery order not confirmed"
          : reordered?.ok
            ? `Shopify gallery order reconciled (${reordered?.moved || 0} moved, ${reordered?.uploaded || 0} uploaded)`
            : "Shopify gallery order not confirmed",
      };
    }
    shopifyImpliedIds = intakePhotoIdsFromShopifyOrder(intakeIds, pairs, positions);
  }

  const shopifyImpliedHash = snapshotHash(shopifyImpliedIds.join("|"));
  const plan = planImageOrderSync({
    snapshotImagesHash: snapImages,
    intakeOrderHash: intakeHash,
    shopifyImpliedIntakeHash: shopifyImpliedHash,
  });

  if (plan.action === "adopt_shopify" && shopifyImpliedIds.length) {
    const stamped = await stampIntakeShopifyImageSync(item.id, {
      status: "synced",
      shopifyProductId: productId,
      photoIds: shopifyImpliedIds,
      detail: "Intake gallery order adopted from Shopify",
      log,
    });
    return {
      action: "adopt_shopify",
      reason: plan.reason,
      reordered: false,
      moved: 0,
      uploaded: 0,
      intakeItemId: item.id,
      stamped: Boolean(stamped?.ok),
      detail: "Intake gallery order adopted from Shopify",
    };
  }

  if (plan.action === "push_intake") {
    const reordered = await handleIntakeImagesReorder(
      {
        intakeItemId: item.id,
        shopifyProductId: productId,
        name: product?.title || item.name || null,
        reorderOnly: true,
      },
      { log }
    );
    const alreadyStamped =
      reordered?.status === "reordered" || reordered?.status === "reorder_pending";
    return {
      action: "push_intake",
      reason: plan.reason,
      reordered: Boolean(reordered?.ok),
      moved: reordered?.moved || 0,
      uploaded: reordered?.uploaded || 0,
      intakeItemId: item.id,
      stamped: alreadyStamped,
      detail: reordered?.status === "reorder_pending"
        ? "Shopify gallery order not confirmed"
        : reordered?.ok
          ? `Shopify gallery order reconciled (${reordered?.moved || 0} moved, ${reordered?.uploaded || 0} uploaded)`
          : "Shopify already had display images",
    };
  }

  return {
    action: "noop",
    reason: plan.reason,
    reordered: false,
    moved: 0,
    uploaded: 0,
    intakeItemId: item.id,
    detail: "Shopify gallery order already matched",
  };
}

/**
 * Match desired Intake image URLs to existing Shopify product images and set positions.
 * Unmatched Intake URLs are uploaded; unmatched Shopify images are deleted (reconcile).
 * reorderOnly never uploads or deletes — it only moves images already on the product.
 * Never wipes the gallery: empty imageUrls soft-skips (Shopify product + images stay).
 * Never deletes the Shopify product itself.
 * Never writes the product title, description, tags, or price.
 */
export async function handleIntakeImagesReorder(
  {
    intakeItemId = null,
    shopifyProductId = null,
    name = null,
    imageUrls = null,
    reorderOnly = false,
    rebuildFromCurrent = false,
    queuedAt = null,
    attempt = 0,
  } = {},
  { log } = {}
) {
  const id = String(intakeItemId || "").trim() || null;
  let productId = String(shopifyProductId || "").trim() || null;
  let product = null;

  if (productId) {
    product = await fetchShopifyProductById(productId, { log });
  }

  let item = null;
  if (id && UUID_RE.test(id)) {
    try {
      item = await fetchIntakeItemById(id, { log });
    } catch (_err) {
      item = null;
    }
    if (!product && item) {
      const known = String(item.shopifyProductId || item.shopify_product_id || "").trim();
      if (known) {
        productId = known;
        product = await fetchShopifyProductById(known, { log });
      }
    }
  }

  const fullName = normalizeFullItemName(name || item?.name || "");
  if (!product && fullName) {
    product = await fetchShopifyProductByFullName(fullName, { log });
    if (product?.id) productId = String(product.id);
  }

  if (!product?.id || !productId) {
    return {
      ok: false,
      deferred: true,
      status: "product_not_found",
      reason: "Shopify product not found for image reorder",
      intakeItemId: id,
      name: fullName || null,
    };
  }

  if (rebuildFromCurrent) {
    const resolvedImages = resolveQueuedImageJob({ queuedAt, shopifyProductId: productId }, item);
    if (resolvedImages.action !== "write") {
      return {
        ok: true,
        skipped: true,
        status: resolvedImages.reason || "stale_job",
        reason: resolvedImages.reason || "stale_job",
        shopifyProductId: productId,
        intakeItemId: id,
      };
    }
    imageUrls = resolvedImages.imageUrls;
  }
  // Explicit non-empty array = caller intent. Otherwise load ordered Intake URLs.
  const callerProvidedUrls = !rebuildFromCurrent && Array.isArray(imageUrls) && imageUrls.length > 0;
  let desired = callerProvidedUrls
    ? normalizeCallerImageUrls(imageUrls)
    : item
      ? orderedIntakePhotoUrls(item)
      : [];
  if (!desired.length && item) {
    desired = orderedIntakePhotoUrls(item);
  }
  // Soft-skip empty gallery — never delete all Shopify images / wipe the product.
  if (!desired.length) {
    log?.("info", {
      event: "shopify.intake_image.reorder_skip_empty",
      shopifyProductId: productId,
      intakeItemId: id,
      reason: "no_intake_photos_left_keep_shopify_gallery",
    });
    return {
      ok: true,
      status: "skipped_empty_gallery",
      shopifyProductId: productId,
      intakeItemId: id,
      name: fullName || product.title,
      desired: 0,
      moved: 0,
      uploaded: 0,
      deleted: 0,
    };
  }

  let cookie = null;
  try {
    cookie = await ensureIntakeSessionCookie(log);
  } catch (_err) {
    cookie = null;
  }

  const shopifyImages = Array.isArray(product.images)
    ? [...product.images].filter((img) => img?.id && img?.src)
    : [];

  const desiredFingerprints = [];
  for (const src of desired) {
    try {
      const downloaded = await downloadImageBuffer(src, { cookie });
      const prepared = await prepareShopifyAttachment(downloaded.buffer);
      const hash = await imageAverageHash(prepared.buffer);
      let rawHash = null;
      try {
        rawHash = await imageAverageHash(downloaded.buffer);
      } catch (_hashErr) {
        rawHash = null;
      }
      desiredFingerprints.push({ src, hash, rawHash, prepared });
    } catch (err) {
      log?.("warn", {
        event: "shopify.intake_image.reorder_fingerprint_failed",
        shopifyProductId: productId,
        message: err?.message || String(err),
      });
      desiredFingerprints.push({ src, hash: null, rawHash: null, prepared: null, error: err?.message });
    }
  }

  // Grid reorder: move images already on the product. Do not upload or delete.
  // Zero media is not a finished reorder — fall through and upload Intake photos.
  if (reorderOnly && shopifyImages.length > 0) {
    const reordered = await completeReorderOnly({
      productId,
      intakeItemId: id,
      desiredFingerprints,
      shopifyImages,
      log,
      attempt,
    });
    if (!reordered?.needsUpload) return reordered;
  }

  const shopifyFingerprints = [];
  for (const img of shopifyImages) {
    try {
      const downloaded = await downloadImageBuffer(img.src, { cookie: null });
      const hash = await imageAverageHash(downloaded.buffer);
      shopifyFingerprints.push({ img, hash, used: false });
    } catch (err) {
      log?.("warn", {
        event: "shopify.intake_image.reorder_shopify_fingerprint_failed",
        shopifyProductId: productId,
        imageId: img.id,
        message: err?.message || String(err),
      });
      shopifyFingerprints.push({ img, hash: null, used: false });
    }
  }

  const MATCH_MAX_HAMMING = 12;
  const orderedIds = [];
  const uploaded = [];
  const moved = [];
  const deleted = [];
  const failedMoves = [];
  const failedUploads = [];
  const failedDeletes = [];

  for (let i = 0; i < desiredFingerprints.length; i++) {
    const want = desiredFingerprints[i];
    const position = i + 1;
    let best = null;
    let bestDist = 999;
    if (want.hash != null) {
      for (const cand of shopifyFingerprints) {
        if (cand.used || cand.hash == null) continue;
        const dist = hammingDistance64(want.hash, cand.hash);
        if (dist < bestDist) {
          bestDist = dist;
          best = cand;
        }
      }
    }
    if (best && bestDist <= MATCH_MAX_HAMMING) {
      best.used = true;
      orderedIds.push(String(best.img.id));
      if (!imagePositionNeedsWrite(best.img, position)) continue;
      try {
        await updateShopifyImagePosition(productId, best.img.id, position);
        moved.push({ imageId: best.img.id, position, hamming: bestDist });
      } catch (err) {
        failedMoves.push({ imageId: best.img.id, position });
        log?.("warn", {
          event: "shopify.intake_image.reorder_position_failed",
          shopifyProductId: productId,
          imageId: best.img.id,
          position,
          message: err?.message || String(err),
        });
      }
      continue;
    }
    // No match — upload this Intake URL at the desired position.
    try {
      const created = await uploadIntakeUrlToShopifyProduct(productId, want.src, {
        cookie,
        position,
        log,
      });
      if (created?.id) {
        orderedIds.push(String(created.id));
        uploaded.push({ imageId: created.id, position, src: want.src });
      } else {
        failedUploads.push({ position, src: want.src });
      }
    } catch (err) {
      failedUploads.push({ position, src: want.src });
      log?.("warn", {
        event: "shopify.intake_image.reorder_upload_failed",
        shopifyProductId: productId,
        position,
        message: err?.message || String(err),
      });
    }
  }

  // Delete a confirmed orphan only. A failed fingerprint, or a replacement that
  // did not land, must not empty the gallery. A listing/body write never reaches here.
  const imagesToDelete = galleryImagesToDelete(shopifyFingerprints, {
    uploaded: uploaded.length,
    moved: moved.length,
  });
  for (const cand of imagesToDelete) {
    try {
      await deleteShopifyProductImage(productId, cand.img.id, { log });
      deleted.push({ imageId: cand.img.id });
    } catch (err) {
      failedDeletes.push({ imageId: cand.img.id });
      log?.("warn", {
        event: "shopify.intake_image.reorder_delete_failed",
        shopifyProductId: productId,
        imageId: cand.img.id,
        message: err?.message || String(err),
      });
    }
  }

  const outcome = gallerySyncOutcome({
    desired: desired.length,
    moved: moved.length,
    uploaded: uploaded.length,
    deleted: deleted.length,
    failedMoves: failedMoves.length,
    failedUploads: failedUploads.length,
    failedDeletes: failedDeletes.length,
  });
  const detail = outcome.retry
    ? `Shopify gallery incomplete (${outcome.moved} moved, ${outcome.failed} failed) — will retry`
    : `Shopify gallery reconciled (${moved.length} moved, ${uploaded.length} uploaded, ${deleted.length} deleted)`;
  if (id) {
    await finishGallerySync({
      intakeItemId: id,
      productId,
      outcome,
      detail,
      log,
      retryJob: outcome.retry
        ? { intakeItemId: id, shopifyProductId: productId, imageUrls: desired, reorderOnly }
        : null,
    });
  }

  log?.(outcome.reorderOk ? "info" : "warn", {
    event: outcome.reorderOk ? "shopify.intake_image.reorder_ok" : "shopify.intake_image.reorder_incomplete",
    shopifyProductId: productId,
    intakeItemId: id,
    desired: desired.length,
    moved: outcome.moved,
    uploaded: uploaded.length,
    deleted: deleted.length,
    failed: outcome.failed,
  });

  return {
    ok: outcome.reorderOk,
    status: outcome.retry ? "reorder_pending" : "reordered",
    retry: outcome.retry,
    shopifyProductId: productId,
    intakeItemId: id,
    name: fullName || product.title,
    desired: desired.length,
    moved: outcome.moved,
    uploaded: uploaded.length,
    deleted: deleted.length,
    imageIds: orderedIds,
  };
}

const FURNITURE_WEBSITE_CATS = [
  { code: "A", label: "Art / Mirrors" },
  { code: "B", label: "Bedroom" },
  { code: "D", label: "Dining Room" },
  { code: "L", label: "Living Room" },
  { code: "O", label: "Office Den" },
  { code: "R", label: "Rugs" },
  { code: "X", label: "Accessories" },
  { code: "P", label: "Outdoor / Patio" },
  { code: "G", label: "Lighting" },
];
const LUXURY_WEBSITE_CATS = [
  { code: "A", label: "Accessories" },
  { code: "B", label: "Backpacks" },
  { code: "C", label: "Crossbody" },
  { code: "H", label: "Handbags" },
  { code: "L", label: "Luggage" },
  { code: "O", label: "Other" },
  { code: "S", label: "Small Bags" },
  { code: "T", label: "Totes" },
  { code: "W", label: "Wallets" },
  { code: "R", label: "Scarves" },
  { code: "E", label: "Belts" },
  { code: "NK", label: "Necklaces" },
  { code: "RG", label: "Rings" },
  { code: "BR", label: "Bracelets" },
  { code: "ER", label: "Earrings" },
  { code: "OJ", label: "Other Jewelry" },
];
const DIM_TAG_RE = /^(height|width|depth|weight)\s*:/i;
const JUNK_STATUS_TAG_RE = /^(used|active|sold|draft|archived|inactive)$/i;
const LOCATION_TAG_RE = /^location\s*:/i;

function normalizeMerchType(value) {
  const t = String(value || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
  if (
    t === "luxury" ||
    t === "lg" ||
    t === "luxury goods" ||
    t === "luxurygoods" ||
    t === "accessories" ||
    t === "handbag"
  ) {
    return "luxury";
  }
  return "furniture";
}

function websiteCatLabel(merchType, codeOrLabel) {
  const type = normalizeMerchType(merchType);
  const wanted = String(codeOrLabel || "").trim();
  if (!wanted) return "";
  const opts = type === "luxury" ? LUXURY_WEBSITE_CATS : FURNITURE_WEBSITE_CATS;
  const hit = opts.find(
    (c) =>
      c.code.toLowerCase() === wanted.toLowerCase() ||
      c.label.toLowerCase() === wanted.toLowerCase() ||
      c.label.toLowerCase().replace(/\s+/g, "") === wanted.toLowerCase().replace(/\s+/g, "")
  );
  return hit ? hit.label : wanted;
}

function websiteCatCode(merchType, codeOrLabel) {
  const type = normalizeMerchType(merchType);
  const wanted = String(codeOrLabel || "").trim();
  if (!wanted) return "";
  const opts = type === "luxury" ? LUXURY_WEBSITE_CATS : FURNITURE_WEBSITE_CATS;
  const hit = opts.find(
    (c) =>
      c.code.toLowerCase() === wanted.toLowerCase() ||
      c.label.toLowerCase() === wanted.toLowerCase() ||
      c.label.toLowerCase().replace(/\s+/g, "") === wanted.toLowerCase().replace(/\s+/g, "")
  );
  return hit ? hit.code : "";
}

/** FH/LG from vertical + website-category letter only (Furniture Tags.docx / Handbag Tags.docx). */
function structuredEcommerceCodes(merchType, websiteCategory) {
  const type = normalizeMerchType(merchType);
  const codes = [type === "luxury" ? "LG" : "FH"];
  const cat = websiteCatCode(type, websiteCategory);
  if (cat) codes.push(cat);
  return codes;
}

/**
 * LG / FH in Traxia ecommerce tags always win over merchType / furniture defaults.
 * Incoming ecommerce tags beat stale Shopify tags.
 */
export function resolveMerchTypeFromFhLgTags({
  ecommerceTags,
  existingTags,
  merchType,
} = {}) {
  const fromIncoming = getEcommerceVerticalOverrideFromTags(parseTagList(ecommerceTags));
  if (fromIncoming?.vertical) return fromIncoming.vertical;
  const fromExisting = getEcommerceVerticalOverrideFromTags(parseTagList(existingTags));
  if (fromExisting?.vertical) return fromExisting.vertical;
  return normalizeMerchType(merchType);
}

/**
 * Website Categories (Bedroom / Handbags / …) win when set.
 * Exception: vague "Accessories" yields to a single letter already on the tags
 * (R Scarves, H Handbags, B Bedroom) so Traxia ACCESSORIES cannot wipe the letter.
 */
export function resolveWebsiteCategoryFromFhLgTags({
  ecommerceTags,
  existingTags,
  websiteCategory,
} = {}) {
  const wanted = String(websiteCategory || "").trim();
  const vagueAccessories = !wanted || /^accessories$/i.test(wanted);
  const incoming = getEcommerceClassificationFromTags(parseTagList(ecommerceTags));
  const incomingLetter =
    incoming.categoryTag && !incoming.conflicts.includes("multiple_category_tags")
      ? incoming.categoryTag
      : null;
  if (incomingLetter && vagueAccessories) return incomingLetter;
  if (wanted) return wanted;
  const existing = getEcommerceClassificationFromTags(parseTagList(existingTags));
  if (
    existing.categoryTag &&
    !existing.conflicts.includes("multiple_category_tags")
  ) {
    return existing.categoryTag;
  }
  return "";
}

function departmentFromMerch(merchType) {
  return normalizeMerchType(merchType) === "luxury" ? "Luxury Goods" : "Furniture & Home";
}

/** Shopify Furniture & Home dropdown values (must match store metafield options). */
const FURNITURE_SHOPIFY_CATEGORIES = FURNITURE_WEBSITE_CATS.map((c) => c.label);
const LUXURY_SHOPIFY_CATEGORIES = LUXURY_WEBSITE_CATS.map((c) =>
  c.label === "Other" ? "Other " : c.label
);

/** Traxia inventory codes → website room (BED is not a Shopify metafield value). */
const TRAXIA_CATEGORY_TO_FURNITURE_ROOM = {
  BED: "Bedroom",
  BEDS: "Bedroom",
  BEDDING: "Bedroom",
  "BEDROOM SET": "Bedroom",
  "DAY BED": "Bedroom",
  DRESSER: "Bedroom",
  NIGHTSTAND: "Bedroom",
  MATTRESS: "Bedroom",
  HEADBOARD: "Bedroom",
  ARMOIRE: "Bedroom",
  WARDROBE: "Bedroom",
  SOFA: "Living Room",
  "LOVE SEAT": "Living Room",
  LOVESEAT: "Living Room",
  SECTIONAL: "Living Room",
  CHAIR: "Living Room",
  OTTOMAN: "Living Room",
  CONSOLE: "Living Room",
  "COFFEE TABLE": "Living Room",
  "END TABLE": "Living Room",
  "SIDE TABLE": "Living Room",
  "DINING TABLE": "Dining Room",
  "DINING CHAIR": "Dining Room",
  SIDEBOARD: "Dining Room",
  BUFFET: "Dining Room",
  DESK: "Office Den",
  BOOKCASE: "Office Den",
  CABINET: "Office Den",
  RUG: "Rugs",
  MIRROR: "Art / Mirrors",
  ART: "Art / Mirrors",
  LAMP: "Lighting",
  LIGHTING: "Lighting",
};

/**
 * Resolve Shopify department + child category from Intake (source of truth).
 * Website Categories (Bedroom / B) win over Traxia Category (BED) and never invent Accessories.
 */
export function resolveIntakeShopifyTaxonomy({
  merchType,
  websiteCategory,
  category,
  name,
} = {}) {
  const type = normalizeMerchType(merchType);
  const department = departmentFromMerch(type);
  const websiteLabel = websiteCatLabel(type, websiteCategory);
  const websiteCode = websiteCatCode(type, websiteCategory);
  const rawCategory = String(category || "").trim();
  const traxiaKey = rawCategory.replace(/\s+/g, " ").toUpperCase();
  const fromTraxia =
    type === "furniture" ? TRAXIA_CATEGORY_TO_FURNITURE_ROOM[traxiaKey] || "" : "";

  let shopifyCategory = "";
  if (type === "furniture") {
    if (websiteLabel && FURNITURE_SHOPIFY_CATEGORIES.includes(websiteLabel)) {
      shopifyCategory = websiteLabel;
    } else if (fromTraxia) {
      shopifyCategory = fromTraxia;
    } else if (
      websiteLabel &&
      !/^accessories$/i.test(websiteLabel) &&
      websiteLabel !== rawCategory
    ) {
      shopifyCategory = websiteLabel;
    } else {
      const blob = `${name || ""} ${rawCategory}`.toLowerCase();
      if (/\b(beds?|headboards?|nightstands?|dressers?|mattress(?:es)?)\b/.test(blob)) {
        shopifyCategory = "Bedroom";
      } else if (
        /\b(sofas?|loveseats?|sectionals?|couches?|consoles?|coffee\s*tables?)\b/.test(blob)
      ) {
        shopifyCategory = "Living Room";
      } else if (/\b(dining|sideboards?|buffets?)\b/.test(blob)) {
        shopifyCategory = "Dining Room";
      } else if (/\b(desks?|bookcases?|office)\b/.test(blob)) {
        shopifyCategory = "Office Den";
      } else if (/\b(rugs?|carpets?)\b/.test(blob)) {
        shopifyCategory = "Rugs";
      } else if (/\b(lamps?|chandeliers?|sconces?|lighting)\b/.test(blob)) {
        shopifyCategory = "Lighting";
      } else if (
        websiteCode === "X" ||
        /^accessories$/i.test(websiteLabel) ||
        /^accessories$/i.test(rawCategory)
      ) {
        shopifyCategory = "Accessories";
      }
    }
  } else {
    if (websiteLabel) {
      shopifyCategory = websiteLabel === "Other" ? "Other " : websiteLabel;
    } else if (LUXURY_SHOPIFY_CATEGORIES.includes(rawCategory)) {
      shopifyCategory = rawCategory;
    } else {
      shopifyCategory = "Other ";
    }
    // Vague Traxia ACCESSORIES + scarf title → Scarves (never leave as generic Accessories).
    const nameBlob = `${name || ""} ${rawCategory} ${websiteLabel}`.toLowerCase();
    if (
      /\b(scarf|scarves|shawl|stole|foulard)\b/.test(nameBlob) &&
      (!shopifyCategory || /^accessories$/i.test(shopifyCategory) || /^other\s*$/i.test(shopifyCategory))
    ) {
      shopifyCategory = "Scarves";
    }
  }

  return {
    merchType: type,
    department,
    category: shopifyCategory || (type === "furniture" ? "" : "Other "),
    websiteCode: websiteCode || "",
    websiteLabel: websiteLabel || "",
  };
}

async function applyIntakeShopifyTaxonomyMetafields(
  productId,
  { department, category, merchType },
  { log, existingMetafields = null } = {}
) {
  const id = String(productId || "").trim();
  const dept = String(department || "").trim();
  const cat = String(category || "").trim();
  if (!id || !dept) return { ok: false, reason: "missing_taxonomy" };

  const type = normalizeMerchType(merchType);
  const isFurniture = type === "furniture" || dept === "Furniture & Home";
  const fhNamespace =
    String(process.env.FURNITURE_AND_HOME_METAFIELD_NAMESPACE || "custom").trim() ||
    "custom";
  const fhKey =
    String(process.env.FURNITURE_AND_HOME_METAFIELD_KEY || "furniture_category").trim() ||
    "furniture_category";

  const writes = [
    upsertShopifyProductMetafield(
      id,
      { namespace: "custom", key: "department", value: dept, type: "single_line_text_field" },
      { log, existing: existingMetafields }
    ),
    upsertShopifyProductMetafield(
      id,
      {
        namespace: "custom",
        key: "product_type_group",
        value: dept,
        type: "single_line_text_field",
      },
      { log, existing: existingMetafields }
    ),
    upsertShopifyProductMetafield(
      id,
      {
        namespace: "custom",
        key: "vertical",
        value: isFurniture ? "furniture" : "luxury",
        type: "single_line_text_field",
      },
      { log, existing: existingMetafields }
    ),
  ];

  if (isFurniture && cat) {
    writes.push(
      upsertShopifyProductMetafield(
        id,
        {
          namespace: fhNamespace,
          key: fhKey,
          value: cat,
          type: "single_line_text_field",
        },
        { log, existing: existingMetafields }
      )
    );
  } else if (!isFurniture && cat) {
    writes.push(
      upsertShopifyProductMetafield(
        id,
        {
          namespace: "custom",
          key: "luxury_goods",
          value: cat,
          type: "single_line_text_field",
        },
        { log, existing: existingMetafields }
      )
    );
    writes.push(
      upsertShopifyProductMetafield(
        id,
        {
          namespace: "custom",
          key: "category",
          value: cat,
          type: "single_line_text_field",
        },
        { log, existing: existingMetafields }
      )
    );
  }

  const results = await Promise.all(writes);
  const ok = results.every((r) => r?.ok);
  log?.("info", {
    event: "shopify.listing_update.taxonomy_metafields",
    shopifyProductId: id,
    department: dept,
    category: cat || null,
    furnitureMetafield: isFurniture ? { namespace: fhNamespace, key: fhKey, value: cat } : null,
    ok,
  });
  return { ok, department: dept, category: cat, results };
}

/** Whole numbers without trailing .0 (65 not 65.0); keep real fractions (3.5). */
export function formatDimNumber(value) {
  if (value == null || String(value).trim() === "") return "";
  const n = Number(value);
  if (!Number.isFinite(n)) return String(value).trim();
  if (Math.abs(n - Math.round(n)) < 1e-9) return String(Math.round(n));
  return String(n);
}

function parseTagList(raw) {
  if (Array.isArray(raw)) {
    return raw.map((t) => String(t || "").trim()).filter(Boolean);
  }
  return String(raw || "")
    .split(/[,;\n]+/)
    .map((t) => t.trim())
    .filter(Boolean);
}

function escapeHtml(s) {
  return String(s || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

/** Intake plain description → Shopify body_html (keep existing HTML if already tagged). */
export function descriptionToShopifyHtml(text) {
  const t = String(text || "").trim();
  if (!t) return "";
  if (/<[a-z][\s\S]*>/i.test(t)) return t;
  return t
    .split(/\n+/)
    .map((p) => p.trim())
    .filter(Boolean)
    .map((p) => `<p>${escapeHtml(p)}</p>`)
    .join("");
}

/**
 * Strip Traxia/Extra Measured spill, prior Dimensions/Weight footers, and
 * mid-copy measuring phrases ("Measuring 40 H x 65 W x 29 D inches").
 * Intake dims are re-appended cleanly at the end — never leave duplicates.
 */
export function stripListingDimJunk(text) {
  let s = String(text || "");
  // HTML → plain-ish for mid-copy cleanup (keep paragraphs via newlines).
  s = s
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<\/p>\s*<p[^>]*>/gi, "\n\n")
    .replace(/<\/?p[^>]*>/gi, "\n")
    .replace(/<[^>]+>/g, " ");
  s = s
    .replace(/^current\s+(?:e-commerce\s+)?description\s*:\s*/gim, "")
    .replace(/\bMeasured\s+Height\s*=\s*[\d.]+[^\n.]*(?:Width\s*=\s*[\d.]+)?[^\n.]*(?:Depth\s*=\s*[\d.]+)?[^\n.]*(?:Weight\s*=\s*[\d.]+[^\n.]*)?/gi, "")
    .replace(/\bMeasured\s+(?:Width|Depth|Weight)\s*=\s*[\d.]+[^\n.]*/gi, "")
    // Prior clean footers — same-line Dimensions+Weight, pitcher two-line, Weight-only.
    // Strip ALL occurrences (not only trailing) so listing-update + syncAfter never stack.
    .replace(/\n*Dimensions:\s*[^\n]*/gi, "")
    .replace(/\n*Weight:\s*[\d.]+\s*lb\.?\s*/gi, "")
    .replace(/\bDimensions:\s*Width:[^\n]*/gi, "")
    .replace(/\bWeight:\s*[\d.]+\s*lb\.?/gi, "")
    // Mid-copy measuring: "Measuring 40 H x 65 W x 29 D inches," / "measures 16 H x 13 W…"
    .replace(
      /\bmeasur(?:ing|es|ed)?\s+\d+(?:\.\d+)?\s*[HhWwDdLl]\s*[x×]\s*\d+(?:\.\d+)?\s*[HhWwDdLl](?:\s*[x×]\s*\d+(?:\.\d+)?\s*[HhWwDdLl])?(?:\s*(?:inches|in\.?|")?)?/gi,
      ""
    )
    // Inline HxWxD dumps without "Measuring": "40 H x 65 W x 29 D inches"
    .replace(
      /\b\d+(?:\.\d+)?\s*[Hh]\s*[x×]\s*\d+(?:\.\d+)?\s*[Ww]\s*[x×]\s*\d+(?:\.\d+)?\s*[Dd](?:\s*(?:inches|in\.?|")?)?/gi,
      ""
    )
    .replace(
      /\b\d+(?:\.\d+)?\s*[Ww]\s*[x×]\s*\d+(?:\.\d+)?\s*[Dd]\s*[x×]\s*\d+(?:\.\d+)?\s*[Hh](?:\s*(?:inches|in\.?|")?)?/gi,
      ""
    )
    // "Overall size is about 27.5 wide x 1.5 deep x 41.5 high."
    // Junk dumps go; intentional woven size stays ("It measures 42 inches wide…",
    // "With a footprint of 42 inches wide…", "This is an 8 by 10 rug…").
    .replace(
      /(?:\boverall\s+size\s+is\s+)?(?:\b(?:about|approximately|approx\.?|roughly)\s+)?\d+(?:\.\d+)?\s*(?:(?:"|inches|inch|in\.?)\s*)?wide\s*(?:[x×]|by)\s*\d+(?:\.\d+)?\s*(?:(?:"|inches|inch|in\.?)\s*)?deep\s*(?:[x×]|by)\s*\d+(?:\.\d+)?\s*(?:(?:"|inches|inch|in\.?)\s*)?high\b\.?/gi,
      ""
    )
    // Bare "27.5 wide, 1.5 deep, 41.5 tall" dumps only — not "42 inches wide, …".
    .replace(
      /\b(?:about|approximately|approx\.?|roughly\s+)?\d+(?:\.\d+)?\s*wide\s*,\s*\d+(?:\.\d+)?\s*deep\s*,\s*(?:and\s+)?\d+(?:\.\d+)?\s*(?:high|tall)\b\.?/gi,
      ""
    )
    .replace(/\boverall\s+size\s+is\s*(?:about|approximately|approx\.?|roughly)?\s*(?=[.!]|$)/gi, "")
    .replace(/,\s*,+/g, ",")
    .replace(/\s{2,}/g, " ")
    .replace(/\s+([,.;:])/g, "$1")
    .replace(/\(\s*\)/g, "")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
  return s;
}

/** A blank or zero measurement never replaces a real one. */
export function positiveListingNumber(value) {
  if (value == null || String(value).trim() === "") return null;
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return null;
  return n;
}

/** Last Dimensions/Weight footer in a Shopify body. Prose above it is ignored. */
export function parseListingFooterMeasures(text) {
  const plain = String(text || "")
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<[^>]+>/g, " ");
  const last = (pattern) => {
    const all = [...plain.matchAll(new RegExp(pattern, "gi"))];
    if (!all.length) return null;
    return positiveListingNumber(all[all.length - 1][1]);
  };
  return {
    width: last("Width:\\s*([\\d.]+)"),
    depth: last("Depth:\\s*([\\d.]+)"),
    height: last("Height:\\s*([\\d.]+)"),
    weightLbs: last("Weight:\\s*([\\d.]+)"),
  };
}

/**
 * Keep website category + per-axis units on a dims object so formatShopifyDimsLine
 * can apply rug feet / stored ft without every caller re-merging them.
 */
export function attachListingDimMeta(dims = {}, item = null) {
  const src = dims && typeof dims === "object" ? dims : {};
  const fromItem = item && typeof item === "object" ? item : {};
  const pickMeta = (...keys) => {
    for (const key of keys) {
      const direct = src[key];
      if (direct != null && String(direct).trim() !== "") return direct;
    }
    for (const key of keys) {
      const from = fromItem[key];
      if (from != null && String(from).trim() !== "") return from;
    }
    const noted = noteItemGet(fromItem, keys);
    if (noted != null && String(noted).trim() !== "") return noted;
    return "";
  };
  return {
    ...src,
    websiteCategory: pickMeta("websiteCategory", "website_category"),
    category: pickMeta("category"),
    dimWidthUnit: pickMeta("dimWidthUnit", "dim_width_unit"),
    dimDepthUnit: pickMeta(
      "dimDepthUnit",
      "dim_depth_unit",
      "dimLengthUnit",
      "dim_length_unit"
    ),
    dimHeightUnit: pickMeta("dimHeightUnit", "dim_height_unit"),
  };
}

/**
 * Positive incoming numbers win. Zero, blank, or missing keeps the number
 * already printed in the Shopify footer so a 0 cannot wipe Weight: 10 lb.
 * Category and unit fields are preserved so rug feet cannot be forgotten.
 */
export function resolveListingDims(dims = {}, currentBody = "") {
  const current = parseListingFooterMeasures(currentBody);
  const src = dims && typeof dims === "object" ? dims : {};
  const pick = (incoming, kept) => {
    const n = positiveListingNumber(incoming);
    return n != null ? n : kept;
  };
  return attachListingDimMeta({
    width: pick(src.width ?? src.dimWidth, current.width),
    depth: pick(src.depth ?? src.dimDepth ?? src.dimLength ?? src.length, current.depth),
    height: pick(src.height ?? src.dimHeight, current.height),
    weightLbs: pick(src.weightLbs ?? src.weight, current.weightLbs),
    websiteCategory: src.websiteCategory ?? src.website_category ?? "",
    category: src.category ?? "",
    dimWidthUnit: src.dimWidthUnit ?? src.dim_width_unit ?? "",
    dimDepthUnit:
      src.dimDepthUnit ??
      src.dim_depth_unit ??
      src.dimLengthUnit ??
      src.dim_length_unit ??
      "",
    dimHeightUnit: src.dimHeightUnit ?? src.dim_height_unit ?? "",
  });
}

export function listingFooterNeedsWrite(currentBody, dims) {
  const desired = formatShopifyDimsLine(dims || {});
  const key = (text) => {
    const m = parseListingFooterMeasures(text);
    const fmt = (n) => (n == null ? "" : formatDimNumber(n));
    return [fmt(m.width), fmt(m.depth), fmt(m.height), fmt(m.weightLbs)].join("|");
  };
  return key(currentBody) !== key(desired);
}

/** Rug footprint without an explicit unit: <=30 -> feet, >30 -> inches. Assumption for staff. */
export const RUG_FOOTPRINT_FEET_MAX = 30;
/** Pile / thickness on a rug stays inches. */
export const RUG_PILE_MAX_IN = 2;

/** Rugs from Intake website category / category fields — not from title text. */
export function itemIsRug(itemOrDims = null) {
  const src = itemOrDims && typeof itemOrDims === "object" ? itemOrDims : {};
  const website = String(
    src.websiteCategory ?? src.website_category ?? noteItemGet(src, ["websiteCategory", "website_category"]) ?? ""
  )
    .trim()
    .toLowerCase();
  if (["r", "rug", "rugs"].includes(website)) return true;
  const category = String(src.category ?? noteItemGet(src, ["category"]) ?? "")
    .trim()
    .toLowerCase();
  if (["r", "rug", "rugs"].includes(category) || category.startsWith("rugs")) return true;
  return false;
}

function normalizeDimUnit(value) {
  const text = String(value || "").trim().toLowerCase();
  if (["ft", "feet", "foot", "'"].includes(text)) return "ft";
  if (["in", "inch", "inches", '"'].includes(text)) return "in";
  return "";
}

function rugPileAxis(width, depth, height) {
  const axes = [
    ["width", width],
    ["depth", depth],
    ["height", height],
  ].filter(([, value]) => value != null && value > 0);
  if (!axes.length) return null;
  const thin = axes.filter(([, value]) => value <= RUG_PILE_MAX_IN);
  if (thin.length === 1) return thin[0][0];
  const ordered = [...axes].sort((a, b) => a[1] - b[1]);
  if (ordered.length >= 2 && ordered[0][1] <= RUG_PILE_MAX_IN && ordered[0][1] < ordered[1][1] * 0.25) {
    return ordered[0][0];
  }
  return null;
}

export function resolveAxisDisplayUnit(value, { storedUnit = "", isRug = false, isPile = false } = {}) {
  const stored = normalizeDimUnit(storedUnit);
  if (stored) return stored;
  if (isPile) return "in";
  if (isRug) return value > RUG_FOOTPRINT_FEET_MAX ? "in" : "ft";
  return "in";
}

/** Same unit resolution as the Dimensions footer (rug feet, stored ft/in, pile inches). */
export function resolvedAxisUnits(dims = {}, options = {}) {
  const src = dims && typeof dims === "object" ? dims : {};
  const w = positiveListingNumber(src.width ?? src.dimWidth) ?? 0;
  const d = positiveListingNumber(src.depth ?? src.dimDepth ?? src.dimLength ?? src.length) ?? 0;
  const h = positiveListingNumber(src.height ?? src.dimHeight) ?? 0;
  const isRug = options.isRug === true || itemIsRug(src);
  const pileAxis = isRug ? rugPileAxis(w || null, d || null, h || null) : null;
  const footprintVals = [w, d, h].filter((value, idx) => {
    const axis = ["width", "depth", "height"][idx];
    return value > 0 && pileAxis !== axis;
  });
  const footprintAsInches = Boolean(
    isRug && footprintVals.length && Math.max(...footprintVals) > RUG_FOOTPRINT_FEET_MAX
  );
  const unitFor = (axis, value, storedUnit) => {
    const stored = normalizeDimUnit(storedUnit);
    if (stored) return stored;
    if (pileAxis === axis) return "in";
    if (!isRug) return "in";
    if (footprintAsInches) return "in";
    return "ft";
  };
  return {
    widthUnit: unitFor(
      "width",
      w,
      src.dimWidthUnit ?? src.dim_width_unit ?? options.widthUnit ?? ""
    ),
    depthUnit: unitFor(
      "depth",
      d,
      src.dimDepthUnit ??
        src.dim_depth_unit ??
        src.dimLengthUnit ??
        src.dim_length_unit ??
        options.depthUnit ??
        ""
    ),
    heightUnit: unitFor(
      "height",
      h,
      src.dimHeightUnit ?? src.dim_height_unit ?? options.heightUnit ?? ""
    ),
  };
}

function measureAsInches(value, unit) {
  const n = positiveListingNumber(value);
  if (n == null) return 0;
  return unit === "ft" ? n * 12 : n;
}

function formatAxisMeasure(value, unit) {
  return `${formatDimNumber(value)}${unit === "ft" ? "'" : '"'}`;
}

/** Plain-text dims footer for Shopify (source of truth = Intake, not Traxia). */
export function formatShopifyDimsLine(dims = {}, options = {}) {
  const includeSize = options.includeSize !== false;
  const includeWeight = options.includeWeight !== false;
  const w = positiveListingNumber(dims.width ?? dims.dimWidth);
  const d = positiveListingNumber(dims.depth ?? dims.dimDepth ?? dims.dimLength ?? dims.length);
  const h = positiveListingNumber(dims.height ?? dims.dimHeight);
  const wt = positiveListingNumber(dims.weightLbs ?? dims.weight);
  const { widthUnit, depthUnit, heightUnit } = resolvedAxisUnits(dims, options);
  const parts = [];
  if (includeSize && w != null) parts.push(`Width: ${formatAxisMeasure(w, widthUnit)}`);
  if (includeSize && d != null) parts.push(`Depth: ${formatAxisMeasure(d, depthUnit)}`);
  if (includeSize && h != null) parts.push(`Height: ${formatAxisMeasure(h, heightUnit)}`);
  if (!parts.length && (wt == null || !includeWeight)) return "";
  const sizeLine = parts.length ? `Dimensions: ${parts.join(" × ")}.` : "";
  const weightLine = includeWeight && wt != null ? `Weight: ${formatDimNumber(wt)} lb.` : "";
  // Weight stays on the dimensions line. A missing weight is omitted, not a blank line.
  if (sizeLine && weightLine) return `${sizeLine} ${weightLine}`;
  return sizeLine || weightLine;
}

/**
 * Shopify body = listing DESCRIPTION prose first, then ONE Dimensions+Weight footer.
 * NEVER return dims-only or weight-only (bug that left &lt;br&gt;&lt;br&gt;Dimensions...).
 * One blank line between the last prose paragraph and the dimensions line.
 * Weight stays on that same line. No extra empty paragraph.
 */
const PROMPT_LABEL_RE = /\b(?:AEO|GEO|SEO|Condition note|User notes|Staff note)\s*:\s*/gi;
const SHIPPING_PARCEL =
  "Pickup is available in Scottsdale, and we offer both local and nationwide delivery.";
const SHIPPING_BULKY =
  "Pickup is available in Scottsdale, and we offer both local and nationwide delivery. Please see Delivery, Pickup & Freight Options below the description for additional details.";
const SHIPPING_LUXURY =
  "Nationwide shipping is available for this item. See our return policy and authentication commitment below.";
// Use (?:es)? — scratches? requires the "e" and misses singular "scratch".
const DAMAGE_NOTE_RE =
  /\b(?:scratch(?:es|ed)?|scuff(?:s|ed)?|wear(?:ing)?|worn|stain(?:s|ed)?|crack(?:s|ed)?|chip(?:s|ped|ping)?|blemish(?:es)?|nick(?:s|ed)?|ding(?:s|ed)?|dent(?:s|ed)?|tear(?:s|ed|ing)?|torn|rip(?:s|ped)?|hole(?:s)?|rust(?:ed|y)?|corrod(?:ed|e|ing)?|peel(?:s|ed|ing)?|fray(?:s|ed|ing)?|pill(?:s|ed|ing)?|fade[sd]?|fading|discolor(?:ed|ation)?|damage[d]?|defect(?:s|ive)?|flaw(?:s|ed)?|imperfection(?:s)?|marred|broken|break(?:s|ing)?|missing|loose|loosen(?:ed|ing)?|wobbly|wobble[sd]?|stick(?:s|ing)?|stuck|repair(?:s|ed|ing)?|refinish(?:ed|ing)?|replaced?|not\s+working|doesn'?t\s+work|won'?t\s+work|inoperable|faulty|untested|needs?(?:\s+\w+){0,5}|odor|smell(?:s|ed|ing)?|musty|smoke|water\s+(?:ring|mark|damage|stain)|sun\s+fade|coming\s+apart|issue|problem|wrong|as[\s-]is)\b/i;
const POSITIVE_CONDITION_RE =
  /\b(?:(?:in\s+)?excellent\s+condition|works?\s+great|works?\s+(?:well|fine|perfectly)|fully\s+functional|functions?\s+(?:well|fine)|good\s+working\s+order)\b/i;
const PRICE_NOTE_RE =
  /(?:\$\s*\d|\b(?:msrp|markdown|marked\s+down|wholesale|unit\s+cost)\b|\b(?:retail|cost|asking|original|originally)\s+price\b|\b(?:price|priced|pricing)\b|\b(?:customer|buyer|she|he|they)\s+said\b[^.]*\b(?:\$|\d{2,5}|price)\b|\b(?:paid|priced|asking)\s+(?:at\s+)?\$?\d|^\d{2,5}(?:\.\d{2})?$)/i;
const LOCATION_NOTE_RE =
  /\b(?:box\s+in\s+warehouse|in\s+(?:the\s+)?warehouse|warehouse|stockroom|stock\s*room|back\s*room|in\s+back|in\s+(?:the\s+)?back\s+office|[\w']+'s\s+(?:office|desk|locker)|(?:art\s+)?folder|binder|file\s+cabinet|filing\s+cabinet|paperwork|papers?\s+in\s+(?:the\s+)?(?:office|folder|binder|file)|additional\s+information(?:\s+in\b)?|item\s+is\s+in\s+(?:the\s+)?(?:warehouse|back|office|stockroom)|artwork\s+in\s+(?:the\s+)?folder|see\s+(?:staff|office)|call\s+(?:bernis|staff)|consignor|pickup\s+arrangement|for\s+staff|internal\s+only|hold\s+for\s+\w+)\b/i;
const BRAND_NOTE_RE = /^(?:the\s+)?brand\s*(?:is|=|:)\b|^branded\s+as\b/i;
const FINITE_NOTE_RE =
  /\b(is|are|was|were|has|have|had|shows|show|sits|includes|remains|needs|comes|looks|feels)\b/i;
const COMING_APART_RE =
  /^(?:the\s+)?(.+?)\s+coming\s+(?:(a little|slightly|a bit)\s+)?apart(?:\s+(a little|slightly|a bit))?\b\s*(.*)$/i;
const PLACE_NOTE_RE =
  /^(?:(?:top|bottom|upper|lower)\s+(?:left|right)(?:\s+(?:corner|edge|side))?|(?:left|right)\s+(?:side|edge|corner))$/i;
const SHIP_SENTENCE_RE =
  /\b(shipping|delivery|deliveries|pickup|pick-up|scottsdale|nationwide|freight|return policy|authentication commitment)\b/i;
const BULKY_NAME_RE =
  /\b(sofas?|sectionals?|loveseats?|dining\s*tables?|kitchen\s*tables?|dressers?|armoires?|wardrobes?|sideboards?|buffets?|hutches?|credenzas?|bed\s*frames?|headboards?|china\s*cabinets?|hope\s*chests?|cedar\s*chests?)\b/i;
const FURNITURE_NAME_RE =
  /\b(chairs?|stools?|nightstands?|dressers?|sofas?|sectionals?|tables?|desks?|cabinets?|beds?|benches?|bookcases?|ottomans?|armoires?)\b/i;
const NOT_FURNITURE_RE =
  /\b(paintings?|canvas|artworks?|prints?|handbag|handbags|purse|wallet|scarf|belt|earring|necklace|bracelet|jewelry|jewell?ery|pillow|throw)\b/i;

/** Drop AEO/GEO/SEO/Condition note labels so the shopper reads one flowing description. */
export function flowingBuyerProse(text) {
  let desc = String(text || "").replace(PROMPT_LABEL_RE, "");
  desc = desc.replace(/[ \t]{2,}/g, " ").replace(/ *\n */g, "\n").replace(/\n{3,}/g, "\n\n").trim();
  desc = desc.replace(/(^|[.!?]\s+)([a-z])/g, (_, lead, ch) => lead + ch.toUpperCase());
  return desc.trim();
}

function noteItemGet(item, names) {
  if (!item || typeof item !== "object") return undefined;
  for (const name of names) {
    const value = item[name];
    if (value !== undefined && value !== null && String(value).trim() !== "") return value;
  }
  return undefined;
}

function noteNumber(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : 0;
}

export function clauseMentionsConditionProblem(text) {
  const cleaned = String(text || "").replace(/\s+/g, " ").trim();
  if (!cleaned) return false;
  if (/\bcoming\b/i.test(cleaned) && /\bapart\b/i.test(cleaned)) return true;
  return DAMAGE_NOTE_RE.test(cleaned);
}

export function clauseMentionsCondition(text) {
  const cleaned = String(text || "").replace(/\s+/g, " ").trim();
  if (!cleaned) return false;
  if (isNegatedConditionNote(cleaned)) return true;
  if (POSITIVE_CONDITION_RE.test(cleaned) || positiveConditionSentence(cleaned)) return true;
  return clauseMentionsConditionProblem(cleaned);
}

function splitMixedConditionParts(text) {
  const parts = String(text || "")
    .split(/\s*,\s*|\s+and\s+/i)
    .map((part) => part.replace(/^[\s.,;]+|[\s.,;]+$/g, ""))
    .filter(Boolean);
  if (parts.length < 2) return [text];
  const hasCond = parts.some((part) => clauseMentionsCondition(part));
  const hasOther = parts.some((part) => !clauseMentionsCondition(part));
  if (hasCond && hasOther) return parts;
  return [text];
}

function splitNoteClauses(text) {
  const pieces = [];
  for (const line of String(text || "").replace(/\r/g, "\n").split("\n")) {
    const flat = line.replace(/\s+/g, " ").trim();
    if (!flat) continue;
    const bits = flat.split(
      /\s*;\s*|\s+-\s+|\.\s+|(?=\bBrand\s+is\b)|(?=\bMeasured\b)|(?=\b(?:Height|Width|Depth|Length|Weight)\s*=)/i
    );
    for (const bit of bits) {
      const cleaned = String(bit || "").replace(/^[\s.,;]+|[\s.,;]+$/g, "");
      if (!cleaned) continue;
      for (const part of splitMixedConditionParts(cleaned)) {
        if (part) pieces.push(part);
      }
    }
  }
  return pieces;
}

function isMeasureDump(text) {
  if (/\b(?:extends?|seats?|sleeps?|opens?\s+to)\b/i.test(text)) return false;
  if (/^measured\b/i.test(text)) return true;
  if (!/\b(?:height|width|depth|length|weight)\s*[=:]\s*\d/i.test(text)) return false;
  const letters = text
    .replace(/\b(?:height|width|depth|length|weight|measured|inches|inch|lbs|lb)\b/gi, "")
    .replace(/[\d\s=:,.;]+/g, "");
  return letters.length < 3 || text.length < 80;
}

function cleanNoteClause(part) {
  let text = String(part || "").replace(/\s+/g, " ").replace(/^[\s.,;]+|[\s.,;]+$/g, "");
  if (!text || !/[A-Za-z]/.test(text)) return "";
  if (/\btraxia updated\b/i.test(text)) return "";
  if (BRAND_NOTE_RE.test(text)) return "";
  if (isMeasureDump(text)) return "";
  if (PRICE_NOTE_RE.test(text) && !DAMAGE_NOTE_RE.test(text)) return "";
  if (LOCATION_NOTE_RE.test(text)) {
    let stripped = text.replace(LOCATION_NOTE_RE, " ");
    stripped = stripped
      .replace(/\b(?:tall|short|large|small)\s+box\b|\bbox\s+that\s+says\b/gi, " ")
      .replace(/\s+/g, " ")
      .replace(/^[\s.,;-]+|[\s.,;-]+$/g, "");
    if (DAMAGE_NOTE_RE.test(stripped) || /\bcoming\b/i.test(stripped)) text = stripped;
    else return "";
  }
  return text;
}

export function buyerFacingClauses(notes, conditionIssues) {
  const kept = [];
  const seen = new Set();
  for (const raw of [...splitNoteClauses(notes), ...splitNoteClauses(conditionIssues)]) {
    const cleaned = cleanNoteClause(raw);
    const key = cleaned.toLowerCase();
    if (!cleaned || seen.has(key)) continue;
    if (!clauseMentionsCondition(cleaned)) continue;
    seen.add(key);
    kept.push(cleaned);
  }
  return kept;
}

export function notesAreInternalOnly(notes, conditionIssues) {
  const raw = [String(notes || ""), String(conditionIssues || "")].filter((part) => part.trim()).join("\n").trim();
  if (!raw) return false;
  return buyerFacingClauses(notes, conditionIssues).length === 0;
}

export function notesWarrantAsIs(notes, conditionIssues) {
  for (const clause of buyerFacingClauses(notes, conditionIssues)) {
    if (isNegatedConditionNote(clause)) continue;
    if (POSITIVE_CONDITION_RE.test(clause) || positiveConditionSentence(clause)) continue;
    if (clauseMentionsConditionProblem(clause)) return true;
  }
  return false;
}

function capNote(text) {
  const value = String(text || "").trim();
  if (!value) return "";
  return value.charAt(0).toUpperCase() + value.slice(1);
}

function periodNote(text) {
  const value = String(text || "").replace(/\s+/g, " ").trim();
  if (!value) return "";
  return /[.!?]$/.test(value) ? value : `${value}.`;
}

function theSubject(subject) {
  const value = String(subject || "").replace(/\s+/g, " ").replace(/^[\s.,;]+|[\s.,;]+$/g, "");
  if (!value) return "It";
  if (/^(a|an|the|this|its)\b/i.test(value)) return capNote(value);
  return `The ${value.charAt(0).toLowerCase()}${value.slice(1)}`;
}

function naturalPlace(loc) {
  const value = String(loc || "").replace(/\s+/g, " ").replace(/^[\s.,;]+|[\s.,;]+$/g, "");
  if (!value) return "";
  const bare = value.replace(/^(?:in|at|on)\s+(?:the\s+)?/i, "").trim();
  if (PLACE_NOTE_RE.test(bare)) return `at the ${bare.toLowerCase().replace(/\s+/g, " ")}`;
  if (/^(?:in|at|on|along|near|around)\b/i.test(value)) {
    return value.charAt(0).toLowerCase() + value.slice(1);
  }
  return bare || value;
}

const FLAW_FRAGMENT_RE =
  /^(?:(?:a|an)\s+)?(?:(small|tiny|minor|slight|light)\s+)?(scratches|scratch|chips|chip|cracks|crack|stains|stain|dents|dent|scuffs|scuff|nicks|nick|blemishes|blemish|marks|mark|tears|tear)\s+(in|on|at)\s+(?:the\s+)?(.+)$/i;

/** Shorthand staff flaw → one shopper sentence. Real sentences do not match. */
export function naturalFlawSentence(note) {
  let text = String(note || "").replace(/\s+/g, " ").replace(/^[\s.,;]+|[\s.,;]+$/g, "");
  const letters = text.match(/[A-Za-z]/g) || [];
  const uppers = letters.filter((ch) => ch >= "A" && ch <= "Z").length;
  if (letters.length && uppers / letters.length > 0.6) text = text.toLowerCase();
  text = text.replace(/\bsmallt\b/gi, "small");
  text = text.replace(/\bsmalli\b/gi, "small");
  text = text.replace(/\bsmalt\b/gi, "small");
  text = text.replace(/\b(?:small\s+little|little\s+small)\b/gi, "small");
  text = text.replace(/^(?:(?:sold\s+)?as[\s-]is\s*[:\-]?\s*)+/i, "").replace(/^[\s.,;]+|[\s.,;]+$/g, "");
  text = text.replace(/,?\s*as shown in the photos\.?$/i, "").replace(/^[\s.,;]+|[\s.,;]+$/g, "");
  const match = text.match(FLAW_FRAGMENT_RE);
  if (!match) return "";
  let place = String(match[4] || "").replace(/\s+/g, " ").replace(/^[\s.,;]+|[\s.,;]+$/g, "");
  place = place.replace(/^(?:the)\s+/i, "").toLowerCase();
  if (!place) return "";
  const size = (match[1] || "").toLowerCase();
  const noun = (match[2] || "").toLowerCase();
  const prep = (match[3] || "").toLowerCase();
  const sizeBit = size ? `${size} ` : "";
  if (noun.endsWith("s")) return periodNote(`There are ${sizeBit}${noun} ${prep} the ${place}`);
  return periodNote(`There is a ${sizeBit}${noun} ${prep} the ${place}`);
}

/** Staff note → one sentence. The frame note is written out, not pasted raw. */
export function naturalBuyerSentence(note) {
  let text = String(note || "").replace(/\s+/g, " ").replace(/^[\s.,;]+|[\s.,;]+$/g, "");
  text = text.replace(/^(?:please note|note|additionally)\s*:?\s*/i, "").trim();
  if (!text) return "";
  if (FINITE_NOTE_RE.test(text)) return periodNote(capNote(text));
  const coming = text.match(COMING_APART_RE);
  if (coming && coming[1].trim().split(/\s+/).length <= 6) {
    const degreeBefore = (coming[2] || "").toLowerCase();
    const degreeAfter = (coming[3] || "").toLowerCase();
    let sentence = `${theSubject(coming[1])} is coming`;
    if (degreeBefore) sentence += ` ${degreeBefore} apart`;
    else {
      sentence += " apart";
      if (degreeAfter) sentence += ` ${degreeAfter}`;
    }
    const place = naturalPlace(coming[4] || "");
    if (place) sentence += ` ${place}`;
    return periodNote(sentence);
  }
  return periodNote(capNote(text));
}

function asIsClause(text) {
  const cleaned = String(text || "")
    .replace(/^(?:please note|note)\s*:?\s*/i, "")
    .replace(/^[\s.]+|[\s.]+$/g, "");
  if (/^(?:sold\s+)?as[\s-]is$/i.test(cleaned)) return "sold as is";
  if (!cleaned) return "sold as is";
  return cleaned.charAt(0).toLowerCase() + cleaned.slice(1);
}

/**
 * Never emit "AS IS: …" — that label is body_as_is_paste and restarts the rewrite loop.
 * A flaw note becomes a shopper sentence; a bare as-is sale uses the stock wear line.
 */
function asIsLine(clause) {
  const sentence = naturalFlawSentence(clause);
  if (sentence) return sentence;
  if (isGenericWearClause(clause)) return "";
  const buyer = naturalBuyerSentence(clause);
  if (buyer && !/^AS IS\s*:/i.test(buyer)) return buyer;
  return "";
}

const GENERATED_FLAW_SENTENCE_RE =
  /^there (?:is|are) (?:a |an )?(?:(?:small|tiny|minor|slight|light) )?(?:scratches|scratch|chips|chip|cracks|crack|stains|stain|dents|dent|scuffs|scuff|nicks|nick|blemishes|blemish|marks|mark|tears|tear) (?:in|on|at) the [^.]+\.?$/i;

/** The only wear sentence a buyer description may contain (matches Pricing Engine). */
const STOCK_WEAR_SENTENCE =
  "Sold AS IS with visible wear consistent with prior use; please review photos for the most accurate view of condition.";
/** visible + Extra Info adjectives woven into the same AS IS template */
const WEAR_ADJ =
  "(?:visible|noticeable|significant|heavy|moderate|extensive|substantial|light|slight|minor|normal|expected)";
const CANONICAL_WEAR_RE = new RegExp(
  `Sold AS IS with ${WEAR_ADJ} wear consistent with prior use\\s*;\\s*please review photos for the most accurate view of condition\\.?`,
  "gi"
);
const MANGLED_WEAR_RE = new RegExp(
  `Sold AS IS with ${WEAR_ADJ} wear consistent with prior use\\s+please review photos for the most accurate view of condition\\.?`,
  "gi"
);
const BAD_THIS_ITEM_SOLD_RE = new RegExp(
  `This item is sold AS IS, with ${WEAR_ADJ} wear consistent with prior use\\.?`,
  "gi"
);
const BAD_AS_IS_LABEL_WEAR_RE = new RegExp(
  `AS IS:\\s*this item is sold AS IS,?\\s*with ${WEAR_ADJ} wear consistent with prior use(?:,\\s*as shown in the photos)?\\.?`,
  "gi"
);
const ORPHAN_REVIEW_RE =
  /;?\s*(?:please\s+)?review photos for the most accurate view of condition\.?/gi;
const DANGLING_SOLD_WEAR_RE = new RegExp(
  `Sold AS IS with ${WEAR_ADJ} wear consistent with prior use\\s*;?`,
  "gi"
);
const SPECIFIC_WEAR_NOTE_RE = new RegExp(
  `^(?:(?:sold\\s+)?as[\\s-]is\\s+with\\s+)?(?:with\\s+)?(noticeable|significant|heavy|moderate|extensive|substantial|light|slight|minor|normal|expected)\\s+wear(?:\\s+consistent with prior use)?$`,
  "i"
);
const SHIP_START_RE =
  /Pickup is available in Scottsdale|Nationwide shipping is available/i;
const PRODUCT_TAIL_STOP_RE =
  /Sold AS IS\b|This item is sold AS IS\b|AS IS:\s*this item is sold|Pickup is available\b|Nationwide shipping\b|Dimensions:\s|Noted \w+ are present\b|There (?:is|are) (?:a |an )?(?:small |tiny |minor )?(?:scratch|chip|crack|dent|stain)/i;
const OLD_RH_PARAGRAPH_RE =
  /\s*This RH curved wing\b.*?(?=Sold AS IS\b|This item is sold AS IS\b|AS IS:\s*this item is sold|Pickup is available\b|Nationwide shipping\b|Dimensions:\s|Noted \w+ are present\b|There (?:is|are) (?:a |an )?(?:small |tiny |minor )?(?:scratch|chip|crack|dent|stain)|$)/gi;
const SECOND_PRODUCT_REWRITE_RE =
  /(?<=[.!?])\s+(?:This RH curved wing\b|This\s+(?:RH\s+|Restoration Hardware\s+)?[A-Z][^.]{0,100}?\bfeatures a\b|Designed to anchor your (?:primary )?bedroom\b|Ideal for contemporary and transitional interiors\b).*?(?=Sold AS IS\b|This item is sold AS IS\b|AS IS:\s*this item is sold|Pickup is available\b|Nationwide shipping\b|Dimensions:\s|Noted \w+ are present\b|There (?:is|are) (?:a |an )?(?:small |tiny |minor )?(?:scratch|chip|crack|dent|stain)|$)/gi;

function stripAsIsDumps(prose) {
  return String(prose || "")
    .replace(/\s*AS IS:\s*[^.]+\.?/gi, " ")
    .replace(/\s{2,}/g, " ")
    .trim();
}

function stripStockWearCopies(prose) {
  let text = String(prose || "");
  for (const pattern of [
    CANONICAL_WEAR_RE,
    MANGLED_WEAR_RE,
    BAD_THIS_ITEM_SOLD_RE,
    BAD_AS_IS_LABEL_WEAR_RE,
    ORPHAN_REVIEW_RE,
    DANGLING_SOLD_WEAR_RE,
  ]) {
    text = text.replace(pattern, " ");
  }
  return text.replace(/[ \t]{2,}/g, " ").replace(/\n{3,}/g, "\n\n").trim();
}

function containsStockWear(text) {
  const blob = String(text || "");
  if (!blob) return false;
  const canonical = new RegExp(
    `Sold AS IS with ${WEAR_ADJ} wear consistent with prior use\\s*;\\s*please review photos for the most accurate view of condition\\.?`,
    "i"
  );
  const mangled = new RegExp(
    `Sold AS IS with ${WEAR_ADJ} wear consistent with prior use\\s+please review photos for the most accurate view of condition\\.?`,
    "i"
  );
  const badThis = new RegExp(
    `This item is sold AS IS, with ${WEAR_ADJ} wear consistent with prior use\\.?`,
    "i"
  );
  const badLabel = new RegExp(
    `AS IS:\\s*this item is sold AS IS,?\\s*with ${WEAR_ADJ} wear consistent with prior use`,
    "i"
  );
  if (canonical.test(blob) || mangled.test(blob) || badThis.test(blob) || badLabel.test(blob)) {
    return true;
  }
  const leftover = blob.replace(
    new RegExp(
      `Sold AS IS with ${WEAR_ADJ} wear consistent with prior use\\s*;\\s*please review photos for the most accurate view of condition\\.?`,
      "gi"
    ),
    " "
  );
  return (
    /;?\s*(?:please\s+)?review photos for the most accurate view of condition\.?/i.test(leftover) ||
    new RegExp(`Sold AS IS with ${WEAR_ADJ} wear consistent with prior use\\s*;?`, "i").test(leftover)
  );
}

const COPY_STOP_WORDS = new Set([
  "the", "and", "with", "for", "from", "that", "this", "these", "those",
  "its", "into", "over", "under", "than", "then", "very", "just", "more",
  "most", "such", "also", "while", "which", "who", "are", "was", "were",
  "has", "have", "had", "not", "but", "you", "your", "our", "their",
  "piece", "item", "one", "two",
]);

function copyWords(sentence) {
  return new Set(
    (String(sentence || "").toLowerCase().match(/[a-z0-9]+/g) || []).filter(
      (word) => word.length > 2 && !COPY_STOP_WORDS.has(word)
    )
  );
}

function isListingTailSentence(sentence) {
  const text = String(sentence || "");
  if (containsStockWear(text) || SHIP_START_RE.test(text)) return true;
  return /Sold AS IS\b|This item is sold AS IS\b|AS IS:\s*this item is sold|Pickup is available\b|Nationwide shipping\b|Dimensions:\s|Noted \w+ are present\b|There (?:is|are) (?:a |an )?(?:small |tiny |minor )?(?:scratch|chip|crack|dent|stain)/i.test(
    text
  );
}

/** Light plural folding so "artists" and "artist's" are the same word. */
function copyStem(word) {
  if (word.length > 4 && word.endsWith("ies")) return `${word.slice(0, -3)}y`;
  if (word.length > 3 && word.endsWith("s") && !word.endsWith("ss")) return word.slice(0, -1);
  return word;
}

function copyStemSequence(sentence) {
  return (String(sentence || "").toLowerCase().match(/[a-z0-9]+/g) || [])
    .filter((word) => word.length > 2 && !COPY_STOP_WORDS.has(word))
    .map(copyStem);
}

function copyStems(sentence) {
  return new Set(copyStemSequence(sentence));
}

const ITEM_NAME_SUFFIX_RE =
  /(?:\s*[-–—]\s*)?\d+(?:\.\d+)?(?:\s*[xX×]\s*\d+(?:\.\d+)?){1,2}\s*H?\b(?:\s*[-–—]\s*AS\s*-?\s*IS)?\s*$/i;

/** The item's name without the size token and the AS IS suffix. */
function itemNameCore(item) {
  return String(noteItemGet(item, ["name", "title"]) || "")
    .replace(ITEM_NAME_SUFFIX_RE, "")
    .replace(/\bAS\s*-?\s*IS\b/gi, "")
    .trim();
}

/** Words every sentence about this item may share: its name, brand, and category. */
export function writeupIdentityWords(item) {
  const words = new Set();
  if (!item || typeof item !== "object") return words;
  for (const text of [itemNameCore(item), noteItemGet(item, ["brand"]), noteItemGet(item, ["category"])]) {
    for (const word of copyStems(text)) words.add(word);
  }
  return words;
}

// A write-up is at least this many product sentences. One sentence is never one.
const WRITEUP_MIN_SENTENCES = 2;
// The full item name said again counts only when the name is at least this long.
const WRITEUP_NAME_RUN_MIN_WORDS = 5;

/** True when the sentence carries the item's full name, word for word and in order. */
function sentenceSaysItemName(sentence, nameSequence, atStart = false) {
  const size = nameSequence.length;
  if (size < WRITEUP_NAME_RUN_MIN_WORDS) return false;
  const words = copyStemSequence(sentence);
  const matchesAt = (start) => nameSequence.every((word, offset) => words[start + offset] === word);
  if (atStart) return words.length >= size && matchesAt(0);
  for (let start = 0; start + size <= words.length; start += 1) {
    if (matchesAt(start)) return true;
  }
  return false;
}

/**
 * Index where a second product write-up starts, or null for one description.
 *
 * A later sentence that mentions the same item is not evidence. Words from the
 * item's name, brand, and category are the item's identity and are not counted
 * as repetition (without an item, the opening sentence stands in for the name).
 * A second write-up is one of these two things:
 *   - A second block. At least two product sentences come before it, it is at
 *     least two product sentences long, its first sentence introduces the item
 *     again, and it repeats the first block's own descriptive words: three or
 *     more, 40% of the smaller block, and found in a sentence after the
 *     restart, not only in the restart.
 *   - A new opening. A later sentence begins with the item's full name (five or
 *     more words, in order) when no sentence before it used the full name.
 * Same rule as Pricing Engine _second_writeup_index.
 */
function secondWriteupIndex(sentences, item = null) {
  const product = [];
  sentences.forEach((sentence, index) => {
    if (!isListingTailSentence(sentence)) product.push([index, sentence]);
  });
  if (product.length < 2) return null;
  let identity = writeupIdentityWords(item);
  const nameSequence = item && typeof item === "object" ? copyStemSequence(itemNameCore(item)) : [];
  const stems = product.map(([, sentence]) => copyStems(sentence));
  if (!identity.size) identity = new Set(stems[0]);
  let nameSaid = nameSequence.length > 0 && sentenceSaysItemName(product[0][1], nameSequence);
  for (let position = 1; position < product.length; position += 1) {
    if (nameSequence.length) {
      if (!nameSaid && sentenceSaysItemName(product[position][1], nameSequence, true)) {
        return product[position][0];
      }
      nameSaid = nameSaid || sentenceSaysItemName(product[position][1], nameSequence);
    }
    if (position < WRITEUP_MIN_SENTENCES) continue;
    const first = stems[position];
    if (product.length - position < WRITEUP_MIN_SENTENCES || first.size < 5) continue;
    const head = new Set();
    for (const words of stems.slice(0, position)) for (const word of words) head.add(word);
    const headOwn = new Set([...head].filter((word) => !identity.has(word)));
    const again = [...first].filter((word) => head.has(word) || identity.has(word)).length;
    if (again < 4 || again / first.size < 0.34) continue;
    const tailOwn = new Set();
    for (const words of stems.slice(position)) {
      for (const word of words) if (!identity.has(word)) tailOwn.add(word);
    }
    const shared = [...headOwn].filter((word) => tailOwn.has(word)).length;
    const smaller = Math.max(1, Math.min(headOwn.size, tailOwn.size));
    let later = 0;
    for (const words of stems.slice(position + 1)) {
      const count = [...words].filter((word) => !identity.has(word) && headOwn.has(word)).length;
      if (count > later) later = count;
    }
    if (shared >= 3 && later >= 2 && shared / smaller >= 0.4) return product[position][0];
  }
  return null;
}

/** Indexes of product sentences that repeat an earlier sentence word for word. */
function repeatedSentenceIndexes(sentences) {
  const seen = new Set();
  const repeated = [];
  sentences.forEach((sentence, index) => {
    if (isListingTailSentence(sentence)) return;
    const norm = String(sentence || "").toLowerCase().replace(/\s+/g, " ").trim();
    if (norm.length <= 40) return;
    if (seen.has(norm)) repeated.push(index);
    seen.add(norm);
  });
  return repeated;
}

/**
 * True when a second product write-up follows the first, or a sentence is pasted twice.
 * One later sentence that names the same item is not a second write-up.
 */
export function descriptionHasStackedProductProse(text, item = null) {
  const blob = String(text || "").replace(/\s+/g, " ").trim();
  if (!blob) return false;
  const sentences = splitProseSentences(blob);
  return secondWriteupIndex(sentences, item) != null || repeatedSentenceIndexes(sentences).length > 0;
}

export function descriptionHasStackedWear(text) {
  const blob = String(text || "");
  if (descriptionHasStackedProductProse(blob)) return true;
  const badThis = new RegExp(
    `This item is sold AS IS, with ${WEAR_ADJ} wear consistent with prior use\\.?`,
    "i"
  );
  const badLabel = new RegExp(
    `AS IS:\\s*this item is sold AS IS,?\\s*with ${WEAR_ADJ} wear consistent with prior use`,
    "i"
  );
  if (badThis.test(blob) || badLabel.test(blob)) return true;
  const canonicalGi = new RegExp(
    `Sold AS IS with ${WEAR_ADJ} wear consistent with prior use\\s*;\\s*please review photos for the most accurate view of condition\\.?`,
    "gi"
  );
  const matches = blob.match(canonicalGi);
  if (matches && matches.length > 1) return true;
  const leftover = blob.replace(
    new RegExp(
      `Sold AS IS with ${WEAR_ADJ} wear consistent with prior use\\s*;\\s*please review photos for the most accurate view of condition\\.?`,
      "gi"
    ),
    " "
  );
  return /;?\s*(?:please\s+)?review photos for the most accurate view of condition\.?/i.test(leftover);
}

function isSpecificWearClause(text) {
  const cleaned = String(text || "").replace(/\s+/g, " ").replace(/^[\s.,;]+|[\s.,;]+$/g, "");
  if (!cleaned) return false;
  const match = cleaned.match(SPECIFIC_WEAR_NOTE_RE);
  if (!match) return false;
  const adj = String(match[1] || "").toLowerCase();
  return Boolean(adj && adj !== "visible");
}

function specificWearAdjective(notes, conditionIssues) {
  for (const raw of [...splitNoteClauses(notes), ...splitNoteClauses(conditionIssues)]) {
    const cleaned = cleanNoteClause(raw);
    if (!cleaned || isCustomerSilentNote(cleaned)) continue;
    const match = cleaned.match(SPECIFIC_WEAR_NOTE_RE);
    if (!match) continue;
    const adj = String(match[1] || "").toLowerCase();
    if (adj && adj !== "visible") return adj;
  }
  return "";
}

function wearSentenceForNotes(notes, conditionIssues, { allowStock = false } = {}) {
  const adj = specificWearAdjective(notes, conditionIssues);
  if (adj && allowStock) {
    return (
      `Sold AS IS with ${adj} wear consistent with prior use; ` +
      "please review photos for the most accurate view of condition."
    );
  }
  if (adj) return `There is ${adj} wear.`;
  if (allowStock) return STOCK_WEAR_SENTENCE;
  return "";
}

function isGenericWearClause(text) {
  const cleaned = String(text || "").replace(/\s+/g, " ").replace(/^[\s.,;]+|[\s.,;]+$/g, "");
  if (!cleaned) return false;
  // Bare as-is / visible-wear only. "noticeable wear" is NOT generic.
  if (isSpecificWearClause(cleaned)) return false;
  if (containsStockWear(cleaned)) return true;
  if (/^(?:sold\s+)?as[\s-]is$/i.test(cleaned)) return true;
  if (/^(?:sold\s+)?as[\s-]is(?:\s+with\s+visible wear(?: consistent with prior use)?)?$/i.test(cleaned)) {
    return true;
  }
  return /^(?:with\s+)?visible wear(?: consistent with prior use)?$/i.test(cleaned);
}

function dropOldRhParagraph(prose) {
  const text = String(prose || "");
  if (!/This RH curved wing\b/i.test(text)) return text;
  const rh = text.search(/This RH curved wing\b/i);
  if (
    rh >= 0 &&
    rh <= 40 &&
    !/(?:Curved wing |Grey )leather king low profile(?: platform)? bed/i.test(text.slice(0, rh))
  ) {
    const later = text.slice(rh + 1).search(/This RH curved wing\b/i);
    if (later < 0) return text;
  }
  return text.replace(OLD_RH_PARAGRAPH_RE, " ").replace(/[ \t]{2,}/g, " ").trim();
}

// The fixed sentence older builds published when they had nothing to say
// ("This bed is a straightforward piece for everyday use."). It is not a
// description: it is not counted, not kept, and not restored.
const LEGACY_FILLER_RE = /\bis a straightforward piece for everyday use\b/i;

/** True for no description, or one that is only the old fixed filler sentence. */
export function descriptionIsBlankOrFiller(text) {
  const plain = stripDimFooter(String(text || "")).replace(/\s+/g, " ").trim();
  return !splitProseSentences(plain).some(
    (part) => !LEGACY_FILLER_RE.test(part) && !SHIP_SENTENCE_RE.test(part)
  );
}

/**
 * Keep the first product write-up. A second write-up is dropped, and a sentence
 * pasted twice is kept once. A sentence that only mentions the same item again
 * is not cut.
 */
function keepOneProductParagraph(prose, item = null) {
  let text = String(prose || "").replace(/\s+/g, " ").trim();
  if (!text) return "";
  let sentences = splitProseSentences(text);
  if (sentences.some((part) => LEGACY_FILLER_RE.test(part))) {
    // The fixed sentence older builds published is not a description.
    sentences = sentences.filter((part) => !LEGACY_FILLER_RE.test(part));
    text = sentences.join(" ").replace(/\s{2,}/g, " ").trim();
    if (!text) return "";
  }
  const splitAt = secondWriteupIndex(sentences, item);
  if (splitAt != null) {
    const head = sentences.slice(0, splitAt);
    const tail = sentences.slice(splitAt).filter((part) => isListingTailSentence(part));
    sentences = [...head, ...tail];
  }
  const repeated = new Set(repeatedSentenceIndexes(sentences));
  if (splitAt == null && !repeated.size) return text;
  return sentences
    .filter((_part, index) => !repeated.has(index))
    .join(" ")
    .replace(/\s{2,}/g, " ")
    .trim();
}

function stockWearAllowed(notes, conditionIssues, sourceText) {
  const clauses = [];
  for (const raw of [...splitNoteClauses(notes), ...splitNoteClauses(conditionIssues)]) {
    const cleaned = cleanNoteClause(raw);
    if (cleaned && !isCustomerSilentNote(cleaned)) clauses.push(cleaned);
  }
  if (clauses.some((clause) => isNegatedConditionNote(clause) || positiveConditionSentence(clause))) {
    return false;
  }
  const noteBlob = clauses.join(" ");
  if (containsStockWear(noteBlob) || /\b(?:as[\s-]is|visible wear)\b/i.test(noteBlob)) return true;
  return false;
}

function enforceSingleWearSentence(prose, wearSentence = "") {
  let cleaned = stripStockWearCopies(prose);
  // Drop a dangling "There is … wear." / "Noticeable wear." so we can re-place one beat.
  cleaned = cleaned
    .replace(
      /(?:(?<=[.!?])\s+|^)(?:There is (?:noticeable|significant|heavy|moderate|extensive|substantial|light|slight|minor|normal|expected)\s+wear\.?|(?:Noticeable|Significant|Heavy|Moderate|Extensive|Substantial|Light|Slight|Minor|Normal|Expected)\s+wear\.?)\s*/gi,
      " "
    )
    .replace(/[ \t]{2,}/g, " ")
    .trim();
  const wear = String(wearSentence || "").replace(/\s+/g, " ").trim();
  if (!wear) return cleaned;
  const match = cleaned.search(SHIP_START_RE);
  if (match < 0) return cleaned ? `${cleaned} ${wear}`.trim() : wear;
  const head = cleaned.slice(0, match).replace(/\s+$/, "");
  const tail = cleaned.slice(match);
  if (head) return `${head} ${wear} ${tail}`.trim();
  return `${wear} ${tail}`.trim();
}

function asIsPresent(desc, clause) {
  const plain = String(desc || "").replace(/\s+/g, " ").toLowerCase();
  if (
    containsStockWear(desc) &&
    (containsStockWear(clause) || /\b(?:as[\s-]is|visible wear)\b/i.test(clause || ""))
  ) {
    return true;
  }
  if (
    plain.includes("sold as is") &&
    plain.includes("visible wear") &&
    /\b(?:as[\s-]is|wear)\b/i.test(clause || "")
  ) {
    return true;
  }
  return plain.includes("as is:") && plain.includes(String(clause || "").toLowerCase());
}

function isGeneratedFlawSentence(sentence) {
  return GENERATED_FLAW_SENTENCE_RE.test(String(sentence || "").replace(/\s+/g, " ").trim());
}

function isRawFlawFragmentSentence(sentence) {
  let text = String(sentence || "").replace(/\s+/g, " ").trim().replace(/\.+$/, "");
  text = text.replace(/^AS IS:\s*/i, "").replace(/,?\s*as shown in the photos$/i, "").trim();
  if (!text || isGeneratedFlawSentence(text)) return false;
  return Boolean(naturalFlawSentence(text));
}

function stripUnsupportedFlawSentences(prose, supportedSentences) {
  const keep = new Set(
    (supportedSentences || []).map((sentence) => String(sentence || "").replace(/\s+/g, " ").trim().toLowerCase().replace(/\.+$/, ""))
  );
  const supportedText = [...keep].join(" ");
  const kept = [];
  for (const part of String(prose || "").split(/(?<=[.!?])\s+/)) {
    const piece = part.trim();
    if (!piece) continue;
    if (/^AS IS\s*:/i.test(piece)) continue;
    if (isRawFlawFragmentSentence(piece)) continue;
    if (/\b(?:smallt|smalli|smalt)\b/i.test(piece)) continue;
    if (/\bcrack\b/i.test(piece) && !supportedText.includes("crack")) continue;
    if (isGeneratedFlawSentence(piece) && !keep.has(piece.toLowerCase().replace(/\.+$/, ""))) continue;
    kept.push(piece);
  }
  return kept.join(" ").replace(/\s{2,}/g, " ").trim();
}

const PROFESSIONAL_EXCELLENT = "This piece is in excellent condition.";

/** A positive condition note is one shopper sentence, never an AS IS paste. */
export function positiveConditionSentence(note) {
  let text = String(note || "").replace(/\s+/g, " ").replace(/^[\s.,;]+|[\s.,;]+$/g, "");
  text = text.replace(/^(?:it is|this piece is|the piece is|piece is)\s+/i, "").trim();
  if (/^(?:in\s+)?excellent\s+condition$/i.test(text)) return PROFESSIONAL_EXCELLENT;
  return "";
}

function stripRawExcellentPastes(prose) {
  const kept = [];
  for (const part of String(prose || "").split(/(?<=[.!?])\s+/)) {
    const piece = part.trim();
    if (!piece) continue;
    if (/^(?:this piece is in\s+)?(?:in\s+)?excellent\s+condition\.?$/i.test(piece)) continue;
    kept.push(piece);
  }
  return kept.join(" ").replace(/\s{2,}/g, " ").trim();
}

function isTestJunkNote(note) {
  const words = String(note || "").match(/[A-Za-z]+/g) || [];
  return words.length > 0 && words.every((word) => word.toLowerCase() === "test");
}

const CHATTER_WORDS = new Set([
  "hello", "hi", "hey", "yo", "sup", "howdy", "goodbye", "bye", "test", "testing", "tests",
  "asdf", "asdfg", "asdfgh", "foo", "bar", "blah", "ok", "okay", "xxx", "yyy", "zzz",
  "na", "none", "nothing", "blank", "empty", "lol", "lmao", "haha", "hmm", "uh", "um",
  "idk", "nope", "nah", "yep", "yeah", "thanks", "thank", "whatever",
]);
const CHATTER_FILLER = new Set(["a", "an", "the", "there", "just", "you", "please", "pls"]);
const PRODUCT_TITLE_WORDS = new Set([
  "nightstand", "nightstands", "dresser", "dressers", "bed", "beds", "sofa", "sofas",
  "chair", "chairs", "table", "tables", "painting", "paintings", "oil", "canvas",
  "landscape", "mirror", "mirrors", "lamp", "lamps", "rug", "rugs", "desk", "desks",
  "cabinet", "cabinets", "console", "consoles", "art", "frame", "frames", "wood",
  "wooden", "oak", "walnut", "mahogany", "metal", "glass", "ceramic", "vintage",
  "antique", "original", "pair", "set", "stool", "stools", "bench", "benches",
  "bookcase", "bookcases", "sideboard", "buffet", "armchair", "recliner",
  "sectional", "loveseat", "coffee", "end", "side", "dining", "bedroom", "living",
  "office", "outdoor", "patio", "modern", "midcentury", "century", "finish",
  "black", "white", "brown", "blue", "green", "gold", "silver", "brass",
  "drawer", "drawers", "door", "doors", "shelf", "shelves",
]);
const NEGATED_NOTE_RE = /^(?:there(?:'s| is| are) )?(?:absolutely |definitely )?(?:no|not|without|none|zero|free of)\b(?:\s+\w+){0,6}\s*(?:scratch(?:es|ed)?|crack(?:s|ed)?|damage[d]?|flaw(?:s|ed)?|chip(?:s|ped)?|stain(?:s|ed)?|dent(?:s|ed)?|scuff(?:s|ed)?|nick(?:s|ed)?|blemish(?:es)?|tear(?:s|ed)?|wear|mark(?:s|ed)?|imperfection(?:s)?)\b/i;
const NEGATED_FLAW_SENTENCE_RE = /^there (?:is|are) no\b.{0,80}\b(?:scratch(?:es)?|crack(?:s)?|damage|flaw(?:s)?|chip(?:s)?|stain(?:s)?|dent(?:s)?|scuff(?:s)?|nick(?:s)?|blemish(?:es)?|tear(?:s)?|wear|mark(?:s)?)\b/i;
const META_COMMENTARY_RE = /(?:\bbased on (?:the )?(?:\w+\s+){0,4}(?:name|title|seller|information|data)\b|\battribution is based\b|\bmaker attribution\b|\bno additional\b.{0,80}\b(?:stated|provided|listed|given)\b|\b(?:are|were|is) (?:not )?(?:stated|provided|listed) here\b|\bas described\b|\baccording to the (?:title|name|seller|listing)\b|\b(?:the )?(?:name|title) (?:suggests|provided|indicates|implies)\b)/i;

function noteWords(note) {
  return (String(note || "").replace(/^[\s.,;:!?"'`]+|[\s.,;:!?"'`]+$/g, "").match(/[A-Za-z']+/g) || []).map((word) =>
    word.toLowerCase()
  );
}

function isChatterNote(note) {
  const words = noteWords(note);
  if (!words.length) return false;
  return words.some((word) => CHATTER_WORDS.has(word)) && words.every((word) => CHATTER_WORDS.has(word) || CHATTER_FILLER.has(word));
}

export function isNegatedConditionNote(note) {
  const text = String(note || "").replace(/\s+/g, " ").replace(/^[\s.,;:!?"'`]+|[\s.,;:!?"'`]+$/g, "");
  return Boolean(text) && NEGATED_NOTE_RE.test(text);
}

function isCustomerSilentNote(note) {
  const text = String(note || "").replace(/\s+/g, " ").replace(/^[\s.,;:!?"'`]+|[\s.,;:!?"'`]+$/g, "");
  if (!text) return false;
  if (isTestJunkNote(text) || isChatterNote(text) || isNegatedConditionNote(text)) return true;
  if (naturalFlawSentence(text) || positiveConditionSentence(text)) return false;
  if (clauseMentionsConditionProblem(text)) return false;
  if (/\bcoming\b/i.test(text) && /\bapart\b/i.test(text)) return false;
  if (POSITIVE_CONDITION_RE.test(text)) return false;
  if (!clauseMentionsCondition(text)) return true;
  return noteWords(text).length === 1;
}

export function untrustedTitleTokens(title) {
  const lowered = (String(title || "").match(/[A-Za-z][A-Za-z']*/g) || []).map((word) => word.toLowerCase());
  const junk = new Set(lowered.filter((word) => word === "test" || word === "testing" || word === "tests"));
  if (!junk.size) return junk;
  for (const word of lowered) {
    if (junk.has(word) || PRODUCT_TITLE_WORDS.has(word) || CHATTER_FILLER.has(word)) continue;
    junk.add(word);
  }
  return junk;
}

const BLANK_BRANDS = new Set(["", "unbranded", "nobrand", "no-brand", "non-branded", "nonbranded", "none", "n/a", "na", "unknown"]);
const TEST_WORDS = new Set(["test", "testing", "tests"]);
const TAXONOMY_TAGS = new Set(["fh", "lg", "a", "b", "c", "d", "e", "f", "g", "h", "l", "m", "o", "p", "r", "s", "t", "w", "x"]);
const TAG_FILLER = new Set(["on", "of", "and", "the", "a", "an", "for", "with", "in"]);
const KIND_ALT = "nightstands?|dressers?|beds?|sofas?|chairs?|tables?|paintings?|mirrors?|lamps?|rugs?|desks?|cabinets?|consoles?|stools?|benches?|bookcases?|sideboards?|buffets?|armchairs?|recliners?|sectionals?|loveseats?";
const SAFE_MAKER_WORDS = new Set([
  ...PRODUCT_TITLE_WORDS,
  ...CHATTER_FILLER,
  ...TAG_FILLER,
  "this", "its", "our", "one", "two", "three", "four", "five", "six",
  "compact", "small", "large", "tall", "short", "round", "square", "oval",
  "french", "italian", "american", "danish", "chinese", "japanese", "english",
  "victorian", "colonial", "industrial", "rustic", "coastal", "contemporary",
  "traditional", "beautiful", "handsome", "sturdy", "solid", "single",
  "double", "painted", "carved", "distressed", "refinished", "upholstered",
  "tufted", "clean", "simple", "classic", "sleek", "minimal", "functional",
  "from", "as", "is", "new", "old", "used",
  "zinc", "leather", "platform", "accent", "floor", "wood", "oak",
  "walnut", "marble", "glass", "metal", "brass", "steel", "iron",
  "velvet", "linen", "wool", "rattan", "wicker", "grey", "gray",
  "black", "white", "brown", "blue", "green", "red", "king", "queen",
  "curved", "wing", "low", "high", "modern", "vintage",
]);

export function explicitBrandTokens(brand) {
  const text = String(brand || "").trim().toLowerCase();
  if (BLANK_BRANDS.has(text)) return new Set();
  return new Set((text.match(/[A-Za-z][A-Za-z']*/g) || []).map((word) => word.toLowerCase()));
}

export function unsupportedMakerTokens(description = "", title = "", brand = "") {
  const allowed = explicitBrandTokens(brand);
  const found = new Set();
  const words = `${title}\n${description}`.match(/[A-Za-z][A-Za-z']*/g) || [];
  for (const word of words) {
    const token = word.toLowerCase();
    if (TEST_WORDS.has(token) && !allowed.has(token)) found.add(token);
  }
  const kindRe = new RegExp(`(?:^|(?<=[.!?]\\s))([A-Za-z][A-Za-z']{2,})\\s+(?:${KIND_ALT})\\b`, "gi");
  for (const match of String(description || "").matchAll(kindRe)) {
    const token = match[1].toLowerCase();
    if (allowed.has(token) || SAFE_MAKER_WORDS.has(token) || TEST_WORDS.has(token)) continue;
    found.add(token);
  }
  return found;
}

export function buyerTitlePhrase(title, drop = []) {
  let text = String(title || "").replace(
    /(?:\s*[-–—]\s*)?\d+(?:\.\d+)?(?:\s*[xX×]\s*\d+(?:\.\d+)?){1,2}\s*H?\b(?:\s*[-–—]\s*AS\s*-?\s*IS)?\s*$/i,
    ""
  );
  text = text.replace(/\bAS\s*-?\s*IS\b/gi, "");
  const skip = new Set([...drop].map((word) => String(word).toLowerCase()).concat([...TEST_WORDS]));
  const words = (text.match(/[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*/g) || []).filter((raw) => !skip.has(raw.toLowerCase()));
  if (!words.length) {
    const kind = productKindFromTitle(title) || "piece";
    return `This ${kind}`;
  }
  const phrase = words.join(" ");
  return phrase.slice(0, 1).toUpperCase() + phrase.slice(1).toLowerCase();
}

export function proseHasInventedMaker(description = "", brand = "") {
  const allowed = explicitBrandTokens(brand);
  const text = String(description || "");
  for (const word of text.match(/[A-Za-z][A-Za-z']*/g) || []) {
    const token = word.toLowerCase();
    if (TEST_WORDS.has(token) && !allowed.has(token)) return true;
  }
  const kindRe = new RegExp(`(?:^|(?<=[.!?]\\s))([A-Za-z][A-Za-z']{2,})\\s+(?:${KIND_ALT})\\b`, "gi");
  for (const match of text.matchAll(kindRe)) {
    const token = match[1].toLowerCase();
    if (allowed.has(token) || SAFE_MAKER_WORDS.has(token) || TEST_WORDS.has(token)) continue;
    return true;
  }
  return false;
}

export function stripInventedMakers(prose, title = "", brand = "") {
  const text = String(prose || "").replace(/\s{2,}/g, " ").trim();
  if (!text || !proseHasInventedMaker(text, brand)) return text;
  const kept = text
    .split(/(?<=[.!?])\s+/)
    .map((part) => part.trim())
    .filter((part) => part && !proseHasInventedMaker(part, brand));
  return kept.join(" ").trim();
}

export function freshListingTags({
  name = "",
  brand = "",
  websiteCategory = "",
  merchType = "furniture",
} = {}) {
  const codes = structuredEcommerceCodes(merchType, websiteCategory);
  const seen = new Set(codes.map((code) => code.toLowerCase()));
  const words = [];
  const text = String(name || "")
    .replace(/(?:\s*[-–—]\s*)?\d+(?:\.\d+)?(?:\s*[xX×]\s*\d+(?:\.\d+)?){1,2}\s*H?\b(?:\s*[-–—]\s*AS\s*-?\s*IS)?\s*$/i, "")
    .replace(/\bAS\s*-?\s*IS\b/gi, "");
  const allowed = explicitBrandTokens(brand);
  for (const raw of text.match(/[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*/g) || []) {
    const low = raw.toLowerCase();
    if (seen.has(low) || TEST_WORDS.has(low) || TAG_FILLER.has(low)) continue;
    const parts = low.split(/[^a-z0-9]+/).filter(Boolean);
    if (parts.some((part) => PRODUCT_TITLE_WORDS.has(part) || allowed.has(part))) {
      words.push(low);
      seen.add(low);
    }
  }
  for (const token of [...allowed].sort()) {
    if (!seen.has(token) && !TEST_WORDS.has(token)) {
      words.push(token);
      seen.add(token);
    }
  }
  return [...codes, ...words].join(", ");
}

function tagSegments(tag) {
  return String(tag || "")
    .toLowerCase()
    .split(/[^a-z0-9]+/)
    .filter((part) => part && !TAG_FILLER.has(part));
}

export function tagMakerTokens(tags, title = "", brand = "", category = "", description = "") {
  const found = unsupportedMakerTokens(description, title, brand);
  const grounded = new Set(
    `${title} ${brand} ${category}`.match(/[A-Za-z][A-Za-z']*/g)?.map((word) => word.toLowerCase()) || []
  );
  for (const token of explicitBrandTokens(brand)) grounded.add(token);
  for (const token of TEST_WORDS) grounded.delete(token);
  for (const tag of String(tags || "").split(/[,;\n]+/)) {
    const segments = tagSegments(tag);
    if (!segments.length) continue;
    for (const part of segments) {
      if (TEST_WORDS.has(part)) found.add(part);
    }
    const head = segments[0];
    const headKnown =
      grounded.has(head) ||
      PRODUCT_TITLE_WORDS.has(head) ||
      SAFE_MAKER_WORDS.has(head) ||
      TAXONOMY_TAGS.has(head) ||
      explicitBrandTokens(brand).has(head);
    const kindRe = new RegExp(`^(?:${KIND_ALT})$`, "i");
    if (segments.length === 1 && !headKnown && !/^\d+$/.test(head)) found.add(head);
    else if (segments.length > 1 && !headKnown && segments.slice(1).some((part) => kindRe.test(part))) found.add(head);
  }
  return found;
}

export function stripMakerTags(tags, title = "", brand = "", category = "", description = "") {
  const tokens = tagMakerTokens(tags, title, brand, category, description);
  if (!tokens.size) return String(tags || "").trim();
  return String(tags || "")
    .split(/[,;\n]+/)
    .map((tag) => tag.trim())
    .filter((tag) => tag && !tagSegments(tag).some((part) => tokens.has(part)))
    .join(", ");
}

function splitProseSentences(prose) {
  return String(prose || "")
    .split(/(?<=[.!?])\s+/)
    .map((part) => part.trim())
    .filter(Boolean);
}

function stripMetaCommentary(prose) {
  return splitProseSentences(prose)
    .filter((part) => !META_COMMENTARY_RE.test(part))
    .join(" ")
    .replace(/\s{2,}/g, " ")
    .trim();
}

function dropUntrustedTitleClaims(prose, item) {
  const title = noteItemGet(item, ["name"]) || "";
  const brand = noteItemGet(item, ["brand"]) || "";
  const junk = untrustedTitleTokens(title);
  const cleaned = splitProseSentences(prose)
    .filter((part) => {
      if (META_COMMENTARY_RE.test(part)) return false;
      if (!junk.size) return true;
      return ![...junk].some((token) => new RegExp(`\\b${token.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\b`, "i").test(part));
    })
    .join(" ")
    .replace(/\s{2,}/g, " ")
    .trim();
  return stripInventedMakers(cleaned, title, brand);
}

function productKindFromTitle(title) {
  const blob = String(title || "").toLowerCase();
  const kinds = ["nightstand", "dresser", "painting", "mirror", "sofa", "chair", "table", "desk", "lamp", "rug", "bed"];
  return kinds.find((kind) => new RegExp(`\\b${kind}s?\\b`).test(blob)) || "";
}

/**
 * A short paragraph from recorded fields only. Nothing is invented.
 *
 * Used when there is no description and the model gave nothing usable. It says
 * the item's own name (which carries the type, the artist or maker, the medium
 * or material, and the edition as staff recorded them), the Brand field when
 * the name does not already say it, and the category when the name does not
 * already say it. The size sentence and the Dimensions and Weight line are
 * added by the listing build. Same text as Pricing Engine factual_listing_copy.
 */
export function factualListingCopy(item) {
  const source = item && typeof item === "object" ? item : {};
  const title = String(noteItemGet(source, ["name", "title"]) || "");
  const junk = new Set([...untrustedTitleTokens(title), ...TEST_WORDS]);
  const core = itemNameCore(source).replace(new RegExp(SIZE_TOKEN_IN_PROSE_RE.source, "g"), " ");
  let name = core
    .split(/\s+/)
    .filter((word) => word && !junk.has(word.toLowerCase().replace(/[^a-z']/g, "")))
    .join(" ");
  name = name.replace(/\s+[-–—]+\s+/g, ", ").replace(/^[\s,;:.\-–—]+|[\s,;:.\-–—]+$/g, "");
  let brand = String(noteItemGet(source, ["brand"]) || "").trim();
  if (BLANK_BRANDS.has(brand.toLowerCase())) brand = "";
  const sentences = [];
  const said = copyStems(name);
  if (name) {
    let lead = name;
    const brandWords = copyStems(brand);
    const brandSaid = [...brandWords].every((word) => said.has(word));
    if (brand && !brandSaid && !name.toLowerCase().includes(brand.toLowerCase())) {
      lead = `${name} by ${brand}`;
      for (const word of brandWords) said.add(word);
    }
    sentences.push(`${lead.slice(0, 1).toUpperCase()}${lead.slice(1)}.`);
  } else if (brand) {
    sentences.push(`${brand}.`);
  }
  const category = String(noteItemGet(source, ["category"]) || "")
    .replace(/\s+/g, " ")
    .replace(/^[\s.]+|[\s.]+$/g, "");
  if (category && /[A-Za-z]{3}/.test(category) && ![...copyStems(category)].some((word) => said.has(word))) {
    const label = category
      .split(" ")
      .map((part) =>
        /^[A-Za-z]+$/.test(part) ? `${part.slice(0, 1).toUpperCase()}${part.slice(1).toLowerCase()}` : part
      )
      .join(" ");
    sentences.push(`It is listed under ${label}.`);
  }
  return sentences.join(" ").trim();
}

/** Factual sentence from the recorded fields. The old filler sentence is gone. */
function mechanicalListingCopy(title, item = null) {
  return factualListingCopy(item && typeof item === "object" ? item : { name: title });
}

// One definition of "too thin", shared with Pricing Engine (listing_copy_is_thin).
export const LISTING_MIN_PRODUCT_SENTENCES = 2;
export const LISTING_MIN_PRODUCT_CHARS = 80;

/**
 * Sentences that describe the product.
 *
 * Not counted: the Dimensions and Weight line, the pickup and delivery close,
 * condition and AS IS sentences, comments about the listing data, and a later
 * sentence that only states the size (the listing build writes that one).
 */
export function productSentences(text) {
  const plain = stripDimFooter(
    String(text || "")
      .replace(/<br\s*\/?>|<\/p>/gi, "\n")
      .replace(/<[^>]+>/g, " ")
      .replace(/\n*Dimensions:\s*[^\n]*/gi, "")
      .replace(/\n*Weight:\s*[\d.]+\s*lb\.?\s*/gi, "")
  )
    .replace(/\s+/g, " ")
    .trim();
  const kept = [];
  for (const part of splitProseSentences(plain)) {
    if (SHIP_SENTENCE_RE.test(part) || META_COMMENTARY_RE.test(part) || LEGACY_FILLER_RE.test(part)) continue;
    if (
      isListingTailSentence(part) ||
      isWovenConditionSentence(part) ||
      WHOLE_CONDITION_SENTENCE_RE.test(part)
    ) {
      continue;
    }
    if (kept.length && sentenceHasSizeOrDims(part)) continue;
    if (!copyWords(part).size) continue;
    kept.push(part);
  }
  return kept;
}

/** Too thin to publish as the description: under two product sentences or 80 characters. */
export function listingCopyIsThin(text) {
  const sentences = productSentences(text);
  return (
    sentences.length < LISTING_MIN_PRODUCT_SENTENCES ||
    sentences.join(" ").length < LISTING_MIN_PRODUCT_CHARS
  );
}

export function listingCopyIsPublishable(text) {
  return !listingCopyIsThin(text);
}

/**
 * Strip meta-commentary and junk title claims. Regenerate once, then fall back.
 *
 * When the model's text is rejected, the item's previous description is kept
 * (previous). An item with none gets a factual paragraph from its recorded
 * fields. No filler sentence is written.
 */
export function settleBuyerListingCopy(
  description,
  title = "",
  regenerate = null,
  brand = "",
  previous = "",
  item = null
) {
  const source = item && typeof item === "object" ? item : { name: title, brand };
  const clean = (text) => dropUntrustedTitleClaims(marketingListingBase(text, source), source);
  const fallback = () => {
    const kept = String(previous || "").trim() ? clean(previous) : "";
    return kept || factualListingCopy(source);
  };
  let cleaned = clean(description);
  let regenerated = false;
  if (!listingCopyIsPublishable(cleaned) && typeof regenerate === "function") {
    regenerated = true;
    cleaned = clean(regenerate() || "");
  }
  let flagged = false;
  if (!listingCopyIsPublishable(cleaned)) {
    flagged = true;
    cleaned = fallback();
  }
  const swept = dropUntrustedTitleClaims(cleaned, source);
  if (swept !== cleaned) {
    cleaned = swept || factualListingCopy(source);
    flagged = true;
  }
  return {
    description: cleaned,
    flagged,
    regenerated,
    copyFlag: flagged ? "withheld_meta_commentary" : "",
  };
}

function isWovenConditionSentence(piece) {
  const text = String(piece || "").replace(/\s+/g, " ").trim();
  if (!text) return false;
  if (/^AS IS\s*:/i.test(text)) return true;
  if (/^(?:this piece is in\s+)?(?:in\s+)?excellent\s+condition\.?$/i.test(text)) return true;
  if (isGeneratedFlawSentence(text) || isRawFlawFragmentSentence(text)) return true;
  if (/\b(?:smallt|smalli|smalt)\b/i.test(text)) return true;
  if (/^the .+ is coming(?:\s+\w+){0,4} apart\b/i.test(text)) return true;
  if (NEGATED_FLAW_SENTENCE_RE.test(text)) return true;
  return isTestJunkNote(text) || isChatterNote(text);
}

const WHOLE_CONDITION_SENTENCE_RE =
  /^(?:please note\b.*|(?:it|this)(?:\s+\w+){0,4}\s+(?:shows|has|comes with)\b.*\b(?:wear|scratch|chip|crack|stain|dent|scuff)\b.*|.*\bwear consistent with\b.*)$/i;

function dropWholeConditionSentences(prose) {
  const kept = [];
  for (const part of splitProseSentences(prose)) {
    const piece = part.trim();
    if (!piece) continue;
    if (WHOLE_CONDITION_SENTENCE_RE.test(piece)) continue;
    if (/^Please note,?\.$/i.test(piece) || /^Age\.$/i.test(piece)) continue;
    kept.push(piece);
  }
  return kept.join(" ").trim();
}

function stripWovenConditionSentences(prose) {
  const kept = [];
  for (const part of String(prose || "").split(/(?<=[.!?])\s+/)) {
    const piece = part.trim();
    if (!piece || isWovenConditionSentence(piece)) continue;
    kept.push(piece);
  }
  return kept.join(" ").replace(/\s{2,}/g, " ").trim();
}

const INTERNAL_NOTE_PARAPHRASE_RE =
  /^(?:additional\s+information|artwork\s+in\s+(?:the\s+)?folder|item\s+is\s+in\s+(?:the\s+)?warehouse|see\s+(?:staff|office)|hold\s+for\s+\w+|call\s+\w+(?:\s+before\s+selling)?)\.?$/i;

function stripInternalNoteParaphrases(prose) {
  const kept = [];
  for (const part of splitProseSentences(prose)) {
    const piece = String(part || "").trim();
    if (!piece) continue;
    if (INTERNAL_NOTE_PARAPHRASE_RE.test(piece)) continue;
    kept.push(piece);
  }
  return kept.join(" ").replace(/\s{2,}/g, " ").trim();
}

/** Marketing copy only. Condition sentences and shipping are composed at write time. */
export function marketingListingBase(description, item = null) {
  let prose = stripDimFooter(description).replace(/\s*\n+\s*/g, " ").replace(/\s{2,}/g, " ").trim();
  // Exactly one product paragraph. A glued older rewrite is dropped first.
  // item tells the second write-up check which words are the item's own name, brand, and category.
  prose = keepOneProductParagraph(prose, item);
  prose = stripShippingSentences(prose);
  prose = stripAsIsDumps(prose);
  prose = stripStockWearCopies(prose);
  prose = dropWholeConditionSentences(prose);
  prose = stripWovenConditionSentences(prose);
  prose = stripInternalNoteParaphrases(prose);
  return stripMetaCommentary(prose);
}

function isConditionSentence(raw, sentence) {
  if (sentence === PROFESSIONAL_EXCELLENT) return true;
  if (naturalFlawSentence(raw)) return true;
  if (isGeneratedFlawSentence(sentence)) return true;
  if (/^the .+ is coming(?:\s+\w+){0,4} apart\b/i.test(sentence || "")) return true;
  return false;
}

function stripDimFooter(text) {
  return String(text || "")
    .replace(/\n*Dimensions:\s*.*$/i, "")
    .replace(/\n*Weight:\s*[\d.]+\s*lb\.?\s*$/i, "")
    .trim();
}

function stripShippingSentences(text) {
  return String(text || "")
    .trim()
    .split(/(?<=[.!?])\s+/)
    .filter((part) => part && !SHIP_SENTENCE_RE.test(part))
    .join(" ")
    .trim();
}

function removeRawNote(desc, raw) {
  const rawNorm = String(raw || "").replace(/\s+/g, " ").replace(/^[\s.,;]+|[\s.,;]+$/g, "");
  if (!rawNorm) return desc;
  const pattern = rawNorm.replace(/[.*+?^${}()|[\]\\]/g, "\\$&").replace(/ /g, "\\s+");
  return String(desc || "")
    .replace(new RegExp(`(?:(?<=^)|(?<=[.!?]\\s))\\s*${pattern}\\s*[.!?]?`, "i"), " ")
    .replace(/\s{2,}/g, " ")
    .trim();
}

function noteAlreadyPresent(desc, raw, sentence) {
  const plain = String(desc || "").replace(/\s+/g, " ").toLowerCase();
  const sent = String(sentence || "").toLowerCase().replace(/\.$/, "");
  if (sent && plain.includes(sent)) return true;
  const rawNorm = String(raw || "").replace(/\s+/g, " ").replace(/^[\s.,;]+|[\s.,;]+$/g, "").toLowerCase();
  if (rawNorm && plain.includes(rawNorm)) return true;
  const words = (rawNorm.match(/[a-z0-9']+/g) || []).filter((word) => word.length >= 4);
  if (words.length < 2) return false;
  return plain.split(/(?<=[.!?])\s+/).some((piece) =>
    words.every((word) => new RegExp(`\\b${word}\\b`).test(piece))
  );
}

function listingCopyMode(item) {
  const tags = String(noteItemGet(item, ["ecommerceTags", "ecommerce_tags", "tags"]) || "");
  const merch = String(noteItemGet(item, ["merchType", "merch_type"]) || "").toLowerCase();
  const name = String(noteItemGet(item, ["name"]) || "");
  const codes = new Set(
    tags.split(/[,;\s]+/).map((part) => part.trim().toUpperCase()).filter(Boolean)
  );
  const jewelry =
    ["NK", "ER", "BR", "RG", "OJ"].some((code) => codes.has(code)) ||
    /\b(?:earring|necklace|bracelet|brooch|parure|jewell?ery)\b/i.test(name);
  const luxury = merch === "luxury" || merch === "jewelry" || merch === "lg" || codes.has("LG");
  if (jewelry && (luxury || !codes.has("FH"))) return jewelry ? "jewelry" : "luxury";
  if (
    luxury ||
    (!codes.has("FH") && /\b(?:handbag|purse|tote|crossbody|wallet|scarf|belt|clutch)\b/i.test(name))
  ) {
    return jewelry ? "jewelry" : "luxury";
  }
  return "furniture";
}

/** Longest side in inches for shipping thresholds (feet converted via footer units). */
export function longestListingDim(item) {
  const nested = noteItemGet(item, ["dims"]) || {};
  const width =
    noteNumber(noteItemGet(item, ["dimWidth", "dim_width", "width"])) ||
    noteNumber(nested.width ?? nested.dimWidth);
  const depth =
    noteNumber(noteItemGet(item, ["dimLength", "dimDepth", "dim_length", "dim_depth", "depth"])) ||
    noteNumber(nested.depth ?? nested.dimDepth ?? nested.length ?? nested.dimLength);
  const height =
    noteNumber(noteItemGet(item, ["dimHeight", "dim_height", "height"])) ||
    noteNumber(nested.height ?? nested.dimHeight);
  const unitSrc = attachListingDimMeta(
    {
      width,
      depth,
      height,
      dimWidthUnit: noteItemGet(item, ["dimWidthUnit", "dim_width_unit"]),
      dimDepthUnit: noteItemGet(item, [
        "dimDepthUnit",
        "dim_depth_unit",
        "dimLengthUnit",
        "dim_length_unit",
      ]),
      dimHeightUnit: noteItemGet(item, ["dimHeightUnit", "dim_height_unit"]),
    },
    item
  );
  const { widthUnit, depthUnit, heightUnit } = resolvedAxisUnits(unitSrc);
  return Math.max(
    measureAsInches(width, widthUnit),
    measureAsInches(depth, depthUnit),
    measureAsInches(height, heightUnit)
  );
}

function listingWeightLb(item) {
  const direct = noteNumber(noteItemGet(item, ["weightLbs", "weight_lbs", "weight"]));
  if (direct) return direct;
  const dims = noteItemGet(item, ["dims"]) || {};
  return noteNumber(dims.weightLbs ?? dims.weight);
}

export function shippingSentenceFor(item) {
  if (listingCopyMode(item) === "luxury" || listingCopyMode(item) === "jewelry") return SHIPPING_LUXURY;
  const name = String(noteItemGet(item, ["name"]) || "");
  const cues = `${name} ${noteItemGet(item, ["category"]) || ""}`;
  const longest = longestListingDim(item);
  const weight = listingWeightLb(item);
  if (weight > 50 || longest > 42 || BULKY_NAME_RE.test(cues)) return SHIPPING_BULKY;
  if (NOT_FURNITURE_RE.test(name)) return SHIPPING_PARCEL;
  if (FURNITURE_NAME_RE.test(cues) && (longest >= 36 || weight > 25)) return SHIPPING_BULKY;
  return SHIPPING_PARCEL;
}

function withShippingSentence(prose, item) {
  const cleaned = stripShippingSentences(prose);
  if (!cleaned) return "";
  return `${periodNote(cleaned)} ${shippingSentenceFor(item)}`.trim();
}

const SIZE_TOKEN_IN_PROSE_RE =
  /\b\d+(?:\.\d+)?\s*[xX×]\s*\d+(?:\.\d+)?(?:\s*[xX×]\s*\d+(?:\.\d+)?)?\s*H?\b/;
const SIZE_BY_IN_PROSE_RE =
  /\b\d+(?:\.\d+)?(?:\s*(?:inches|inch|in\.?|feet|foot|ft\.?))?\s+by\s+\d+(?:\.\d+)?(?:\s*(?:inches|inch|in\.?|feet|foot|ft\.?))?(?:\s+by\s+\d+(?:\.\d+)?(?:\s*(?:inches|inch|in\.?|feet|foot|ft\.?))?)?\b/i;
const SIZE_AXIS_IN_PROSE_RE =
  /\b\d+(?:\.\d+)?\s*(?:(?:"|inches|inch|in\.?|feet|foot|ft\.?)\s*)?(?:wide|deep|high|tall|long)\b/i;
const SIZE_FILLER_IN_PROSE_RE =
  /\bin\s+a\s+(?:[\w/-]+\s+){0,6}(?:room[- ]ready\s+)?size\b|\broom[- ]ready\s+size\b|\bin\s+a\s+versatile(?:\s*,\s*room[- ]ready)?\s+size\b|\bin\s+a\s+format\b/i;
const SIZE_MEASURES_RE =
  /\b(?:it\s+)?measures\s+\d+(?:\.\d+)?(?:\s*inches?)?\s+by\s+\d+(?:\.\d+)?(?:\s*inches?)?|\b(?:it\s+)?measures\s+\d+(?:\.\d+)?\s*inches?\s+(?:wide|deep|high|tall|long)\b|\bthis\s+is\s+an?\s+\d+(?:\.\d+)?\s+by\s+\d+(?:\.\d+)?\s+rug\b/i;
const DANGLING_SIZE_PHRASE_RE =
  /\bin\s+a\s+format\b|\bmeasures\b(?=\s*(?:and\b|,|\.|$))|\bat\s+(?=,|\.|$)/i;
const WEAR_OR_AS_IS_SENT_RE =
  /\b(?:sold\s+)?AS\s*-?\s*IS\b|\bnoticeable\s+wear\b|\bvisible\s+wear\b|\bthere\s+is\s+a\b|\bthere\s+are\b|\bplease\s+review\s+photos\b/i;

/** True when a sentence carries a size token, axis measure, or size-filler clause. */
export function sentenceHasSizeOrDims(sentence) {
  const text = String(sentence || "");
  if (!text.trim()) return false;
  return (
    SIZE_TOKEN_IN_PROSE_RE.test(text) ||
    SIZE_BY_IN_PROSE_RE.test(text) ||
    SIZE_AXIS_IN_PROSE_RE.test(text) ||
    SIZE_FILLER_IN_PROSE_RE.test(text) ||
    SIZE_MEASURES_RE.test(text) ||
    /\b(?:width|depth|height|length)\s*[:=]\s*\d/i.test(text)
  );
}

function stripSizeNoiseFromSentence(sentence) {
  let text = String(sentence || "");
  text = text.replace(SIZE_FILLER_IN_PROSE_RE, " ");
  text = text.replace(SIZE_MEASURES_RE, " ");
  text = text.replace(SIZE_TOKEN_IN_PROSE_RE, " ");
  text = text.replace(SIZE_BY_IN_PROSE_RE, " ");
  text = text.replace(SIZE_AXIS_IN_PROSE_RE, " ");
  text = text.replace(/\b(?:width|depth|height|length)\s*[:=]\s*[\d.]+\s*"?/gi, " ");
  text = text.replace(DANGLING_SIZE_PHRASE_RE, " ");
  text = text.replace(/\s*[-–—]\s*/g, " ");
  text = text.replace(/\s{2,}/g, " ");
  text = text.replace(/\s+([,;:])/g, "$1");
  text = text.replace(/^[,\s;:]+/, "");
  text = text.replace(/\s+,/g, ",");
  text = text.replace(/,\s*,+/g, ",");
  text = text.replace(/\bin\s+for\b/gi, "for");
  text = text.replace(/\s+\band\b(?=\s+(?:for|with|that|which|bringing|brings)\b)/gi, " ");
  text = text.replace(/\s{2,}/g, " ").replace(/^[,;\s]+|[,;\s]+$/g, "");
  if (text && !/[.!?]$/.test(text)) text += ".";
  if (text && /^[a-z]/.test(text)) text = text[0].toUpperCase() + text.slice(1);
  return text;
}

function normSizeWords(text) {
  return String(text || "")
    .toLowerCase()
    .match(/[a-z0-9]+/g) || [];
}

function titleCorePhrase(item = null) {
  const name = String(noteItemGet(item, ["name"]) || "");
  const cleaned = name.replace(SIZE_TOKEN_IN_PROSE_RE, " ").replace(/\s*[-–—]\s*/g, " ");
  return buyerTitlePhrase(cleaned);
}

export function openingIsTitleRestatement(opening, item = null) {
  const text = String(opening || "").replace(/\s+/g, " ").trim().replace(/[.!?]+$/, "");
  if (!text) return true;
  const title = String(noteItemGet(item, ["name"]) || "");
  const core = String(titleCorePhrase(item) || "").replace(/\s+/g, " ").trim().replace(/[.!?]+$/, "");
  const openWords = normSizeWords(text);
  const coreWords = normSizeWords(core);
  if (!openWords.length) return true;
  const junk = untrustedTitleTokens(title);
  const hasJunk = Boolean(junk.size && openWords.some((word) => junk.has(word)));
  const placeTail = (remainder) => {
    if (!remainder.length || !["for", "in"].includes(remainder[0]) || remainder.length > 6) {
      return false;
    }
    return /^(?:for|in)\s+(?:a\s+|an\s+|the\s+)?(?:primary\s+|guest\s+)?(?:bedrooms?|living\s+rooms?|dining(?:\s+rooms?)?|hallways?|entries|entryways?|suites?|homes?|houses?|rooms?)\b/.test(
      remainder.join(" ")
    );
  };
  if (coreWords.length && openWords.join(" ") === coreWords.join(" ")) {
    // Exact title core with staff junk is a dump; clean product ID is fine.
    return hasJunk;
  }
  if (coreWords.length && openWords.join(" ").startsWith(coreWords.join(" "))) {
    const remainder = openWords.slice(coreWords.length);
    if (!remainder.length) return hasJunk;
    // Title + short place tail ("for bedrooms", "in a suite") is still a dump.
    // Descriptive "with a warm aged finish" is real copy and must stay.
    if (placeTail(remainder)) return true;
  }
  if (coreWords.length) {
    const coreSet = new Set(coreWords);
    const overlap = openWords.filter((word) => coreSet.has(word)).length;
    const stop = new Set([
      "a", "an", "the", "and", "or", "for", "with", "in", "on", "to", "of",
      "this", "that", "is", "are", "as", "at", "by", "from",
    ]);
    const contentExtra = openWords.filter(
      (word) => !coreSet.has(word) && !stop.has(word) && word.length > 3
    );
    // "landscape painting" after a title is real copy, not a title dump.
    if (contentExtra.length >= 2) return false;
    if (
      overlap >= Math.max(3, Math.floor(0.85 * openWords.length)) &&
      openWords.length <= coreWords.length + 4 &&
      openWords.length - overlap <= 2 &&
      contentExtra.length === 0
    ) {
      // Clean fact-only rebuild (junk already stripped) is intentional.
      if (!hasJunk && openWords.length <= 10) return false;
      return true;
    }
  }
  return false;
}

/**
 * Rebuilt opening from Intake facts only (brand, style, color, material, type).
 * No invented qualities ("clean lines", "tall storage", "comfortable silhouette").
 */
export function seoOpeningFallback(item = null, avoidEcho = "") {
  const title = String(noteItemGet(item, ["name"]) || "");
  const blob = title.toLowerCase();
  const brand = String(noteItemGet(item, ["brand"]) || "").trim();
  const kind = productKindFromTitle(title) || "";
  let material = "";
  if (/\bwool\b/.test(blob)) material = "wool";
  else if (/\bleather\b/.test(blob)) material = "leather";
  else if (/\boak\b/.test(blob)) material = "oak";
  else if (/\bwalnut\b/.test(blob)) material = "walnut";
  else if (/\bteak\b/.test(blob)) material = "teak";
  else if (/\bwood\b/.test(blob)) material = "wood";
  let color = "";
  const colorMatch = blob.match(
    /\b(blonde|blond|grey|gray|black|white|brown|ivory|cream|navy|red|blue|green|beige|tan|ebony|ebonized)\b/
  );
  if (colorMatch) color = colorMatch[1] === "blond" ? "blonde" : colorMatch[1];
  const style = /\bpost[\s-]?modern\b/.test(blob)
    ? "post-modern"
    : /\bmodern\b/.test(blob)
      ? "modern"
      : /\btraditional\b/.test(blob)
        ? "traditional"
        : /\btransitional\b/.test(blob)
          ? "transitional"
          : "";
  const join = (...parts) => parts.filter(Boolean).join(" ").replace(/\s{2,}/g, " ").trim();
  let kindLabel = kind || "piece";
  if (kind === "rug" || /\brugs?\b/.test(blob)) {
    kindLabel = /\barea\s+rugs?\b/.test(blob) ? "area rug" : "rug";
  } else if (/\b(armoires?|wardrobes?)\b/.test(blob)) {
    kindLabel = /\bwardrobes?\b/.test(blob) ? "wardrobe" : "armoire";
  } else if (kind === "bed" || /\bbeds?\b/.test(blob)) {
    kindLabel = "bed";
  } else if (/\bconsoles?\b/.test(blob)) {
    kindLabel = "console";
  } else if (kind === "sofa" || /\bsofas?\b/.test(blob)) {
    kindLabel = "sofa";
  } else if (kind === "chair" || /\bchairs?\b/.test(blob)) {
    kindLabel = "chair";
  } else if (kind === "table" || /\btables?\b/.test(blob)) {
    kindLabel = "table";
  } else if (kind === "desk" || /\bdesks?\b/.test(blob)) {
    kindLabel = "desk";
  }
  const full = join(brand, style, color, material, kindLabel);
  const short = join(brand, kindLabel) || join(style, kindLabel) || kindLabel;
  const candidates = [
    full ? `${full}.` : "",
    short ? `${short}.` : "",
    `This ${kindLabel}.`,
  ].filter(Boolean);
  const avoid = String(avoidEcho || "");
  for (const sentence of candidates) {
    if (!sharesSubstantialPhrase(sentence, avoid)) {
      return sentence[0].toUpperCase() + sentence.slice(1);
    }
  }
  const fallback = candidates[candidates.length - 1] || "This piece.";
  return fallback[0].toUpperCase() + fallback.slice(1);
}

function storedAxisInches(item = null) {
  const dims = item?.dims && typeof item.dims === "object" ? item.dims : {};
  const width = positiveListingNumber(
    noteItemGet(item, ["dimWidth", "dim_width"]) ?? dims.width ?? dims.dimWidth
  );
  const depth = positiveListingNumber(
    noteItemGet(item, ["dimDepth", "dim_depth", "dimLength", "dim_length"]) ??
      dims.depth ??
      dims.dimDepth ??
      dims.dimLength
  );
  const height = positiveListingNumber(
    noteItemGet(item, ["dimHeight", "dim_height"]) ?? dims.height ?? dims.dimHeight
  );
  return { width, depth, height };
}

export function rugSizePhrase(item = null) {
  const title = String(noteItemGet(item, ["name"]) || "");
  const match = title.match(SIZE_TOKEN_IN_PROSE_RE);
  if (match) {
    const nums = match[0].match(/\d+(?:\.\d+)?/g) || [];
    if (nums.length >= 2) return `${nums[0]} by ${nums[1]}`;
  }
  const { width, height } = storedAxisInches(item);
  if (width != null && height != null) return `${formatDimNumber(width)} by ${formatDimNumber(height)}`;
  return "";
}

function isWallArtItem(item = null) {
  const title = String(noteItemGet(item, ["name"]) || "");
  const blob = title.toLowerCase();
  const kind = productKindFromTitle(title) || "";
  if (["art", "mirror", "painting"].includes(kind)) return true;
  const cat = String(
    noteItemGet(item, ["websiteCategory", "website_category", "category"]) || ""
  )
    .trim()
    .toLowerCase();
  if (["a", "art", "art / mirrors", "art/mirrors", "art mirrors"].includes(cat)) return true;
  return /\b(paintings?|prints?|giclees?|canvases?|wall\s+arts?|mirrors?|lithographs?)\b/.test(blob);
}

export function labeledFurnitureInches(item = null) {
  const title = String(noteItemGet(item, ["name"]) || "");
  const blob = title.toLowerCase();
  const kind = productKindFromTitle(title) || "";
  const wallArt = isWallArtItem(item);
  let { width, depth, height } = storedAxisInches(item);
  const bits = [];
  if (width != null) bits.push(`${formatDimNumber(width)} inches wide`);
  if (
    depth != null &&
    !(
      wallArt ||
      (depth <= 2 && width != null && height != null && /\b(rugs?|mirrors?|paintings?|art)\b/.test(blob))
    )
  ) {
    bits.push(`${formatDimNumber(depth)} inches deep`);
  }
  if ((kind === "bed" || /\bbeds?\b/.test(blob)) && width != null && depth != null && (height == null || height <= 20)) {
    return [`${formatDimNumber(width)} inches wide`, `${formatDimNumber(depth)} inches deep`];
  }
  if (height != null) {
    bits.push(
      wallArt
        ? `${formatDimNumber(height)} inches high`
        : `${formatDimNumber(height)} inches tall`
    );
  }
  if (bits.length === 3 && depth != null && width != null && height != null && depth < Math.min(width, height) * 0.35) {
    return [bits[0], bits[2]];
  }
  return bits;
}

function joinMeasureBits(bits, { wallArt = false } = {}) {
  if (!bits.length) return "";
  if (bits.length === 1) return bits[0];
  if (bits.length === 2) return wallArt ? `${bits[0]} by ${bits[1]}` : `${bits[0]} and ${bits[1]}`;
  return `${bits[0]}, ${bits[1]}, and ${bits[2]}`;
}

/** Compact size cue. Rugs: 8 by 10. Furniture: labeled inches. */
export function displaySizePhrase(item = null) {
  const title = String(noteItemGet(item, ["name"]) || "");
  const blob = title.toLowerCase();
  const kind = productKindFromTitle(title) || "";
  if (kind === "rug" || /\brugs?\b/.test(blob)) return rugSizePhrase(item);
  const bits = labeledFurnitureInches(item);
  if (bits.length) return joinMeasureBits(bits);
  return rugSizePhrase(item);
}

function ngrams(words, size) {
  if (words.length < size) return new Set();
  const out = new Set();
  for (let i = 0; i <= words.length - size; i += 1) out.add(words.slice(i, i + size).join(" "));
  return out;
}

export function sharesSubstantialPhrase(left, right) {
  const a = normSizeWords(left);
  const b = normSizeWords(right);
  if (!a.length || !b.length) return false;
  const stop = new Set([
    "a", "an", "the", "and", "or", "for", "with", "in", "on", "to", "of", "it", "its",
    "this", "that", "is", "are", "as", "at", "by", "from", "into", "without",
  ]);
  for (const n of [5, 4, 3]) {
    const leftGrams = [...ngrams(a, n)].filter((gram) => !gram.split(" ").every((part) => stop.has(part)));
    const rightGrams = ngrams(b, n);
    if (leftGrams.some((gram) => rightGrams.has(gram))) return true;
  }
  return false;
}

export function naturalSizeSentence(item = null, existingProse = "") {
  const title = String(noteItemGet(item, ["name"]) || "");
  const blob = title.toLowerCase();
  const kind = productKindFromTitle(title) || "";
  const existing = String(existingProse || "");
  if (kind === "rug" || /\brugs?\b/.test(blob)) {
    const size = rugSizePhrase(item);
    if (!size) return "";
    const candidates = [
      `This is an ${size} rug that anchors a seating group without feeling oversized.`,
      `This is an ${size} rug with enough presence to define a conversation area.`,
      `Sized as an ${size}, it still leaves walkways open around a bed or sofa.`,
    ];
    return candidates.find((sentence) => !sharesSubstantialPhrase(sentence, existing)) || candidates[candidates.length - 1];
  }
  const bits = labeledFurnitureInches(item);
  if (!bits.length) return "";
  const wallArt = isWallArtItem(item);
  const measure = joinMeasureBits(bits, { wallArt });
  let candidates;
  if (wallArt) {
    candidates = [
      `It measures ${measure}.`,
      `At ${measure}, it suits a living room or hallway wall.`,
    ];
  } else if (/\b(armoires?|wardrobes?|dressers?|cabinets?|bookcases?|sideboards?|buffets?)\b/.test(blob)) {
    candidates = [
      `It measures ${measure}, so it works along a bedroom wall or hallway.`,
      `It measures ${measure}, giving it a real storage footprint without crowding the room.`,
      `With a footprint of ${measure}, it reads as tall storage rather than a bulky box.`,
    ];
  } else if (kind === "sofa" || kind === "chair" || /\b(sofas?|loveseats?|sectionals?|armchairs?|recliners?)\b/.test(blob)) {
    candidates = [
      `It measures ${measure}, so it fits a living room or seating nook cleanly.`,
      `At ${measure}, it has a comfortable everyday seating scale.`,
    ];
  } else if (kind === "table" || kind === "desk" || /\b(consoles?|tables?|desks?)\b/.test(blob)) {
    candidates = [
      `It measures ${measure}, so it works for dining or desk use without feeling oversized.`,
      `At ${measure}, the top has a practical everyday footprint.`,
    ];
  } else if (kind === "bed" || kind === "nightstand" || /\b(beds?|nightstands?)\b/.test(blob)) {
    candidates = [
      `It measures ${measure}, so it suits a primary or guest bedroom layout.`,
      `At ${measure}, it keeps a low bedroom footprint.`,
    ];
  } else {
    candidates = [
      `It measures ${measure}, so it works in everyday rooms without feeling oversized.`,
      `At ${measure}, it keeps a practical footprint.`,
    ];
  }
  return candidates.find((sentence) => !sharesSubstantialPhrase(sentence, existing)) || candidates[candidates.length - 1];
}

function insertSizeSentence(rest, sizeSentence) {
  if (!sizeSentence) return [...rest];
  const out = [...rest];
  let insertAt = 0;
  for (let idx = 0; idx < out.length; idx += 1) {
    if (WEAR_OR_AS_IS_SENT_RE.test(out[idx])) {
      insertAt = idx;
      break;
    }
    if (idx === 0) {
      insertAt = 1;
      continue;
    }
    insertAt = Math.min(idx + 1, out.length);
    break;
  }
  out.splice(insertAt, 0, sizeSentence);
  return out;
}

/**
 * Keep size out of sentence 1; weave it once later when the item has a size.
 * Title-only leftovers are rebuilt. Size wording varies and avoids repeated phrases.
 */
export function enforceOpeningWithoutSize(prose, item = null) {
  let text = String(prose || "").replace(/\s{2,}/g, " ").trim();
  if (!text) return "";
  let footer = "";
  const foot = text.match(/(\n*Dimensions:\s*[\s\S]*)$/i);
  if (foot) {
    footer = foot[1];
    text = text.slice(0, foot.index).trim();
  }
  const parts = splitProseSentences(text);
  const bodyParts = [];
  let shipping = "";
  for (const part of parts) {
    if (SHIP_SENTENCE_RE.test(part)) {
      shipping = shipping ? `${shipping} ${part}` : part;
    } else {
      bodyParts.push(part);
    }
  }
  if (!bodyParts.length) {
    let rebuilt = text;
    if (shipping) rebuilt = rebuilt ? `${rebuilt} ${shipping}` : shipping;
    return footer ? `${rebuilt}\n\n${footer.trim()}` : rebuilt;
  }
  let opening = bodyParts[0];
  const rest = bodyParts.slice(1);
  if (sentenceHasSizeOrDims(opening)) {
    opening = stripSizeNoiseFromSentence(opening);
  }
  const keptRestPreview = [];
  for (const part of rest) {
    if (sentenceHasSizeOrDims(part)) continue;
    keptRestPreview.push(part);
  }
  if (openingIsTitleRestatement(opening, item) || (opening.match(/[A-Za-z]{2,}/g) || []).length < 4) {
    opening = seoOpeningFallback(item, keptRestPreview.join(" "));
  }
  const keptRest = [...keptRestPreview];
  if (!sentenceHasSizeOrDims(opening)) {
    const sizeSentence = naturalSizeSentence(item, [opening, ...keptRest].join(" "));
    if (sizeSentence) keptRest.splice(0, keptRest.length, ...insertSizeSentence(keptRest, sizeSentence));
  }
  let rebuilt = [opening, ...keptRest].join(" ").replace(/\s{2,}/g, " ").trim();
  if (shipping) rebuilt = `${rebuilt} ${shipping}`.trim();
  if (footer) {
    rebuilt = `${rebuilt}\n\n${footer.trim()}`.replace(/\n{3,}/g, "\n\n").trim();
  }
  return rebuilt;
}

/**
 * Rebuild the buyer paragraph from the marketing copy and the current note.
 * Old flaw sentences and AS IS pastes are dropped first. The previous Shopify
 * body is not a splice base, and the sync snapshot is not an input.
 * Warehouse, price, Brand is, and measure dumps stay out. Coming apart is not AS IS.
 * A second call does not repeat the flaw sentence or the shipping sentence.
 */
export function finalizeListingDescription(description, item, notes, conditionIssues) {
  const source = item && typeof item === "object" ? item : {};
  const noteText = notes !== undefined ? notes : noteItemGet(source, ["notes"]);
  const issueText =
    conditionIssues !== undefined
      ? conditionIssues
      : noteItemGet(source, ["conditionIssues", "condition_issues"]);
  const sourceText = String(description || "");
  // From-scratch base: one product paragraph only. Dual rewrites are discarded here.
  // Never splice the previous Shopify body.
  let prose = marketingListingBase(description, source);
  let allowWear = stockWearAllowed(noteText, issueText, sourceText);
  // Title "- AS IS" + specific Extra Info (noticeable wear) → one woven AS IS beat.
  if (!allowWear && specificWearAdjective(noteText, issueText)) {
    const title = String(noteItemGet(source, ["name"]) || "");
    if (/\bas[\s-]is\b/i.test(title)) allowWear = true;
  }
  const wearSentence = wearSentenceForNotes(noteText, issueText, { allowStock: allowWear });
  const hadProse = Boolean(prose);
  const product = [];
  const asIs = [];
  const seen = new Set();
  const rawClauses = [...splitNoteClauses(noteText), ...splitNoteClauses(issueText)];
  const noteNegates = rawClauses.some((raw) => isNegatedConditionNote(cleanNoteClause(raw)));
  for (const raw of rawClauses) {
    const cleaned = cleanNoteClause(raw);
    const key = cleaned.toLowerCase();
    if (!cleaned || seen.has(key) || isCustomerSilentNote(cleaned)) continue;
    // Generic AND specific wear notes are handled by wearSentence — never as_is dump.
    if (isGenericWearClause(cleaned) || isSpecificWearClause(cleaned)) continue;
    if (
      noteNegates &&
      (naturalFlawSentence(cleaned) || positiveConditionSentence(cleaned) || DAMAGE_NOTE_RE.test(cleaned))
    ) {
      continue;
    }
    seen.add(key);
    prose = removeRawNote(prose, cleaned);
    prose = removeRawNote(prose, raw);
    const positive = positiveConditionSentence(cleaned);
    if (positive) {
      product.push([cleaned, positive]);
      continue;
    }
    const flawSentence = naturalFlawSentence(cleaned);
    if (flawSentence) product.push([cleaned, flawSentence]);
    else if (/\bcoming\b/i.test(cleaned) && /\bapart\b/i.test(cleaned)) {
      const sentence = naturalBuyerSentence(cleaned);
      if (sentence) product.push([cleaned, sentence]);
    } else if (/\bas[\s-]is\b/i.test(cleaned) || DAMAGE_NOTE_RE.test(cleaned)) asIs.push(asIsClause(cleaned));
    // Anything else in Notes or What is wrong stays private.
  }
  const conditionRows = product.filter(([raw, sentence]) => isConditionSentence(raw, sentence));
  if (conditionRows.length > 1) {
    const last = conditionRows[conditionRows.length - 1];
    const rest = product.filter((row) => !conditionRows.includes(row));
    product.length = 0;
    product.push(...rest, last);
  }
  for (const [raw, sentence] of product) {
    if (noteAlreadyPresent(prose, raw, sentence)) continue;
    prose = prose ? `${prose} ${sentence}`.trim() : sentence;
  }
  if (!prose && !asIs.length && !allowWear && !wearSentence) return "";
  if (hadProse || product.length) prose = withShippingSentence(prose, source);
  for (const clause of asIs) {
    if (!clause || isGenericWearClause(clause) || containsStockWear(clause)) continue;
    if (isSpecificWearClause(clause)) continue;
    if (asIsPresent(prose, clause)) continue;
    const line = asIsLine(clause);
    if (!line || /^AS IS\s*:/i.test(line)) continue;
    prose = `${prose} ${line}`.trim();
  }
  prose = dropUntrustedTitleClaims(prose, source);
  if (hadProse && !stripShippingSentences(prose).trim()) {
    prose = withShippingSentence(mechanicalListingCopy(noteItemGet(source, ["name"]), source), source);
  }
  // One wear beat before Scottsdale. Specific notes (noticeable wear) win over stock-only.
  prose = enforceSingleWearSentence(prose.replace(/\s{2,}/g, " ").trim(), wearSentence);
  // Opening never carries size; one natural size weave later when dims/title have one.
  prose = enforceOpeningWithoutSize(prose, source);
  return prose.replace(/\s{2,}/g, " ").trim();
}

/** Intake notes for the Shopify body. Measured spill, warehouse, price, and Brand is stay out. */
export function shopifyBuyerNote(notes, conditionIssues) {
  return buyerFacingClauses(notes, conditionIssues).join(" ");
}

/**
 * Same prose Intake saved: flowing copy, one flaw sentence, one shipping line.
 * Tags are not touched. A second call does not stack the sentence or the shipping line.
 */
export function appendIntakeNotesToDescription(description, notes, conditionIssues, item) {
  return finalizeListingDescription(flowingBuyerProse(description), item, notes, conditionIssues);
}

function proseAlreadyStatesWeight(text) {
  return /\bweigh(?:s|ing)\s+(?:about\s+|approximately\s+|roughly\s+)?\d+(?:\.\d+)?\s*(?:lb|lbs|pounds?)\b/i.test(
    String(text || "")
  );
}

export function composeShopifyBodyHtml(description, dims, item = null) {
  const cleaned = stripListingDimJunk(description).replace(/\b(\d+)\.0\b/g, "$1");
  if (!cleaned) return "";
  const listingDims = attachListingDimMeta(dims, item);
  const dimBlock = formatShopifyDimsLine(listingDims, {
    includeWeight: !proseAlreadyStatesWeight(cleaned),
  });
  const paragraphs = cleaned
    .split(/\n+/)
    .map((p) => p.trim())
    .filter(Boolean);
  let html = paragraphs.map((p) => `<p>${escapeHtml(p)}</p>`).join("");
  if (!dimBlock) return html;
  const footerInner = escapeHtml(dimBlock).replace(/\s*\n+\s*/g, " ").replace(/\s{2,}/g, " ").trim();
  // One blank line inside the last paragraph. A second <p> plus <br><br>
  // stacks Shopify's paragraph margin into a large gap.
  if (html) {
    return html.replace(/(?:<br\s*\/?>\s*)*<\/p>\s*$/i, `<br><br>${footerInner}</p>`);
  }
  return `<p>${footerInner}</p>`;
}

/** True when text is empty or only Dimensions:/Weight: footer noise. */
export function isDimsOnlyListingText(text) {
  const cleaned = stripListingDimJunk(text);
  return !cleaned;
}

/** Too thin to stand as the description (matches Pricing Engine prose_is_thin): the one shared minimum. */
export function listingProseIsThin(text) {
  return listingCopyIsThin(text);
}

/**
 * Under 48 characters of text: a scribble, not a description someone wrote.
 * Only the Shopify-to-Intake mirror asks this. It is not the minimum for a
 * finished description (listingProseIsThin): a single sentence typed in
 * Shopify is still the user's description and still reaches Intake.
 */
export function bodyTooShortToMirror(text) {
  return stripListingDimJunk(text).replace(/\s+/g, " ").trim().length < 48;
}

const INTERNAL_OPS_RE =
  /\b(box\s+in\s+warehouse|in\s+(?:the\s+)?warehouse|warehouse|tall\s+box|short\s+box|large\s+box|small\s+box|box\s+that\s+says|packing\s+(?:note|location|slip)|pack(?:ed|ing)?\s+(?:in|at|location)|back\s*room|storage\s+(?:room|area|shelf|bin)|staff\s+note|internal\s+note|for\s+staff|do\s+not\s+(?:list|publish|sell)|shelf\s*[#:]?\s*\d+|aisle\s*[#:]?\s*\w+|bin\s*[#:]?\s*\w+|on\s+the\s+(?:shelf|rack|pallet)|left\s+of\s+(?:the\s+)?(?:door|aisle)|pull\s+from\s+(?:warehouse|back)|location\s*:\s*(?!the\s+lost)|traxia updated)\b/i;

const CUSTOMER_CONDITION_RE =
  /\b(damage(?:d)?|flaw(?:s|ed)?|scratch(?:ed|es)?|chip(?:ped|s)?|crack(?:ed|s)?|stain(?:ed|s)?|wear(?:ing)?|worn|tear(?:s|ed)?|torn|dent(?:ed|s)?|fade(?:d)?|missing|repair(?:ed|s)?|scuff(?:ed|s)?|imperfection(?:s)?|as[\s-]is|condition|loose|broken|mark(?:s|ed)?\s+on|discolor(?:ed|ation)?)\b/i;

/** Warehouse / packing / staff notes typed into Shopify Description — not customer copy. */
export function isInternalOpsListingNote(text) {
  const plain = stripListingDimJunk(text).replace(/\s+/g, " ").trim();
  if (!plain || !INTERNAL_OPS_RE.test(plain)) return false;
  if (plain.length <= 180) return true;
  if (CUSTOMER_CONDITION_RE.test(plain) && plain.length >= 120) return false;
  return bodyTooShortToMirror(plain) || plain.length < 220;
}

/** Damage / flaw / condition notes the shopper should know. */
export function isCustomerConditionListingNote(text) {
  const plain = stripListingDimJunk(text).replace(/\s+/g, " ").trim();
  if (!plain || isInternalOpsListingNote(plain)) return false;
  return CUSTOMER_CONDITION_RE.test(plain);
}

const MARKETING_BODY_RE =
  /\b(?:AEO|GEO|SEO|living room|bedroom|palette|wall art|focal piece|designed to)\b/i;

/** A short flaw note. A marketing paragraph that mentions a scratch is not one. */
export function isShortConditionBlurb(text) {
  const plain = stripListingDimJunk(text).replace(/\s+/g, " ").trim();
  if (!plain || plain.length > 180) return false;
  const stops = plain.match(/[.!?]/g);
  if (stops && stops.length > 2) return false;
  if (MARKETING_BODY_RE.test(plain)) return false;
  return true;
}

/**
 * Classify Shopify body_html for Intake mirror.
 * Prefer Intake Helper prose over warehouse scribbles and short non-marketing text.
 */
export function classifyShopifyBodyForIntake(shopifyBody, intakeDescription = "") {
  const plain = stripListingDimJunk(shopifyBody).replace(/\s+/g, " ").trim();
  const intakePlain = stripListingDimJunk(intakeDescription).replace(/\s+/g, " ").trim();
  const intakeHasHelper = Boolean(intakePlain) && !bodyTooShortToMirror(intakePlain);

  if (!plain) {
    return {
      kind: "empty",
      applyDescription: false,
      applyNotes: false,
      // Traxia e-com often wipes body — one Intake re-push restores Helper copy.
      restoreListing: true,
      plain: "",
    };
  }
  if (isInternalOpsListingNote(plain)) {
    return {
      kind: "internal_ops",
      applyDescription: false,
      applyNotes: false,
      restoreListing: true,
      plain,
    };
  }
  if (isCustomerConditionListingNote(plain) && isShortConditionBlurb(plain)) {
    return {
      kind: "customer_condition",
      applyDescription: !intakeHasHelper,
      applyNotes: true,
      restoreListing: true,
      plain,
    };
  }
  if (bodyTooShortToMirror(plain) || (intakeHasHelper && plain.length + 40 < intakePlain.length)) {
    return {
      kind: "thin",
      applyDescription: false,
      applyNotes: false,
      restoreListing: Boolean(intakeHasHelper),
      plain,
    };
  }
  const stackedShopify =
    descriptionHasStackedProductProse(plain) || descriptionHasStackedWear(plain) || /AS\s*IS\s*:/i.test(plain);
  // Stacked dual product paragraphs / AS IS paste on Shopify must be replaced from Intake.
  if (intakeHasHelper && stackedShopify) {
    return {
      kind: "marketing",
      applyDescription: false,
      applyNotes: false,
      restoreListing: true,
      plain,
    };
  }
  // Real marketing-length Shopify copy: keep Intake Helper; do not hammer resync.
  if (intakeHasHelper) {
    return {
      kind: "marketing",
      applyDescription: false,
      applyNotes: false,
      restoreListing: false,
      plain,
    };
  }
  return {
    kind: "marketing",
    applyDescription: true,
    applyNotes: false,
    restoreListing: false,
    plain,
  };
}

const ART_CATEGORY_RE = /^\s*ART\b/i;
const ART_NAME_RE =
  /\b(?:giclee|lithograph|serigraph|etching|engraving|woodcut|linocut|screen\s*print|silkscreen|artist'?s?\s+proof|oil\s+on\s+(?:canvas|board|panel|linen|masonite)|acrylic\s+on\s+\w+|watercolou?r|gouache|mixed\s+media\s+on|paintings?|art\s+print|limited\s+edition\s+print|bronze\s+sculpture)\b/i;
const NOT_ART_NAME_RE = /\b(?:mirrors?|easels?|lamps?|tables?|chairs?|rugs?|trays?|chests?|cabinets?)\b/i;
const NOT_AN_ARTIST_WORDS = new Set([
  "abstract", "antique", "art", "artist", "artwork", "bronze", "canvas", "color",
  "contemporary", "edition", "etching", "framed", "giclee", "landscape", "large",
  "limited", "lithograph", "modern", "oil", "original", "painting", "pair",
  "photograph", "portrait", "print", "proof", "sculpture", "serigraph", "set",
  "signed", "small", "southwest", "southwestern", "still", "unframed", "untitled",
  "unsigned", "vintage", "wall", "watercolor", "western", "test",
]);

/** A work of art: an ART category, or an art medium in the name. Mirrors are not art. */
export function isArtListing({ name, category, merchType } = {}) {
  if (normalizeMerchType(merchType) === "luxury") return false;
  const categoryText = String(category || "");
  const title = String(name || "");
  if (ART_CATEGORY_RE.test(categoryText)) {
    // The website label "Art / Mirrors" covers both. The name says which.
    if (/mirror/i.test(categoryText)) return !/\bmirrors?\b/i.test(title);
    return true;
  }
  // Staff chose another category (MIRROR, TRAY, CHEST). The name alone does not make it art.
  if (categoryText.trim()) return false;
  return ART_NAME_RE.test(title) && !NOT_ART_NAME_RE.test(title);
}

/** The person named at the start of an art title, before the dash. Empty when unclear. */
export function artArtistFromTitle(name) {
  const head = String(name || "").trim().split(/\s+[-–—]\s+/)[0].replace(/^[\s,]+|[\s,]+$/g, "");
  const words = head.split(/\s+/).filter(Boolean);
  if (words.length < 2 || words.length > 4) return "";
  for (const word of words) {
    if (!/^[A-Z][A-Za-z'.-]*$/.test(word)) return "";
    if (NOT_AN_ARTIST_WORDS.has(word.toLowerCase().replace(/^\.+|\.+$/g, ""))) return "";
  }
  return head;
}

function plainMeasure(value) {
  const number = Number(value);
  if (!Number.isFinite(number) || number <= 0) return "";
  return String(Number(number.toFixed(3)));
}

/** Recorded face size of an artwork as width by height. Empty when not recorded. */
export function artSizeFact({ name, dims } = {}) {
  let width = plainMeasure(dims?.width ?? dims?.dimWidth);
  let height = plainMeasure(dims?.height ?? dims?.dimHeight);
  if (!(width && height)) {
    const token = String(name || "").match(
      /(\d+(?:\.\d+)?)\s*[xX×]\s*(\d+(?:\.\d+)?)\s*H?\s*(?:[-–—]\s*AS\s*-?\s*IS\s*)?$/i
    );
    if (token) {
      width = plainMeasure(token[1]);
      height = plainMeasure(token[2]);
    }
  }
  return width && height ? `${width} inches wide by ${height} inches high` : "";
}

const PROSE_SHARED_RULES = `Write one flowing description of one paragraph. No section labels.
Sentence 1 says what the item is in the words a shopper would search. Never put size, dimensions, W x D x H, or a title size token such as 8x10 or 42X23X75 in sentence 1, and never room-ready size filler.
One sentence stands alone as the answer to what is this: it names the maker or artist, the item, and the material or medium, and still makes sense when an assistant repeats only that sentence.
Use the facts in the record. You may add a widely known fact about a named artist, maker, or brand, such as what they are known for, stated as a fact about them and not about this piece.
You have not seen this piece. Do not describe what it looks like (subject, scene, colors, pattern, finish, shape) unless the title or the staff notes say so.
Never invent condition, damage, wear, repairs, provenance, authenticity, or a history of ownership.
Length follows the facts. Give each real fact one clear sentence and stop when the facts run out. There is no required sentence count, and two or three true sentences are better than six padded ones.
Do not write a line that could describe any item, such as it changes the way a room feels, adds character to any space, a timeless piece, a statement piece, a conversation starter, or perfect for any room.
Say the full item name only once. Do not begin a later sentence by repeating the whole title. Write one new description. Do not repeat, quote, or append any earlier description.
Never wrap a sentence, tagline, or highlight in quotation marks.
Do not include condition, wear, or AS IS wording; one sentence is added later from the current condition note only, and AS IS appears only there.
Do not write pickup, delivery, shipping, or freight; the Scottsdale pickup and delivery close is added once at the end.
Never say the copy is based on the name provided, that attribution is based on a name, or that no additional details are stated.
Never write the words SEO, AEO, GEO, or Condition note in the description.
Use whole numbers when the fractional part is zero (65 not 65.0). Use STAFF EXTRA INFORMATION for accuracy.
A single Dimensions and Weight footer is added later. Never dump Measured:/Height=/Width=/Depth=/Weight= or FH/X/LG codes. Do NOT put a Dimensions:/Weight: footer.
ASCII only. Return JSON {"description":"...","tags":"..."}.`;

/**
 * System and user messages for generateShopifyListingProse. Art has its own
 * instructions; furniture and luxury keep their maker rule.
 */
export function buildListingProsePrompt({
  name,
  notes,
  brand,
  category,
  merchType,
  tags,
  dims,
  dimHint,
} = {}) {
  const mode = normalizeMerchType(merchType) === "luxury" ? "luxury" : "furniture";
  const title = String(name || "").trim();
  const brandName = String(brand || "").trim();
  const art = isArtListing({ name: title, category, merchType: mode });
  const head = "You write Shopify product body prose for Lost & Found Resale (Scottsdale).";
  let modeRules;
  if (art) {
    modeRules = `COPY MODE ART. This is a work of art, not furniture. Ignore every rule about rooms, drawers, finishes, hardware, and construction.
Say what the record gives, in this order: the artist, the title of the work, the medium (for example oil on canvas, giclee on canvas, lithograph, serigraph, watercolor, or bronze), the edition type (original, limited edition, artist's proof, numbered, or open edition), whether it is signed, whether it is framed, and the size as width by height.
The artist is the Brand field when it is set. When the Brand field is empty, the artist is the person named in the title; name the artist. Give the full name once, then the surname.
The title of the work is the part of the item name that is not the artist, the medium, the edition, or the size. Write it as plain words without quotation marks.
Sentence 1: artist, work title, medium, and edition type.
You may say what the named artist is widely known for, such as the subjects, region, period, or movement, and what the medium or edition type means to a collector. If you are not sure the artist is widely known, give the name and nothing more about the artist.
Do not describe what this piece shows (its subject, scene, figures, colors, or mood) unless the staff notes say so.
State the size once, in a later sentence, as width by height in inches. Never give a depth for wall art.
Signed means the record says signed. Never invent a signature, an edition number, a certificate, a date, or an exhibition history.
Do not write placement or decorating advice, and no line that could describe any artwork, such as art changes the way a room feels or it brings any wall to life.
Do not turn test words in the title into an artist, a title, or a medium.`;
  } else if (mode === "luxury") {
    modeRules = `COPY MODE LUXURY GOODS (Traxia E Commerce Helper). Furniture room-placement rules do NOT apply.
Sentence 1: brand, silhouette, color, material, item type.
Name only real entities: house, material, colorway, hardware, silhouette.
Do not turn test words or personal names in the title into a maker. If Brand is empty, do not name a maker. If the maker is unknown, do not mention a maker.
No high/wide/deep furniture "when displayed" sentence. No room/entryway/vanity filler.`;
  } else {
    modeRules = `COPY MODE FURNITURE / HOME — strongest search, answer, and entity copy.
Sentence 1: brand or maker if known, item type, material, color, style or motif.
Mention size once later in a natural sentence about what it fits or where it works (for example At 8 by 10, it fits living rooms...).
Name real entities: maker, material, finish, color, motif, era or date when a real brand is known.
Do not turn test words or personal names in the title into a maker, brand, or material. If the maker is unknown, do not mention a maker.
Lead with brand + item type when known; else "This" + item type. Mention a placement or use only when it follows from what the piece is. Do not list rooms to fill space.`;
  }
  const system = `${head}\n${modeRules}\n${PROSE_SHARED_RULES}`;
  const lines = [
    `Generate Shopify listing DESCRIPTION prose (no dims footer).`,
    `NAME/TITLE: ${title || "(none)"}`,
    `STAFF EXTRA INFORMATION / NOTES: ${String(notes || "").trim() || "(none)"}`,
  ];
  if (art) {
    const artist = brandName || artArtistFromTitle(title);
    if (brandName) lines.push(`ARTIST (the Brand field): ${brandName}`);
    else if (artist) {
      lines.push(
        `ARTIST (from the title, because the Brand field is empty): ${artist}. Name this artist in the description. If these words are plainly not a person or a studio, name no artist.`
      );
    } else {
      lines.push(
        `ARTIST: the Brand field is empty. If the title names a person or a studio, that is the artist and you name them. If the title names no one, name no artist.`
      );
    }
    const size = artSizeFact({ name: title, dims });
    if (size) {
      lines.push(`RECORDED SIZE: ${size}. State it once, in a later sentence, as width by height. Never in sentence 1.`);
    }
  } else {
    lines.push(`BRAND: ${brandName || "(empty). Do not name a maker."}`);
  }
  lines.push(
    `CATEGORY: ${String(category || "").trim() || "(none)"}`,
    `MERCH MODE: ${art ? "art" : mode}`,
    `E-COMMERCE TAGS: ${tags || "(none)"}`
  );
  if (!art) {
    lines.push(
      `DIMENSION FACTS (never in sentence 1; mention size once later in a natural what-it-fits sentence; one Dimensions footer is added later): ${dimHint || "(none)"}`
    );
  }
  lines.push(`Return JSON only.`);
  return { system, user: lines.join("\n"), mode: art ? "art" : mode };
}

/**
 * Helper-style OpenAI body prose (furniture vs luxury) when Intake description is empty.
 * Uses OPENAI_API_KEY on sync-server. Never includes Dimensions footer.
 */
export async function generateShopifyListingProse(
  {
    name,
    notes,
    description,
    brand,
    category,
    websiteCategory,
    merchType,
    ecommerceTags,
    dims,
  } = {},
  { log, post } = {}
) {
  const key = String(process.env.OPENAI_API_KEY || "").trim();
  if (!key) return { ok: false, reason: "openai_missing" };
  const mode = normalizeMerchType(merchType) === "luxury" ? "luxury" : "furniture";
  const title = normalizeFullItemName(name);
  // Condition content only — never folder/warehouse/staff noise for the model.
  const extra = buyerFacingClauses(notes, "").join(". ").trim();
  const existing = stripListingDimJunk(description || "");
  if (!title && !extra && !existing) return { ok: false, reason: "no_input" };

  const dimHint = formatShopifyDimsLine(
    attachListingDimMeta(dims || {}, { websiteCategory, category })
  );
  const staleCopy = proseHasInventedMaker(existing, brand);
  const promptTags = staleCopy
    ? freshListingTags({ name: title, brand, merchType: mode })
    : stripMakerTags(ecommerceTags, title, brand, category, existing);
  const { system, user } = buildListingProsePrompt({
    name: title,
    notes: extra,
    brand,
    category,
    merchType: mode,
    tags: promptTags,
    dims,
    dimHint,
  });
  // The item's recorded identity: what the second write-up check and the factual fallback use.
  const identity = { name: title, brand: String(brand || "").trim(), category: String(category || "").trim() };

  const complete = typeof post === "function" ? post : (url, data, config) => axios.post(url, data, config);
  try {
    const model = String(process.env.OPENAI_LISTING_MODEL || process.env.OPENAI_MODEL || "gpt-4o-mini").trim();
    const resp = await complete(
      "https://api.openai.com/v1/chat/completions",
      {
        model,
        messages: [
          { role: "system", content: system },
          { role: "user", content: user },
        ],
        temperature: 0.35,
        response_format: { type: "json_object" },
      },
      {
        headers: {
          Authorization: `Bearer ${key}`,
          "Content-Type": "application/json",
        },
        timeout: 90000,
        validateStatus: () => true,
      }
    );
    if (resp.status >= 400) {
      log?.("warn", {
        event: "shopify.listing_prose.openai_http",
        status: resp.status,
        body: JSON.stringify(resp.data || {}).slice(0, 240),
      });
      return { ok: false, reason: `openai_http_${resp.status}` };
    }
    const raw = resp.data?.choices?.[0]?.message?.content || "{}";
    let parsed = {};
    try {
      parsed = JSON.parse(raw);
    } catch (_err) {
      return { ok: false, reason: "openai_bad_json" };
    }
    let prose = stripListingDimJunk(String(parsed.description || "").trim());
    if (!prose) return { ok: false, reason: "empty_description" };
    // Rejected text falls back to the description the item already has, or to recorded facts.
    let settled = settleBuyerListingCopy(prose, title, null, brand, existing, identity);
    if (settled.flagged) {
      const again = await complete(
        "https://api.openai.com/v1/chat/completions",
        {
          model,
          messages: [
            { role: "system", content: system },
            { role: "user", content: user },
          ],
          temperature: 0.2,
          response_format: { type: "json_object" },
        },
        {
          headers: {
            Authorization: `Bearer ${key}`,
            "Content-Type": "application/json",
          },
          timeout: 90000,
          validateStatus: () => true,
        }
      );
      if (again.status < 400) {
        let againParsed = {};
        try {
          againParsed = JSON.parse(again.data?.choices?.[0]?.message?.content || "{}");
        } catch (_err) {
          againParsed = {};
        }
        const againProse = stripListingDimJunk(String(againParsed.description || "").trim());
        if (againProse) {
          settled = settleBuyerListingCopy(againProse, title, null, brand, existing, identity);
        }
      }
    }
    prose = settled.description;
    if (!prose) return { ok: false, reason: "empty_description" };
    log?.("info", {
      event: "shopify.listing_prose.ok",
      mode,
      title: title.slice(0, 120),
      len: prose.length,
    });
    return {
      ok: true,
      description: prose,
      tags: stripMakerTags(parsed.tags || "", title, brand, category, `${existing}\n${prose}`) || null,
      mode,
    };
  } catch (err) {
    log?.("warn", {
      event: "shopify.listing_prose.error",
      message: err?.message || String(err),
    });
    return { ok: false, reason: err?.message || "openai_error" };
  }
}

/** Truncate at a word boundary (Shopify SEO title ~70, meta ~155–160). */
export function truncateAtWord(text, maxLen) {
  const s = String(text || "").replace(/\s+/g, " ").trim();
  if (!s || !maxLen || s.length <= maxLen) return s;
  const cut = s.slice(0, Math.max(1, maxLen));
  const sp = cut.lastIndexOf(" ");
  const out = (sp > Math.floor(maxLen * 0.55) ? cut.slice(0, sp) : cut).trim();
  return out.replace(/[,:;.-]+$/g, "").trim();
}

const ITEM_TYPE_WORD_RE =
  /\b(?:chairs?|stools?|nightstands?|dressers?|sofas?|sectionals?|loveseats?|tables?|desks?|cabinets?|beds?|benches?|bookcases?|ottomans?|armoires?|sideboards?|buffets?|credenzas?|consoles?|hutch(?:es)?|mirrors?|lamps?|chandeliers?|sconces?|rugs?|paintings?|prints?|canvas|giclee|lithographs?|serigraphs?|sculptures?|vases?|bowls?|trays?|pillows?|handbags?|totes?|wallets?|necklaces?|earrings?|bracelets?)\b/gi;

/**
 * Cut a title to maxLen at a word boundary without losing the item type.
 * The item type is the last type word in the title (Canvas, Bed, Desk), or its
 * last word. When a plain cut would drop it, the words before it are cut instead.
 */
export function truncateTitleKeepingType(title, maxLen) {
  const s = String(title || "").replace(/\s+/g, " ").trim();
  if (!s || !maxLen || s.length <= maxLen) return s;
  // A cut never ends on a joining word (with, and, of).
  const plain = truncateAtWord(s, maxLen)
    .replace(/\s+(?:and|with|of|in|on|for|the|a|an|by|&)$/i, "")
    .replace(/[\s,:;.-]+$/g, "");
  let typeStart = -1;
  let typeEnd = -1;
  for (const match of s.matchAll(ITEM_TYPE_WORD_RE)) {
    typeStart = match.index;
    typeEnd = match.index + match[0].length;
  }
  if (typeStart < 0) {
    const last = s.match(/[A-Za-z][A-Za-z'-]*$/);
    if (last) {
      typeStart = last.index;
      typeEnd = s.length;
    }
  }
  if (typeStart < 0 || typeEnd <= plain.length) return plain;
  const tail = s.slice(typeStart).trim();
  if (tail.length > Math.floor(maxLen / 2)) return plain;
  const front = truncateAtWord(s.slice(0, typeStart).trim(), maxLen - tail.length - 1)
    .replace(/\s+(?:and|with|of|in|on|for|the|a|an|by|&)$/i, "")
    // A possessive left without its noun (Artist's Proof cut to Artist's) goes too.
    .replace(/\s+[A-Za-z]+'s$/i, "")
    .replace(/[\s,:;.-]+$/g, "");
  return `${front} ${tail}`.trim();
}

/** The sentence that says what the item is: the one carrying the most of its name and brand. */
export function answerSentence(sentences, { name, brand } = {}) {
  const list = (sentences || []).filter(Boolean);
  if (!list.length) return "";
  const identity = writeupIdentityWords({ name, brand });
  if (!identity.size) return list[0];
  let best = list[0];
  let bestScore = -1;
  for (const sentence of list) {
    const words = copyStems(sentence);
    let score = 0;
    for (const word of identity) if (words.has(word)) score += 1;
    if (score > bestScore) {
      best = sentence;
      bestScore = score;
    }
  }
  return best;
}

export function splitListingSentences(text) {
  const s = String(text || "").replace(/\s+/g, " ").trim();
  if (!s) return [];
  return s
    .split(/(?<=[.!?])\s+/)
    .map((p) => p.trim())
    .filter(Boolean);
}

/**
 * Lost & Found listing copy is written as SEO + AEO + GEO in one description.
 * Derive Shopify Search engine listing + AEO/GEO metafield values from it.
 *
 * Overrides (optional Intake / payload fields) win when non-empty:
 *   seoTitle, seoDescription, aeoDescription, geoDescription
 */
export function deriveListingSeoFields({
  name,
  description,
  brand,
  category,
  dims,
  seoTitle: seoTitleOverride,
  seoDescription: seoDescriptionOverride,
  aeoDescription: aeoDescriptionOverride,
  geoDescription: geoDescriptionOverride,
} = {}) {
  const cleaned = flowingBuyerProse(stripListingDimJunk(description || ""));
  const sentences = splitListingSentences(cleaned).filter(
    (s) => !/\b(shipping|pickup|pick-up|nationwide|freight|return policy)\b/i.test(s)
  );
  const rawTitle = normalizeFullItemName(name);
  const title =
    String(rawTitle || "")
      .replace(
        /\s*[-–]\s*\d+(?:\.\d+)?\s*[xX×]\s*\d+(?:\.\d+)?(?:\s*[xX×]\s*\d+(?:\.\d+)?\s*H?)?\s*$/i,
        ""
      )
      // The same size token with an H after two numbers (39.5 x 45.5H), also before "- AS IS".
      .replace(
        /\s*[-–—]\s*\d+(?:\.\d+)?(?:\s*[xX×]\s*\d+(?:\.\d+)?){1,2}\s*H?(?=\s*(?:[-–—]\s*AS\s*-?\s*IS\s*)?$)/i,
        ""
      )
      .replace(/\s+/g, " ")
      .trim() || rawTitle;

  // Cut at a word boundary that keeps the item type (the end of the name).
  const seoTitle =
    truncateTitleKeepingType(String(seoTitleOverride || "").trim() || title, 70) || title;

  let seoDescription = flowingBuyerProse(String(seoDescriptionOverride || "").trim());
  if (!seoDescription) {
    seoDescription = sentences[0] || cleaned;
    if (seoDescription.length < 110 && sentences[1]) {
      seoDescription = `${seoDescription} ${sentences[1]}`;
    }
  }
  seoDescription = truncateAtWord(seoDescription, 160);

  let aeoDescription = flowingBuyerProse(String(aeoDescriptionOverride || "").trim());
  if (!aeoDescription) {
    // The answer to "what is this" is the sentence that names the item, not the longest one.
    const product = productSentences(cleaned);
    aeoDescription = answerSentence(product.length ? product : sentences, { name: title, brand }) || cleaned;
  }
  aeoDescription = truncateAtWord(aeoDescription, 400);

  let geoDescription = flowingBuyerProse(String(geoDescriptionOverride || "").trim());
  if (!geoDescription) {
    const fact = (sentences[0] || title || "").replace(/\s+/g, " ").trim();
    const brandName = String(brand || "").trim();
    const bits = [];
    if (fact) bits.push(/[.!?]$/.test(fact) ? fact : `${fact}.`);
    if (
      brandName &&
      !/^unbranded$/i.test(brandName) &&
      !fact.toLowerCase().includes(brandName.toLowerCase())
    ) {
      bits.push(/[.!?]$/.test(brandName) ? brandName : `${brandName}.`);
    }
    bits.push("Offered by Lost & Found Resale in Scottsdale, Arizona.");
    geoDescription = bits.join(" ").replace(/\s+/g, " ").trim();
  }
  geoDescription = truncateAtWord(geoDescription, 400);

  return {
    seoTitle: seoTitle || null,
    seoDescription: seoDescription || null,
    aeoDescription: aeoDescription || null,
    geoDescription: geoDescription || null,
  };
}

/** One page is the whole set for our products. A full page may be cut short, so it is not trusted. */
const METAFIELD_INDEX_PAGE = 250;

function metafieldIndexKey(namespace, key) {
  return `${String(namespace || "").trim().toLowerCase()}.${String(key || "").trim().toLowerCase()}`;
}

/**
 * Read every metafield on the product in ONE call, keyed by namespace.key.
 * Returns null when the read failed or may be incomplete; callers then fall
 * back to the per-key lookup, so a write is never skipped on a guess.
 */
export async function fetchShopifyProductMetafieldIndex(productId, { log } = {}) {
  const id = String(productId || "").trim();
  const base = shopifyRestBase();
  if (!base || !id) return null;
  try {
    const res = await axios.get(`${base}/products/${id}/metafields.json`, {
      headers: shopifyHeaders(),
      params: { limit: METAFIELD_INDEX_PAGE },
      timeout: 20000,
      validateStatus: () => true,
    });
    if (res.status >= 400 || !Array.isArray(res.data?.metafields)) {
      log?.("warn", {
        event: "shopify.metafield.index_failed",
        shopifyProductId: id,
        status: res.status,
      });
      return null;
    }
    const list = res.data.metafields;
    if (list.length >= METAFIELD_INDEX_PAGE) return null;
    const index = new Map();
    for (const row of list) {
      if (!row?.id) continue;
      index.set(metafieldIndexKey(row.namespace, row.key), {
        id: row.id,
        value: row.value,
        type: row.type,
      });
    }
    return index;
  } catch (err) {
    log?.("warn", {
      event: "shopify.metafield.index_failed",
      shopifyProductId: id,
      message: err?.message || String(err),
    });
    return null;
  }
}

/** Pure: what one metafield write needs, given the product's current metafields. */
export function planMetafieldWrite(existing, { namespace, key, value, type = "multi_line_text_field" } = {}) {
  if (!(existing instanceof Map)) return { action: "lookup" };
  const hit = existing.get(metafieldIndexKey(namespace, key));
  if (!hit?.id) return { action: "create" };
  const same =
    String(hit.value ?? "").trim() === String(value ?? "").trim() &&
    (!hit.type || String(hit.type) === String(type));
  return same ? { action: "skip", id: hit.id } : { action: "update", id: hit.id };
}

/**
 * Upsert product metafield via REST (create or update by namespace+key).
 * Prefers `custom`, falls back to `lostfound` if custom definition rejects.
 * With `existing` (fetchShopifyProductMetafieldIndex) the per-key lookup is
 * skipped, and a metafield that already holds this value is not written.
 */
async function upsertShopifyProductMetafield(
  productId,
  { namespace, key, value, type = "multi_line_text_field" },
  { log, existing = null } = {}
) {
  const id = String(productId || "").trim();
  const base = shopifyRestBase();
  const ns = String(namespace || "").trim();
  const k = String(key || "").trim();
  const val = String(value || "").trim();
  if (!base || !id || !ns || !k || !val) return { ok: false, reason: "missing_metafield_args" };

  let existingId = null;
  const planned = planMetafieldWrite(existing, { namespace: ns, key: k, value: val, type });
  if (planned.action === "skip") {
    return { ok: true, id: planned.id, namespace: ns, key: k, unchanged: true };
  }
  if (planned.action === "update") existingId = planned.id;
  if (planned.action === "lookup") {
    try {
      const listRes = await axios.get(`${base}/products/${id}/metafields.json`, {
        headers: shopifyHeaders(),
        params: { namespace: ns, key: k },
        timeout: 20000,
        validateStatus: () => true,
      });
      const list = Array.isArray(listRes.data?.metafields) ? listRes.data.metafields : [];
      const hit = list.find(
        (m) =>
          String(m?.namespace || "").toLowerCase() === ns.toLowerCase() &&
          String(m?.key || "").toLowerCase() === k.toLowerCase()
      );
      if (hit?.id) existingId = hit.id;
    } catch (_err) {
      /* list failed — try create */
    }
  }

  if (existingId) {
    const putRes = await axios.put(
      `${base}/metafields/${existingId}.json`,
      { metafield: { id: existingId, value: val, type } },
      { headers: shopifyHeaders(), timeout: 20000, validateStatus: () => true }
    );
    if (putRes.status >= 400) {
      const reason = formatShopifyErrorBody(putRes.data?.errors ?? putRes.data);
      log?.("warn", {
        event: "shopify.metafield.update_failed",
        shopifyProductId: id,
        namespace: ns,
        key: k,
        status: putRes.status,
        message: reason,
      });
      return { ok: false, reason };
    }
    if (existing instanceof Map) {
      existing.set(metafieldIndexKey(ns, k), { id: existingId, value: val, type });
    }
    return { ok: true, id: existingId, namespace: ns, key: k };
  }

  const postRes = await axios.post(
    `${base}/products/${id}/metafields.json`,
    { metafield: { namespace: ns, key: k, value: val, type } },
    { headers: shopifyHeaders(), timeout: 20000, validateStatus: () => true }
  );
  if (postRes.status >= 400) {
    const reason = formatShopifyErrorBody(postRes.data?.errors ?? postRes.data);
    log?.("warn", {
      event: "shopify.metafield.create_failed",
      shopifyProductId: id,
      namespace: ns,
      key: k,
      status: postRes.status,
      message: reason,
    });
    return { ok: false, reason };
  }
  const createdId = postRes.data?.metafield?.id || null;
  if (existing instanceof Map && createdId) {
    existing.set(metafieldIndexKey(ns, k), { id: createdId, value: val, type });
  }
  return {
    ok: true,
    id: createdId,
    namespace: ns,
    key: k,
  };
}

/**
 * Write SEO (Search engine listing) + AEO/GEO metafields on a Shopify product.
 * SEO → metafields_global_* (Admin: Search engine listing).
 * AEO/GEO → custom.aeo_description / custom.geo_description (fallback lostfound.*).
 */
export async function applyShopifySeoAeoGeo(productId, fields, { log, existingMetafields = null } = {}) {
  const id = String(productId || "").trim();
  const base = shopifyRestBase();
  if (!base || !id || !fields) return { ok: false, reason: "missing_args" };

  const seoTitle = String(fields.seoTitle || "").trim();
  const seoDescription = String(fields.seoDescription || "").trim();
  const result = { ok: true, seo: false, aeo: false, geo: false, details: [] };

  if (seoTitle || seoDescription) {
    const product = { id: Number(id) || id };
    if (seoTitle) product.metafields_global_title_tag = seoTitle;
    if (seoDescription) product.metafields_global_description_tag = seoDescription;
    const res = await axios.put(
      `${base}/products/${id}.json`,
      { product },
      { headers: shopifyHeaders(), timeout: 45000, validateStatus: () => true }
    );
    if (res.status >= 400) {
      const reason = formatShopifyErrorBody(res.data?.errors ?? res.data);
      log?.("warn", {
        event: "shopify.seo_global.update_failed",
        shopifyProductId: id,
        status: res.status,
        message: reason,
      });
      result.ok = false;
      result.details.push({ kind: "seo", ok: false, reason });
    } else {
      result.seo = true;
      result.details.push({ kind: "seo", ok: true });
    }
  }

  async function writePair(kind, key, value) {
    const val = String(value || "").trim();
    if (!val) return;
    let written = await upsertShopifyProductMetafield(
      id,
      { namespace: "custom", key, value: val, type: "multi_line_text_field" },
      { log, existing: existingMetafields }
    );
    if (!written.ok) {
      written = await upsertShopifyProductMetafield(
        id,
        { namespace: "lostfound", key, value: val, type: "multi_line_text_field" },
        { log, existing: existingMetafields }
      );
    }
    result[kind] = Boolean(written.ok);
    result.details.push({ kind, ok: written.ok, namespace: written.namespace, reason: written.reason });
    if (!written.ok) result.ok = false;
  }

  await writePair("aeo", "aeo_description", fields.aeoDescription);
  await writePair("geo", "geo_description", fields.geoDescription);

  log?.("info", {
    event: "shopify.seo_aeo_geo.applied",
    shopifyProductId: id,
    seo: result.seo,
    aeo: result.aeo,
    geo: result.geo,
  });
  return result;
}

function dimTagValues(dims = {}) {
  const out = [];
  const w = positiveListingNumber(dims.width ?? dims.dimWidth);
  const d = positiveListingNumber(dims.depth ?? dims.dimDepth ?? dims.dimLength ?? dims.length);
  const h = positiveListingNumber(dims.height ?? dims.dimHeight);
  const wt = positiveListingNumber(dims.weightLbs ?? dims.weight);
  if (w != null) out.push(`Width: ${formatDimNumber(w)}`);
  if (d != null) out.push(`Depth: ${formatDimNumber(d)}`);
  if (h != null) out.push(`Height: ${formatDimNumber(h)}`);
  // Match existing Shopify/ecommerce tag shape: "Weight: 13" (no unit suffix).
  if (wt != null) out.push(`Weight: ${formatDimNumber(wt)}`);
  return out;
}

function managedListingTagSet(_merchType) {
  // Only vertical codes are always managed — X is Accessories website-cat, not default furniture.
  const set = new Set(["fh", "lg"]);
  set.add("furniture & home");
  set.add("luxury goods");
  set.add("furniture");
  set.add("luxury");
  set.add("livingroom");
  set.add("diningroom");
  // Both verticals. Selected FH/LG + category code is added back by the caller.
  // X remains only when Furniture Accessories is selected.
  const opts = FURNITURE_WEBSITE_CATS.concat(LUXURY_WEBSITE_CATS);
  for (const c of opts) {
    set.add(c.code.toLowerCase());
    set.add(c.label.toLowerCase());
    set.add(c.label.toLowerCase().replace(/\s+/g, ""));
    set.add(c.label.toLowerCase().replace(/\s+/g, "-"));
  }
  return set;
}

/**
 * Keep Traxia/system Shopify tags; replace Intake listing tags + dim tags
 * with the exact ecommerceTags (+ department / website category / dims).
 * LG/FH (and a single category letter) in ecommerceTags always win over merchType.
 */
export function mergeShopifyListingTags({
  existingTags,
  ecommerceTags,
  merchType,
  websiteCategory,
  dims,
} = {}) {
  const type = resolveMerchTypeFromFhLgTags({ ecommerceTags, existingTags, merchType });
  const cat = resolveWebsiteCategoryFromFhLgTags({
    ecommerceTags,
    existingTags,
    websiteCategory,
  });
  const managed = managedListingTagSet(type);
  const kept = parseTagList(existingTags).filter((t) => {
    if (DIM_TAG_RE.test(t)) return false;
    if (JUNK_STATUS_TAG_RE.test(t) || LOCATION_TAG_RE.test(t)) return false;
    if (managed.has(t.toLowerCase())) return false;
    return true;
  });
  // Field-mapped codes ONLY from vertical + website category (FH/LG + letter).
  // SEO keywords come from ecommerceTags / Helper — never dump department/cat labels.
  const structured = structuredEcommerceCodes(type, cat);
  const listing = parseTagList(ecommerceTags).filter((t) => {
    const low = String(t || "").trim().toLowerCase();
    if (!low) return false;
    if (JUNK_STATUS_TAG_RE.test(t) || LOCATION_TAG_RE.test(t)) return false;
    if (managed.has(low)) return false;
    if (structured.some((c) => c.toLowerCase() === low)) return false;
    return true;
  });
  const add = [...structured, ...listing];
  add.push(...dimTagValues(dims));
  const out = [...kept];
  for (const tag of add) {
    const t = String(tag || "").trim();
    if (!t) continue;
    if (out.some((x) => x.toLowerCase() === t.toLowerCase())) continue;
    out.push(t);
  }
  return out;
}

async function updateShopifyProductListing(productId, patch, { log, existingProduct } = {}) {
  const id = String(productId || "").trim();
  const base = shopifyRestBase();
  if (!base || !id) return { ok: false, reason: "missing_product" };
  const product = { id: Number(id) || id };
  if (patch?.nameOnly) {
    delete patch.bodyHtml;
    delete patch.tags;
    delete patch.price;
    delete patch.retailPrice;
    delete patch.images;
    delete patch.productType;
    delete patch.vendor;
    delete patch.seoTitle;
    delete patch.seoDescription;
  }
  // NEVER send empty body_html — that is the Traxia price-sync wipe root cause.
  if (patch.bodyHtml != null) {
    const html = String(patch.bodyHtml).trim();
    if (html && !isDimsOnlyListingText(html)) {
      product.body_html = patch.bodyHtml;
    } else {
      log?.("warn", {
        event: "shopify.listing_update.refuse_empty_body",
        shopifyProductId: id,
      });
    }
  }
  if (patch.tags != null) product.tags = Array.isArray(patch.tags) ? patch.tags.join(", ") : String(patch.tags);
  if (patch.status === "active" || patch.status === "draft") product.status = patch.status;
  if (patch.productType != null && String(patch.productType).trim() !== "") {
    product.product_type = String(patch.productType).trim();
  }
  if (patch.vendor != null && String(patch.vendor).trim() !== "") {
    product.vendor = String(patch.vendor).trim();
  }
  if (patch.title != null && String(patch.title).trim() !== "") {
    product.title = String(patch.title).trim();
  }
  // Native Shopify Search engine listing (Admin → product → Search engine listing).
  if (patch.seoTitle != null && String(patch.seoTitle).trim() !== "") {
    product.metafields_global_title_tag = String(patch.seoTitle).trim();
  }
  if (patch.seoDescription != null && String(patch.seoDescription).trim() !== "") {
    product.metafields_global_description_tag = String(patch.seoDescription).trim();
  }
  // Price in the SAME product PUT (never a separate hammering variants call). Never send images.
  const priceRaw = patch.price != null ? patch.price : patch.retailPrice;
  if (priceRaw != null && String(priceRaw).trim() !== "") {
    const priceNum = Number(priceRaw);
    if (Number.isFinite(priceNum) && priceNum >= 0) {
      const variants = Array.isArray(existingProduct?.variants) ? existingProduct.variants : [];
      const first = variants[0];
      if (first?.id) {
        product.variants = [{ id: first.id, price: priceNum.toFixed(2) }];
      }
    }
  }
  // Refuse sparse wipe payloads that only clear content.
  // Never send images. Shopify deletes every file when a product PUT includes images: [].
  const safeProduct = listingProductOmitsImages(product);
  const keys = Object.keys(safeProduct).filter((k) => k !== "id");
  if (!keys.length) {
    return { ok: true, product: existingProduct || null, skipped: true };
  }
  const res = await axios.put(
    `${base}/products/${id}.json`,
    { product: safeProduct },
    { headers: shopifyHeaders(), timeout: 45000, validateStatus: () => true }
  );
  if (res.status >= 400) {
    const reason = formatShopifyErrorBody(res.data?.errors ?? res.data);
    log?.("warn", {
      event: "shopify.listing_update.failed",
      shopifyProductId: id,
      status: res.status,
      message: reason,
    });
    return { ok: false, reason: `Shopify product update HTTP ${res.status}: ${reason}` };
  }
  return { ok: true, product: res.data?.product || null };
}

/**
 * Intake listing / taxonomy change → Shopify description + tags (+ product type)
 * + SEO Search engine listing + AEO/GEO metafields.
 * Same description + ecommerce tags Intake uses for Traxia e-com.
 *
 * Body fields: intakeItemId?, shopifyProductId?, name?,
 * description?, ecommerceTags?, merchType?, websiteCategory?, category?,
 * brand?, dims? { width, depth, height, weightLbs },
 * seoTitle?, seoDescription?, aeoDescription?, geoDescription?
 */
/** Per Intake item: one in-flight listing-update; newer payloads collapse into next run. */
const listingUpdateInflight = new Map(); // key -> { promise, startedAt }
const listingUpdatePending = new Map(); // key -> { payload, log }
/** Quiet ms before draining coalesced follow-up (latest wins). */
const LISTING_PENDING_SETTLE_MS = 250;
/** At most this many corrective rebuilds after a settled write (stops AS IS paste loops). */
const LISTING_CORRECTIVE_MAX_PASSES = 2;
/** Drop a stuck in_flight entry so coalesce can run once with the latest payload. */
const LISTING_INFLIGHT_MAX_MS = Math.max(
  30_000,
  Math.min(5 * 60_000, Number(process.env.LISTING_UPDATE_MAX_MS || 120_000) || 120_000)
);

function listingUpdateKey(payload = {}) {
  const id = String(payload.intakeItemId || payload.itemId || "").trim();
  if (id && UUID_RE.test(id)) return `intake:${id}`;
  const pid = String(payload.shopifyProductId || "").trim();
  if (pid) return `shopify:${pid}`;
  const name = normalizeFullItemName(payload.name || payload.title || "");
  return name ? `name:${name.toLowerCase()}` : `anon:${Date.now()}`;
}

/** Test helper: clear coalesce maps between cases. */
export function resetListingUpdateCoalesceForTests() {
  listingUpdateInflight.clear();
  listingUpdatePending.clear();
}

export function listingUpdateCoalesceSnapshotForTests() {
  return {
    inflight: [...listingUpdateInflight.keys()],
    pending: [...listingUpdatePending.keys()],
  };
}

function listingInflightEntry(key) {
  const entry = listingUpdateInflight.get(key);
  if (!entry) return null;
  const started = Number(entry.startedAt) || 0;
  if (started && Date.now() - started > LISTING_INFLIGHT_MAX_MS) {
    listingUpdateInflight.delete(key);
    return null;
  }
  return entry;
}

/**
 * The Intake fields the body rebuild reads (description base, notes, Extra
 * Information, dims, tags), as sent with the save or just loaded here. Used
 * only when it is for this item; anything else falls back to a fetch.
 */
export function inlineIntakeItem(payload = {}, intakeItemId = "") {
  const row = payload?.intakeItem;
  if (!row || typeof row !== "object" || Array.isArray(row)) return null;
  const id = String(intakeItemId || "").trim();
  if (!id || String(row.id || "").trim() !== id) return null;
  return row;
}

async function rebuildPayloadIfQueued(payload = {}, { log, loadItem } = {}) {
  if (!payload?.__rebuildFromCurrent && !payload?.__retryAttempt && !payload?.__corrective) {
    return { action: "write", payload };
  }
  const id = String(payload.intakeItemId || payload.itemId || "").trim();
  if (!id) return { action: "drop", reason: "missing_item" };
  // Item fields only: no audit history, no repair passes.
  const load =
    loadItem || ((itemId, opts) => fetchIntakeItemById(itemId, { ...opts, lite: true }));
  let item = null;
  try {
    item = await load(id, { log });
  } catch (err) {
    log?.("warn", {
      event: "shopify.listing_update.reload_failed",
      message: err?.message || String(err),
    });
    return { action: "drop", reason: "reload_failed" };
  }
  const resolved = resolveQueuedListingJob(payload, item);
  if (resolved.action !== "write") return resolved;
  return {
    action: "write",
    payload: {
      ...resolved.payload,
      __retryAttempt: payload.__retryAttempt,
      __corrective: payload.__corrective === true,
      __correctivePass: Number(payload.__correctivePass || 0) || 0,
      __forceCleanBody: payload.__forceCleanBody === true || payload.forceCleanBody === true,
      replaceBody: true,
      rewriteBody: true,
      forceRefresh: payload.__forceCleanBody === true || payload.forceCleanBody === true || payload.forceRefresh === true,
      __rebuiltFromCurrent: true,
      allowOpenAi: false,
      // The row just loaded is the current one. The write reuses it instead of
      // reading the item a second time. A stale copy from the first payload is dropped.
      intakeItem: item && String(item.id || "").trim() === id ? item : undefined,
    },
  };
}

/** A listing-update result that wrote the Shopify product (not deferred, not a dropped job). */
export function listingResultWroteShopify(result) {
  return (
    Boolean(result?.ok) &&
    !result.deferred &&
    !result.skipped &&
    result.status !== "coalesced" &&
    Boolean(result.shopifyProductId)
  );
}

/**
 * onFollowUpWrite(result) is called after a coalesced follow-up or a corrective
 * rebuild wrote Shopify. Those runs start on a timer with no caller waiting, so
 * this is how the caller of the first write learns of them (echo suppression).
 */
export async function handleIntakeListingUpdate(
  payload = {},
  { log, loadItem, execute, onFollowUpWrite } = {}
) {
  let active = payload;
  if (payload.__rebuildFromCurrent || payload.__retryAttempt || payload.__corrective) {
    const resolved = await rebuildPayloadIfQueued(payload, { log, loadItem });
    if (resolved.action !== "write") {
      return { ok: true, skipped: true, reason: resolved.reason || "stale_job", name: resolved.name || null };
    }
    active = resolved.payload;
  }
  const key = listingUpdateKey(active);
  if (listingInflightEntry(key)) {
    // Latest wins: one follow-up rebuilds from current Intake (name + from-scratch body).
    // Do not force OpenAI here — __rebuildFromCurrent already reloads Intake + SET body.
    listingUpdatePending.set(key, {
      payload: {
        ...active,
        __rebuildFromCurrent: true,
        replaceBody: true,
        rewriteBody: true,
        allowOpenAi: false,
      },
      log,
    });
    log?.("info", {
      event: "shopify.listing_update.coalesced",
      key,
      reason: "in_flight",
    });
    return {
      // ok:false + deferred so callers (PE / syncAfter) do not treat this as a write.
      ok: false,
      status: "coalesced",
      deferred: true,
      reason: "in_flight_collapsed",
      intakeItemId: String(payload.intakeItemId || payload.itemId || "").trim() || null,
      shopifyProductId: String(payload.shopifyProductId || "").trim() || null,
    };
  }

  const runNow = execute || handleIntakeListingUpdateNow;
  const run = (async () => {
    try {
      return await runNow(active, { log });
    } finally {
      listingUpdateInflight.delete(key);
      const pending = listingUpdatePending.get(key);
      if (pending) {
        listingUpdatePending.delete(key);
        // Short settle so a trailing PE burst collapses into ONE latest apply.
        setTimeout(() => {
          void handleIntakeListingUpdate(pending.payload, {
            log: pending.log,
            loadItem,
            execute,
            onFollowUpWrite,
          })
            .then((result) => {
              if (onFollowUpWrite && listingResultWroteShopify(result)) onFollowUpWrite(result);
            })
            .catch(() => {});
        }, LISTING_PENDING_SETTLE_MS);
      }
    }
  })();
  listingUpdateInflight.set(key, { promise: run, startedAt: Date.now() });
  return run;
}

// One description generation per item per change. The attempt is remembered by
// the item's recorded facts, so a model that is down, or that answers thin
// again, is not asked a second time until one of those facts changes.
// Same rule as Pricing Engine claim_generation.
const LISTING_GENERATION_MEMORY_MAX = 5000;
const listingGenerationAttempts = new Map();

/** One hash of the recorded facts a description is written from. */
export function listingGenerationFactsKey({
  name,
  brand,
  category,
  notes,
  conditionIssues,
  dims,
  description,
} = {}) {
  const parts = [name, brand, category, notes, conditionIssues].map((value) =>
    String(value ?? "").trim()
  );
  for (const axis of ["width", "depth", "height", "weightLbs"]) {
    const value = dims?.[axis];
    parts.push(value == null ? "" : String(value).trim());
  }
  // Only whether there is a description, so storing one is not a new change and deleting one is.
  parts.push(isDimsOnlyListingText(description) ? "blank" : "text");
  return createHash("sha256").update(JSON.stringify(parts)).digest("hex").slice(0, 16);
}

/** True once per item per change. A second ask for the same facts is refused. */
export function claimListingGeneration(itemKey, factsKey) {
  const key = String(itemKey || "").trim();
  if (!key) return false;
  if (listingGenerationAttempts.get(key) === factsKey) return false;
  if (listingGenerationAttempts.size >= LISTING_GENERATION_MEMORY_MAX && !listingGenerationAttempts.has(key)) {
    const stale = [...listingGenerationAttempts.keys()].slice(0, LISTING_GENERATION_MEMORY_MAX / 2);
    for (const old of stale) listingGenerationAttempts.delete(old);
  }
  listingGenerationAttempts.set(key, factsKey);
  return true;
}

/** Forget generation attempts (one item, or everything). Tests and restarts. */
export function resetListingGenerationMemory(itemKey = null) {
  if (itemKey == null) listingGenerationAttempts.clear();
  else listingGenerationAttempts.delete(String(itemKey).trim());
}

async function handleIntakeListingUpdateNow(payload = {}, { log } = {}) {
  const intakeItemId = String(payload.intakeItemId || payload.itemId || "").trim() || null;
  const fullName = normalizeFullItemName(payload.name || payload.title || "");
  const priorName = normalizeFullItemName(payload.priorName || payload.previousName || "");
  let productId = String(payload.shopifyProductId || "").trim() || null;
  let product = null;

  if (productId) {
    product = await fetchShopifyProductById(productId, { log });
  }
  // Rename: find by prior title first, then new title.
  if (!product && priorName) {
    product = await fetchShopifyProductByFullName(priorName, { log });
    if (product?.id) productId = String(product.id);
  }
  if (!product && fullName) {
    product = await fetchShopifyProductByFullName(fullName, { log });
    if (product?.id) productId = String(product.id);
  }
  if (!product || !productId) {
    if (intakeItemId) {
      await stampIntakeShopifyImageSync(intakeItemId, {
        status: "pending",
        detail: "Shopify listing update deferred — product not found yet",
        log,
      });
    }
    return {
      ok: false,
      deferred: true,
      status: "product_not_found",
      reason: "Shopify product not found for listing update",
      intakeItemId,
      name: fullName || null,
    };
  }

  const tagsHint =
    payload.ecommerceTags != null
      ? payload.ecommerceTags
      : payload.tags != null
        ? payload.tags
        : "";
  // LG/FH in ecommerce tags always win — never default furniture over merchant LG.
  const merchType = resolveMerchTypeFromFhLgTags({
    ecommerceTags: tagsHint,
    existingTags: product?.tags,
    merchType: payload.merchType || payload.vertical,
  });
  const websiteCategory = resolveWebsiteCategoryFromFhLgTags({
    ecommerceTags: tagsHint,
    existingTags: product?.tags,
    websiteCategory: String(payload.websiteCategory || payload.subcategory || "").trim(),
  });
  const taxonomy = resolveIntakeShopifyTaxonomy({
    merchType,
    websiteCategory,
    category: payload.category,
    name: fullName || product.title,
  });
  const catLabel = taxonomy.category || taxonomy.websiteLabel || String(payload.category || "").trim() || "";
  const description = payload.description != null ? String(payload.description) : null;
  const ecommerceTags =
    payload.ecommerceTags != null
      ? payload.ecommerceTags
      : payload.tags != null
        ? payload.tags
        : null;
  const dims = payload.dims && typeof payload.dims === "object" ? payload.dims : {
    width: payload.dimWidth ?? payload.width,
    depth: payload.dimDepth ?? payload.dimLength ?? payload.depth,
    height: payload.dimHeight ?? payload.height,
    weightLbs: payload.weightLbs ?? payload.weight,
  };
  const brand = payload.brand != null ? String(payload.brand).trim() : "";

  const seoFields = deriveListingSeoFields({
    name: fullName || product.title,
    description: description != null ? description : "",
    brand,
    category: catLabel || String(payload.category || "").trim(),
    dims,
    seoTitle: payload.seoTitle || payload.seo_title,
    seoDescription: payload.seoDescription || payload.seo_description,
    aeoDescription: payload.aeoDescription || payload.aeo_description,
    geoDescription: payload.geoDescription || payload.geo_description,
  });

  // Resolve body prose: never dims/weight-only. forceRefresh / empty → regenerate Helper copy.
  const forceRefresh = payload.forceRefresh === true || payload.force_refresh === true;
  const replaceBody =
    payload.replaceBody === true ||
    payload.replace_body === true ||
    payload.rewriteBody === true ||
    payload.rewrite_body === true ||
    payload.__forceCleanBody === true ||
    payload.forceCleanBody === true;
  const allowOpenAi = payload.allowOpenAi !== false && payload.allow_openai !== false;
  let bodyProse = description != null ? String(description) : "";
  // An empty string is a cleared Traxia note. `||` would skip it and keep the old flaw.
  let notes = payload.notes ?? payload.extraInformation ?? payload.extra ?? "";
  let conditionNote = payload.conditionIssues ?? payload.condition_issues ?? "";
  const clearedNote =
    payload.clearedNote === true ||
    ((payload.conditionIssues === "" || payload.condition_issues === "") && !String(notes).trim());
  let tagsForGen = ecommerceTags != null ? ecommerceTags : "";
  const liveShopifyPlain = stripListingDimJunk(product.body_html || "").replace(/\s+/g, " ").trim();
  const shopifyBodyPolluted =
    !liveShopifyPlain ||
    /AS\s*IS\s*:/i.test(liveShopifyPlain) ||
    descriptionHasStackedWear(liveShopifyPlain) ||
    descriptionHasStackedProductProse(liveShopifyPlain, {
      name: fullName || product.title || "",
      brand,
      category: String(payload.category || "").trim(),
    });
  // Empty Shopify body or dual/AS IS garbage → always rebuild from Intake finalize (not splice).
  // From-scratch: never concat prior Shopify HTML.
  const mustRebuildBody =
    forceRefresh || replaceBody || shopifyBodyPolluted || isDimsOnlyListingText(bodyProse) || !liveShopifyPlain;
  if (mustRebuildBody) {
    if (intakeItemId) {
      try {
        // Pricing Engine sends the current Intake fields with the save, so the
        // Shopify write does not wait for an item GET. Without them, read the
        // item without its audit history (lite).
        const intake =
          inlineIntakeItem(payload, intakeItemId) ||
          (await fetchIntakeItemById(intakeItemId, { log, lite: true }));
        if (intake) {
          if (!clearedNote) {
            if (!String(notes || "").trim()) notes = intake.notes || "";
            // Extra Information lands on conditionIssues — must not stay empty when notes already set.
            if (!String(conditionNote || "").trim()) {
              conditionNote = intake.conditionIssues || intake.condition_issues || "";
            }
          }
          if (forceRefresh || replaceBody || shopifyBodyPolluted || !String(bodyProse || "").trim()) {
            const intakeDesc =
              intake.description ||
              intake.ecommerceDescription ||
              intake.listingDescription ||
              "";
            // Raw Intake base only — finalize below rewrites from scratch (one paragraph + notes).
            if (intakeDesc) bodyProse = intakeDesc;
          }
          // Body-only rebuild still needs Intake dims for the HTML footer — never
          // writes title/price/dim product fields when syncFields omits them.
          if (
            positiveMeasure(dims.width) == null &&
            positiveMeasure(intake.dimWidth ?? intake.dim_width) != null
          ) {
            dims.width = positiveMeasure(intake.dimWidth ?? intake.dim_width);
          }
          if (
            positiveMeasure(dims.depth) == null &&
            positiveMeasure(intake.dimDepth ?? intake.dim_depth ?? intake.dimLength ?? intake.dim_length) !=
              null
          ) {
            dims.depth = positiveMeasure(
              intake.dimDepth ?? intake.dim_depth ?? intake.dimLength ?? intake.dim_length
            );
          }
          if (
            positiveMeasure(dims.height) == null &&
            positiveMeasure(intake.dimHeight ?? intake.dim_height) != null
          ) {
            dims.height = positiveMeasure(intake.dimHeight ?? intake.dim_height);
          }
          if (
            positiveMeasure(dims.weightLbs) == null &&
            positiveMeasure(intake.weightLbs ?? intake.weight_lbs) != null
          ) {
            dims.weightLbs = positiveMeasure(intake.weightLbs ?? intake.weight_lbs);
          }
          if (!tagsForGen) tagsForGen = intake.ecommerceTags || intake.tags || "";
        }
      } catch (_err) {
        /* optional */
      }
    }
    // At most one generation per item per change: a model that is down or
    // answers thin again is not asked again while the item's facts are the same.
    const generationClaimed =
      allowOpenAi &&
      (forceRefresh || isDimsOnlyListingText(bodyProse)) &&
      claimListingGeneration(
        intakeItemId || productId,
        listingGenerationFactsKey({
          name: fullName || product.title,
          brand,
          category: catLabel || String(payload.category || "").trim(),
          notes,
          conditionIssues: conditionNote,
          dims,
          description: bodyProse,
        })
      );
    if (generationClaimed) {
      const generated = await generateShopifyListingProse(
        {
          name: fullName || product.title,
          notes,
          description: bodyProse,
          brand,
          category: catLabel || String(payload.category || "").trim(),
          websiteCategory,
          merchType,
          ecommerceTags: tagsForGen,
          dims,
        },
        { log }
      );
      if (generated.ok && generated.description) {
        bodyProse = generated.description;
        if (
          forceRefresh ||
          ecommerceTags == null ||
          String(ecommerceTags).trim() === ""
        ) {
          if (generated.tags) payload.ecommerceTags = generated.tags;
        }
      } else if (isDimsOnlyListingText(bodyProse) || descriptionIsBlankOrFiller(bodyProse)) {
        // The model gave nothing and the item has no description (or only the
        // old fixed filler sentence): a short paragraph from recorded fields.
        // An item with a description keeps it.
        const factual = factualListingCopy({
          name: fullName || product.title,
          brand,
          category: catLabel || String(payload.category || "").trim(),
        });
        if (factual) bodyProse = factual;
      }
    }
  }

  const syncFields = Array.isArray(payload.syncFields) ? payload.syncFields : null;
  const nameOnly =
    payload.nameOnly === true ||
    (Array.isArray(syncFields) && syncFields.length === 1 && syncFields[0] === "name");
  const scoped = nameOnly || Array.isArray(syncFields);
  const allowField = (field) => {
    if (!scoped) return true;
    if (nameOnly) return field === "name";
    // replaceBody / polluted Shopify body always allows a body write even when syncFields omitted it.
    if (field === "body" && (replaceBody || shopifyBodyPolluted)) return true;
    return syncFields.includes(field);
  };
  const patch = {};
  // ALWAYS: prose first, then Intake notes (when not already in the prose), then ONE
  // Dimensions+Weight footer. Never dims-only. Never blank body.
  // Never preserve warehouse/ops scribbles as the public description.
  // Never concatenate with the prior Shopify dual paragraph — finalize collapses to one.
  const rawListingProse = bodyProse;
  bodyProse = appendIntakeNotesToDescription(
    bodyProse,
    notes,
    clearedNote ? "" : conditionNote,
    listingBuildItemFromRow(
      {
        name: fullName || product.title || "",
        brand: payload.brand || brand || "",
        merchType,
        ecommerceTags: String(tagsForGen || ecommerceTags || ""),
        websiteCategory,
        category: payload.category,
        dimWidth: dims?.width ?? dims?.dimWidth,
        dimDepth: dims?.depth ?? dims?.dimDepth ?? dims?.dimLength,
        dimHeight: dims?.height ?? dims?.dimHeight,
        weightLbs: dims?.weightLbs ?? dims?.weight,
        dimWidthUnit: dims?.dimWidthUnit ?? dims?.dim_width_unit ?? payload.dimWidthUnit,
        dimDepthUnit:
          dims?.dimDepthUnit ??
          dims?.dim_depth_unit ??
          dims?.dimLengthUnit ??
          payload.dimDepthUnit,
        dimHeightUnit: dims?.dimHeightUnit ?? dims?.dim_height_unit ?? payload.dimHeightUnit,
      }
    )
  );
  const clearedBody =
    !nameOnly &&
    clearedNote &&
    listingCarriesStaleFlaw(product.body_html || "", "", "", {
      name: fullName || product.title || "",
    });
  const listingDims = {
    ...resolveListingDims(dims, product.body_html || ""),
    websiteCategory:
      payload.websiteCategory ||
      payload.website_category ||
      dims?.websiteCategory ||
      dims?.website_category ||
      "",
    category: payload.category || dims?.category || "",
    dimWidthUnit: dims?.dimWidthUnit || dims?.dim_width_unit || payload.dimWidthUnit || "",
    dimDepthUnit:
      dims?.dimDepthUnit ||
      dims?.dim_depth_unit ||
      dims?.dimLengthUnit ||
      dims?.dim_length_unit ||
      payload.dimDepthUnit ||
      "",
    dimHeightUnit: dims?.dimHeightUnit || dims?.dim_height_unit || payload.dimHeightUnit || "",
  };
  // From-scratch SET: discard prior Shopify body entirely whenever we write body/dims.
  if (
    (allowField("body") || allowField("dims") || clearedBody || replaceBody || shopifyBodyPolluted || !liveShopifyPlain) &&
    !isDimsOnlyListingText(bodyProse)
  ) {
    const html = composeShopifyBodyHtml(bodyProse, listingDims);
    if (html) patch.bodyHtml = html;
  } else if (
    !isDimsOnlyListingText(product.body_html || "") &&
    !isInternalOpsListingNote(product.body_html || "") &&
    !listingProseIsThin(product.body_html || "")
  ) {
    // Preserve existing Shopify body if we would otherwise wipe real marketing copy.
    log?.("info", {
      event: "shopify.listing_update.preserve_existing_body",
      shopifyProductId: productId,
      intakeItemId,
    });
  } else {
    if (isInternalOpsListingNote(product.body_html || "")) {
      log?.("info", {
        event: "shopify.listing_update.ignore_internal_ops_body",
        shopifyProductId: productId,
        intakeItemId,
        sample: stripListingDimJunk(product.body_html || "").slice(0, 120),
      });
    }
    log?.("warn", {
      event: "shopify.listing_update.skip_dims_only_body",
      shopifyProductId: productId,
      intakeItemId,
      name: (fullName || "").slice(0, 120),
    });
  }
  let tagsForMerge =
    payload.ecommerceTags != null
      ? payload.ecommerceTags
      : ecommerceTags != null
        ? ecommerceTags
        : "";
  if (proseHasInventedMaker(rawListingProse, payload.brand || "")) {
    tagsForMerge = freshListingTags({
      name: fullName || product.title || "",
      brand: payload.brand || "",
      websiteCategory: websiteCategory || taxonomy.websiteCode || "",
      merchType,
    });
    payload.ecommerceTags = tagsForMerge;
  }
  // Taxonomy tags (FH + B Bedroom, etc.) must land even when body is preserved —
  // otherwise syncAfter sees Traxia Category: ACCESSORIES and writes Accessories metafields.
  const hasTaxonomySignal = Boolean(
    websiteCategory || taxonomy.websiteCode || taxonomy.category || tagsForMerge || dims
  );
  if ((allowField("tags") || allowField("dims")) && hasTaxonomySignal) {
    patch.tags = mergeShopifyListingTags({
      existingTags: product.tags,
      ecommerceTags: tagsForMerge || "",
      merchType: taxonomy.merchType,
      websiteCategory: websiteCategory || taxonomy.websiteCode || taxonomy.category,
      dims: allowField("dims") ? listingDims : dims,
    });
    const drop = tagMakerTokens(
      [...patch.tags, String(tagsForMerge || ""), String(product.tags || "")].join(", "),
      fullName || product.title || "",
      payload.brand || "",
      payload.category || "",
      `${rawListingProse}\n${product.body_html || ""}`
    );
    if (drop.size) {
      patch.tags = patch.tags.filter((tag) => !tagSegments(tag).some((part) => drop.has(part)));
    }
  }
  // Product type = Intake website room (Bedroom), never Traxia BED / Accessories junk.
  if (allowField("tags") && taxonomy.category) patch.productType = taxonomy.category;
  if (allowField("tags") && brand) patch.vendor = brand;
  // Retail price from Intake in the same PUT (price change must not wipe body).
  const priceFromPayload =
    payload.price != null
      ? payload.price
      : payload.retailPrice != null
        ? payload.retailPrice
        : payload.retail_price;
  if (allowField("price") && priceFromPayload != null && String(priceFromPayload).trim() !== "") {
    patch.price = priceFromPayload;
  }
  // Rename ONLY when the name field changed. Dim sync must never rewrite the
  // Shopify title (keeps Traxia/user text like "bernis karfen" and 42X23X75).
  if (
    allowField("name") &&
    fullName &&
    normalizeFullItemName(product.title) !== fullName
  ) {
    patch.title = fullName;
    if (nameOnly) patch.nameOnly = true;
  }
  const channelStatus = String(payload.shopifyStatus || payload.status || "").trim().toLowerCase();
  if (channelStatus === "active" || channelStatus === "draft") {
    if (String(product.status || "").toLowerCase() !== channelStatus) {
      patch.status = channelStatus;
    }
  }
  if (!nameOnly && seoFields.seoTitle) patch.seoTitle = seoFields.seoTitle;
  if (!nameOnly && seoFields.seoDescription) patch.seoDescription = seoFields.seoDescription;

  const needsTaxonomyMetafields = !nameOnly && Boolean(taxonomy.department && taxonomy.category);
  if (
    !Object.keys(patch).length &&
    !seoFields.aeoDescription &&
    !seoFields.geoDescription &&
    !needsTaxonomyMetafields
  ) {
    return {
      ok: true,
      status: "noop",
      shopifyProductId: productId,
      intakeItemId,
    };
  }

  // Product PUT + AEO/GEO + department/Furniture & Home metafields in parallel.
  // Intake website category is written directly so syncAfter cannot leave Accessories on beds.
  const needsProductPut = Object.keys(patch).length > 0;
  const needsAeoGeo = !nameOnly && Boolean(seoFields.aeoDescription || seoFields.geoDescription);
  // The product PUT (title + body + tags) is issued first, so it takes the
  // first Shopify slot. The metafields are then read ONCE, and only the ones
  // whose value changed are written.
  const productWrite = needsProductPut
    ? updateShopifyProductListing(productId, patch, { log, existingProduct: product })
    : Promise.resolve({ ok: true, product });
  const metafieldIndex =
    needsAeoGeo || needsTaxonomyMetafields
      ? fetchShopifyProductMetafieldIndex(productId, { log })
      : Promise.resolve(null);
  const [updated, seoMeta, taxonomyMeta] = await Promise.all([
    productWrite,
    needsAeoGeo
      ? metafieldIndex.then((existingMetafields) =>
          applyShopifySeoAeoGeo(
            productId,
            {
              seoTitle: null,
              seoDescription: null,
              aeoDescription: seoFields.aeoDescription,
              geoDescription: seoFields.geoDescription,
            },
            { log, existingMetafields }
          )
        )
      : Promise.resolve({ ok: true, aeo: false, geo: false }),
    needsTaxonomyMetafields
      ? metafieldIndex.then((existingMetafields) =>
          applyIntakeShopifyTaxonomyMetafields(
            productId,
            {
              department: taxonomy.department,
              category: taxonomy.category,
              merchType: taxonomy.merchType,
            },
            { log, existingMetafields }
          )
        )
      : Promise.resolve({ ok: true, skipped: true }),
  ]);
  if (!updated.ok) {
    const failure = {
      ok: false,
      status: "update_failed",
      reason: updated.reason || "update_failed",
      shopifyProductId: productId,
      intakeItemId,
    };
    if (listingFailureShouldRetry(failure)) {
      if (intakeItemId) {
        await stampIntakeShopifyImageSync(intakeItemId, {
          status: "pending",
          shopifyProductId: productId,
          detail: "Shopify sync error: rate limited, will retry",
          log,
        });
      }
      scheduleListingUpdateRetry(payload, { log });
      return { ...failure, status: "update_pending", retry: true };
    }
    if (intakeItemId) {
      await stampIntakeShopifyImageSync(intakeItemId, {
        status: "failed",
        shopifyProductId: productId,
        detail: `Shopify listing update failed: ${updated.reason || "error"}`,
        log,
      });
    }
    return failure;
  }

  if (intakeItemId) {
    // Explicitly no resync — this stamp must not bounce into another listing-update.
    await stampIntakeShopifyImageSync(intakeItemId, {
      status: "synced",
      shopifyProductId: productId,
      traxiaSku: extractShopifyVariantSku(updated.product || product),
      detail: "Shopify listing synced (description / tags / dims / SEO·AEO·GEO)",
      resyncListing: false,
      log,
    });
  }
  // Cache title→id so ensure/photo-added stop polling.
  if (fullName && productId) {
    titlePollHitCache.set(fullName.toLowerCase(), {
      product: updated.product || product,
      at: Date.now(),
    });
  }
  log?.("info", {
    event: "shopify.listing_update.ok",
    shopifyProductId: productId,
    intakeItemId,
    updatedKeys: Object.keys(patch),
    taxonomy: {
      department: taxonomy.department,
      category: taxonomy.category,
      websiteCategory: websiteCategory || taxonomy.websiteCode || null,
      metafieldsOk: taxonomyMeta?.ok !== false,
    },
    seo: Boolean(seoFields.seoTitle || seoFields.seoDescription),
    aeo: Boolean(seoMeta?.aeo),
    geo: Boolean(seoMeta?.geo),
  });
  const correctivePass = Number(payload.__correctivePass || 0) || 0;
  const cleanliness = planSettledCleanliness(
    {
      title: fullName || product.title,
      body: bodyProse,
      tags: patch.tags || payload.ecommerceTags || "",
      note: conditionNote || notes,
      damage: /\b(?:scratch|crack|chip|stain|tear|dent|damage)\b/i.test(String(conditionNote || notes || "")),
      dims,
      // The same identity the build used, so the second write-up check agrees with it.
      brand: payload.brand || brand || "",
      category: payload.category || "",
    },
    { corrective: payload.__corrective === true || correctivePass > 0 }
  );
  const updateKey = listingUpdateKey(payload);
  if (
    cleanliness.action === "rebuild" &&
    intakeItemId &&
    !listingUpdatePending.has(updateKey)
  ) {
    if (correctivePass >= LISTING_CORRECTIVE_MAX_PASSES) {
      log?.("warn", {
        event: "shopify.clean_product.corrective_cap",
        intakeItemId,
        violations: cleanliness.violations,
        correctivePass,
      });
      await stampIntakeShopifyImageSync(intakeItemId, {
        status: "pending",
        shopifyProductId: productId,
        detail:
          cleanliness.note ||
          "Shopify sync error: listing still needs a cleanup after corrective rebuilds",
        resyncListing: false,
        log,
      });
    } else {
      listingUpdatePending.set(updateKey, {
        payload: {
          intakeItemId,
          shopifyProductId: productId,
          __rebuildFromCurrent: true,
          __corrective: true,
          __correctivePass: correctivePass + 1,
          __forceCleanBody:
            cleanliness.forceCleanBody === true ||
            cleanliness.violations?.includes("body_as_is_paste"),
          replaceBody: true,
          rewriteBody: true,
          forceRefresh:
            cleanliness.forceCleanBody === true ||
            cleanliness.violations?.includes("body_as_is_paste"),
          allowOpenAi: false,
          queuedAt: new Date().toISOString(),
        },
        log,
      });
    }
  } else if (cleanliness.action === "stop" && intakeItemId) {
    log?.("warn", {
      event: "shopify.clean_product.stopped",
      intakeItemId,
      violations: cleanliness.violations,
    });
    await stampIntakeShopifyImageSync(intakeItemId, {
      status: "pending",
      shopifyProductId: productId,
      detail: cleanliness.note,
      resyncListing: false,
      log,
    });
  }
  return {
    ok: true,
    status: "updated",
    cleanliness,
    shopifyProductId: productId,
    intakeItemId,
    name: fullName || product.title,
    updatedKeys: Object.keys(patch),
    // The description this write put in Shopify. Pricing Engine keeps it and
    // recognises the webhook for this write by it. Left out when this write
    // did not send a description.
    ...(needsProductPut &&
    !updated.skipped &&
    patch.bodyHtml != null &&
    String(patch.bodyHtml).trim() &&
    !isDimsOnlyListingText(patch.bodyHtml)
      ? { shopifyBodyFingerprint: shopifyBodyFingerprint(patch.bodyHtml) }
      : {}),
    taxonomy: {
      department: taxonomy.department,
      category: taxonomy.category,
      websiteCategory: websiteCategory || taxonomy.websiteCode || null,
    },
    seo: {
      title: seoFields.seoTitle,
      description: seoFields.seoDescription,
      aeo: seoFields.aeoDescription,
      geo: seoFields.geoDescription,
      metafields: seoMeta,
    },
  };
}
