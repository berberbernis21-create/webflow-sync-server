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
import sharp from "sharp";

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

export async function stampIntakeShopifyImageSync(
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
    if (resyncListing != null) body.resyncListing = Boolean(resyncListing);
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

/**
 * Traxia name/SKU → Shopify (via Traxia e-com) → Intake feedback loop.
 * Match Intake by External Link / intake-uuid in product fields, else exact title.
 * Stamps Intake name + traxiaSku + shopifyProductId (no Traxia extension API).
 */
export async function mirrorShopifyProductToIntake(product, { log } = {}) {
  if (!product?.id) return { ok: false, reason: "missing_product" };
  if (!intakeShopifyImagesConfigured()) {
    return { ok: false, reason: "missing_intake_creds" };
  }

  let intakeItemId = extractIntakeItemIdFromShopifyProduct(product);
  const title = normalizeFullItemName(product.title);
  const sku = extractShopifyVariantSku(product);
  const shopifyProductId = String(product.id);

  if (!intakeItemId && title) {
    try {
      const byName = await fetchIntakeItemByFullName(title, { log });
      if (byName?.id) intakeItemId = String(byName.id);
    } catch (err) {
      log?.("warn", {
        event: "shopify.intake_mirror.lookup_failed",
        shopifyProductId,
        message: err?.message || String(err),
      });
    }
  }

  // Prefer stamped id on prior Intake row when title changed (lookup by product id).
  if (!intakeItemId) {
    try {
      const cookie = await ensureIntakeSessionCookie(log);
      const res = await intakeGet(`/api/lost-and-found-intake/items`, {
        cookie,
        params: { q: shopifyProductId, page: 1, pageSize: 20 },
      });
      const items =
        res.data?.items ||
        res.data?.results ||
        (Array.isArray(res.data) ? res.data : []);
      const hit = (Array.isArray(items) ? items : []).find(
        (it) =>
          String(it?.shopifyProductId || it?.shopify_product_id || "").trim() ===
          shopifyProductId
      );
      if (hit?.id) intakeItemId = String(hit.id);
    } catch (_err) {
      /* optional */
    }
  }

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
  const stamped = await stampIntakeShopifyImageSync(intakeItemId, {
    // Mirror-only: do not force image sync status.
    status: null,
    shopifyProductId,
    traxiaSku: sku,
    name: title || null,
    retailPrice: variantPrice,
    // Intake queues coalesced listing-update after name/price mirror (no hammer here).
    resyncListing: true,
    detail: title
      ? `Mirrored Shopify title/SKU/price → Intake (${shopifyProductId})`
      : `Mirrored Shopify product id/SKU/price → Intake (${shopifyProductId})`,
    log,
  });

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
  });
  return {
    ok: Boolean(stamped.ok),
    intakeItemId,
    shopifyProductId,
    traxiaSku: sku,
    retailPrice: variantPrice,
    name: title || null,
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
    // Existing media must NOT skip organization sync — reconcile Intake order/primary now.
    try {
      const desired = normalizeCallerImageUrls(imageUrlsHint);
      const reorderPayload = {
        intakeItemId: intakeItemId || null,
        shopifyProductId: String(product.id),
        name: product.title || null,
        imageUrls: desired.length ? desired : null,
      };
      const reordered = await handleIntakeImagesReorder(reorderPayload, { log });
      const skipped = {
        ...empty,
        status: "skipped_has_images",
        intakeItemId: intakeItemId || null,
        shopifyProductId: product.id,
        reordered: Boolean(reordered?.ok),
        moved: reordered?.moved || 0,
        uploaded: reordered?.uploaded || 0,
      };
      if (intakeItemId) {
        await stampIntakeShopifyImageSync(intakeItemId, {
          status: "synced",
          shopifyProductId: product.id,
          detail:
            reordered?.ok
              ? `Shopify gallery order reconciled (${reordered.moved || 0} moved, ${reordered.uploaded || 0} uploaded)`
              : "Shopify already had display images",
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
  let listingIntakeId = resolvedIntakeId;
  try {
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
      const listing = await handleIntakeListingUpdate(
        {
          intakeItemId: listingIntakeId || (intakeItem?.id ? String(intakeItem.id) : null),
          shopifyProductId: String(shopifyProduct.id),
          name: (intakeItem && intakeItem.name) || name,
          description:
            (intakeItem &&
              (intakeItem.description ||
                intakeItem.ecommerceDescription ||
                intakeItem.listingDescription)) ||
            "",
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

/**
 * Match desired Intake image URLs to existing Shopify product images and set positions.
 * Unmatched Intake URLs are uploaded; unmatched Shopify images are deleted (reconcile).
 * Never wipes the gallery: empty imageUrls soft-skips (Shopify product + images stay).
 * Never deletes the Shopify product itself.
 */
export async function handleIntakeImagesReorder(
  {
    intakeItemId = null,
    shopifyProductId = null,
    name = null,
    imageUrls = null,
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

  // Explicit non-empty array = caller intent. Otherwise load ordered Intake URLs.
  const callerProvidedUrls = Array.isArray(imageUrls) && imageUrls.length > 0;
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
      desiredFingerprints.push({ src, hash, prepared });
    } catch (err) {
      log?.("warn", {
        event: "shopify.intake_image.reorder_fingerprint_failed",
        shopifyProductId: productId,
        message: err?.message || String(err),
      });
      desiredFingerprints.push({ src, hash: null, prepared: null, error: err?.message });
    }
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
      try {
        await updateShopifyImagePosition(productId, best.img.id, position);
        orderedIds.push(String(best.img.id));
        moved.push({ imageId: best.img.id, position, hamming: bestDist });
      } catch (err) {
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
      }
    } catch (err) {
      log?.("warn", {
        event: "shopify.intake_image.reorder_upload_failed",
        shopifyProductId: productId,
        position,
        message: err?.message || String(err),
      });
    }
  }

  // Delete Shopify orphans not matched to the Intake set (photo delete / reorder reconcile).
  for (const cand of shopifyFingerprints) {
    if (cand.used) continue;
    try {
      await deleteShopifyProductImage(productId, cand.img.id, { log });
      deleted.push({ imageId: cand.img.id });
    } catch (err) {
      log?.("warn", {
        event: "shopify.intake_image.reorder_delete_failed",
        shopifyProductId: productId,
        imageId: cand.img.id,
        message: err?.message || String(err),
      });
    }
  }

  if (id) {
    await stampIntakeShopifyImageSync(id, {
      status: "synced",
      shopifyProductId: productId,
      traxiaSku: extractShopifyVariantSku(product),
      detail: `Shopify gallery reconciled (${moved.length} moved, ${uploaded.length} uploaded, ${deleted.length} deleted)`,
      log,
    });
  }

  log?.("info", {
    event: "shopify.intake_image.reorder_ok",
    shopifyProductId: productId,
    intakeItemId: id,
    desired: desired.length,
    moved: moved.length,
    uploaded: uploaded.length,
    deleted: deleted.length,
  });

  return {
    ok: true,
    status: "reordered",
    shopifyProductId: productId,
    intakeItemId: id,
    name: fullName || product.title,
    desired: desired.length,
    moved: moved.length,
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

function departmentFromMerch(merchType) {
  return normalizeMerchType(merchType) === "luxury" ? "Luxury Goods" : "Furniture & Home";
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
    .replace(/\s{2,}/g, " ")
    .replace(/\s+([,.;:])/g, "$1")
    .replace(/\(\s*\)/g, "")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
  return s;
}

/** Plain-text dims footer for Shopify (source of truth = Intake, not Traxia). */
export function formatShopifyDimsLine(dims = {}) {
  const w = dims.width ?? dims.dimWidth;
  const d = dims.depth ?? dims.dimDepth ?? dims.dimLength ?? dims.length;
  const h = dims.height ?? dims.dimHeight;
  const wt = dims.weightLbs ?? dims.weight;
  const parts = [];
  if (w != null && String(w).trim() !== "" && Number.isFinite(Number(w))) {
    parts.push(`Width: ${formatDimNumber(w)}"`);
  }
  if (d != null && String(d).trim() !== "" && Number.isFinite(Number(d))) {
    parts.push(`Depth: ${formatDimNumber(d)}"`);
  }
  if (h != null && String(h).trim() !== "" && Number.isFinite(Number(h))) {
    parts.push(`Height: ${formatDimNumber(h)}"`);
  }
  if (!parts.length && (wt == null || String(wt).trim() === "")) return "";
  const sizeLine = parts.length ? `Dimensions: ${parts.join(" × ")}.` : "";
  const weightLine =
    wt != null && String(wt).trim() !== "" && Number.isFinite(Number(wt))
      ? `Weight: ${formatDimNumber(wt)} lb.`
      : "";
  // Weight on its own line under Dimensions (not same-line).
  if (sizeLine && weightLine) return `${sizeLine}\n${weightLine}`;
  return sizeLine || weightLine;
}

/**
 * Shopify body = listing DESCRIPTION prose first, then ONE Dimensions+Weight footer.
 * NEVER return dims-only or weight-only (bug that left &lt;br&gt;&lt;br&gt;Dimensions...).
 * Single blank line between prose and Dimensions; Weight under Dimensions.
 */
export function composeShopifyBodyHtml(description, dims) {
  const cleaned = stripListingDimJunk(description).replace(/\b(\d+)\.0\b/g, "$1");
  if (!cleaned) return "";
  const dimBlock = formatShopifyDimsLine(dims);
  const paragraphs = cleaned
    .split(/\n+/)
    .map((p) => p.trim())
    .filter(Boolean);
  let html = paragraphs.map((p) => `<p>${escapeHtml(p)}</p>`).join("");
  if (!dimBlock) return html;
  const footerInner = escapeHtml(dimBlock).replace(/\n/g, "<br>");
  // Attach footer to the last prose paragraph with one blank line (two <br>),
  // so Shopify does not render a second <p> margin gap.
  if (html) {
    return html.replace(/<\/p>\s*$/i, `<br><br>${footerInner}</p>`);
  }
  return `<p>${footerInner}</p>`;
}

/** True when text is empty or only Dimensions:/Weight: footer noise. */
export function isDimsOnlyListingText(text) {
  const cleaned = stripListingDimJunk(text);
  return !cleaned;
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
    merchType,
    ecommerceTags,
    dims,
  } = {},
  { log } = {}
) {
  const key = String(process.env.OPENAI_API_KEY || "").trim();
  if (!key) return { ok: false, reason: "openai_missing" };
  const mode = normalizeMerchType(merchType) === "luxury" ? "luxury" : "furniture";
  const title = normalizeFullItemName(name);
  const extra = String(notes || "").trim();
  const existing = stripListingDimJunk(description || "");
  if (!title && !extra && !existing) return { ok: false, reason: "no_input" };

  const furnitureSystem = `You write Shopify product body prose for Lost & Found Resale (Scottsdale).
COPY MODE FURNITURE / HOME — same writing craft as luxury Helper listings (concrete facts, SEO/AEO/GEO), furniture product facts only (never fake luxury/handbag claims).
5 to 7 specific sentences. SEO sentence 1: brand if known, item type, material, color, style — the shopper search phrase.
AEO: one quotable sentence on what it is, how used/placed, and one concrete visual or construction fact.
GEO: brand, material, finish, colorway, silhouette only when supported. No eye-catching / conversation-starting filler.
Lead with brand + item type when known; else "This" + item type. One or two placement ideas when they fit.
Use whole numbers when fractional part is zero (65 not 65.0). Use STAFF EXTRA INFORMATION for accuracy.
Never dump Measured:/Height=/Width=/Depth=/Weight= or FH/X/LG codes. Do NOT put a Dimensions:/Weight: footer.
ASCII only. Return JSON {"description":"...","tags":"..."}.`;

  const luxurySystem = `You write Shopify product body prose for Lost & Found Resale (Scottsdale).
COPY MODE LUXURY GOODS (Traxia E Commerce Helper). Furniture room-placement rules do NOT apply.
4 to 6 sentences. SEO sentence 1: brand, silhouette, color, material, item type.
AEO: one quotable sentence on what it is, how carried/worn, hardware/closure/strap.
GEO: house, material, colorway, hardware, silhouette only when supported.
No high/wide/deep furniture "when displayed" sentence. No room/entryway/vanity filler.
Use STAFF EXTRA INFORMATION for accuracy. Never dump Measured: dumps or LG codes into prose.
Do NOT put a Dimensions:/Weight: footer. ASCII only. Return JSON {"description":"...","tags":"..."}.`;

  const system = mode === "luxury" ? luxurySystem : furnitureSystem;
  const dimHint = formatShopifyDimsLine(dims || {});
  const user = [
    `Generate Shopify listing DESCRIPTION prose (no dims footer).`,
    `NAME/TITLE: ${title || "(none)"}`,
    `EXISTING DESCRIPTION: ${existing || "(none)"}`,
    `STAFF EXTRA INFORMATION / NOTES: ${extra || "(none)"}`,
    `BRAND: ${String(brand || "").trim() || "(none)"}`,
    `CATEGORY: ${String(category || "").trim() || "(none)"}`,
    `MERCH MODE: ${mode}`,
    `E-COMMERCE TAGS: ${String(ecommerceTags || "").trim() || "(none)"}`,
    `DIMENSION FACTS (optional natural mention once for furniture only): ${dimHint || "(none)"}`,
    `Return JSON only.`,
  ].join("\n");

  try {
    const model = String(process.env.OPENAI_LISTING_MODEL || process.env.OPENAI_MODEL || "gpt-4o-mini").trim();
    const resp = await axios.post(
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
    log?.("info", {
      event: "shopify.listing_prose.ok",
      mode,
      title: title.slice(0, 120),
      len: prose.length,
    });
    return {
      ok: true,
      description: prose,
      tags: String(parsed.tags || "").trim() || null,
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
  const cleaned = stripListingDimJunk(description || "");
  const sentences = splitListingSentences(cleaned);
  const title = normalizeFullItemName(name);

  const seoTitle =
    truncateAtWord(String(seoTitleOverride || "").trim() || title, 70) || title;

  let seoDescription = String(seoDescriptionOverride || "").trim();
  if (!seoDescription) {
    seoDescription = sentences[0] || cleaned;
    if (seoDescription.length < 90 && sentences[1]) {
      seoDescription = `${seoDescription} ${sentences[1]}`;
    }
  }
  seoDescription = truncateAtWord(seoDescription, 160);

  let aeoDescription = String(aeoDescriptionOverride || "").trim();
  if (!aeoDescription) {
    aeoDescription =
      sentences.find((s) => /^(this|these|it|a |an |the )/i.test(s)) ||
      sentences[1] ||
      sentences[0] ||
      cleaned;
  }
  aeoDescription = truncateAtWord(aeoDescription, 400);

  let geoDescription = String(geoDescriptionOverride || "").trim();
  if (!geoDescription) {
    const dimLine = formatShopifyDimsLine(dims || {});
    const parts = [
      brand && String(brand).trim(),
      category && String(category).trim(),
      title,
      dimLine,
      sentences[0],
    ].filter(Boolean);
    // Dedupe near-identical chunks while preserving order.
    const seen = new Set();
    const uniq = [];
    for (const p of parts) {
      const k = p.toLowerCase();
      if (seen.has(k)) continue;
      seen.add(k);
      uniq.push(p);
    }
    geoDescription = uniq.join(". ").replace(/\.\s*\./g, ".");
  }
  geoDescription = truncateAtWord(geoDescription, 400);

  return {
    seoTitle: seoTitle || null,
    seoDescription: seoDescription || null,
    aeoDescription: aeoDescription || null,
    geoDescription: geoDescription || null,
  };
}

/**
 * Upsert product metafield via REST (create or update by namespace+key).
 * Prefers `custom`, falls back to `lostfound` if custom definition rejects.
 */
async function upsertShopifyProductMetafield(
  productId,
  { namespace, key, value, type = "multi_line_text_field" },
  { log } = {}
) {
  const id = String(productId || "").trim();
  const base = shopifyRestBase();
  const ns = String(namespace || "").trim();
  const k = String(key || "").trim();
  const val = String(value || "").trim();
  if (!base || !id || !ns || !k || !val) return { ok: false, reason: "missing_metafield_args" };

  let existingId = null;
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
  return {
    ok: true,
    id: postRes.data?.metafield?.id || null,
    namespace: ns,
    key: k,
  };
}

/**
 * Write SEO (Search engine listing) + AEO/GEO metafields on a Shopify product.
 * SEO → metafields_global_* (Admin: Search engine listing).
 * AEO/GEO → custom.aeo_description / custom.geo_description (fallback lostfound.*).
 */
export async function applyShopifySeoAeoGeo(productId, fields, { log } = {}) {
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
      { log }
    );
    if (!written.ok) {
      written = await upsertShopifyProductMetafield(
        id,
        { namespace: "lostfound", key, value: val, type: "multi_line_text_field" },
        { log }
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
  const w = dims.width ?? dims.dimWidth;
  const d = dims.depth ?? dims.dimDepth ?? dims.dimLength ?? dims.length;
  const h = dims.height ?? dims.dimHeight;
  const wt = dims.weightLbs ?? dims.weight;
  if (w != null && String(w).trim() !== "" && Number.isFinite(Number(w))) {
    out.push(`Width: ${formatDimNumber(w)}`);
  }
  if (d != null && String(d).trim() !== "" && Number.isFinite(Number(d))) {
    out.push(`Depth: ${formatDimNumber(d)}`);
  }
  if (h != null && String(h).trim() !== "" && Number.isFinite(Number(h))) {
    out.push(`Height: ${formatDimNumber(h)}`);
  }
  // Match existing Shopify/ecommerce tag shape: "Weight: 13" (no unit suffix).
  if (wt != null && String(wt).trim() !== "" && Number.isFinite(Number(wt))) {
    out.push(`Weight: ${formatDimNumber(wt)}`);
  }
  return out;
}

function managedListingTagSet(merchType) {
  const type = normalizeMerchType(merchType);
  // Only vertical codes are always managed — X is Accessories website-cat, not default furniture.
  const set = new Set(
    (type === "luxury" ? ["LG"] : ["FH"]).map((s) => s.toLowerCase())
  );
  set.add("furniture & home");
  set.add("luxury goods");
  set.add("furniture");
  set.add("luxury");
  set.add("livingroom");
  set.add("diningroom");
  const opts = type === "luxury" ? LUXURY_WEBSITE_CATS : FURNITURE_WEBSITE_CATS;
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
 */
export function mergeShopifyListingTags({
  existingTags,
  ecommerceTags,
  merchType,
  websiteCategory,
  dims,
} = {}) {
  const managed = managedListingTagSet(merchType);
  const kept = parseTagList(existingTags).filter((t) => {
    if (DIM_TAG_RE.test(t)) return false;
    if (managed.has(t.toLowerCase())) return false;
    return true;
  });
  // Field-mapped codes ONLY from vertical + website category (FH/LG + letter).
  // SEO keywords come from ecommerceTags / Helper — never dump department/cat labels.
  const structured = structuredEcommerceCodes(merchType, websiteCategory);
  const listing = parseTagList(ecommerceTags).filter((t) => {
    const low = String(t || "").trim().toLowerCase();
    if (!low) return false;
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
  const keys = Object.keys(product).filter((k) => k !== "id");
  if (!keys.length) {
    return { ok: true, product: existingProduct || null, skipped: true };
  }
  const res = await axios.put(
    `${base}/products/${id}.json`,
    { product },
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
export async function handleIntakeListingUpdate(payload = {}, { log } = {}) {
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

  const merchType = normalizeMerchType(payload.merchType || payload.vertical);
  const websiteCategory = String(payload.websiteCategory || payload.subcategory || "").trim();
  const catLabel =
    websiteCatLabel(merchType, websiteCategory) ||
    String(payload.category || "").trim() ||
    "";
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
  let bodyProse = description != null ? String(description) : "";
  let notes = payload.notes || payload.extraInformation || payload.extra || "";
  let tagsForGen = ecommerceTags != null ? ecommerceTags : "";
  if (forceRefresh || isDimsOnlyListingText(bodyProse)) {
    if (intakeItemId) {
      try {
        const intake = await fetchIntakeItemById(intakeItemId, { log });
        if (intake) {
          if (!notes) notes = intake.notes || intake.conditionIssues || "";
          if (forceRefresh || !String(bodyProse || "").trim()) {
            const intakeDesc =
              intake.description ||
              intake.ecommerceDescription ||
              intake.listingDescription ||
              "";
            if (intakeDesc) bodyProse = intakeDesc;
          }
          if (!tagsForGen) tagsForGen = intake.ecommerceTags || intake.tags || "";
        }
      } catch (_err) {
        /* optional */
      }
    }
    if (forceRefresh || isDimsOnlyListingText(bodyProse)) {
      const generated = await generateShopifyListingProse(
        {
          name: fullName || product.title,
          notes,
          description: bodyProse,
          brand,
          category: catLabel || String(payload.category || "").trim(),
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
      }
    }
  }

  const patch = {};
  // ALWAYS: prose first, then ONE Dimensions+Weight footer. Never dims-only. Never blank body.
  if (!isDimsOnlyListingText(bodyProse)) {
    const html = composeShopifyBodyHtml(bodyProse, dims);
    if (html) patch.bodyHtml = html;
  } else if (!isDimsOnlyListingText(product.body_html || "")) {
    // Preserve existing Shopify body if we would otherwise wipe it.
    log?.("info", {
      event: "shopify.listing_update.preserve_existing_body",
      shopifyProductId: productId,
      intakeItemId,
    });
  } else {
    log?.("warn", {
      event: "shopify.listing_update.skip_dims_only_body",
      shopifyProductId: productId,
      intakeItemId,
      name: (fullName || "").slice(0, 120),
    });
  }
  const tagsForMerge =
    payload.ecommerceTags != null
      ? payload.ecommerceTags
      : ecommerceTags != null
        ? ecommerceTags
        : "";
  // Always pair tags with body_html — never tags-only / dims-footer-only updates.
  if (patch.bodyHtml && (tagsForMerge !== "" || dims)) {
    patch.tags = mergeShopifyListingTags({
      existingTags: product.tags,
      ecommerceTags: tagsForMerge || "",
      merchType,
      websiteCategory,
      dims,
    });
  } else if (!patch.bodyHtml && (tagsForMerge !== "" || dims)) {
    log?.("warn", {
      event: "shopify.listing_update.skip_tags_without_body",
      shopifyProductId: productId,
      intakeItemId,
      name: (fullName || "").slice(0, 120),
    });
  }
  // Product type = Intake category (e.g. LOVE SEAT / CABINET), not website-cat Accessories.
  const productType =
    String(payload.category || "").trim() ||
    (websiteCatCode(merchType, websiteCategory) ? catLabel : "");
  if (productType) patch.productType = productType;
  if (brand) patch.vendor = brand;
  // Retail price from Intake in the same PUT (price change must not wipe body).
  const priceFromPayload =
    payload.price != null
      ? payload.price
      : payload.retailPrice != null
        ? payload.retailPrice
        : payload.retail_price;
  if (priceFromPayload != null && String(priceFromPayload).trim() !== "") {
    patch.price = priceFromPayload;
  }
  // Rename: write new Intake name onto Shopify product title.
  if (
    fullName &&
    normalizeFullItemName(product.title) !== fullName
  ) {
    patch.title = fullName;
  }
  if (seoFields.seoTitle) patch.seoTitle = seoFields.seoTitle;
  if (seoFields.seoDescription) patch.seoDescription = seoFields.seoDescription;

  if (!Object.keys(patch).length && !seoFields.aeoDescription && !seoFields.geoDescription) {
    return {
      ok: true,
      status: "noop",
      shopifyProductId: productId,
      intakeItemId,
    };
  }

  // Product PUT + AEO/GEO metafields in parallel (SEO globals ride on the product PUT).
  // One coherent write: body + tags + price — never images[], never empty body_html.
  const needsProductPut = Object.keys(patch).length > 0;
  const needsAeoGeo = Boolean(seoFields.aeoDescription || seoFields.geoDescription);
  const [updated, seoMeta] = await Promise.all([
    needsProductPut
      ? updateShopifyProductListing(productId, patch, { log, existingProduct: product })
      : Promise.resolve({ ok: true, product }),
    needsAeoGeo
      ? applyShopifySeoAeoGeo(
          productId,
          {
            seoTitle: null,
            seoDescription: null,
            aeoDescription: seoFields.aeoDescription,
            geoDescription: seoFields.geoDescription,
          },
          { log }
        )
      : Promise.resolve({ ok: true, aeo: false, geo: false }),
  ]);
  if (!updated.ok) {
    if (intakeItemId) {
      await stampIntakeShopifyImageSync(intakeItemId, {
        status: "failed",
        shopifyProductId: productId,
        detail: `Shopify listing update failed: ${updated.reason || "error"}`,
        log,
      });
    }
    return {
      ok: false,
      status: "update_failed",
      reason: updated.reason || "update_failed",
      shopifyProductId: productId,
      intakeItemId,
    };
  }

  if (intakeItemId) {
    await stampIntakeShopifyImageSync(intakeItemId, {
      status: "synced",
      shopifyProductId: productId,
      traxiaSku: extractShopifyVariantSku(updated.product || product),
      detail: "Shopify listing synced (description / tags / dims / SEO·AEO·GEO)",
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
    seo: Boolean(seoFields.seoTitle || seoFields.seoDescription),
    aeo: Boolean(seoMeta?.aeo),
    geo: Boolean(seoMeta?.geo),
  });
  return {
    ok: true,
    status: "updated",
    shopifyProductId: productId,
    intakeItemId,
    name: fullName || product.title,
    updatedKeys: Object.keys(patch),
    seo: {
      title: seoFields.seoTitle,
      description: seoFields.seoDescription,
      aeo: seoFields.aeoDescription,
      geo: seoFields.geoDescription,
      metafields: seoMeta,
    },
  };
}
