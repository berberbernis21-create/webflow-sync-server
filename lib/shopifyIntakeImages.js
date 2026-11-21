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

/** Default Shopify title-search poll cadence (~5s). Override via SHOPIFY_TITLE_POLL(_INTERVAL)_MS. */
export function shopifyTitlePollIntervalMs(fallback = 5000) {
  const raw =
    process.env.SHOPIFY_TITLE_POLL_INTERVAL_MS ||
    process.env.SHOPIFY_TITLE_POLL_MS ||
    "";
  const n = parseInt(String(raw).trim() || String(fallback), 10);
  if (!Number.isFinite(n) || n < 500) return Math.max(500, Number(fallback) || 5000);
  return Math.min(30_000, n);
}

/** Backoff cap for title poll. Default matches interval so cadence stays ~5s. */
export function shopifyTitlePollMaxDelayMs(intervalMs = 5000) {
  const base = Math.max(500, Number(intervalMs) || shopifyTitlePollIntervalMs());
  const raw = process.env.SHOPIFY_TITLE_POLL_MAX_DELAY_MS || "";
  const n = parseInt(String(raw).trim() || String(base), 10);
  if (!Number.isFinite(n) || n < 500) return base;
  return Math.max(base, Math.min(60_000, n));
}

export async function pollShopifyProductByFullName(
  fullName,
  { log, maxWaitMs = 120000, intervalMs, backoff = false } = {}
) {
  const started = Date.now();
  let attempt = 0;
  let delayMs = Math.max(500, Number(intervalMs) || shopifyTitlePollIntervalMs());
  const maxDelayMs = shopifyTitlePollMaxDelayMs(delayMs);
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
    // Stale Traxia tag often survives after media lands — clear it whenever we see images.
    await stripNoImagesTag(product.id, { log });
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

  if (intakeItemId) {
    await stampIntakeShopifyImageSync(intakeItemId, {
      status: "pending",
      detail: `Waiting for Shopify product (Traxia e-com Save; poll ~${Math.round(pollEveryMs / 1000)}s) then Intake→Shopify images`,
      log,
    });
  }

  // Steady ~5s cadence after e-com Save (no exponential backoff) until product lands.
  const shopifyProduct = await pollShopifyProductByFullName(name, {
    log,
    maxWaitMs,
    intervalMs: pollEveryMs,
    backoff: false,
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

  // Append without position — specifying an occupied slot 422s on products that already have media.
  try {
    await uploadIntakeUrlToShopifyProduct(product.id, src, {
      cookie,
      position: null,
      log,
    });
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

  await stripNoImagesTag(product.id, { log });

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
