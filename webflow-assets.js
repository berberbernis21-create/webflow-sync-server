/**
 * Webflow Assets API — SHA1-based deduplication.
 * Canonical flow: download → SHA1 → check existing by hash (cache + list API) → create only if no match → upload to S3.
 * Never create duplicate assets; SHA1 is the single source of truth for asset identity.
 */

import axios from "axios";
import crypto from "crypto";
import fs from "fs";
import path from "path";
import FormData from "form-data";

const DATA_DIR = "./data";
const CACHE_FILENAME = "assetHashCache.json";

function ensureDataDir() {
  if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
  }
}

/**
 * Load per-site cache: { [siteId]: { [sha1]: { id, hostedUrl } } }
 */
function loadAssetCache() {
  try {
    ensureDataDir();
    const filePath = path.join(DATA_DIR, CACHE_FILENAME);
    if (!fs.existsSync(filePath)) return {};
    const raw = fs.readFileSync(filePath, "utf8");
    return JSON.parse(raw);
  } catch (err) {
    console.error("[webflow-assets] Failed to load asset cache:", err.message);
    return {};
  }
}

function saveAssetCache(cache) {
  try {
    ensureDataDir();
    const filePath = path.join(DATA_DIR, CACHE_FILENAME);
    fs.writeFileSync(filePath, JSON.stringify(cache, null, 2), "utf8");
  } catch (err) {
    console.error("[webflow-assets] Failed to save asset cache:", err.message);
  }
}

/**
 * Download image from URL (e.g. Shopify CDN) or read from local path.
 * @returns { Promise<Buffer> }
 */
async function downloadImage(urlOrPath) {
  if (!urlOrPath || typeof urlOrPath !== "string") {
    throw new Error("downloadImage: url or path required");
  }
  const isUrl = urlOrPath.startsWith("http://") || urlOrPath.startsWith("https://");
  if (isUrl) {
    const resp = await axios.get(urlOrPath, { responseType: "arraybuffer", timeout: 30000 });
    return Buffer.from(resp.data);
  }
  if (!fs.existsSync(urlOrPath)) {
    throw new Error(`downloadImage: file not found: ${urlOrPath}`);
  }
  return fs.readFileSync(urlOrPath);
}

/**
 * Compute SHA1 hash of file contents (single source of truth for asset identity).
 */
function sha1OfBuffer(buffer) {
  return crypto.createHash("sha1").update(buffer).digest("hex");
}

/**
 * List all assets for site (paginated). If API returns fileHash, we can match by it.
 */
async function listAllAssetsByHash(siteId, token) {
  const limit = 100;
  let offset = 0;
  const byHash = {};
  const headers = { Authorization: `Bearer ${token}` };

  while (true) {
    const resp = await axios.get(
      `https://api.webflow.com/v2/sites/${siteId}/assets`,
      { params: { limit, offset }, headers }
    );
    const assets = resp.data?.assets ?? [];
    for (const a of assets) {
      const hash = a.fileHash ?? a.filehash;
      if (hash && (a.id && (a.hostedUrl || a.assetUrl))) {
        byHash[hash] = { id: a.id, hostedUrl: a.hostedUrl || a.assetUrl };
      }
    }
    const pagination = resp.data?.pagination;
    if (!pagination || assets.length < limit) break;
    offset += limit;
    if (offset >= (pagination.total ?? offset + 1)) break;
  }
  return byHash;
}

/**
 * Resolve image to a Webflow asset: reuse by SHA1 or create and upload.
 * Deduplication happens before POST /assets; we never create a duplicate.
 *
 * @param {string} siteId - Webflow site ID
 * @param {string} token - Webflow API token
 * @param {string} imageUrlOrPath - Shopify CDN URL or local file path
 * @param {string} [fileName] - Optional filename (defaults to last path segment or "image")
 * @returns { Promise<{ id: string, hostedUrl: string } | null> } - null if download/upload fails
 */
export async function resolveWebflowAsset(siteId, token, imageUrlOrPath, fileName) {
  if (!siteId || !token || !imageUrlOrPath) return null;

  let buffer;
  try {
    buffer = await downloadImage(imageUrlOrPath);
  } catch (err) {
    console.warn("[webflow-assets] Download failed:", imageUrlOrPath, err.message);
    return null;
  }

  const sha1 = sha1OfBuffer(buffer);
  const cache = loadAssetCache();
  const siteCache = (cache[siteId] = cache[siteId] || {});

  if (siteCache[sha1]) {
    return siteCache[sha1];
  }

  // Optional: fill from API if list returns fileHash
  try {
    const fromApi = await listAllAssetsByHash(siteId, token);
    if (fromApi[sha1]) {
      siteCache[sha1] = fromApi[sha1];
      saveAssetCache(cache);
      return fromApi[sha1];
    }
  } catch (err) {
    console.warn("[webflow-assets] List assets failed (continuing without API dedupe):", err.message);
  }

  const name =
    fileName ||
    (imageUrlOrPath.includes("/")
      ? imageUrlOrPath.replace(/\?.*$/, "").split("/").pop()
      : path.basename(imageUrlOrPath)) ||
    "image";
  const ext = path.extname(name) || ".jpg";
  const finalFileName = ext === path.extname(name) ? name : `${name}${ext}`;

  let createResp;
  try {
    createResp = await axios.post(
      `https://api.webflow.com/v2/sites/${siteId}/assets`,
      { fileName: finalFileName, fileHash: sha1 },
      { headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" } }
    );
  } catch (err) {
    console.warn("[webflow-assets] Create asset failed:", err.response?.status, err.response?.data || err.message);
    return null;
  }

  const uploadUrl = createResp.data?.uploadUrl;
  const uploadDetails = createResp.data?.uploadDetails;
  const id = createResp.data?.id;
  const hostedUrl = createResp.data?.hostedUrl || createResp.data?.assetUrl;

  if (!uploadUrl || !id) {
    console.warn("[webflow-assets] Create response missing uploadUrl or id");
    return null;
  }
  if (!uploadDetails || typeof uploadDetails !== "object") {
    console.warn("[webflow-assets] Create response missing uploadDetails");
    return null;
  }

  const contentType = createResp.data?.contentType || "image/jpeg";
  const form = new FormData();
  for (const [key, value] of Object.entries(uploadDetails)) {
    form.append(key, value);
  }
  form.append("file", buffer, { filename: finalFileName, contentType });

  try {
    await axios.post(uploadUrl, form, {
      headers: form.getHeaders(),
      maxBodyLength: Infinity,
      maxContentLength: Infinity,
    });
  } catch (err) {
    console.warn("[webflow-assets] S3 upload failed:", err.message);
    return null;
  }

  if (!hostedUrl) {
    console.warn("[webflow-assets] Create response missing hostedUrl");
    return null;
  }

  siteCache[sha1] = { id, hostedUrl };
  saveAssetCache(cache);
  return { id, hostedUrl };
}

/**
 * Resolve multiple image URLs in order. Returns array of { fileId, url } or { url } for use in fieldData.
 * Skips nulls (failed resolution) and falls back to original URL if USE_WEBFLOW_ASSETS resolution fails.
 */
export async function resolveWebflowImageFields(siteId, token, imageUrls, useFileId = true) {
  if (!siteId || !token || !Array.isArray(imageUrls)) {
    return imageUrls.map((url) => (url ? (useFileId ? { url } : { url }) : null)).filter(Boolean);
  }
  const result = [];
  for (const url of imageUrls) {
    if (!url) continue;
    const asset = await resolveWebflowAsset(siteId, token, url);
    if (asset) {
      result.push(useFileId ? { fileId: asset.id, url: asset.hostedUrl } : { url: asset.hostedUrl });
    } else {
      result.push(useFileId ? { url } : { url });
    }
  }
  return result;
}
