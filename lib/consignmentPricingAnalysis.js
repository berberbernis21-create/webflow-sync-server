/**
 * AI comparable pricing for consignment submissions (internal email only).
 * Partnership pipeline: Google Vision → Custom Search → Gemini (comp notes) → OpenAI vision (final JSON).
 *
 * Env (Render / .env):
 * - OPENAI_MODEL — final pricing with vision (default gpt-5.2)
 * - OPENAI_MODEL_FALLBACK — when primary model unavailable (default gpt-4.1)
 * - GEMINI_MODEL — intermediate analyst (default gemini-2.5-flash)
 * - GEMINI_API_KEY, OPENAI_API_KEY, GOOGLE_API_KEY, GOOGLE_CSE_ID, Vision credentials
 * - CONSIGNMENT_PRICING_TOTAL_BUDGET_MS, CONSIGNMENT_PRICING_ITEM_TIMEOUT_MS
 * - CONSIGNMENT_PRICING_MAX_VISION_IMAGES — Google Vision only (default 8; speed/cost cap)
 * - CONSIGNMENT_OPENAI_MAX_IMAGES — GPT vision per item (default 10; 0 = unlimited up to 10 cap)
 * - CONSIGNMENT_MAX_ITEMS — max items per submission and pricing analysis (default 10)
 * - CONSIGNMENT_PRICING_MS_PER_ITEM, CONSIGNMENT_PRICING_MAX_BUDGET_MS — wall-clock budget scaling
 * - CONSIGNMENT_PRICING_MAX_IMAGE_DIM — max edge px for OpenAI images (default 1024; uses sharp if installed)
 */

import axios from "axios";
import crypto from "crypto";
import fs from "fs";
import {
  buildCategorizedLinks,
  classifyLinkChannel,
  compSourceTierScore,
  getCompSourceTier,
  isCredibleCompSource,
  isJunkCompSource,
} from "./consignmentPricingDisplay.js";
import { itemCategoryIsHandbag } from "./consignmentBrand.js";
import {
  tightenPricingAnalysis,
  collectVerifiedResaleComps,
  collectVerifiedRetailComps,
  computeCompStats,
  computeRetailAnchoredRange,
  computeRetailCompStats,
  parseConsignorRetailUsd,
  itemHasPurchaseProof,
  normalizeBrandAliases,
  listingMatchesItem,
  listingConflictsWithItem,
} from "./consignmentCompTuning.js";
import { resolvePhotoBundle, shouldShowMultiPieceCallout } from "./consignmentMultiPiece.js";
import { MAX_PRICING_ITEMS } from "./consignmentLimits.js";
import { isJpegBuffer, normalizePhotoBufferForPdf } from "./consignmentImageNormalize.js";
import { resolveItemNumber } from "./consignmentValidation.js";
import {
  filterRowsToLiveLinks,
  filterSourcesToLiveLinks,
  verifyLinksLive,
} from "./consignmentLinkVerify.js";

const VISION_URL = "https://vision.googleapis.com/v1/images:annotate";
const CSE_URL = "https://www.googleapis.com/customsearch/v1";
const GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models";
const OPENAI_URL = "https://api.openai.com/v1/chat/completions";

const DEFAULT_OPENAI_MODEL = "gpt-5.2";
const DEFAULT_OPENAI_FALLBACK_MODEL = "gpt-4.1";
const DEFAULT_GEMINI_MODEL = "gemini-2.5-flash";
const GEMINI_MODEL_FALLBACKS = ["gemini-2.5-flash", "gemini-2.0-flash-lite", "gemini-1.5-flash"];
const VISION_PROVIDER_LABEL = "google-cloud-vision";

const MAX_IMAGE_DIMENSION = Math.max(
  256,
  Math.min(2048, parseInt(process.env.CONSIGNMENT_PRICING_MAX_IMAGE_DIM || "1024", 10) || 1024)
);
const MAX_IMAGE_BYTES = 1_800_000;

/** Short cap for sync/inline use only; background jobs use submissionPricingBudgetMs({ background: true }). */
const TOTAL_BUDGET_MS = Math.max(
  1000,
  parseInt(process.env.CONSIGNMENT_PRICING_TOTAL_BUDGET_MS || "20000", 10) || 20000
);
const PER_ITEM_TIMEOUT_MS = Math.max(
  5000,
  Math.min(
    TOTAL_BUDGET_MS,
    parseInt(process.env.CONSIGNMENT_PRICING_ITEM_TIMEOUT_MS || "12000", 10) || 12000
  )
);
const BACKGROUND_ITEM_TIMEOUT_MS = Math.max(
  45000,
  parseInt(process.env.CONSIGNMENT_PRICING_BACKGROUND_ITEM_TIMEOUT_MS || "90000", 10) ||
    90000
);
const BACKGROUND_MS_PER_ITEM = Math.max(
  45000,
  parseInt(process.env.CONSIGNMENT_PRICING_MS_PER_ITEM || "75000", 10) || 75000
);
const BACKGROUND_MAX_BUDGET_MS = Math.max(
  BACKGROUND_MS_PER_ITEM,
  parseInt(process.env.CONSIGNMENT_PRICING_MAX_BUDGET_MS || "600000", 10) || 600000
);
const MAX_CONCURRENT_ITEMS = Math.max(
  1,
  Math.min(4, parseInt(process.env.CONSIGNMENT_PRICING_CONCURRENCY || "2", 10) || 2)
);
const MAX_VISION_IMAGES = Math.max(
  1,
  Math.min(12, parseInt(process.env.CONSIGNMENT_PRICING_MAX_VISION_IMAGES || "10", 10) || 10)
);
/** Default 10 — ChatGPT vision practical cap per request. Set CONSIGNMENT_OPENAI_MAX_IMAGES=0 for unlimited. */
function parseOpenAiMaxImages() {
  const raw = process.env.CONSIGNMENT_OPENAI_MAX_IMAGES;
  if (raw === undefined || raw === null || String(raw).trim() === "") return 10;
  const n = parseInt(String(raw).trim(), 10);
  if (!Number.isFinite(n) || n <= 0) return 0;
  return Math.min(10, n);
}
const OPENAI_MAX_IMAGES = parseOpenAiMaxImages();
const MAX_CSE_QUERIES = Math.max(
  1,
  Math.min(10, parseInt(process.env.CONSIGNMENT_PRICING_MAX_CSE_QUERIES || "7", 10) || 7)
);

let visionTokenCache = { token: null, expiresAtMs: 0 };

function logPricing(level, payload) {
  const msg = { ts: new Date().toISOString(), scope: "consignment_pricing", ...payload };
  if (level === "error") console.error("[consignment-pricing]", JSON.stringify(msg));
  else if (level === "warn") console.warn("[consignment-pricing]", JSON.stringify(msg));
  else console.log("[consignment-pricing]", JSON.stringify(msg));
}

function toBase64Url(jsonObj) {
  return Buffer.from(JSON.stringify(jsonObj), "utf8")
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

function parseJsonEnv(...names) {
  for (const name of names) {
    const raw = String(process.env[name] || "").trim();
    if (!raw) continue;
    try {
      return JSON.parse(raw);
    } catch {
      logPricing("warn", { event: "credentials.parse_failed", env: name });
    }
  }
  return null;
}

function loadServiceAccountCredentials() {
  const fromVision = parseJsonEnv("GOOGLE_VISION_CREDENTIALS_JSON");
  if (fromVision?.client_email && fromVision?.private_key) return fromVision;

  const fromSearch = parseJsonEnv("GOOGLE_SEARCH_CREDENTIALS_JSON");
  if (fromSearch?.client_email && fromSearch?.private_key) return fromSearch;

  const credPath = String(process.env.GOOGLE_APPLICATION_CREDENTIALS || "").trim();
  if (credPath && fs.existsSync(credPath)) {
    try {
      const parsed = JSON.parse(fs.readFileSync(credPath, "utf8"));
      if (parsed?.client_email && parsed?.private_key) return parsed;
    } catch {
      logPricing("warn", { event: "credentials.file_read_failed", path: credPath });
    }
  }
  return null;
}

async function getGoogleAccessToken(scope) {
  const apiKey = String(process.env.GOOGLE_API_KEY || "").trim();
  if (apiKey) return { type: "api_key", value: apiKey };

  const now = Date.now();
  if (visionTokenCache.token && visionTokenCache.expiresAtMs > now + 60_000) {
    return { type: "bearer", value: visionTokenCache.token };
  }

  const svc = loadServiceAccountCredentials();
  if (!svc?.client_email || !svc?.private_key) return null;

  const privateKey = String(svc.private_key).replace(/\\n/g, "\n").trim();
  const iat = Math.floor(now / 1000);
  const exp = iat + 3600;
  const header = { alg: "RS256", typ: "JWT" };
  const claims = {
    iss: svc.client_email,
    scope,
    aud: "https://oauth2.googleapis.com/token",
    iat,
    exp,
  };
  const signingInput = `${toBase64Url(header)}.${toBase64Url(claims)}`;
  const signer = crypto.createSign("RSA-SHA256");
  signer.update(signingInput);
  signer.end();
  const signature = signer
    .sign(privateKey, "base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
  const assertion = `${signingInput}.${signature}`;
  const body = new URLSearchParams({
    grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer",
    assertion,
  }).toString();

  const resp = await axios.post("https://oauth2.googleapis.com/token", body, {
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    timeout: 20000,
  });
  const accessToken = resp.data?.access_token;
  const expiresIn = Number(resp.data?.expires_in || 3600);
  if (!accessToken) return null;
  visionTokenCache = {
    token: accessToken,
    expiresAtMs: Date.now() + Math.max(300, expiresIn - 60) * 1000,
  };
  return { type: "bearer", value: accessToken };
}

function withTimeout(promise, ms, label) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

function extractPricesFromText(text) {
  const prices = [];
  const src = String(text || "");
  // Prefer $40k / 40k / $1.5K before bare $40 (avoids stripping the k multiplier).
  const withSuffix = /\$?\s?([\d,]+(?:\.\d+)?)\s*([kKmM])\b/g;
  let m;
  while ((m = withSuffix.exec(src))) {
    const base = Number(String(m[1]).replace(/,/g, ""));
    if (!Number.isFinite(base)) continue;
    const mult = String(m[2]).toLowerCase() === "m" ? 1_000_000 : 1000;
    const n = Math.round(base * mult);
    if (n >= 5 && n <= 500_000) prices.push(n);
  }
  const re = /\$\s?([\d,]+(?:\.\d{2})?)/g;
  while ((m = re.exec(src))) {
    const n = Number(String(m[1]).replace(/,/g, ""));
    if (Number.isFinite(n) && n >= 5 && n <= 500_000) prices.push(n);
  }
  return prices;
}

function uniqStrings(arr, max = 30) {
  const seen = new Set();
  const out = [];
  for (const s of arr || []) {
    const t = String(s || "").trim();
    if (!t || seen.has(t.toLowerCase())) continue;
    seen.add(t.toLowerCase());
    out.push(t);
    if (out.length >= max) break;
  }
  return out;
}

/** Manufacturer site hints from the *submitted* brand only — never invent a brand from Vision. */
function brandSiteHintsFromSubmittedBrand(brandRaw) {
  const blob = String(brandRaw || "").toLowerCase();
  if (!blob) return [];
  const sites = [];
  const push = (host) => {
    if (host && !sites.includes(host)) sites.push(host);
  };

  if (/stanford/.test(blob)) push("stanfordfurniture.com");
  if (/stickley/.test(blob)) push("stickley.com");
  if (/\bbaker\b/.test(blob)) push("bakerfurniture.com");
  if (/century/.test(blob)) push("centuryfurniture.com");
  if (/henredon/.test(blob)) push("henredon.com");
  if (/thomasville/.test(blob)) push("thomasville.com");
  if (/ethan allen|ethanallen/.test(blob)) push("ethanallen.com");
  if (/mitchel+l?\s*gold|mitchell\s*gold|bob\s*williams/.test(blob)) {
    // No reliable official retail domain — brand-led marketplace search instead.
  }
  if (/restoration hardware|\brh\b/.test(blob)) {
    push("rh.com");
    push("restorationhardware.com");
  }
  if (/design within reach|\bdwr\b/.test(blob)) push("dwr.com");
  if (/\barticle\b/.test(blob)) push("article.com");
  if (/crate\s*&?\s*barrel|crateandbarrel/.test(blob)) push("crateandbarrel.com");
  if (/pottery barn|potterybarn/.test(blob)) push("potterybarn.com");
  if (/west elm|westelm/.test(blob)) push("westelm.com");
  if (/\barhaus\b/.test(blob)) push("arhaus.com");
  if (/\bcb2\b/.test(blob)) push("cb2.com");
  if (/room\s*&?\s*board|roomandboard/.test(blob)) push("roomandboard.com");
  if (/bernhardt/.test(blob)) push("bernhardt.com");

  return sites.slice(0, 3);
}

/**
 * Build CSE queries.
 * - If consignor submitted a brand → every query leads with that brand (typo aliases OK for search).
 * - If no brand → search name/category/photos only; do NOT invent a brand from Vision/web entities.
 */
function buildItemSearchQueries(item, visionBundle) {
  const submittedBrand = String(item?.brand || "").trim();
  const brandInfo = normalizeBrandAliases(submittedBrand);
  // Use submitted brand text for display/search; aliases only expand typos when brand exists.
  const brand = brandInfo.searchBrand || submittedBrand;
  const name = String(item?.itemName || "").trim();
  const category = String(item?.category || "").trim();
  const isHandbag = itemCategoryIsHandbag(item);
  const hasBrand = Boolean(brand);
  const productBits = [name, category].filter(Boolean).join(" ").slice(0, 80);
  const queries = [];

  if (isHandbag) {
    if (hasBrand && name) {
      queries.push(`${brand} ${name} new retail price`);
      queries.push(`${brand} ${name} msrp`);
      queries.push(`${brand} ${name} fashionphile`);
      queries.push(`${brand} ${name} therealreal`);
      queries.push(`${brand} ${name} rebag`);
      queries.push(`${brand} ${name} ebay`);
    } else if (name) {
      queries.push(`${name} ${category} handbag resale`.trim());
      queries.push(`${name} fashionphile OR therealreal`.trim());
    }
  } else if (hasBrand) {
    // Brand-first — never a generic Wayfair category search without brand.
    const sites = [
      ...brandSiteHintsFromSubmittedBrand(submittedBrand),
      "chairish.com",
      "1stdibs.com",
      "aptdeco.com",
      "kashew.com",
      "wayfair.com",
    ].slice(0, 6);
    for (const site of sites) {
      queries.push(`site:${site} ${brand} ${productBits || name}`.trim());
    }
    queries.push(`${brand} ${name} retail price`.trim());
    queries.push(`${brand} ${name} msrp`.trim());
    queries.push(`"${brand}" ${name || category} used resale`.trim());
    queries.push(`${brand} ${name || category} chairish OR aptdeco OR kashew`.trim());
    // Typo / alias expansion only when a brand was submitted (never invent when blank).
    for (const alias of (brandInfo.aliases || []).slice(0, 2)) {
      if (!alias || alias.toLowerCase() === brand.toLowerCase()) continue;
      queries.push(`${alias} ${productBits || name}`.trim());
    }
  } else {
    // No brand submitted — do not invent one. Name/category + visual material clues only.
    const visionCue = [
      ...(visionBundle?.labels || []),
      ...(visionBundle?.webEntities || []),
    ]
      .map((s) => String(s || "").toLowerCase())
      .filter((s) =>
        /\b(leather|iron|wrought|whipstitch|brutalist|bamboo|tufted|nailhead|velvet|brass|oak|walnut)\b/.test(
          s
        )
      )
      .slice(0, 4);
    const styleBits = uniqStrings(visionCue, 4).join(" ");
    const seatingQuery = [styleBits, productBits || name, "leather iron bar stool"]
      .filter(Boolean)
      .join(" ")
      .replace(/\s+/g, " ")
      .trim();

    if (productBits) {
      queries.push(`${productBits} leather iron bar stool`.trim());
      queries.push(`${productBits} ${styleBits} 1stdibs`.trim());
      queries.push(`site:1stdibs.com ${productBits} ${styleBits}`.trim());
      queries.push(`site:chairish.com ${productBits} ${styleBits}`.trim());
      queries.push(`site:kashew.com ${productBits} leather stool`.trim());
      queries.push(`${seatingQuery} resale`.trim());
    }
    // Vision entities as visual clue only when they look product-like (not random).
    for (const entity of (visionBundle?.webEntities || []).slice(0, 3)) {
      const e = String(entity || "").trim();
      if (e.length < 4) continue;
      if (/\b(lamp|lighting|porcelain|rose|vase|mirror)\b/i.test(e)) continue;
      queries.push(`${e} ${name || "bar stool"} leather`.trim());
      queries.push(`site:1stdibs.com ${e}`.trim());
    }
  }

  // Detected text on tags/labels — useful, but never invent a brand from it when none submitted.
  for (const textLine of (visionBundle?.detectedText || []).slice(0, 2)) {
    if (String(textLine).length < 4) continue;
    if (hasBrand) {
      queries.push(`"${textLine}" ${brand}`.trim());
    } else {
      queries.push(`"${textLine}" ${name || category || "furniture"}`.trim());
    }
  }

  return uniqStrings(
    queries.map((q) => String(q || "").replace(/\s+/g, " ").trim()).filter(Boolean),
    10
  );
}

/** Strong reverse-image evidence — never treat as disposable noise. */
function isStrongVisionMatch(rowOrPage) {
  const kind = String(rowOrPage?.visionKind || rowOrPage?.kind || "").toLowerCase();
  if (kind === "page_match" || kind === "full_image_match") return true;
  if ((rowOrPage?.seenInPhotos || 0) >= 2) return true;
  if ((rowOrPage?.score || 0) >= 0.85 && rowOrPage?.fromVision) return true;
  return false;
}

/** Turn Vision reverse-image page matches into search rows (all photos merged). */
function searchRowsFromVisionPages(visionBundle, item = null) {
  const pages = Array.isArray(visionBundle?.visionPages) ? visionBundle.visionPages : [];
  const byUrl = new Map();
  for (const p of pages) {
    const url = String(p?.url || "").trim();
    if (!url.startsWith("http")) continue;
    if (isJunkCompSource(url, p.title || "", "")) continue;
    if (item && listingConflictsWithItem(item, p.title, "", url)) continue;
    const strong = isStrongVisionMatch(p);
    const tier = getCompSourceTier(url, p.title || "", "");
    // Keep strong image matches even on lesser-known manufacturer hosts.
    if (!strong && (!tier || tier === "weak")) continue;
    // Strong visual match still must be the same product family when we can tell.
    if (item && strong && !listingMatchesItem(item, p.title, "", url)) {
      // Allow strong vision pages only when they don't conflict; family conflict already blocked.
      // Require at least a seating/product noun overlap for priced later use.
      const title = String(p.title || "").toLowerCase();
      const itemBlob = `${item?.itemName || ""} ${item?.category || ""}`.toLowerCase();
      const needStool = /\bstools?\b/.test(itemBlob);
      if (needStool && !/\bstools?\b/.test(title) && !/\bchairs?\b/.test(title)) continue;
    }
    const kindBoost = p.kind === "page_match" ? 30 : p.kind === "full_image_match" ? 22 : 5;
    const multiBoost = Math.min(20, ((p.seenInPhotos || 1) - 1) * 10);
    const score =
      compSourceTierScore(tier || "listing") + kindBoost + multiBoost + Math.round((p.score || 0) * 10);
    const prev = byUrl.get(url);
    if (prev && prev._rank >= score) {
      prev.seenInPhotos = Math.max(prev.seenInPhotos || 1, p.seenInPhotos || 1);
      continue;
    }
    const title = String(p.title || "").trim() || "Visual match";
    const snippet = `Google Vision ${p.kind || "match"} (multi-photo reverse image${
      (p.seenInPhotos || 1) > 1 ? `, seen in ${p.seenInPhotos} photos` : ""
    })`;
    const prices = extractPricesFromText(`${title} ${snippet}`);
    byUrl.set(url, {
      title,
      url,
      snippet,
      priceHint: prices.length ? prices[0] : null,
      prices,
      query: "vision:multi_image_web_detection",
      fromVision: true,
      strongVisionMatch: strong,
      visionKind: p.kind || "match",
      seenInPhotos: p.seenInPhotos || 1,
      _rank: score,
    });
  }
  return [...byUrl.values()].sort((a, b) => b._rank - a._rank);
}

function rankSearchResult(row) {
  const tier = getCompSourceTier(row.url, row.title, row.snippet);
  let score = compSourceTierScore(tier);
  if (row.fromVision) score += 25;
  if (row.strongVisionMatch || isStrongVisionMatch(row)) score += 35;
  if (row.priceHint != null) score += 8;
  if (isCredibleCompSource(row.url, row.title, row.snippet)) score += 5;
  return score;
}

function extractDetectedTextFromVision(result) {
  const full = String(result?.fullTextAnnotation?.text || "").trim();
  if (full) {
    return uniqStrings(
      full
        .split(/\n/)
        .map((line) => line.replace(/\s+/g, " ").trim())
        .filter((line) => line.length >= 2 && line.length <= 120),
      12
    );
  }

  const sparse = (result?.textAnnotations || [])
    .slice(1)
    .map((row) => String(row?.description || "").trim())
    .filter(Boolean);
  return uniqStrings(sparse, 12);
}

function extractLogosFromVision(result) {
  return uniqStrings(
    (result?.logoAnnotations || [])
      .filter((logo) => (logo.score ?? 0) >= 0.45 && logo.description)
      .sort((a, b) => (b.score ?? 0) - (a.score ?? 0))
      .map((logo) => logo.description),
    8
  );
}

async function annotateImageWithVision(file) {
  const auth = await getGoogleAccessToken("https://www.googleapis.com/auth/cloud-vision");
  if (!auth) {
    throw new Error("Google Vision credentials not configured");
  }

  let buffer = file?.buffer;
  if (!buffer?.length) {
    throw new Error("Photo buffer is empty.");
  }
  if (!isJpegBuffer(buffer)) {
    buffer = await normalizePhotoBufferForPdf(buffer, file.mimetype);
  }

  const content = buffer.toString("base64");
  const requestBody = {
    requests: [
      {
        image: { content },
        features: [
          { type: "WEB_DETECTION", maxResults: 20 },
          { type: "LABEL_DETECTION", maxResults: 12 },
          { type: "LOGO_DETECTION", maxResults: 8 },
          { type: "DOCUMENT_TEXT_DETECTION", maxResults: 1 },
        ],
      },
    ],
  };

  const url =
    auth.type === "api_key"
      ? `${VISION_URL}?key=${encodeURIComponent(auth.value)}`
      : VISION_URL;
  const headers =
    auth.type === "bearer" ? { Authorization: `Bearer ${auth.value}` } : {};

  const resp = await axios.post(url, requestBody, {
    headers: { "Content-Type": "application/json", ...headers },
    timeout: 25000,
  });

  const result = resp.data?.responses?.[0];
  if (result?.error) {
    throw new Error(result.error.message || "Vision API error");
  }

  const web = result?.webDetection || {};
  const labels = (result?.labelAnnotations || [])
    .filter((l) => (l.score ?? 0) >= 0.6)
    .map((l) => l.description)
    .filter(Boolean);

  const webEntities = (web.webEntities || [])
    .filter((e) => (e.score ?? 0) >= 0.5 && e.description)
    .sort((a, b) => (b.score ?? 0) - (a.score ?? 0))
    .map((e) => ({ description: e.description, score: e.score ?? 0 }));

  const pages = (web.pagesWithMatchingImages || []).map((p) => ({
    title: p.pageTitle || "",
    url: p.url || "",
    score: 1,
    kind: "page_match",
  }));

  const fullMatches = (web.fullMatchingImages || []).map((img) => ({
    title: "Visual match",
    url: img.url || "",
    score: 0.9,
    kind: "full_image_match",
  }));

  const similar = (web.visuallySimilarImages || []).slice(0, 5).map((img) => ({
    title: "Similar listing",
    url: img.url || "",
    score: 0.55,
    kind: "similar_image",
  }));

  return {
    labels: uniqStrings(labels, 12),
    webEntities: webEntities.map((e) => e.description),
    webEntityScores: webEntities,
    visionPages: [...pages, ...fullMatches, ...similar].filter((p) => p.url),
    logos: extractLogosFromVision(result),
    detectedText: extractDetectedTextFromVision(result),
  };
}

async function runVisionOnPhotos(photos) {
  const slice = (photos || []).slice(0, MAX_VISION_IMAGES);
  if (!slice.length) {
    return emptyVisionBundle();
  }

  const merged = emptyVisionBundle();

  // Parallel reverse-image (WEB_DETECTION) on every photo in the slice, then merge.
  const visionResults = await Promise.all(
    slice.map(async (file, idx) => {
      try {
        const one = await annotateImageWithVision(file);
        return { ...one, photoIndex: idx + 1 };
      } catch (err) {
        logPricing("warn", {
          event: "vision.image_failed",
          photoIndex: idx + 1,
          message: err?.message || String(err),
        });
        return null;
      }
    })
  );

  const entityScoreMap = new Map();
  const pageByUrl = new Map();

  for (const one of visionResults) {
    if (!one) continue;
    merged.labels.push(...one.labels);
    merged.logos.push(...(one.logos || []));
    merged.detectedText.push(...(one.detectedText || []));
    for (const row of one.webEntityScores || []) {
      const key = String(row.description || "").trim();
      if (!key) continue;
      const prev = entityScoreMap.get(key) || 0;
      entityScoreMap.set(key, Math.max(prev, Number(row.score) || 0));
    }
    for (const page of one.visionPages || []) {
      const url = String(page.url || "").trim();
      if (!url) continue;
      const prev = pageByUrl.get(url);
      const score = Number(page.score) || 0;
      if (!prev || score > (prev.score || 0)) {
        pageByUrl.set(url, {
          ...page,
          seenInPhotos: (prev?.seenInPhotos || 0) + 1,
        });
      } else if (prev) {
        prev.seenInPhotos = (prev.seenInPhotos || 1) + 1;
      }
    }
  }

  // Prefer entities / pages seen across multiple photos.
  merged.webEntityScores = [...entityScoreMap.entries()]
    .map(([description, score]) => ({ description, score }))
    .sort((a, b) => b.score - a.score);
  merged.webEntities = merged.webEntityScores.map((e) => e.description);
  merged.visionPages = [...pageByUrl.values()].sort((a, b) => {
    const multi = (b.seenInPhotos || 1) - (a.seenInPhotos || 1);
    if (multi) return multi;
    return (b.score || 0) - (a.score || 0);
  });

  merged.labels = uniqStrings(merged.labels, 15);
  merged.webEntities = uniqStrings(merged.webEntities, 15);
  merged.logos = uniqStrings(merged.logos, 10);
  merged.detectedText = uniqStrings(merged.detectedText, 16);
  merged.photosAnalyzed = visionResults.filter(Boolean).length;
  return merged;
}

async function googleCustomSearch(query) {
  const apiKey = String(process.env.GOOGLE_API_KEY || "").trim();
  const cx = String(process.env.GOOGLE_CSE_ID || "").trim();
  if (!apiKey || !cx) {
    throw new Error("GOOGLE_API_KEY or GOOGLE_CSE_ID not configured");
  }

  const resp = await axios.get(CSE_URL, {
    params: { key: apiKey, cx, q: query, num: 8 },
    timeout: 15000,
  });

  return (resp.data?.items || []).map((item) => {
    const snippet = [item.title, item.snippet].filter(Boolean).join(" ");
    const prices = extractPricesFromText(snippet);
    return {
      title: item.title || "",
      url: item.link || "",
      snippet: item.snippet || "",
      priceHint: prices.length ? prices[0] : null,
      prices,
    };
  });
}

async function fetchSearchResults(item, visionBundle) {
  const queryCap = itemCategoryIsHandbag(item) ? Math.max(MAX_CSE_QUERIES, 8) : Math.max(MAX_CSE_QUERIES, 8);
  const queries = buildItemSearchQueries(item, visionBundle).slice(0, queryCap);
  const all = [];

  // Multi-image Vision matches first — these are reverse-image evidence from every photo.
  all.push(...searchRowsFromVisionPages(visionBundle, item));

  const queryResults = await Promise.all(
    queries.map(async (q) => {
      try {
        const hits = await googleCustomSearch(q);
        return hits.map((hit) => ({ ...hit, query: q }));
      } catch (err) {
        logPricing("warn", { event: "cse.query_failed", query: q, message: err?.message });
        return [];
      }
    })
  );
  for (const hits of queryResults) {
    all.push(...hits);
  }

  const byUrl = new Map();
  for (const row of all) {
    const url = String(row.url || "").trim();
    if (!url) continue;
    if (isJunkCompSource(url, row.title, row.snippet)) continue;
    // Drop obvious wrong-product hits before ranking (lamp ≠ stool).
    if (listingConflictsWithItem(item, row.title, row.snippet, url)) continue;
    const ranked = { ...row, _rank: rankSearchResult(row) };
    const prev = byUrl.get(url);
    if (!prev || ranked._rank > (prev._rank || 0)) {
      byUrl.set(url, ranked);
    }
  }

  return [...byUrl.values()]
    .sort((a, b) => (b._rank || 0) - (a._rank || 0))
    .slice(0, 32)
    .map(({ _rank, ...rest }) => rest);
}

function defaultAnalysisPayload() {
  return {
    photoBundle: {
      mixedItemsDetected: false,
      mixedItemsConfidence: null,
      distinctItemCount: 1,
      photoObservations: "",
      pieces: [],
    },
    comparableComps: {
      average: null,
      high: null,
      medium: null,
      low: null,
      confidence: "low",
    },
    suggestedPricing: {
      rangeLow: null,
      rangeHigh: null,
      retailEstimate: null,
      pricingAnchor: null,
      velocityLabel: "Standard Seller",
      rationale: "",
    },
    /** Directional brand/category guidance for reviewers — never the pricing anchor. */
    reviewerGuidance: {
      noExactRetailComps: false,
      typicalRetailLow: null,
      typicalRetailHigh: null,
      impliedAskLow: null,
      impliedAskHigh: null,
      basis: "",
      notes: "",
    },
    sources: [],
  };
}

function normalizeReviewerGuidance(rawGuidance, suggested = {}) {
  const g = rawGuidance && typeof rawGuidance === "object" ? rawGuidance : {};
  const num = (v) => {
    const n = Number(v);
    return Number.isFinite(n) && n > 0 ? Math.round(n) : null;
  };
  let typicalLow = num(g.typicalRetailLow ?? g.typicalUsd ?? g.lowUsd);
  let typicalHigh = num(g.typicalRetailHigh ?? g.highUsd ?? g.typicalUsd);
  if (typicalLow && typicalHigh && typicalLow > typicalHigh) {
    [typicalLow, typicalHigh] = [typicalHigh, typicalLow];
  }
  // If model only gave one typical retail, treat as a ±20% band.
  if (typicalLow && !typicalHigh) typicalHigh = Math.round(typicalLow * 1.2);
  if (typicalHigh && !typicalLow) typicalLow = Math.round(typicalHigh * 0.8);

  let impliedLow = num(g.impliedAskLow);
  let impliedHigh = num(g.impliedAskHigh);
  if (typicalLow && typicalHigh && (!impliedLow || !impliedHigh)) {
    impliedLow = Math.round(typicalLow * 0.3);
    impliedHigh = Math.round(typicalHigh * 0.5);
  }

  const basis = String(g.basis || "").trim().slice(0, 400);
  const notes = String(g.notes || g.rationale || "").trim().slice(0, 900);
  const noExact =
    Boolean(g.noExactRetailComps) ||
    (!suggested?.retailEstimate && String(suggested?.pricingAnchor || "") !== "retail_30_50");

  if (!typicalLow && !typicalHigh && !notes && !basis) {
    return {
      noExactRetailComps: noExact,
      typicalRetailLow: null,
      typicalRetailHigh: null,
      impliedAskLow: null,
      impliedAskHigh: null,
      basis: "",
      notes: "",
    };
  }

  return {
    noExactRetailComps: noExact,
    typicalRetailLow: typicalLow,
    typicalRetailHigh: typicalHigh,
    impliedAskLow: impliedLow,
    impliedAskHigh: impliedHigh,
    basis,
    notes,
  };
}

function normalizeAnalysisJson(raw) {
  const base = defaultAnalysisPayload();
  if (!raw || typeof raw !== "object") return base;

  const comps = raw.comparableComps || {};
  const suggested = raw.suggestedPricing || {};
  const bundle = raw.photoBundle || {};
  const num = (v) => {
    const n = Number(v);
    return Number.isFinite(n) && n > 0 ? Math.round(n) : null;
  };

  // Raw model bundle only — finalizePhotoBundle runs after we know the item + photo count.
  base.photoBundle = {
    mixedItemsDetected: Boolean(bundle.mixedItemsDetected),
    mixedItemsConfidence: ["high", "medium", "low"].includes(
      String(bundle.mixedItemsConfidence || "").toLowerCase()
    )
      ? String(bundle.mixedItemsConfidence).toLowerCase()
      : null,
    distinctItemCount: Math.max(1, Math.round(Number(bundle.distinctItemCount) || 1)),
    photoObservations: String(bundle.photoObservations || "").trim().slice(0, 1200),
    pieces: Array.isArray(bundle.pieces) ? bundle.pieces : [],
  };

  base.comparableComps = {
    average: num(comps.average),
    high: num(comps.high),
    medium: num(comps.medium),
    low: num(comps.low),
    confidence: ["high", "medium", "low"].includes(String(comps.confidence || "").toLowerCase())
      ? String(comps.confidence).toLowerCase()
      : "low",
  };

  const anchorRaw = String(suggested.pricingAnchor || "").trim().toLowerCase();
  // Reject legacy consignor anchor from the model — tightenPricingAnalysis owns customer-input lines.
  const pricingAnchor = ["retail_30_50", "resale_comp_average"].includes(anchorRaw)
    ? anchorRaw
    : null;

  let rationale = String(suggested.rationale || "").trim().slice(0, 1500);

  base.suggestedPricing = {
    rangeLow: num(suggested.rangeLow),
    rangeHigh: num(suggested.rangeHigh),
    retailEstimate: num(suggested.retailEstimate),
    pricingAnchor,
    velocityLabel: String(suggested.velocityLabel || "Standard Seller").trim() || "Standard Seller",
    rationale,
  };

  base.reviewerGuidance = normalizeReviewerGuidance(raw.reviewerGuidance, base.suggestedPricing);

  const sources = Array.isArray(raw.sources) ? raw.sources : [];
  base.sources = sources
    .map((s) => {
      const ch = String(s?.channel || "").trim().toLowerCase();
      const channel = ["retail", "resale", "customer"].includes(ch) ? ch : null;
      return {
        title: String(s?.title || "").trim(),
        url: String(s?.url || "").trim(),
        price: num(s?.price),
        channel,
        matchStrength: String(s?.matchStrength || "weak").toLowerCase() === "strong" ? "strong" : "weak",
      };
    })
    .filter((s) => s.url)
    .slice(0, 30);

  return base;
}

function extractJsonFromLlmText(text) {
  const raw = String(text || "").trim();
  if (!raw) return null;
  const fence = raw.match(/```(?:json)?\s*([\s\S]*?)```/i);
  const candidate = fence ? fence[1].trim() : raw;
  try {
    return JSON.parse(candidate);
  } catch {
    const start = candidate.indexOf("{");
    const end = candidate.lastIndexOf("}");
    if (start >= 0 && end > start) {
      try {
        return JSON.parse(candidate.slice(start, end + 1));
      } catch {
        return null;
      }
    }
  }
  return null;
}

function getGeminiModel() {
  return String(process.env.GEMINI_MODEL || DEFAULT_GEMINI_MODEL).trim() || DEFAULT_GEMINI_MODEL;
}

function getOpenAiModel() {
  return String(process.env.OPENAI_MODEL || DEFAULT_OPENAI_MODEL).trim() || DEFAULT_OPENAI_MODEL;
}

function getOpenAiFallbackModels(primaryModel) {
  const fallback =
    String(process.env.OPENAI_MODEL_FALLBACK || DEFAULT_OPENAI_FALLBACK_MODEL).trim() ||
    DEFAULT_OPENAI_FALLBACK_MODEL;
  return uniqStrings([primaryModel, fallback], 3);
}

function openAiGenerationLimits(model) {
  const m = String(model || "").toLowerCase();
  if (m.startsWith("gpt-5") || m.startsWith("o1") || m.startsWith("o3") || m.includes("gpt-4.1")) {
    return { max_completion_tokens: 1400 };
  }
  return { max_tokens: 1400 };
}

function buildFullItemFields(item) {
  const sellerOriginal = String(item?.originalPrice || "").trim();
  const parsedSellerRetail = parseConsignorRetailUsd(item);
  return {
    name: item?.itemName,
    category: item?.category,
    brand: item?.brand,
    age: item?.age,
    condition: item?.condition,
    conditionNotes: item?.conditionNotes,
    dimensions: {
      width: item?.width,
      depth: item?.depth,
      height: item?.height,
    },
    sellerOriginalPriceRaw: sellerOriginal || null,
    sellerOriginalPriceParsedUsd: parsedSellerRetail,
    sellerOriginalPriceNote:
      "Customer-provided only. Never use as the research retailEstimate or market recommendation anchor. Parse suffixes correctly ($40k = 40000).",
    hasPurchaseProof: itemHasPurchaseProof(item),
    proof: item?.proof || item?.proofOfPurchase || null,
    notes: item?.notes,
  };
}

function buildLlmContext(item, visionBundle, searchResults) {
  return {
    item: buildFullItemFields(item),
    vision: {
      labels: visionBundle.labels,
      webEntities: visionBundle.webEntities,
      webEntityScores: (visionBundle.webEntityScores || []).slice(0, 12),
      logos: (visionBundle.logos || []).slice(0, 8),
      detectedText: (visionBundle.detectedText || []).slice(0, 12),
      visionPages: visionBundle.visionPages.slice(0, 16).map((p) => ({
        title: p.title,
        url: p.url,
        kind: p.kind,
        score: p.score,
      })),
    },
    searchResults: searchResults.slice(0, 20).map((r) => ({
      title: r.title,
      url: r.url,
      snippet: r.snippet,
      priceHint: r.priceHint,
      query: r.query,
    })),
    pricingStrategy: {
      shop: "Lost & Found Resale Interiors (Scottsdale consignment)",
      similarItemSearch:
        "ALL photos were reverse-image scanned (Google Vision WEB_DETECTION) and merged. Prefer vision page matches + known retailer/manufacturer URLs (Wayfair, RH, DWR, brand sites, Chairish, 1stDibs). Ignore blogs, forums, Pinterest, Reddit. Study ALL photos together for brand/model/material.",
      comparableCompsRule:
        "comparableComps = average/spread of credible NEW/FULL RETAIL prices from linked research listings ONLY. Never put customer-stated retail into comparableComps or retailEstimate.",
      anchorOrder: [
        "1) RETAIL RESEARCH ANCHOR (required for market recommendation): credible new/full retail from linked listings. pricingAnchor=retail_30_50. rangeLow/rangeHigh = 30–50% of research retailEstimate.",
        "2) RESALE CONTEXT: marketplace prices as supporting sources only — channel=resale, never pricingAnchor for the main ask when retail exists.",
        "3) If no research retail: pricingAnchor=resale_comp_average only as last resort, or leave ranges null — do NOT fall back to sellerOriginalPrice.",
      ],
      customerInputRule:
        "sellerOriginalPrice is a separate customer-input line for the team. Mention it in rationale as customer-stated only. High confidence requires hasPurchaseProof=true plus solid retail comps.",
      multiItemRule:
        "If photos show multiple distinct pieces that are not one SKU/set (e.g. different sofas, chairs, tables), set photoBundle.mixedItemsDetected=true and describe each distinct item. Do not average unrelated pieces into one price.",
      neverRules:
        "Never use sale/clearance/outlet as retail comps. Never treat $40k as $40. Never invent URLs. Exclude unrelated brands/models.",
    },
  };
}

/** Search snippets with linked URLs only — no LLM-only prices. */
function buildHeuristicPricingFromEvidence(searchResults, item) {
  const consignorRetail = parseConsignorRetailUsd(item);
  const hasProof = itemHasPurchaseProof(item);
  const { entries: retailEntries } = collectVerifiedRetailComps(searchResults, [], item);
  const { entries: resaleEntries } = collectVerifiedResaleComps(searchResults, [], item);

  const retailPrices = retailEntries.map((e) => e.price);

  if (!retailPrices.length) {
    if (resaleEntries.length < 2) return null;
    const resaleStats = computeCompStats(resaleEntries.map((e) => e.price));
    if (!resaleStats) return null;
    return {
      comparableComps: resaleStats,
      suggestedPricing: {
        rangeLow: Math.round(resaleStats.average * 0.9),
        rangeHigh: Math.round(resaleStats.average * 1.1),
        retailEstimate: null,
        pricingAnchor: "resale_comp_average",
        velocityLabel: "Standard Seller",
        rationale: [
          "Heuristic fallback: no retail reference found; resale context only (LLM unavailable). Confirm retail before quoting 30–50%.",
          consignorRetail
            ? `Customer-stated retail ($${consignorRetail.toLocaleString("en-US")}) is separate input only — not the research anchor.`
            : null,
        ]
          .filter(Boolean)
          .join(" "),
      },
      sources: resaleEntries.map((e) => ({
        title: e.title,
        url: e.url,
        price: e.price,
        channel: "resale",
        matchStrength: "weak",
      })),
    };
  }

  const retailStats = computeRetailCompStats(retailPrices, { hasProof });
  const retailEstimate = retailStats.average;
  const range = computeRetailAnchoredRange(retailEstimate);
  const sources = [
    ...retailEntries.map((e) => ({
      title: e.title,
      url: e.url,
      price: e.price,
      channel: "retail",
      matchStrength: "strong",
    })),
    ...resaleEntries.map((e) => ({
      title: e.title,
      url: e.url,
      price: e.price,
      channel: "resale",
      matchStrength: "weak",
    })),
  ];

  return {
    comparableComps: retailStats,
    suggestedPricing: {
      rangeLow: range.rangeLow,
      rangeHigh: range.rangeHigh,
      retailEstimate,
      pricingAnchor: "retail_30_50",
      velocityLabel: "Standard Seller",
      rationale: [
        "Heuristic from linked search results (LLM unavailable). Ask anchored at 30–50% of research retail estimate; resale links are directional only.",
        consignorRetail
          ? `Customer-stated retail ($${consignorRetail.toLocaleString("en-US")}) is separate input only — not blended into the research anchor.`
          : null,
      ]
        .filter(Boolean)
        .join(" "),
    },
    sources,
  };
}

const GEMINI_INTERMEDIATE_SYSTEM = `You are an intermediate resale market analyst for a Scottsdale consignment store.
You receive Google Vision labels/web entities, Custom Search snippets, and full item metadata (no photos).
Synthesize evidence into structured comp notes for a senior pricing model. Do NOT output final Lost & Found pricing.

Return JSON ONLY:
{
  "productSummary": string,
  "identifiedProduct": string,
  "mixedItemsSuspected": boolean,
  "distinctItemsNotes": string,
  "visionInsights": string,
  "searchHighlights": [{ "title": string, "url": string, "priceHint": number|null, "relevance": "high"|"medium"|"low", "note": string }],
  "compNotes": [{ "description": string, "estimatedUsd": number|null, "confidence": "high"|"medium"|"low", "sourceUrl": string|null }],
  "priceSignals": { "typicalUsd": number|null, "lowUsd": number|null, "highUsd": number|null, "rationale": string }
}

Rules:
- Use only evidence from the payload; do not invent URLs.
- Gather similar items from retail AND resale channels; tag each price as retail comp vs resale context.
- compNotes for retail listings should reflect new/full retail USD prices (inputs for comparableComps).
- compNotes for resale listings are directional context only — not the pricing anchor.
- sellerOriginalPrice / customer stated retail is context only — never treat it as a verified retail comp.
- If exact SKU comps are missing, still fill priceSignals with a brand/category typical retail band (e.g. Italian leather sectionals from this brand tier) and explain in rationale.
- If vision/search evidence suggests multiple unrelated pieces in one submission, set mixedItemsSuspected=true and describe them.
- Matching pairs/sets of the same furniture SKU (e.g. two identical dining chairs) are NOT mixed items.
- Be concise; this feeds the final pricing model.`;

const OPENAI_FINAL_SYSTEM = `You are the final resale pricing analyst for Lost & Found Resale Interiors (Scottsdale consignment).
You receive consignor photos, full item fields, Google Vision/CSE evidence, and Gemini intermediate comp notes.
Using ALL photos and evidence, identify what the item(s) actually are and produce research-based comparable comps and suggested consignment pricing.

Return JSON ONLY with this exact shape:
{
  "photoBundle": {
    "mixedItemsDetected": boolean,
    "mixedItemsConfidence": "high"|"medium"|"low"|null,
    "distinctItemCount": number,
    "photoObservations": string,
    "pieces": [{ "label": string, "description": string, "pricingNote": string }]
  },
  "comparableComps": { "average": number|null, "high": number|null, "medium": number|null, "low": number|null, "confidence": "high"|"medium"|"low" },
  "suggestedPricing": {
    "rangeLow": number|null,
    "rangeHigh": number|null,
    "retailEstimate": number|null,
    "pricingAnchor": "retail_30_50"|"resale_comp_average"|null,
    "velocityLabel": string,
    "rationale": string
  },
  "reviewerGuidance": {
    "noExactRetailComps": boolean,
    "typicalRetailLow": number|null,
    "typicalRetailHigh": number|null,
    "impliedAskLow": number|null,
    "impliedAskHigh": number|null,
    "basis": string,
    "notes": string
  },
  "sources": [{ "title": string, "url": string, "price": number|null, "channel": "retail"|"resale"|"customer", "matchStrength": "strong"|"weak" }]
}

Rules:
- Study ALL photos together as one evidence pack (labels, feet, fabric, tags, scale). Do not ID from a single photo when others are available.
- Brand rule: If item.brand is provided, treat it as authoritative — search/comps must stay on that brand (allow minor spelling variants only). Do NOT replace it with a different brand from Vision. If item.brand is empty, do NOT invent a brand; describe style/category and only mention possible brands as uncertain guesses in photoObservations.
- Identify model family, material, color, configuration from photos. Call out labels/tags/text in photos.
- Multi-piece (ONLY when confident they are UNRELATED SKUs):
  • Titles like "2 art pieces" or photos of clearly different artworks/furniture → mixedItemsDetected=true, mixedItemsConfidence="high" or "medium".
  • Matching pairs/sets (e.g. "2 wingback dining chairs", matching stools, dining set, patio set) → mixedItemsDetected=false; price as one SKU/set.
  • When mixed: fill pieces[] with a short description per piece. Do NOT invent one blended market price. Leave rangeLow/rangeHigh null if pieces need separate pricing.
  • Never set mixedItemsDetected=true on low confidence.
- When online comps are thin: still fill reviewerGuidance with a useful brand/category band from what the photos show; say what to verify next (Lens on each angle, manufacturer catalogs).
- Locate similar items across retail (manufacturer sites, dealers, Wayfair, etc.) AND resale (eBay, Facebook Marketplace, consignment, our shop).
- comparableComps + retailEstimate: ONLY from credible NEW/FULL RETAIL research listings with URLs in sources. NEVER use sellerOriginalPrice / customer-stated retail as retailEstimate or as a comparableComp.
- Customer-stated price: mention in rationale as customer input only. Parse correctly: "$40k" means 40000 USD, not 40.
- Resale listings → sources with channel=resale only (directional). Never put resale into comparableComps.
- pricingAnchor:
  • retail_30_50 — when research retail exists. retailEstimate from research. rangeLow/rangeHigh = 30–50% of that research retailEstimate.
  • resale_comp_average — last resort only when no research retail; note retail must be confirmed.
  • null — when evidence is insufficient; leave rangeLow/rangeHigh/retailEstimate null rather than inventing from customer input.
- NEVER use pricingAnchor consignor_retail_30_50. Customer input is never the market recommendation anchor.
- reviewerGuidance (ALWAYS fill when exact linked retail comps are missing OR thin):
  • Say clearly that no exact retail comps were verified for this SKU/config.
  • Still give a useful brand/category directional band: e.g. "Italian leather L-sectionals from Gamma / similar contemporary brands typically retail ~$X–$Y new; Lost & Found ask often lands near 30–50% of that band."
  • typicalRetailLow/High = that brand/category retail band (not customer-stated price).
  • impliedAskLow/High = roughly 30–50% of that typical retail band (reviewer starting point only).
  • basis: short label like "Gamma / Italian contemporary leather sectional category".
  • notes: 2–4 sentences for the human reviewer — what the photos show, nearby brands/models, what to verify, why confidence is low.
  • This is NOT the market recommendation and must never replace retail_30_50 when linked comps exist.
- confidence: "high" ONLY when item.hasPurchaseProof is true AND you have solid matching retail comps. Otherwise medium/low. Thin or title-only matches → low.
- velocityLabel: "High Seller / Fast Seller" when resale context shows strong demand; otherwise "Standard Seller".
- sources: ONLY URLs that appear in the payload searchResults / visionPages. Never invent or guess links. Tag channel retail|resale|customer. Strong visual matches → matchStrength "strong" ONLY when the listing is the same product type (leather bar stool ≠ lamp, rush chair, or random Kashew hit). Prefer 1stDibs/Chairish product pages with leather + iron/whipstitch cues when photos show that.
- All money numbers positive integers (USD) or null.`;

function normalizeImageMime(file) {
  const fromMulter = String(file?.mimetype || "").trim().toLowerCase();
  if (fromMulter === "image/jpeg" || fromMulter === "image/jpg") return "image/jpeg";
  if (fromMulter === "image/png") return "image/png";
  if (fromMulter === "image/webp") return "image/webp";
  const buf = file?.buffer;
  if (buf?.length >= 12) {
    if (buf[0] === 0xff && buf[1] === 0xd8) return "image/jpeg";
    if (buf[0] === 0x89 && buf[1] === 0x50) return "image/png";
    if (buf[4] === 0x57 && buf[5] === 0x45 && buf[6] === 0x42 && buf[7] === 0x50) {
      return "image/webp";
    }
  }
  return "image/jpeg";
}

async function resizePhotoBuffer(buffer, mimetype = "") {
  try {
    return await normalizePhotoBufferForPdf(buffer, mimetype);
  } catch {
    return buffer;
  }
}

function photosForOpenAi(photos) {
  const all = photos || [];
  const cap = OPENAI_MAX_IMAGES > 0 ? OPENAI_MAX_IMAGES : 10;
  return all.slice(0, cap);
}

async function preparePhotoDataUrls(photos) {
  const slice = photosForOpenAi(photos);
  const urls = [];
  let skippedLarge = 0;
  for (const file of slice) {
    if (!file?.buffer?.length) continue;
    try {
      let buffer = file.buffer;
      if (buffer.length > 350_000 || !isJpegBuffer(buffer)) {
        buffer = await resizePhotoBuffer(buffer, file.mimetype);
      }
      if (buffer.length > 350_000) {
        const sharpMod = await import("sharp");
        const sharp = sharpMod.default || sharpMod;
        buffer = await sharp(buffer)
          .rotate()
          .resize({
            width: MAX_IMAGE_DIMENSION,
            height: MAX_IMAGE_DIMENSION,
            fit: "inside",
            withoutEnlargement: true,
          })
          .jpeg({ quality: 82, mozjpeg: true })
          .toBuffer();
      }
      if (buffer.length > MAX_IMAGE_BYTES) {
        skippedLarge += 1;
        continue;
      }
      const mime =
        buffer[0] === 0xff && buffer[1] === 0xd8 ? "image/jpeg" : normalizeImageMime(file);
      urls.push(`data:${mime};base64,${buffer.toString("base64")}`);
    } catch (err) {
      logPricing("warn", {
        event: "image.prepare_failed",
        message: err?.message || String(err),
      });
    }
  }
  if (skippedLarge > 0) {
    logPricing("warn", {
      event: "openai.images_skipped_large",
      skipped: skippedLarge,
      sent: urls.length,
      attempted: slice.length,
    });
  }
  return urls;
}

function perItemTimeoutMs(photoCount, { background = false } = {}) {
  const n = Number(photoCount) || 0;
  if (background) {
    const base = BACKGROUND_ITEM_TIMEOUT_MS;
    if (n <= 3) return base;
    const extraMs = Math.min((n - 3) * 8000, 30_000);
    return Math.min(base + extraMs, 120_000);
  }
  const base = PER_ITEM_TIMEOUT_MS;
  if (n <= 3) return base;
  const extraMs = Math.min((n - 3) * 1500, 12_000);
  return Math.min(base + extraMs, submissionPricingBudgetMs(MAX_PRICING_ITEMS, { background: false }));
}

/** Scale wall-clock budget (Vision → Search → Gemini → OpenAI per item). */
export function submissionPricingBudgetMs(itemCount, { background = false } = {}) {
  const n = Math.min(Math.max(1, Number(itemCount) || 1), MAX_PRICING_ITEMS);
  if (background) {
    const scaled = n * BACKGROUND_MS_PER_ITEM;
    return Math.min(BACKGROUND_MAX_BUDGET_MS, scaled);
  }
  const perItemMs = Math.max(
    5000,
    parseInt(process.env.CONSIGNMENT_PRICING_MS_PER_ITEM || "8000", 10) || 8000
  );
  const scaled = n * perItemMs;
  const ceiling = Math.max(
    TOTAL_BUDGET_MS,
    parseInt(process.env.CONSIGNMENT_PRICING_MAX_BUDGET_MS || "120000", 10) || 120000
  );
  return Math.min(ceiling, Math.max(TOTAL_BUDGET_MS, scaled));
}

function pricingSkippedOverItemCap(item, itemNumber) {
  return {
    itemNumber,
    itemName: String(item?.itemName ?? "").trim() || `Item ${itemNumber}`,
    available: false,
    reason: "pricing_item_limit",
    modelsUsed: [],
  };
}

async function callGeminiIntermediate(context) {
  const key = String(process.env.GEMINI_API_KEY || "").trim();
  if (!key) return null;

  const configured = getGeminiModel();
  const models = [...new Set([configured, ...GEMINI_MODEL_FALLBACKS])];

  for (const model of models) {
    const url = `${GEMINI_URL}/${encodeURIComponent(model)}:generateContent?key=${encodeURIComponent(key)}`;
    try {
      const resp = await axios.post(
        url,
        {
          contents: [
            {
              role: "user",
              parts: [
                {
                  text: `${GEMINI_INTERMEDIATE_SYSTEM}\n\nPayload:\n${JSON.stringify(context)}`,
                },
              ],
            },
          ],
          generationConfig: {
            temperature: 0.2,
            maxOutputTokens: 1400,
            responseMimeType: "application/json",
          },
        },
        { timeout: 22000 }
      );

      const text =
        resp.data?.candidates?.[0]?.content?.parts?.map((p) => p.text).join("") || "";
      return extractJsonFromLlmText(text);
    } catch (err) {
      logLlmApiError("gemini", err);
      const status = err?.response?.status;
      if (status === 404 || status === 400) continue;
      return null;
    }
  }

  return null;
}

function logLlmApiError(provider, err) {
  const status = err?.response?.status;
  const apiMessage =
    err?.response?.data?.error?.message ||
    err?.response?.data?.message ||
    err?.message ||
    String(err);
  logPricing("warn", {
    event: `${provider}.failed`,
    status,
    message: apiMessage,
  });
}

function isOpenAiModelUnavailableError(err) {
  const status = err?.response?.status;
  if (status === 404) return true;
  const msg = String(
    err?.response?.data?.error?.message || err?.response?.data?.message || err?.message || ""
  ).toLowerCase();
  return (
    status === 400 &&
    (msg.includes("model") ||
      msg.includes("does not exist") ||
      msg.includes("not found") ||
      msg.includes("not available"))
  );
}

async function callOpenAiFinal(context, geminiBrief, imageDataUrls, model) {
  const key = String(process.env.OPENAI_API_KEY || "").trim();
  if (!key) return null;

  const userParts = [
    {
      type: "text",
      text: [
        "Finalize pricing using the payload below, all attached photos, and Gemini intermediate notes.",
        "",
        "Structured payload:",
        JSON.stringify(context),
        "",
        "Gemini intermediate comp notes:",
        JSON.stringify(geminiBrief || { note: "Gemini unavailable — use vision and search only." }),
      ].join("\n"),
    },
  ];

  for (let i = 0; i < (imageDataUrls || []).length; i += 1) {
    // First photos get higher detail so labels/tags/configuration are readable.
    userParts.push({
      type: "image_url",
      image_url: { url: imageDataUrls[i], detail: i < 6 ? "high" : "low" },
    });
  }

  const resp = await axios.post(
    OPENAI_URL,
    {
      model,
      temperature: 0.2,
      ...openAiGenerationLimits(model),
      response_format: { type: "json_object" },
      messages: [
        { role: "system", content: OPENAI_FINAL_SYSTEM },
        { role: "user", content: userParts },
      ],
    },
    {
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${key}`,
      },
      timeout: 28000,
    }
  );

  const text = resp.data?.choices?.[0]?.message?.content || "";
  return extractJsonFromLlmText(text);
}

async function synthesizePricing(item, visionBundle, searchResults, photos) {
  const context = buildLlmContext(item, visionBundle, searchResults);
  const modelsUsed = [];

  if ((photos || []).length) {
    modelsUsed.push(VISION_PROVIDER_LABEL);
  }

  const geminiModel = getGeminiModel();
  const [geminiBrief, imageDataUrls] = await Promise.all([
    (async () => {
      try {
        const brief = await callGeminiIntermediate(context);
        return brief;
      } catch (err) {
        logLlmApiError("gemini", err);
        return null;
      }
    })(),
    preparePhotoDataUrls(photos),
  ]);
  if (geminiBrief) modelsUsed.push(`gemini:${geminiModel}`);
  if (imageDataUrls.length) {
    modelsUsed.push(`openai-images:${imageDataUrls.length}`);
  }

  let parsed = null;
  const primaryModel = getOpenAiModel();
  const modelsToTry = getOpenAiFallbackModels(primaryModel);

  for (const model of modelsToTry) {
    try {
      parsed = await callOpenAiFinal(context, geminiBrief, imageDataUrls, model);
      if (parsed) {
        modelsUsed.push(`openai:${model}`);
        break;
      }
    } catch (err) {
      logLlmApiError("openai", err);
      if (!isOpenAiModelUnavailableError(err)) break;
    }
  }

  if (!parsed) {
    parsed = buildHeuristicPricingFromEvidence(searchResults, item);
    if (parsed) modelsUsed.push("heuristic");
  }

  // Prefer OpenAI reviewerGuidance; fall back to Gemini priceSignals for reviewer notes.
  if (parsed && !parsed.reviewerGuidance && geminiBrief?.priceSignals) {
    parsed.reviewerGuidance = {
      noExactRetailComps: true,
      typicalRetailLow: geminiBrief.priceSignals.lowUsd ?? geminiBrief.priceSignals.typicalUsd,
      typicalRetailHigh: geminiBrief.priceSignals.highUsd ?? geminiBrief.priceSignals.typicalUsd,
      basis: [item?.brand, item?.category, item?.itemName].filter(Boolean).join(" / "),
      notes: geminiBrief.priceSignals.rationale || geminiBrief.productSummary || "",
    };
  }

  const normalized = normalizeAnalysisJson(parsed);
  normalized.photoBundle = resolvePhotoBundle(
    item,
    normalized.photoBundle,
    (photos || []).length
  );
  if (normalized.photoBundle.mixedItemsDetected) {
    const n = normalized.photoBundle.distinctItemCount;
    const conf = String(normalized.photoBundle.mixedItemsConfidence || "medium").toUpperCase();
    const note = `MULTIPLE DISTINCT PIECES (${n}, confidence ${conf}): ${
      normalized.photoBundle.photoObservations || "Do not treat as a single SKU — price each piece separately."
    }`;
    const rationale = String(normalized.suggestedPricing.rationale || "");
    if (!rationale.includes("MULTIPLE DISTINCT PIECES")) {
      normalized.suggestedPricing.rationale = `${note} ${rationale}`.trim().slice(0, 1800);
    }
    // Do not blend a single ask across distinct pieces.
    normalized.suggestedPricing.rangeLow = null;
    normalized.suggestedPricing.rangeHigh = null;
    normalized.suggestedPricing.pricingAnchor = null;
    normalized.suggestedPricing.retailEstimate = null;
  }

  const strongVisionUrls = new Set(
    visionBundle.visionPages
      .filter((p) => (p.score ?? 0) >= 0.65)
      .map((p) => p.url)
  );

  normalized.sources = normalized.sources.map((s) => {
    if (s.matchStrength === "strong") return s;
    if (strongVisionUrls.has(s.url)) return { ...s, matchStrength: "strong" };
    return s;
  });

  return {
    analysis: normalized,
    modelsUsed: uniqStrings(modelsUsed, 6),
    openaiImageCount: imageDataUrls.length,
  };
}

function hasGoogleVisionCredentials() {
  if (Boolean(process.env.GOOGLE_API_KEY?.trim())) return true;
  if (parseJsonEnv("GOOGLE_VISION_CREDENTIALS_JSON")) return true;
  if (parseJsonEnv("GOOGLE_SEARCH_CREDENTIALS_JSON")) return true;

  const credPath = String(process.env.GOOGLE_APPLICATION_CREDENTIALS || "").trim();
  if (!credPath || !fs.existsSync(credPath)) return false;
  try {
    const parsed = JSON.parse(fs.readFileSync(credPath, "utf8"));
    return Boolean(parsed?.client_email && parsed?.private_key);
  } catch {
    return false;
  }
}

function hasComparablePricing(analysis) {
  const comps = analysis?.comparableComps || {};
  const suggested = analysis?.suggestedPricing || {};
  const hasComp = ["average", "high", "medium", "low"].some((k) => comps[k] != null);
  const hasSuggested = suggested.rangeLow != null && suggested.rangeHigh != null;
  // Still show the team block when research comps are thin but customer input / photo callouts exist.
  const hasCustomerLine = suggested.customerStatedRetail != null;
  const hasPhotoCallout = shouldShowMultiPieceCallout(analysis?.photoBundle);
  const hasRationale = Boolean(String(suggested.rationale || "").trim());
  const g = analysis?.reviewerGuidance || {};
  const hasGuidance =
    Boolean(String(g.notes || "").trim()) ||
    (g.typicalRetailLow != null && g.typicalRetailHigh != null);
  return hasComp || hasSuggested || hasCustomerLine || hasPhotoCallout || hasRationale || hasGuidance;
}

/** Diagnostics for Render logs / support (does not expose secrets). */
export function getPricingConfigStatus() {
  const hasVision = hasGoogleVisionCredentials();
  const hasSearch =
    Boolean(process.env.GOOGLE_API_KEY?.trim()) && Boolean(process.env.GOOGLE_CSE_ID?.trim());
  const hasGemini = Boolean(process.env.GEMINI_API_KEY?.trim());
  const hasOpenAi = Boolean(process.env.OPENAI_API_KEY?.trim());
  const hasLlm = hasGemini && hasOpenAi;

  return {
    configured: hasVision && hasSearch && hasLlm,
    vision: hasVision,
    search: hasSearch,
    llm: hasLlm,
    hasGoogleApiKey: Boolean(process.env.GOOGLE_API_KEY?.trim()),
    hasCseId: Boolean(process.env.GOOGLE_CSE_ID?.trim()),
    hasGemini,
    hasOpenAi,
    openAiModel: getOpenAiModel(),
    geminiModel: getGeminiModel(),
    visionMaxImages: MAX_VISION_IMAGES,
    openAiMaxImages: OPENAI_MAX_IMAGES || null,
  };
}

function pricingConfigured() {
  return getPricingConfigStatus().configured;
}

export function isConsignmentPricingEnabled() {
  const flag = String(process.env.CONSIGNMENT_PRICING_ENABLED ?? "true")
    .trim()
    .toLowerCase();
  return flag !== "false" && flag !== "0" && flag !== "no";
}

function placeholderResultsForItems(items, reason) {
  return items.map((item, index) => {
    const itemNumber = resolveItemNumber(item, index);
    return {
      itemNumber,
      itemName: String(item?.itemName ?? "").trim() || `Item ${itemNumber}`,
      available: false,
      reason,
      modelsUsed: [],
    };
  });
}

/**
 * Run pricing with a wall-clock deadline. Each item is analyzed in turn (with limited
 * concurrency) until the deadline; completed items keep their comps — nothing is wiped.
 */
export async function analyzeConsignmentItemsPricingWithBudget({
  items,
  photoGroups,
  budgetMs,
  background = true,
} = {}) {
  const analyzeCount = Math.min(items?.length ?? 0, MAX_PRICING_ITEMS);
  const effectiveBudgetMs =
    budgetMs ?? submissionPricingBudgetMs(analyzeCount, { background });
  const deadlineMs = Date.now() + effectiveBudgetMs;

  if (!isConsignmentPricingEnabled()) {
    logPricing("info", { event: "skipped", reason: "disabled_env" });
    return {
      results: placeholderResultsForItems(items, "disabled"),
      modelsUsed: [],
      configured: false,
      skipped: true,
      timedOut: false,
    };
  }

  try {
    const value = await analyzeConsignmentItemsPricing({
      items,
      photoGroups,
      deadlineMs,
      background,
    });
    const timedOut = value.results.some((r) => r.reason === "budget_exceeded");
    if (timedOut) {
      logPricing("warn", {
        event: "budget_partial",
        budgetMs: effectiveBudgetMs,
        itemCount: items?.length ?? 0,
        completed: value.results.filter((r) => r.available).length,
      });
    }
    return { ...value, timedOut };
  } catch (err) {
    logPricing("error", {
      event: "batch.failed",
      message: err?.message || String(err),
    });
    return {
      results: placeholderResultsForItems(items, "error"),
      modelsUsed: [],
      configured: pricingConfigured(),
      timedOut: false,
    };
  }
}

function emptyVisionBundle() {
  return {
    labels: [],
    webEntities: [],
    webEntityScores: [],
    visionPages: [],
    logos: [],
    detectedText: [],
    photosAnalyzed: 0,
  };
}

async function analyzeOneItem(item, itemNumber, photos, itemDeadlineMs) {
  const itemName = String(item?.itemName ?? "").trim() || `Item ${itemNumber}`;
  const remaining = () =>
    typeof itemDeadlineMs === "number" && Number.isFinite(itemDeadlineMs)
      ? Math.max(2000, itemDeadlineMs - Date.now())
      : perItemTimeoutMs(photos?.length || 0, { background: true });

  if (!photos?.length) {
    return {
      itemNumber,
      itemName,
      available: false,
      reason: "no_photos",
      modelsUsed: [],
    };
  }

  let visionBundle = emptyVisionBundle();
  try {
    visionBundle = await withTimeout(
      runVisionOnPhotos(photos),
      Math.min(40_000, remaining()),
      "vision"
    );
  } catch (err) {
    logPricing("warn", {
      event: "vision.stage_failed",
      itemNumber,
      message: err?.message || String(err),
    });
  }

  let searchResults = [];
  try {
    // Attach Vision material/style labels so comps can require leather/iron when photos show it.
    item = {
      ...item,
      _visionHints: [
        ...(visionBundle.labels || []),
        ...(visionBundle.webEntities || []).slice(0, 8),
        ...(visionBundle.logos || []),
      ],
    };
    searchResults = await withTimeout(
      fetchSearchResults(item, visionBundle),
      Math.min(22_000, remaining()),
      "search"
    );
  } catch (err) {
    logPricing("warn", {
      event: "search.stage_failed",
      itemNumber,
      message: err?.message || String(err),
    });
  }

  // Probe Google/Vision candidate URLs while the LLM runs — dead links never become comps.
  const linkVerifyPromise = withTimeout(
    verifyLinksLive(searchResults.map((r) => r.url)),
    Math.min(18_000, remaining()),
    "link_verify"
  ).catch((err) => {
    logPricing("warn", {
      event: "link_verify.stage_failed",
      itemNumber,
      message: err?.message || String(err),
    });
    return null;
  });

  let analysis = defaultAnalysisPayload();
  let modelsUsed = [];
  let openaiImageCount = 0;
  try {
    const synthesized = await withTimeout(
      synthesizePricing(item, visionBundle, searchResults, photos),
      Math.min(55_000, remaining()),
      "synthesize"
    );
    analysis = synthesized.analysis;
    modelsUsed = synthesized.modelsUsed;
    openaiImageCount = synthesized.openaiImageCount;
  } catch (err) {
    logPricing("warn", {
      event: "synthesize.stage_failed",
      itemNumber,
      message: err?.message || String(err),
    });
    const prices = searchResults.flatMap((r) => r.prices || []);
    if (prices.length >= 2) {
      analysis = normalizeAnalysisJson(buildHeuristicPricingFromEvidence(searchResults, item));
      modelsUsed = ["heuristic"];
    }
  }

  const liveFromSearch = await linkVerifyPromise;
  const payloadUrls = new Set(
    searchResults.map((r) => String(r.url || "").trim()).filter((u) => u.startsWith("http"))
  );

  if (liveFromSearch) {
    const before = searchResults.length;
    // Keep strong Vision matches even if the live probe fails (CDNs/bot blocks).
    // Only price-anchor comps require linkVerified === true later.
    searchResults = searchResults
      .map((r) => {
        const u = String(r.url || "").trim();
        const live = liveFromSearch.has(u);
        const strong = r.strongVisionMatch || isStrongVisionMatch(r);
        if (live) return { ...r, linkVerified: true };
        if (strong) return { ...r, linkVerified: "visual_only", strongVisionMatch: true };
        return null;
      })
      .filter(Boolean);
    logPricing("info", {
      event: "link_verify.complete",
      itemNumber,
      candidates: before,
      kept: searchResults.length,
      live: searchResults.filter((r) => r.linkVerified === true).length,
      visualOnly: searchResults.filter((r) => r.linkVerified === "visual_only").length,
    });
  } else {
    try {
      const liveRows = await withTimeout(
        filterRowsToLiveLinks(searchResults),
        Math.min(12_000, remaining()),
        "link_verify_fallback"
      );
      const liveSet = new Set(liveRows.map((r) => String(r.url || "").trim()));
      searchResults = searchResults
        .map((r) => {
          const u = String(r.url || "").trim();
          if (liveSet.has(u)) return { ...r, linkVerified: true };
          if (r.strongVisionMatch || isStrongVisionMatch(r)) {
            return { ...r, linkVerified: "visual_only", strongVisionMatch: true };
          }
          return null;
        })
        .filter(Boolean);
    } catch {
      // Don't wipe Google evidence on probe failure — mark for soft use.
      searchResults = searchResults.map((r) => ({
        ...r,
        linkVerified:
          r.strongVisionMatch || isStrongVisionMatch(r) ? "visual_only" : false,
      }));
    }
  }

  if (analysis && typeof analysis === "object") {
    const keptUrls = new Set(searchResults.map((r) => String(r.url || "").trim()));
    // LLM may only cite URLs from the Google/Vision payload we still kept.
    analysis.sources = (analysis.sources || [])
      .filter((s) => {
        const u = String(s?.url || "").trim();
        if (!u.startsWith("http")) return false;
        if (!payloadUrls.has(u)) return false;
        if (!keptUrls.has(u)) return false;
        // Never keep wrong-product LLM citations (lamp for a stool, etc.).
        if (listingConflictsWithItem(item, s.title, "", u)) return false;
        if (!listingMatchesItem(item, s.title, "", u) && !s.strongVisionMatch) return false;
        if (s.strongVisionMatch && listingConflictsWithItem(item, s.title, "", u)) return false;
        return true;
      })
      .map((s) => {
        const row = searchResults.find((r) => String(r.url || "").trim() === String(s.url || "").trim());
        const matched = listingMatchesItem(item, s.title || row?.title, "", String(s.url || "").trim());
        return {
          ...s,
          linkVerified: row?.linkVerified ?? s.linkVerified,
          strongVisionMatch: Boolean(
            (row?.strongVisionMatch || s.matchStrength === "strong") && matched
          ),
          matchStrength: matched && s.matchStrength === "strong" ? "strong" : matched ? s.matchStrength || "weak" : "weak",
        };
      })
      .filter((s) => listingMatchesItem(item, s.title, "", String(s.url || "").trim()));

    // Re-check only non-strong sources that still need a live stamp.
    const needsLive = analysis.sources.filter((s) => s.linkVerified !== true && !s.strongVisionMatch);
    if (needsLive.length) {
      try {
        const liveSources = await withTimeout(
          filterSourcesToLiveLinks(needsLive, { max: 12 }),
          Math.min(10_000, remaining()),
          "source_link_verify"
        );
        const liveSet = new Set(liveSources.map((s) => String(s.url || "").trim()));
        analysis.sources = analysis.sources.filter(
          (s) => s.linkVerified === true || s.strongVisionMatch || liveSet.has(String(s.url || "").trim())
        );
      } catch {
        analysis.sources = analysis.sources.filter(
          (s) => s.linkVerified === true || s.strongVisionMatch || s.linkVerified === "visual_only"
        );
      }
    }

    analysis = tightenPricingAnalysis(analysis, item, searchResults);
    analysis.categorizedLinks = buildCategorizedLinks({
      item,
      analysis,
    });
    analysis.linkVerification = {
      required: true,
      keptHits: searchResults.length,
      liveSearchHits: searchResults.filter((r) => r.linkVerified === true).length,
      strongVisionKept: searchResults.filter((r) => r.strongVisionMatch).length,
      verifiedRetail: (analysis.verifiedRetailCompEntries || []).length,
      verifiedResale: (analysis.verifiedResaleCompEntries || []).length,
    };
  }

  const hasComps = hasComparablePricing(analysis);
  const photoTotal = (photos || []).length;

  logPricing("info", {
    event: "item.complete",
    itemNumber,
    photosAnalyzed: visionBundle.photosAnalyzed || photos.length,
    visionPageMatches: (visionBundle.visionPages || []).length,
    searchHits: searchResults.length,
    modelsUsed,
    available: hasComps,
    photoCount: photoTotal,
    visionImageCount: Math.min(photoTotal, MAX_VISION_IMAGES),
    openaiImageCount: openaiImageCount ?? 0,
  });

  return {
    itemNumber,
    itemName,
    available: hasComps,
    reason: hasComps ? undefined : "no_comps",
    analysis,
    modelsUsed,
  };
}

async function mapPool(items, concurrency, mapper) {
  const results = new Array(items.length);
  let nextIndex = 0;

  async function worker() {
    while (nextIndex < items.length) {
      const i = nextIndex++;
      results[i] = await mapper(items[i], i);
    }
  }

  const workers = Array.from({ length: Math.min(concurrency, items.length) }, () => worker());
  await Promise.all(workers);
  return results;
}

/**
 * Run pricing analysis for each consignment item (non-blocking failures per item).
 * @param {{ items: object[], photoGroups: Map<number, object[]> }} params
 * @returns {Promise<{ results: object[], modelsUsed: string[], configured: boolean }>}
 */
function budgetExceededPlaceholder(item, itemNumber) {
  return {
    itemNumber,
    itemName: String(item?.itemName ?? "").trim() || `Item ${itemNumber}`,
    available: false,
    reason: "budget_exceeded",
    modelsUsed: [],
  };
}

export async function analyzeConsignmentItemsPricing({
  items,
  photoGroups,
  deadlineMs,
  background = true,
} = {}) {
  const configStatus = getPricingConfigStatus();
  const deadline =
    typeof deadlineMs === "number" && Number.isFinite(deadlineMs)
      ? deadlineMs
      : Date.now() +
        submissionPricingBudgetMs(Math.min(items?.length ?? 1, MAX_PRICING_ITEMS), { background });

  if (!configStatus.configured) {
    logPricing("info", { event: "skipped", reason: "not_configured", config: configStatus });
    const results = items.map((item, index) => {
      const itemNumber = resolveItemNumber(item, index);
      return {
        itemNumber,
        itemName: String(item?.itemName ?? "").trim() || `Item ${itemNumber}`,
        available: false,
        reason: "not_configured",
        modelsUsed: [],
      };
    });
    return { results, modelsUsed: [], configured: false, configStatus };
  }

  const jobs = items.map((item, index) => {
    const itemNumber = resolveItemNumber(item, index);
    const photos = photoGroups.get(itemNumber) || [];
    return { item, itemNumber, photos, index };
  });

  const allModels = new Set();

  const results = await mapPool(jobs, MAX_CONCURRENT_ITEMS, async (job) => {
    const { item, itemNumber, photos, index } = job;

    if (index >= MAX_PRICING_ITEMS) {
      return pricingSkippedOverItemCap(item, itemNumber);
    }

    if (Date.now() >= deadline) {
      logPricing("info", {
        event: "item.deadline_skip",
        itemNumber,
        remainingMs: 0,
      });
      return budgetExceededPlaceholder(item, itemNumber);
    }

    const remainingMs = Math.max(500, deadline - Date.now());
    const jobsLeft = Math.max(1, jobs.length - index);
    const fairShareMs = Math.floor(remainingMs / jobsLeft);
    const itemTimeout = Math.min(
      perItemTimeoutMs(photos.length, { background }),
      fairShareMs,
      remainingMs
    );
    const itemDeadlineMs = Date.now() + itemTimeout;

    try {
      const result = await analyzeOneItem(item, itemNumber, photos, itemDeadlineMs);
      for (const m of result.modelsUsed || []) allModels.add(m);
      return result;
    } catch (err) {
      const timedOut = String(err?.message || "").includes("timed out");
      logPricing(timedOut ? "warn" : "error", {
        event: timedOut ? "item.timeout" : "item.failed",
        itemNumber,
        message: err?.message || String(err),
        photoCount: photos.length,
        itemTimeoutMs: itemTimeout,
      });
      return {
        itemNumber,
        itemName: String(item?.itemName ?? "").trim() || `Item ${itemNumber}`,
        available: false,
        reason: timedOut ? "item_timeout" : "error",
        modelsUsed: [],
      };
    }
  });

  return {
    results,
    modelsUsed: [...allModels],
    configured: true,
  };
}

export function getStrongSourcesForItem(pricingResult) {
  if (!pricingResult?.available || !pricingResult.analysis) return [];
  return (pricingResult.analysis.sources || []).filter((s) => s.matchStrength === "strong");
}
