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
 * - CONSIGNMENT_PRICING_MAX_VISION_IMAGES — Google Vision only (default 3; speed/cost cap)
 * - CONSIGNMENT_OPENAI_MAX_IMAGES — GPT vision per item (default 10; 0 = unlimited up to 10 cap)
 * - CONSIGNMENT_MAX_ITEMS — max items per submission and pricing analysis (default 10)
 * - CONSIGNMENT_PRICING_MS_PER_ITEM, CONSIGNMENT_PRICING_MAX_BUDGET_MS — wall-clock budget scaling
 * - CONSIGNMENT_PRICING_MAX_IMAGE_DIM — max edge px for OpenAI images (default 1024; uses sharp if installed)
 */

import axios from "axios";
import crypto from "crypto";
import fs from "fs";
import { buildCategorizedLinks, classifyLinkChannel } from "./consignmentPricingDisplay.js";
import { itemCategoryIsHandbag } from "./consignmentBrand.js";
import { tightenPricingAnalysis, collectVerifiedResaleComps, computeCompStats, computeHandbagMarketRecommendation } from "./consignmentCompTuning.js";
import { MAX_PRICING_ITEMS } from "./consignmentLimits.js";
import { isJpegBuffer, normalizePhotoBufferForPdf } from "./consignmentImageNormalize.js";
import { resolveItemNumber } from "./consignmentValidation.js";

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
  Math.min(5, parseInt(process.env.CONSIGNMENT_PRICING_MAX_VISION_IMAGES || "3", 10) || 3)
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
  Math.min(8, parseInt(process.env.CONSIGNMENT_PRICING_MAX_CSE_QUERIES || "5", 10) || 5)
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
  const re = /\$\s?([\d,]+(?:\.\d{2})?)/g;
  let m;
  while ((m = re.exec(String(text || "")))) {
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

function buildItemSearchQueries(item, visionBundle) {
  const brand = String(item?.brand || "").trim();
  const name = String(item?.itemName || "").trim();
  const category = String(item?.category || "").trim();
  const isHandbag = itemCategoryIsHandbag(item);
  const core = [brand, name, category].filter(Boolean).join(" ").slice(0, 100);
  const queries = [];

  if (isHandbag) {
    if (brand && name) {
      queries.push(`${brand} ${name} fashionphile pre owned price`);
      queries.push(`${brand} ${name} therealreal resale`);
      queries.push(`${brand} ${name} rebag consignment`);
      queries.push(`${brand} ${name} ebay pre owned handbag`);
      queries.push(`${brand} ${name} poshmark`);
    }
    queries.push(`${core} luxury handbag resale price`.trim());
    queries.push(`${core} consignment marketplace used`.trim());
  } else {
    queries.push(`${core} used resale price`.trim());
    queries.push(`${core} consignment marketplace`.trim());
  }

  for (const logo of (visionBundle.logos || []).slice(0, 2)) {
    queries.push(`${logo} ${name || category} used resale price`.trim());
    if (brand) queries.push(`${brand} ${logo} consignment`.trim());
  }

  for (const label of visionBundle.labels.slice(0, 2)) {
    queries.push(`${brand} ${label} used price`.trim());
  }
  for (const entity of visionBundle.webEntities.slice(0, 2)) {
    queries.push(`${entity} resale consignment price`.trim());
  }
  for (const textLine of (visionBundle.detectedText || []).slice(0, 2)) {
    queries.push(`${textLine} ${brand} used resale`.trim());
  }
  return uniqStrings(queries.filter(Boolean), isHandbag ? 10 : 8);
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

  const visionResults = await Promise.all(
    slice.map(async (file) => {
      try {
        return await annotateImageWithVision(file);
      } catch (err) {
        logPricing("warn", {
          event: "vision.image_failed",
          message: err?.message || String(err),
        });
        return null;
      }
    })
  );

  for (const one of visionResults) {
    if (!one) continue;
    merged.labels.push(...one.labels);
    merged.webEntities.push(...one.webEntities);
    merged.webEntityScores.push(...one.webEntityScores);
    merged.visionPages.push(...one.visionPages);
    merged.logos.push(...(one.logos || []));
    merged.detectedText.push(...(one.detectedText || []));
  }

  merged.labels = uniqStrings(merged.labels, 15);
  merged.webEntities = uniqStrings(merged.webEntities, 15);
  merged.logos = uniqStrings(merged.logos, 10);
  merged.detectedText = uniqStrings(merged.detectedText, 16);
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
  const queryCap = itemCategoryIsHandbag(item) ? Math.max(MAX_CSE_QUERIES, 6) : MAX_CSE_QUERIES;
  const queries = buildItemSearchQueries(item, visionBundle).slice(0, queryCap);
  const all = [];
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
    if (!byUrl.has(url)) byUrl.set(url, row);
  }
  return [...byUrl.values()].slice(0, 24);
}

function defaultAnalysisPayload() {
  return {
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
    sources: [],
  };
}

function normalizeAnalysisJson(raw) {
  const base = defaultAnalysisPayload();
  if (!raw || typeof raw !== "object") return base;

  const comps = raw.comparableComps || {};
  const suggested = raw.suggestedPricing || {};
  const num = (v) => {
    const n = Number(v);
    return Number.isFinite(n) && n > 0 ? Math.round(n) : null;
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
  const pricingAnchor = ["retail_30_50", "resale_comp_average", "consignor_retail_30_50"].includes(
    anchorRaw
  )
    ? anchorRaw
    : null;

  base.suggestedPricing = {
    rangeLow: num(suggested.rangeLow),
    rangeHigh: num(suggested.rangeHigh),
    retailEstimate: num(suggested.retailEstimate),
    pricingAnchor,
    velocityLabel: String(suggested.velocityLabel || "Standard Seller").trim() || "Standard Seller",
    rationale: String(suggested.rationale || "").trim().slice(0, 1500),
  };

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
    sellerOriginalPrice: sellerOriginal || null,
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
        "Locate similar items across retail (Wayfair, brands, stores), resale marketplaces, our shop, and vision matches.",
      comparableCompsRule:
        "comparableComps = average/spread of RESALE/secondary prices for like items (e.g. eBay, Facebook, consignment listings).",
      anchorOrder: [
        "1) RESALE COMPS (preferred): gather resale marketplace prices (Fashionphile, The RealReal, Rebag, eBay, Poshmark, Facebook). pricingAnchor=resale_comp_average; rangeLow/rangeHigh = market recommendation band around comparableComps.average.",
        "2) RETAIL REFERENCE: if credible new/full retail is found, use as reference only; still cite resale comps in comparableComps.",
        "3) FALLBACK: consignor originalPrice as reference only when no online comps; pricingAnchor=consignor_retail_30_50.",
      ],
      customerLanguage:
        'In rationale, call the suggested range a "market recommendation" based on online resale comps. Do not lead with "30-50% of retail" phrasing.',
      neverRules:
        "Never use sale/clearance as retail. Resale comp prices must not exceed consignor stated retail. Exclude unrelated brands/models. Never apply a retail percentage formula to a resale listing price.",
    },
  };
}

function parseSellerRetailUsd(item) {
  const raw = String(item?.originalPrice ?? "").replace(/,/g, "");
  const n = Number(raw.replace(/[^\d.]/g, ""));
  return Number.isFinite(n) && n > 0 ? Math.round(n) : null;
}

/** Search snippets with linked URLs only — no LLM-only prices. */
function buildHeuristicPricingFromEvidence(searchResults, item) {
  const { entries, retail } = collectVerifiedResaleComps(searchResults, [], item);
  if (entries.length < 2) return null;
  const prices = entries.map((e) => e.price);
  prices.sort((a, b) => a - b);
  const stats = computeCompStats(prices, retail);
  if (!stats) return null;
  const sources = entries.map((e) => ({
    title: e.title,
    url: e.url,
    price: e.price,
    channel: "resale",
    matchStrength: "strong",
  }));
  const rec = computeHandbagMarketRecommendation(stats, retail);
  if (retail && rec) {
    return {
      comparableComps: stats,
      suggestedPricing: {
        rangeLow: rec.rangeLow,
        rangeHigh: rec.rangeHigh,
        retailEstimate: retail,
        pricingAnchor: "resale_comp_average",
        velocityLabel: "Standard Seller",
        rationale:
          "Heuristic from linked search results only (LLM unavailable). See URLs in PDF.",
      },
      sources,
    };
  }
  return {
    comparableComps: stats,
    suggestedPricing: {
      rangeLow: Math.max(1, Math.round(stats.average * 0.92)),
      rangeHigh: Math.max(1, Math.round(stats.average * 1.08)),
      retailEstimate: stats.average,
      pricingAnchor: "resale_comp_average",
      velocityLabel: "Standard Seller",
      rationale: "Heuristic from linked search results only (LLM unavailable).",
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
  "visionInsights": string,
  "searchHighlights": [{ "title": string, "url": string, "priceHint": number|null, "relevance": "high"|"medium"|"low", "note": string }],
  "compNotes": [{ "description": string, "estimatedUsd": number|null, "confidence": "high"|"medium"|"low", "sourceUrl": string|null }],
  "priceSignals": { "typicalUsd": number|null, "lowUsd": number|null, "highUsd": number|null, "rationale": string }
}

Rules:
- Use only evidence from the payload; do not invent URLs.
- Gather similar items from retail AND resale channels; tag each price as resale comp vs original retail when possible.
- compNotes for resale listings should reflect secondary-market USD prices (inputs for comparableComps averages).
- Be concise; this feeds the final pricing model.`;

const OPENAI_FINAL_SYSTEM = `You are the final resale pricing analyst for Lost & Found Resale Interiors (Scottsdale consignment).
You receive consignor photos, full item fields, Google Vision/CSE evidence, and Gemini intermediate comp notes.
Using ALL of that evidence (especially the photos), produce final comparable comps and suggested consignment pricing.

Return JSON ONLY with this exact shape:
{
  "comparableComps": { "average": number, "high": number, "medium": number, "low": number, "confidence": "high"|"medium"|"low" },
  "suggestedPricing": {
    "rangeLow": number,
    "rangeHigh": number,
    "retailEstimate": number,
    "pricingAnchor": "retail_30_50"|"resale_comp_average"|"consignor_retail_30_50",
    "velocityLabel": string,
    "rationale": string
  },
  "sources": [{ "title": string, "url": string, "price": number|null, "channel": "retail"|"resale"|"customer", "matchStrength": "strong"|"weak" }]
}

Rules:
- Locate similar items across retail (Wayfair, brands, stores) AND resale (eBay, Facebook, our shop, etc.).
- comparableComps: always the average/spread of RESALE/secondary-market prices for like items (same brand/model when possible).
- Resale comp high/low/average must never exceed consignor stated retail or credible new retail.
- pricingAnchor and suggested ask (apply first match):
  • resale_comp_average — PREFERRED when resale marketplace prices exist. comparableComps = average/spread of those resale prices. rangeLow/rangeHigh = market recommendation band (~±10% of average). Set pricingAnchor=resale_comp_average.
  • retail_30_50 — credible new/full RETAIL found; use retailEstimate as reference only. rangeLow/rangeHigh still driven primarily by resale comps when available.
  • consignor_retail_30_50 — last resort: consignor originalPrice as reference when no online comps.
- In rationale: call the range a **market recommendation** based on online resale comps. Cite resale comp average separately from any retail reference. Avoid leading with "30-50% of retail".
- velocityLabel: use "High Seller / Fast Seller" when comps show strong demand/fast turnover signals; otherwise "Standard Seller".
- sources: include all useful URLs from searchResults and vision pages. Tag each with channel: "retail" (new/full price at stores), "resale" (marketplace/used/consignment/our shop), or "customer" (consignor-provided only if in payload). Mark strong visual matches matchStrength "strong".
- Do not invent URLs; only use URLs from the payload.
- All numbers must be positive integers (USD).`;

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

  for (const url of imageDataUrls || []) {
    userParts.push({
      type: "image_url",
      image_url: { url, detail: "low" },
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

  const normalized = normalizeAnalysisJson(parsed);
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
  return hasComp || hasSuggested;
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

  if (analysis && typeof analysis === "object") {
    analysis = tightenPricingAnalysis(analysis, item, searchResults);
    analysis.categorizedLinks = buildCategorizedLinks({
      item,
      analysis,
    });
  }

  const hasComps = hasComparablePricing(analysis);
  const photoTotal = (photos || []).length;

  logPricing("info", {
    event: "item.complete",
    itemNumber,
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
