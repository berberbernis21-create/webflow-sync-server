/**
 * AI comparable pricing for consignment submissions (internal email only).
 * Pipeline: Google Vision → Custom Search → Gemini + OpenAI structured summary.
 */

import axios from "axios";
import crypto from "crypto";
import fs from "fs";
import { resolveItemNumber } from "./consignmentValidation.js";

const VISION_URL = "https://vision.googleapis.com/v1/images:annotate";
const CSE_URL = "https://www.googleapis.com/customsearch/v1";
const GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models";
const OPENAI_URL = "https://api.openai.com/v1/chat/completions";

/** Wall-clock cap for all items in one submission (keeps HTTP path fast when not deferred). */
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
const MAX_CONCURRENT_ITEMS = Math.max(
  1,
  Math.min(4, parseInt(process.env.CONSIGNMENT_PRICING_CONCURRENCY || "2", 10) || 2)
);
const MAX_VISION_IMAGES = Math.max(
  1,
  Math.min(5, parseInt(process.env.CONSIGNMENT_PRICING_MAX_VISION_IMAGES || "3", 10) || 3)
);
const MAX_CSE_QUERIES = Math.max(
  1,
  Math.min(4, parseInt(process.env.CONSIGNMENT_PRICING_MAX_CSE_QUERIES || "2", 10) || 2)
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
  const parts = [
    item?.brand,
    item?.itemName,
    item?.category,
    item?.age,
    "used resale price",
  ]
    .map((p) => String(p || "").trim())
    .filter(Boolean);
  const base = parts.join(" ").slice(0, 120);
  const queries = [base];
  for (const label of visionBundle.labels.slice(0, 3)) {
    queries.push(`${label} used price`);
  }
  for (const entity of visionBundle.webEntities.slice(0, 2)) {
    queries.push(`${entity} resale consignment`);
  }
  return uniqStrings(queries, 5);
}

async function annotateImageWithVision(file) {
  const auth = await getGoogleAccessToken("https://www.googleapis.com/auth/cloud-vision");
  if (!auth) {
    throw new Error("Google Vision credentials not configured");
  }

  const content = file.buffer.toString("base64");
  const requestBody = {
    requests: [
      {
        image: { content },
        features: [
          { type: "WEB_DETECTION", maxResults: 20 },
          { type: "LABEL_DETECTION", maxResults: 12 },
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
  };
}

async function runVisionOnPhotos(photos) {
  const slice = (photos || []).slice(0, MAX_VISION_IMAGES);
  if (!slice.length) {
    return { labels: [], webEntities: [], webEntityScores: [], visionPages: [] };
  }

  const merged = {
    labels: [],
    webEntities: [],
    webEntityScores: [],
    visionPages: [],
  };

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
  }

  merged.labels = uniqStrings(merged.labels, 15);
  merged.webEntities = uniqStrings(merged.webEntities, 15);
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
  const queries = buildItemSearchQueries(item, visionBundle).slice(0, MAX_CSE_QUERIES);
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

  base.suggestedPricing = {
    rangeLow: num(suggested.rangeLow),
    rangeHigh: num(suggested.rangeHigh),
    retailEstimate: num(suggested.retailEstimate),
    velocityLabel: String(suggested.velocityLabel || "Standard Seller").trim() || "Standard Seller",
    rationale: String(suggested.rationale || "").trim().slice(0, 500),
  };

  const sources = Array.isArray(raw.sources) ? raw.sources : [];
  base.sources = sources
    .map((s) => ({
      title: String(s?.title || "").trim(),
      url: String(s?.url || "").trim(),
      price: num(s?.price),
      matchStrength: String(s?.matchStrength || "weak").toLowerCase() === "strong" ? "strong" : "weak",
    }))
    .filter((s) => s.url)
    .slice(0, 20);

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

function buildLlmContext(item, visionBundle, searchResults) {
  const sellerOriginal = String(item?.originalPrice || "").trim();
  return {
    item: {
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
    },
    vision: {
      labels: visionBundle.labels,
      webEntities: visionBundle.webEntities,
      strongPages: visionBundle.visionPages
        .filter((p) => (p.score ?? 0) >= 0.65)
        .slice(0, 12)
        .map((p) => ({ title: p.title, url: p.url, kind: p.kind })),
    },
    searchResults: searchResults.slice(0, 18).map((r) => ({
      title: r.title,
      url: r.url,
      snippet: r.snippet,
      priceHint: r.priceHint,
    })),
    pricingStrategy: {
      consignmentRangePercentOfRetail: "30-50%",
      shop: "Lost & Found Resale Interiors (Scottsdale consignment)",
    },
  };
}

const LLM_SYSTEM = `You are a resale pricing analyst for a Scottsdale consignment store.
Using ONLY the provided vision labels, web entities, search hits, and item fields, estimate comparable market pricing.

Return JSON ONLY with this exact shape:
{
  "comparableComps": { "average": number, "high": number, "medium": number, "low": number, "confidence": "high"|"medium"|"low" },
  "suggestedPricing": {
    "rangeLow": number,
    "rangeHigh": number,
    "retailEstimate": number,
    "velocityLabel": string,
    "rationale": string
  },
  "sources": [{ "title": string, "url": string, "price": number|null, "matchStrength": "strong"|"weak" }]
}

Rules:
- comparableComps must reflect realistic USD resale/secondary market prices from evidence.
- suggestedPricing.rangeLow/rangeHigh: Lost & Found consignment ask range at 30-50% of estimated retail (comps), rounded to sensible dollars.
- velocityLabel: use "High Seller / Fast Seller" when comps show strong demand/fast turnover signals; otherwise "Standard Seller".
- sources: include only listings you believe are strong visual/product matches (mark matchStrength "strong"). Weak matches must be "weak" or omitted.
- Do not invent URLs; only use URLs from searchResults or vision.strongPages.
- All numbers must be positive integers (USD).`;

async function callGemini(context) {
  const key = String(process.env.GEMINI_API_KEY || "").trim();
  if (!key) return null;

  const model =
    String(process.env.GEMINI_MODEL || "gemini-2.0-flash").trim() || "gemini-2.0-flash";
  const url = `${GEMINI_URL}/${encodeURIComponent(model)}:generateContent?key=${encodeURIComponent(key)}`;

  const resp = await axios.post(
    url,
    {
      contents: [
        {
          role: "user",
          parts: [
            {
              text: `${LLM_SYSTEM}\n\nAnalyze:\n${JSON.stringify(context)}`,
            },
          ],
        },
      ],
      generationConfig: {
        temperature: 0.2,
        maxOutputTokens: 1200,
        responseMimeType: "application/json",
      },
    },
    { timeout: 22000 }
  );

  const text =
    resp.data?.candidates?.[0]?.content?.parts?.map((p) => p.text).join("") || "";
  return extractJsonFromLlmText(text);
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

async function callOpenAi(context) {
  const key = String(process.env.OPENAI_API_KEY || "").trim();
  if (!key) return null;

  const model =
    String(process.env.OPENAI_MODEL || "gpt-4.1").trim() || "gpt-4.1";

  const resp = await axios.post(
    OPENAI_URL,
    {
      model,
      temperature: 0.2,
      max_tokens: 1200,
      response_format: { type: "json_object" },
      messages: [
        { role: "system", content: LLM_SYSTEM },
        { role: "user", content: JSON.stringify(context) },
      ],
    },
    {
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${key}`,
      },
      timeout: 22000,
    }
  );

  const text = resp.data?.choices?.[0]?.message?.content || "";
  return extractJsonFromLlmText(text);
}

async function synthesizePricing(item, visionBundle, searchResults) {
  const context = buildLlmContext(item, visionBundle, searchResults);
  const modelsUsed = [];

  let parsed = null;
  try {
    parsed = await callGemini(context);
    if (parsed) modelsUsed.push(String(process.env.GEMINI_MODEL || "gemini-2.0-flash"));
  } catch (err) {
    logLlmApiError("gemini", err);
  }

  if (!parsed) {
    try {
      parsed = await callOpenAi(context);
      if (parsed) modelsUsed.push(String(process.env.OPENAI_MODEL || "gpt-4.1"));
    } catch (err) {
      logLlmApiError("openai", err);
    }
  }

  if (!parsed) {
    const prices = searchResults.flatMap((r) => r.prices || []);
    if (prices.length >= 2) {
      prices.sort((a, b) => a - b);
      const low = prices[0];
      const high = prices[prices.length - 1];
      const avg = Math.round(prices.reduce((s, p) => s + p, 0) / prices.length);
      const retail = Math.round(avg * 1.35);
      parsed = {
        comparableComps: {
          average: avg,
          high,
          medium: prices[Math.floor(prices.length / 2)],
          low,
          confidence: "low",
        },
        suggestedPricing: {
          rangeLow: Math.round(retail * 0.3),
          rangeHigh: Math.round(retail * 0.5),
          retailEstimate: retail,
          velocityLabel: "Standard Seller",
          rationale: "Heuristic from search snippet prices (LLM unavailable).",
        },
        sources: searchResults.slice(0, 6).map((r) => ({
          title: r.title,
          url: r.url,
          price: r.priceHint,
          matchStrength: "weak",
        })),
      };
      modelsUsed.push("heuristic");
    }
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

  for (const page of visionBundle.visionPages.filter((p) => (p.score ?? 0) >= 0.65)) {
    if (normalized.sources.some((s) => s.url === page.url)) continue;
    normalized.sources.push({
      title: page.title || page.url,
      url: page.url,
      price: null,
      matchStrength: "strong",
    });
  }

  return { analysis: normalized, modelsUsed: uniqStrings(modelsUsed, 5) };
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
  const hasLlm =
    Boolean(process.env.GEMINI_API_KEY?.trim()) || Boolean(process.env.OPENAI_API_KEY?.trim());

  return {
    configured: hasVision && hasSearch && hasLlm,
    vision: hasVision,
    search: hasSearch,
    llm: hasLlm,
    hasGoogleApiKey: Boolean(process.env.GOOGLE_API_KEY?.trim()),
    hasCseId: Boolean(process.env.GOOGLE_CSE_ID?.trim()),
    hasGemini: Boolean(process.env.GEMINI_API_KEY?.trim()),
    hasOpenAi: Boolean(process.env.OPENAI_API_KEY?.trim()),
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
 * Run pricing with a hard wall-clock cap for the whole submission (all items).
 * On timeout or top-level failure, returns placeholder results — never throws.
 */
export async function analyzeConsignmentItemsPricingWithBudget({
  items,
  photoGroups,
  budgetMs = TOTAL_BUDGET_MS,
} = {}) {
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

  let timeoutId;
  const timeoutPromise = new Promise((resolve) => {
    timeoutId = setTimeout(() => resolve({ timedOut: true }), budgetMs);
  });

  const analysisPromise = analyzeConsignmentItemsPricing({ items, photoGroups })
    .then((value) => ({ timedOut: false, value }))
    .catch((err) => {
      logPricing("error", {
        event: "batch.failed",
        message: err?.message || String(err),
      });
      return {
        timedOut: false,
        value: {
          results: placeholderResultsForItems(items, "error"),
          modelsUsed: [],
          configured: pricingConfigured(),
        },
      };
    });

  const winner = await Promise.race([analysisPromise, timeoutPromise]);
  clearTimeout(timeoutId);

  if (winner.timedOut) {
    logPricing("warn", {
      event: "budget_exceeded",
      budgetMs,
      itemCount: items?.length ?? 0,
    });
    return {
      results: placeholderResultsForItems(items, "budget_exceeded"),
      modelsUsed: [],
      configured: pricingConfigured(),
      timedOut: true,
    };
  }

  return { ...winner.value, timedOut: false };
}

async function analyzeOneItem(item, itemNumber, photos) {
  const itemName = String(item?.itemName ?? "").trim() || `Item ${itemNumber}`;

  if (!photos?.length) {
    return {
      itemNumber,
      itemName,
      available: false,
      reason: "no_photos",
      modelsUsed: [],
    };
  }

  const visionBundle = await runVisionOnPhotos(photos);
  const searchResults = await fetchSearchResults(item, visionBundle);
  const { analysis, modelsUsed } = await synthesizePricing(item, visionBundle, searchResults);

  const hasComps = hasComparablePricing(analysis);

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
export async function analyzeConsignmentItemsPricing({ items, photoGroups }) {
  const configStatus = getPricingConfigStatus();
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
    return { item, itemNumber, photos };
  });

  const allModels = new Set();

  const results = await mapPool(jobs, MAX_CONCURRENT_ITEMS, async (job) => {
    const { item, itemNumber, photos } = job;
    try {
      const result = await withTimeout(
        analyzeOneItem(item, itemNumber, photos),
        PER_ITEM_TIMEOUT_MS,
        `item_${itemNumber}`
      );
      for (const m of result.modelsUsed || []) allModels.add(m);
      return result;
    } catch (err) {
      logPricing("error", {
        event: "item.failed",
        itemNumber,
        message: err?.message || String(err),
      });
      return {
        itemNumber,
        itemName: String(item?.itemName ?? "").trim() || `Item ${itemNumber}`,
        available: false,
        reason: "error",
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
