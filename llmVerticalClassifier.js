/**
 * LLM-based vertical classifier: LUXURY vs HOME_INTERIOR.
 * Replaces keyword-only logic with GPT semantic classification.
 * Never returns UNKNOWN; defaults to HOME_INTERIOR on low confidence or failure.
 */

import axios from "axios";

const OPENAI_API_URL = "https://api.openai.com/v1/chat/completions";
const DEFAULT_MODEL = "gpt-4o-mini";

/** Strong furniture indicators: if present, force HOME_INTERIOR unless clearly wearable/jewelry. */
const FURNITURE_INDICATORS = [
  "sofa", "sofas", "table", "tables", "dresser", "dressers", "cabinet", "cabinets",
  "desk", "desks", "lamp", "lamps", "chandelier", "chandeliers", "bed", "beds",
  "console", "consoles", "bookshelf", "bookshelves", "chair", "chairs",
  "ottoman", "ottomans", "rug", "rugs", "mirror", "mirrors", "painting", "paintings",
  "headboard", "nightstand", "buffet", "sideboard", "armoire", "credenza",
];

/** Wearable/jewelry terms: if present with furniture, allow LUXURY. */
const WEARABLE_INDICATORS = [
  "handbag", "handbags", "bag", "bags", "wallet", "wallets", "watch", "watches",
  "bracelet", "bracelets", "earring", "earrings", "necklace", "necklaces",
  "ring", "rings", "clutch", "tote", "totes", "belt", "belts", "scarf", "scarves",
  "backpack", "backpacks", "jewelry", "jewellery", "pendant", "brooch", "barrette",
];

function stripHtml(html) {
  if (!html || typeof html !== "string") return "";
  return html.replace(/<[^>]*>/g, " ").replace(/\s+/g, " ").trim();
}

function getProductText(product) {
  const title = (product.title || "").trim();
  const productType = (product.product_type || "").trim();
  const vendor = (product.vendor || "").trim();
  const tags = Array.isArray(product.tags)
    ? product.tags
    : typeof product.tags === "string"
      ? product.tags.split(",").map((s) => s.trim()).filter(Boolean)
      : [];
  const tagsStr = tags.join(", ");
  const description = stripHtml(product.body_html || "").trim();
  return { title, productType, vendor, tagsStr, description };
}

const SYSTEM_PROMPT = `You are a high-precision retail classification engine.
Classify products as either:

LUXURY: wearable designer goods, fine jewelry, watches, prestige fashion brands, high-end collectible personal items (e.g. handbags, clutches, scarves, belts, wallets, designer accessories).

HOME_INTERIOR: furniture, decor, lighting, artwork, home objects, even if expensive or premium.

Important rules:
- Expensive furniture is still HOME_INTERIOR.
- Gold finish furniture is not LUXURY.
- Designer style furniture is not LUXURY.
- Wearable branded prestige goods are LUXURY.
- A Rolex wall clock is HOME_INTERIOR (decor); a Rolex wristwatch is LUXURY.
- If uncertain, choose HOME_INTERIOR.
- Never output anything except valid JSON.`;

const USER_PROMPT_TEMPLATE = `Classify this product. Return only valid JSON in this exact format (no markdown, no extra text):
{"category": "LUXURY" or "HOME_INTERIOR", "confidence": number between 0 and 1, "reasoning": "short explanation"}

Product title: {{title}}
Product type: {{productType}}
Vendor: {{vendor}}
Tags: {{tagsStr}}
Description: {{description}}`;

function buildUserPrompt(product) {
  const { title, productType, vendor, tagsStr, description } = getProductText(product);
  return USER_PROMPT_TEMPLATE
    .replace("{{title}}", title || "(none)")
    .replace("{{productType}}", productType || "(none)")
    .replace("{{vendor}}", vendor || "(none)")
    .replace("{{tagsStr}}", tagsStr || "(none)")
    .replace("{{description}}", description ? description.slice(0, 2000) : "(none)");
}

/**
 * Returns true if text (lowercase) contains any of the terms as whole-word match.
 */
function hasAnyWord(text, terms) {
  if (!text || typeof text !== "string") return false;
  const lower = text.toLowerCase();
  return terms.some((term) => {
    const re = new RegExp("\\b" + term.replace(/[.*+?^${}()|[\]\\]/g, "\\$&") + "\\b", "i");
    return re.test(lower);
  });
}

/**
 * Safety: if product has strong furniture indicators and is NOT clearly wearable/jewelry, force HOME_INTERIOR.
 */
function applyFurnitureSafetyOverride(product, category) {
  if (category !== "LUXURY") return category;
  const { title, description } = getProductText(product);
  const combined = `${title} ${description}`.toLowerCase();
  const hasFurniture = FURNITURE_INDICATORS.some((w) => combined.includes(w));
  if (!hasFurniture) return category;
  const hasWearable = hasAnyWord(combined, WEARABLE_INDICATORS);
  if (hasWearable) return category;
  return "HOME_INTERIOR";
}

/**
 * Parse and validate model output. Returns null if invalid.
 */
function parseModelOutput(rawContent) {
  const trimmed = (rawContent || "").trim();
  // Strip possible markdown code block
  const jsonStr = trimmed.replace(/^```(?:json)?\s*/i, "").replace(/\s*```\s*$/i, "").trim();
  let parsed;
  try {
    parsed = JSON.parse(jsonStr);
  } catch {
    return null;
  }
  if (!parsed || typeof parsed !== "object") return null;
  const cat = parsed.category;
  if (cat !== "LUXURY" && cat !== "HOME_INTERIOR") return null;
  const confidence = typeof parsed.confidence === "number" ? parsed.confidence : parseFloat(parsed.confidence);
  const safeConfidence = Number.isFinite(confidence) ? Math.max(0, Math.min(1, confidence)) : 0;
  return {
    category: cat,
    confidence: safeConfidence,
    reasoning: typeof parsed.reasoning === "string" ? parsed.reasoning : "",
  };
}

/**
 * Second validation pass: ask model to re-evaluate. If disagreement with first pass, choose HOME_INTERIOR.
 */
async function secondPassCheck(product, firstCategory, openaiKey, model, logPayload) {
  const userPrompt = `Re-evaluate. Is this product truly LUXURY (wearable/prestige goods) or is it premium home decor / furniture?
Product: ${(product.title || "").slice(0, 200)}. Type: ${product.product_type || ""}.
First classification was: ${firstCategory}.
Reply with JSON only: {"category": "LUXURY" or "HOME_INTERIOR", "confidence": 0-1, "reasoning": "brief"}`;
  try {
    const res = await axios.post(
      OPENAI_API_URL,
      {
        model,
        messages: [
          { role: "system", content: "You re-evaluate product category. Output only valid JSON." },
          { role: "user", content: userPrompt },
        ],
        temperature: 0,
        top_p: 0,
        max_tokens: 200,
      },
      {
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${openaiKey}`,
        },
        timeout: 15000,
      }
    );
    const raw = res.data?.choices?.[0]?.message?.content;
    const second = parseModelOutput(raw);
    if (second && second.category !== firstCategory) {
      if (logPayload) {
        logPayload.secondPassRaw = raw;
        logPayload.secondPassParsed = second;
        logPayload.secondPassOverride = "HOME_INTERIOR";
      }
      return "HOME_INTERIOR";
    }
    return firstCategory;
  } catch (err) {
    if (logPayload) logPayload.secondPassError = err.message || String(err);
    return firstCategory;
  }
}

/**
 * Classify a Shopify product as LUXURY or HOME_INTERIOR using GPT.
 * Never returns UNKNOWN. Log payload can be passed in for audit logging.
 *
 * @param {object} product - Shopify product { title, product_type, vendor, tags, body_html }
 * @param {object} [logPayload] - Optional object to attach raw/parsed/overrides for logging
 * @param {Function} [logFn] - Optional logger(message, data)
 * @returns {Promise<{ category: "LUXURY" | "HOME_INTERIOR", confidence: number, reasoning: string }>}
 */
export async function classifyWithLLM(product, logPayload = {}, logFn = null) {
  const openaiKey = process.env.OPENAI_API_KEY;
  const model = (process.env.OPENAI_VERTICAL_MODEL || DEFAULT_MODEL).trim() || DEFAULT_MODEL;
  const useSecondPass = process.env.LLM_VERTICAL_SECOND_PASS === "true" || process.env.LLM_VERTICAL_SECOND_PASS === "1";

  const defaultResult = {
    category: "HOME_INTERIOR",
    confidence: 0,
    reasoning: "fallback: no API key or classification failure",
  };

  if (!openaiKey || typeof openaiKey !== "string" || !openaiKey.trim()) {
    if (logFn) logFn("warn", { event: "llm_vertical.no_api_key", message: "OPENAI_API_KEY missing" });
    logPayload.raw = null;
    logPayload.parsed = null;
    logPayload.final = defaultResult;
    logPayload.override = "no_api_key";
    return defaultResult;
  }

  const userPrompt = buildUserPrompt(product);

  try {
    const res = await axios.post(
      OPENAI_API_URL,
      {
        model,
        messages: [
          { role: "system", content: SYSTEM_PROMPT },
          { role: "user", content: userPrompt },
        ],
        temperature: 0,
        top_p: 0,
        max_tokens: 300,
      },
      {
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${openaiKey}`,
        },
        timeout: 20000,
      }
    );

    const rawContent = res.data?.choices?.[0]?.message?.content;
    logPayload.raw = rawContent;

    let parsed = parseModelOutput(rawContent);
    if (!parsed) {
      logPayload.parsed = null;
      logPayload.final = defaultResult;
      logPayload.override = "parse_failed";
      if (logFn) logFn("warn", { event: "llm_vertical.parse_failed", raw: rawContent?.slice(0, 200) });
      return defaultResult;
    }
    logPayload.parsed = { ...parsed };

    let category = parsed.category;
    let override = null;

    // Confidence threshold: < 0.65 → HOME_INTERIOR
    if (parsed.confidence < 0.65) {
      category = "HOME_INTERIOR";
      override = "low_confidence";
    }

    // Optional second pass: if first said LUXURY and second disagrees → HOME_INTERIOR
    if (useSecondPass && category === "LUXURY") {
      category = await secondPassCheck(product, category, openaiKey, model, logPayload);
      if (category === "HOME_INTERIOR" && logPayload.secondPassOverride) override = "second_pass_disagreement";
    }

    // Safety: strong furniture indicators → HOME_INTERIOR unless clearly wearable
    const afterSafety = applyFurnitureSafetyOverride(product, category);
    if (afterSafety !== category) {
      category = afterSafety;
      override = override || "furniture_safety_override";
    }

    const final = {
      category,
      confidence: parsed.confidence,
      reasoning: parsed.reasoning,
    };
    logPayload.final = final;
    if (override) logPayload.override = override;

    if (logFn) {
      logFn("info", {
        event: "llm_vertical.classified",
        category: final.category,
        confidence: final.confidence,
        override: override || undefined,
        reasoning: final.reasoning?.slice(0, 150),
      });
    }
    return final;
  } catch (err) {
    const message = err.response?.data?.error?.message || err.message || String(err);
    if (logFn) logFn("error", { event: "llm_vertical.error", message });
    logPayload.raw = null;
    logPayload.parsed = null;
    logPayload.final = defaultResult;
    logPayload.override = "request_failed";
    logPayload.error = message;
    return defaultResult;
  }
}
