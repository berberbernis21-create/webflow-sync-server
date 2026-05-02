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
  "satchel", "satchels", "mule", "mules", "boot", "boots", "shoe", "shoes",
  "sandal", "sandals", "loafer", "loafers", "sneaker", "sneakers",
];

/** Strong wearable/footwear: if present in title/type/tags, always LUXURY (never furniture). Backpacks, boots, shoes, mules, bags, etc. */
const STRONG_LUXURY_SIGNALS = [
  "backpack", "backpacks", "boot", "boots", "chelsea", "shoe", "shoes", "mule", "mules",
  "handbag", "handbags", "bag", "bags", "wallet", "wallets", "clutch", "tote", "totes",
  "belt", "belts", "scarf", "scarves", "satchel", "satchels", "briefcase", "crossbody", "luggage",
  "earring", "earrings", "bracelet", "bracelets", "necklace", "necklaces", "jewelry", "jewellery",
  "sandal", "sandals", "pump", "pumps", "heel", "heels", "sneaker", "sneakers", "loafer", "loafers", "slide", "slides",
  "oxford", "oxfords", "espadrille", "espadrilles", "wedge", "wedges", "stiletto", "stilettos",
  "trainer", "trainers", "footwear", "hobo", "woc", "pochette", "minaudiere",
];

/** If title/vendor/tags hint these brands, allow vision fallback when text model says HOME_INTERIOR. */
const LUXURY_HOUSE_SIGNALS = [
  "louboutin", "christian louboutin", "chloe", "chloé", "chanel", "hermes", "hermès", "gucci",
  "prada", "fendi", "dior", "balenciaga", "valentino", "saint laurent", "ysl", "celine", "céline",
  "bottega", "givenchy", "burberry", "versace", "jimmy choo", "manolo", "goyard", "moynat",
  "delvaux", "valextra", "loewe", "bvlgari", "bulgari", "cartier", "van cleef", "tiffany",
];
// Note: "flat"/"flats" intentionally omitted — matches geometry (e.g. "Flat Oval … Lamp") and home decor; shoe flats still classify via LLM + other shoe terms.

/** Footwear — always Luxury Goods, never Furniture & Home (title / type / tags only). */
const FOOTWEAR_LUXURY_TERMS = [
  "sneaker",
  "sneakers",
  "shoe",
  "shoes",
  "footwear",
  "trainer",
  "trainers",
  "boot",
  "boots",
  "mule",
  "mules",
  "loafer",
  "loafers",
  "oxford",
  "oxfords",
  "sandal",
  "sandals",
  "heel",
  "heels",
  "pump",
  "pumps",
  "espadrille",
  "espadrilles",
  "slide",
  "slides",
  "stiletto",
  "stilettos",
  "wedge",
  "wedges",
  "cleat",
  "cleats",
  "slingback",
  "slingbacks",
];

/**
 * Sneakers, boots, sandals, mules, and all footwear — always Luxury (never HOME_INTERIOR).
 * Includes compound product names ending in mule/mules (e.g. "Pyramule") where \bmule\b would miss.
 */
export function productLooksLikeFootwearLuxury(product) {
  const { title, productType, tagsStr } = getProductText(product);
  const head = `${title} ${productType} ${tagsStr}`.toLowerCase();
  if (!head.trim()) return false;
  if (hasAnyWord(head, FOOTWEAR_LUXURY_TERMS)) return true;
  if (/\w*mules?\b/i.test(head)) return true;
  return false;
}

/** Decorative/costume masks (masquerade, wall, feathered mask) → Furniture & Home Accessories, not LUXURY. */
const DECOR_MASK_INDICATORS = ["masquerade", "feathered mask", "decorative mask", "wall mask", "costume mask"];

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

/** Same typography normalization as furniture accessory title overrides (Shopify en dashes, ZWSP, Latin accents). */
function normalizeForVerticalMatch(raw) {
  return (raw || "")
    .trim()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[\u200B-\u200D\uFEFF]/g, "")
    .replace(/[\u2010-\u2015\u2212\uFF0D]/g, "-")
    .toLowerCase();
}

/**
 * Title / type / tags clearly indicate a lighting fixture — always HOME_INTERIOR (Furniture & Home), never LUXURY.
 * Runs before STRONG_LUXURY so mis-tagged "bag"/etc. cannot send a lamp to Luxury.
 */
function titleTypeTagsLookLikeLighting(product) {
  const { title, productType, tagsStr, description } = getProductText(product);
  const descHead = description ? description.slice(0, 1200) : "";
  const t = normalizeForVerticalMatch(`${title} ${productType} ${tagsStr} ${descHead}`);
  if (!t.trim()) return false;

  const jewelryPendantPhrase =
    /\bpendants?\s+(necklace|necklaces|charm|charms)\b/.test(t) ||
    /\b(necklace|necklaces)\s+pendants?\b/.test(t) ||
    /\bpendants?\s+on\s+(a\s+)?(chain|rope|cord)\b/.test(t) ||
    /\blocket\s+pendants?\b/.test(t);

  if (!jewelryPendantPhrase) {
    if (/\bpendants?\s+(light|lights|lamp|lamps|fixture|fixtures|chandelier|chandeliers|sconce)\b/.test(t)) return true;
    if (/\b(light|lights|lamp|lamps|fixture|fixtures|chandelier|chandeliers)\s+pendants?\b/.test(t)) return true;
    if (/\b(ceiling|island|kitchen)\s+pendants?\b/.test(t)) return true;
    if (/\bpendants?\s+lighting\b/.test(t)) return true;
    if (/\bpendants?\s*[-–]\s*\d+\s*[x×]\s*\d+/i.test(t)) return true;
    if (
      /\bpendants?\b/.test(t) &&
      /\b(chandelier|chandeliers|sconce|converted|canopy|hardwired|flush\s+mount|semi-flush|track\s+light|junction|luminaire|electrical)\b/.test(t)
    ) {
      return true;
    }
  }

  if (/\b(table|floor|desk|bedside|torchiere)\s+lamps?\b/.test(t)) return true;
  if (/\b(lampshades?|chandeliers?|sconces?|torchieres?)\b/.test(t)) return true;
  if (/\blamps?\b/.test(t)) return true;
  return false;
}

/** True when name/type/tags/description clearly indicate a lamp or fixture — stops "Oval Ring … Table Lamp" matching jewelry `ring`. */
export function productLooksLikeLightingFixture(product) {
  return titleTypeTagsLookLikeLighting(product);
}

const SYSTEM_PROMPT = `You are a high-precision retail classification engine.
Classify products as either:

LUXURY: wearable designer goods, fine jewelry, watches, prestige fashion brands, high-end collectible personal items (e.g. handbags, clutches, scarves, belts, wallets, designer accessories).

HOME_INTERIOR: furniture, decor, lighting, artwork, home objects, even if expensive or premium.

Important rules:
- Footwear only (sneakers, athletic shoes, boots, sandals, heels, designer collaborations e.g. Asics x Comme des Garçons) is always LUXURY — never HOME_INTERIOR / furniture / decor.
- Physical books, magazines, DVDs/Blu-rays, VHS, laserdiscs, and films/movies/documentaries (including silent films and royal/coronation titles) are HOME_INTERIOR — decor/media inventory — not LUXURY even if collectible or prestigious.
- Decorative home boxes (wood or wooden box, domed box, lidded box, keepsake/storage/decorative box, jewelry/watch box as a tabletop object) are HOME_INTERIOR — not handbags — even if the photo looks rounded like a bag.
- Expensive furniture is still HOME_INTERIOR.
- Gold finish furniture is not LUXURY.
- Designer style furniture is not LUXURY.
- Wearable branded prestige goods are LUXURY.
- A Rolex wall clock is HOME_INTERIOR (decor); a Rolex wristwatch is LUXURY.
- Decorative or costume masks (e.g. masquerade masks, feathered masks) are HOME_INTERIOR (decor/accessories), not LUXURY.
- Table lamps, floor lamps, chandeliers, sconces, ceiling/island/kitchen pendants, pendant lights/lamps/fixtures — including premium or ceramic lamps — are HOME_INTERIOR. Necklace / charm / chain pendants are LUXURY jewelry, not lighting. The word "flat" describing shape (e.g. "flat oval lamp base") is not footwear.
- Mirrored chests, mirrored dressers, door chests, armoires, and similar case goods are HOME_INTERIOR furniture — not handbags or LUXURY bags even if glossy or reflective.
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

/** Books and AV media — always HOME_INTERIOR (Furniture & Home flow), never LUXURY. */
const BOOK_FILM_MEDIA_SUBSTRINGS = [
  "silent film",
  "feature film",
  "short film",
  "motion picture",
  "coffee table book",
  "art book",
  "illustrated book",
  "graphic novel",
  "comic book",
  "picture book",
  "hardcover book",
  "paperback book",
  "box set",
  "boxed set",
  "blu-ray",
  "blu ray",
  "4k ultra hd",
];

const BOOK_FILM_MEDIA_WORDS = [
  "dvd",
  "vhs",
  "laserdisc",
  "documentary",
  "screenplay",
  "hardcover",
  "paperback",
  "encyclopedia",
  "memoir",
  "biography",
  "anthology",
  "trilogy",
  "saga",
  "isbn",
  "novel",
  "novella",
  "audiobook",
  "book",
  "books",
];

/**
 * @param {object} product - Shopify product
 * @returns {boolean}
 */
export function productLooksLikeBookFilmOrMedia(product) {
  const { title, productType, tagsStr, description } = getProductText(product);
  const blob = normalizeForVerticalMatch(`${title} ${productType} ${tagsStr} ${description}`).toLowerCase();
  if (!blob.trim()) return false;
  // Wearable jewelry copy is never book/film inventory (e.g. "multi-media opera necklace", pendant charms).
  if (
    /\bnecklaces?\b/.test(blob) ||
    /\bbracelets?\b/.test(blob) ||
    /\bearrings?\b/.test(blob) ||
    /\bchokers?\b/.test(blob) ||
    /\banklets?\b/.test(blob) ||
    /\blockets?\b/.test(blob) ||
    /\bbrooches?\b/.test(blob) ||
    /\bcharms?\b/.test(blob)
  ) {
    return false;
  }
  for (const s of BOOK_FILM_MEDIA_SUBSTRINGS) {
    if (s && blob.includes(s)) return true;
  }
  return BOOK_FILM_MEDIA_WORDS.some((w) => hasAnyWord(blob, [w]));
}

/** Lidded / decorative boxes sold as home decor — HOME_INTERIOR, never mistaken for handbags via vision/tags. */
const FURNITURE_HOME_BOX_SUBSTRINGS = [
  "domed box",
  "wood box",
  "wooden box",
  "box w/ lid",
  "box w/lid",
  "box with lid",
  "lidded box",
  "keepsake box",
  "decorative box",
  "storage box",
  "trinket box",
  "jewelry box",
  "watch box",
  "cigar box",
  "tea box",
  "music box",
];

/**
 * @param {object} product - Shopify product
 * @returns {boolean}
 */
export function productLooksLikeFurnitureHomeBox(product) {
  const { title, productType, tagsStr, description } = getProductText(product);
  const blob = normalizeForVerticalMatch(`${title} ${productType} ${tagsStr} ${description}`).toLowerCase();
  if (!blob.trim()) return false;
  for (const s of FURNITURE_HOME_BOX_SUBSTRINGS) {
    if (s && blob.includes(s)) return true;
  }
  if (/\bwood(?:en)?\s+box\b/i.test(blob)) return true;
  if (/\bdomed\b/i.test(blob) && /\bbox\b/i.test(blob)) return true;
  return false;
}

/** Dressers, mirrored chests, door chests — case goods; vision often mistakes mirrored furniture for handbags. */
const CASE_GOODS_SUBSTRINGS = [
  "mirrored chest",
  "mirror chest",
  "door chest",
  "2 door chest",
  "two door chest",
  "3 door chest",
  "three door chest",
  "4 door chest",
  "four door chest",
  "chest of drawers",
  "gentleman's chest",
  "gentlemans chest",
  "bachelor's chest",
  "bachelors chest",
  "lingerie chest",
  "double dresser",
  "triple dresser",
  "highboy",
  "lowboy",
];

/**
 * @param {object} product - Shopify product
 * @returns {boolean}
 */
export function productLooksLikeFurnitureCaseGoods(product) {
  if (productLooksLikeFootwearLuxury(product)) return false;
  const { title, productType, tagsStr, description } = getProductText(product);
  const descHead = description ? description.slice(0, 2000) : "";
  const blob = normalizeForVerticalMatch(`${title} ${productType} ${tagsStr} ${descHead}`).toLowerCase();
  if (!blob.trim()) return false;
  for (const s of CASE_GOODS_SUBSTRINGS) {
    if (s && blob.includes(s)) return true;
  }
  if (/\bmirror(?:ed)?\s+(chest|dresser|armoire|wardrobe|cabinet|nightstand|buffet|sideboard|credenza)s?\b/i.test(blob)) return true;
  if (/\b(chest|dresser|armoire)s?\s+with\s+mirrors?\b/i.test(blob)) return true;
  if (/\b\d[\s-]*door\b/i.test(blob) && /\bchest\b/i.test(blob)) return true;
  if (/\bchest\b/i.test(blob) && /\b(drawers?|doors?)\b/i.test(blob)) return true;
  if (/\bhooker\b/i.test(blob) && /\bchest\b/i.test(blob)) return true;
  return false;
}

function getProductImageUrls(product, max = 4) {
  const imgs = product?.images;
  if (!Array.isArray(imgs)) return [];
  return imgs.map((i) => (i && i.src ? String(i.src).trim() : "")).filter(Boolean).slice(0, max);
}

function luxuryBrandInTitleOrVendor(product) {
  const { title, vendor } = getProductText(product);
  const blob = normalizeForVerticalMatch(`${title} ${vendor}`);
  if (!blob) return false;
  return LUXURY_HOUSE_SIGNALS.some((brand) => {
    const b = normalizeForVerticalMatch(brand);
    return b && blob.includes(b);
  });
}

/** Footwear / bag shapes in copy — vision fallback when text model wrongly said HOME_INTERIOR. */
const VISION_FALLBACK_LEXICAL = [
  "boot", "boots", "mule", "mules", "satchel", "sneaker", "sneakers", "loafer", "loafers",
  "sandal", "sandals", "heel", "heels", "pump", "pumps", "oxford", "oxfords", "chelsea",
  "stiletto", "stilettos", "espadrille", "espadrilles", "footwear", "wedge", "wedges",
  "handbag", "crossbody", "clutch", "tote", "wallet", "backpack",
];

function shouldRunVisionVerticalFallback(product, textResult) {
  if (productLooksLikeBookFilmOrMedia(product)) return false;
  if (productLooksLikeFurnitureHomeBox(product)) return false;
  if (productLooksLikeFurnitureCaseGoods(product)) return false;
  if (productLooksLikeLightingFixture(product)) return false;
  if (textResult?.category !== "HOME_INTERIOR") return false;
  if (process.env.LLM_VERTICAL_VISION_FALLBACK === "0" || process.env.LLM_VERTICAL_VISION_FALLBACK === "false") {
    return false;
  }
  if (getProductImageUrls(product).length === 0) return false;

  const { title, productType, tagsStr, description } = getProductText(product);
  const head = `${title} ${productType} ${tagsStr}`.toLowerCase();
  if (hasAnyWord(head, STRONG_LUXURY_SIGNALS)) return true;
  if (luxuryBrandInTitleOrVendor(product)) return true;

  const descBlob = `${title} ${description}`.toLowerCase();
  if (hasAnyWord(descBlob, VISION_FALLBACK_LEXICAL)) return true;

  const conf = typeof textResult.confidence === "number" ? textResult.confidence : 0;
  if (conf > 0 && conf < 0.55) return true;

  return false;
}

/**
 * Vision-only vertical check (GPT-4o multimodal). Used when text classifier likely wrong for luxury footwear/bags.
 */
async function classifyVerticalWithVision(product, openaiKey, logPayload = {}, logFn = null) {
  const urls = getProductImageUrls(product, 4);
  if (!urls.length) return null;

  const { title, productType, vendor } = getProductText(product);
  const system = `You see product photo(s) from a consignment store with two websites:
- LUXURY: designer shoes, boots, mules, heels, handbags, totes, satchels, clutches, wallets, jewelry, belts, scarves, sunglasses, small leather goods worn/carried.
- HOME_INTERIOR: furniture, sofas, tables, dressers, mirrored chests, door chests, armoires, nightstands, lamps, chandeliers, rugs, mirrors as furniture, wall art as decor objects, home decor.

Rules: Footwear and handbags are ALWAYS LUXURY even if shiny or sculptural. A red carpet stiletto is LUXURY. A table lamp or dining chair is HOME_INTERIOR. A mirrored bedroom chest or dresser is HOME_INTERIOR — not a handbag even if reflective.
Reply with JSON only: {"category":"LUXURY" or "HOME_INTERIOR","confidence":0-1,"reasoning":"brief"}`;

  const userText = `Title (may be wrong): ${title || "(none)"}\nType: ${productType || "(none)"}\nVendor: ${vendor || "(none)"}\nClassify from the image(s).`;

  const userContent = [{ type: "text", text: userText }];
  for (const url of urls) {
    userContent.push({ type: "image_url", image_url: { url, detail: "low" } });
  }

  try {
    const res = await axios.post(
      OPENAI_API_URL,
      {
        model: (process.env.OPENAI_VERTICAL_VISION_MODEL || "gpt-4o").trim() || "gpt-4o",
        messages: [
          { role: "system", content: system },
          { role: "user", content: userContent },
        ],
        temperature: 0,
        max_tokens: 200,
      },
      {
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${openaiKey}`,
        },
        timeout: 45000,
      }
    );
    const raw = res.data?.choices?.[0]?.message?.content;
    if (logPayload) {
      logPayload.visionRaw = raw;
    }
    const parsed = parseModelOutput(raw);
    if (logPayload) logPayload.visionParsed = parsed;
    if (!parsed) return null;
    if (logFn) {
      logFn("info", {
        event: "llm_vertical.vision_fallback",
        category: parsed.category,
        confidence: parsed.confidence,
      });
    }
    return parsed;
  } catch (err) {
    const message = err.response?.data?.error?.message || err.message || String(err);
    if (logPayload) logPayload.visionError = message;
    if (logFn) logFn("warn", { event: "llm_vertical.vision_error", message });
    return null;
  }
}

/**
 * Safety: if product has strong furniture indicators and is NOT clearly wearable/jewelry, force HOME_INTERIOR.
 * Uses whole-word matching for furniture words — substring checks caused false positives ("table" in "vegetable",
 * "mirror" in "mirrored leather", etc.) and misrouted satchels/boots/mules.
 * Strong-luxury / wearable checks use title+type+tags only so description phrases like "dust bag included"
 * on furniture do not block legitimate downgrades.
 */
function applyFurnitureSafetyOverride(product, category) {
  if (category !== "LUXURY") return category;
  const { title, productType, tagsStr, description } = getProductText(product);
  const nameAndTypeAndTags = `${title} ${productType} ${tagsStr}`.toLowerCase();
  const combined = `${nameAndTypeAndTags} ${description}`.toLowerCase();

  if (hasAnyWord(nameAndTypeAndTags, STRONG_LUXURY_SIGNALS)) return category;
  if (hasAnyWord(nameAndTypeAndTags, WEARABLE_INDICATORS)) return category;

  const hasFurniture = hasAnyWord(combined, FURNITURE_INDICATORS);
  if (!hasFurniture) return category;
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

  const { title, productType, tagsStr } = getProductText(product);
  const nameAndTypeAndTags = `${title} ${productType} ${tagsStr}`.toLowerCase();

  // Decorative/costume masks (masquerade, feathered mask, etc.) → Furniture & Home Accessories
  if (DECOR_MASK_INDICATORS.some((term) => nameAndTypeAndTags.includes(term))) {
    const decorMaskResult = {
      category: "HOME_INTERIOR",
      confidence: 1,
      reasoning: "Decorative or costume mask (e.g. masquerade); Furniture & Home Accessories.",
    };
    logPayload.raw = null;
    logPayload.parsed = null;
    logPayload.final = decorMaskResult;
    logPayload.override = "decor_mask";
    if (logFn) logFn("info", { event: "llm_vertical.decor_mask", category: "HOME_INTERIOR", reason: "masquerade/decor mask" });
    return decorMaskResult;
  }

  if (titleTypeTagsLookLikeLighting(product)) {
    const lightingResult = {
      category: "HOME_INTERIOR",
      confidence: 1,
      reasoning: "Lighting fixture in title, product type, or tags (lamp/chandelier/sconce/etc.); Furniture & Home.",
    };
    logPayload.raw = null;
    logPayload.parsed = null;
    logPayload.final = lightingResult;
    logPayload.override = "lighting_title_type_tags";
    if (logFn) logFn("info", { event: "llm_vertical.lighting", category: "HOME_INTERIOR", reason: "lamp/lighting in name or type" });
    return lightingResult;
  }

  if (productLooksLikeBookFilmOrMedia(product)) {
    const mediaResult = {
      category: "HOME_INTERIOR",
      confidence: 1,
      reasoning: "Book, film, or physical media (DVD/VHS/etc.); Furniture & Home inventory, not wearable luxury.",
    };
    logPayload.raw = null;
    logPayload.parsed = null;
    logPayload.final = mediaResult;
    logPayload.override = "book_film_media";
    if (logFn) logFn("info", { event: "llm_vertical.book_film_media", category: "HOME_INTERIOR" });
    return mediaResult;
  }

  if (productLooksLikeFootwearLuxury(product)) {
    const footwearResult = {
      category: "LUXURY",
      confidence: 1,
      reasoning: "Footwear (shoes, boots, mules, sandals); always Luxury Goods, never Furniture & Home.",
    };
    logPayload.raw = null;
    logPayload.parsed = null;
    logPayload.final = footwearResult;
    logPayload.override = "footwear_always_luxury";
    if (logFn) logFn("info", { event: "llm_vertical.footwear", category: "LUXURY" });
    return footwearResult;
  }

  if (productLooksLikeFurnitureHomeBox(product)) {
    const boxResult = {
      category: "HOME_INTERIOR",
      confidence: 1,
      reasoning: "Decorative or lidded home box (wood/dom/storage/etc.); Furniture & Home, not a handbag.",
    };
    logPayload.raw = null;
    logPayload.parsed = null;
    logPayload.final = boxResult;
    logPayload.override = "furniture_home_box";
    if (logFn) logFn("info", { event: "llm_vertical.furniture_home_box", category: "HOME_INTERIOR" });
    return boxResult;
  }

  if (productLooksLikeFurnitureCaseGoods(product)) {
    const caseResult = {
      category: "HOME_INTERIOR",
      confidence: 1,
      reasoning: "Case goods (e.g. mirrored chest, door chest, dresser); Furniture & Home, not Luxury handbags.",
    };
    logPayload.raw = null;
    logPayload.parsed = null;
    logPayload.final = caseResult;
    logPayload.override = "furniture_case_goods";
    if (logFn) logFn("info", { event: "llm_vertical.furniture_case_goods", category: "HOME_INTERIOR" });
    return caseResult;
  }

  // Strong wearable/footwear in title/type/tags → LUXURY unless title/description clearly furniture (e.g. lamp + stray "bag" tag)
  if (hasAnyWord(nameAndTypeAndTags, STRONG_LUXURY_SIGNALS)) {
    const afterFurnitureSafety = applyFurnitureSafetyOverride(product, "LUXURY");
    if (afterFurnitureSafety === "HOME_INTERIOR") {
      const overrideResult = {
        category: "HOME_INTERIOR",
        confidence: 1,
        reasoning:
          "Wearable keyword in type/tags but title/description indicate furniture/home (e.g. table lamp); Furniture & Home.",
      };
      logPayload.raw = null;
      logPayload.parsed = null;
      logPayload.final = overrideResult;
      logPayload.override = "furniture_safety_over_strong_luxury";
      if (logFn)
        logFn("info", {
          event: "llm_vertical.strong_luxury_overridden",
          category: "HOME_INTERIOR",
          reason: "furniture cue in title/description",
        });
      return overrideResult;
    }
    const strongLuxuryResult = {
      category: "LUXURY",
      confidence: 1,
      reasoning: "Strong wearable/footwear signal in title, type, or tags (e.g. backpack, boots, handbag).",
    };
    logPayload.raw = null;
    logPayload.parsed = null;
    logPayload.final = strongLuxuryResult;
    logPayload.override = "strong_luxury_signal";
    if (logFn) logFn("info", { event: "llm_vertical.strong_luxury", category: "LUXURY", reason: "wearable/footwear in name or type" });
    return strongLuxuryResult;
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

    if (category === "HOME_INTERIOR" && productLooksLikeFootwearLuxury(product)) {
      category = "LUXURY";
      override = override ? `${override}+footwear_luxury_rescue` : "footwear_luxury_rescue";
    }

    let final = {
      category,
      confidence: parsed.confidence,
      reasoning: parsed.reasoning,
    };
    logPayload.final = final;
    if (override) logPayload.override = override;

    if (shouldRunVisionVerticalFallback(product, final)) {
      const vision = await classifyVerticalWithVision(product, openaiKey, logPayload, logFn);
      if (vision && vision.category === "LUXURY" && vision.confidence >= 0.5) {
        final = {
          category: "LUXURY",
          confidence: vision.confidence,
          reasoning: `[vision] ${vision.reasoning || ""}`.trim(),
        };
        logPayload.final = final;
        const prev = logPayload.override;
        logPayload.override = prev ? `${prev}+vision_luxury` : "vision_luxury";
        if (logFn) {
          logFn("info", {
            event: "llm_vertical.classified",
            category: final.category,
            confidence: final.confidence,
            override: logPayload.override,
            reasoning: final.reasoning?.slice(0, 150),
          });
        }
        return final;
      }
    }

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
