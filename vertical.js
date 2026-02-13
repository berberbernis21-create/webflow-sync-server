/**
 * Vertical detection: Luxury / Accessories vs Furniture & Home
 * Uses Shopify product data (title, vendor, tags, product_type) to classify
 * before Webflow matching, SOLD logic, or CMS routing.
 * Name-first: prioritize title (+ type, tags); use description only when needed.
 * Default to furniture when uncertain.
 */

import { detectBrandFromProduct } from "./brand.js";
import { CATEGORY_KEYWORDS } from "./categoryKeywords.js";
import { CATEGORY_KEYWORDS_FURNITURE } from "./categoryKeywordsFurniture.js";

/** Furniture items that contain words like "luggage" or "rack" — check these FIRST so they don't match luxury. */
const FURNITURE_TRAP_PHRASES = [
  "luggage rack", "coat rack", "hat rack", "umbrella stand", "hall tree",
  "towel rack", "magazine rack", "wine rack", "plant stand", "lamp stand",
];

const FURNITURE_SIGNALS = [
  "furniture",
  "furniture & home",
  "furniture and home",
  "home",
  "home decor",
  "living room",
  "dining room",
  "bedroom",
  "office",
  "outdoor",
  "patio",
  "rug",
  "lighting",
  "mirror",
  "art",
  "painting",
  "pottery",
  "sculpture",
  "carved",
  "statue",
  "antique",
  "wood",
  "rack",
  "stands",
  "pedestal",
  "drawers",
  "sofa",
  "chair",
  "table",
  "desk",
  "cabinet",
  "dresser",
  "lamp",
];

const LUXURY_SIGNALS = [
  "luxury",
  "handbag",
  "bag",
  "accessories",
  "wallet",
  "scarf",
  "belt",
  "tote",
  "crossbody",
  "backpack",
  "luggage",
  "leather",
  "designer",
];

function normalize(str = "") {
  return String(str)
    .toLowerCase()
    .trim();
}

function textMatchesAny(text, list) {
  if (!text) return false;
  const n = normalize(text);
  return list.some((signal) => n.includes(normalize(signal)));
}

/** Match keyword as whole word so "art" doesn't match "smart", "desk" doesn't match "desktop". */
function matchWordBoundary(text, keyword) {
  const k = keyword.trim().toLowerCase();
  if (!k) return false;
  const escaped = k.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  try {
    return new RegExp("\\b" + escaped + "\\b", "i").test(text);
  } catch {
    return text.includes(k);
  }
}

function getTagsArray(product) {
  const t = product.tags;
  if (Array.isArray(t)) return t;
  if (typeof t === "string") return t.split(",").map((s) => s.trim()).filter(Boolean);
  return [];
}

/** Strip HTML tags for use in text-based matching. */
function stripHtml(html) {
  if (!html || typeof html !== "string") return "";
  return html.replace(/<[^>]*>/g, " ").replace(/\s+/g, " ").trim();
}

/** Name + type + tags (no description) — use first for furniture so we don't rely on description. */
function getNameAndTags(product) {
  const tagsStr = getTagsArray(product).join(" ");
  const parts = [
    product.title || "",
    product.product_type || "",
    product.vendor || "",
    tagsStr,
  ];
  return parts.join(" ").toLowerCase();
}

/** Full combined: name + type + tags + description — used when name-only is unclear. */
function getCombinedProductText(product) {
  const nameAndTags = getNameAndTags(product);
  const descriptionText = stripHtml(product.body_html || "");
  return [nameAndTags, descriptionText].filter(Boolean).join(" ").toLowerCase();
}

/** Strong bag/accessory/footwear words in title → luxury (handbag, belt, shoe, etc. are very easy to detect). */
const TITLE_LUXURY_WORDS = [
  "backpack", "backpacks", "handbag", "handbags", "bag", "bags",
  "tote bag", "tote bags", "sling bag", "sling bags", "envelope bag",
  "shoulder bag", "shoulder bags", "shoulder handbag", "shoulder handbags",
  "messenger bag", "messenger bags", "hobo bag", "hobo bags",
  "clutch", "tote", "totes", "crossbody", "wallet", "wallets", "luggage", "satchel", "briefcase",
  "agenda", "agenda cover", "belt", "belts", "scarf", "scarves",
  "purse", "purses", "card holder", "key pouch", "woc", "wallet on chain",
  "ballet flats", "flats", "heels", "pumps", "sneakers", "loafers", "boots", "sandals", "mules", "slides", "shoes",
];

/** Substring signals for luxury — avoid phrases that are furniture (e.g. luggage rack). */
const BAG_SUBSTRINGS = [
  " bag", "bag ", "handbag", "handbags", " tote", "tote ", "tote with", "shopping tote",
  " clutch", " wallet", " wallet on chain", "wallets", " hobo", "hobo bag",
  " shoulder bag", " sling bag", "backpack", "backpacks", "satchel", "crossbody", "briefcase",
  "drawstring", "double flap", "flap bag", " purse", "purse ",
];

export function detectVertical(product) {
  const nameAndTags = getNameAndTags(product);
  const combined = getCombinedProductText(product);
  const title = (product.title || "").toLowerCase();
  const typeAndTags = [product.product_type || "", getTagsArray(product).join(" ")].join(" ").toLowerCase();

  // 0) Furniture trap: luggage rack, coat rack, umbrella stand, etc. — these contain "luggage" but are furniture
  const textForTrap = `${title} ${typeAndTags}`;
  for (const phrase of FURNITURE_TRAP_PHRASES) {
    if (textForTrap.includes(phrase.toLowerCase())) return "furniture";
  }

  // 1) Strong luxury signals in title/type/tags — handbag, belt, wallet, shoe, etc. are very easy to detect
  for (const s of BAG_SUBSTRINGS) {
    if (title.includes(s) || typeAndTags.includes(s)) return "luxury";
  }
  if (TITLE_LUXURY_WORDS.some((w) => matchWordBoundary(title, w))) return "luxury";
  if (TITLE_LUXURY_WORDS.some((w) => matchWordBoundary(typeAndTags, w))) return "luxury";

  // 2) Furniture keywords — name first (title + type + tags), then description if needed
  const furnitureCategories = Object.values(CATEGORY_KEYWORDS_FURNITURE).flat();
  const hasFurnitureInName = furnitureCategories.some((kw) => matchWordBoundary(nameAndTags, kw));
  if (hasFurnitureInName) return "furniture";
  // If name didn't match, use description when it exists
  const descriptionText = stripHtml(product.body_html || "");
  if (descriptionText) {
    const hasFurnitureInCombined = furnitureCategories.some((kw) => matchWordBoundary(combined, kw));
    if (hasFurnitureInCombined) return "furniture";
  }

  const tagsStr = getTagsArray(product).join(" ");
  // 3) Type/tag/NAME signals: furniture vs luxury — check BEFORE luxury brand so product semantics win
  // (e.g. "Wood Pedestal" in title = furniture even if brand is in luxury list)
  const nameTypeTags = nameAndTags;
  if (textMatchesAny(product.product_type, FURNITURE_SIGNALS) || textMatchesAny(tagsStr, FURNITURE_SIGNALS) || textMatchesAny(nameTypeTags, FURNITURE_SIGNALS)) {
    return "furniture";
  }
  if (textMatchesAny(product.product_type, LUXURY_SIGNALS) || textMatchesAny(tagsStr, LUXURY_SIGNALS)) {
    return "luxury";
  }

  // 4) Known luxury brand in vendor or title → luxury (after furniture semantics)
  if (detectBrandFromProduct(product.title, product.vendor)) return "luxury";

  // 5) Luxury categories in full combined (word-boundary)
  const luxuryCategories = Object.values(CATEGORY_KEYWORDS).flat();
  const hasLuxuryKeyword = luxuryCategories.some((kw) => matchWordBoundary(combined, kw));
  if (hasLuxuryKeyword) return "luxury";

  // 6) Default: furniture when uncertain (decor, antiques, vague items should not land in luxury)
  return "furniture";
}
