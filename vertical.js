/**
 * Vertical detection: Luxury / Accessories vs Furniture & Home
 * Uses Shopify product data (title, vendor, tags, product_type) to classify
 * before Webflow matching, SOLD logic, or CMS routing.
 */

import { detectBrandFromProduct } from "./brand.js";
import { CATEGORY_KEYWORDS } from "./categoryKeywords.js";
import { CATEGORY_KEYWORDS_FURNITURE } from "./categoryKeywordsFurniture.js";

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
  "pottery",
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

function getCombinedProductText(product) {
  const tagsStr = getTagsArray(product).join(" ");
  const descriptionText = stripHtml(product.body_html || "");
  const parts = [
    product.title || "",
    product.product_type || "",
    product.vendor || "",
    tagsStr,
    descriptionText,
  ];
  return parts.join(" ").toLowerCase();
}

/** Strong bag/accessory words in title → luxury even if tags say furniture. */
const TITLE_LUXURY_WORDS = ["backpack", "backpacks", "handbag", "handbags", "bag", "bags", "clutch", "tote", "totes", "crossbody", "wallet", "wallets", "luggage", "satchel", "briefcase", "messenger bag", "shoulder bag", "agenda", "agenda cover", "belt", "belts", "scarf", "scarves"];

/**
 * Detect vertical from Shopify product.
 * @param {Object} product - Shopify product (title, vendor, tags, product_type, body_html)
 * @returns {'luxury'|'furniture'}
 */
export function detectVertical(product) {
  const combined = getCombinedProductText(product);
  const title = (product.title || "").toLowerCase();

  // 0) Known luxury brand in vendor or title → luxury (so Gucci/Hermes etc. never go to furniture)
  if (detectBrandFromProduct(product.title, product.vendor)) {
    return "luxury";
  }

  // 1) Title clearly indicates bag/accessory → luxury (overrides furniture tags like "art")
  if (TITLE_LUXURY_WORDS.some((w) => matchWordBoundary(title, w))) {
    return "luxury";
  }

  const tagsStr = getTagsArray(product).join(" ");
  // 1) Explicit type/tag: furniture wins
  if (textMatchesAny(product.product_type, FURNITURE_SIGNALS) || textMatchesAny(tagsStr, FURNITURE_SIGNALS)) {
    return "furniture";
  }
  if (textMatchesAny(product.product_type, LUXURY_SIGNALS) || textMatchesAny(tagsStr, LUXURY_SIGNALS)) {
    return "luxury";
  }

  // 2) Name + description (and tags/type/vendor in combined): check furniture keywords first (word-boundary so "art" doesn't match "smart")
  const furnitureCategories = Object.values(CATEGORY_KEYWORDS_FURNITURE).flat();
  const hasFurnitureKeyword = furnitureCategories.some((kw) => matchWordBoundary(combined, kw));

  if (hasFurnitureKeyword) return "furniture";

  // 3) Luxury categories (word-boundary so "bag" doesn't match inside other words)
  const luxuryCategories = Object.values(CATEGORY_KEYWORDS).flat();
  const hasLuxuryKeyword = luxuryCategories.some((kw) => matchWordBoundary(combined, kw));

  if (hasLuxuryKeyword) return "luxury";

  // 4) Default: luxury (existing behavior)
  return "luxury";
}
