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

/** Strong bag/accessory/footwear words in title → luxury first (so any bag/shoe in name never gets furniture). */
const TITLE_LUXURY_WORDS = [
  "backpack", "backpacks", "handbag", "handbags", "bag", "bags",
  "tote bag", "tote bags", "sling bag", "sling bags", "envelope bag",
  "shoulder bag", "shoulder bags", "shoulder handbag", "shoulder handbags",
  "messenger bag", "messenger bags", "hobo bag", "hobo bags",
  "clutch", "tote", "totes", "crossbody", "wallet", "wallets", "luggage", "satchel", "briefcase",
  "agenda", "agenda cover", "belt", "belts", "scarf", "scarves",
  "ballet flats", "flats", "heels", "pumps", "sneakers", "loafers", "boots", "sandals", "mules", "slides", "shoes",
];

/**
 * Detect vertical from Shopify product.
 * @param {Object} product - Shopify product (title, vendor, tags, product_type, body_html)
 * @returns {'luxury'|'furniture'}
 */
export function detectVertical(product) {
  const combined = getCombinedProductText(product);
  const title = (product.title || "").toLowerCase();
  const typeAndTags = [product.product_type || "", getTagsArray(product).join(" ")].join(" ").toLowerCase();

  // 0) Title OR product type/tags clearly indicate bag/accessory → luxury first (bags/shoes never furniture)
  if (TITLE_LUXURY_WORDS.some((w) => matchWordBoundary(title, w))) {
    return "luxury";
  }
  if (TITLE_LUXURY_WORDS.some((w) => matchWordBoundary(typeAndTags, w))) {
    return "luxury";
  }

  // 1) Strong furniture keyword in name/description (word-boundary) → furniture (so "McCarty Pottery", "canvas art" never go to luxury)
  const furnitureCategories = Object.values(CATEGORY_KEYWORDS_FURNITURE).flat();
  const hasFurnitureKeyword = furnitureCategories.some((kw) => matchWordBoundary(combined, kw));
  if (hasFurnitureKeyword) return "furniture";

  // 2) Known luxury brand in vendor or title → luxury (so Gucci/Hermes etc. never go to furniture)
  if (detectBrandFromProduct(product.title, product.vendor)) {
    return "luxury";
  }

  const tagsStr = getTagsArray(product).join(" ");
  // 3) Explicit type/tag: furniture wins
  if (textMatchesAny(product.product_type, FURNITURE_SIGNALS) || textMatchesAny(tagsStr, FURNITURE_SIGNALS)) {
    return "furniture";
  }
  if (textMatchesAny(product.product_type, LUXURY_SIGNALS) || textMatchesAny(tagsStr, LUXURY_SIGNALS)) {
    return "luxury";
  }

  // 4) Luxury categories (word-boundary so "bag" doesn't match inside other words)
  const luxuryCategories = Object.values(CATEGORY_KEYWORDS).flat();
  const hasLuxuryKeyword = luxuryCategories.some((kw) => matchWordBoundary(combined, kw));

  if (hasLuxuryKeyword) return "luxury";

  // 5) Default: luxury (existing behavior)
  return "luxury";
}
