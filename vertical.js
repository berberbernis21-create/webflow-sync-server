/**
 * Vertical detection: Luxury / Accessories vs Furniture & Home
 * Uses Shopify product data (title, vendor, tags, product_type) to classify
 * before Webflow matching, SOLD logic, or CMS routing.
 */

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

function getTagsArray(product) {
  const t = product.tags;
  if (Array.isArray(t)) return t;
  if (typeof t === "string") return t.split(",").map((s) => s.trim()).filter(Boolean);
  return [];
}

function getCombinedProductText(product) {
  const tagsStr = getTagsArray(product).join(" ");
  const parts = [
    product.title || "",
    product.product_type || "",
    product.vendor || "",
    tagsStr,
  ];
  return parts.join(" ").toLowerCase();
}

/**
 * Detect vertical from Shopify product.
 * @param {Object} product - Shopify product (title, vendor, tags, product_type)
 * @returns {'luxury'|'furniture'}
 */
export function detectVertical(product) {
  const combined = getCombinedProductText(product);
  const title = product.title || "";

  const tagsStr = getTagsArray(product).join(" ");
  // 1) Explicit type/tag: furniture wins
  if (textMatchesAny(product.product_type, FURNITURE_SIGNALS) || textMatchesAny(tagsStr, FURNITURE_SIGNALS)) {
    return "furniture";
  }
  if (textMatchesAny(product.product_type, LUXURY_SIGNALS) || textMatchesAny(tagsStr, LUXURY_SIGNALS)) {
    return "luxury";
  }

  // 2) Title/keyword: check furniture categories first
  const furnitureCategories = Object.values(CATEGORY_KEYWORDS_FURNITURE).flat();
  const hasFurnitureKeyword = furnitureCategories.some((kw) => combined.includes(normalize(kw)));

  if (hasFurnitureKeyword) return "furniture";

  // 3) Luxury categories
  const luxuryCategories = Object.values(CATEGORY_KEYWORDS).flat();
  const hasLuxuryKeyword = luxuryCategories.some((kw) => combined.includes(normalize(kw)));

  if (hasLuxuryKeyword) return "luxury";

  // 4) Default: luxury (existing behavior)
  return "luxury";
}
