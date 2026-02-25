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
  "shoe rack", "shoe shelf", "shoe cabinet", "shoe organizer", "shoe storage",
  "jewelry box", "jewelry armoire", "jewelry cabinet", "jewelry display", "jewelry organizer",
  "jewelry stand", "jewelry holder", "jewelry tray", "jewelry chest", "jewelry case",
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

/** Any mention of shoes or jewelry in title/name → luxury, unless description says it's furniture (box, rack, etc.). */
const SHOE_JEWELRY_TITLE_WORDS = [
  "shoe", "shoes", "footwear", "sneaker", "sneakers", "heel", "heels", "boot", "boots",
  "sandal", "sandals", "loafer", "loafers", "pump", "pumps", "mule", "mules", "slide", "slides", "flat", "flats",
  "jewelry", "jewellery", "jewel", "earring", "earrings", "bracelet", "bracelets",
  "necklace", "necklaces", "ring", "rings", "pendant", "pendants", "brooch", "barrette", "barrettes",
];

/** Description phrases that mean "this product is a furniture item" (e.g. jewelry box, shoe rack). */
const DESCRIPTION_FURNITURE_PHRASES = [
  "jewelry box", "jewelry armoire", "jewelry cabinet", "jewelry organizer", "jewelry display",
  "jewelry stand", "jewelry holder", "jewelry tray", "jewelry chest", "jewelry case",
  "shoe rack", "shoe shelf", "shoe cabinet", "shoe organizer", "shoe storage", "shoe stand",
  "display case", "display cabinet", "storage cabinet", "storage box",
  "keeps your jewelry", "store your jewelry", "display your jewelry", "for storing jewelry",
  "holds your jewelry", "organize your jewelry", "keeps your shoes", "store your shoes",
  "entrance bench", "hall tree",
];

/** Strong bag/accessory/footwear/jewelry words in title → luxury (handbag, belt, shoe, earring, etc. are very easy to detect). */
const TITLE_LUXURY_WORDS = [
  "backpack", "backpacks", "handbag", "handbags", "bag", "bags",
  "tote bag", "tote bags", "sling bag", "sling bags", "envelope bag",
  "shoulder bag", "shoulder bags", "shoulder handbag", "shoulder handbags",
  "messenger bag", "messenger bags", "hobo bag", "hobo bags",
  "clutch", "tote", "totes", "crossbody", "wallet", "wallets", "luggage", "satchel", "briefcase",
  "agenda", "agenda cover", "belt", "belts", "scarf", "scarves",
  "purse", "purses", "card holder", "key pouch", "woc", "wallet on chain",
  "ballet flats", "flats", "heels", "pumps", "sneakers", "loafers", "boots", "sandals", "mules", "slides", "shoes",
  "jewelry", "earring", "earrings", "bracelet", "bracelets", "necklace", "necklaces", "pouch", "barrette", "barrettes",
];

/** Substring signals for luxury — avoid phrases that are furniture (e.g. luggage rack). */
const BAG_SUBSTRINGS = [
  " bag", "bag ", "handbag", "handbags", " tote", "tote ", "tote with", "shopping tote",
  " clutch", " wallet", " wallet on chain", "wallets", " hobo", "hobo bag",
  " shoulder bag", " sling bag", "backpack", "backpacks", "satchel", "crossbody", "briefcase",
  "drawstring", "double flap", "flap bag", " purse", "purse ",
];

/** True when title/type/tags clearly indicate a bag, shoe, or jewelry (so we don't send canvas totes to Furniture due to "canvas"). */
export function hasStrongLuxurySignalsInTitle(product) {
  const title = (product.title || "").toLowerCase();
  const typeAndTags = [product.product_type || "", getTagsArray(product).join(" ")].join(" ").toLowerCase();
  const nameForCheck = `${title} ${typeAndTags}`;
  if (BAG_SUBSTRINGS.some((s) => title.includes(s) || typeAndTags.includes(s))) return true;
  if (TITLE_LUXURY_WORDS.some((w) => matchWordBoundary(nameForCheck, w))) return true;
  if (SHOE_JEWELRY_TITLE_WORDS.some((w) => matchWordBoundary(nameForCheck, w))) return true;
  return false;
}

export function detectVertical(product) {
  const nameAndTags = getNameAndTags(product);
  const combined = getCombinedProductText(product);
  const title = (product.title || "").toLowerCase();
  const typeAndTags = [product.product_type || "", getTagsArray(product).join(" ")].join(" ").toLowerCase();
  const descriptionText = stripHtml(product.body_html || "").toLowerCase();

  // 0a) Pillow and painting always → furniture (before any luxury checks; "canvas tote bag" has no pillow/painting so stays luxury in step 1)
  const alwaysFurnitureWords = ["pillow", "pillows", "painting", "paintings"];
  if (alwaysFurnitureWords.some((w) => matchWordBoundary(nameAndTags, w))) return "furniture";

  // 0a2) Art-on-canvas phrases → furniture ("plant painting on canvas", "acrylic painting on canvas"; "canvas tote bag" has none of these)
  const nameAndTagsLower = nameAndTags.toLowerCase();
  const artOnCanvasPhrases = ["painting on canvas", "acrylic on canvas", "acrylic painting", "paintings on canvas", "on canvas by", "oil on canvas", "plant painting"];
  if (artOnCanvasPhrases.some((phrase) => nameAndTagsLower.includes(phrase))) return "furniture";

  // 0) Furniture trap: luggage rack, coat rack, jewelry box, shoe rack, etc. — check FIRST so they don't match luxury
  const textForTrap = `${title} ${typeAndTags}`;
  for (const phrase of FURNITURE_TRAP_PHRASES) {
    if (textForTrap.includes(phrase.toLowerCase())) return "furniture";
  }

  // 0b) Shoes/jewelry in name or title → Luxury, unless description says it's a furniture item (box, rack, cabinet, etc.)
  const nameForShoeJewelry = `${title} ${typeAndTags}`;
  const hasShoeOrJewelryInName = SHOE_JEWELRY_TITLE_WORDS.some((w) => matchWordBoundary(nameForShoeJewelry, w));
  if (hasShoeOrJewelryInName) {
    const descriptionSaysFurniture = DESCRIPTION_FURNITURE_PHRASES.some((phrase) =>
      descriptionText.includes(phrase.toLowerCase())
    );
    if (descriptionSaysFurniture) return "furniture";
    return "luxury";
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
