/**
 * Traxia e-commerce vertical + category tags (Shopify product tags).
 * LG / FH are absolute vertical locks — sync and LLM must never override them.
 */

export const ECOMMERCE_VERTICAL_TAG_FURNITURE = "FH";
export const ECOMMERCE_VERTICAL_TAG_LUXURY = "LG";

export const ECOMMERCE_TAG_PREFIX = /\b(?:E[\s_-]?COMMERCE|ECOMMERCE)\b/;

const ECOMMERCE_CATEGORY_CODES = new Set([
  "A", "B", "C", "D", "E", "G", "H", "J", "L", "O", "P", "R", "S", "T", "W", "X",
  "NK", "RG", "BR", "ER", "OJ",
]);

/** Expand comma-joined Traxia tag strings into individual tags. */
export function expandProductTags(product) {
  const t = product?.tags;
  let raw = [];
  if (Array.isArray(t)) raw = t;
  else if (typeof t === "string") raw = t.split(",").map((s) => s.trim()).filter(Boolean);
  const expanded = [];
  for (const tag of raw) {
    const s = String(tag || "").trim();
    if (!s) continue;
    if (s.includes(",")) {
      for (const part of s.split(",")) {
        const p = part.trim();
        if (p) expanded.push(p);
      }
    } else {
      expanded.push(s);
    }
  }
  return expanded;
}

function ecommerceCategoryCodeFromTag(normalized) {
  if (!normalized) return null;
  if (ECOMMERCE_CATEGORY_CODES.has(normalized)) return normalized;

  const combined = normalized.match(
    /(?:^|(?:E[\s_-]?COMMERCE|ECOMMERCE)[^A-Z0-9]*)(?:FH|LG)[\s,./:-]+(NK|RG|BR|ER|OJ|[A-Z])\b/
  );
  if (combined && ECOMMERCE_CATEGORY_CODES.has(combined[1])) return combined[1];

  if (ECOMMERCE_TAG_PREFIX.test(normalized)) {
    const prefixed = normalized.match(
      /\b(?:E[\s_-]?COMMERCE|ECOMMERCE)\b[^A-Z0-9]*(NK|RG|BR|ER|OJ|[A-Z])\b/
    );
    if (prefixed && !["FH", "LG"].includes(prefixed[1]) && ECOMMERCE_CATEGORY_CODES.has(prefixed[1])) {
      return prefixed[1];
    }
  }
  return null;
}

/**
 * Canonical, order-independent Traxia taxonomy state.
 * Tagged products use this fingerprint as the only authorization for taxonomy changes.
 */
export function getEcommerceClassificationFromTags(tags) {
  const rawTags = Array.isArray(tags) ? tags : [];
  const verticalTags = new Set();
  const categoryTags = new Set();

  for (const rawTag of rawTags) {
    const normalized = String(rawTag || "").trim().toUpperCase();
    if (!normalized) continue;

    const vertical = getEcommerceVerticalOverrideFromTags([normalized]);
    if (vertical?.tag) verticalTags.add(vertical.tag);

    const category = ecommerceCategoryCodeFromTag(normalized);
    if (category) categoryTags.add(category);
  }

  const verticalList = [...verticalTags].sort();
  const categoryList = [...categoryTags].sort();
  const conflicts = [];
  if (verticalList.length > 1) conflicts.push("multiple_vertical_tags");
  if (categoryList.length > 1) conflicts.push("multiple_category_tags");

  const verticalTag = verticalList.length === 1 ? verticalList[0] : null;
  const categoryTag = categoryList.length === 1 ? categoryList[0] : null;
  const tagged = verticalList.length > 0 || categoryList.length > 0;

  return {
    tagged,
    verticalTag,
    vertical:
      verticalTag === ECOMMERCE_VERTICAL_TAG_FURNITURE
        ? "furniture"
        : verticalTag === ECOMMERCE_VERTICAL_TAG_LUXURY
          ? "luxury"
          : null,
    categoryTag,
    conflicts,
    fingerprint: tagged
      ? `V:${verticalList.join("+") || "-"}|C:${categoryList.join("+") || "-"}`
      : "",
  };
}

/**
 * Returns true/false for tagged inventory, or null when the product is untagged
 * and the caller should use best-guess classification.
 */
export function ecommerceTagsAuthorizeVerticalChange(classification, existingVertical) {
  if (!classification?.tagged) return null;
  if (classification.conflicts?.length) return false;
  if (!classification.vertical || classification.vertical === existingVertical) return false;
  // A valid FH/LG tag is the authority even if stale cache claims the same fingerprint.
  return true;
}

/** FH → Furniture & Home, LG → Luxury Goods. Category letters are separate. */
export function getEcommerceVerticalOverrideFromTags(tags) {
  if (!Array.isArray(tags) || !tags.length) return null;

  for (const rawTag of tags) {
    const tag = String(rawTag || "").trim();
    if (!tag) continue;
    const normalized = tag.toUpperCase();

    const direct = normalized.match(/^([A-Z]{2})$/);
    if (direct) {
      if (direct[1] === ECOMMERCE_VERTICAL_TAG_FURNITURE) {
        return { vertical: "furniture", tag: direct[1] };
      }
      if (direct[1] === ECOMMERCE_VERTICAL_TAG_LUXURY) {
        return { vertical: "luxury", tag: direct[1] };
      }
      continue;
    }

    if (/^FH(?:[\s,./:-]|$)/.test(normalized)) {
      return { vertical: "furniture", tag: ECOMMERCE_VERTICAL_TAG_FURNITURE };
    }
    if (/^LG(?:[\s,./:-]|$)/.test(normalized)) {
      return { vertical: "luxury", tag: ECOMMERCE_VERTICAL_TAG_LUXURY };
    }

    const prefixed = normalized.match(
      /\b(?:E[\s_-]?COMMERCE|ECOMMERCE)\b[^A-Z0-9]*([A-Z]{2})\b/
    );
    if (prefixed) {
      if (prefixed[1] === ECOMMERCE_VERTICAL_TAG_FURNITURE) {
        return { vertical: "furniture", tag: prefixed[1] };
      }
      if (prefixed[1] === ECOMMERCE_VERTICAL_TAG_LUXURY) {
        return { vertical: "luxury", tag: prefixed[1] };
      }
    }
  }

  return null;
}

/** Absolute vertical from LG/FH — never overridden by classifier, LLM, or Category: ACCESSORIES. */
export function getHardFhLgVerticalLockFromTags(tags) {
  const verticalTag = getEcommerceVerticalOverrideFromTags(tags);
  if (!verticalTag) return null;
  return {
    vertical: verticalTag.vertical,
    tag: verticalTag.tag,
    source: "hard_ecommerce_vertical_tag",
  };
}

export function getHardFhLgVerticalLockFromProduct(product) {
  return getHardFhLgVerticalLockFromTags(expandProductTags(product));
}

export function productHasLuxuryEcommerceTag(product) {
  return getEcommerceVerticalOverrideFromTags(expandProductTags(product))?.vertical === "luxury";
}

export function productHasFurnitureEcommerceTag(product) {
  return getEcommerceVerticalOverrideFromTags(expandProductTags(product))?.vertical === "furniture";
}

export function isEcommerceVerticalOnlyTag(normalized) {
  if (normalized === ECOMMERCE_VERTICAL_TAG_FURNITURE || normalized === ECOMMERCE_VERTICAL_TAG_LUXURY) {
    return true;
  }
  return (
    ECOMMERCE_TAG_PREFIX.test(normalized) &&
    new RegExp(`${ECOMMERCE_TAG_PREFIX.source}[^A-Z0-9]*(FH|LG)\\s*$`).test(normalized)
  );
}
