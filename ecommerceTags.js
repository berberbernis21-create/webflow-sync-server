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

/**
 * The owner's two documents ("Furniture Tags.docx", "Handbag Tags.docx").
 * A category letter is read inside its vertical: O is Office Den under FH and
 * Other under LG. Pricing Engine holds the same tables (ecommerce_codes.py).
 */
export const ECOMMERCE_VERTICALS = {
  FH: { merchType: "furniture", label: "Furniture & Home" },
  LG: { merchType: "luxury", label: "Luxury Goods" },
};
export const ECOMMERCE_CATEGORY_LETTERS = {
  FH: {
    A: "Art / Mirrors",
    B: "Bedroom",
    D: "Dining Room",
    L: "Living Room",
    O: "Office Den",
    R: "Rugs",
    X: "Accessories",
    P: "Outdoor / Patio",
    G: "Lighting",
  },
  LG: {
    A: "Accessories",
    B: "Backpacks",
    C: "Crossbody",
    H: "Handbags",
    L: "Luggage",
    O: "Other",
    S: "Small Bags",
    T: "Totes",
    W: "Wallets",
    R: "Scarves",
    E: "Belts",
    NK: "Necklaces",
    RG: "Rings",
    BR: "Bracelets",
    ER: "Earrings",
    OJ: "Other Jewelry",
  },
};
/** "Legacy / General Jewelry: J → Other Jewelry" (Handbag Tags.docx). */
export const ECOMMERCE_LEGACY_LETTERS = { LG: { J: "OJ" } };
/** "Watches → Accessories" (Handbag Tags.docx). A category name, not a tag code. */
export const ECOMMERCE_LABEL_ALIASES = { LG: { watches: "A" } };

const LUXURY_VERTICAL_WORDS = new Set([
  "luxury", "lg", "luxury goods", "luxurygoods", "handbag", "handbags", "jewelry", "accessories",
]);
const FURNITURE_VERTICAL_WORDS = new Set([
  "furniture", "fh", "furniture & home", "furniture / home", "furniture/home", "furniture home",
]);
const CODE_PREFIX = "(?:E[\\s_-]?COMMERCE[\\s:_-]*)?";
const VERTICAL_ONLY_TAG_RE = new RegExp(`^${CODE_PREFIX}(FH|LG)$`);
const COMBINED_CODE_TAG_RE = new RegExp(`^${CODE_PREFIX}(FH|LG)[\\s,./:_-]+([A-Z]{1,2})$`);
const LETTER_TAG_RE = new RegExp(`^${CODE_PREFIX}([A-Z]{1,2})$`);

export function splitEcommerceTags(tags) {
  const parts = Array.isArray(tags) ? tags : String(tags ?? "").split(/[,;\n]+/);
  return parts.map((part) => String(part ?? "").trim()).filter(Boolean);
}

/** FH or LG from a merch type, a label or a code. "" when it names neither. */
export function ecommerceVerticalCode(value) {
  const text = String(value ?? "").trim().toLowerCase().replace(/\s+/g, " ");
  if (LUXURY_VERTICAL_WORDS.has(text)) return "LG";
  if (FURNITURE_VERTICAL_WORDS.has(text)) return "FH";
  return "";
}

/** The letter for a code or a category name, inside one vertical. "" when unknown. */
export function ecommerceCategoryLetter(vertical, value) {
  const table = ECOMMERCE_CATEGORY_LETTERS[String(vertical || "").toUpperCase()];
  const raw = String(value ?? "").trim().replace(/\s+/g, " ");
  if (!table || !raw) return "";
  const upper = raw.toUpperCase();
  if (Object.prototype.hasOwnProperty.call(table, upper)) return upper;
  const legacy = ECOMMERCE_LEGACY_LETTERS[vertical]?.[upper];
  if (legacy) return legacy;
  const low = raw.toLowerCase();
  const squeezed = low.replace(/ /g, "");
  for (const [code, label] of Object.entries(table)) {
    const name = label.toLowerCase();
    if (low === name || squeezed === name.replace(/ /g, "")) return code;
  }
  return ECOMMERCE_LABEL_ALIASES[vertical]?.[low] || "";
}

export function ecommerceCodesKey(vertical, letter) {
  return `${String(vertical || "").toUpperCase()}|${String(letter || "").toUpperCase()}`;
}

/** { vertical, letter } when a tag has the shape of a code. Either part may be "". */
export function parseEcommerceCodeTag(tag) {
  const text = String(tag ?? "").trim().toUpperCase().replace(/\s+/g, " ");
  if (!text) return null;
  let match = VERTICAL_ONLY_TAG_RE.exec(text);
  if (match) return { vertical: match[1], letter: "" };
  match = COMBINED_CODE_TAG_RE.exec(text);
  if (match) return { vertical: match[1], letter: match[2] };
  match = LETTER_TAG_RE.exec(text);
  if (match) return { vertical: "", letter: match[1] };
  return null;
}

/** A vertical code, or a category letter of either vertical. */
export function isEcommerceStructuralTag(tag) {
  const parsed = parseEcommerceCodeTag(tag);
  if (!parsed) return false;
  if (parsed.vertical) return true;
  return Object.keys(ECOMMERCE_CATEGORY_LETTERS).some((code) =>
    Boolean(ecommerceCategoryLetter(code, parsed.letter))
  );
}

/**
 * Read the codes on a tag list. The letter is resolved inside the list's own
 * vertical; with no vertical code, fallbackVertical (the vertical the item
 * already has) is used. scope forces the vertical the letters are read in.
 */
export function resolveEcommerceCodes(tags, { fallbackVertical = "", scope = "" } = {}) {
  const parts = splitEcommerceTags(tags);
  const parsed = parts.map((raw) => ({ raw, code: parseEcommerceCodeTag(raw) }));
  const verticals = [];
  const verticalTags = {};
  for (const { raw, code } of parsed) {
    if (!code?.vertical) continue;
    if (!verticals.includes(code.vertical)) verticals.push(code.vertical);
    (verticalTags[code.vertical] ||= []).push(raw);
  }
  const vertical = verticals.length === 1 ? verticals[0] : "";
  const forced = String(scope || "").trim().toUpperCase();
  const fallback = String(fallbackVertical || "").trim().toUpperCase();
  let use = "";
  if (ECOMMERCE_CATEGORY_LETTERS[forced]) use = forced;
  else if (vertical) use = vertical;
  else if (!verticals.length && ECOMMERCE_CATEGORY_LETTERS[fallback]) use = fallback;
  const letters = [];
  const letterTags = {};
  if (use) {
    for (const { raw, code } of parsed) {
      if (!code?.letter) continue;
      if (code.vertical && code.vertical !== use) continue;
      const letter = ecommerceCategoryLetter(use, code.letter);
      if (!letter) continue;
      if (!letters.includes(letter)) letters.push(letter);
      (letterTags[letter] ||= []).push(raw);
    }
  }
  const conflicts = [];
  if (verticals.length > 1) conflicts.push("multiple_vertical_tags");
  if (letters.length > 1) conflicts.push("multiple_category_tags");
  return {
    tags: parts,
    verticals,
    vertical,
    verticalTags,
    scope: use,
    letters,
    letter: letters.length === 1 ? letters[0] : "",
    letterTags,
    conflicts,
  };
}

/**
 * What an Intake listing write does to the codes on a Shopify product.
 *
 * replace:  a person changed the dropdowns in the Intake app (authority
 *           "intake"), or Shopify still holds exactly the codes Intake itself
 *           put there last (base). Intake's codes take their place.
 * protect:  anything else. The codes on Shopify stay as they are. A missing
 *           vertical code or a missing letter is filled in; a second letter is
 *           never added.
 * conflict: Shopify holds two verticals or two letters and nothing says which
 *           is the latest. Every code tag is left untouched.
 */
export function planListingCodeTags({
  existingTags,
  intakeVertical,
  intakeLetter,
  authority = "",
  base = "",
} = {}) {
  const iv = ecommerceVerticalCode(intakeVertical);
  const ic = iv ? ecommerceCategoryLetter(iv, intakeLetter) : "";
  const shop = resolveEcommerceCodes(existingTags, { fallbackVertical: iv });
  const shopifyKey = ecommerceCodesKey(shop.vertical, shop.letter);
  const conflicts = shop.conflicts;
  const sameAsBase = Boolean(base) && !conflicts.length && shopifyKey === String(base);
  const person = String(authority || "").toLowerCase() === "intake";
  if (iv && (person || sameAsBase)) {
    const remove = [];
    for (const list of Object.values(shop.verticalTags)) remove.push(...list);
    const scopes = new Set([shop.scope, iv, ...shop.verticals].filter(Boolean));
    // A letter is taken off only when a letter replaces it, or a person chose none.
    if (ic || person) {
      for (const use of scopes) {
        const found = resolveEcommerceCodes(existingTags, { scope: use });
        for (const list of Object.values(found.letterTags)) remove.push(...list);
      }
    }
    const letter = ic || (person ? "" : shop.vertical === iv || !shop.vertical ? shop.letter : "");
    return {
      mode: "replace",
      vertical: iv,
      letter,
      remove: [...new Set(remove)],
      add: [iv, ic].filter(Boolean),
      conflicts: [],
      shopifyKey,
    };
  }
  if (conflicts.length) {
    return { mode: "conflict", vertical: iv, letter: ic, remove: [], add: [], conflicts, shopifyKey };
  }
  const vertical = shop.vertical || iv;
  let letter = shop.letter;
  const add = [];
  if (!shop.vertical && iv) add.push(iv);
  if (!letter && ic && vertical === iv) {
    letter = ic;
    add.push(ic);
  }
  return { mode: "protect", vertical, letter, remove: [], add, conflicts: [], shopifyKey };
}

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
