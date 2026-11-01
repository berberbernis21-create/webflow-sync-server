/**
 * Parse freight "Find Item" input: product URL, Shopify SKU, or listing title.
 * A catalog SKU is the whole entry, about 6 or 7 characters, like KVLUZK.
 * The input is read as one string. A space followed by more words is a title,
 * so "Lamp 12" is never joined into a code.
 * A title that includes dimensions (10X10X12, 20X1X36) is always a name.
 * A miss on a letters-only code still falls through to title search.
 * A code that contains a digit is SKU-only so it is not fuzzy-matched to a title.
 * @returns {{ kind: "slug"|"sku"|"name", value: string, raw: string }}
 */
export function extractListingLookupQuery(raw) {
  const s = String(raw || "").trim();
  if (!s) return { kind: "name", value: "", raw: s };
  const looksUrl =
    /^https?:\/\//i.test(s) ||
    /lostandfoundresale\.com/i.test(s) ||
    /\/products?\//i.test(s);
  if (looksUrl) {
    try {
      const href = /^https?:\/\//i.test(s) ? s : `https://${s.replace(/^\/+/, "")}`;
      const u = new URL(href);
      const parts = u.pathname.split("/").filter(Boolean);
      const pi = parts.findIndex((p) => p === "product" || p === "products");
      if (pi >= 0 && parts[pi + 1]) {
        return {
          kind: "slug",
          value: decodeURIComponent(parts[pi + 1]).replace(/\/+$/, ""),
          raw: s,
        };
      }
      if (parts.length) {
        return {
          kind: "slug",
          value: decodeURIComponent(parts[parts.length - 1]).replace(/\/+$/, ""),
          raw: s,
        };
      }
    } catch {
      /* fall through */
    }
    return { kind: "name", value: s, raw: s };
  }
  const labeled = stripSkuLabel(s);
  if (containsListingDimensions(labeled)) {
    return { kind: "name", value: s, raw: s };
  }
  // Space, then more words: the whole input is a title. Do not join the words.
  if (/\s/.test(labeled)) {
    return { kind: "name", value: s, raw: s };
  }
  if (looksLikeListingSku(labeled)) {
    return { kind: "sku", value: labeled, raw: s };
  }
  return { kind: "name", value: s, raw: s };
}

function collapseListingWhitespace(raw) {
  return String(raw || "").replace(/[\s\u00a0]+/g, "");
}

/** "SKU: AB-12" / "SKU AB-12" → "AB-12". Leaves "SKU123" unchanged. */
export function stripSkuLabel(raw) {
  const s = String(raw || "").trim();
  const labeled = s.match(/^sku\s*[:#-]\s*(.+)$/i) || s.match(/^sku\s+(.+)$/i);
  return labeled ? labeled[1].trim() : s;
}

/** Size text such as 10X10X12, 20X1X36, or 60x30x40H. Never a SKU. */
export function containsListingDimensions(raw) {
  return /\d+(?:\.\d+)?(?:[x×]\d+(?:\.\d+)?){1,2}h?/i.test(String(raw || ""));
}

/**
 * Compact catalog codes. Multi-word titles are not SKUs.
 * Requires no spaces so "Ethan Allen sofa" stays a name lookup.
 */
export function looksLikeListingSku(raw) {
  const s = String(raw || "").trim();
  if (!s || s.length < 2 || s.length > 64) return false;
  if (/\s/.test(s)) return false;
  if (/^https?:\/\//i.test(s) || /lostandfoundresale\.com/i.test(s) || /\/products?\//i.test(s)) {
    return false;
  }
  if (containsListingDimensions(s)) return false;
  return /^[A-Za-z0-9][A-Za-z0-9._+\-/]*$/.test(s);
}

/** Digit codes are exact SKU lookups. Letter-only tokens still try the title afterward. */
export function listingSkuSkipsTitleFallback(lookup) {
  return lookup?.kind === "sku" && /\d/.test(String(lookup.value || ""));
}

/** Case-insensitive SKU key with every space removed. */
export function normalizeListingSkuKey(raw) {
  const s = collapseListingWhitespace(String(raw ?? "").trim());
  if (!s || s.length > 64) return "";
  return s.toLowerCase();
}

/**
 * Admin search strings for one variant SKU.
 * Unquoted `sku:CODE` first when the code has no search operators — quoted
 * `sku:"CODE"` often misses the same variant in Shopify's index.
 * Quote when the code contains whitespace or : \ ( ) * ".
 * Hyphens stay unquoted (`sku:XYZ-12345` is valid Shopify syntax).
 * @returns {string[]}
 */
export function shopifySkuSearchQueries(sku) {
  const compact = collapseListingWhitespace(String(sku ?? "").trim());
  if (!compact) return [];
  const escaped = compact.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
  const quoted = `sku:"${escaped}"`;
  if (/[\s:"\\()*]/.test(compact)) return [quoted];
  const unquoted = `sku:${escaped}`;
  return unquoted === quoted ? [quoted] : [unquoted, quoted];
}

function escapeListingSkuRegex(key) {
  return String(key).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * 2 = exact normalized SKU, 1 = the floor code plus Shopify's numeric suffix
 * (`GND4FH` → `GND4FH-2877`), 0 = not this SKU.
 * A title hit is never a match.
 */
export function storedListingSkuMatchRank(stored, query) {
  const storedKey = normalizeListingSkuKey(stored);
  const key = normalizeListingSkuKey(query);
  if (!storedKey || !key) return 0;
  if (storedKey === key) return 2;
  const suffix = new RegExp(`^${escapeListingSkuRegex(key)}-\\d+$`);
  return suffix.test(storedKey) ? 1 : 0;
}

/**
 * Keep an exact SKU, or a single `CODE-digits` variant. Two different suffixes
 * are ambiguous and return null so a shorter code cannot grab the wrong item.
 * @param {Array<{ sku?: string }>} candidates
 */
export function pickListingSkuMatch(candidates, query) {
  const ranked = [];
  for (const candidate of candidates || []) {
    const rank = storedListingSkuMatchRank(candidate?.sku, query);
    if (rank <= 0) continue;
    ranked.push({ candidate, rank, key: normalizeListingSkuKey(candidate.sku) });
  }
  const exact = ranked.find((row) => row.rank === 2);
  if (exact) return exact.candidate;
  const suffixKeys = new Set(ranked.filter((row) => row.rank === 1).map((row) => row.key));
  if (suffixKeys.size === 1) return ranked.find((row) => row.rank === 1).candidate;
  return null;
}
