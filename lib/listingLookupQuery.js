/**
 * Parse freight "Find Item" input: product URL, Shopify SKU, or listing title.
 * SKU is a compact code (no spaces), optionally prefixed with "SKU:" or "SKU ".
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
  const skuValue = stripSkuLabel(s);
  if (skuValue && looksLikeListingSku(skuValue)) {
    return { kind: "sku", value: skuValue, raw: s };
  }
  return { kind: "name", value: s, raw: s };
}

/** "SKU: AB-12" / "SKU AB-12" → "AB-12". Leaves "SKU123" unchanged. */
export function stripSkuLabel(raw) {
  const s = String(raw || "").trim();
  const labeled = s.match(/^sku\s*[:#-]\s*(.+)$/i) || s.match(/^sku\s+(.+)$/i);
  return labeled ? labeled[1].trim() : s;
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
  // Dimension fragments such as 60x30x40H stay title lookups.
  if (/^\d+(?:\.\d+)?(?:[x×]\d+(?:\.\d+)?){1,2}h?$/i.test(s)) return false;
  return /^[A-Za-z0-9][A-Za-z0-9._+\-/]*$/.test(s);
}

/** Digit codes are exact SKU lookups. Letter-only tokens still try the title afterward. */
export function listingSkuSkipsTitleFallback(lookup) {
  return lookup?.kind === "sku" && /\d/.test(String(lookup.value || ""));
}

/** Case-insensitive SKU key. Empty when the value has spaces (that is a title). */
export function normalizeListingSkuKey(raw) {
  const s = String(raw ?? "").trim();
  if (!s || /\s/.test(s) || s.length > 64) return "";
  return s.toLowerCase();
}
