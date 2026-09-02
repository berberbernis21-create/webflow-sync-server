/**
 * Customer-facing listing titles / shop links for freight quotes.
 * CMS may still say "(No Longer Available)"; we never show that to customers.
 */

export const SHOP_ALL_URL = "https://www.lostandfoundresale.com/all-for-sale";

export function stripNoLongerAvailableSuffix(name) {
  return String(name ?? "")
    .replace(/\s*\(\s*No Longer Available\s*\)\s*/gi, " ")
    .replace(/\s*\(\s*SOLD\s*\)\s*/gi, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/** True when a title field is actually a pasted product / page URL. */
export function titleLooksLikeProductUrl(value) {
  const s = String(value || "").trim();
  if (!s) return false;
  return (
    /^https?:\/\//i.test(s) ||
    /lostandfoundresale\.com\//i.test(s) ||
    /\/products?\//i.test(s)
  );
}

/**
 * Turn a product URL/slug into a readable title when the real name was never stored.
 * e.g. .../faux-bois-branch-cane-back-...-94707 → "Faux Bois Branch Cane Back …"
 */
export function humanizeProductUrlTitle(value, fallback = "Item") {
  const raw = String(value || "").trim();
  if (!raw) return fallback;
  let slug = raw;
  try {
    const href = /^https?:\/\//i.test(raw) ? raw : `https://${raw.replace(/^\/+/, "")}`;
    const u = new URL(href);
    const parts = u.pathname.split("/").filter(Boolean);
    const pi = parts.findIndex((p) => p === "product" || p === "products");
    slug = decodeURIComponent(parts[pi >= 0 && parts[pi + 1] ? pi + 1 : parts.length - 1] || "");
  } catch {
    slug = raw.split("/").filter(Boolean).pop() || raw;
  }
  slug = slug.replace(/\/+$/, "").split("?")[0].split("#")[0];
  // Drop trailing catalog id like -94707
  slug = slug.replace(/-\d{4,}$/g, "");
  const words = slug
    .replace(/[-_+]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .split(" ")
    .filter(Boolean)
    .map((w) => {
      if (/^(x|and|of|the|a|an)$/i.test(w)) return w.toLowerCase();
      return w.charAt(0).toUpperCase() + w.slice(1).toLowerCase();
    });
  if (!words.length) return fallback;
  words[0] = words[0].charAt(0).toUpperCase() + words[0].slice(1);
  return words.join(" ");
}

/** True when the listing is sold / no longer available for purchase. */
export function listingLooksSold(item = {}) {
  const flag = item?.sold ?? item?.is_sold ?? item?.isSold;
  if (flag === true || flag === 1 || String(flag).toLowerCase() === "true") return true;
  const t = String(item?.title || "");
  return /\(\s*No Longer Available\s*\)/i.test(t) || /\(\s*SOLD\s*\)/i.test(t);
}

/** Clean title; appends (SOLD) when the piece is no longer for sale. */
export function customerFacingTitle(item, fallback = "Item") {
  let raw = String(item?.title || "").trim();
  if (titleLooksLikeProductUrl(raw)) {
    raw = humanizeProductUrlTitle(raw, fallback);
  }
  const base = stripNoLongerAvailableSuffix(raw || fallback) || fallback;
  // Still a URL somehow — last resort humanize from product_url.
  const cleaned = titleLooksLikeProductUrl(base)
    ? humanizeProductUrlTitle(item?.product_url || item?.productUrl || base, fallback)
    : base;
  return listingLooksSold(item) ? `${cleaned} (SOLD)` : cleaned;
}

/** Customer CTA URL — sold items always go to Shop All. */
export function customerShopUrl(item) {
  if (listingLooksSold(item)) return SHOP_ALL_URL;
  const url = String(item?.product_url || item?.productUrl || "").trim();
  return url || null;
}

export function customerCtaLabel(item) {
  if (listingLooksSold(item)) return "Shop All";
  return String(item?.product_url || item?.productUrl || "").trim() ? "Buy Now" : "Find Item";
}
