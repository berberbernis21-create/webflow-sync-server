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

/** True when the listing is sold / no longer available for purchase. */
export function listingLooksSold(item = {}) {
  const flag = item?.sold ?? item?.is_sold ?? item?.isSold;
  if (flag === true || flag === 1 || String(flag).toLowerCase() === "true") return true;
  const t = String(item?.title || "");
  return /\(\s*No Longer Available\s*\)/i.test(t) || /\(\s*SOLD\s*\)/i.test(t);
}

/** Clean title; appends (SOLD) when the piece is no longer for sale. */
export function customerFacingTitle(item, fallback = "Item") {
  const base = stripNoLongerAvailableSuffix(item?.title || fallback) || fallback;
  return listingLooksSold(item) ? `${base} (SOLD)` : base;
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
