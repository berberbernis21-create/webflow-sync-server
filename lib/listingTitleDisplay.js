/**
 * Customer-facing listing titles / shop links for freight quotes.
 * CMS may still say "(No Longer Available)"; we never show that to customers.
 */

export const SHOP_ALL_URL = "https://www.lostandfoundresale.com/all-for-sale";

/**
 * First usable absolute https image URL from candidates (strings or {url|src} objects).
 * Email clients reject protocol-relative and relative srcs; String(object) → "[object Object]".
 * Prefers Webflow / website-files CDN when both that and Shopify CDN are present.
 */
export function absoluteHttpsImageUrl(...candidates) {
  const urls = [];
  for (const raw of candidates) {
    if (raw == null || raw === "") continue;
    if (typeof raw === "object") {
      const nested = absoluteHttpsImageUrl(raw.url, raw.src, raw.image_url, raw.imageUrl);
      if (nested) urls.push(nested);
      continue;
    }
    let s = String(raw).trim();
    if (!s || s === "[object Object]") continue;
    if (s.startsWith("//")) s = `https:${s}`;
    if (/^http:\/\//i.test(s)) s = `https://${s.slice(7)}`;
    if (/^https:\/\//i.test(s)) urls.push(s);
  }
  if (!urls.length) return "";
  const webflow = urls.find((u) =>
    /cdn\.prod\.website-files\.com|uploads-ssl\.webflow\.com|website-files\.com/i.test(u)
  );
  return webflow || urls[0];
}

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
