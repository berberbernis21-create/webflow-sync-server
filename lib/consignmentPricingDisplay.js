/**
 * Shared pricing display for internal team email (blurred $) and internal PDF (full detail).
 */

import { shouldShowMultiPieceCallout } from "./consignmentMultiPiece.js";

/** Shown after "Analysis:" in internal email (and PDF summary footer). */
export const EMAIL_ANALYSIS_LABEL =
  "Multimodal computer vision, embedding-based similarity search, and automated market-comp synthesis";

/** Top of internal team notification. */
export const INTERNAL_EMAIL_PRICING_NOTICE =
  "Full analysis is in the attached PDF.";

/** Labels for the consignment reviewer reading this submission. */
export function getSuggestedPricingLabels(suggested) {
  const anchor = String(suggested?.pricingAnchor || "").toLowerCase();
  if (anchor === "resale_comp_average") {
    return {
      rangeLabel: "Market recommendation (retail unverified)",
      anchorLabel: "Resale context only — confirm retail",
    };
  }
  if (anchor === "retail_30_50") {
    return {
      rangeLabel: "Market recommendation (30–50% of retail)",
      anchorLabel: "Retail estimate from research (linked listings)",
    };
  }
  if (anchor === "consignor_retail_30_50") {
    return {
      rangeLabel: "Market recommendation (from consignor retail)",
      anchorLabel: "Consignor-stated retail (not research)",
    };
  }
  return {
    rangeLabel: "Market recommendation (research)",
    anchorLabel: "Pricing reference",
  };
}

export function formatUsd(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return "N/A";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(n);
}

export function capitalizeConfidence(value) {
  const s = String(value || "low").toLowerCase();
  if (s === "high" || s === "medium" || s === "low") {
    return s.charAt(0).toUpperCase() + s.slice(1);
  }
  return "Low";
}

export function findPricingForItem(pricingResults, itemNumber) {
  if (!Array.isArray(pricingResults)) return null;
  const n = Number(itemNumber);
  return (
    pricingResults.find((p) => p.itemNumber === itemNumber || p.itemNumber === n) || null
  );
}

function escapeHtml(str) {
  return String(str ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function blurPriceHtml(formattedValue) {
  const label = escapeHtml(formattedValue);
  return [
    `<span style="display:inline-block;position:relative;">`,
    `<span style="filter:blur(6px);-webkit-filter:blur(6px);color:transparent;text-shadow:0 0 10px rgba(26,60,52,0.45);user-select:none;">${label}</span>`,
    `<span style="position:absolute;left:0;top:0;right:0;bottom:0;" aria-hidden="true"></span>`,
    `</span>`,
  ].join("");
}

/** Keep in sync with parseMoneyUsd in consignmentCompTuning.js ($40k → 40000). */
function parseConsignorRetailUsd(item) {
  let raw = String(item?.originalPrice ?? "").trim();
  if (!raw) return null;
  raw = raw.replace(/,/g, "");
  const kMatch = raw.match(/(\d+(?:\.\d+)?)\s*[kK]\b/);
  if (kMatch) {
    const n = Number(kMatch[1]) * 1000;
    return Number.isFinite(n) && n > 0 ? Math.round(n) : null;
  }
  const mMatch = raw.match(/(\d+(?:\.\d+)?)\s*[mM]\b/);
  if (mMatch) {
    const n = Number(mMatch[1]) * 1_000_000;
    return Number.isFinite(n) && n > 0 ? Math.round(n) : null;
  }
  const thousandWord = raw.match(/(\d+(?:\.\d+)?)\s*thousand\b/i);
  if (thousandWord) {
    const n = Number(thousandWord[1]) * 1000;
    return Number.isFinite(n) && n > 0 ? Math.round(n) : null;
  }
  const n = Number(raw.replace(/[^\d.]/g, ""));
  return Number.isFinite(n) && n > 0 ? Math.round(n) : null;
}

export function getResearchFindingRows(suggested, comps, compEvidence) {
  const rows = [];
  const anchor = String(suggested?.pricingAnchor || "").toLowerCase();
  const retailCount = compEvidence?.linkedRetailCount ?? 0;
  const resaleCount = compEvidence?.linkedResaleCount ?? 0;

  if (anchor === "retail_30_50" && retailCount > 0) {
    rows.push({
      label: "Retail comps found",
      valueText: `${retailCount} linked listing${retailCount === 1 ? "" : "s"}`,
    });
    if (suggested?.retailEstimate) {
      rows.push({ label: "Retail estimate (research)", value: suggested.retailEstimate });
    }
    if (comps?.average) {
      rows.push({ label: "Retail average (linked)", value: comps.average });
    }
    if (comps?.high != null && comps?.low != null) {
      rows.push({
        label: "Retail high / low",
        valueText: `${formatUsd(comps.high)} / ${formatUsd(comps.low)}`,
      });
    }
  } else {
    rows.push({ label: "Retail comps found", valueText: "None verified" });
  }

  if (resaleCount > 0) {
    rows.push({
      label: "Resale context listings",
      valueText: `${resaleCount} (directional only)`,
    });
    if (compEvidence?.resaleCompAverage) {
      rows.push({ label: "Resale context average", value: compEvidence.resaleCompAverage });
    }
  } else {
    rows.push({ label: "Resale context listings", valueText: "None verified" });
  }

  return rows;
}

export function getCustomerInputRows(suggested, item) {
  const rows = [];
  const consignorRetail =
    suggested?.customerStatedRetail != null
      ? Number(suggested.customerStatedRetail)
      : parseConsignorRetailUsd(item);
  if (!Number.isFinite(consignorRetail) || consignorRetail <= 0) {
    rows.push({ label: "Consignor-stated retail", valueText: "Not provided" });
    return rows;
  }
  rows.push({ label: "Consignor-stated retail", value: consignorRetail });
  const impliedLow =
    suggested?.customerImpliedAskLow != null
      ? Number(suggested.customerImpliedAskLow)
      : Math.round(consignorRetail * 0.3);
  const impliedHigh =
    suggested?.customerImpliedAskHigh != null
      ? Number(suggested.customerImpliedAskHigh)
      : Math.round(consignorRetail * 0.5);
  if (impliedLow > 0 && impliedHigh > 0) {
    rows.push({
      label: "If we used consignor input only (30–50% of their retail)",
      valueText: `${formatUsd(impliedLow)} - ${formatUsd(impliedHigh)}`,
    });
  }
  return rows;
}

export function getReviewerGuidanceRows(reviewerGuidance) {
  const g = reviewerGuidance || {};
  const rows = [];
  if (g.noExactRetailComps || (g.typicalRetailLow && g.typicalRetailHigh) || g.notes) {
    if (g.basis) {
      rows.push({ label: "Reviewer guidance basis", valueText: String(g.basis), value: null });
    }
    if (g.typicalRetailLow != null && g.typicalRetailHigh != null) {
      rows.push({
        label: "Typical brand/category retail (not verified comps)",
        valueText: `${formatUsd(g.typicalRetailLow)} - ${formatUsd(g.typicalRetailHigh)}`,
        value: null,
      });
    }
    if (g.impliedAskLow != null && g.impliedAskHigh != null) {
      rows.push({
        label: "Directional ask from that band (reviewer starting point)",
        valueText: `${formatUsd(g.impliedAskLow)} - ${formatUsd(g.impliedAskHigh)}`,
        value: null,
      });
    }
  }
  return rows;
}

export function formatReviewerGuidanceNotes(reviewerGuidance) {
  const notes = String(reviewerGuidance?.notes || "").trim();
  if (!notes) return "";
  if (reviewerGuidance?.noExactRetailComps) {
    return `No exact retail comps verified. ${notes}`;
  }
  return notes;
}

export function getImageMatchConfidence(analysis) {
  const comps = analysis?.comparableComps || {};
  const sources = Array.isArray(analysis?.sources) ? analysis.sources : [];
  const strong = sources.filter((s) => String(s?.matchStrength || "").toLowerCase() === "strong").length;
  const retailCount = analysis?.compEvidence?.linkedRetailCount ?? 0;
  const base = capitalizeConfidence(comps.confidence);
  if (strong >= 2 && retailCount >= 1) {
    return { label: base, detail: `${strong} strong visual matches among research links` };
  }
  if (strong >= 1) {
    return { label: base, detail: `${strong} strong visual match from photos vs listings` };
  }
  if (retailCount >= 1) {
    return { label: base, detail: "Retail comps found; visual match not strongly confirmed" };
  }
  return { label: "Low", detail: "No strong photo-to-listing matches confirmed" };
}

export function hasResearchEvidence(analysis) {
  const retail = analysis?.compEvidence?.linkedRetailCount ?? 0;
  const resale = analysis?.compEvidence?.linkedResaleCount ?? 0;
  const g = analysis?.reviewerGuidance || {};
  const hasGuidanceBand = g.typicalRetailLow != null && g.typicalRetailHigh != null;
  return retail > 0 || resale > 0 || hasGuidanceBand;
}

export function buildFinalPricingSummary({ suggested, item, reviewerGuidance, analysis } = {}) {
  const retailCount = analysis?.compEvidence?.linkedRetailCount ?? 0;
  const guidance = reviewerGuidance || {};
  const customerRows = getCustomerInputRows(suggested, item);
  const customerRetail = customerRows.find((r) => r.label === "Consignor-stated retail");
  const match = getImageMatchConfidence(analysis);

  const found =
    retailCount > 0
      ? `Research found ${retailCount} verified retail listing${retailCount === 1 ? "" : "s"}${
          suggested?.retailEstimate ? ` (retail estimate ${formatUsd(suggested.retailEstimate)})` : ""
        }.`
      : guidance.typicalRetailLow != null && guidance.typicalRetailHigh != null
        ? `No verified retail comps — brand/category research points to about ${formatUsd(guidance.typicalRetailLow)}–${formatUsd(guidance.typicalRetailHigh)} typical retail.`
        : "No verified retail comps were found in research.";

  const suggest =
    suggested?.rangeLow != null && suggested?.rangeHigh != null
      ? `Market recommendation: ${formatUsd(suggested.rangeLow)}–${formatUsd(suggested.rangeHigh)}, based on research and our 30–50% of retail pricing.`
      : guidance.impliedAskLow != null && guidance.impliedAskHigh != null
        ? `Directional ask from the typical retail band (30–50%): ${formatUsd(guidance.impliedAskLow)}–${formatUsd(guidance.impliedAskHigh)}. Confirm with better comps if you can.`
        : "Not enough verified comps yet for a market recommendation — review the factors and links below.";

  const customerBit =
    customerRetail?.value != null
      ? `Consignor stated ${formatUsd(customerRetail.value)} (their input only — not used as the research retail).`
      : "Consignor did not provide a retail price.";

  return [
    found,
    `Image match: ${match.label} — ${match.detail}.`,
    suggest,
    customerBit,
    "We price at ~30–50% of verified retail when we have it; consignor input is checked separately.",
  ].join(" ");
}

export function formatPricingRationaleForDisplay(rationale) {
  return String(rationale || "")
    .replace(/\bconsignor_retail_30_50\b/gi, "customer-stated retail")
    .replace(/\bretail_30_50\b/gi, "online retail reference")
    .replace(/\bresale_comp_average\b/gi, "resale context")
    .replace(/\s{2,}/g, " ")
    .trim();
}

const PRICING_UNAVAILABLE_REASONS = {
  not_configured:
    "Server pricing APIs are not fully configured (check Render env: Google Vision, Custom Search, Gemini, and OpenAI).",
  no_photos: "No photos were matched to this item for analysis.",
  error: "Pricing analysis encountered an error for this item.",
  item_timeout:
    "Automated comp lookup did not finish in time. Full comps and links are in the attached PDF.",
  no_comps: "No reliable comparable prices could be derived.",
  budget_exceeded:
    "Automated comp lookup did not finish in time. Full comps and links are in the attached PDF.",
  pricing_item_limit: "Automated comp analysis runs on the first 10 items per submission.",
  disabled: "Pricing analysis is disabled on the server.",
};

function pricingUnavailableHeadline(pricing) {
  const reason = String(pricing?.reason || "").trim();
  if (
    reason === "budget_exceeded" ||
    reason === "item_timeout" ||
    reason === "pricing_item_limit"
  ) {
    return "Included in submission — manual comp review";
  }
  return "Comparable analysis unavailable";
}

function pricingUnavailableDetail(pricing) {
  const reason = String(pricing?.reason || "").trim();
  return PRICING_UNAVAILABLE_REASONS[reason] || "";
}

/** Remove dollar amounts from email summary so replies do not leak pricing to customers. */
function sanitizeEmailSummaryText(text) {
  let s = String(text || "").trim();
  if (!s) return s;
  s = s.replace(/\$\s?[\d,]+(?:\.\d{1,2})?/g, "[see PDF]");
  s = s.replace(/(\[see PDF\]\s*){2,}/gi, "[see PDF] ");
  s = s.replace(
    /30[–-]50%\s+of[^.]*(?:suggested ask|retail\/comp anchor|market recommendation)[^.]*\./gi,
    "Market recommendation is 30–50% of credible retail; see attached PDF. "
  );
  s = s.replace(/\s{2,}/g, " ").trim();
  return s;
}

/**
 * Short 2–3 sentence email blurb; full research stays in the PDF.
 */
function buildShortEmailPricingBlurb(pricing, item = null) {
  const analysis = pricing?.analysis;
  if (!pricing?.available || !analysis) {
    const headline = pricingUnavailableHeadline(pricing);
    return `${headline}. Please check the attached PDF for details.`;
  }

  const { comparableComps: comps, suggestedPricing: suggested, photoBundle, reviewerGuidance } =
    analysis;
  const retailCount = analysis?.compEvidence?.linkedRetailCount ?? 0;
  const resaleCount = analysis?.compEvidence?.linkedResaleCount ?? 0;
  const match = getImageMatchConfidence(analysis);
  const showMulti = shouldShowMultiPieceCallout(photoBundle);

  const sentences = [];

  if (showMulti) {
    sentences.push(
      `Multiple pieces may be in the photos — please check the PDF before treating this as one item.`
    );
  }

  if (suggested?.rangeLow != null && suggested?.rangeHigh != null) {
    sentences.push(
      `We found a market recommendation from research (amounts blurred here — see PDF). Comp confidence: ${capitalizeConfidence(comps?.confidence)}; image match: ${match.label}.`
    );
  } else if (retailCount > 0 || resaleCount > 0) {
    sentences.push(
      `We found some research leads (${retailCount} retail / ${resaleCount} resale context), but not enough verified comps for a firm ask yet.`
    );
  } else {
    const basis = String(reviewerGuidance?.basis || "").trim();
    const shortBasis = basis.length > 120 ? `${basis.slice(0, 117).trim()}…` : basis;
    sentences.push(
      shortBasis
        ? `No verified retail comps yet. Research points to: ${shortBasis}.`
        : `No verified retail or resale comps yet from research.`
    );
  }

  sentences.push(`Please check the attached PDF for full comps, links, and pricing detail.`);

  return sentences.slice(0, 3).join(" ");
}

/**
 * Internal email: short Suggested Pricing blurb (full detail in PDF).
 * Customer-stated fields stay in the item bullets above this block.
 */
export function buildInternalEmailPricingHtml(pricing, item = null) {
  const blurb = buildShortEmailPricingBlurb(pricing, item);
  return [
    `<div style="margin-top:16px;padding:14px;background:#f9f7f4;border-radius:6px;border:1px solid #e8e2d9;">`,
    `<p style="margin:0 0 8px;font-size:13px;font-weight:700;color:#1a3c34;text-transform:uppercase;letter-spacing:0.04em;">Suggested Pricing for Lost &amp; Found</p>`,
    `<p style="margin:0;font-size:13px;color:#444;line-height:1.55;">${escapeHtml(blurb)}</p>`,
    `</div>`,
  ].join("");
}

export function buildInternalEmailPricingText(pricing, item = null) {
  return `Suggested Pricing for Lost & Found: ${buildShortEmailPricingBlurb(pricing, item)}`;
}

/* ---------- Link classification / PDF helpers (kept for analysis pipeline) ---------- */

/** Known resale / consignment marketplaces (directional comps). */
export const RESALE_HOST_HINTS = [
  "ebay.",
  "facebook.com",
  "marketplace",
  "mercari",
  "poshmark",
  "grailed",
  "therealreal",
  "fashionphile",
  "rebag.com",
  "vestiairecollective",
  "yoogiscloset",
  "luxuryexchange",
  "1stdibs",
  "chairish",
  "etsy.com",
  "auctionninja",
  "liveauctioneers",
  "invaluable.com",
  "offers.ebay",
  "lostandfoundresale",
  "lostandfoundhandbags",
  "craigslist",
  "offerup",
  "kashew",
  "kaiyo",
  "aptdeco",
  "reperch",
  "designplusgallery",
  "secondhandstoryshop",
];

/**
 * Preferred full-price retail / manufacturer hosts — highest trust for pricing anchors.
 * Keep manufacturer domains here when known; brand→site mapping also lives in search queries.
 */
export const PREFERRED_RETAIL_HOSTS = [
  "wayfair.com",
  "crateandbarrel.com",
  "potterybarn.com",
  "westelm.com",
  "rh.com",
  "restorationhardware.com",
  "arhaus.com",
  "cb2.com",
  "roomandboard.com",
  "article.com",
  "dwr.com",
  "designwithinreach.com",
  "ethanallen.com",
  "ashleyfurniture.com",
  "frontgate.com",
  "ballarddesigns.com",
  "serenaandlily.com",
  "anthropologie.com",
  "williams-sonoma.com",
  "ikea.com",
  "bloomingdales.com",
  "neimanmarcus.com",
  "saksfifthavenue.com",
  "nordstrom.com",
  "macys.com",
  "stanfordfurniture.com",
  "stickley.com",
  "bakerfurniture.com",
  "centuryfurniture.com",
  "henredon.com",
  "thomasville.com",
  "la-z-boy.com",
  "roomstogo.com",
  "havertys.com",
  "potterybarnkids.com",
];

/** Broader retail host substrings (includes preferred). */
export const RETAIL_HOST_HINTS = [
  ...PREFERRED_RETAIL_HOSTS.map((h) => h.replace(/^\./, "")),
  "wayfair",
  "crateandbarrel",
  "potterybarn",
  "westelm",
  "rh.com",
  "arhaus",
  "anthropologie",
  "cb2.com",
  "roomandboard",
  "ashleyfurniture",
  "designwithinreach",
  "article.com",
  "ethanallen",
  "frontgate",
  "ballarddesigns",
  "restorationhardware",
  "williams-sonoma",
  "bloomingdales",
  "neimanmarcus",
  "macys.com",
  "nordstrom",
  "ikea.",
  "manufacturer",
  "factorydirect",
];

function extractUrlsFromText(text) {
  const re = /https?:\/\/[^\s<>"')]+/gi;
  return [...new Set((String(text || "").match(re) || []).map((u) => u.replace(/[.,;]+$/, "")))];
}

const EXCLUDED_NON_COMP_HOSTS = [
  "yelp.com",
  "mapquest.com",
  "google.com",
  "goo.gl",
  "g.co",
  "instagram.com",
  "pinterest.com",
  "pin.it",
  "tiktok.com",
  "youtube.com",
  "youtu.be",
  "reddit.com",
  "redd.it",
  "quora.com",
  "wikipedia.org",
  "facebook.com/groups",
];

export function isJunkCompSource(url, title = "", snippet = "") {
  const u = String(url || "").toLowerCase();
  const blob = `${u} ${title} ${snippet}`.toLowerCase();
  let host = "";
  try {
    host = new URL(String(url || "").trim()).hostname.replace(/^www\./, "").toLowerCase();
  } catch {
    host = "";
  }
  if (!host) return true;
  if (EXCLUDED_NON_COMP_HOSTS.some((h) => host === h || host.endsWith(`.${h}`) || u.includes(h))) {
    return true;
  }
  if (host.endsWith(".edu") || host.endsWith(".gov")) return true;
  // Category/keyword search result pages are not product comps (e.g. wayfair.com/keyword.php?…).
  if (
    /keyword\.php|\/keyword\/|\/keywords\/|\/search\?|\/catalogsearch\/|\/filter\?|\/collections\/all\?|\/buy\/[a-z0-9-]+\/?$/.test(
      u
    )
  ) {
    return true;
  }
  if (/\.pdf(\?|#|$)/i.test(u) && /tuition|syllabus|academic|fee|credit/.test(blob)) return true;
  if (/\b(tuition|academic year|credit hour|course catalog)\b/.test(blob)) return true;
  if (/\/r\/|reddit\.com|forum|discussion thread/.test(blob)) return true;
  return false;
}

export function classifyLinkChannel(url, title = "", snippet = "") {
  const u = String(url || "").toLowerCase();
  const blob = `${u} ${title} ${snippet}`.toLowerCase();
  if (isJunkCompSource(url, title, snippet)) return null;
  for (const h of RETAIL_HOST_HINTS) {
    if (blob.includes(h)) return "retail";
  }
  for (const h of RESALE_HOST_HINTS) {
    if (blob.includes(h)) return "resale";
  }
  if (/marketplace|resale|consignment|pre-owned|secondhand|used\s/.test(blob)) return "resale";
  if (/new-in-stock|\/product\/|add-to-cart|retail/.test(blob)) return "retail";
  return null;
}

function safeUrl(url) {
  const u = String(url || "").trim();
  return u.startsWith("http://") || u.startsWith("https://") ? u : "";
}

function hostOf(url) {
  try {
    return new URL(url).hostname.replace(/^www\./, "").toLowerCase();
  } catch {
    return "";
  }
}

function looksLikeListingUrl(url) {
  const u = String(url || "").toLowerCase();
  if (
    /keyword\.php|\/keyword\/|\/search\?|\/catalogsearch\/|\/filter\?/.test(u)
  ) {
    return false;
  }
  return (
    /\/(product|products|item|listing|listings|p|shop|store|pdp)\/|sku=|variant=|product_id=|listing_id=|\/furniture\/pdp\//.test(
      u
    ) || /facebook\.com\/(marketplace|groups)\//.test(u)
  );
}

/**
 * Source reliability tier for ranking / filtering comps.
 * preferred_retail > known_retail > known_resale > listing > weak
 */
export function getCompSourceTier(url, title = "", snippet = "") {
  if (isJunkCompSource(url, title, snippet)) return null;
  const host = hostOf(url);
  if (!host) return null;
  const u = String(url || "").toLowerCase();
  const blob = `${u} ${title || ""} ${snippet || ""}`.toLowerCase();

  if (PREFERRED_RETAIL_HOSTS.some((h) => host === h || host.endsWith(`.${h}`) || u.includes(h))) {
    return "preferred_retail";
  }
  if (RETAIL_HOST_HINTS.some((h) => u.includes(h) || host.includes(String(h).replace(/^\./, "")))) {
    return "known_retail";
  }
  if (RESALE_HOST_HINTS.some((h) => u.includes(h) || host.includes(String(h).replace(/^\./, "")))) {
    return "known_resale";
  }
  if (looksLikeListingUrl(url) || /\/product\/|add-to-cart|buy now|msrp|retail price/.test(blob)) {
    return "listing";
  }
  return "weak";
}

export function compSourceTierScore(tier) {
  switch (tier) {
    case "preferred_retail":
      return 100;
    case "known_retail":
      return 80;
    case "known_resale":
      return 55;
    case "listing":
      return 35;
    case "weak":
      return 10;
    default:
      return 0;
  }
}

export function isCredibleCompSource(url, title = "", snippet = "", channel = null) {
  if (isJunkCompSource(url, title, snippet)) return false;
  const ch = channel || classifyLinkChannel(url, title, snippet);
  const tier = getCompSourceTier(url, title, snippet);
  if (!tier || tier === "weak") return false;
  const host = hostOf(url);
  if (!host) return false;

  // Retail anchor: prefer known/preferred hosts. Generic /product/ pages only if channel says retail.
  if (ch === "retail") {
    return tier === "preferred_retail" || tier === "known_retail" || tier === "listing";
  }
  if (ch === "resale") {
    return tier === "known_resale" || tier === "listing";
  }
  // Unknown channel — only accept clear marketplace/retail hosts.
  return tier === "preferred_retail" || tier === "known_retail" || tier === "known_resale";
}

export function buildCategorizedLinks({ item, analysis }) {
  const buckets = { resale: [], retail: [], customer: [], visual: [] };
  const seen = new Set();

  for (const entry of analysis?.verifiedRetailCompEntries || []) {
    const url = safeUrl(entry?.url);
    if (!url || seen.has(url)) continue;
    seen.add(url);
    buckets.retail.push({
      title: entry.title || url,
      url,
      price: entry.price,
      channel: "retail",
      note: entry.strongVisionMatch
        ? "Strong image match + verified live listing — used for the 30–50% retail basis"
        : "Verified live listing — used for the 30–50% retail basis",
      matchStrength: "strong",
      linkVerified: true,
    });
  }

  for (const entry of analysis?.verifiedResaleCompEntries || []) {
    const url = safeUrl(entry?.url);
    if (!url || seen.has(url)) continue;
    seen.add(url);
    buckets.resale.push({
      title: entry.title || url,
      url,
      price: entry.price,
      channel: "resale",
      note: "Verified live listing — resale context only (not the retail basis)",
      matchStrength: "weak",
      linkVerified: true,
    });
  }

  // Preserve Google Vision strong matches even when we don't yet have a scrapeable price.
  for (const s of analysis?.sources || []) {
    const url = safeUrl(s?.url);
    if (!url || seen.has(url)) continue;
    const strong =
      s.strongVisionMatch ||
      String(s.matchStrength || "").toLowerCase() === "strong" ||
      s.linkVerified === "visual_only";
    if (!strong) continue;
    seen.add(url);
    buckets.visual.push({
      title: s.title || url,
      url,
      price: s.price ?? null,
      channel: "visual",
      note:
        s.linkVerified === true
          ? "Strong Google image match (live) — review for retail price"
          : "Strong Google image match — kept even if live probe was blocked; confirm in browser",
      matchStrength: "strong",
      linkVerified: s.linkVerified,
    });
  }

  const retail = parseConsignorRetailUsd(item);
  if (retail) {
    buckets.customer.push({
      title: "Consignor-stated original retail (submission form)",
      url: null,
      price: retail,
      channel: "customer",
      note: "Consignor input only — not used as the research retail",
    });
  }

  const customerUrls = new Set();
  for (const field of [item?.notes, item?.conditionNotes, item?.warnings]) {
    for (const url of extractUrlsFromText(field)) {
      if (customerUrls.has(url)) continue;
      customerUrls.add(url);
      buckets.customer.push({
        title: "URL from consignor notes",
        url,
        price: null,
        channel: "customer",
        note: "Consignor-provided link",
      });
    }
  }

  for (const key of ["resale", "retail", "customer", "visual"]) {
    buckets[key].sort((a, b) => String(a.title).localeCompare(String(b.title)));
  }
  return buckets;
}

function ensurePdfSpace(doc, neededHeight, margin, contentBottom) {
  if (doc.y + neededHeight <= contentBottom) return;
  doc.addPage();
  doc.y = margin;
}

function drawPdfLinkGroup(doc, links, { margin, contentWidth, contentBottom }) {
  if (!links?.length) {
    doc.font("Helvetica-Oblique").fontSize(9).fillColor("#888").text("(none)", margin, doc.y, {
      width: contentWidth,
    });
    doc.moveDown(0.2);
    return;
  }
  for (const link of links) {
    ensurePdfSpace(doc, 32, margin, contentBottom);
    const pricePart = link.price != null ? ` — ${formatUsd(link.price)}` : "";
    doc.fillColor("#333").font("Helvetica").fontSize(9);
    doc.text(link.title ? `• ${link.title}${pricePart}` : `• ${formatUsd(link.price)}`, margin, doc.y, {
      width: contentWidth,
      lineGap: 1,
    });
    if (link.url) {
      doc.fillColor("#1a73e8").font("Helvetica").fontSize(8.8);
      doc.text(`  ${link.url}`, margin, doc.y, {
        width: contentWidth,
        underline: true,
        link: link.url,
        lineGap: 1,
      });
    }
    if (link.note) {
      doc.fillColor("#666").font("Helvetica").fontSize(8.5);
      doc.text(`  ${link.note}`, margin, doc.y, { width: contentWidth, lineGap: 1 });
    }
    doc.moveDown(0.12);
  }
}

/**
 * PDF pricing: ask first (bold), short why, then reasons + links.
 */
export function drawPdfPricingSection(doc, pricing, { brandColor, margin, contentWidth, contentBottom, item }) {
  ensurePdfSpace(doc, 80, margin, contentBottom);
  doc.moveDown(0.25);

  if (!pricing?.available || !pricing.analysis) {
    doc
      .font("Helvetica-Oblique")
      .fontSize(10)
      .fillColor("#666")
      .text(pricingUnavailableHeadline(pricing), margin, doc.y, { width: contentWidth });
    doc.moveDown(0.4);
    return;
  }

  const { comparableComps: comps, suggestedPricing: suggested, reviewerGuidance } = pricing.analysis;
  const labels = getSuggestedPricingLabels(suggested);
  const researchRows = getResearchFindingRows(suggested, comps, pricing.analysis?.compEvidence);
  const guidanceRows = getReviewerGuidanceRows(reviewerGuidance);
  const customerRows = getCustomerInputRows(suggested, item);
  const match = getImageMatchConfidence(pricing.analysis);
  const summary = buildFinalPricingSummary({
    suggested,
    item,
    reviewerGuidance,
    analysis: pricing.analysis,
  });

  const fromRetailBand =
    reviewerGuidance?.impliedAskLow != null && reviewerGuidance?.impliedAskHigh != null
      ? `${formatUsd(reviewerGuidance.impliedAskLow)} - ${formatUsd(reviewerGuidance.impliedAskHigh)}`
      : null;
  const rangeText =
    suggested.rangeLow != null && suggested.rangeHigh != null
      ? `${formatUsd(suggested.rangeLow)} - ${formatUsd(suggested.rangeHigh)}`
      : fromRetailBand || "Pending research comps";

  // --- Price first ---
  ensurePdfSpace(doc, 58, margin, contentBottom);
  const boxY = doc.y;
  doc.save();
  doc.rect(margin, boxY, contentWidth, 50).fillAndStroke("#e7f0ed", brandColor);
  doc.restore();
  doc.font("Helvetica-Bold").fontSize(9).fillColor(brandColor);
  doc.text("SUGGESTED LIST PRICE", margin + 10, boxY + 7, { width: contentWidth - 20 });
  doc.font("Helvetica").fontSize(8).fillColor("#555");
  doc.text(labels.rangeLabel, margin + 10, boxY + 18, { width: contentWidth - 20 });
  doc.font("Helvetica-Bold").fontSize(18).fillColor(brandColor);
  doc.text(rangeText, margin + 10, boxY + 28, { width: contentWidth - 20 });
  doc.y = boxY + 56;

  doc.font("Helvetica-Bold").fontSize(9.5).fillColor("#333");
  doc.text(`Seller velocity: ${suggested.velocityLabel || "Standard Seller"}`, margin, doc.y, {
    width: contentWidth,
  });
  doc.moveDown(0.25);

  // For the person reviewing this consignment submission
  doc
    .font("Helvetica-Bold")
    .fontSize(10)
    .fillColor(brandColor)
    .text("Based on these factors and what we found", margin);
  doc.moveDown(0.1);
  doc.font("Helvetica").fontSize(9.5).fillColor("#333").text(summary, margin, doc.y, {
    width: contentWidth,
    lineGap: 2.5,
  });
  doc.moveDown(0.3);

  const drawRow = (label, value) => {
    doc.font("Helvetica-Bold").fontSize(9).fillColor("#333");
    doc.text(`${label}: `, margin, doc.y, { continued: true, width: contentWidth });
    doc.font("Helvetica").text(String(value), { width: contentWidth });
    doc.moveDown(0.1);
  };

  doc.font("Helvetica-Bold").fontSize(10).fillColor(brandColor).text("Research detail", margin);
  doc.moveDown(0.12);
  for (const row of researchRows) {
    const display =
      row.valueText != null && String(row.valueText).trim()
        ? String(row.valueText)
        : formatUsd(row.value);
    drawRow(row.label, display);
  }
  drawRow("Image match", `${match.label} — ${match.detail}`);
  if (guidanceRows.length && (pricing.analysis?.compEvidence?.linkedRetailCount ?? 0) < 1) {
    doc.moveDown(0.08);
    doc.font("Helvetica-Oblique").fontSize(9).fillColor("#666").text("Category context (no verified retail):", margin);
    doc.moveDown(0.08);
    for (const row of guidanceRows) {
      const display =
        row.valueText != null && String(row.valueText).trim()
          ? String(row.valueText)
          : formatUsd(row.value);
      drawRow(row.label, display);
    }
  }

  doc.moveDown(0.2);
  doc.font("Helvetica-Bold").fontSize(10).fillColor(brandColor).text("What the consignor said", margin);
  doc.moveDown(0.1);
  for (const row of customerRows) {
    const display =
      row.valueText != null && String(row.valueText).trim()
        ? String(row.valueText)
        : formatUsd(row.value);
    drawRow(row.label, display);
  }

  doc.moveDown(0.25);
  doc.font("Helvetica-Bold").fontSize(10).fillColor(brandColor).text("Links used in research", margin);
  doc.moveDown(0.12);
  const groups = pricing.analysis.categorizedLinks || {
    retail: [],
    resale: [],
    customer: [],
    visual: [],
  };
  doc.font("Helvetica-Bold").fontSize(9).fillColor(brandColor).text("Strong image matches (Google Vision)", margin);
  doc.moveDown(0.08);
  drawPdfLinkGroup(doc, groups.visual, { margin, contentWidth, contentBottom });
  doc.font("Helvetica-Bold").fontSize(9).fillColor(brandColor).text("Retail comps (priced + live)", margin);
  doc.moveDown(0.08);
  drawPdfLinkGroup(doc, groups.retail, { margin, contentWidth, contentBottom });
  doc.font("Helvetica-Bold").fontSize(9).fillColor(brandColor).text("Resale context", margin);
  doc.moveDown(0.08);
  drawPdfLinkGroup(doc, groups.resale, { margin, contentWidth, contentBottom });
  doc.font("Helvetica-Bold").fontSize(9).fillColor(brandColor).text("Customer / reference", margin);
  doc.moveDown(0.08);
  drawPdfLinkGroup(doc, groups.customer, { margin, contentWidth, contentBottom });

  doc.moveDown(0.15);
  doc.font("Helvetica").fontSize(8).fillColor("#888").text(`Analysis: ${EMAIL_ANALYSIS_LABEL}`, margin, doc.y, {
    width: contentWidth,
  });
  doc.moveDown(0.35);
}
