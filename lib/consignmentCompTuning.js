/**
 * Consignment comps: retail prices anchor the 30–50% ask; resale listings are supporting context only.
 */
import { classifyLinkChannel } from "./consignmentPricingDisplay.js";
import { itemCategoryIsHandbag } from "./consignmentBrand.js";

const MIN_LINKED_RESALE_CONTEXT = Math.max(
  1,
  parseInt(process.env.CONSIGNMENT_MIN_LINKED_COMPS || "2", 10) || 2
);

const RETAIL_ANCHOR_LOW = 0.3;
const RETAIL_ANCHOR_HIGH = 0.5;

export function parseConsignorRetailUsd(item) {
  const raw = String(item?.originalPrice ?? "").replace(/,/g, "");
  const n = Number(raw.replace(/[^\d.]/g, ""));
  return Number.isFinite(n) && n > 0 ? Math.round(n) : null;
}

function safeHttpUrl(url) {
  const u = String(url || "").trim();
  return u.startsWith("http://") || u.startsWith("https://") ? u : null;
}

export function listingMatchesItem(item, title, snippet, url) {
  const brand = String(item?.brand || "").trim().toLowerCase();
  if (!brand) return true;
  const blob = `${title || ""} ${snippet || ""} ${url || ""}`.toLowerCase();
  if (blob.includes(brand)) return true;
  const primary = brand.split(/\s+of\s+/)[0].trim();
  if (primary.length >= 3 && blob.includes(primary)) return true;
  const words = brand.split(/\s+/).filter((w) => w.length >= 3);
  if (words.length >= 2 && words.every((w) => blob.includes(w))) return true;
  if (words.length && words[0].length >= 4 && blob.includes(words[0])) return true;
  return !itemCategoryIsHandbag(item);
}

function looksLikeSaleSnippet(snippet = "") {
  return /\b(clearance|final sale|open box|refurb|outlet|as-is|damaged|scratch and dent)\b/i.test(
    String(snippet || "")
  );
}

export function trimIqrOutliers(sortedPrices) {
  const arr = [...sortedPrices].sort((a, b) => a - b);
  if (arr.length < 4) return arr;
  const q1 = arr[Math.floor(arr.length * 0.25)];
  const q3 = arr[Math.floor(arr.length * 0.75)];
  const iqr = q3 - q1;
  const lo = q1 - 1.5 * iqr;
  const hi = q3 + 1.5 * iqr;
  return arr.filter((p) => p >= lo && p <= hi);
}

export function filterResalePricesForItem(prices, retailCeiling) {
  let out = [...new Set((prices || []).map((p) => Math.round(Number(p))).filter((p) => p >= 25))];
  out.sort((a, b) => a - b);
  if (retailCeiling && retailCeiling > 0) {
    const minP = Math.round(retailCeiling * 0.12);
    const maxP = Math.round(retailCeiling * 1.02);
    out = out.filter((p) => p >= minP && p <= maxP);
  }
  return trimIqrOutliers(out);
}

export function filterRetailPricesForItem(prices) {
  let out = [...new Set((prices || []).map((p) => Math.round(Number(p))).filter((p) => p >= 25))];
  out.sort((a, b) => a - b);
  return trimIqrOutliers(out);
}

function collectVerifiedChannelComps(searchResults, sources, item, channel) {
  const consignorRetail = parseConsignorRetailUsd(item);
  const byUrl = new Map();

  const consider = (price, meta) => {
    const url = safeHttpUrl(meta?.url);
    if (!url) return;
    if (looksLikeSaleSnippet(meta?.snippet)) return;
    const p = Math.round(Number(price));
    if (!Number.isFinite(p) || p < 25) return;
    if (
      channel === "resale" &&
      consignorRetail &&
      (p > consignorRetail * 1.02 || p < consignorRetail * 0.12)
    ) {
      return;
    }
    if (
      itemCategoryIsHandbag(item) &&
      !listingMatchesItem(item, meta?.title, meta?.snippet, url)
    ) {
      return;
    }
    const prev = byUrl.get(url);
    if (!prev || prev.price == null) {
      byUrl.set(url, {
        price: p,
        title: meta?.title || url,
        url,
        snippet: meta?.snippet || "",
        channel,
      });
    }
  };

  for (const row of searchResults || []) {
    const url = safeHttpUrl(row.url);
    if (!url) continue;
    if (classifyLinkChannel(url, row.title, row.snippet) !== channel) continue;
    const meta = { title: row.title, url, snippet: row.snippet };
    if (row.priceHint != null) consider(row.priceHint, meta);
    for (const p of row.prices || []) consider(p, meta);
  }

  for (const s of sources || []) {
    if (String(s?.channel || "").toLowerCase() !== channel) continue;
    if (s.price == null) continue;
    consider(s.price, { title: s.title, url: s.url, snippet: "" });
  }

  const allEntries = [...byUrl.values()].filter((e) => e.url && e.price != null);
  const filterFn = channel === "resale" ? filterResalePricesForItem : filterRetailPricesForItem;
  const filterArg = channel === "resale" ? consignorRetail : undefined;
  const survivingPrices = filterFn(
    allEntries.map((e) => e.price),
    filterArg
  );
  const entries = allEntries.filter((e) => survivingPrices.includes(e.price));
  return { prices: survivingPrices, entries, consignorRetail };
}

/**
 * Linked resale listings — directional context only, not the pricing anchor.
 */
export function collectVerifiedResaleComps(searchResults, sources, item) {
  return collectVerifiedChannelComps(searchResults, sources, item, "resale");
}

/**
 * Linked new/full retail listings — primary input for comparableComps and the 30–50% anchor.
 */
export function collectVerifiedRetailComps(searchResults, sources, item) {
  return collectVerifiedChannelComps(searchResults, sources, item, "retail");
}

export function computeCompStats(prices, retailCeiling = null) {
  if (!prices?.length) return null;
  let sorted = [...prices].sort((a, b) => a - b);
  if (retailCeiling && retailCeiling > 0) {
    sorted = sorted.filter((p) => p <= Math.round(retailCeiling * 1.02));
  }
  if (!sorted.length) return null;
  const avg = Math.round(sorted.reduce((s, p) => s + p, 0) / sorted.length);
  return {
    average: avg,
    low: sorted[0],
    high: sorted[sorted.length - 1],
    medium: sorted[Math.floor(sorted.length / 2)],
    confidence: sorted.length >= 3 ? "medium" : "low",
  };
}

export function computeRetailCompStats(prices) {
  if (!prices?.length) return null;
  const sorted = [...prices].sort((a, b) => a - b);
  const avg = Math.round(sorted.reduce((s, p) => s + p, 0) / sorted.length);
  return {
    average: avg,
    low: sorted[0],
    high: sorted[sorted.length - 1],
    medium: sorted[Math.floor(sorted.length / 2)],
    confidence: sorted.length >= 3 ? "medium" : sorted.length >= 2 ? "low" : "low",
  };
}

/** Lost & Found consignment ask band: 30–50% of credible retail. */
export function computeRetailAnchoredRange(retailEstimate) {
  const retail = Math.round(Number(retailEstimate));
  if (!Number.isFinite(retail) || retail <= 0) return null;
  return {
    rangeLow: Math.round(retail * RETAIL_ANCHOR_LOW),
    rangeHigh: Math.round(retail * RETAIL_ANCHOR_HIGH),
  };
}

function resolveRetailEstimate(onlineRetailEntries, consignorRetail) {
  const onlinePrices = onlineRetailEntries.map((e) => e.price);
  if (onlinePrices.length >= 2) {
    const stats = computeRetailCompStats(onlinePrices);
    return {
      retailEstimate: stats.average,
      anchor: "retail_30_50",
      stats,
      source: "linked_retail_listings",
    };
  }
  if (onlinePrices.length === 1) {
    const online = onlinePrices[0];
    if (consignorRetail) {
      const blended = Math.round((online + consignorRetail) / 2);
      const compPrices = [online, consignorRetail];
      return {
        retailEstimate: blended,
        anchor: "retail_30_50",
        stats: computeRetailCompStats(compPrices),
        source: "linked_retail_listings",
      };
    }
    return {
      retailEstimate: online,
      anchor: "retail_30_50",
      stats: computeRetailCompStats([online]),
      source: "linked_retail_listings",
    };
  }
  if (consignorRetail) {
    return {
      retailEstimate: consignorRetail,
      anchor: "consignor_retail_30_50",
      stats: computeRetailCompStats([consignorRetail]),
      source: "consignor_retail_reference",
    };
  }
  return null;
}

function buildRetailAnchoredRationale({
  item,
  retailStats,
  retailEstimate,
  retailEntries,
  resaleEntries,
  consignorRetail,
  anchor,
}) {
  const label = [item?.brand, item?.itemName].filter(Boolean).join(" ").trim() || "this item";
  const range = computeRetailAnchoredRange(retailEstimate);
  const parts = [
    `Market recommendation for ${label} is anchored at 30–50% of credible retail ($${range.rangeLow.toLocaleString("en-US")}–$${range.rangeHigh.toLocaleString("en-US")} from retail estimate $${retailEstimate.toLocaleString("en-US")}).`,
  ];

  if (retailEntries.length) {
    parts.push(
      `Retail comparable comps: ${retailEntries.length} linked new/full retail listing${retailEntries.length === 1 ? "" : "s"} (average $${retailStats.average.toLocaleString("en-US")}; high $${retailStats.high.toLocaleString("en-US")}, low $${retailStats.low.toLocaleString("en-US")}).`
    );
  } else if (consignorRetail && anchor === "consignor_retail_30_50") {
    parts.push(
      `No linked online retail listings with confirmed prices — using consignor stated retail ($${consignorRetail.toLocaleString("en-US")}) for the anchor.`
    );
  }

  if (resaleEntries.length) {
    const resalePrices = resaleEntries.map((e) => e.price);
    const resaleAvg =
      resalePrices.length >= 2
        ? Math.round(resalePrices.reduce((s, p) => s + p, 0) / resalePrices.length)
        : resalePrices[0];
    parts.push(
      `${resaleEntries.length} linked resale listing${resaleEntries.length === 1 ? "" : "s"} included for directional context only (avg ~$${resaleAvg.toLocaleString("en-US")}) — not used as the pricing anchor.`
    );
  } else {
    parts.push("No verified resale listings found for directional context.");
  }

  if (retailEntries.length < 2 && !consignorRetail) {
    parts.push("Few retail comps — confirm listings manually before quoting the consignor.");
  }

  return parts.join(" ");
}

function applyResaleOnlyFallback(analysis, item, resaleEntries) {
  const resalePrices = resaleEntries.map((e) => e.price);
  const stats = computeCompStats(resalePrices);
  analysis.comparableComps = stats || {
    average: null,
    high: null,
    medium: null,
    low: null,
    confidence: "low",
  };
  analysis.suggestedPricing.pricingAnchor = "resale_comp_average";
  if (stats) {
    analysis.suggestedPricing.rangeLow = Math.round(stats.average * 0.9);
    analysis.suggestedPricing.rangeHigh = Math.round(stats.average * 1.1);
    analysis.suggestedPricing.retailEstimate = null;
  }
  analysis.verifiedRetailCompEntries = [];
  analysis.verifiedResaleCompEntries = resaleEntries;
  analysis.verifiedCompEntries = resaleEntries;
  analysis.compEvidence = {
    mode: "resale_context_only",
    linkedRetailCount: 0,
    linkedResaleCount: resaleEntries.length,
    minRequired: MIN_LINKED_RESALE_CONTEXT,
  };
  const label = [item?.brand, item?.itemName].filter(Boolean).join(" ").trim() || "this item";
  analysis.suggestedPricing.rationale = [
    `No credible retail reference found for ${label}.`,
    stats
      ? `Directional resale context only (${resaleEntries.length} linked listing${resaleEntries.length === 1 ? "" : "s"}; avg $${stats.average.toLocaleString("en-US")}) — team should establish retail before quoting 30–50%.`
      : "Manual retail and resale research required before quoting.",
  ].join(" ");
  return analysis;
}

/**
 * Recompute comps from linked listings only. LLM numbers without URLs are ignored.
 * Retail anchors the ask; resale is supporting context.
 */
export function tightenPricingAnalysis(analysis, item, searchResults) {
  if (!analysis?.suggestedPricing) return analysis;

  const consignorRetail = parseConsignorRetailUsd(item);
  const { entries: retailEntries } = collectVerifiedRetailComps(
    searchResults,
    analysis.sources,
    item
  );
  const { entries: resaleEntries } = collectVerifiedResaleComps(
    searchResults,
    analysis.sources,
    item
  );

  const resolved = resolveRetailEstimate(retailEntries, consignorRetail);

  if (!resolved) {
    if (resaleEntries.length >= MIN_LINKED_RESALE_CONTEXT) {
      return applyResaleOnlyFallback(analysis, item, resaleEntries);
    }
    analysis.compEvidence = {
      mode: "insufficient",
      linkedRetailCount: retailEntries.length,
      linkedResaleCount: resaleEntries.length,
      minRequired: MIN_LINKED_RESALE_CONTEXT,
    };
    analysis.verifiedRetailCompEntries = retailEntries;
    analysis.verifiedResaleCompEntries = resaleEntries;
    analysis.verifiedCompEntries = retailEntries;
    analysis.suggestedPricing.rationale =
      "Insufficient retail reference and no resale context — manual comp research required.";
    return analysis;
  }

  const range = computeRetailAnchoredRange(resolved.retailEstimate);
  const compPrices = retailEntries.map((e) => e.price);
  if (
    consignorRetail &&
    resolved.anchor === "consignor_retail_30_50" &&
    !compPrices.includes(consignorRetail)
  ) {
    compPrices.push(consignorRetail);
  }
  const retailStats =
    computeRetailCompStats(compPrices.length ? compPrices : [resolved.retailEstimate]) ||
    resolved.stats;

  analysis.comparableComps = retailStats;
  analysis.suggestedPricing.pricingAnchor = resolved.anchor;
  analysis.suggestedPricing.rangeLow = range.rangeLow;
  analysis.suggestedPricing.rangeHigh = range.rangeHigh;
  analysis.suggestedPricing.retailEstimate = resolved.retailEstimate;
  analysis.suggestedPricing.rationale = buildRetailAnchoredRationale({
    item,
    retailStats,
    retailEstimate: resolved.retailEstimate,
    retailEntries,
    resaleEntries,
    consignorRetail,
    anchor: resolved.anchor,
  });
  analysis.verifiedRetailCompEntries = retailEntries;
  analysis.verifiedResaleCompEntries = resaleEntries;
  analysis.verifiedCompEntries = retailEntries;
  const resalePrices = resaleEntries.map((e) => e.price);
  analysis.compEvidence = {
    mode: resolved.source,
    linkedRetailCount: retailEntries.length,
    linkedResaleCount: resaleEntries.length,
    resaleCompAverage:
      resalePrices.length >= 2
        ? Math.round(resalePrices.reduce((s, p) => s + p, 0) / resalePrices.length)
        : resalePrices[0] ?? null,
    minRequired: MIN_LINKED_RESALE_CONTEXT,
  };
  return analysis;
}

/** @deprecated Use computeRetailAnchoredRange — kept for import compatibility. */
export function computeHandbagMarketRecommendation(compStats, retail) {
  if (retail && retail > 0) return computeRetailAnchoredRange(retail);
  if (compStats?.average) {
    return {
      rangeLow: Math.round(compStats.average * 0.9),
      rangeHigh: Math.round(compStats.average * 1.1),
    };
  }
  return null;
}
