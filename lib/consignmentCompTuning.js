/**
 * Tighten handbag resale comps: brand relevance, retail ceiling, outlier removal.
 */
import { classifyLinkChannel } from "./consignmentPricingDisplay.js";
import { itemCategoryIsHandbag } from "./consignmentBrand.js";

export function parseConsignorRetailUsd(item) {
  const raw = String(item?.originalPrice ?? "").replace(/,/g, "");
  const n = Number(raw.replace(/[^\d.]/g, ""));
  return Number.isFinite(n) && n > 0 ? Math.round(n) : null;
}

export function listingMatchesItem(item, title, snippet, url) {
  const brand = String(item?.brand || "").trim().toLowerCase();
  if (!brand) return true;
  const blob = `${title || ""} ${snippet || ""} ${url || ""}`.toLowerCase();
  if (blob.includes(brand)) return true;
  const words = brand.split(/\s+/).filter((w) => w.length >= 3);
  if (words.length >= 2 && words.every((w) => blob.includes(w))) return true;
  return !itemCategoryIsHandbag(item);
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

/** Resale comps must sit below stated/new retail and above junk scrapes. */
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

export function collectVerifiedResaleComps(searchResults, sources, item) {
  const retail = parseConsignorRetailUsd(item);
  const byPrice = new Map();

  const consider = (price, meta) => {
    const p = Math.round(Number(price));
    if (!Number.isFinite(p) || p < 25) return;
    if (retail && (p > retail * 1.02 || p < retail * 0.12)) return;
    if (
      itemCategoryIsHandbag(item) &&
      !listingMatchesItem(item, meta?.title, meta?.snippet, meta?.url)
    ) {
      return;
    }
    if (!byPrice.has(p)) {
      byPrice.set(p, {
        price: p,
        title: meta?.title || meta?.url || "Resale listing",
        url: meta?.url || null,
        channel: "resale",
      });
    }
  };

  for (const row of searchResults || []) {
    if (classifyLinkChannel(row.url, row.title, row.snippet) !== "resale") continue;
    const meta = { title: row.title, url: row.url, snippet: row.snippet };
    if (row.priceHint != null) consider(row.priceHint, meta);
    for (const p of row.prices || []) consider(p, meta);
  }

  for (const s of sources || []) {
    if (String(s?.channel || "").toLowerCase() !== "resale") continue;
    if (s.price != null) {
      consider(s.price, { title: s.title, url: s.url });
    }
  }

  const prices = filterResalePricesForItem([...byPrice.keys()], retail);
  const entries = prices.map((p) => byPrice.get(p)).filter(Boolean);
  return { prices, entries, retail };
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
    confidence: sorted.length >= 5 ? "medium" : "low",
  };
}

/** Handbag ask band: resale comps capped vs consignor retail (typical consignment band ~30–52%). */
export function computeHandbagMarketRecommendation(compStats, retail) {
  if (!compStats?.average) return null;
  const avg = compStats.average;
  if (retail && retail > 0) {
    const bandLow = Math.round(retail * 0.3);
    const bandHigh = Math.round(retail * 0.52);
    const trustedAvg = Math.min(avg, Math.round(retail * 0.58));
    const rangeLow = Math.max(bandLow, Math.min(Math.round(trustedAvg * 0.92), bandHigh));
    const rangeHigh = Math.min(bandHigh, Math.max(Math.round(trustedAvg * 1.08), rangeLow + 1));
    return { rangeLow, rangeHigh };
  }
  return {
    rangeLow: Math.round(avg * 0.9),
    rangeHigh: Math.round(avg * 1.1),
  };
}

function buildTightenedRationale({ item, stats, retail, compCount, excludedNote }) {
  const label = [item?.brand, item?.itemName].filter(Boolean).join(" ").trim() || "this item";
  const parts = [
    `Market recommendation for ${label} uses ${compCount} verified online resale listing price${compCount === 1 ? "" : "s"}.`,
    `Resale comp average: $${stats.average.toLocaleString("en-US")} (high $${stats.high.toLocaleString("en-US")}, low $${stats.low.toLocaleString("en-US")}).`,
  ];
  if (retail) {
    parts.push(`Consignor stated retail $${retail.toLocaleString("en-US")} is shown as reference only.`);
  }
  if (excludedNote) parts.push(excludedNote);
  parts.push("Outliers above retail or unrelated listings were excluded.");
  return parts.join(" ");
}

/**
 * Recompute comps + market recommendation from verified evidence; clamp LLM noise.
 */
export function tightenPricingAnalysis(analysis, item, searchResults) {
  if (!analysis?.suggestedPricing) return analysis;

  const retail = parseConsignorRetailUsd(item);
  const { prices: verifiedPrices, entries } = collectVerifiedResaleComps(
    searchResults,
    analysis.sources,
    item
  );

  const llmPrices = [
    analysis.comparableComps?.low,
    analysis.comparableComps?.medium,
    analysis.comparableComps?.high,
    analysis.comparableComps?.average,
  ].filter((p) => Number.isFinite(p) && p > 0);

  const rawCount = new Set([...verifiedPrices, ...llmPrices]).size;
  const merged =
    verifiedPrices.length >= 2
      ? filterResalePricesForItem(verifiedPrices, retail)
      : filterResalePricesForItem(
          [...new Set([...verifiedPrices, ...llmPrices.map((p) => Math.round(p))])],
          retail
        );

  const excludedNote =
    rawCount > merged.length
      ? `${rawCount - merged.length} price signal(s) dropped (above retail, wrong brand, or outlier).`
      : null;

  if (merged.length >= 2) {
    const stats = computeCompStats(merged, retail);
    const rec = computeHandbagMarketRecommendation(stats, retail);
    analysis.comparableComps = stats;
    analysis.suggestedPricing.pricingAnchor = "resale_comp_average";
    analysis.suggestedPricing.rangeLow = rec.rangeLow;
    analysis.suggestedPricing.rangeHigh = rec.rangeHigh;
    if (retail) analysis.suggestedPricing.retailEstimate = retail;
    analysis.suggestedPricing.rationale = buildTightenedRationale({
      item,
      stats,
      retail,
      compCount: merged.length,
      excludedNote,
    });
    analysis.verifiedCompEntries = entries;
    return analysis;
  }

  if (retail) {
    const low = Math.round(retail * 0.32);
    const high = Math.round(retail * 0.48);
    analysis.comparableComps = {
      average: Math.round((low + high) / 2),
      high,
      low,
      medium: Math.round((low + high) / 2),
      confidence: "low",
    };
    analysis.suggestedPricing.pricingAnchor = "consignor_retail_30_50";
    analysis.suggestedPricing.rangeLow = low;
    analysis.suggestedPricing.rangeHigh = high;
    analysis.suggestedPricing.retailEstimate = retail;
    analysis.suggestedPricing.rationale = [
      `Insufficient verified online resale comps for ${item?.itemName || "this item"}.`,
      `Market recommendation uses consignor stated retail ($${retail.toLocaleString("en-US")}) and typical luxury resale positioning.`,
      excludedNote || "No same-brand resale listings with reliable prices were confirmed.",
    ]
      .filter(Boolean)
      .join(" ");
    analysis.verifiedCompEntries = entries;
  }

  return analysis;
}
