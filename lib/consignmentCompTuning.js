/**
 * Handbag resale comps: only prices backed by real listing URLs count.
 */
import { classifyLinkChannel } from "./consignmentPricingDisplay.js";
import { itemCategoryIsHandbag } from "./consignmentBrand.js";

const MIN_LINKED_COMPS = Math.max(
  2,
  parseInt(process.env.CONSIGNMENT_MIN_LINKED_COMPS || "2", 10) || 2
);

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

/**
 * Collect resale comps only from real URLs with extractable prices (search + LLM sources with url+price).
 */
export function collectVerifiedResaleComps(searchResults, sources, item) {
  const retail = parseConsignorRetailUsd(item);
  const byUrl = new Map();

  const consider = (price, meta) => {
    const url = safeHttpUrl(meta?.url);
    if (!url) return;
    const p = Math.round(Number(price));
    if (!Number.isFinite(p) || p < 25) return;
    if (retail && (p > retail * 1.02 || p < retail * 0.12)) return;
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
        channel: "resale",
      });
    }
  };

  for (const row of searchResults || []) {
    const url = safeHttpUrl(row.url);
    if (!url) continue;
    if (classifyLinkChannel(url, row.title, row.snippet) !== "resale") continue;
    const meta = { title: row.title, url, snippet: row.snippet };
    if (row.priceHint != null) consider(row.priceHint, meta);
    for (const p of row.prices || []) consider(p, meta);
  }

  for (const s of sources || []) {
    if (String(s?.channel || "").toLowerCase() !== "resale") continue;
    if (s.price == null) continue;
    consider(s.price, { title: s.title, url: s.url, snippet: "" });
  }

  const allEntries = [...byUrl.values()].filter((e) => e.url && e.price != null);
  const survivingPrices = filterResalePricesForItem(
    allEntries.map((e) => e.price),
    retail
  );
  const entries = allEntries.filter((e) => survivingPrices.includes(e.price));
  return { prices: survivingPrices, entries, retail };
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

function applyRetailReferenceFallback(analysis, item, retail, linkedEntries, note) {
  const low = Math.round(retail * 0.32);
  const high = Math.round(retail * 0.48);
  analysis.comparableComps = {
    average: null,
    high: null,
    medium: null,
    low: null,
    confidence: "low",
  };
  analysis.suggestedPricing.pricingAnchor = "consignor_retail_30_50";
  analysis.suggestedPricing.rangeLow = low;
  analysis.suggestedPricing.rangeHigh = high;
  analysis.suggestedPricing.retailEstimate = retail;
  analysis.compEvidence = {
    mode: "retail_reference_only",
    linkedListingCount: linkedEntries.length,
    minRequired: MIN_LINKED_COMPS,
  };
  analysis.verifiedCompEntries = linkedEntries;

  const parts = [
    `Not enough linked resale listings with confirmed prices (need at least ${MIN_LINKED_COMPS}; found ${linkedEntries.length}).`,
    `Market recommendation uses consignor stated retail ($${retail.toLocaleString("en-US")}) and typical luxury resale positioning until team verifies comps manually.`,
  ];
  if (linkedEntries.length === 1) {
    const one = linkedEntries[0];
    parts.push(
      `One directional linked listing at $${one.price.toLocaleString("en-US")} — see URL under Resale comps used in pricing (not averaged).`
    );
  }
  if (note) parts.push(note);
  analysis.suggestedPricing.rationale = parts.join(" ");
  return analysis;
}

function buildLinkedCompRationale({ item, stats, retail, linkedEntries }) {
  const label = [item?.brand, item?.itemName].filter(Boolean).join(" ").trim() || "this item";
  const parts = [
    `Market recommendation for ${label} is based on ${linkedEntries.length} linked resale listing${linkedEntries.length === 1 ? "" : "s"} with prices extracted from the page snippet (URLs below).`,
    `Resale comp average: $${stats.average.toLocaleString("en-US")} (high $${stats.high.toLocaleString("en-US")}, low $${stats.low.toLocaleString("en-US")}).`,
  ];
  if (retail) {
    parts.push(`Consignor stated retail $${retail.toLocaleString("en-US")} is reference only, not used as a comp price.`);
  }
  if (linkedEntries.length < 3) {
    parts.push("Few linked comps — confirm listings manually before quoting the consignor.");
  }
  return parts.join(" ");
}

/**
 * Recompute comps from linked listings only. LLM numbers without URLs are ignored.
 */
export function tightenPricingAnalysis(analysis, item, searchResults) {
  if (!analysis?.suggestedPricing) return analysis;

  const retail = parseConsignorRetailUsd(item);
  const { entries: linkedEntries } = collectVerifiedResaleComps(
    searchResults,
    analysis.sources,
    item
  );

  const linkedPrices = linkedEntries.map((e) => e.price);

  if (linkedEntries.length >= MIN_LINKED_COMPS) {
    const stats = computeCompStats(linkedPrices, retail);
    if (!stats) {
      return retail
        ? applyRetailReferenceFallback(analysis, item, retail, linkedEntries, "Linked prices failed validation filters.")
        : analysis;
    }
    const rec = computeHandbagMarketRecommendation(stats, retail);
    analysis.comparableComps = stats;
    analysis.suggestedPricing.pricingAnchor = "resale_comp_average";
    analysis.suggestedPricing.rangeLow = rec.rangeLow;
    analysis.suggestedPricing.rangeHigh = rec.rangeHigh;
    if (retail) analysis.suggestedPricing.retailEstimate = retail;
    analysis.suggestedPricing.rationale = buildLinkedCompRationale({
      item,
      stats,
      retail,
      linkedEntries,
    });
    analysis.verifiedCompEntries = linkedEntries;
    analysis.compEvidence = {
      mode: "linked_resale_listings",
      linkedListingCount: linkedEntries.length,
      minRequired: MIN_LINKED_COMPS,
    };
    return analysis;
  }

  if (retail) {
    return applyRetailReferenceFallback(
      analysis,
      item,
      retail,
      linkedEntries,
      linkedEntries.length
        ? "Additional search results lacked both a usable URL and extractable price."
        : "No same-brand resale listings with both URL and price were found in search."
    );
  }

  analysis.compEvidence = {
    mode: "insufficient",
    linkedListingCount: linkedEntries.length,
    minRequired: MIN_LINKED_COMPS,
  };
  analysis.verifiedCompEntries = linkedEntries;
  analysis.suggestedPricing.rationale =
    "Insufficient linked resale listings and no consignor retail reference — manual comp research required.";
  return analysis;
}
