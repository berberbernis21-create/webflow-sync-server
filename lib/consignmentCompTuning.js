/**
 * Consignment comps: retail prices anchor the 30–50% ask; resale listings are supporting context only.
 */
import {
  classifyLinkChannel,
  compSourceTierScore,
  getCompSourceTier,
  isCredibleCompSource,
  isJunkCompSource,
} from "./consignmentPricingDisplay.js";
import { itemCategoryIsHandbag } from "./consignmentBrand.js";
import { resolvePhotoBundle, shouldShowMultiPieceCallout } from "./consignmentMultiPiece.js";

const MIN_LINKED_RESALE_CONTEXT = Math.max(
  1,
  parseInt(process.env.CONSIGNMENT_MIN_LINKED_COMPS || "2", 10) || 2
);

const RETAIL_ANCHOR_LOW = 0.3;
const RETAIL_ANCHOR_HIGH = 0.5;

/**
 * Parse free-text money. "$40k" / "40K" / "1.5k" → thousands; "40,000" → 40000.
 * Never treat a trailing "k" as noise (that produced $40 instead of $40,000).
 */
export function parseMoneyUsd(rawInput) {
  let raw = String(rawInput ?? "").trim();
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

export function parseConsignorRetailUsd(item) {
  return parseMoneyUsd(item?.originalPrice);
}

/** True when consignor provided proof/receipt (handbags or free-text proof fields). */
export function itemHasPurchaseProof(item) {
  const proof = String(item?.proof ?? item?.proofOfPurchase ?? "").trim().toLowerCase();
  if (!proof) return false;
  if (["no", "n", "none", "false", "0", "na", "n/a", "-"].includes(proof)) return false;
  return true;
}

export function computeCustomerImpliedAsk(consignorRetail) {
  const retail = Math.round(Number(consignorRetail));
  if (!Number.isFinite(retail) || retail <= 0) return null;
  return {
    customerStatedRetail: retail,
    customerImpliedAskLow: Math.round(retail * RETAIL_ANCHOR_LOW),
    customerImpliedAskHigh: Math.round(retail * RETAIL_ANCHOR_HIGH),
  };
}

function safeHttpUrl(url) {
  const u = String(url || "").trim();
  return u.startsWith("http://") || u.startsWith("https://") ? u : null;
}

/**
 * Normalize common consignor brand typos into search aliases.
 * e.g. "Mitchel and Gold" → Mitchell Gold + Bob Williams
 */
export function normalizeBrandAliases(brandRaw) {
  const raw = String(brandRaw || "").trim();
  if (!raw) return { canonical: "", searchBrand: "", aliases: [] };
  const lower = raw.toLowerCase().replace(/\+/g, " ").replace(/[&]/g, " ").replace(/\s+/g, " ").trim();

  if (
    /mitchel+l?\s*and\s*gold|mitchel+l?\s*gold|mitchell\s*gold|mg\s*bw|bob\s*williams/.test(lower)
  ) {
    return {
      canonical: "Mitchell Gold + Bob Williams",
      searchBrand: "Mitchell Gold Bob Williams",
      aliases: [
        "mitchell gold",
        "mitchell gold bob williams",
        "mitchell gold + bob williams",
        "mgbw",
        "bob williams",
        "mitchel and gold",
        "mitchel gold",
        "mitchel & gold",
      ],
    };
  }

  return {
    canonical: raw,
    searchBrand: raw,
    aliases: [lower, ...lower.split(/\s+/).filter((w) => w.length >= 4)].slice(0, 6),
  };
}

export function brandAppearsInText(brandRaw, text) {
  const blob = String(text || "").toLowerCase();
  if (!blob) return false;
  const { aliases, searchBrand, canonical } = normalizeBrandAliases(brandRaw);
  for (const a of [canonical, searchBrand, ...aliases]) {
    const al = String(a || "")
      .toLowerCase()
      .replace(/\+/g, " ")
      .replace(/\s+/g, " ")
      .trim();
    if (al && blob.includes(al)) return true;
  }
  // Fuzzy: mitchell/mitchel + gold both present (covers "Mitchel and Gold" vs listing text).
  if (/mitchel+l?/.test(String(brandRaw || "").toLowerCase()) && /mitchel+l?/.test(blob) && /\bgold\b/.test(blob)) {
    return true;
  }
  const words = String(brandRaw || "")
    .toLowerCase()
    .split(/[^a-z0-9]+/)
    .filter((w) => w.length >= 4 && !/^(and|with|from|the|furniture|corporation|company|inc)$/.test(w));
  if (words.length >= 2 && words.every((w) => blob.includes(w))) return true;
  if (words.length === 1 && blob.includes(words[0])) return true;
  return false;
}

/**
 * Product family / material gates so a leather bar stool never comps to a lamp or rush chair.
 */
const PRODUCT_FAMILY_PATTERNS = [
  {
    family: "lighting",
    pattern: /\b(lamps?|lighting|chandelier|sconces?|pendant\s*light|floor\s*lamp|table\s*lamp)\b/i,
  },
  {
    family: "seating",
    pattern:
      /\b(stools?|bar\s*stools?|counter\s*stools?|chairs?|sofa|sectional|loveseat|bench(es)?|ottoman|settee|seating)\b/i,
  },
  {
    family: "tables",
    pattern: /\b(dining\s*table|coffee\s*table|side\s*table|console\s*table|end\s*table|desk)\b/i,
  },
  {
    family: "storage",
    pattern: /\b(dresser|cabinet|sideboard|buffet|bookcase|armoire|chest)\b/i,
  },
  {
    family: "rugs",
    pattern: /\b(rugs?|carpet|runner)\b/i,
  },
  {
    family: "art",
    pattern: /\b(painting|artwork|print|sculpture|canvas|gallery)\b/i,
  },
];

export function detectProductFamily(text) {
  const blob = String(text || "").toLowerCase();
  if (!blob) return null;
  for (const row of PRODUCT_FAMILY_PATTERNS) {
    if (row.pattern.test(blob)) return row.family;
  }
  return null;
}

function itemEvidenceBlob(item) {
  const visionHints = Array.isArray(item?._visionHints)
    ? item._visionHints.join(" ")
    : String(item?._visionHints || "");
  return [
    item?.itemName,
    item?.category,
    item?.brand,
    item?.notes,
    item?.conditionNotes,
    item?.material,
    item?.materials,
    visionHints,
  ]
    .map((s) => String(s || "").trim())
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
}

/** Hard reject clearly wrong product types / materials. */
export function listingConflictsWithItem(item, title, snippet, url) {
  const itemBlob = itemEvidenceBlob(item);
  // Prefer title+url for matching — CSE snippets often echo our query words and create false positives.
  const listingBlob = `${title || ""} ${url || ""}`.toLowerCase();
  if (!itemBlob || !listingBlob) return false;

  const itemFamily = detectProductFamily(itemBlob);
  const listingFamily = detectProductFamily(listingBlob);
  if (itemFamily && listingFamily && itemFamily !== listingFamily) return true;

  // Leather seating must not match rush/cane/wicker-only listings.
  if (
    itemFamily === "seating" &&
    /\bleather\b/.test(itemBlob) &&
    /\b(rush\s*seat|cane\s*seat|rattan|wicker|woven\s*seat)\b/.test(listingBlob) &&
    !/\bleather\b/.test(listingBlob)
  ) {
    return true;
  }

  // Bar/counter stools should not match dining/side chairs unless stool cues OR strong shared style materials.
  if (
    /\b(bar\s*stools?|counter\s*stools?)\b/.test(itemBlob) &&
    /\bchairs?\b/.test(listingBlob) &&
    !/\b(stools?|bar\s*stool|counter\s*stool)\b/.test(listingBlob)
  ) {
    const sharedStyle =
      /\b(leather|whipstitch|wrought\s*iron|brutalist|ilana\s*goor|faux\s*bamboo)\b/.test(
        listingBlob
      ) &&
      /\b(leather|iron|whipstitch|brutalist|bamboo)\b/.test(itemBlob);
    if (!sharedStyle) return true;
  }

  return false;
}

export function listingMatchesItem(item, title, snippet, url) {
  const brand = String(item?.brand || "").trim();
  const name = String(item?.itemName || "").trim().toLowerCase();
  const category = String(item?.category || "").trim().toLowerCase();
  // Title + URL only — do not use CSE snippet (it often contains our own query terms).
  const blob = `${title || ""} ${url || ""}`.toLowerCase();
  if (!blob) return false;

  if (listingConflictsWithItem(item, title, snippet, url)) return false;

  // If consignor gave a brand, comps MUST reflect that brand.
  if (brand) {
    return brandAppearsInText(brand, blob);
  }

  const itemBlob = itemEvidenceBlob(item);
  const itemFamily = detectProductFamily(itemBlob);
  const listingFamily = detectProductFamily(blob);
  if (itemFamily && listingFamily && itemFamily !== listingFamily) return false;

  // Require same seating subtype cues when present.
  if (/\bleather\b/.test(itemBlob) && !/\bleather\b/.test(blob)) {
    // Allow only if still clearly same stool/chair family AND iron/metal/whipstitch cues match.
    const hasSharedStructure =
      /\b(stool|bar\s*stool|counter\s*stool|whipstitch|wrought\s*iron|brutalist|ilana\s*goor)\b/.test(
        blob
      );
    if (!hasSharedStructure) return false;
  }

  const stop = new Set([
    "with",
    "from",
    "custom",
    "upholstered",
    "piece",
    "pieces",
    "item",
    "items",
    "pair",
    "set",
    "made",
    "high",
    "end",
    "sofa",
    "sectional",
    "chair",
    "chairs",
    "table",
    "accent",
    "rustic",
    "vintage",
    "modern",
    "contemporary",
  ]);
  const nouns = `${name} ${category}`
    .split(/[^a-z0-9]+/i)
    .map((w) => w.toLowerCase())
    .filter((w) => w.length >= 4 && !stop.has(w));
  const hit = nouns.filter((w) => blob.includes(w)).length;
  if (hit >= 2) return true;

  // Strong single noun / style match for same product family.
  if (itemFamily && listingFamily === itemFamily) {
    if (/\bleather\b/.test(itemBlob) && !/\bleather\b/.test(blob)) {
      // Leather submission: listing must show leather OR a known style synonym with iron/metal seating.
      if (!/\b(whipstitch|wrought\s*iron|brutalist|ilana\s*goor)\b/.test(blob)) return false;
    }
    if (hit >= 1) return true;
    if (
      /\b(stools?|bar\s*stool|counter\s*stool|sofa|sectional|ottoman|ilana\s*goor|brutalist)\b/.test(
        blob
      )
    ) {
      return true;
    }
  }
  return false;
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
    // Price anchors require a live OK link. Strong Vision matches without live/price stay as evidence only.
    if (meta?.linkVerified !== true) return;
    if (looksLikeSaleSnippet(meta?.snippet)) return;
    const strongVision = Boolean(meta?.strongVisionMatch || meta?.fromVision);
    if (!strongVision && !isCredibleCompSource(url, meta?.title, meta?.snippet, channel)) return;
    if (strongVision && isJunkCompSource(url, meta?.title, meta?.snippet)) return;
    const p = Math.round(Number(price));
    if (!Number.isFinite(p) || p < 25) return;
    if (
      channel === "resale" &&
      consignorRetail &&
      (p > consignorRetail * 1.02 || p < consignorRetail * 0.12)
    ) {
      return;
    }
    // Always require a real product match — never accept a lamp as a stool "context".
    if (!listingMatchesItem(item, meta?.title, meta?.snippet, url)) return;
    const prev = byUrl.get(url);
    if (!prev || prev.price == null) {
      byUrl.set(url, {
        price: p,
        title: meta?.title || url,
        url,
        snippet: meta?.snippet || "",
        channel,
        strongVisionMatch: strongVision,
        linkVerified: true,
      });
    }
  };

  for (const row of searchResults || []) {
    const url = safeHttpUrl(row.url);
    if (!url) continue;
    const strongVision = Boolean(row.strongVisionMatch || row.fromVision);
    const channelGuess = classifyLinkChannel(url, row.title, row.snippet);
    // Strong Vision matches with unclear channel can still feed retail when priced + live.
    if (channelGuess !== channel) {
      if (!(strongVision && channel === "retail" && channelGuess == null)) continue;
    }
    if (row.linkVerified !== true) continue;
    const meta = {
      title: row.title,
      url,
      snippet: row.snippet,
      fromVision: Boolean(row.fromVision),
      strongVisionMatch: strongVision,
      linkVerified: row.linkVerified,
    };
    if (row.priceHint != null) consider(row.priceHint, meta);
    for (const p of row.prices || []) consider(p, meta);
  }

  for (const s of sources || []) {
    if (String(s?.channel || "").toLowerCase() !== channel) continue;
    if (s.price == null) continue;
    if (s.linkVerified !== true) continue;
    consider(s.price, {
      title: s.title,
      url: s.url,
      snippet: "",
      fromVision: Boolean(s.strongVisionMatch),
      strongVisionMatch: Boolean(s.strongVisionMatch),
      linkVerified: s.linkVerified,
    });
  }

  const allEntries = [...byUrl.values()].filter((e) => e.url && e.price != null);
  const filterFn = channel === "resale" ? filterResalePricesForItem : filterRetailPricesForItem;
  const filterArg = channel === "resale" ? consignorRetail : undefined;
  const survivingPrices = filterFn(
    allEntries.map((e) => e.price),
    filterArg
  );
  let entries = allEntries.filter((e) => survivingPrices.includes(e.price));

  // Prefer preferred/known retail hosts when we have enough — but never drop strong Vision matches.
  if (channel === "retail" && entries.length > 2) {
    const preferred = entries.filter((e) => {
      if (e.strongVisionMatch) return true;
      const tier = getCompSourceTier(e.url, e.title, e.snippet);
      return tier === "preferred_retail" || tier === "known_retail";
    });
    if (preferred.length >= 2) entries = preferred;
  }

  entries.sort((a, b) => {
    const ta = compSourceTierScore(getCompSourceTier(a.url, a.title, a.snippet));
    const tb = compSourceTierScore(getCompSourceTier(b.url, b.title, b.snippet));
    return tb - ta;
  });

  return {
    prices: entries.map((e) => e.price),
    entries,
    consignorRetail,
  };
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

export function resolveCompConfidence({ retailCount = 0, hasProof = false } = {}) {
  const n = Math.max(0, Math.floor(Number(retailCount) || 0));
  // High only when purchase proof/receipt is present AND we have verified retail comps.
  if (hasProof && n >= 2) return "high";
  if (n >= 3) return "medium";
  if (n >= 1) return "low";
  return "low";
}

export function computeRetailCompStats(prices, { hasProof = false } = {}) {
  if (!prices?.length) return null;
  const sorted = [...prices].sort((a, b) => a - b);
  const avg = Math.round(sorted.reduce((s, p) => s + p, 0) / sorted.length);
  return {
    average: avg,
    low: sorted[0],
    high: sorted[sorted.length - 1],
    medium: sorted[Math.floor(sorted.length / 2)],
    confidence: resolveCompConfidence({ retailCount: sorted.length, hasProof }),
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

/**
 * Research-only retail estimate. Never blend or substitute consignor stated retail —
 * that stays on a separate "customer input" line in the email/PDF.
 */
function resolveRetailEstimate(onlineRetailEntries, { hasProof = false } = {}) {
  const onlinePrices = onlineRetailEntries.map((e) => e.price);
  if (onlinePrices.length >= 2) {
    const stats = computeRetailCompStats(onlinePrices, { hasProof });
    return {
      retailEstimate: stats.average,
      anchor: "retail_30_50",
      stats,
      source: "linked_retail_listings",
    };
  }
  if (onlinePrices.length === 1) {
    const online = onlinePrices[0];
    return {
      retailEstimate: online,
      anchor: "retail_30_50",
      stats: computeRetailCompStats([online], { hasProof }),
      source: "linked_retail_listings",
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
    `Suggested list price for ${label}: $${range.rangeLow.toLocaleString("en-US")}–$${range.rangeHigh.toLocaleString("en-US")}, based on these factors and what we found (30–50% of retail estimate $${retailEstimate.toLocaleString("en-US")}).`,
  ];

  if (retailEntries.length) {
    parts.push(
      `Retail comps: ${retailEntries.length} linked new/full retail listing${retailEntries.length === 1 ? "" : "s"} (average $${retailStats.average.toLocaleString("en-US")}; high $${retailStats.high.toLocaleString("en-US")}, low $${retailStats.low.toLocaleString("en-US")}).`
    );
  } else {
    parts.push(
      "No linked online retail listings with confirmed prices — confirm retail manually (consignor-stated retail is shown separately and is not the research retail)."
    );
  }

  if (resaleEntries.length) {
    const resalePrices = resaleEntries.map((e) => e.price);
    const resaleAvg =
      resalePrices.length >= 2
        ? Math.round(resalePrices.reduce((s, p) => s + p, 0) / resalePrices.length)
        : resalePrices[0];
    parts.push(
      `${resaleEntries.length} linked resale listing${resaleEntries.length === 1 ? "" : "s"} for context only (avg ~$${resaleAvg.toLocaleString("en-US")}) — not used as the retail basis.`
    );
  } else {
    parts.push("No verified resale listings found for context.");
  }

  if (consignorRetail) {
    const implied = computeCustomerImpliedAsk(consignorRetail);
    parts.push(
      `Consignor-stated retail ($${consignorRetail.toLocaleString("en-US")}) is shown separately — 30–50% of that would be $${implied.customerImpliedAskLow.toLocaleString("en-US")}–$${implied.customerImpliedAskHigh.toLocaleString("en-US")}; do not treat as verified research.`
    );
  }

  if (retailEntries.length < 2) {
    parts.push("Few retail comps — confirm listings manually before quoting the consignor.");
  }

  return parts.join(" ");
}

function applyResaleOnlyFallback(analysis, item, resaleEntries) {
  const multi = shouldShowMultiPieceCallout(analysis?.photoBundle);
  // Multi-piece / art-style bundles: do not invent a single blended ask from thin resale.
  if (multi) {
    analysis.comparableComps = {
      average: null,
      high: null,
      medium: null,
      low: null,
      confidence: "low",
    };
    analysis.suggestedPricing.pricingAnchor = null;
    analysis.suggestedPricing.rangeLow = null;
    analysis.suggestedPricing.rangeHigh = null;
    analysis.suggestedPricing.retailEstimate = null;
    analysis.verifiedRetailCompEntries = [];
    analysis.verifiedResaleCompEntries = resaleEntries;
    analysis.verifiedCompEntries = [];
    analysis.compEvidence = {
      mode: "insufficient",
      linkedRetailCount: 0,
      linkedResaleCount: resaleEntries.length,
      minRequired: MIN_LINKED_RESALE_CONTEXT,
      multiPiece: true,
    };
    const n = analysis.photoBundle?.distinctItemCount || 2;
    analysis.suggestedPricing.rationale = [
      `Confident multi-piece submission (${n} distinct pieces) — price recommendation withheld; do not blend into one range.`,
      resaleEntries.length
        ? `${resaleEntries.length} directional resale link${resaleEntries.length === 1 ? "" : "s"} kept for reviewer context only.`
        : "No credible linked resale listings found.",
      "Analyze and price each piece separately after identification.",
    ].join(" ");
    return analysis;
  }

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
function attachCustomerStatedPricing(analysis, consignorRetail) {
  const implied = computeCustomerImpliedAsk(consignorRetail);
  if (!implied) {
    analysis.suggestedPricing.customerStatedRetail = null;
    analysis.suggestedPricing.customerImpliedAskLow = null;
    analysis.suggestedPricing.customerImpliedAskHigh = null;
    return;
  }
  analysis.suggestedPricing.customerStatedRetail = implied.customerStatedRetail;
  analysis.suggestedPricing.customerImpliedAskLow = implied.customerImpliedAskLow;
  analysis.suggestedPricing.customerImpliedAskHigh = implied.customerImpliedAskHigh;
}

/** Keep LLM brand/category guidance for reviewers; never promote it to market-rec anchor. */
function finalizeReviewerGuidance(analysis, item, { hasExactRetail = false } = {}) {
  const g = analysis.reviewerGuidance && typeof analysis.reviewerGuidance === "object"
    ? { ...analysis.reviewerGuidance }
    : {};
  const brand = String(item?.brand || "").trim();
  const category = String(item?.category || "").trim();
  const name = String(item?.itemName || "").trim();
  if (!g.basis) {
    g.basis = [brand, category || name].filter(Boolean).join(" / ") || "similar category";
  }
  if (!hasExactRetail) {
    g.noExactRetailComps = true;
  }
  if (
    g.typicalRetailLow != null &&
    g.typicalRetailHigh != null &&
    (g.impliedAskLow == null || g.impliedAskHigh == null)
  ) {
    g.impliedAskLow = Math.round(Number(g.typicalRetailLow) * RETAIL_ANCHOR_LOW);
    g.impliedAskHigh = Math.round(Number(g.typicalRetailHigh) * RETAIL_ANCHOR_HIGH);
  }
  analysis.reviewerGuidance = g;

  if (!hasExactRetail && g.typicalRetailLow != null && g.typicalRetailHigh != null) {
    const band = `$${Number(g.typicalRetailLow).toLocaleString("en-US")}–$${Number(g.typicalRetailHigh).toLocaleString("en-US")}`;
    const ask =
      g.impliedAskLow != null && g.impliedAskHigh != null
        ? ` Directional Lost & Found ask from that band: $${Number(g.impliedAskLow).toLocaleString("en-US")}–$${Number(g.impliedAskHigh).toLocaleString("en-US")} (reviewer starting point only).`
        : "";
    const guidanceLine = `No exact linked retail comps verified — items in this brand/category (${g.basis}) typically retail around ${band}.${ask}`;
    const rationale = String(analysis.suggestedPricing.rationale || "");
    if (!rationale.includes("typically retail around")) {
      analysis.suggestedPricing.rationale = [rationale, guidanceLine, g.notes]
        .filter(Boolean)
        .join(" ")
        .trim();
    }
  }
}

export function tightenPricingAnalysis(analysis, item, searchResults) {
  if (!analysis?.suggestedPricing) return analysis;

  const consignorRetail = parseConsignorRetailUsd(item);
  const hasProof = itemHasPurchaseProof(item);
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

  attachCustomerStatedPricing(analysis, consignorRetail);

  // Re-resolve multi-piece with title heuristics + model output (confidence-gated).
  analysis.photoBundle = resolvePhotoBundle(item, analysis.photoBundle || {}, 0);

  const resolved = resolveRetailEstimate(retailEntries, { hasProof });

  if (!resolved) {
    if (resaleEntries.length >= MIN_LINKED_RESALE_CONTEXT) {
      const out = applyResaleOnlyFallback(analysis, item, resaleEntries);
      attachCustomerStatedPricing(out, consignorRetail);
      finalizeReviewerGuidance(out, item, { hasExactRetail: false });
      if (consignorRetail) {
        out.suggestedPricing.rationale = [
          out.suggestedPricing.rationale,
          `Consignor-stated retail ($${consignorRetail.toLocaleString("en-US")}) is listed separately and was not used as the research retail.`,
        ]
          .filter(Boolean)
          .join(" ");
      }
      return out;
    }
    analysis.comparableComps = {
      average: null,
      high: null,
      medium: null,
      low: null,
      confidence: "low",
    };
    analysis.suggestedPricing.pricingAnchor = null;
    analysis.suggestedPricing.rangeLow = null;
    analysis.suggestedPricing.rangeHigh = null;
    analysis.suggestedPricing.retailEstimate = null;
    analysis.compEvidence = {
      mode: "insufficient",
      linkedRetailCount: retailEntries.length,
      linkedResaleCount: resaleEntries.length,
      minRequired: MIN_LINKED_RESALE_CONTEXT,
    };
    analysis.verifiedRetailCompEntries = retailEntries;
    analysis.verifiedResaleCompEntries = resaleEntries;
    analysis.verifiedCompEntries = retailEntries;
    const multi = shouldShowMultiPieceCallout(analysis.photoBundle);
    analysis.compEvidence.multiPiece = multi;
    analysis.suggestedPricing.rationale = [
      multi
        ? `Confident multi-piece submission (${analysis.photoBundle?.distinctItemCount || "2+"} distinct pieces, confidence ${String(analysis.photoBundle?.mixedItemsConfidence || "medium").toUpperCase()}) — price recommendation withheld; do not blend into one range.`
        : "Insufficient verified retail comps from research — no recommended price until comps are found.",
      consignorRetail
        ? `Consignor-stated retail ($${consignorRetail.toLocaleString("en-US")}) is shown separately (not used as the research retail).`
        : null,
      multi
        ? "Analyze each piece from photos (artist/signature/medium when art). Junk/non-listing search hits were ignored."
        : "See reviewer guidance below for brand/category directional range.",
    ]
      .filter(Boolean)
      .join(" ");
    finalizeReviewerGuidance(analysis, item, { hasExactRetail: false });
    return analysis;
  }

  const range = computeRetailAnchoredRange(resolved.retailEstimate);
  const compPrices = retailEntries.map((e) => e.price);
  const retailStats =
    computeRetailCompStats(compPrices.length ? compPrices : [resolved.retailEstimate], {
      hasProof,
    }) || resolved.stats;

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
    hasPurchaseProof: hasProof,
  };
  finalizeReviewerGuidance(analysis, item, {
    hasExactRetail: retailEntries.length >= 2,
  });
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
