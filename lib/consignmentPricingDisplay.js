/**
 * Shared pricing display for internal team email (blurred $) and internal PDF (full detail).
 */

import { shouldShowMultiPieceCallout } from "./consignmentMultiPiece.js";

/** Shown after "Analysis:" in internal email (and PDF summary footer). */
export const EMAIL_ANALYSIS_LABEL =
  "Multimodal computer vision, embedding-based similarity search, and automated market-comp synthesis";

/** Top of internal team notification. */
export const INTERNAL_EMAIL_PRICING_NOTICE = "Full analysis is in the attached PDF.";

/** Customer-facing labels — retail-anchored 30–50% ask. */
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
  // Legacy anchor kept for old cached analyses — never preferred for new runs.
  if (anchor === "consignor_retail_30_50") {
    return {
      rangeLabel: "Market recommendation (legacy — customer input)",
      anchorLabel: "Customer-stated retail (not research)",
    };
  }
  return {
    rangeLabel: "Market recommendation (research)",
    anchorLabel: "Pricing reference",
  };
}

/** Research findings only (no customer input, no ask). */
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
    rows.push({
      label: "Retail comps found",
      valueText: "None verified",
    });
  }

  if (resaleCount > 0) {
    rows.push({
      label: "Resale context listings",
      valueText: `${resaleCount} (directional only — not the retail anchor)`,
    });
    if (compEvidence?.resaleCompAverage) {
      rows.push({ label: "Resale context average", value: compEvidence.resaleCompAverage });
    }
  } else {
    rows.push({ label: "Resale context listings", valueText: "None verified" });
  }

  return rows;
}

/** Customer-stated input only. */
export function getCustomerInputRows(suggested, item) {
  const rows = [];
  const consignorRetail =
    suggested?.customerStatedRetail != null
      ? Number(suggested.customerStatedRetail)
      : parseConsignorRetailUsd(item);
  if (!Number.isFinite(consignorRetail) || consignorRetail <= 0) {
    rows.push({ label: "Customer-stated retail", valueText: "Not provided" });
    return rows;
  }
  rows.push({ label: "Customer-stated retail", value: consignorRetail });
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
      label: "If we used customer input only (30–50%)",
      valueText: `${formatUsd(impliedLow)} - ${formatUsd(impliedHigh)}`,
    });
  }
  return rows;
}

/** @deprecated Prefer getResearchFindingRows + getCustomerInputRows */
export function getPricingDetailRows(suggested, comps, item, compEvidence) {
  return [
    ...getResearchFindingRows(suggested, comps, compEvidence),
    ...getCustomerInputRows(suggested, item),
  ];
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

/**
 * One closing summary after research + suggestion + customer input.
 */
export function buildFinalPricingSummary({ suggested, comps, item, reviewerGuidance, analysis } = {}) {
  const labels = getSuggestedPricingLabels(suggested);
  const retailCount = analysis?.compEvidence?.linkedRetailCount ?? 0;
  const guidance = reviewerGuidance || {};
  const customerRows = getCustomerInputRows(suggested, item);
  const customerRetail = customerRows.find((r) => r.label === "Customer-stated retail");
  const match = getImageMatchConfidence(analysis);

  const found =
    retailCount > 0
      ? `Research found ${retailCount} verified retail listing${retailCount === 1 ? "" : "s"}${
          suggested?.retailEstimate
            ? ` (retail estimate ${formatUsd(suggested.retailEstimate)})`
            : ""
        }.`
      : guidance.typicalRetailLow != null && guidance.typicalRetailHigh != null
        ? `No verified retail comps — brand/category research points to about ${formatUsd(guidance.typicalRetailLow)}–${formatUsd(guidance.typicalRetailHigh)} typical retail.`
        : "No verified retail comps were found in research.";

  const suggest =
    suggested?.rangeLow != null && suggested?.rangeHigh != null
      ? `Our research-based ask is ${formatUsd(suggested.rangeLow)}–${formatUsd(suggested.rangeHigh)} (${labels.rangeLabel}).`
      : "Our research-based ask is pending until verified comps are confirmed.";

  const customerBit =
    customerRetail?.value != null
      ? `Customer stated ${formatUsd(customerRetail.value)} (input only — not the research anchor).`
      : "Customer did not provide a retail price.";

  return [
    "Based on the research above, image-match confidence, our suggested ask, and what the customer stated:",
    found,
    `Image match confidence: ${match.label} — ${match.detail}.`,
    suggest,
    customerBit,
    "Approach: price from verified retail research at ~30–50% when available; treat customer input as a separate check, not the anchor.",
  ].join(" ");
}

/** Reviewer-only brand/category guidance rows (not the market recommendation). */
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

/**
 * Collapse reviewer guidance + summary into one team-notes block (they often repeat).
 * Prefer structured guidance rows; append only unique summary lines.
 */
export function buildCollapsedTeamNotes({ rationale = "", guidanceNotes = "", hasGuidanceRows = false } = {}) {
  const guide = String(guidanceNotes || "").trim();
  let summary = String(rationale || "").trim();
  if (!guide && !summary) {
    return { showGuidanceSection: false, notesHtmlOrText: "", skipSeparateSummary: true };
  }

  if (guide && summary) {
    // Drop summary sentences that restate typical-retail / directional-ask / customer-stated bands.
    summary = summary
      .replace(/Customer-stated retail[^.]*\./gi, "")
      .replace(/No exact linked retail comps verified[^.]*\./gi, "")
      .replace(/items in this brand\/category[^.]*\./gi, "")
      .replace(/typically retail around[^.]*\./gi, "")
      .replace(/Directional Lost & Found ask from that band[^.]*\./gi, "")
      .replace(/See reviewer guidance below[^.]*\./gi, "")
      .replace(/\s{2,}/g, " ")
      .trim();
    // If what's left is mostly a rehash of guidance notes, drop it.
    const guideHead = guide.slice(0, 80).toLowerCase();
    if (summary && guideHead && summary.toLowerCase().includes(guideHead.slice(0, 40))) {
      summary = "";
    }
  }

  const combined = [guide, summary].filter(Boolean).join(" ");
  return {
    showGuidanceSection: hasGuidanceRows || Boolean(combined),
    notesText: combined,
    skipSeparateSummary: true,
  };
}

/** Rewrite internal anchor language for team-facing summary text. */
export function formatPricingRationaleForDisplay(rationale, suggested) {
  let s = String(rationale || "").trim();
  if (!s) return s;
  s = s.replace(/\bconsignor_retail_30_50\b/gi, "customer-stated retail (legacy — not research)");
  s = s.replace(/\bretail_30_50\b/gi, "online retail reference (30–50% research anchor)");
  s = s.replace(/\bresale_comp_average\b/gi, "resale context only (retail unverified)");
  return s.replace(/\s{2,}/g, " ").trim();
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

/** Blurred dollar values in HTML email (full numbers in attached PDF). */
function blurPriceHtml(formattedValue) {
  const label = escapeHtml(formattedValue);
  return [
    `<span style="display:inline-block;position:relative;">`,
    `<span style="filter:blur(6px);-webkit-filter:blur(6px);color:transparent;text-shadow:0 0 10px rgba(26,60,52,0.45);user-select:none;">${label}</span>`,
    `<span style="position:absolute;left:0;top:0;right:0;bottom:0;" aria-hidden="true"></span>`,
    `</span>`,
  ].join("");
}

function emailSectionTitle(title) {
  return `<p style="margin:14px 0 6px;font-size:13px;font-weight:700;color:#1a3c34;text-transform:uppercase;letter-spacing:0.04em;">${escapeHtml(title)}</p>`;
}

function emailRowsTable(rows, { blurMoney = true, emphasize = false } = {}) {
  if (!rows?.length) return "";
  const parts = [`<table style="border-collapse:collapse;width:100%;font-size:13px;">`];
  for (const row of rows) {
    const display =
      row.valueText != null && String(row.valueText).trim()
        ? String(row.valueText)
        : formatUsd(row.value);
    const looksMoney = /\$|\d/.test(display) && !/none verified|not provided|listing/i.test(display);
    const valueHtml =
      blurMoney && looksMoney ? blurPriceHtml(display) : escapeHtml(display);
    const labelStyle = emphasize
      ? "padding:6px 12px 6px 0;color:#1a3c34;width:48%;font-weight:700;font-size:14px;"
      : "padding:4px 12px 4px 0;color:#555;width:48%;";
    const valueStyle = emphasize
      ? "padding:6px 0;font-weight:700;font-size:16px;color:#1a3c34;"
      : "padding:4px 0;";
    parts.push(
      `<tr><td style="${labelStyle}">${escapeHtml(row.label)}</td><td style="${valueStyle}">${valueHtml}</td></tr>`
    );
  }
  parts.push(`</table>`);
  return parts.join("");
}

function emailSuggestBoxHtml(labels, suggested) {
  const rangeText =
    suggested?.rangeLow != null && suggested?.rangeHigh != null
      ? `${formatUsd(suggested.rangeLow)} - ${formatUsd(suggested.rangeHigh)}`
      : "Pending research comps";
  const hasRange = suggested?.rangeLow != null && suggested?.rangeHigh != null;
  return [
    `<div style="margin:4px 0 10px;padding:12px 14px;background:#e7f0ed;border:2px solid #1a3c34;border-radius:6px;">`,
    `<p style="margin:0 0 4px;font-size:11px;font-weight:700;letter-spacing:0.05em;text-transform:uppercase;color:#1a3c34;">Our suggested ask</p>`,
    `<p style="margin:0 0 2px;font-size:12px;color:#555;">${escapeHtml(labels.rangeLabel)}</p>`,
    `<p style="margin:0;font-size:22px;font-weight:700;line-height:1.25;color:#1a3c34;">${
      hasRange ? blurPriceHtml(rangeText) : escapeHtml(rangeText)
    }</p>`,
    `<p style="margin:8px 0 0;font-size:13px;"><strong>Seller velocity:</strong> ${escapeHtml(
      suggested?.velocityLabel || "Standard Seller"
    )}</p>`,
    `</div>`,
  ].join("");
}

function emailLinksBreakoutHtml(categorizedLinks) {
  const groups = categorizedLinks || { retail: [], resale: [], customer: [] };
  const sections = [
    { key: "retail", title: "Retail comps (pricing anchor)", color: "#1a3c34" },
    { key: "resale", title: "Resale context (directional only)", color: "#1e3a5f" },
    { key: "customer", title: "Customer / reference", color: "#6b4f1d" },
  ];
  const parts = [];
  for (const sec of sections) {
    const links = groups[sec.key] || [];
    parts.push(
      `<p style="margin:10px 0 4px;font-size:12px;font-weight:700;color:${sec.color};">${escapeHtml(sec.title)}</p>`
    );
    if (!links.length) {
      parts.push(`<p style="margin:0 0 6px;font-size:12px;color:#888;font-style:italic;">(none)</p>`);
      continue;
    }
    parts.push(`<ul style="margin:0 0 8px;padding-left:18px;font-size:12px;color:#333;line-height:1.45;">`);
    for (const link of links.slice(0, 12)) {
      const priceBit =
        link.price != null ? ` — ${blurPriceHtml(formatUsd(link.price))}` : "";
      const title = escapeHtml(link.title || link.url || "Link");
      if (link.url) {
        parts.push(
          `<li style="margin:0 0 4px;"><a href="${escapeHtml(link.url)}" target="_blank" rel="noopener noreferrer" style="color:#1a73e8;font-weight:600;text-decoration:underline;">${title}</a>${priceBit}</li>`
        );
      } else {
        parts.push(`<li style="margin:0 0 4px;"><strong>${title}</strong>${priceBit}</li>`);
      }
    }
    parts.push(`</ul>`);
  }
  return parts.join("");
}

/**
 * Internal email: research first → suggestion → customer input → one closing summary.
 */
export function buildInternalEmailPricingHtml(pricing, item = null) {
  if (!pricing?.available || !pricing.analysis) {
    const reason = String(pricing?.reason || "").trim();
    const headline = pricingUnavailableHeadline(pricing);
    const detail = pricingUnavailableDetail(pricing);
    const detailHtml =
      detail && !["budget_exceeded", "item_timeout", "pricing_item_limit"].includes(reason)
        ? `<p style="margin:8px 0 0;font-size:12px;color:#999;line-height:1.45;">${escapeHtml(detail)}</p>`
        : "";
    return `<div style="margin-top:16px;padding:12px;background:#f9f7f4;border-radius:6px;border:1px solid #e8e2d9;"><p style="margin:0;font-size:13px;color:#777;font-style:italic;">${escapeHtml(headline)}</p>${detailHtml}<p style="margin:8px 0 0;font-size:11px;color:#999;">Comparable comps and source links are in the attached PDF.</p></div>`;
  }

  const { comparableComps: comps, suggestedPricing: suggested, photoBundle, reviewerGuidance } =
    pricing.analysis;
  const labels = getSuggestedPricingLabels(suggested);
  const researchRows = getResearchFindingRows(suggested, comps, pricing.analysis?.compEvidence);
  const guidanceRows = getReviewerGuidanceRows(reviewerGuidance);
  const customerRows = getCustomerInputRows(suggested, item);
  const match = getImageMatchConfidence(pricing.analysis);
  const finalSummary = sanitizeEmailSummaryText(
    buildFinalPricingSummary({
      suggested,
      comps,
      item,
      reviewerGuidance,
      analysis: pricing.analysis,
    })
  );
  const showMulti = shouldShowMultiPieceCallout(photoBundle);
  const confLabel = String(photoBundle?.mixedItemsConfidence || "medium").toUpperCase();
  const mixedNote = showMulti
    ? `Multiple distinct pieces detected (${photoBundle.distinctItemCount || "2+"}, confidence ${confLabel}) — see PDF; do not treat as one SKU.`
    : "";

  const parts = [
    `<div style="margin-top:16px;padding:14px;background:#f9f7f4;border-radius:6px;border:1px solid #e8e2d9;">`,
    `<p style="margin:0 0 10px;font-size:12px;color:#777;">Full links and numbers are in the attached PDF.</p>`,
  ];
  if (mixedNote) {
    parts.push(
      `<p style="margin:0 0 10px;font-size:13px;color:#8a3b12;font-weight:600;">${escapeHtml(mixedNote)}</p>`
    );
  }

  // 1) Research findings
  parts.push(emailSectionTitle("1. What we found (research)"));
  parts.push(emailRowsTable(researchRows));
  if (guidanceRows.length && (pricing.analysis?.compEvidence?.linkedRetailCount ?? 0) < 1) {
    parts.push(
      `<p style="margin:8px 0 0;font-size:12px;color:#777;">No verified retail — brand/category context:</p>`,
      emailRowsTable(guidanceRows)
    );
  }

  // 2) Image match
  parts.push(emailSectionTitle("2. Image match confidence"));
  parts.push(
    emailRowsTable([
      { label: "Match confidence", valueText: match.label },
      { label: "Notes", valueText: match.detail },
    ], { blurMoney: false })
  );

  // 3) Our suggestion — bold / easy to scan
  parts.push(emailSectionTitle("3. What we suggest"));
  parts.push(emailSuggestBoxHtml(labels, suggested));

  // 4) Links used (right under suggestion)
  parts.push(emailSectionTitle("4. Links used in research"));
  parts.push(emailLinksBreakoutHtml(pricing.analysis.categorizedLinks));

  // 5) Customer input
  parts.push(emailSectionTitle("5. What the customer said"));
  parts.push(emailRowsTable(customerRows));

  // 6) One closing summary
  parts.push(emailSectionTitle("6. Summary — how we would price it"));
  parts.push(
    `<p style="margin:0;font-size:13px;color:#444;line-height:1.55;">${escapeHtml(finalSummary)}</p>`
  );

  parts.push(
    `<p style="margin:12px 0 0;font-size:11px;color:#999;">Analysis: ${escapeHtml(EMAIL_ANALYSIS_LABEL)}</p>`,
    `</div>`
  );

  return parts.join("");
}

export function buildInternalEmailPricingText(pricing, item = null) {
  if (!pricing?.available || !pricing.analysis) {
    const headline = pricingUnavailableHeadline(pricing);
    const detail = pricingUnavailableDetail(pricing);
    return detail ? `${headline}: ${detail}` : headline;
  }
  const { suggestedPricing: suggested } = pricing.analysis;
  const match = getImageMatchConfidence(pricing.analysis);
  const finalSummary = sanitizeEmailSummaryText(
    buildFinalPricingSummary({
      suggested,
      comps: pricing.analysis.comparableComps,
      item,
      reviewerGuidance: pricing.analysis.reviewerGuidance,
      analysis: pricing.analysis,
    })
  );
  const lines = [
    "Item detail: see attached PDF (internal analysis only).",
    `  Image match: ${match.label} — ${match.detail}`,
    `  Seller velocity: ${suggested.velocityLabel || "Standard Seller"}`,
    finalSummary ? `  Summary: ${finalSummary}` : "",
    `  Analysis: ${EMAIL_ANALYSIS_LABEL}`,
    "  Research links: see attached PDF",
  ];
  return lines.filter(Boolean).join("\n");
}

const PDF_BLOCK_ESTIMATE = 120;

const RESALE_HOST_HINTS = [
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
  "lostandfoundresale",
  "lostandfoundhandbags",
  "craigslist",
];

const RETAIL_HOST_HINTS = [
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
  "ikea.com",
  "ballarddesigns",
  "houzz.com/product",
  "overstock.com",
];

function extractUrlsFromText(text) {
  const re = /https?:\/\/[^\s<>"')]+/gi;
  return [...new Set((String(text || "").match(re) || []).map((u) => u.replace(/[.,;]+$/, "")))];
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
  if (/marketplace|resale|consignment|pre-owned|secondhand|used\s/.test(blob)) {
    return "resale";
  }
  if (/new-in-stock|\/product\/|add-to-cart|retail/.test(blob)) {
    return "retail";
  }
  // Do not default unknown pages (PDFs, blogs, .edu) to "resale".
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

/** Pages that look like comps but are tuition, forums, blogs, etc. */
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
  if (/\.pdf(\?|#|$)/i.test(u) && /tuition|syllabus|academic|fee|credit/.test(blob)) return true;
  if (/\b(tuition|academic year|credit hour|course catalog)\b/.test(blob)) return true;
  if (/\/r\/|reddit\.com|forum|discussion thread/.test(blob)) return true;
  if (/\b(looking for|trying to find|under \$\d+)\b/.test(blob) && /reddit|forum|quora/.test(blob)) {
    return true;
  }
  return false;
}

/**
 * Credible retail/resale listing sources only — unknown hosts need a listing-shaped URL.
 */
export function isCredibleCompSource(url, title = "", snippet = "", channel = null) {
  if (isJunkCompSource(url, title, snippet)) return false;
  const ch = channel || classifyLinkChannel(url, title, snippet);
  const u = String(url || "").toLowerCase();
  const host = hostOf(url);
  if (!host) return false;

  const knownRetail = RETAIL_HOST_HINTS.some((h) => u.includes(h) || host.includes(h.replace(/^\./, "")));
  const knownResale = RESALE_HOST_HINTS.some((h) => u.includes(h) || host.includes(h.replace(/^\./, "")));
  if (ch === "retail" && (knownRetail || looksLikeListingUrl(url))) return true;
  if (ch === "resale" && (knownResale || looksLikeListingUrl(url))) return true;
  // Unknown hosts: only if clearly a product/listing path.
  return looksLikeListingUrl(url);
}

function tokenizeComparableQuery(item) {
  const text = `${item?.itemName || ""} ${item?.brand || ""} ${item?.category || ""}`.toLowerCase();
  return [...new Set(text.split(/[^a-z0-9]+/g).filter((w) => w.length >= 4))].slice(0, 16);
}

function looksLikeListingUrl(url) {
  const u = String(url || "").toLowerCase();
  return (
    /\/(product|products|item|listing|listings|p|shop|store)\/|sku=|variant=|product_id=|listing_id=/.test(
      u
    ) || /facebook\.com\/(marketplace|groups)\//.test(u)
  );
}

function isLikelyComparableSource(entry, item) {
  const url = safeUrl(entry?.url);
  if (!url) return false;
  if (!isCredibleCompSource(url, entry?.title, "", entry?.channel)) return false;

  const matchStrength = String(entry?.matchStrength || "").toLowerCase();
  const isStrong = matchStrength === "strong";

  // Prefer strong visual matches; allow some listing-like URLs through even if not labeled strong.
  if (!isStrong && !looksLikeListingUrl(url)) return false;

  // Basic keyword overlap gate to avoid random/irrelevant pages (e.g. business directories).
  const tokens = tokenizeComparableQuery(item);
  if (!tokens.length) return true;
  const blob = `${entry?.title || ""} ${url}`.toLowerCase();
  const hits = tokens.reduce((n, t) => (blob.includes(t) ? n + 1 : n), 0);
  return hits >= 1;
}

/**
 * Merge consignor context + linked comps used in pricing (honest — no orphan LLM URLs).
 */
export function buildCategorizedLinks({ item, analysis }) {
  const buckets = { resale: [], retail: [], customer: [] };

  for (const entry of analysis?.verifiedRetailCompEntries || []) {
    const url = safeUrl(entry?.url);
    if (!url) continue;
    buckets.retail.push({
      title: entry.title || url,
      url,
      price: entry.price,
      channel: "retail",
      note: "Used in retail comp average / pricing anchor",
      matchStrength: "strong",
    });
  }

  for (const entry of analysis?.verifiedResaleCompEntries || []) {
    const url = safeUrl(entry?.url);
    if (!url) continue;
    buckets.resale.push({
      title: entry.title || url,
      url,
      price: entry.price,
      channel: "resale",
      note: "Directional resale context — not used as pricing anchor",
      matchStrength: "weak",
    });
  }

  const retail = parseConsignorRetailUsd(item);
  if (retail) {
    buckets.customer.push({
      title: "Customer-stated original retail (submission form)",
      url: null,
      price: retail,
      channel: "customer",
      note: "Input only — not used as the research market-recommendation anchor",
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

  for (const key of ["resale", "retail", "customer"]) {
    buckets[key].sort((a, b) => String(a.title).localeCompare(String(b.title)));
  }

  return buckets;
}

function drawPdfLinkGroup(doc, title, links, { brandColor, margin, contentWidth, contentBottom }) {
  if (!links?.length) {
    doc.font("Helvetica-Oblique").fontSize(9).fillColor("#888").text("(none)", margin, doc.y, {
      width: contentWidth,
    });
    doc.moveDown(0.2);
    return;
  }
  doc.font("Helvetica").fontSize(9).fillColor("#333");
  for (const link of links) {
    ensurePdfSpace(doc, 32, margin, contentBottom);
    const pricePart = link.price != null ? ` — ${formatUsd(link.price)}` : "";
    doc.fillColor("#333").font("Helvetica").fontSize(9);
    doc.text(link.title ? `• ${link.title}${pricePart}` : `• ${formatUsd(link.price)}`, margin, doc.y, {
      width: contentWidth,
      lineGap: 1,
    });
    if (link.url) {
      // Make the URL itself explicitly clickable and easy to copy.
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

function drawPdfCategorizedLinks(doc, categorizedLinks, opts, { asSectionTitle = null } = {}) {
  const { brandColor, margin, contentWidth, contentBottom } = opts;
  const groups = categorizedLinks || { resale: [], retail: [], customer: [] };

  doc.moveDown(0.2);
  doc
    .font("Helvetica-Bold")
    .fontSize(10)
    .fillColor(brandColor)
    .text(asSectionTitle || "Links used in research", margin);
  doc.moveDown(0.15);

  doc.font("Helvetica-Bold").fontSize(9.5).fillColor(brandColor).text("Retail comps (pricing anchor)", margin);
  doc.moveDown(0.1);
  drawPdfLinkGroup(doc, "", groups.retail, opts);

  doc.moveDown(0.15);
  doc.font("Helvetica-Bold").fontSize(9.5).fillColor(brandColor).text("Resale context (directional only)", margin);
  doc.moveDown(0.1);
  drawPdfLinkGroup(doc, "", groups.resale, opts);

  doc.moveDown(0.15);
  doc.font("Helvetica-Bold").fontSize(9.5).fillColor(brandColor).text("Customer / reference", margin);
  doc.moveDown(0.1);
  drawPdfLinkGroup(doc, "", groups.customer, opts);
}

function ensurePdfSpace(doc, neededHeight, margin, contentBottom) {
  if (doc.y + neededHeight <= contentBottom) return;
  doc.addPage();
  doc.y = margin;
}

/**
 * Full analysis: research → image match → bold suggestion → links → customer → final summary.
 */
export function drawPdfPricingSection(doc, pricing, { brandColor, margin, contentWidth, contentBottom, item }) {
  ensurePdfSpace(doc, PDF_BLOCK_ESTIMATE, margin, contentBottom);

  doc.moveDown(0.4);
  doc.font("Helvetica-Bold").fontSize(11).fillColor(brandColor).text("Pricing Analysis", margin);
  doc.moveDown(0.25);

  if (!pricing?.available || !pricing.analysis) {
    const headline = pricingUnavailableHeadline(pricing);
    const detail = pricingUnavailableDetail(pricing);
    doc
      .font("Helvetica-Oblique")
      .fontSize(10)
      .fillColor("#666")
      .text(detail ? `${headline}. ${detail}` : headline, margin, doc.y, {
        width: contentWidth,
        lineGap: 2,
      });
    doc.moveDown(0.5);
    return;
  }

  const { comparableComps: comps, suggestedPricing: suggested, photoBundle, reviewerGuidance } =
    pricing.analysis;
  const labels = getSuggestedPricingLabels(suggested);
  const researchRows = getResearchFindingRows(suggested, comps, pricing.analysis?.compEvidence);
  const guidanceRows = getReviewerGuidanceRows(reviewerGuidance);
  const customerRows = getCustomerInputRows(suggested, item);
  const match = getImageMatchConfidence(pricing.analysis);
  const finalSummary = buildFinalPricingSummary({
    suggested,
    comps,
    item,
    reviewerGuidance,
    analysis: pricing.analysis,
  });

  const drawRow = (label, value) => {
    doc.font("Helvetica-Bold").fontSize(9.5).fillColor("#333");
    doc.text(`${label}: `, margin, doc.y, { continued: true, width: contentWidth });
    doc.font("Helvetica").text(String(value), { width: contentWidth });
    doc.moveDown(0.12);
  };

  const drawSection = (title) => {
    ensurePdfSpace(doc, 40, margin, contentBottom);
    doc.moveDown(0.2);
    doc.font("Helvetica-Bold").fontSize(10).fillColor(brandColor).text(title, margin);
    doc.moveDown(0.12);
  };

  if (shouldShowMultiPieceCallout(photoBundle)) {
    const conf = String(photoBundle.mixedItemsConfidence || "medium").toUpperCase();
    doc
      .font("Helvetica-Bold")
      .fontSize(10)
      .fillColor("#8a3b12")
      .text(
        `Multiple distinct pieces detected (${photoBundle.distinctItemCount || "2+"}, confidence ${conf})`,
        margin,
        doc.y,
        { width: contentWidth }
      );
    doc.moveDown(0.15);
  }

  drawSection("1. What we found (research)");
  for (const row of researchRows) {
    const display =
      row.valueText != null && String(row.valueText).trim()
        ? String(row.valueText)
        : formatUsd(row.value);
    drawRow(row.label, display);
  }
  if (guidanceRows.length && (pricing.analysis?.compEvidence?.linkedRetailCount ?? 0) < 1) {
    doc
      .font("Helvetica-Oblique")
      .fontSize(9)
      .fillColor("#666")
      .text("No verified retail — brand/category context:", margin, doc.y, { width: contentWidth });
    doc.moveDown(0.1);
    for (const row of guidanceRows) {
      const display =
        row.valueText != null && String(row.valueText).trim()
          ? String(row.valueText)
          : formatUsd(row.value);
      drawRow(row.label, display);
    }
  }

  drawSection("2. Image match confidence");
  drawRow("Match confidence", match.label);
  drawRow("Notes", match.detail);

  drawSection("3. What we suggest");
  ensurePdfSpace(doc, 56, margin, contentBottom);
  const boxY = doc.y;
  const rangeText =
    suggested.rangeLow != null && suggested.rangeHigh != null
      ? `${formatUsd(suggested.rangeLow)} - ${formatUsd(suggested.rangeHigh)}`
      : "Pending research comps";
  doc.save();
  doc.rect(margin, boxY, contentWidth, 48).fillAndStroke("#e7f0ed", "#1a3c34");
  doc.restore();
  doc.font("Helvetica-Bold").fontSize(9).fillColor("#1a3c34");
  doc.text("OUR SUGGESTED ASK", margin + 10, boxY + 6, { width: contentWidth - 20 });
  doc.font("Helvetica").fontSize(8.5).fillColor("#555");
  doc.text(labels.rangeLabel, margin + 10, boxY + 18, { width: contentWidth - 20 });
  doc.font("Helvetica-Bold").fontSize(16).fillColor("#1a3c34");
  doc.text(rangeText, margin + 10, boxY + 28, { width: contentWidth - 20 });
  doc.y = boxY + 54;
  drawRow("Seller velocity", suggested.velocityLabel || "Standard Seller");

  drawPdfCategorizedLinks(
    doc,
    pricing.analysis.categorizedLinks,
    { brandColor, margin, contentWidth, contentBottom },
    { asSectionTitle: "4. Links used in research" }
  );

  drawSection("5. What the customer said");
  for (const row of customerRows) {
    const display =
      row.valueText != null && String(row.valueText).trim()
        ? String(row.valueText)
        : formatUsd(row.value);
    drawRow(row.label, display);
  }

  drawSection("6. Summary — how we would price it");
  doc.font("Helvetica").fontSize(9.5).fillColor("#333").text(finalSummary, margin, doc.y, {
    width: contentWidth,
    lineGap: 3,
  });

  doc.moveDown(0.2);
  doc
    .font("Helvetica")
    .fontSize(8.5)
    .fillColor("#888")
    .text(`Analysis: ${EMAIL_ANALYSIS_LABEL}`, margin, doc.y, {
      width: contentWidth,
      lineGap: 2,
    });

  doc.moveDown(0.4);
}
