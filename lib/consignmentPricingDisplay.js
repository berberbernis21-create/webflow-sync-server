/**
 * Shared pricing display for internal team email (blurred $) and internal PDF (full detail).
 */


/** Shown after "Analysis:" in internal email (and PDF summary footer). */
export const EMAIL_ANALYSIS_LABEL =
  "Multimodal computer vision, embedding-based similarity search, and automated market-comp synthesis";

/** Top of internal team notification. */
export const INTERNAL_EMAIL_PRICING_NOTICE = "Full analysis is in the attached PDF.";

/** Customer-facing labels — retail-anchored 30–50% ask. */
export function getSuggestedPricingLabels(suggested) {
  const anchor = String(suggested?.pricingAnchor || "retail_30_50").toLowerCase();
  if (anchor === "resale_comp_average") {
    return {
      rangeLabel: "Market recommendation (retail unverified)",
      anchorLabel: "Resale context only — confirm retail",
    };
  }
  if (anchor === "consignor_retail_30_50") {
    return {
      rangeLabel: "Market recommendation (30–50% of retail)",
      anchorLabel: "Consignor stated retail",
    };
  }
  if (anchor === "retail_30_50") {
    return {
      rangeLabel: "Market recommendation (30–50% of retail)",
      anchorLabel: "Retail comp average (verified listings)",
    };
  }
  return {
    rangeLabel: "Market recommendation (30–50% of retail)",
    anchorLabel: "Pricing reference",
  };
}

/** Secondary pricing rows for email/PDF. */
export function getPricingDetailRows(suggested, comps, item, compEvidence) {
  const rows = [];
  const consignorRetail = parseConsignorRetailUsd(item);
  const anchor = String(suggested?.pricingAnchor || "").toLowerCase();
  const retailAnchored = anchor === "retail_30_50" || anchor === "consignor_retail_30_50";
  const resaleContextCount = compEvidence?.linkedResaleCount ?? 0;

  if (retailAnchored && suggested?.retailEstimate) {
    rows.push({
      label: "Retail estimate (pricing anchor)",
      value: suggested.retailEstimate,
    });
  }
  if (retailAnchored && comps?.average) {
    rows.push({
      label: "Retail comp average (linked listings)",
      value: comps.average,
    });
  }
  if (resaleContextCount > 0 && compEvidence?.resaleCompAverage) {
    rows.push({
      label: "Resale context average (not anchor)",
      value: compEvidence.resaleCompAverage,
    });
  }
  if (consignorRetail && anchor !== "consignor_retail_30_50") {
    rows.push({
      label: "Consignor stated retail",
      value: consignorRetail,
    });
  }
  return rows;
}

function parseConsignorRetailUsd(item) {
  const raw = String(item?.originalPrice ?? "").replace(/,/g, "");
  const n = Number(raw.replace(/[^\d.]/g, ""));
  return Number.isFinite(n) && n > 0 ? Math.round(n) : null;
}

/** Rewrite internal anchor language for team-facing summary text. */
export function formatPricingRationaleForDisplay(rationale, suggested) {
  let s = String(rationale || "").trim();
  if (!s) return s;
  s = s.replace(/\bconsignor_retail_30_50\b/gi, "consignor stated retail (30–50% anchor)");
  s = s.replace(/\bretail_30_50\b/gi, "online retail reference (30–50% anchor)");
  s = s.replace(/\bresale_comp_average\b/gi, "resale context only (retail unverified)");
  return s.replace(/\s{2,}/g, " ").trim();
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

/**
 * Internal email: blurred pricing, full summary paragraph, no comp links (see PDF).
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

  const { comparableComps: comps, suggestedPricing: suggested } = pricing.analysis;
  const rationale = sanitizeEmailSummaryText(
    formatPricingRationaleForDisplay(suggested.rationale, suggested)
  );
  const labels = getSuggestedPricingLabels(suggested);
  const detailRows = getPricingDetailRows(suggested, comps, item, pricing.analysis?.compEvidence);

  const parts = [
    `<div style="margin-top:16px;padding:14px;background:#f9f7f4;border-radius:6px;border:1px solid #e8e2d9;">`,
    `<p style="margin:0 0 10px;font-size:12px;color:#777;">Item detail: see attached PDF (internal analysis only).</p>`,
    `<p style="margin:0 0 10px;font-size:13px;font-weight:700;color:#1a3c34;text-transform:uppercase;letter-spacing:0.04em;">Suggested Pricing for Lost &amp; Found</p>`,
    `<table style="border-collapse:collapse;width:100%;font-size:13px;">`,
    `<tr><td style="padding:4px 12px 4px 0;color:#555;">${escapeHtml(labels.rangeLabel)}</td><td style="padding:4px 0;font-weight:600;">${blurPriceHtml(`${formatUsd(suggested.rangeLow)} - ${formatUsd(suggested.rangeHigh)}`)}</td></tr>`,
  ];
  for (const row of detailRows) {
    parts.push(
      `<tr><td style="padding:4px 12px 4px 0;color:#555;">${escapeHtml(row.label)}</td><td style="padding:4px 0;">${blurPriceHtml(formatUsd(row.value))}</td></tr>`
    );
  }
  parts.push(
    `<tr><td style="padding:4px 12px 4px 0;color:#555;">Seller velocity</td><td style="padding:4px 0;">${escapeHtml(suggested.velocityLabel || "Standard Seller")}</td></tr>`,
    `<tr><td style="padding:4px 12px 4px 0;color:#555;">Comp confidence</td><td style="padding:4px 0;">${escapeHtml(capitalizeConfidence(comps.confidence))}</td></tr>`,
    `</table>`,
  );

  if (rationale) {
    parts.push(
      `<p style="margin:14px 0 6px;font-size:13px;font-weight:700;color:#1a3c34;text-transform:uppercase;letter-spacing:0.04em;">Summary</p>`,
      `<p style="margin:0;font-size:13px;color:#444;line-height:1.55;">${escapeHtml(rationale)}</p>`
    );
  }

  parts.push(
    `<p style="margin:12px 0 0;font-size:11px;color:#999;">Analysis: ${escapeHtml(EMAIL_ANALYSIS_LABEL)}</p>`,
    `</div>`
  );

  return parts.join("");
}

export function buildInternalEmailPricingText(pricing) {
  if (!pricing?.available || !pricing.analysis) {
    const headline = pricingUnavailableHeadline(pricing);
    const detail = pricingUnavailableDetail(pricing);
    return detail ? `${headline}: ${detail}` : headline;
  }
  const { suggestedPricing: suggested } = pricing.analysis;
  const lines = [
    "Item detail: see attached PDF (internal analysis only).",
    `  Seller velocity: ${suggested.velocityLabel || "Standard Seller"}`,
    suggested.rationale
      ? `  Summary: ${sanitizeEmailSummaryText(formatPricingRationaleForDisplay(suggested.rationale, suggested))}`
      : "",
    `  Analysis: ${EMAIL_ANALYSIS_LABEL}`,
    "  Comparable comps and source links: see attached PDF",
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
  return "resale";
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
];

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
  const host = hostOf(url);
  if (!host) return false;
  if (EXCLUDED_NON_COMP_HOSTS.includes(host)) return false;
  if (host.endsWith(".googleusercontent.com")) return false;

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
      title: "Consignor stated original retail (submission form)",
      url: null,
      price: retail,
      channel: "customer",
      note:
        analysis?.compEvidence?.mode === "consignor_retail_reference"
          ? "Used as retail anchor when no online retail comps found"
          : "Reference — included in retail estimate when applicable",
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

function drawPdfCategorizedLinks(doc, categorizedLinks, opts) {
  const { brandColor, margin, contentWidth, contentBottom } = opts;
  const groups = categorizedLinks || { resale: [], retail: [], customer: [] };
  const total =
    (groups.resale?.length || 0) +
    (groups.retail?.length || 0) +
    (groups.customer?.length || 0);
  if (!total) return;

  doc.moveDown(0.35);
  doc.font("Helvetica-Bold").fontSize(10).fillColor(brandColor).text("Links Used in Analysis", margin);
  doc.moveDown(0.2);

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
 * Full comps, suggested pricing, summary, and source links (internal PDF only).
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

  const { comparableComps: comps, suggestedPricing: suggested, compEvidence } = pricing.analysis;
  const rationale = formatPricingRationaleForDisplay(suggested.rationale, suggested);
  const retailAnchored =
    compEvidence?.mode === "linked_retail_listings" ||
    compEvidence?.mode === "consignor_retail_reference";

  const drawRow = (label, value) => {
    doc.font("Helvetica-Bold").fontSize(9.5).fillColor("#333");
    doc.text(`${label}: `, margin, doc.y, { continued: true, width: contentWidth });
    doc.font("Helvetica").text(String(value), { width: contentWidth });
    doc.moveDown(0.15);
  };

  doc.font("Helvetica-Bold").fontSize(10).fillColor(brandColor).text("Comparable Comps (retail)", margin);
  doc.moveDown(0.15);
  if (retailAnchored && comps?.average) {
    drawRow("Linked retail listings used", String(compEvidence?.linkedRetailCount ?? "—"));
    drawRow("Retail average", formatUsd(comps.average));
    drawRow("High / Medium / Low", `${formatUsd(comps.high)} / ${formatUsd(comps.medium)} / ${formatUsd(comps.low)}`);
    drawRow("Confidence", capitalizeConfidence(comps.confidence));
    if ((compEvidence?.linkedResaleCount ?? 0) > 0) {
      drawRow(
        "Resale context listings",
        `${compEvidence.linkedResaleCount} (directional only — not averaged into comps)`
      );
    }
  } else if (compEvidence?.mode === "resale_context_only" && comps?.average) {
    doc
      .font("Helvetica-Oblique")
      .fontSize(9.5)
      .fillColor("#666")
      .text(
        "No retail reference found — resale context only below. Confirm retail before using a 30–50% anchor.",
        margin,
        doc.y,
        { width: contentWidth, lineGap: 2 }
      );
    doc.moveDown(0.15);
    drawRow("Resale context average", formatUsd(comps.average));
  } else {
    doc
      .font("Helvetica-Oblique")
      .fontSize(9.5)
      .fillColor("#666")
      .text(
        "No verified retail comps with confirmed prices. See summary and URLs below.",
        margin,
        doc.y,
        { width: contentWidth, lineGap: 2 }
      );
    doc.moveDown(0.15);
  }

  const labels = getSuggestedPricingLabels(suggested);
  doc.moveDown(0.2);
  doc.font("Helvetica-Bold").fontSize(10).fillColor(brandColor).text("Suggested Pricing for Lost & Found", margin);
  doc.moveDown(0.15);
  drawRow(labels.rangeLabel, `${formatUsd(suggested.rangeLow)} - ${formatUsd(suggested.rangeHigh)}`);
  for (const row of getPricingDetailRows(suggested, comps, item, pricing.analysis?.compEvidence)) {
    drawRow(row.label, formatUsd(row.value));
  }
  drawRow("Seller velocity", suggested.velocityLabel || "Standard Seller");

  if (rationale) {
    doc.moveDown(0.25);
    doc.font("Helvetica-Bold").fontSize(10).fillColor(brandColor).text("Summary", margin);
    doc.moveDown(0.1);
    doc.font("Helvetica").fontSize(9.5).fillColor("#333").text(rationale, margin, doc.y, {
      width: contentWidth,
      lineGap: 3,
    });
  }

  drawPdfCategorizedLinks(doc, pricing.analysis.categorizedLinks, {
    brandColor,
    margin,
    contentWidth,
    contentBottom,
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
