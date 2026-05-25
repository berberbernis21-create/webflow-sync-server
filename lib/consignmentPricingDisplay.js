/**
 * Shared pricing display for internal team email (blurred $) and internal PDF (full detail).
 */


const EMAIL_ANALYSIS_LABEL =
  "Property Vision, vector, and large-scale language models";

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
export function buildInternalEmailPricingHtml(pricing) {
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
  const rationale = String(suggested.rationale || "").trim();

  const parts = [
    `<div style="margin-top:16px;padding:14px;background:#f9f7f4;border-radius:6px;border:1px solid #e8e2d9;">`,
    `<p style="margin:0 0 6px;font-size:12px;color:#777;">Dollar amounts are hidden here. Open the attached PDF for comparable comps, source links, and exact pricing.</p>`,
    `<p style="margin:0 0 10px;font-size:13px;font-weight:700;color:#1a3c34;text-transform:uppercase;letter-spacing:0.04em;">Suggested Pricing for Lost &amp; Found</p>`,
    `<table style="border-collapse:collapse;width:100%;font-size:13px;">`,
    `<tr><td style="padding:4px 12px 4px 0;color:#555;">Suggested range (30-50% of retail)</td><td style="padding:4px 0;font-weight:600;">${blurPriceHtml(`${formatUsd(suggested.rangeLow)} - ${formatUsd(suggested.rangeHigh)}`)}</td></tr>`,
    `<tr><td style="padding:4px 12px 4px 0;color:#555;">Est. retail (comps)</td><td style="padding:4px 0;">${blurPriceHtml(formatUsd(suggested.retailEstimate))}</td></tr>`,
    `<tr><td style="padding:4px 12px 4px 0;color:#555;">Seller velocity</td><td style="padding:4px 0;">${escapeHtml(suggested.velocityLabel || "Standard Seller")}</td></tr>`,
    `<tr><td style="padding:4px 12px 4px 0;color:#555;">Comp confidence</td><td style="padding:4px 0;">${escapeHtml(capitalizeConfidence(comps.confidence))}</td></tr>`,
    `</table>`,
  ];

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
    "Suggested Pricing (amounts hidden in email — see attached PDF):",
    `  Seller velocity: ${suggested.velocityLabel || "Standard Seller"}`,
    suggested.rationale ? `  Summary: ${suggested.rationale}` : "",
    `  Analysis: ${EMAIL_ANALYSIS_LABEL}`,
    "  Comparable comps and source links: see attached PDF",
  ];
  return lines.filter(Boolean).join("\n");
}

const PDF_BLOCK_ESTIMATE = 120;

function ensurePdfSpace(doc, neededHeight, margin, contentBottom) {
  if (doc.y + neededHeight <= contentBottom) return;
  doc.addPage();
  doc.y = margin;
}

/**
 * Full comps, suggested pricing, summary, and source links (internal PDF only).
 */
export function drawPdfPricingSection(doc, pricing, { brandColor, margin, contentWidth, contentBottom }) {
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

  const { comparableComps: comps, suggestedPricing: suggested } = pricing.analysis;
  const rationale = String(suggested.rationale || "").trim();

  const drawRow = (label, value) => {
    doc.font("Helvetica-Bold").fontSize(9.5).fillColor("#333");
    doc.text(`${label}: `, margin, doc.y, { continued: true, width: contentWidth });
    doc.font("Helvetica").text(String(value), { width: contentWidth });
    doc.moveDown(0.15);
  };

  doc.font("Helvetica-Bold").fontSize(10).fillColor(brandColor).text("Comparable Comps", margin);
  doc.moveDown(0.15);
  drawRow("Average price", formatUsd(comps.average));
  drawRow("High / Medium / Low", `${formatUsd(comps.high)} / ${formatUsd(comps.medium)} / ${formatUsd(comps.low)}`);
  drawRow("Confidence", capitalizeConfidence(comps.confidence));

  doc.moveDown(0.2);
  doc.font("Helvetica-Bold").fontSize(10).fillColor(brandColor).text("Suggested Pricing for Lost & Found", margin);
  doc.moveDown(0.15);
  drawRow(
    "Suggested range (30-50% of retail)",
    `${formatUsd(suggested.rangeLow)} - ${formatUsd(suggested.rangeHigh)}`
  );
  drawRow("Est. retail (comps)", formatUsd(suggested.retailEstimate));
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

  const sources = (pricing.analysis.sources || []).filter((s) => s.matchStrength === "strong");
  if (sources.length) {
    doc.moveDown(0.35);
    doc.font("Helvetica-Bold").fontSize(10).fillColor(brandColor).text("Comparable Sources (strong matches)", margin);
    doc.moveDown(0.15);
    doc.font("Helvetica").fontSize(9).fillColor("#333");
    for (const src of sources) {
      const pricePart = src.price != null ? ` — ${formatUsd(src.price)}` : "";
      const line = `• ${src.title || src.url}${pricePart}\n  ${src.url}`;
      ensurePdfSpace(doc, 28, margin, contentBottom);
      doc.text(line, margin, doc.y, { width: contentWidth, lineGap: 1 });
      doc.moveDown(0.1);
    }
  }

  if (pricing.modelsUsed?.length) {
    doc.moveDown(0.2);
    doc
      .font("Helvetica")
      .fontSize(8.5)
      .fillColor("#888")
      .text(`Analysis pipeline: ${pricing.modelsUsed.join(" → ")}`, margin, doc.y, {
        width: contentWidth,
      });
  }

  doc.moveDown(0.4);
}
