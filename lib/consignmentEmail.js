import { sendEmailWithAttachments } from "../emailService.js";
import {
  formatItemDimensions,
  getConsignmentBrand,
  getItemDetailFields,
} from "./consignmentBrand.js";
import {
  displayValue,
  formatDimensions,
  resolveItemNumber,
} from "./consignmentValidation.js";
import { buildCustomerPdfFilename, buildPhotoFilename } from "./consignmentFilenames.js";
import { generateCustomerConsignmentPdf } from "./consignmentCustomerPdf.js";

const EMAIL_FONT = "Arial,Helvetica,sans-serif";

function escapeHtml(str) {
  return String(str ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function formatUsd(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return "N/A";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(n);
}

function capitalizeConfidence(value) {
  const s = String(value || "low").toLowerCase();
  if (s === "high" || s === "medium" || s === "low") {
    return s.charAt(0).toUpperCase() + s.slice(1);
  }
  return "Low";
}

function findPricingForItem(pricingResults, itemNumber) {
  if (!Array.isArray(pricingResults)) return null;
  const n = Number(itemNumber);
  return (
    pricingResults.find((p) => p.itemNumber === itemNumber || p.itemNumber === n) || null
  );
}

const PRICING_UNAVAILABLE_REASONS = {
  not_configured:
    "Server pricing APIs are not fully configured (check Render env: Google Vision, Custom Search, Gemini, and OpenAI).",
  no_photos: "No photos were matched to this item for analysis.",
  error: "Pricing analysis encountered an error for this item. Your submission was still received.",
  item_timeout:
    "Automated comp lookup did not finish in time for this item. Your submission, photos, and details were still received and will be reviewed by our team.",
  no_comps:
    "No reliable comparable prices could be derived (common for art and one-of-a-kind pieces).",
  budget_exceeded:
    "Automated comp lookup did not finish in time for this item. Your submission, photos, and details were still received and will be reviewed by our team.",
  pricing_item_limit:
    "Automated comp analysis runs on the first 10 items per submission. This item was still received and will be reviewed manually.",
  disabled: "Pricing analysis is disabled on the server (CONSIGNMENT_PRICING_ENABLED=false).",
};

function pricingUnavailableDetail(pricing) {
  const reason = String(pricing?.reason || "").trim();
  return PRICING_UNAVAILABLE_REASONS[reason] || "";
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

function buildPricingSectionHtml(pricing) {
  if (!pricing?.available || !pricing.analysis) {
    const reason = String(pricing?.reason || "").trim();
    const headline = pricingUnavailableHeadline(pricing);
    const detail = pricingUnavailableDetail(pricing);
    const detailHtml =
      detail && !["budget_exceeded", "item_timeout", "pricing_item_limit"].includes(reason)
        ? `<p style="margin:8px 0 0;font-size:12px;color:#999;line-height:1.45;">${escapeHtml(detail)}</p>`
        : "";
    return `<div style="margin-top:16px;padding:12px;background:#f9f7f4;border-radius:6px;border:1px solid #e8e2d9;"><p style="margin:0;font-size:13px;color:#777;font-style:italic;">${escapeHtml(headline)}</p>${detailHtml}</div>`;
  }

  const { comparableComps: comps, suggestedPricing: suggested } = pricing.analysis;
  return [
    `<div style="margin-top:16px;padding:14px;background:#f9f7f4;border-radius:6px;border:1px solid #e8e2d9;">`,
    `<p style="margin:0 0 10px;font-size:13px;font-weight:700;color:#1a3c34;text-transform:uppercase;letter-spacing:0.04em;">Comparable Comps</p>`,
    `<table style="border-collapse:collapse;width:100%;font-size:13px;">`,
    `<tr><td style="padding:4px 12px 4px 0;color:#555;">Average price</td><td style="padding:4px 0;font-weight:600;">${escapeHtml(formatUsd(comps.average))}</td></tr>`,
    `<tr><td style="padding:4px 12px 4px 0;color:#555;">High / Medium / Low</td><td style="padding:4px 0;">${escapeHtml(`${formatUsd(comps.high)} / ${formatUsd(comps.medium)} / ${formatUsd(comps.low)}`)}</td></tr>`,
    `<tr><td style="padding:4px 12px 4px 0;color:#555;">Confidence</td><td style="padding:4px 0;">${escapeHtml(capitalizeConfidence(comps.confidence))}</td></tr>`,
    `</table>`,
    `<p style="margin:14px 0 10px;font-size:13px;font-weight:700;color:#1a3c34;text-transform:uppercase;letter-spacing:0.04em;">Suggested Pricing for Lost &amp; Found</p>`,
    `<table style="border-collapse:collapse;width:100%;font-size:13px;">`,
    `<tr><td style="padding:4px 12px 4px 0;color:#555;">Suggested range (30-50% of retail)</td><td style="padding:4px 0;font-weight:600;">${escapeHtml(`${formatUsd(suggested.rangeLow)} - ${formatUsd(suggested.rangeHigh)}`)}</td></tr>`,
    `<tr><td style="padding:4px 12px 4px 0;color:#555;">Est. retail (comps)</td><td style="padding:4px 0;">${escapeHtml(formatUsd(suggested.retailEstimate))}</td></tr>`,
    `<tr><td style="padding:4px 12px 4px 0;color:#555;">Seller velocity</td><td style="padding:4px 0;">${escapeHtml(suggested.velocityLabel || "Standard Seller")}</td></tr>`,
    suggested.rationale
      ? `<tr><td colspan="2" style="padding:8px 0 0;color:#666;font-size:12px;line-height:1.45;">${escapeHtml(suggested.rationale)}</td></tr>`
      : "",
    `</table>`,
    pricing.modelsUsed?.length
      ? `<p style="margin:10px 0 0;font-size:11px;color:#999;">Analysis: ${escapeHtml(pricing.modelsUsed.join(" → "))}</p>`
      : "",
    `</div>`,
  ].join("");
}

function buildPricingSectionText(pricing) {
  if (!pricing?.available || !pricing.analysis) {
    const headline = pricingUnavailableHeadline(pricing);
    const detail = pricingUnavailableDetail(pricing);
    const reason = String(pricing?.reason || "").trim();
    if (["budget_exceeded", "item_timeout", "pricing_item_limit"].includes(reason)) {
      return headline;
    }
    return detail ? `${headline}: ${detail}` : headline;
  }
  const { comparableComps: comps, suggestedPricing: suggested } = pricing.analysis;
  return [
    "Comparable Comps:",
    `  Average price: ${formatUsd(comps.average)}`,
    `  High / Medium / Low: ${formatUsd(comps.high)} / ${formatUsd(comps.medium)} / ${formatUsd(comps.low)}`,
    `  Confidence: ${capitalizeConfidence(comps.confidence)}`,
    "Suggested Pricing for Lost & Found:",
    `  Suggested range (30-50% of retail): ${formatUsd(suggested.rangeLow)} - ${formatUsd(suggested.rangeHigh)}`,
    `  Est. retail (comps): ${formatUsd(suggested.retailEstimate)}`,
    `  Seller velocity: ${suggested.velocityLabel || "Standard Seller"}`,
    suggested.rationale ? `  Note: ${suggested.rationale}` : "",
    pricing.modelsUsed?.length ? `  Models: ${pricing.modelsUsed.join(" → ")}` : "",
  ]
    .filter(Boolean)
    .join("\n");
}

function buildComparableSourcesAppendix(pricingResults, items) {
  if (!Array.isArray(pricingResults) || !pricingResults.length) return { html: "", text: "" };

  const sections = [];
  const textLines = ["", "--- Comparable Sources ---"];

  for (let i = 0; i < items.length; i++) {
    const item = items[i];
    const itemNumber = resolveItemNumber(item, i);
    const itemName = displayValue(item.itemName);
    const pricing = findPricingForItem(pricingResults, itemNumber);
    const strong = (pricing?.analysis?.sources || []).filter((s) => s.matchStrength === "strong");
    if (!strong.length) continue;

    let html = `<div style="margin:16px 0 0;"><p style="margin:0 0 8px;font-weight:600;color:#1a3c34;">Item ${itemNumber}: ${escapeHtml(itemName)}</p><ul style="margin:0;padding-left:20px;font-size:13px;line-height:1.5;">`;
    const textItem = [`Item ${itemNumber}: ${itemName}`];
    for (const src of strong) {
      const pricePart = src.price != null ? ` (${formatUsd(src.price)})` : "";
      html += `<li style="margin-bottom:6px;"><a href="${escapeHtml(src.url)}" style="color:#1a3c34;">${escapeHtml(src.title || src.url)}</a>${escapeHtml(pricePart)}</li>`;
      textItem.push(` - ${src.title || src.url}: ${src.url}${src.price != null ? ` (${formatUsd(src.price)})` : ""}`);
    }
    html += `</ul></div>`;
    sections.push(html);
    textLines.push(...textItem);
  }

  if (!sections.length) return { html: "", text: "" };

  const html = [
    `<div style="margin-top:32px;padding-top:20px;border-top:2px solid #ddd;">`,
    `<h3 style="margin:0 0 16px;color:#1a3c34;">Comparable Sources</h3>`,
    sections.join(""),
    `</div>`,
  ].join("");

  return { html, text: textLines.join("\n") };
}

function formatAddress(body) {
  const parts = [
    body.customerStreetAddress,
    body.customerCity,
    body.customerState,
    body.customerZip,
  ]
    .map((p) => (p == null ? "" : String(p).trim()))
    .filter(Boolean);
  return parts.length ? parts.join(", ") : "Not provided";
}

function countUploadedPhotos(photoGroups) {
  let total = 0;
  for (const photos of photoGroups.values()) total += photos.length;
  return total;
}

function customerItemLine(item) {
  const name = String(item?.itemName ?? "").trim() || "Unnamed Item";
  const category = String(item?.category ?? "").trim() || "Not provided";
  const condition = String(item?.condition ?? "").trim() || "Not provided";
  return `${name} - ${category} - ${condition}`;
}

function ctaButtonTextColor(email, primary) {
  if (primary) return "#ffffff";
  if (email.ctaSecondaryTextColor) return email.ctaSecondaryTextColor;
  const bg = String(email.ctaSecondaryBg || "").trim().toLowerCase();
  const darkSecondary =
    bg === "#1a1a1a" || bg === "#111111" || bg === "#000" || bg === "#000000";
  return darkSecondary ? "#ffffff" : email.colorHeading;
}

function ctaButtonHtml(label, url, { primary = false, email } = {}) {
  const bg = primary ? email.ctaPrimaryBg : email.ctaSecondaryBg;
  const border = primary ? email.ctaPrimaryBorder : email.ctaSecondaryBorder;
  const textColor = ctaButtonTextColor(email, primary);
  return [
    `<table role="presentation" cellpadding="0" cellspacing="0" style="margin:0 0 10px;">`,
    `<tr><td align="center" style="border-radius:0;background:${bg};border:1px solid ${border};">`,
    `<a href="${escapeHtml(url)}" target="_blank" style="display:inline-block;padding:12px 20px;font-family:${EMAIL_FONT};font-size:14px;font-weight:700;color:${textColor};text-decoration:none;line-height:1.2;">${escapeHtml(label)}</a>`,
    `</td></tr></table>`,
  ].join("");
}

function socialPillHtml({ shortLabel, subtitle, url }, email) {
  return [
    `<table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="margin:0 0 8px;">`,
    `<tr><td align="center" style="border-radius:0;background:${email.bgCard === "#ffffff" ? "#faf7f0" : "#faf8f5"};border:1px solid ${email.border};">`,
    `<a href="${escapeHtml(url)}" target="_blank" style="display:block;padding:10px 14px;font-family:${EMAIL_FONT};font-size:13px;font-weight:700;color:${email.colorHeading};text-decoration:none;line-height:1.3;">`,
    `${escapeHtml(shortLabel)} <span style="font-weight:400;color:${email.colorMuted};">· ${escapeHtml(subtitle)}</span>`,
    `</a></td></tr></table>`,
  ].join("");
}

function shopLinksText(links) {
  const lines = [];
  for (const { label, url } of links) {
    lines.push(`${label}: ${url}`);
  }
  return lines.join("\n");
}

function socialLinksText(links) {
  const lines = [];
  for (const { shortLabel, subtitle, url } of links) {
    lines.push(`${shortLabel} (${subtitle}): ${url}`);
  }
  return lines.join("\n");
}

/**
 * Customer receipt email (summary in body; optional PDF attachment).
 */
export function buildCustomerConfirmationEmail(submission, items, groupedPhotos, { submittedAt } = {}) {
  const brand = getConsignmentBrand(submission, items);
  const email = brand.email;
  const customerName = String(submission?.customerName ?? "").trim() || "there";
  const customerEmail = String(submission?.customerEmail ?? "").trim();
  const customerPhone = String(submission?.customerPhone ?? "").trim() || "Not provided";
  const submitted =
    submittedAt ||
    new Date().toLocaleString("en-US", {
      timeZone: "America/Phoenix",
      dateStyle: "full",
      timeStyle: "short",
    });
  const itemCount = items.length;
  const photoCount = countUploadedPhotos(groupedPhotos);
  const numberedItems = items.map((item, i) => `${i + 1}. ${customerItemLine(item)}`);

  const summaryRows = [
    ["Consignor", customerName === "there" ? "Not provided" : customerName],
    ["Email", customerEmail || "Not provided"],
    ["Phone", customerPhone],
    ["Submission Date", submitted],
    ["Total Items Submitted", String(itemCount)],
    ["Total Photos Uploaded", String(photoCount)],
  ];

  let summaryTableHtml = `<table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="border-collapse:collapse;">`;
  for (const [label, value] of summaryRows) {
    summaryTableHtml += `<tr><td style="padding:10px 12px 10px 0;font-family:${EMAIL_FONT};font-weight:600;color:${email.colorHeading};vertical-align:top;white-space:nowrap;font-size:13px;line-height:1.4;">${escapeHtml(label)}</td><td style="padding:10px 0;font-family:${EMAIL_FONT};color:${email.colorText};font-size:14px;line-height:1.5;">${escapeHtml(value)}</td></tr>`;
  }
  summaryTableHtml += `</table>`;

  const itemsListHtml = numberedItems
    .map(
      (line) =>
        `<tr><td style="padding:0 0 10px 0;font-family:${EMAIL_FONT};font-size:14px;line-height:1.5;color:${email.colorText};">${escapeHtml(line)}</td></tr>`
    )
    .join("");

  const summaryBoxBg = brand.key === "handbags" ? "#faf7f0" : "#faf8f5";

  const aboutUsHtml = brand.aboutParagraphs.map(
    (p) =>
      `<p style="margin:0 0 14px;font-family:${EMAIL_FONT};font-size:14px;line-height:1.65;color:${email.colorText};">${escapeHtml(p)}</p>`
  ).join("");

  const shopCtasHtml = brand.shopLinks.map(({ label, url }, i) =>
    ctaButtonHtml(label, url, { primary: i === 0, email })
  ).join("");

  const socialCells = brand.socialLinks.map(
    (link) =>
      `<td width="50%" valign="top" style="padding:0 4px 8px 4px;">${socialPillHtml(link, email)}</td>`
  );
  const socialRows = [];
  for (let i = 0; i < socialCells.length; i += 2) {
    socialRows.push(`<tr>${socialCells.slice(i, i + 2).join("")}</tr>`);
  }
  const socialGridHtml = [
    `<table role="presentation" cellpadding="0" cellspacing="0" width="100%">`,
    socialRows.join(""),
    `</table>`,
  ].join("");

  const html = [
    `<div style="margin:0;padding:0;background:${email.bgOuter};">`,
    `<table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="background:${email.bgOuter};padding:24px 12px;">`,
    `<tr><td align="center">`,
    `<table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="max-width:600px;background:${email.bgCard};border-radius:0;border:1px solid ${email.border};">`,
    `<tr><td style="padding:32px 28px 24px;text-align:center;background:${email.headerBg};">`,
    `<p style="margin:0 0 6px;font-family:${EMAIL_FONT};font-size:26px;font-weight:700;letter-spacing:0.06em;text-transform:uppercase;color:${email.headerTitleColor};line-height:1.2;">Lost &amp; Found</p>`,
    `<p style="margin:0 0 8px;font-family:${EMAIL_FONT};font-size:14px;font-weight:700;letter-spacing:0.04em;color:${email.headerSubtitleColor};line-height:1.3;">${escapeHtml(brand.headerLine2)}</p>`,
    `<p style="margin:0;font-family:${EMAIL_FONT};font-size:14px;color:${email.headerTaglineColor};line-height:1.4;">${escapeHtml(brand.headerTagline)}</p>`,
    `</td></tr>`,
    `<tr><td style="padding:28px 28px 8px;font-family:${EMAIL_FONT};line-height:1.6;color:${email.colorText};">`,
    `<p style="margin:0 0 16px;font-family:${EMAIL_FONT};font-size:16px;font-weight:700;color:${email.colorHeading};">Hi ${escapeHtml(customerName)},</p>`,
    `<p style="margin:0 0 14px;font-family:${EMAIL_FONT};font-size:15px;line-height:1.6;color:${email.colorText};">${brand.thankYouParagraphHtml}</p>`,
    `<p style="margin:0 0 14px;font-family:${EMAIL_FONT};font-size:15px;line-height:1.6;color:${email.colorText};">Our team is reviewing your submission and will get back to you within 1-2 business days.</p>`,
    `<p style="margin:0 0 24px;font-family:${EMAIL_FONT};font-size:15px;line-height:1.6;color:${email.colorText};">A copy of your <strong style="color:${email.colorHeading};">submission summary</strong> is attached for your records.</p>`,
    `<table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="margin:0 0 28px;border:1px solid ${email.border};border-radius:0;background:${summaryBoxBg};">`,
    `<tr><td style="padding:18px 20px 8px;">`,
    `<p style="margin:0 0 14px;font-family:${EMAIL_FONT};font-size:13px;font-weight:700;letter-spacing:0.06em;text-transform:uppercase;color:${email.colorAccent};">Submission Summary</p>`,
    summaryTableHtml,
    `</td></tr>`,
    `<tr><td style="padding:8px 20px 18px;border-top:1px solid ${email.border};">`,
    `<p style="margin:0 0 12px;font-family:${EMAIL_FONT};font-size:13px;font-weight:700;letter-spacing:0.06em;text-transform:uppercase;color:${email.colorAccent};">Items Submitted</p>`,
    `<table role="presentation" cellpadding="0" cellspacing="0" width="100%">${itemsListHtml}</table>`,
    `</td></tr></table>`,
    `<p style="margin:0 0 12px;font-family:${EMAIL_FONT};font-size:15px;font-weight:700;color:${email.colorHeading};">${escapeHtml(brand.aboutTitle)}</p>`,
    aboutUsHtml,
    `<p style="margin:8px 0 28px;font-family:${EMAIL_FONT};font-size:15px;line-height:1.6;color:${email.colorText};">Thank you again for trusting us with your ${brand.key === "handbags" ? "handbags" : "pieces"}. We appreciate the opportunity to review your submission.</p>`,
    `<p style="margin:0 0 14px;font-family:${EMAIL_FONT};font-size:13px;font-weight:700;letter-spacing:0.06em;text-transform:uppercase;color:${email.colorAccent};">Connect With Us</p>`,
    socialGridHtml,
    `<p style="margin:20px 0 12px;font-family:${EMAIL_FONT};font-size:13px;font-weight:700;letter-spacing:0.06em;text-transform:uppercase;color:${email.colorAccent};">Shop Online</p>`,
    shopCtasHtml,
    `</td></tr>`,
    `<tr><td style="padding:20px 28px 28px;text-align:center;border-top:1px solid ${email.border};background:${summaryBoxBg};">`,
    `<p style="margin:0 0 6px;font-family:${EMAIL_FONT};font-size:14px;font-weight:700;color:${email.colorHeading};">${escapeHtml(brand.legalName)}</p>`,
    `<p style="margin:0;font-family:${EMAIL_FONT};font-size:15px;"><a href="tel:+14805887006" style="color:${email.colorAccent};text-decoration:none;font-weight:700;">480-588-7006</a></p>`,
    `</td></tr></table>`,
    `</td></tr></table>`,
    `</div>`,
  ].join("");

  const text = [
    brand.customerSubject,
    "",
    `Hi ${customerName},`,
    "",
    brand.thankYouLine,
    "",
    "Our team is reviewing your submission and will get back to you within 1-2 business days.",
    "",
    "A copy of your submission summary is attached for your records.",
    "",
    "Submission Summary:",
    ...summaryRows.map(([label, value]) => `${label}: ${value}`),
    "",
    "Items Submitted:",
    ...numberedItems,
    "",
    `${brand.aboutTitle}:`,
    ...brand.aboutParagraphs,
    "",
    `Thank you again for trusting us with your ${brand.key === "handbags" ? "handbags" : "pieces"}. We appreciate the opportunity to review your submission.`,
    "",
    "Connect with us:",
    socialLinksText(brand.socialLinks),
    "",
    "Shop online:",
    shopLinksText(brand.shopLinks),
    "",
    brand.legalName,
    "480-588-7006",
  ].join("\n");

  return {
    subject: brand.customerSubject,
    html,
    text,
  };
}

/** Send customer confirmation via Resend with branded submission-summary PDF. */
export async function sendCustomerConfirmationEmail(submission, items, groupedPhotos, { submittedAt } = {}) {
  const to = String(submission?.customerEmail ?? "").trim();
  if (!to) {
    throw new Error("Missing customer email");
  }
  const { subject, html, text } = buildCustomerConfirmationEmail(submission, items, groupedPhotos, {
    submittedAt,
  });

  const attachments = [];
  try {
    const customerPdfBuffer = await generateCustomerConsignmentPdf({
      body: submission,
      items,
      photoGroups: groupedPhotos,
      submittedAt,
    });
    if (customerPdfBuffer?.length) {
      attachments.push({
        filename: buildCustomerPdfFilename(submission?.customerName),
        content: customerPdfBuffer,
        contentType: "application/pdf",
      });
    }
  } catch (pdfErr) {
    console.error("[consignment] customer PDF generation failed:", pdfErr?.message || pdfErr);
  }

  return sendEmailWithAttachments({ to, subject, html, text, attachments });
}

/**
 * Build HTML + Resend attachments (inline CID + renamed file attachments + PDF).
 * Each photo is one attachment with contentId for cid: inline display in HTML.
 */
export function buildConsignmentEmail({
  body,
  items,
  photoGroups,
  pdfBuffer,
  pdfFilename,
  submittedAt,
  pricingResults = null,
}) {
  const brand = getConsignmentBrand(body, items);
  const accentColor = brand.internalPdfColor;
  const consignorName = String(body.customerName || "").trim();
  const itemCount = items.length;
  let photoCount = 0;
  for (const photos of photoGroups.values()) photoCount += photos.length;

  const verticalLabel = brand.key === "handbags" ? "Handbags" : "Furniture";
  const subject = `New ${verticalLabel} Consignment - ${consignorName || "Unknown"} - ${itemCount} Item(s)`;

  const summaryRows = [
    ["Consignor", displayValue(consignorName)],
    ["Email", displayValue(body.customerEmail)],
    ["Phone", displayValue(body.customerPhone)],
    ["Address", formatAddress(body)],
    ["Item location", displayValue(body.sameItemLocation)],
    ["Pickup / delivery notes", displayValue(body.pickupNotes || body.pickupLocation)],
    ["Submission category", displayValue(body.submissionCategory)],
    ["Source", displayValue(body.source)],
    ["Submitted", submittedAt],
    ["Items", String(itemCount)],
    ["Photos", String(photoCount)],
  ];

  let summaryHtml = `<table style="border-collapse:collapse;width:100%;max-width:640px;">`;
  for (const [label, value] of summaryRows) {
    summaryHtml += `<tr><td style="padding:6px 12px 6px 0;font-weight:600;vertical-align:top;">${escapeHtml(label)}</td><td style="padding:6px 0;">${escapeHtml(value)}</td></tr>`;
  }
  summaryHtml += `</table>`;

  const attachments = [];
  const inlineSections = [];

  if (pdfBuffer?.length) {
    attachments.push({
      filename: pdfFilename,
      content: pdfBuffer,
      contentType: "application/pdf",
    });
  }

  for (let i = 0; i < items.length; i++) {
    const item = items[i];
    const itemNumber = resolveItemNumber(item, i);
    const photos = photoGroups.get(itemNumber) || [];

    let itemHtml = `<div style="margin:24px 0;padding:16px;border:1px solid #ddd;border-radius:0;">`;
    itemHtml += `<h3 style="margin:0 0 12px;color:${accentColor};">Item #${itemNumber}: ${escapeHtml(displayValue(item.itemName))}</h3>`;
    itemHtml += `<ul style="margin:0;padding-left:20px;line-height:1.5;">`;
    for (const [label, value] of getItemDetailFields(item, brand.key)) {
      if (label === "Warnings" && displayValue(value) === "Not provided") continue;
      itemHtml += `<li><strong>${escapeHtml(label)}:</strong> ${escapeHtml(displayValue(value))}</li>`;
    }
    itemHtml += `<li><strong>Photos:</strong> ${photos.length}</li>`;
    itemHtml += `</ul>`;

    const pricing = findPricingForItem(pricingResults, itemNumber);
    itemHtml += buildPricingSectionHtml(pricing);

    if (photos.length) {
      itemHtml += `<div style="margin-top:12px;">`;
      for (let p = 0; p < photos.length; p++) {
        const file = photos[p];
        const photoIndex = p + 1;
        const filename = buildPhotoFilename({
          consignorName,
          itemNumber,
          itemName: item.itemName,
          photoIndex,
          mimetype: file.mimetype,
        });
        const contentId = `item-${itemNumber}-photo-${photoIndex}`;

        attachments.push({
          filename,
          content: file.buffer,
          contentType: file.mimetype || "image/jpeg",
          contentId,
        });

        itemHtml += `<p style="margin:8px 0 4px;font-size:13px;color:#555;">${escapeHtml(filename)}</p>`;
        itemHtml += `<img src="cid:${contentId}" alt="${escapeHtml(filename)}" style="max-width:100%;height:auto;border:1px solid #eee;border-radius:4px;margin-bottom:12px;" />`;
      }
      itemHtml += `</div>`;
    }
    itemHtml += `</div>`;
    inlineSections.push(itemHtml);
  }

  const sourcesAppendix = buildComparableSourcesAppendix(pricingResults, items);

  const html = [
    `<div style="font-family:Georgia,'Times New Roman',serif;line-height:1.5;color:#222;max-width:720px;">`,
    `<h2 style="color:${accentColor};margin:0 0 8px;">${escapeHtml(brand.internalTitle)}</h2>`,
    `<p style="margin:0 0 20px;font-size:14px;color:#555;">New ${escapeHtml(verticalLabel.toLowerCase())} consignment submission</p>`,
    `<h3 style="margin:0 0 12px;color:${accentColor};">Summary</h3>`,
    summaryHtml,
    `<h3 style="margin:24px 0 12px;color:${accentColor};">Items</h3>`,
    inlineSections.join(""),
    sourcesAppendix.html,
    `</div>`,
  ].join("");

  const textBlocks = [
    `${brand.internalTitle} - New ${verticalLabel.toLowerCase()} consignment submission`,
    "",
    "Summary:",
    ...summaryRows.map(([k, v]) => `  ${k}: ${v}`),
    "",
    "Items:",
  ];

  for (let i = 0; i < items.length; i++) {
    const item = items[i];
    const itemNumber = resolveItemNumber(item, i);
    const photos = photoGroups.get(itemNumber) || [];
    textBlocks.push(
      "",
      `--- Item #${itemNumber} ---`,
      `Name: ${displayValue(item.itemName)}`,
      ...getItemDetailFields(item, brand.key)
        .filter(([label, value]) => label !== "Warnings" || displayValue(value) !== "Not provided")
        .map(([label, value]) => `${label}: ${displayValue(value)}`),
      `Photos: ${photos.length} (see HTML / attachments)`,
      buildPricingSectionText(findPricingForItem(pricingResults, itemNumber))
    );
  }

  if (sourcesAppendix.text) {
    textBlocks.push(sourcesAppendix.text);
  }

  return {
    subject,
    html,
    text: textBlocks.join("\n"),
    attachments,
  };
}
