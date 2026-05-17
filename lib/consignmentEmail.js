import { sendEmail } from "../emailService.js";
import {
  displayValue,
  formatDimensions,
  resolveItemNumber,
} from "./consignmentValidation.js";
import { buildPhotoFilename } from "./consignmentFilenames.js";

const CUSTOMER_CONFIRMATION_SUBJECT =
  "We Received Your Consignment Submission - Lost & Found Resale Interiors";

/** Paraphrased from lostandfoundresale.com homepage and /about (May 2026). */
const ABOUT_US_PARAGRAPHS = [
  "Lost & Found Resale is a curated Scottsdale destination for high-end furniture, luxury resale handbags and accessories, and bespoke design services. Since 2012, we have brought together distinctive, one-of-a-kind pieces for clients who value character, craftsmanship, and timeless appeal.",
  "Our showroom on Greenway Hayden Loop is thoughtfully staged so you can picture each piece in your own home. From mid- to high-end furniture, rugs, art, and lighting to authenticated luxury accessories, we are selective about what we accept and how we present it—creating an inspiring, easy-to-shop experience in the heart of Scottsdale.",
  "When your items are accepted, they are supported by more than our showroom floor: our website, online shopping channels, social media, and growing community help connect great pieces with buyers across Arizona and beyond.",
];

const CUSTOMER_SHOP_LINKS = [
  {
    label: "Shop Lost & Found Resale",
    url: "https://www.lostandfoundresale.com",
  },
  {
    label: "Luxury Handbags & Accessories",
    url: "https://www.lostandfoundhandbags.com",
  },
];

const CUSTOMER_SOCIAL_LINKS = [
  {
    shortLabel: "Instagram",
    subtitle: "Resale & Furniture",
    url: "https://www.instagram.com/lostandfoundresale/?utm_source=email&utm_medium=signature&utm_campaign=outreach",
  },
  {
    shortLabel: "Facebook",
    subtitle: "Resale & Furniture",
    url: "https://www.facebook.com/LostAndFoundResale?utm_source=email&utm_medium=signature&utm_campaign=outreach",
  },
  {
    shortLabel: "Instagram",
    subtitle: "Luxury",
    url: "https://www.instagram.com/lost.foundluxury/?utm_source=email&utm_medium=signature&utm_campaign=outreach",
  },
  {
    shortLabel: "Facebook",
    subtitle: "Luxury",
    url: "https://www.facebook.com/profile.php?id=61584002517357&utm_source=email&utm_medium=signature&utm_campaign=outreach",
  },
];

const EMAIL_FONT = "Arial,Helvetica,sans-serif";
const EMAIL_COLOR_TEXT = "#333333";
const EMAIL_COLOR_MUTED = "#666666";
const EMAIL_COLOR_HEADING = "#1a1a1a";
const EMAIL_COLOR_ACCENT = "#8b7355";
const EMAIL_COLOR_ACCENT_DARK = "#6d5a44";
const EMAIL_BG_OUTER = "#f0ebe4";
const EMAIL_BG_CARD = "#ffffff";
const EMAIL_BORDER = "#e5dfd6";

function escapeHtml(str) {
  return String(str ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
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

function ctaButtonHtml(label, url, { primary = false } = {}) {
  const bg = primary ? EMAIL_COLOR_ACCENT : EMAIL_COLOR_HEADING;
  const border = primary ? EMAIL_COLOR_ACCENT_DARK : EMAIL_COLOR_HEADING;
  return [
    `<table role="presentation" cellpadding="0" cellspacing="0" style="margin:0 0 10px;">`,
    `<tr><td align="center" style="border-radius:6px;background:${bg};border:1px solid ${border};">`,
    `<a href="${escapeHtml(url)}" target="_blank" style="display:inline-block;padding:12px 20px;font-family:${EMAIL_FONT};font-size:14px;font-weight:600;color:#ffffff;text-decoration:none;border-radius:6px;line-height:1.2;">${escapeHtml(label)}</a>`,
    `</td></tr></table>`,
  ].join("");
}

function socialPillHtml({ shortLabel, subtitle, url }) {
  return [
    `<table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="margin:0 0 8px;">`,
    `<tr><td align="center" style="border-radius:24px;background:#faf8f5;border:1px solid ${EMAIL_BORDER};">`,
    `<a href="${escapeHtml(url)}" target="_blank" style="display:block;padding:10px 14px;font-family:${EMAIL_FONT};font-size:13px;font-weight:600;color:${EMAIL_COLOR_HEADING};text-decoration:none;line-height:1.3;">`,
    `${escapeHtml(shortLabel)} <span style="font-weight:400;color:${EMAIL_COLOR_MUTED};">· ${escapeHtml(subtitle)}</span>`,
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
 * Customer receipt email (no attachments, no images).
 */
export function buildCustomerConfirmationEmail(submission, items, groupedPhotos, { submittedAt } = {}) {
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
    summaryTableHtml += `<tr><td style="padding:10px 12px 10px 0;font-family:${EMAIL_FONT};font-weight:600;color:${EMAIL_COLOR_HEADING};vertical-align:top;white-space:nowrap;font-size:13px;line-height:1.4;">${escapeHtml(label)}</td><td style="padding:10px 0;font-family:${EMAIL_FONT};color:${EMAIL_COLOR_TEXT};font-size:14px;line-height:1.5;">${escapeHtml(value)}</td></tr>`;
  }
  summaryTableHtml += `</table>`;

  const itemsListHtml = numberedItems
    .map(
      (line) =>
        `<tr><td style="padding:0 0 10px 0;font-family:${EMAIL_FONT};font-size:14px;line-height:1.5;color:${EMAIL_COLOR_TEXT};">${escapeHtml(line)}</td></tr>`
    )
    .join("");

  const aboutUsHtml = ABOUT_US_PARAGRAPHS.map(
    (p) =>
      `<p style="margin:0 0 14px;font-family:${EMAIL_FONT};font-size:14px;line-height:1.65;color:${EMAIL_COLOR_TEXT};">${escapeHtml(p)}</p>`
  ).join("");

  const shopCtasHtml = CUSTOMER_SHOP_LINKS.map(({ label, url }, i) =>
    ctaButtonHtml(label, url, { primary: i === 0 })
  ).join("");

  const socialCells = CUSTOMER_SOCIAL_LINKS.map(
    (link) =>
      `<td width="50%" valign="top" style="padding:0 4px 8px 4px;">${socialPillHtml(link)}</td>`
  );
  const socialGridHtml = [
    `<table role="presentation" cellpadding="0" cellspacing="0" width="100%">`,
    `<tr>${socialCells.slice(0, 2).join("")}</tr>`,
    `<tr>${socialCells.slice(2, 4).join("")}</tr>`,
    `</table>`,
  ].join("");

  const html = [
    `<div style="margin:0;padding:0;background:${EMAIL_BG_OUTER};">`,
    `<table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="background:${EMAIL_BG_OUTER};padding:24px 12px;">`,
    `<tr><td align="center">`,
    `<table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="max-width:600px;background:${EMAIL_BG_CARD};border-radius:10px;border:1px solid ${EMAIL_BORDER};">`,
    `<tr><td style="padding:32px 28px 24px;text-align:center;background:${EMAIL_COLOR_HEADING};">`,
    `<p style="margin:0 0 8px;font-family:${EMAIL_FONT};font-size:11px;font-weight:600;letter-spacing:0.12em;text-transform:uppercase;color:${EMAIL_COLOR_ACCENT};">Lost &amp; Found</p>`,
    `<p style="margin:0 0 6px;font-family:${EMAIL_FONT};font-size:22px;font-weight:700;color:#ffffff;line-height:1.25;">Resale Interiors</p>`,
    `<p style="margin:0;font-family:${EMAIL_FONT};font-size:14px;color:#d4ccc4;line-height:1.4;">Curated resale &amp; design · Scottsdale, Arizona</p>`,
    `</td></tr>`,
    `<tr><td style="padding:28px 28px 8px;font-family:${EMAIL_FONT};line-height:1.6;color:${EMAIL_COLOR_TEXT};">`,
    `<p style="margin:0 0 16px;font-family:${EMAIL_FONT};font-size:16px;font-weight:600;color:${EMAIL_COLOR_HEADING};">Hi ${escapeHtml(customerName)},</p>`,
    `<p style="margin:0 0 14px;font-family:${EMAIL_FONT};font-size:15px;line-height:1.6;color:${EMAIL_COLOR_TEXT};">Thank you for choosing Lost &amp; Found Resale Interiors. We are grateful you thought of us for your pieces—and we wanted to confirm that <strong style="color:${EMAIL_COLOR_HEADING};">we received your submission</strong>.</p>`,
    `<p style="margin:0 0 24px;font-family:${EMAIL_FONT};font-size:15px;line-height:1.6;color:${EMAIL_COLOR_TEXT};">Our team is reviewing your submission now. If your pieces are a good fit for the showroom, we will reach out by email with next steps.</p>`,
    `<table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="margin:0 0 28px;border:1px solid ${EMAIL_BORDER};border-radius:8px;background:#faf8f5;">`,
    `<tr><td style="padding:18px 20px 8px;">`,
    `<p style="margin:0 0 14px;font-family:${EMAIL_FONT};font-size:13px;font-weight:700;letter-spacing:0.06em;text-transform:uppercase;color:${EMAIL_COLOR_ACCENT};">Submission Summary</p>`,
    summaryTableHtml,
    `</td></tr>`,
    `<tr><td style="padding:8px 20px 18px;border-top:1px solid ${EMAIL_BORDER};">`,
    `<p style="margin:0 0 12px;font-family:${EMAIL_FONT};font-size:13px;font-weight:700;letter-spacing:0.06em;text-transform:uppercase;color:${EMAIL_COLOR_ACCENT};">Items Submitted</p>`,
    `<table role="presentation" cellpadding="0" cellspacing="0" width="100%">${itemsListHtml}</table>`,
    `</td></tr></table>`,
    `<p style="margin:0 0 12px;font-family:${EMAIL_FONT};font-size:15px;font-weight:700;color:${EMAIL_COLOR_HEADING};">About Lost &amp; Found</p>`,
    aboutUsHtml,
    `<p style="margin:8px 0 28px;font-family:${EMAIL_FONT};font-size:15px;line-height:1.6;color:${EMAIL_COLOR_TEXT};">Thank you again for trusting us with your pieces. We appreciate the opportunity to review your submission.</p>`,
    `<p style="margin:0 0 14px;font-family:${EMAIL_FONT};font-size:13px;font-weight:700;letter-spacing:0.06em;text-transform:uppercase;color:${EMAIL_COLOR_ACCENT};">Connect With Us</p>`,
    socialGridHtml,
    `<p style="margin:20px 0 12px;font-family:${EMAIL_FONT};font-size:13px;font-weight:700;letter-spacing:0.06em;text-transform:uppercase;color:${EMAIL_COLOR_ACCENT};">Shop Online</p>`,
    shopCtasHtml,
    `</td></tr>`,
    `<tr><td style="padding:20px 28px 28px;text-align:center;border-top:1px solid ${EMAIL_BORDER};background:#faf8f5;">`,
    `<p style="margin:0 0 6px;font-family:${EMAIL_FONT};font-size:14px;font-weight:600;color:${EMAIL_COLOR_HEADING};">Lost &amp; Found Resale Interiors, LLC</p>`,
    `<p style="margin:0;font-family:${EMAIL_FONT};font-size:15px;"><a href="tel:+14805887006" style="color:${EMAIL_COLOR_ACCENT};text-decoration:none;font-weight:600;">480-588-7006</a></p>`,
    `</td></tr></table>`,
    `</td></tr></table>`,
    `</div>`,
  ].join("");

  const text = [
    CUSTOMER_CONFIRMATION_SUBJECT,
    "",
    `Hi ${customerName},`,
    "",
    "Thank you for choosing Lost & Found Resale Interiors. We are grateful you thought of us for your pieces—and we wanted to confirm that we received your submission.",
    "",
    "Our team is reviewing your submission now. If your pieces are a good fit for the showroom, we will reach out by email with next steps.",
    "",
    "Submission Summary:",
    ...summaryRows.map(([label, value]) => `${label}: ${value}`),
    "",
    "Items Submitted:",
    ...numberedItems,
    "",
    "About Lost & Found:",
    ...ABOUT_US_PARAGRAPHS,
    "",
    "Thank you again for trusting us with your pieces. We appreciate the opportunity to review your submission.",
    "",
    "Connect with us:",
    socialLinksText(CUSTOMER_SOCIAL_LINKS),
    "",
    "Shop online:",
    shopLinksText(CUSTOMER_SHOP_LINKS),
    "",
    "Lost & Found Resale Interiors, LLC",
    "480-588-7006",
  ].join("\n");

  return {
    subject: CUSTOMER_CONFIRMATION_SUBJECT,
    html,
    text,
  };
}

/** Send customer confirmation via Resend (no attachments). */
export async function sendCustomerConfirmationEmail(submission, items, groupedPhotos, options = {}) {
  const to = String(submission?.customerEmail ?? "").trim();
  if (!to) {
    throw new Error("Missing customer email");
  }
  const { subject, html, text } = buildCustomerConfirmationEmail(
    submission,
    items,
    groupedPhotos,
    options
  );
  return sendEmail({ to, subject, html, text });
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
}) {
  const consignorName = String(body.customerName || "").trim();
  const itemCount = items.length;
  let photoCount = 0;
  for (const photos of photoGroups.values()) photoCount += photos.length;

  const subject = `New Consignment Submission - ${consignorName || "Unknown"} - ${itemCount} Item(s)`;

  const summaryRows = [
    ["Consignor", displayValue(consignorName)],
    ["Email", displayValue(body.customerEmail)],
    ["Phone", displayValue(body.customerPhone)],
    ["Address", formatAddress(body)],
    ["Item location", displayValue(body.sameItemLocation)],
    ["Pickup / delivery notes", displayValue(body.pickupNotes || body.pickupLocation)],
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

    let itemHtml = `<div style="margin:24px 0;padding:16px;border:1px solid #ddd;border-radius:8px;">`;
    itemHtml += `<h3 style="margin:0 0 12px;color:#1a3c34;">Item #${itemNumber}: ${escapeHtml(displayValue(item.itemName))}</h3>`;
    itemHtml += `<ul style="margin:0;padding-left:20px;line-height:1.5;">`;
    itemHtml += `<li><strong>Category:</strong> ${escapeHtml(displayValue(item.category))}</li>`;
    itemHtml += `<li><strong>Brand / maker:</strong> ${escapeHtml(displayValue(item.brand))}</li>`;
    itemHtml += `<li><strong>Age:</strong> ${escapeHtml(displayValue(item.age))}</li>`;
    itemHtml += `<li><strong>Condition:</strong> ${escapeHtml(displayValue(item.condition))}</li>`;
    itemHtml += `<li><strong>Original price:</strong> ${escapeHtml(displayValue(item.originalPrice))}</li>`;
    itemHtml += `<li><strong>Dimensions (W×D×H):</strong> ${escapeHtml(formatDimensions(item))}</li>`;
    itemHtml += `<li><strong>Condition notes:</strong> ${escapeHtml(displayValue(item.conditionNotes))}</li>`;
    itemHtml += `<li><strong>Notes:</strong> ${escapeHtml(displayValue(item.notes))}</li>`;
    itemHtml += `<li><strong>Warnings:</strong> ${escapeHtml(displayValue(item.warnings))}</li>`;
    itemHtml += `<li><strong>Photos:</strong> ${photos.length}</li>`;
    itemHtml += `</ul>`;

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

  const html = [
    `<div style="font-family:Georgia,'Times New Roman',serif;line-height:1.5;color:#222;max-width:720px;">`,
    `<h2 style="color:#1a3c34;margin:0 0 8px;">L&amp;F Resale Interiors</h2>`,
    `<p style="margin:0 0 20px;font-size:14px;color:#555;">New consignment submission</p>`,
    `<h3 style="margin:0 0 12px;color:#1a3c34;">Summary</h3>`,
    summaryHtml,
    `<h3 style="margin:24px 0 12px;color:#1a3c34;">Items</h3>`,
    inlineSections.join(""),
    `</div>`,
  ].join("");

  const textBlocks = [
    "L&F Resale Interiors — New consignment submission",
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
      `Category: ${displayValue(item.category)}`,
      `Brand: ${displayValue(item.brand)}`,
      `Age: ${displayValue(item.age)}`,
      `Condition: ${displayValue(item.condition)}`,
      `Original price: ${displayValue(item.originalPrice)}`,
      `Dimensions: ${formatDimensions(item)}`,
      `Condition notes: ${displayValue(item.conditionNotes)}`,
      `Notes: ${displayValue(item.notes)}`,
      `Warnings: ${displayValue(item.warnings)}`,
      `Photos: ${photos.length} (see HTML / attachments)`
    );
  }

  return {
    subject,
    html,
    text: textBlocks.join("\n"),
    attachments,
  };
}
