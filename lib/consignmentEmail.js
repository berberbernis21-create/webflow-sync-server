import { sendEmail } from "../emailService.js";
import {
  displayValue,
  formatDimensions,
  resolveItemNumber,
} from "./consignmentValidation.js";
import { buildPhotoFilename } from "./consignmentFilenames.js";

const CUSTOMER_CONFIRMATION_SUBJECT =
  "We Received Your Consignment Submission - Lost & Found Resale Interiors";

const REVIEW_PROCESS_TEXT =
  "Submitting an item does not guarantee acceptance, but we carefully review every submission. Clear photos, dimensions, condition details, and brand or maker information help us review your items faster. Email is required because approvals, next steps, delivery windows, consignment details, and inventory records are handled in writing. Address information is optional. If your items are accepted, we will email you with next steps, approved delivery window options, and available delivery options which are billed at $95.00/hr.";

const BRAND_MESSAGE_TEXT =
  "When you consign with Lost & Found, your accepted items are supported by more than a local showroom. Our platform helps connect great pieces with buyers through our website, online shopping channels, social media, and our growing customer community.";

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
    label: "Instagram - Resale & Furniture",
    url: "https://www.instagram.com/lostandfoundresale/?utm_source=email&utm_medium=signature&utm_campaign=outreach",
  },
  {
    label: "Facebook - Resale & Furniture",
    url: "https://www.facebook.com/LostAndFoundResale?utm_source=email&utm_medium=signature&utm_campaign=outreach",
  },
  {
    label: "Instagram - Luxury",
    url: "https://www.instagram.com/lost.foundluxury/?utm_source=email&utm_medium=signature&utm_campaign=outreach",
  },
  {
    label: "Facebook - Luxury",
    url: "https://www.facebook.com/profile.php?id=61584002517357&utm_source=email&utm_medium=signature&utm_campaign=outreach",
  },
];

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

function linksHtml(links) {
  return links
    .map(
      ({ label, url }) =>
        `<p style="margin:0 0 10px;font-size:14px;line-height:1.5;"><strong>${escapeHtml(label)}:</strong><br/><a href="${escapeHtml(url)}" style="color:#1a3c34;word-break:break-all;">${escapeHtml(url)}</a></p>`
    )
    .join("");
}

function linksText(links) {
  const lines = [];
  for (const { label, url } of links) {
    lines.push(`${label}:`, url, "");
  }
  return lines.join("\n").trimEnd();
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

  let summaryTableHtml = `<table role="presentation" cellpadding="0" cellspacing="0" style="border-collapse:collapse;width:100%;">`;
  for (const [label, value] of summaryRows) {
    summaryTableHtml += `<tr><td style="padding:8px 16px 8px 0;font-weight:600;color:#1a3c34;vertical-align:top;white-space:nowrap;font-size:14px;">${escapeHtml(label)}:</td><td style="padding:8px 0;color:#444;font-size:14px;">${escapeHtml(value)}</td></tr>`;
  }
  summaryTableHtml += `</table>`;

  const itemsListHtml = numberedItems
    .map(
      (line) =>
        `<li style="margin:0 0 8px;font-size:14px;line-height:1.5;color:#444;">${escapeHtml(line)}</li>`
    )
    .join("");

  const html = [
    `<div style="margin:0;padding:0;background:#f7f5f2;">`,
    `<table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="background:#f7f5f2;padding:24px 12px;">`,
    `<tr><td align="center">`,
    `<table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="max-width:600px;background:#ffffff;border-radius:8px;border:1px solid #e8e4de;">`,
    `<tr><td style="padding:28px 24px 8px;text-align:center;border-bottom:1px solid #e8e4de;">`,
    `<p style="margin:0 0 6px;font-family:Georgia,'Times New Roman',serif;font-size:18px;font-weight:600;color:#1a3c34;">Lost &amp; Found Resale Interiors, LLC</p>`,
    `<p style="margin:0;font-family:Georgia,'Times New Roman',serif;font-size:15px;color:#5c6b66;">We Received Your Submission</p>`,
    `</td></tr>`,
    `<tr><td style="padding:24px 28px 32px;font-family:Georgia,'Times New Roman',serif;line-height:1.6;color:#333;">`,
    `<p style="margin:0 0 16px;font-size:15px;">Hi ${escapeHtml(customerName)},</p>`,
    `<p style="margin:0 0 16px;font-size:15px;">Thank you for choosing Lost &amp; Found Resale Interiors. We are grateful you thought of us for your pieces and wanted to let you know that we received your submission.</p>`,
    `<p style="margin:0 0 16px;font-size:15px;">Our team will carefully review the information and photos you submitted. If your items are a good fit, we will follow up by email with next steps.</p>`,
    `<p style="margin:0 0 20px;font-size:15px;">${escapeHtml(REVIEW_PROCESS_TEXT)}</p>`,
    `<div style="margin:0 0 20px;padding:18px;background:#faf9f7;border:1px solid #e8e4de;border-radius:6px;">`,
    `<p style="margin:0 0 12px;font-size:15px;font-weight:600;color:#1a3c34;">Submission Summary:</p>`,
    summaryTableHtml,
    `<p style="margin:20px 0 8px;font-size:15px;font-weight:600;color:#1a3c34;">Items Submitted:</p>`,
    `<ol style="margin:0;padding-left:20px;">${itemsListHtml}</ol>`,
    `</div>`,
    `<p style="margin:0 0 20px;font-size:15px;color:#444;">${escapeHtml(BRAND_MESSAGE_TEXT)}</p>`,
    `<p style="margin:0 0 16px;font-size:15px;">Thank you again for choosing Lost &amp; Found Resale Interiors. We appreciate the opportunity to review your pieces.</p>`,
    `<p style="margin:0 0 4px;font-size:15px;font-weight:600;color:#1a3c34;">Lost &amp; Found Resale Interiors, LLC</p>`,
    `<p style="margin:0 0 20px;font-size:15px;">480-588-7006</p>`,
    linksHtml(CUSTOMER_SHOP_LINKS, true),
    `<p style="margin:16px 0 8px;font-size:14px;font-weight:600;color:#1a3c34;">Follow us:</p>`,
    linksHtml(CUSTOMER_SOCIAL_LINKS, true),
    `</td></tr></table>`,
    `</td></tr></table>`,
    `</motion>`,
  ].join("").replace(/<\/motion>/g, "</motion>").replace(/<\/motion>/g, "</motion>");

  const text = [
    CUSTOMER_CONFIRMATION_SUBJECT,
    "",
    `Hi ${customerName},`,
    "",
    "Thank you for choosing Lost & Found Resale Interiors. We are grateful you thought of us for your pieces and wanted to let you know that we received your submission.",
    "",
    "Our team will carefully review the information and photos you submitted. If your items are a good fit, we will follow up by email with next steps.",
    "",
    REVIEW_PROCESS_TEXT,
    "",
    "Submission Summary:",
    ...summaryRows.map(([label, value]) => `${label}: ${value}`),
    "",
    "Items Submitted:",
    ...numberedItems,
    "",
    BRAND_MESSAGE_TEXT,
    "",
    "Thank you again for choosing Lost & Found Resale Interiors. We appreciate the opportunity to review your pieces.",
    "",
    "Lost & Found Resale Interiors, LLC",
    "480-588-7006",
    "",
    linksText(CUSTOMER_SHOP_LINKS),
    "",
    "Follow us:",
    linksText(CUSTOMER_SOCIAL_LINKS),
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
    inlineSections.push(itemHtml.replace(/<motion style="margin-top:12px;">/g, '<div style="margin-top:12px;">'));
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
