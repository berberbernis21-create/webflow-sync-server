import { sendEmailWithAttachments } from "../emailService.js";
import { getConsignmentBrand } from "./consignmentBrand.js";
import {
  displayValue,
  resolveItemNumber,
  resolvePreferredSubmissionType,
} from "./consignmentValidation.js";
import { buildCustomerPdfFilename, buildPhotoFilename } from "./consignmentFilenames.js";
import { generateCustomerConsignmentPdf } from "./consignmentCustomerPdf.js";
import {
  buildInternalEmailPricingHtml,
  buildInternalEmailPricingText,
  findPricingForItem,
  INTERNAL_EMAIL_PRICING_NOTICE,
} from "./consignmentPricingDisplay.js";
import { buildGoogleItemSearchButtonHtml } from "./consignmentGoogleSearch.js";
import { buildConsignorProfileTableHtml } from "./consignmentProfileDisplay.js";

const EMAIL_FONT = "Arial,Helvetica,sans-serif";

function escapeHtml(str) {
  return String(str ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function countUploadedPhotos(photoGroups) {
  let total = 0;
  for (const photos of photoGroups.values()) total += photos.length;
  return total;
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
    `<p style="margin:0 0 14px;font-family:${EMAIL_FONT};font-size:15px;line-height:1.6;color:${email.colorText};">${brand.customerFollowUpParagraphHtml}</p>`,
    `<p style="margin:0 0 14px;font-family:${EMAIL_FONT};font-size:15px;line-height:1.6;color:${email.colorText};">${escapeHtml(brand.customerPricingParagraph)}</p>`,
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
    brand.customerFollowUpParagraph,
    "",
    brand.customerPricingParagraph,
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
export async function sendCustomerConfirmationEmail(submission, items, groupedPhotos, { submittedAt, skipPdf = false } = {}) {
  const to = String(submission?.customerEmail ?? "").trim();
  if (!to) {
    throw new Error("Missing customer email");
  }
  const { subject, html, text } = buildCustomerConfirmationEmail(submission, items, groupedPhotos, {
    submittedAt,
  });

  const attachments = [];
  if (!skipPdf) {
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
  }

  return sendEmailWithAttachments({ to, subject, html, text, attachments });
}

function buildProcessingNotesHtml(processingWarnings = []) {
  const lines = (processingWarnings || []).filter(Boolean);
  if (!lines.length) return "";
  return [
    `<div style="margin:0 0 20px;padding:12px 14px;font-size:13px;color:#7a2e0e;background:#fff4ed;border-left:3px solid #e04f16;line-height:1.55;">`,
    `<strong>Processing notes</strong>`,
    `<ul style="margin:8px 0 0;padding-left:18px;">`,
    ...lines.map((line) => `<li>${escapeHtml(line)}</li>`),
    `</ul>`,
    `</div>`,
  ].join("");
}

function formatPhotoFailureLine(failure) {
  const name = failure?.originalname || "photo";
  const mime = failure?.mimetype || "unknown type";
  const reason = failure?.message || "conversion failed";
  return `Image conversion failed: ${name} (${mime}) — ${reason}`;
}

/**
 * Build HTML + Resend attachments (inline CID + renamed file attachments + PDF).
 * Each photo is one attachment with contentId for cid: inline display in HTML.
 */
export function buildConsignmentEmail({
  body,
  items,
  photoGroups,
  originalPhotoGroups = null,
  originalItemCount = null,
  photoFailures = [],
  processingWarnings = [],
  pdfBuffer,
  pdfFilename,
  submittedAt,
  pricingResults = null,
}) {
  const emailPhotoGroups = photoGroups;
  const sourcePhotoGroups = originalPhotoGroups || photoGroups;
  const brand = getConsignmentBrand(body, items);
  const accentColor = brand.internalPdfColor;
  const consignorName = String(body.customerName || "").trim();
  const consignorEmail = String(body.customerEmail || "").trim();
  const itemCount = items.length;
  const submittedItemCount =
    Number.isFinite(Number(originalItemCount)) && Number(originalItemCount) > 0
      ? Number(originalItemCount)
      : itemCount;
  const splitExpanded = itemCount > submittedItemCount;
  let uploadedPhotoCount = 0;
  for (const photos of sourcePhotoGroups.values()) uploadedPhotoCount += photos.length;

  const verticalLabel = brand.key === "handbags" ? "Handbags" : "Furniture";
  const subject = `New ${verticalLabel} Consignment - ${consignorName || "Unknown"} - ${itemCount} Item(s)`;

  const allWarnings = [...processingWarnings];
  if (photoFailures.length) {
    allWarnings.push(
      `${photoFailures.length} of ${uploadedPhotoCount} uploaded photo(s) could not be converted — filenames and errors are listed under each item.`
    );
  }
  if (!pdfBuffer?.length) {
    allWarnings.push("Internal PDF was not attached (generation failed or skipped).");
  }

  const itemsLabel = splitExpanded
    ? `${itemCount} analysis items (from ${submittedItemCount} submission line${submittedItemCount === 1 ? "" : "s"})`
    : String(itemCount);

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
    const photos = emailPhotoGroups.get(itemNumber) || [];
    const itemFailures = photoFailures.filter(
      (row) =>
        row.itemNumber === itemNumber ||
        (item?._splitChild && row.itemNumber === item.splitFromItemNumber)
    );

    // Slim item card: name + pricing summary + photos (detail lives in PDF).
    let itemHtml = `<div style="margin:18px 0;padding:14px 0;border-top:1px solid #e8e8e8;">`;
    itemHtml += `<h3 style="margin:0 0 6px;color:${accentColor};font-size:16px;">Item #${itemNumber}: ${escapeHtml(displayValue(item.itemName))}</h3>`;
    const quickBits = [
      displayValue(item.category) !== "Not provided" ? displayValue(item.category) : null,
      displayValue(item.brand || item.designer) !== "Not provided"
        ? displayValue(item.brand || item.designer)
        : null,
      `${photos.length} photo${photos.length === 1 ? "" : "s"}`,
    ].filter(Boolean);
    if (quickBits.length) {
      itemHtml += `<p style="margin:0 0 10px;font-size:13px;color:#666;">${escapeHtml(quickBits.join(" · "))}</p>`;
    }

    const pricing = findPricingForItem(pricingResults, itemNumber);
    itemHtml += buildInternalEmailPricingHtml(pricing, item);

    if (itemFailures.length) {
      itemHtml += `<p style="margin:10px 0 0;font-size:12px;color:#b42318;">`;
      itemHtml += itemFailures.map((f) => escapeHtml(formatPhotoFailureLine(f))).join("<br/>");
      itemHtml += `</p>`;
    }

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
        // Email: prefer direct Lens URL (works in clients). PDF uses short redirect.
        const lensUrl = file.googleLensDirectUrl || file.googleLensUrl;

        try {
          attachments.push({
            filename,
            content: file.buffer,
            contentType: file.mimetype || "image/jpeg",
            contentId,
          });
          itemHtml += `<table style="border-collapse:collapse;margin:6px 0 2px;width:100%;"><tr>`;
          itemHtml += `<td style="padding:0 8px 0 0;font-size:12px;color:#777;vertical-align:middle;">Photo ${photoIndex}</td>`;
          itemHtml += `<td style="padding:0;text-align:right;vertical-align:middle;white-space:nowrap;">${buildGoogleItemSearchButtonHtml(item, { photoIndex, lensUrl })}</td>`;
          itemHtml += `</tr></table>`;
          itemHtml += `<img src="cid:${contentId}" alt="${escapeHtml(filename)}" style="max-width:100%;height:auto;border:1px solid #eee;margin-bottom:10px;" />`;
        } catch (attachErr) {
          const line = `Image attachment failed: ${filename} — ${attachErr?.message || attachErr}`;
          allWarnings.push(line);
          itemHtml += `<p style="margin:8px 0;font-size:13px;color:#b42318;">${escapeHtml(line)}</p>`;
        }
      }
      itemHtml += `</div>`;
    }
    itemHtml += `</div>`;
    inlineSections.push(itemHtml);
  }

  const consignorLine = [displayValue(consignorName), displayValue(consignorEmail)]
    .filter((v) => v && v !== "Not provided")
    .join(" · ");

  const profileValues = {
    name: displayValue(consignorName),
    email: displayValue(consignorEmail),
    phone: displayValue(body.customerPhone),
    address: formatAddress(body),
    itemLocation: displayValue(body.sameItemLocation),
    pickup: displayValue(body.pickupNotes || body.pickupLocation),
    preferredType: resolvePreferredSubmissionType(body),
    source: displayValue(body.source),
    category: displayValue(body.submissionCategory),
    submitted: String(submittedAt || "Not provided"),
    items: itemsLabel,
    photos: String(uploadedPhotoCount),
  };

  const html = [
    `<div style="font-family:Arial,Helvetica,sans-serif;line-height:1.5;color:#222;max-width:640px;">`,
    `<h2 style="color:${accentColor};margin:0 0 4px;font-size:20px;">${escapeHtml(brand.internalTitle)}</h2>`,
    `<p style="margin:0 0 14px;font-size:14px;color:#555;">New ${escapeHtml(verticalLabel.toLowerCase())} · ${escapeHtml(consignorLine || "Unknown")} · ${escapeHtml(itemsLabel)} item(s) · ${uploadedPhotoCount} photo(s)</p>`,
    buildProcessingNotesHtml(allWarnings),
    `<p style="margin:0 0 8px;font-size:13px;color:#444;"><strong>Internal only.</strong> ${escapeHtml(INTERNAL_EMAIL_PRICING_NOTICE)}</p>`,
    `<h3 style="margin:18px 0 0;color:${accentColor};font-size:15px;">Items</h3>`,
    inlineSections.join(""),
    `<h3 style="margin:28px 0 10px;color:${accentColor};font-size:15px;">Additional consignor information</h3>`,
    `<p style="margin:0 0 12px;font-size:13px;color:#666;">Full consignor contact and submission details for the team.</p>`,
    buildConsignorProfileTableHtml(profileValues),
    `</div>`,
  ].join("");

  const textBlocks = [
    `${brand.internalTitle} - New ${verticalLabel.toLowerCase()} consignment`,
    `${consignorLine || "Unknown"} · ${itemsLabel} item(s) · ${uploadedPhotoCount} photo(s)`,
    "",
    `Internal only. ${INTERNAL_EMAIL_PRICING_NOTICE}`,
  ];
  if (allWarnings.length) {
    textBlocks.push("", "Processing notes:", ...allWarnings.map((line) => `  - ${line}`));
  }
  textBlocks.push("", "Items:");

  for (let i = 0; i < items.length; i++) {
    const item = items[i];
    const itemNumber = resolveItemNumber(item, i);
    textBlocks.push(
      "",
      `Item #${itemNumber}: ${displayValue(item.itemName)}`,
      buildInternalEmailPricingText(findPricingForItem(pricingResults, itemNumber), item)
    );
  }

  textBlocks.push(
    "",
    "Additional consignor information:",
    `  Name: ${profileValues.name}`,
    `  Email: ${profileValues.email}`,
    `  Phone: ${profileValues.phone}`,
    `  Address: ${profileValues.address}`,
    `  Item location: ${profileValues.itemLocation}`,
    `  Pickup / delivery: ${profileValues.pickup}`,
    `  Preferred type: ${profileValues.preferredType}`,
    `  Source: ${profileValues.source}`,
    `  Category: ${profileValues.category}`,
    `  Submitted: ${profileValues.submitted}`,
    `  Items: ${profileValues.items}`,
    `  Photos: ${profileValues.photos}`
  );

  return {
    subject,
    html,
    text: textBlocks.join("\n"),
    attachments,
  };
}
