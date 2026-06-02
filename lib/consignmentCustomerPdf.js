import PDFDocument from "pdfkit";
import {
  CUSTOMER_FOLLOW_UP_WILL_REVIEW_PROMPTLY_EMAIL,
  formatItemDimensions,
  getConsignmentBrand,
  getCustomerPolicySections,
} from "./consignmentBrand.js";
import {
  displayValue,
  formatDimensions,
  resolveItemNumber,
} from "./consignmentValidation.js";

const COLOR_HEADING = "#1a1a1a";
const COLOR_ACCENT = "#8b7355";
const COLOR_MUTED = "#666666";
const COLOR_LIGHT_BG = "#faf8f5";
const COLOR_BORDER = "#e5dfd6";
const MARGIN = 50;
const CONTENT_WIDTH = 512;
const HEADER_BAR_HEIGHT = 78;
const THUMB_MAX = 56;
const FOOTER_RESERVE = 52;
const FOOTER_LINE =
  "Lost & Found Resale Interiors, LLC | 480-588-7006 | lostandfoundresale.com";

const DISCLAIMER_TITLE = "Important: Not an Acceptance Notice";
const DISCLAIMER_BODY = `This document is a copy of your submission only. It does NOT mean your items have been accepted for consignment. ${CUSTOMER_FOLLOW_UP_WILL_REVIEW_PROMPTLY_EMAIL} if your items are approved.`;

const CONTENT_TOP_TOLERANCE = 4;

function pageContentBottom(doc) {
  return doc.page.height - MARGIN - FOOTER_RESERVE;
}

function contentTopY(doc, { withHeader = false } = {}) {
  return withHeader ? HEADER_BAR_HEIGHT + 18 : MARGIN;
}

function isPageEffectivelyBlank(doc, { withHeader = false } = {}) {
  return doc.y <= contentTopY(doc, { withHeader }) + CONTENT_TOP_TOLERANCE;
}

function addPageIfHasContent(doc, theme, { withHeader = false } = {}) {
  if (!isPageEffectivelyBlank(doc, { withHeader })) {
    doc.addPage();
    if (withHeader) drawPageChrome(doc, theme, { header: true });
  }
}

function formatAddress(body) {
  const parts = [body.customerStreetAddress, body.customerCity, body.customerState, body.customerZip]
    .map((p) => (p == null ? "" : String(p).trim()))
    .filter(Boolean);
  return parts.length ? parts.join(", ") : null;
}

function countPhotos(photoGroups) {
  let total = 0;
  for (const photos of photoGroups.values()) total += photos.length;
  return total;
}

function measureImageFit(doc, buffer, maxW, maxH) {
  const img = doc.openImage(buffer);
  const scale = Math.min(maxW / img.width, maxH / img.height, 1);
  return { width: img.width * scale, height: img.height * scale };
}

function ensureVerticalSpace(doc, neededHeight, theme) {
  if (doc.y + neededHeight <= pageContentBottom(doc)) return;
  if (!isPageEffectivelyBlank(doc, { withHeader: true })) {
    doc.addPage();
    drawPageChrome(doc, theme, { header: true });
  }
}

function drawPageWatermark(doc, pdfBrand) {
  const cx = doc.page.width / 2;
  const cy = doc.page.height / 2;
  doc.save();
  doc.rotate(-35, { origin: [cx, cy] });
  doc.font("Helvetica-Bold").fontSize(44).fillColor(pdfBrand.colorHeading, 0.05);
  doc.text("LOST & FOUND", cx - 210, cy - 24, { width: 420, align: "center" });
  doc.font("Helvetica").fontSize(13).fillColor(pdfBrand.colorAccent, 0.06);
  doc.text(pdfBrand.watermarkSub, cx - 210, cy + 26, { width: 420, align: "center" });
  doc.restore();
}

function drawHeaderBar(doc, pdfBrand, headerTagline) {
  doc.save();
  doc.rect(0, 0, doc.page.width, HEADER_BAR_HEIGHT).fill(pdfBrand.colorHeading);
  doc.fillColor("#ffffff").font("Helvetica-Bold").fontSize(26).text("LOST & FOUND", MARGIN, 12, {
    characterSpacing: 1.6,
  });
  doc.fillColor(pdfBrand.colorAccent === pdfBrand.colorHeading ? "#faf7f0" : pdfBrand.colorAccent)
    .font("Helvetica-Bold")
    .fontSize(14)
    .text(pdfBrand.headerSub, MARGIN, 42, {
      characterSpacing: 0.6,
    });
  doc.font("Helvetica").fontSize(8).fillColor("#d4ccc4").text(headerTagline, MARGIN, 60);
  doc.restore();
  doc.y = HEADER_BAR_HEIGHT + 18;
}

function drawPageChrome(doc, theme, { header = false } = {}) {
  drawPageWatermark(doc, theme.pdf);
  if (header) drawHeaderBar(doc, theme.pdf, theme.brand.headerTagline);
}

function drawLabelValue(doc, label, value, theme, { indent = 0 } = {}) {
  const pdf = theme?.pdf || {};
  const x = MARGIN + indent;
  const w = CONTENT_WIDTH - indent;
  doc.font("Helvetica-Bold").fontSize(9.5).fillColor(pdf.colorHeading || COLOR_HEADING);
  doc.text(`${label}: `, x, doc.y, { continued: true, width: w });
  doc.font("Helvetica").fillColor("#333333").text(String(value), { width: w });
  return doc.y + 5;
}

function drawDisclaimer(doc, theme) {
  const pdf = theme.pdf;
  const title = theme.brand.disclaimerTitle || DISCLAIMER_TITLE;
  const body = theme.brand.disclaimerBody || DISCLAIMER_BODY;
  const pad = 12;
  const innerW = CONTENT_WIDTH - pad * 2;
  doc.font("Helvetica-Bold").fontSize(11);
  const titleH = doc.heightOfString(title, { width: innerW });
  doc.font("Helvetica").fontSize(10);
  const bodyH = doc.heightOfString(body, { width: innerW, lineGap: 3 });
  const boxHeight = pad * 2 + titleH + 8 + bodyH + 4;
  const boxY = doc.y;
  doc.roundedRect(MARGIN, boxY, CONTENT_WIDTH, boxHeight, 5).fill(pdf.colorLightBg);
  doc.roundedRect(MARGIN, boxY, CONTENT_WIDTH, boxHeight, 5).lineWidth(1.5).strokeColor(pdf.colorAccent).stroke();
  doc.font("Helvetica-Bold").fontSize(11).fillColor(pdf.colorHeading);
  doc.text(title, MARGIN + pad, boxY + pad, { width: innerW });
  doc.font("Helvetica").fontSize(10).fillColor("#333333");
  doc.text(body, MARGIN + pad, boxY + pad + titleH + 8, { width: innerW, lineGap: 3 });
  doc.y = boxY + boxHeight + 16;
}

function drawItemSection(doc, item, itemNumber, photos, brandKey, pdfBrand, theme) {
  ensureVerticalSpace(doc, 120, theme);
  doc.font("Helvetica-Bold").fontSize(13).fillColor(pdfBrand.colorAccent).text(`Item #${itemNumber}`, MARGIN);
  doc.font("Helvetica-Bold").fontSize(12).fillColor(pdfBrand.colorHeading).text(displayValue(item.itemName), MARGIN, doc.y + 2);
  doc.moveDown(0.35);

  const fields =
    brandKey === "handbags"
      ? [
          ["Category", displayValue(item.category)],
          ["Brand", displayValue(item.brand)],
          ["Color", displayValue(item.color)],
          ["Material", displayValue(item.material)],
          ["Condition", displayValue(item.condition)],
          ["Approximate size", formatItemDimensions(item, brandKey)],
          ["Proof of purchase", displayValue(item.proof)],
          ["Serial / date code", displayValue(item.authCode)],
          ["Accessories", displayValue(item.accessories)],
          ["Condition notes", displayValue(item.conditionNotes)],
          ["Additional notes", displayValue(item.notes)],
          ["Photos submitted", String(photos.length)],
        ]
      : [
          ["Category", displayValue(item.category)],
          ["Brand", displayValue(item.brand)],
          ["Condition", displayValue(item.condition)],
          ["Dimensions (W×D×H)", formatDimensions(item)],
          ["Age", displayValue(item.age)],
          ["Original price", displayValue(item.originalPrice)],
          ["Notes", displayValue(item.notes || item.conditionNotes)],
          ["Photos submitted", String(photos.length)],
        ];
  for (const [label, value] of fields) {
    drawLabelValue(doc, label, value, theme);
  }

  if (photos[0]?.buffer) {
    try {
      const { width, height } = measureImageFit(doc, photos[0].buffer, 120, THUMB_MAX);
      ensureVerticalSpace(doc, height + 10, theme);
      const y = doc.y + 4;
      doc.font("Helvetica").fontSize(8).fillColor(pdfBrand.colorMuted).text("Photo preview", MARGIN, y);
      doc.image(photos[0].buffer, MARGIN, y + 12, { width, height });
      doc.y = y + 12 + height + 10;
    } catch {
      /* skip bad image */
    }
  }
  doc.moveDown(0.5);
  doc.moveTo(MARGIN, doc.y).lineTo(MARGIN + CONTENT_WIDTH, doc.y).strokeColor(pdfBrand.colorBorder).lineWidth(0.5).stroke();
  doc.moveDown(0.6);
}

function drawPolicySection(doc, section, theme) {
  ensureVerticalSpace(doc, 48, theme);
  doc.font("Helvetica-Bold").fontSize(11.5).fillColor(theme.pdf.colorAccent).text(section.title, MARGIN, doc.y, {
    width: CONTENT_WIDTH,
  });
  doc.moveDown(0.3);
  doc.font("Helvetica").fontSize(9.5).fillColor("#333333");
  for (const para of section.paragraphs) {
    const indent = para.startsWith("•") ? 10 : 0;
    const w = CONTENT_WIDTH - indent;
    const h = doc.heightOfString(para, { width: w, lineGap: 2 });
    ensureVerticalSpace(doc, h + 6, theme);
    doc.text(para, MARGIN + indent, doc.y, { width: w, lineGap: 2 });
    doc.moveDown(0.35);
  }
  doc.moveDown(0.4);
}

function addPageFooters(doc, theme) {
  const range = doc.bufferedPageRange();
  const pageCount = range.count;
  const footerY = pageContentBottom(doc) + 6;
  for (let i = 0; i < pageCount; i++) {
    doc.switchToPage(range.start + i);
    drawPageWatermark(doc, theme.pdf);
    doc.font("Helvetica").fontSize(7.5).fillColor(theme.pdf.colorMuted);
    doc.text(theme.pdf.footer, MARGIN, footerY, {
      width: CONTENT_WIDTH,
      align: "center",
      lineBreak: false,
    });
    doc.fontSize(7).text(`Page ${i + 1} of ${pageCount}`, MARGIN, footerY + 11, {
      width: CONTENT_WIDTH,
      align: "center",
      lineBreak: false,
    });
  }
}

/**
 * Branded customer-facing submission summary + consignment reference (not acceptance).
 */
export function generateCustomerConsignmentPdf({ body, items, photoGroups, submittedAt }) {
  return new Promise((resolve, reject) => {
    const brand = getConsignmentBrand(body, items);
    const theme = { brand, pdf: brand.pdf, policySections: getCustomerPolicySections(brand.key) };
    const chunks = [];
    const doc = new PDFDocument({
      size: "LETTER",
      margin: MARGIN,
      bufferPages: true,
      info: {
        Title:
          brand.key === "handbags"
            ? "Luxury Handbag Consignment Submission Summary"
            : "Consignment Submission Summary",
        Author: brand.pdf.author,
        Subject: "Submission copy and consignment reference",
      },
    });

    doc.on("data", (chunk) => chunks.push(chunk));
    doc.on("end", () => resolve(Buffer.concat(chunks)));
    doc.on("error", reject);

    drawPageChrome(doc, theme, { header: true });

    doc
      .font("Helvetica")
      .fontSize(10)
      .fillColor(theme.pdf.colorAccent)
      .text(theme.brand.pdfSummaryEyebrow || "Consignment Submission Summary", MARGIN);
    doc.moveDown(0.15);
    doc
      .font("Helvetica-Bold")
      .fontSize(17)
      .fillColor(theme.pdf.colorHeading)
      .text(theme.brand.pdfRecordTitle || "Your Submission Record", MARGIN);
    doc.moveDown(0.5);

    drawDisclaimer(doc, theme);

    doc.font("Helvetica-Bold").fontSize(10).fillColor(theme.pdf.colorAccent).text("Submitted", MARGIN);
    doc.font("Helvetica").fontSize(10).fillColor("#333333").text(submittedAt, MARGIN, doc.y + 2);
    doc.moveDown(0.7);

    doc.font("Helvetica-Bold").fontSize(11).fillColor(theme.pdf.colorHeading).text("Consignor", MARGIN);
    doc.moveDown(0.25);

    const address = formatAddress(body);
    const consignorRows = [
      ["Name", displayValue(body.customerName)],
      ["Email", displayValue(body.customerEmail)],
      ["Phone", displayValue(body.customerPhone)],
      ...(address ? [["Address", address]] : []),
      [
        "Pickup location",
        displayValue(body.pickupLocation || body.sameItemLocation),
      ],
      ["Pickup / delivery notes", displayValue(body.pickupNotes)],
      ["Source", displayValue(body.source)],
    ];

    for (const [label, value] of consignorRows) {
      drawLabelValue(doc, label, value, theme);
    }

    doc.moveDown(0.4);
    doc.font("Helvetica-Bold").fontSize(11).fillColor(theme.pdf.colorHeading).text("Summary", MARGIN);
    doc.moveDown(0.2);
    drawLabelValue(doc, "Items submitted", String(items.length), theme);
    drawLabelValue(doc, "Photos uploaded", String(countPhotos(photoGroups)), theme);

    addPageIfHasContent(doc, theme, { withHeader: true });
    doc.font("Helvetica-Bold").fontSize(14).fillColor(theme.pdf.colorHeading).text("Items Submitted", MARGIN);
    doc.moveDown(0.15);
    doc
      .font("Helvetica")
      .fontSize(9)
      .fillColor(theme.pdf.colorMuted)
      .text(theme.brand.pdfItemsIntro || "Details below reflect what you entered on our consignment form.", MARGIN, doc.y, {
        width: CONTENT_WIDTH,
      });
    doc.moveDown(0.6);

    for (let i = 0; i < items.length; i++) {
      const itemNumber = resolveItemNumber(items[i], i);
      const photos = photoGroups.get(itemNumber) || [];
      drawItemSection(doc, items[i], itemNumber, photos, brand.key, theme.pdf, theme);
    }

    addPageIfHasContent(doc, theme, { withHeader: true });
    doc
      .font("Helvetica-Bold")
      .fontSize(14)
      .fillColor(theme.pdf.colorHeading)
      .text(theme.brand.pdfPoliciesTitle || "For Accepted Items & Policies", MARGIN);
    doc.moveDown(0.2);
    doc
      .font("Helvetica")
      .fontSize(9)
      .fillColor(theme.pdf.colorMuted)
      .text(
        theme.brand.pdfPoliciesIntro ||
          "The following summarizes our standard services and consignment policies for your reference. Acceptance and contract terms are confirmed separately if your items are approved.",
        MARGIN,
        doc.y,
        { width: CONTENT_WIDTH, lineGap: 2 }
      );
    doc.moveDown(0.7);

    for (const section of theme.policySections) {
      drawPolicySection(doc, section, theme);
    }

    addPageFooters(doc, theme);
    doc.end();
  });
}
