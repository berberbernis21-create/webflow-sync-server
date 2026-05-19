import PDFDocument from "pdfkit";
import { formatItemDimensions, getConsignmentBrand, getItemDetailFields } from "./consignmentBrand.js";
import {
  displayValue,
  formatDimensions,
  resolveItemNumber,
} from "./consignmentValidation.js";

const BRAND_COLOR = "#1a3c34";
const MARGIN = 50;
const CONTENT_WIDTH = 512;
const MAX_IMAGE_HEIGHT = 280;
const IMAGE_GAP = 14;
const FOOTER_RESERVE = 36;
const CONTENT_TOP_TOLERANCE = 4;
const FIELD_BLOCK_ESTIMATE = 28;

function pageContentBottom(doc) {
  return doc.page.height - MARGIN - FOOTER_RESERVE;
}

function isPageEffectivelyBlank(doc) {
  return doc.y <= MARGIN + CONTENT_TOP_TOLERANCE;
}

/** Add a page only when the current page already has body content below the top margin. */
function addPageIfHasContent(doc) {
  if (!isPageEffectivelyBlank(doc)) {
    doc.addPage();
  }
}

function ensureVerticalSpace(doc, neededHeight) {
  if (doc.y + neededHeight <= pageContentBottom(doc)) return;
  if (!isPageEffectivelyBlank(doc)) {
    doc.addPage();
  }
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

function drawLabelValue(doc, label, value) {
  doc
    .font("Helvetica-Bold")
    .fontSize(10)
    .fillColor("#333")
    .text(`${label}: `, MARGIN, doc.y, { continued: true, width: CONTENT_WIDTH });
  doc.font("Helvetica").text(String(value), { width: CONTENT_WIDTH });
  return doc.y + 6;
}

/** Natural dimensions scaled to max width/height, aspect ratio preserved. */
function measureImageFit(doc, buffer, maxW, maxH) {
  const img = doc.openImage(buffer);
  const scale = Math.min(maxW / img.width, maxH / img.height, 1);
  return { width: img.width * scale, height: img.height * scale };
}

/**
 * Draw image at doc.y, then advance doc.y past the image + gap.
 * PDFKit does not update doc.y when x/y are passed explicitly.
 */
function drawImageFit(doc, buffer, maxW, maxH) {
  let width;
  let height;
  try {
    ({ width, height } = measureImageFit(doc, buffer, maxW, maxH));
  } catch {
    doc.font("Helvetica-Oblique").fontSize(9).fillColor("#888").text("(Image could not be embedded)", MARGIN);
    doc.y = doc.y + 14 + IMAGE_GAP;
    return doc.y;
  }

  ensureVerticalSpace(doc, height + IMAGE_GAP);
  const y = doc.y;
  doc.image(buffer, MARGIN, y, { width, height });
  doc.y = y + height + IMAGE_GAP;
  return doc.y;
}

/**
 * Generate a polished consignment PDF: header, consignor block, per-item sections
 * with fields and photos, page break between items, page numbers in footer.
 */
export function generateConsignmentPdf({ body, items, photoGroups, submittedAt }) {
  return new Promise((resolve, reject) => {
    const brand = getConsignmentBrand(body, items);
    const brandColor = brand.internalPdfColor;
    const chunks = [];
    const doc = new PDFDocument({
      size: "LETTER",
      margin: MARGIN,
      bufferPages: true,
      info: {
        Title: "Consignment Submission",
        Author: brand.shortName,
      },
    });

    doc.on("data", (chunk) => chunks.push(chunk));
    doc.on("end", () => resolve(Buffer.concat(chunks)));
    doc.on("error", reject);

    doc.font("Helvetica-Bold").fontSize(22).fillColor(brandColor).text(brand.internalTitle, MARGIN, MARGIN);
    doc.font("Helvetica").fontSize(14).fillColor("#444").text("Consignment Submission", MARGIN, doc.y + 4);
    doc.moveDown(0.5);
    doc.fontSize(10).fillColor("#666").text(`Generated: ${submittedAt}`, MARGIN);

    doc.moveDown(1);
    doc.font("Helvetica-Bold").fontSize(13).fillColor(brandColor).text("Consignor Information", MARGIN);
    doc.moveDown(0.3);

    const consignorRows = [
      ["Name", displayValue(body.customerName)],
      ["Email", displayValue(body.customerEmail)],
      ["Phone", displayValue(body.customerPhone)],
      ["Address", formatAddress(body)],
      ["Item location", displayValue(body.sameItemLocation)],
      ["Pickup / delivery notes", displayValue(body.pickupNotes || body.pickupLocation)],
      ["Source", displayValue(body.source)],
      ["Submission category", displayValue(body.submissionCategory)],
    ];

    for (const [label, value] of consignorRows) {
      drawLabelValue(doc, label, value);
    }

    for (let i = 0; i < items.length; i++) {
      const item = items[i];
      const itemNumber = resolveItemNumber(item, i);
      const photos = photoGroups.get(itemNumber) || [];

      if (i > 0) {
        addPageIfHasContent(doc);
      } else {
        doc.moveDown(1);
      }

      doc.font("Helvetica-Bold").fontSize(14).fillColor(brandColor).text(`Item #${itemNumber}`, MARGIN);
      doc.font("Helvetica-Bold").fontSize(12).fillColor("#222").text(displayValue(item.itemName), MARGIN, doc.y + 2);
      doc.moveDown(0.5);

      const fields = [
        ...getItemDetailFields(item, brand.key).map(([label, value]) => [
          label,
          label.includes("Dimensions") || label.includes("size")
            ? brand.key === "handbags"
              ? formatItemDimensions(item, brand.key)
              : formatDimensions(item)
            : displayValue(value),
        ]),
        ["Photo count", String(photos.length)],
      ];

      for (const [label, value] of fields) {
        ensureVerticalSpace(doc, FIELD_BLOCK_ESTIMATE);
        drawLabelValue(doc, label, value);
      }

      if (photos.length) {
        doc.moveDown(0.5);
        doc.font("Helvetica-Bold").fontSize(11).fillColor(brandColor).text("Photos", MARGIN);
        doc.moveDown(0.3);

        for (let p = 0; p < photos.length; p++) {
          const file = photos[p];
          let blockHeight = 12 + IMAGE_GAP;
          try {
            const { height } = measureImageFit(doc, file.buffer, CONTENT_WIDTH, MAX_IMAGE_HEIGHT);
            blockHeight = 12 + height + IMAGE_GAP;
          } catch {
            blockHeight = 26 + IMAGE_GAP;
          }
          ensureVerticalSpace(doc, blockHeight);

          doc.font("Helvetica").fontSize(9).fillColor("#555").text(`Photo ${p + 1}`, MARGIN);
          doc.y = doc.y + 4;
          drawImageFit(doc, file.buffer, CONTENT_WIDTH, MAX_IMAGE_HEIGHT);
        }
      }
    }

    const range = doc.bufferedPageRange();
    const pageCount = range.count;
    const footerY = pageContentBottom(doc) + 10;
    for (let i = 0; i < pageCount; i++) {
      doc.switchToPage(range.start + i);
      doc.font("Helvetica").fontSize(8).fillColor("#999").text(
        `Page ${i + 1} of ${pageCount}`,
        MARGIN,
        footerY,
        { width: CONTENT_WIDTH, align: "center", lineBreak: false }
      );
    }

    doc.end();
  });
}
