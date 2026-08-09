import PDFDocument from "pdfkit";
import { preparePhotoGroupsForPdf } from "./consignmentImageNormalize.js";
import { formatItemDimensions, getConsignmentBrand, getItemDetailFields } from "./consignmentBrand.js";
import {
  displayValue,
  formatDimensions,
  resolveItemNumber,
  resolvePreferredSubmissionType,
} from "./consignmentValidation.js";
import {
  drawPdfPricingSection,
  findPricingForItem,
} from "./consignmentPricingDisplay.js";
import { drawPdfGoogleItemSearchLinks } from "./consignmentGoogleSearch.js";
import { drawConsignorProfileTable } from "./consignmentProfileDisplay.js";

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

function measureImageFit(doc, buffer, maxW, maxH) {
  const img = doc.openImage(buffer);
  const scale = Math.min(maxW / img.width, maxH / img.height, 1);
  return { width: img.width * scale, height: img.height * scale };
}

function drawImageFit(doc, buffer, maxW, maxH, label = "") {
  let width;
  let height;
  try {
    ({ width, height } = measureImageFit(doc, buffer, maxW, maxH));
  } catch {
    const suffix = label ? ` — ${label}` : "";
    doc
      .font("Helvetica-Oblique")
      .fontSize(9)
      .fillColor("#888")
      .text(`(Image could not be embedded${suffix})`, MARGIN);
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
 * PDF: consignor name at top; price early per item; full consignor profile at end.
 */
export async function generateConsignmentPdf({
  body,
  items,
  photoGroups,
  submittedAt,
  pricingResults = null,
}) {
  const pdfPhotoGroups = [...photoGroups.values()].some((photos) =>
    (photos || []).some((file) => file?.consignmentPhotoNormalized)
  )
    ? photoGroups
    : await preparePhotoGroupsForPdf(photoGroups);

  return new Promise((resolve, reject) => {
    const brand = getConsignmentBrand(body, items);
    const brandColor = brand.internalPdfColor || BRAND_COLOR;
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
    doc.moveDown(0.45);
    doc
      .fontSize(11)
      .fillColor("#222")
      .text(
        `Consignor: ${displayValue(body.customerName)} · ${displayValue(body.customerEmail)}`,
        MARGIN
      );
    doc.fontSize(10).fillColor("#666").text(`Generated: ${submittedAt}`, MARGIN);
    doc.moveDown(0.6);

    for (let i = 0; i < items.length; i++) {
      const item = items[i];
      const itemNumber = resolveItemNumber(item, i);
      const photos = pdfPhotoGroups.get(itemNumber) || [];

      if (i > 0) {
        addPageIfHasContent(doc);
      }

      doc.font("Helvetica-Bold").fontSize(14).fillColor(brandColor).text(`Item #${itemNumber}`, MARGIN);
      doc.font("Helvetica-Bold").fontSize(12).fillColor("#222").text(displayValue(item.itemName), MARGIN, doc.y + 2);
      doc.moveDown(0.25);
      if (item?._splitChild) {
        doc
          .font("Helvetica")
          .fontSize(9)
          .fillColor("#8a3b12")
          .text(
            `Split piece ${item.splitPieceIndex} of ${item.splitPieceCount} from submission Item #${item.splitFromItemNumber} (confidence ${String(item.splitConfidence || "").toUpperCase()})`,
            MARGIN,
            doc.y,
            { width: CONTENT_WIDTH }
          );
        doc.moveDown(0.2);
      }

      // Pricing FIRST — main thing
      const pricing = findPricingForItem(pricingResults, itemNumber);
      drawPdfPricingSection(doc, pricing, {
        brandColor,
        margin: MARGIN,
        contentWidth: CONTENT_WIDTH,
        contentBottom: pageContentBottom(doc),
        item,
      });

      // Compact item fields after pricing
      doc.font("Helvetica-Bold").fontSize(10).fillColor(brandColor).text("Item details", MARGIN);
      doc.moveDown(0.15);
      const fields = [
        ...getItemDetailFields(item, brand.key)
          .filter(([label]) => label !== "Warnings")
          .map(([label, value]) => [
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
        doc.moveDown(0.4);
        doc.font("Helvetica-Bold").fontSize(11).fillColor(brandColor).text("Photos", MARGIN);
        doc.moveDown(0.2);

        for (let p = 0; p < photos.length; p++) {
          const file = photos[p];
          let blockHeight = 36 + IMAGE_GAP;
          try {
            const { height } = measureImageFit(doc, file.buffer, CONTENT_WIDTH, MAX_IMAGE_HEIGHT);
            blockHeight = 36 + height + IMAGE_GAP;
          } catch {
            blockHeight = 50 + IMAGE_GAP;
          }
          ensureVerticalSpace(doc, blockHeight);

          doc.font("Helvetica").fontSize(9).fillColor("#555").text(`Photo ${p + 1}`, MARGIN);
          doc.y = doc.y + 2;
          drawPdfGoogleItemSearchLinks(doc, item, {
            margin: MARGIN,
            contentWidth: CONTENT_WIDTH,
            photoIndex: p + 1,
            lensUrl: file.googleLensUrl,
          });
          drawImageFit(
            doc,
            file.buffer,
            CONTENT_WIDTH,
            MAX_IMAGE_HEIGHT,
            file.originalname || file.mimetype || ""
          );
        }
      }
    }

    // Consignor profile at the very end
    addPageIfHasContent(doc);
    doc.font("Helvetica-Bold").fontSize(13).fillColor(brandColor).text("Additional consignor information", MARGIN);
    doc.moveDown(0.2);
    doc
      .font("Helvetica")
      .fontSize(9.5)
      .fillColor("#666")
      .text("Full consignor contact and submission details for the team.", MARGIN, doc.y, {
        width: CONTENT_WIDTH,
      });
    doc.moveDown(0.35);
    drawConsignorProfileTable(
      doc,
      {
        name: displayValue(body.customerName),
        email: displayValue(body.customerEmail),
        phone: displayValue(body.customerPhone),
        address: formatAddress(body),
        itemLocation: displayValue(body.sameItemLocation),
        pickup: displayValue(body.pickupNotes || body.pickupLocation),
        preferredType: resolvePreferredSubmissionType(body),
        source: displayValue(body.source),
        category: displayValue(body.submissionCategory),
        submitted: displayValue(submittedAt),
        items: String(items.length),
        photos: String(
          [...pdfPhotoGroups.values()].reduce((n, photos) => n + (photos?.length || 0), 0)
        ),
      },
      { margin: MARGIN, contentWidth: CONTENT_WIDTH }
    );

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
