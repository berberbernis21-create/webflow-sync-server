import PDFDocument from "pdfkit";
import {
  displayValue,
  formatDimensions,
  resolveItemNumber,
} from "./consignmentValidation.js";

const BRAND_COLOR = "#1a3c34";
const MARGIN = 50;
const CONTENT_WIDTH = 512;

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

/** Scale image to fit within max width/height for PDF layout. */
function drawImageFit(doc, buffer, x, y, maxW, maxH) {
  try {
    doc.image(buffer, x, y, { fit: [maxW, maxH], align: "center" });
    return doc.y;
  } catch {
    doc.font("Helvetica-Oblique").fontSize(9).fillColor("#888").text("(Image could not be embedded)", x, y);
    return doc.y + 14;
  }
}

/**
 * Generate a polished consignment PDF: header, consignor block, per-item sections
 * with fields and photos, page break between items, page numbers in footer.
 */
export function generateConsignmentPdf({ body, items, photoGroups, submittedAt }) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    const doc = new PDFDocument({
      size: "LETTER",
      margin: MARGIN,
      bufferPages: true,
      info: {
        Title: "Consignment Submission",
        Author: "L&F Resale Interiors",
      },
    });

    doc.on("data", (chunk) => chunks.push(chunk));
    doc.on("end", () => resolve(Buffer.concat(chunks)));
    doc.on("error", reject);

    doc.font("Helvetica-Bold").fontSize(22).fillColor(BRAND_COLOR).text("L&F Resale Interiors", MARGIN, MARGIN);
    doc.font("Helvetica").fontSize(14).fillColor("#444").text("Consignment Submission", MARGIN, doc.y + 4);
    doc.moveDown(0.5);
    doc.fontSize(10).fillColor("#666").text(`Generated: ${submittedAt}`, MARGIN);

    doc.moveDown(1);
    doc.font("Helvetica-Bold").fontSize(13).fillColor(BRAND_COLOR).text("Consignor Information", MARGIN);
    doc.moveDown(0.3);

    const consignorRows = [
      ["Name", displayValue(body.customerName)],
      ["Email", displayValue(body.customerEmail)],
      ["Phone", displayValue(body.customerPhone)],
      ["Address", formatAddress(body)],
      ["Item location", displayValue(body.sameItemLocation)],
      ["Pickup / delivery notes", displayValue(body.pickupNotes || body.pickupLocation)],
      ["Source", displayValue(body.source)],
    ];

    for (const [label, value] of consignorRows) {
      drawLabelValue(doc, label, value);
    }

    for (let i = 0; i < items.length; i++) {
      const item = items[i];
      const itemNumber = resolveItemNumber(item, i);
      const photos = photoGroups.get(itemNumber) || [];

      if (i > 0) {
        doc.addPage();
      } else {
        doc.moveDown(1);
      }

      doc.font("Helvetica-Bold").fontSize(14).fillColor(BRAND_COLOR).text(`Item #${itemNumber}`, MARGIN);
      doc.font("Helvetica-Bold").fontSize(12).fillColor("#222").text(displayValue(item.itemName), MARGIN, doc.y + 2);
      doc.moveDown(0.5);

      const fields = [
        ["Category", displayValue(item.category)],
        ["Brand / maker", displayValue(item.brand)],
        ["Age", displayValue(item.age)],
        ["Condition", displayValue(item.condition)],
        ["Original price", displayValue(item.originalPrice)],
        ["Dimensions (W×D×H)", formatDimensions(item)],
        ["Condition notes", displayValue(item.conditionNotes)],
        ["Notes", displayValue(item.notes)],
        ["Warnings", displayValue(item.warnings)],
        ["Photo count", String(photos.length)],
      ];

      for (const [label, value] of fields) {
        if (doc.y > doc.page.height - 120) doc.addPage();
        drawLabelValue(doc, label, value);
      }

      if (photos.length) {
        doc.moveDown(0.5);
        doc.font("Helvetica-Bold").fontSize(11).fillColor(BRAND_COLOR).text("Photos", MARGIN);
        doc.moveDown(0.3);

        for (let p = 0; p < photos.length; p++) {
          const file = photos[p];
          if (doc.y > doc.page.height - 200) {
            doc.addPage();
          }
          doc.font("Helvetica").fontSize(9).fillColor("#555").text(`Photo ${p + 1}`, MARGIN);
          const imgY = doc.y + 4;
          drawImageFit(doc, file.buffer, MARGIN, imgY, CONTENT_WIDTH, 220);
          doc.moveDown(0.5);
        }
      }
    }

    const range = doc.bufferedPageRange();
    const pageCount = range.count;
    for (let i = 0; i < pageCount; i++) {
      doc.switchToPage(range.start + i);
      doc.font("Helvetica").fontSize(8).fillColor("#999").text(
        `Page ${i + 1} of ${pageCount}`,
        MARGIN,
        doc.page.height - MARGIN + 10,
        { width: CONTENT_WIDTH, align: "center" }
      );
    }

    doc.end();
  });
}
