import PDFDocument from "pdfkit";
import { displayValue, resolveItemNumber } from "./consignmentValidation.js";

const BRAND_COLOR = "#4a5d3f";
const BRAND_LIGHT = "#eef1eb";
const MARGIN = 50;
const CONTENT_WIDTH = 512;
const HEADER_BAR_HEIGHT = 72;
const MAX_IMAGE_HEIGHT = 280;
const THUMB_MAX = 72;
const IMAGE_GAP = 10;
const FOOTER_RESERVE = 40;

/** Branded reference copy — paraphrased for flow; legal meaning preserved. */
const REFERENCE_SECTIONS = [
  {
    title: "Design Services",
    paragraphs: [
      "Lost & Found Resale Interiors offers in-house design services. Our designers can help you refresh a room or plan a full home update. In-home design consultations are available for a fee—ask our team for current rates and scheduling.",
    ],
  },
  {
    title: "Consignment Terms",
    paragraphs: [
      "Consignment is a 50/50 split between consignor and Lost & Found Resale Interiors, LLC. Each consignment runs on a 90-day agreement. We typically price accepted items at 30–50% of estimated retail value, based on condition, demand, and our pricing standards.",
      "After your items are accepted, we send digital consignment contracts and related paperwork by email for your review and signature. Items must meet our quality standards: clean, complete, and in sellable condition. We reserve the right to decline items that do not fit our showroom, brand, or condition requirements.",
      "By submitting items for consignment, you represent that you own the items or have authority to consign them, and that descriptions and photos are accurate to the best of your knowledge.",
    ],
  },
  {
    title: "What Happens After the 90-Day Contract",
    paragraphs: [
      "When your 90-day consignment term ends, a 7-day grace period applies. By day 97, any remaining unsold items must be picked up by you, donated through our charity program (where offered), or moved to store inventory per our policies and your prior agreements.",
    ],
  },
  {
    title: "Post-90-Day Options",
    paragraphs: [
      "If items remain unsold after the contract period, you may retrieve them (by appointment), donate them to a partner charity (where available), or request a contract extension. Extensions, when approved, may use a 65/35 consignor/store split for the extended term—details are confirmed in writing.",
    ],
  },
  {
    title: "Pricing Structure",
    paragraphs: [
      "Our showroom pricing generally targets 30–50% of estimated retail for comparable pieces in similar condition. We use market data, condition, and demand— including tools informed by years of Scottsdale resale experience since 2012 — to set fair, competitive prices. Final prices may be adjusted with your agreement when appropriate.",
    ],
  },
  {
    title: "Condition and Acceptance",
    appliesIfAccepted: true,
    paragraphs: [
      "Accepted items must be clean and presentation-ready. If professional cleaning is required before we can merchandise an item, a $25 cleaning fee may apply (we will communicate this before proceeding).",
    ],
  },
  {
    title: "Additional Fees and Discounting",
    appliesIfAccepted: true,
    paragraphs: [
      "Credit card sales may include a 3% processing fee where applicable. Discounting on slow-moving inventory is not automatic: we may discuss price adjustments after 30 days (up to ~15% reduction) or after 60 days (up to ~35% reduction) with your input, depending on the item and market.",
    ],
  },
  {
    title: "Check Issuance",
    appliesIfAccepted: true,
    paragraphs: [
      "Consignor payments are typically issued on the 10th of the month following a sale, by check. Checks may be picked up at our Scottsdale showroom or mailed to the address on file—confirm your preference with our team.",
    ],
  },
  {
    title: "Prices Subject to Verification",
    paragraphs: [
      "Retail references, original purchase prices, and online comparables you provide are helpful but not guaranteed. We verify pricing and condition in person before finalizing showroom tags and contracts.",
    ],
  },
  {
    title: "Out-of-State Shipping",
    paragraphs: [
      "For furniture and large pieces shipped outside Arizona, we recommend obtaining quotes from multiple freight providers. Suggested starting points:",
      "• FreightCenter (recommended starting point)",
      "• FreightQuote",
      "• uShip",
      "Please request roll-wrapped, blanket-wrapped, or similar protective service. Liftgate delivery is often required when there is no loading dock. Consignors are responsible for coordinating pickup, freight payment, and delivery windows unless otherwise agreed in writing.",
    ],
  },
  {
    title: "Local Delivery Options",
    appliesIfAccepted: true,
    paragraphs: [
      "Local white-glove delivery within our service area is available at $95 per hour and up (two-person team, minimums may apply). Items are typically staged on our floor for up to three business days before delivery scheduling. Contact us for a quote tailored to your address, stairs, and item size.",
    ],
  },
  {
    title: "Donation Services",
    appliesIfAccepted: true,
    paragraphs: [
      "Once your items are accepted into consignment, our team will follow up by email with next steps. Third-party delivery and donation pickup companies are independent contractors; consignors are generally responsible for delivery fees unless we specify otherwise in your agreement.",
    ],
  },
  {
    title: "Our Handbag Division",
    paragraphs: [
      "Lost + Found Resale Handbags specializes in authenticated luxury handbags, accessories, and fine jewelry. Visit lostandfoundhandbags.com to shop or learn more about consigning luxury pieces with our dedicated team.",
    ],
  },
  {
    title: "Connect With Us",
    paragraphs: [
      "Lost & Found Resale Interiors, LLC — Scottsdale, Arizona",
      "Phone: 480-588-7006",
      "Furniture & home: lostandfoundresale.com",
      "Luxury handbags: lostandfoundhandbags.com",
      "Follow us on Instagram and Facebook @lostandfoundresale (resale & furniture) and @lost.foundluxury (luxury).",
    ],
  },
];

function pageContentBottom(doc) {
  return doc.page.height - MARGIN - FOOTER_RESERVE;
}

function formatAddress(body) {
  const parts = [body.customerStreetAddress, body.customerCity, body.customerState, body.customerZip]
    .map((p) => (p == null ? "" : String(p).trim()))
    .filter(Boolean);
  return parts.length ? parts.join(", ") : null;
}

function customerItemLine(item) {
  const name = String(item?.itemName ?? "").trim() || "Unnamed Item";
  const category = String(item?.category ?? "").trim() || "Not provided";
  const condition = String(item?.condition ?? "").trim() || "Not provided";
  return `${name} - ${category} - ${condition}`;
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

function ensureVerticalSpace(doc, neededHeight) {
  if (doc.y + neededHeight > pageContentBottom(doc)) {
    doc.addPage();
    drawPageWatermark(doc);
  }
}

function drawImageFit(doc, buffer, maxW, maxH) {
  let width;
  let height;
  try {
    ({ width, height } = measureImageFit(doc, buffer, maxW, maxH));
  } catch {
    doc.font("Helvetica-Oblique").fontSize(8).fillColor("#888").text("(Photo unavailable)", MARGIN);
    doc.y = doc.y + 12 + IMAGE_GAP;
    return doc.y;
  }
  ensureVerticalSpace(doc, height + IMAGE_GAP);
  const y = doc.y;
  doc.image(buffer, MARGIN, y, { width, height });
  doc.y = y + height + IMAGE_GAP;
  return doc.y;
}

function drawPageWatermark(doc) {
  const cx = doc.page.width / 2;
  const cy = doc.page.height / 2;
  doc.save();
  doc.rotate(-35, { origin: [cx, cy] });
  doc.font("Helvetica-Bold").fontSize(42).fillColor("#4a5d3f", 0.06);
  doc.text("LOST + FOUND", cx - 200, cy - 20, { width: 400, align: "center" });
  doc.font("Helvetica").fontSize(14).fillColor("#4a5d3f", 0.05);
  doc.text("Lost & Found Resale Interiors", cx - 200, cy + 28, { width: 400, align: "center" });
  doc.restore();
}

function drawHeaderBar(doc) {
  doc.save();
  doc.rect(0, 0, doc.page.width, HEADER_BAR_HEIGHT).fill(BRAND_COLOR);
  doc.fillColor("#ffffff").font("Helvetica-Bold").fontSize(11).text("LOST + FOUND", MARGIN, 18, {
    characterSpacing: 1.2,
  });
  doc.font("Helvetica").fontSize(9).text("Lost & Found Resale Interiors, LLC", MARGIN, 34);
  doc.fontSize(8).fillColor("#d8e0d4").text("Scottsdale · 480-588-7006 · lostandfoundresale.com", MARGIN, 48);
  doc.restore();
  doc.y = HEADER_BAR_HEIGHT + 20;
}

function drawLabelValue(doc, label, value, { indent = 0 } = {}) {
  const x = MARGIN + indent;
  doc
    .font("Helvetica-Bold")
    .fontSize(9.5)
    .fillColor("#333")
    .text(`${label}: `, x, doc.y, { continued: true, width: CONTENT_WIDTH - indent });
  doc.font("Helvetica").text(String(value), { width: CONTENT_WIDTH - indent });
  return doc.y + 4;
}

function drawDisclaimer(doc) {
  const pad = 10;
  const innerW = CONTENT_WIDTH - pad * 2;
  const title = "Important — This Is Not an Acceptance Notice";
  const body =
    "This document is a receipt of what you submitted online. It does not guarantee that your items will be accepted for consignment. Our team will review your submission and follow up by email with next steps if your pieces are a good fit for the showroom.";
  doc.font("Helvetica-Bold").fontSize(10);
  const titleH = doc.heightOfString(title, { width: innerW });
  doc.font("Helvetica").fontSize(9);
  const bodyH = doc.heightOfString(body, { width: innerW, paragraphGap: 4 });
  const boxHeight = pad * 2 + titleH + 6 + bodyH;
  const boxY = doc.y;
  doc.roundedRect(MARGIN, boxY, CONTENT_WIDTH, boxHeight, 4).fill(BRAND_LIGHT);
  doc
    .roundedRect(MARGIN, boxY, CONTENT_WIDTH, boxHeight, 4)
    .lineWidth(1)
    .strokeColor(BRAND_COLOR)
    .stroke();
  doc.font("Helvetica-Bold").fontSize(10).fillColor(BRAND_COLOR);
  doc.text(title, MARGIN + pad, boxY + pad, { width: innerW });
  doc.font("Helvetica").fontSize(9).fillColor("#333");
  doc.text(body, MARGIN + pad, boxY + pad + titleH + 6, { width: innerW, paragraphGap: 4 });
  doc.y = boxY + boxHeight + 14;
}

function collectThumbnailBuffers(photoGroups, items, max = 8) {
  const thumbs = [];
  for (let i = 0; i < items.length && thumbs.length < max; i++) {
    const itemNumber = resolveItemNumber(items[i], i);
    const photos = photoGroups.get(itemNumber) || [];
    if (photos[0]?.buffer) thumbs.push(photos[0].buffer);
  }
  return thumbs;
}

function drawThumbnailRow(doc, buffers) {
  if (!buffers.length) return;
  const cols = Math.min(buffers.length, 4);
  const cellW = (CONTENT_WIDTH - (cols - 1) * 8) / cols;
  const rowH = THUMB_MAX + 16;
  ensureVerticalSpace(doc, rowH + 8);
  doc.font("Helvetica-Bold").fontSize(9).fillColor(BRAND_COLOR).text("Submitted photos (preview)", MARGIN);
  doc.y += 4;
  const rowY = doc.y;
  for (let i = 0; i < buffers.length; i++) {
    const col = i % cols;
    const row = Math.floor(i / cols);
    if (row > 0 && col === 0) {
      doc.y = rowY + row * (THUMB_MAX + 12);
    }
    const x = MARGIN + col * (cellW + 8);
    const y = row === 0 ? rowY : rowY + row * (THUMB_MAX + 12);
    try {
      const { width, height } = measureImageFit(doc, buffers[i], cellW - 4, THUMB_MAX);
      doc.image(buffers[i], x + (cellW - width) / 2, y, { width, height });
    } catch {
      doc.rect(x, y, cellW, THUMB_MAX).strokeColor("#ccc").stroke();
    }
  }
  const rows = Math.ceil(buffers.length / cols);
  doc.y = rowY + rows * (THUMB_MAX + 12) + 8;
}

function drawSection(doc, section) {
  ensureVerticalSpace(doc, 60);
  let heading = section.title;
  if (section.appliesIfAccepted) {
    heading += " (Applies if your items are accepted)";
  }
  doc.font("Helvetica-Bold").fontSize(12).fillColor(BRAND_COLOR).text(heading, MARGIN, doc.y, {
    width: CONTENT_WIDTH,
  });
  doc.moveDown(0.35);
  doc.font("Helvetica").fontSize(9.5).fillColor("#333");
  for (const para of section.paragraphs) {
    if (para.startsWith("•")) {
      ensureVerticalSpace(doc, 20);
      doc.text(para, MARGIN + 8, doc.y, { width: CONTENT_WIDTH - 8, paragraphGap: 3 });
    } else {
      const h = doc.heightOfString(para, { width: CONTENT_WIDTH, paragraphGap: 4 });
      ensureVerticalSpace(doc, h + 8);
      doc.text(para, MARGIN, doc.y, { width: CONTENT_WIDTH, paragraphGap: 4, align: "left" });
    }
    doc.moveDown(0.25);
  }
  doc.moveDown(0.6);
}

function addFormFields(doc, body, submittedAt) {
  if (typeof doc.initForm !== "function" || typeof doc.formText !== "function") {
    return;
  }
  doc.font("Helvetica");
  doc.initForm();
  const fieldY = doc.y;
  const fieldW = CONTENT_WIDTH;
  const fieldH = 16;
  const gap = 22;
  const labels = [
    ["consignorName", "Consignor name", displayValue(body.customerName)],
    ["consignorEmail", "Email", displayValue(body.customerEmail)],
    ["consignorPhone", "Phone", displayValue(body.customerPhone)],
    ["submissionDate", "Submission date", submittedAt],
  ];
  doc.font("Helvetica-Bold").fontSize(9).fillColor(BRAND_COLOR).text("Editable contact fields", MARGIN, fieldY);
  let y = fieldY + 14;
  for (const [name, label, value] of labels) {
    doc.font("Helvetica").fontSize(8).fillColor("#555").text(label, MARGIN, y);
    y += 11;
    doc.formText(name, MARGIN, y, fieldW, fieldH, {
      value: value === "Not provided" ? "" : value,
      fontSize: 10,
    });
    y += gap;
  }
  doc.y = y + 6;
}

function addPageFooters(doc) {
  const range = doc.bufferedPageRange();
  const pageCount = range.count;
  for (let i = 0; i < pageCount; i++) {
    doc.switchToPage(range.start + i);
    drawPageWatermark(doc);
    doc.font("Helvetica").fontSize(7.5).fillColor("#888");
    doc.text(
      `Page ${i + 1} of ${pageCount} · Lost & Found Resale Interiors · lostandfoundresale.com`,
      MARGIN,
      doc.page.height - MARGIN + 8,
      { width: CONTENT_WIDTH, align: "center" }
    );
  }
}

/**
 * Branded customer-facing submission summary + consignment reference guide.
 * No internal pricing comps; prominent non-acceptance disclaimer.
 */
export function generateCustomerConsignmentPdf({ body, items, photoGroups, submittedAt }) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    const doc = new PDFDocument({
      size: "LETTER",
      margin: MARGIN,
      bufferPages: true,
      info: {
        Title: "Consignment Submission Summary",
        Author: "Lost & Found Resale Interiors, LLC",
        Subject: "Submission receipt and consignment reference",
      },
    });

    doc.on("data", (chunk) => chunks.push(chunk));
    doc.on("end", () => resolve(Buffer.concat(chunks)));
    doc.on("error", reject);

    drawPageWatermark(doc);
    drawHeaderBar(doc);

    doc.font("Helvetica-Bold").fontSize(18).fillColor(BRAND_COLOR).text("Consignment Submission Summary", MARGIN);
    doc.moveDown(0.4);
    drawDisclaimer(doc);

    const address = formatAddress(body);
    const summaryRows = [
      ["Consignor", displayValue(body.customerName)],
      ["Email", displayValue(body.customerEmail)],
      ["Phone", displayValue(body.customerPhone)],
      ...(address ? [["Address", address]] : []),
      ["Submitted", submittedAt],
      ["Items submitted", String(items.length)],
      ["Photos uploaded", String(countPhotos(photoGroups))],
    ];

    doc.font("Helvetica-Bold").fontSize(11).fillColor(BRAND_COLOR).text("Your submission", MARGIN);
    doc.moveDown(0.25);
    for (const [label, value] of summaryRows) {
      drawLabelValue(doc, label, value);
    }
    doc.moveDown(0.3);

    doc.font("Helvetica-Bold").fontSize(11).fillColor(BRAND_COLOR).text("Items", MARGIN);
    doc.moveDown(0.2);
    doc.font("Helvetica").fontSize(9.5).fillColor("#333");
    for (let i = 0; i < items.length; i++) {
      ensureVerticalSpace(doc, 14);
      doc.text(`${i + 1}. ${customerItemLine(items[i])}`, MARGIN, doc.y, { width: CONTENT_WIDTH });
    }
    doc.moveDown(0.5);

    const thumbs = collectThumbnailBuffers(photoGroups, items);
    if (thumbs.length) {
      drawThumbnailRow(doc, thumbs);
    }

    addFormFields(doc, body, submittedAt);

    doc.addPage();
    drawPageWatermark(doc);
    drawHeaderBar(doc);
    doc.font("Helvetica-Bold").fontSize(15).fillColor(BRAND_COLOR).text("Consignment Reference Guide", MARGIN);
    doc.moveDown(0.2);
    doc
      .font("Helvetica")
      .fontSize(9)
      .fillColor("#555")
      .text(
        "The following summarizes our standard policies and services. This guide is for your records; acceptance and contract terms are confirmed separately if your items are approved.",
        MARGIN,
        doc.y,
        { width: CONTENT_WIDTH }
      );
    doc.moveDown(0.8);

    for (const section of REFERENCE_SECTIONS) {
      drawSection(doc, section);
    }

    addPageFooters(doc);
    doc.end();
  });
}
