import PDFDocument from "pdfkit";
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

const DISCLAIMER_TITLE = "Important — Not an Acceptance Notice";
const DISCLAIMER_BODY =
  "This document is a copy of your submission only. It does NOT mean your items have been accepted for consignment. Our team will review your submission and contact you by email if your items are approved.";

/** Policy reference sections (paraphrased; legal meaning preserved). */
const POLICY_SECTIONS = [
  {
    title: "Design Services",
    paragraphs: [
      "Lost & Found Resale Interiors offers in-house design services. Our designers can help refresh a single room or plan a full-home update. In-home design consultations are available for a fee—contact us for current rates and scheduling.",
    ],
  },
  {
    title: "What Happens After the 90-Day Contract",
    paragraphs: [
      "Each consignment agreement runs for 90 days. When that term ends, a 7-day grace period applies. By day 97, any remaining unsold items must be picked up by you, donated through our charity program (where offered), or handled per your written agreement—including movement to store inventory when applicable.",
    ],
  },
  {
    title: "Prices Subject to Verification",
    paragraphs: [
      "Retail references, original purchase prices, and online comparables you provide are helpful but not guaranteed. We verify pricing, condition, and market demand in person before finalizing showroom tags and contracts.",
    ],
  },
  {
    title: "Out-of-State Shipping",
    paragraphs: [
      "For furniture and large pieces shipped outside Arizona, obtain quotes from multiple freight providers. Suggested starting points:",
      "• FreightCenter (recommended): freightcenter.com",
      "• FreightQuote: freightquote.com",
      "• uShip: uship.com",
      "Request roll-wrapped or blanket-wrapped service. Liftgate delivery is often required when there is no loading dock. Consignors are responsible for coordinating pickup, freight payment, and delivery windows unless otherwise agreed in writing.",
    ],
  },
  {
    title: "Local Delivery Options",
    paragraphs: [
      "Local white-glove delivery within our service area is available at $95 per hour and up (two-person team; minimums may apply). Accepted items are typically staged on our floor for up to three business days before delivery scheduling. Contact us for a quote based on your address, stairs, and item size.",
    ],
  },
  {
    title: "Consignment Policy",
    paragraphs: [
      "Accepted items are generally priced at 30–50% of estimated retail value, based on condition, demand, and our pricing standards. Consignment uses a 50/50 split between consignor and Lost & Found Resale Interiors, LLC on a standard 90-day agreement.",
      "Credit card sales may include a 3% processing fee where applicable. Markdowns on slow-moving inventory may be discussed by length of stay—for example, up to approximately 15% after 30 days or up to approximately 35% after 60 days—with your input when appropriate.",
    ],
  },
  {
    title: "Our Handbag Division",
    paragraphs: [
      "Lost + Found Resale Handbags is our dedicated luxury division for authenticated designer handbags, accessories, and fine jewelry—curated Scottsdale pieces you can explore at lostandfoundhandbags.com.",
    ],
  },
  {
    title: "Donation Services",
    paragraphs: [
      "When you request donation at the end of a consignment term (or per your agreement), our team can coordinate donation to partner charities where available. Donation pickup and logistics may involve third-party services; fees and scheduling are confirmed in writing.",
    ],
  },
  {
    title: "Once Items Are Accepted",
    paragraphs: [
      "After your items are accepted, we follow up by email with digital consignment contracts and next steps. Third-party delivery, freight, and donation pickup companies are independent contractors. Unless we specify otherwise in your agreement, consignors are responsible for delivery and pickup costs.",
    ],
  },
  {
    title: "Consignment Terms",
    paragraphs: [
      "Consignment is a 50/50 split between consignor and Lost & Found Resale Interiors, LLC on a 90-day agreement. After acceptance, we send digital contracts and related paperwork by email for your review and signature.",
      "We reserve the right to decline items that do not fit our showroom, brand, or condition requirements. Accepted items must be clean, complete, and in sellable condition. You represent that you own the items or have authority to consign them, and that descriptions and photos are accurate to the best of your knowledge.",
      "If items remain unsold after the contract period, you may retrieve them (by appointment), donate them where offered, or request an extension. Approved extensions may use a 65/35 consignor/store split for the extended term—details are confirmed in writing.",
      "Our pricing uses market data, condition, and demand—including tools informed by years of Scottsdale resale experience and comparable-market analysis—to set fair, competitive showroom prices, generally targeting 30–50% of estimated retail for similar pieces.",
      "If professional cleaning is required before we can merchandise an item, a $25 cleaning fee may apply (we communicate this before proceeding). Discounting is not automatic; we may discuss adjustments after 30 days (up to ~15%) or 60 days (up to ~35%) depending on the item and market.",
      "Consignor payments are typically issued on the 10th of the month following a sale, by check—pickup at our Scottsdale showroom or mail to the address on file.",
    ],
  },
];

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

function addPageIfHasContent(doc, { withHeader = false } = {}) {
  if (!isPageEffectivelyBlank(doc, { withHeader })) {
    doc.addPage();
    if (withHeader) drawPageChrome(doc, { header: true });
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

function ensureVerticalSpace(doc, neededHeight) {
  if (doc.y + neededHeight <= pageContentBottom(doc)) return;
  if (!isPageEffectivelyBlank(doc, { withHeader: true })) {
    doc.addPage();
    drawPageChrome(doc, { header: true });
  }
}

function drawPageWatermark(doc) {
  const cx = doc.page.width / 2;
  const cy = doc.page.height / 2;
  doc.save();
  doc.rotate(-35, { origin: [cx, cy] });
  doc.font("Helvetica-Bold").fontSize(44).fillColor(COLOR_HEADING, 0.05);
  doc.text("LOST + FOUND", cx - 210, cy - 24, { width: 420, align: "center" });
  doc.font("Helvetica").fontSize(13).fillColor(COLOR_ACCENT, 0.06);
  doc.text("Lost & Found Resale Interiors", cx - 210, cy + 26, { width: 420, align: "center" });
  doc.restore();
}

function drawHeaderBar(doc) {
  doc.save();
  doc.rect(0, 0, doc.page.width, HEADER_BAR_HEIGHT).fill(COLOR_HEADING);
  doc.fillColor(COLOR_ACCENT).font("Helvetica-Bold").fontSize(10).text("LOST + FOUND", MARGIN, 16, {
    characterSpacing: 1.4,
  });
  doc.fillColor("#ffffff").font("Helvetica-Bold").fontSize(16).text("RESALE INTERIORS", MARGIN, 32);
  doc.font("Helvetica").fontSize(8).fillColor("#d4ccc4").text("Scottsdale, Arizona", MARGIN, 54);
  doc.restore();
  doc.y = HEADER_BAR_HEIGHT + 18;
}

function drawPageChrome(doc, { header = false } = {}) {
  drawPageWatermark(doc);
  if (header) drawHeaderBar(doc);
}

function drawLabelValue(doc, label, value, { indent = 0 } = {}) {
  const x = MARGIN + indent;
  const w = CONTENT_WIDTH - indent;
  doc.font("Helvetica-Bold").fontSize(9.5).fillColor(COLOR_HEADING);
  doc.text(`${label}: `, x, doc.y, { continued: true, width: w });
  doc.font("Helvetica").fillColor("#333333").text(String(value), { width: w });
  return doc.y + 5;
}

function drawDisclaimer(doc) {
  const pad = 12;
  const innerW = CONTENT_WIDTH - pad * 2;
  doc.font("Helvetica-Bold").fontSize(11);
  const titleH = doc.heightOfString(DISCLAIMER_TITLE, { width: innerW });
  doc.font("Helvetica").fontSize(10);
  const bodyH = doc.heightOfString(DISCLAIMER_BODY, { width: innerW, lineGap: 3 });
  const boxHeight = pad * 2 + titleH + 8 + bodyH + 4;
  const boxY = doc.y;
  doc.roundedRect(MARGIN, boxY, CONTENT_WIDTH, boxHeight, 5).fill(COLOR_LIGHT_BG);
  doc.roundedRect(MARGIN, boxY, CONTENT_WIDTH, boxHeight, 5).lineWidth(1.5).strokeColor(COLOR_ACCENT).stroke();
  doc.font("Helvetica-Bold").fontSize(11).fillColor(COLOR_HEADING);
  doc.text(DISCLAIMER_TITLE, MARGIN + pad, boxY + pad, { width: innerW });
  doc.font("Helvetica").fontSize(10).fillColor("#333333");
  doc.text(DISCLAIMER_BODY, MARGIN + pad, boxY + pad + titleH + 8, { width: innerW, lineGap: 3 });
  doc.y = boxY + boxHeight + 16;
}

function drawItemSection(doc, item, itemNumber, photos) {
  ensureVerticalSpace(doc, 120);
  doc.font("Helvetica-Bold").fontSize(13).fillColor(COLOR_ACCENT).text(`Item #${itemNumber}`, MARGIN);
  doc.font("Helvetica-Bold").fontSize(12).fillColor(COLOR_HEADING).text(displayValue(item.itemName), MARGIN, doc.y + 2);
  doc.moveDown(0.35);

  const fields = [
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
    drawLabelValue(doc, label, value);
  }

  if (photos[0]?.buffer) {
    try {
      const { width, height } = measureImageFit(doc, photos[0].buffer, 120, THUMB_MAX);
      ensureVerticalSpace(doc, height + 10);
      const y = doc.y + 4;
      doc.font("Helvetica").fontSize(8).fillColor(COLOR_MUTED).text("Photo preview", MARGIN, y);
      doc.image(photos[0].buffer, MARGIN, y + 12, { width, height });
      doc.y = y + 12 + height + 10;
    } catch {
      /* skip bad image */
    }
  }
  doc.moveDown(0.5);
  doc.moveTo(MARGIN, doc.y).lineTo(MARGIN + CONTENT_WIDTH, doc.y).strokeColor(COLOR_BORDER).lineWidth(0.5).stroke();
  doc.moveDown(0.6);
}

function drawPolicySection(doc, section) {
  ensureVerticalSpace(doc, 48);
  doc.font("Helvetica-Bold").fontSize(11.5).fillColor(COLOR_ACCENT).text(section.title, MARGIN, doc.y, {
    width: CONTENT_WIDTH,
  });
  doc.moveDown(0.3);
  doc.font("Helvetica").fontSize(9.5).fillColor("#333333");
  for (const para of section.paragraphs) {
    const indent = para.startsWith("•") ? 10 : 0;
    const w = CONTENT_WIDTH - indent;
    const h = doc.heightOfString(para, { width: w, lineGap: 2 });
    ensureVerticalSpace(doc, h + 6);
    doc.text(para, MARGIN + indent, doc.y, { width: w, lineGap: 2 });
    doc.moveDown(0.35);
  }
  doc.moveDown(0.4);
}

function addPageFooters(doc) {
  const range = doc.bufferedPageRange();
  const pageCount = range.count;
  const footerY = pageContentBottom(doc) + 6;
  for (let i = 0; i < pageCount; i++) {
    doc.switchToPage(range.start + i);
    drawPageWatermark(doc);
    doc.font("Helvetica").fontSize(7.5).fillColor(COLOR_MUTED);
    doc.text(FOOTER_LINE, MARGIN, footerY, {
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
    const chunks = [];
    const doc = new PDFDocument({
      size: "LETTER",
      margin: MARGIN,
      bufferPages: true,
      info: {
        Title: "Consignment Submission Summary",
        Author: "Lost & Found Resale Interiors, LLC",
        Subject: "Submission copy and consignment reference",
      },
    });

    doc.on("data", (chunk) => chunks.push(chunk));
    doc.on("end", () => resolve(Buffer.concat(chunks)));
    doc.on("error", reject);

    drawPageChrome(doc, { header: true });

    doc.font("Helvetica").fontSize(10).fillColor(COLOR_ACCENT).text("Consignment Submission Summary", MARGIN);
    doc.moveDown(0.15);
    doc.font("Helvetica-Bold").fontSize(17).fillColor(COLOR_HEADING).text("Your Submission Record", MARGIN);
    doc.moveDown(0.5);

    drawDisclaimer(doc);

    doc.font("Helvetica-Bold").fontSize(10).fillColor(COLOR_ACCENT).text("Submitted", MARGIN);
    doc.font("Helvetica").fontSize(10).fillColor("#333333").text(submittedAt, MARGIN, doc.y + 2);
    doc.moveDown(0.7);

    doc.font("Helvetica-Bold").fontSize(11).fillColor(COLOR_HEADING).text("Consignor", MARGIN);
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
      drawLabelValue(doc, label, value);
    }

    doc.moveDown(0.4);
    doc.font("Helvetica-Bold").fontSize(11).fillColor(COLOR_HEADING).text("Summary", MARGIN);
    doc.moveDown(0.2);
    drawLabelValue(doc, "Items submitted", String(items.length));
    drawLabelValue(doc, "Photos uploaded", String(countPhotos(photoGroups)));

    addPageIfHasContent(doc, { withHeader: true });
    doc.font("Helvetica-Bold").fontSize(14).fillColor(COLOR_HEADING).text("Items Submitted", MARGIN);
    doc.moveDown(0.15);
    doc
      .font("Helvetica")
      .fontSize(9)
      .fillColor(COLOR_MUTED)
      .text("Details below reflect what you entered on our consignment form.", MARGIN, doc.y, {
        width: CONTENT_WIDTH,
      });
    doc.moveDown(0.6);

    for (let i = 0; i < items.length; i++) {
      const itemNumber = resolveItemNumber(items[i], i);
      const photos = photoGroups.get(itemNumber) || [];
      drawItemSection(doc, items[i], itemNumber, photos);
    }

    addPageIfHasContent(doc, { withHeader: true });
    doc.font("Helvetica-Bold").fontSize(14).fillColor(COLOR_HEADING).text("For Accepted Items & Policies", MARGIN);
    doc.moveDown(0.2);
    doc
      .font("Helvetica")
      .fontSize(9)
      .fillColor(COLOR_MUTED)
      .text(
        "The following summarizes our standard services and consignment policies for your reference. Acceptance and contract terms are confirmed separately if your items are approved.",
        MARGIN,
        doc.y,
        { width: CONTENT_WIDTH, lineGap: 2 }
      );
    doc.moveDown(0.7);

    for (const section of POLICY_SECTIONS) {
      drawPolicySection(doc, section);
    }

    addPageFooters(doc);
    doc.end();
  });
}
