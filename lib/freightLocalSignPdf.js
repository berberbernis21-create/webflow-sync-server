/**
 * One-page branded local delivery sign form for the shop to print.
 * Local Arizona delivery only (not pickup — customer is not on-site for pickup).
 * Attached on internal emails alongside the full estimate summary.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import PDFDocument from "pdfkit";
import { sanitizeFilenamePart } from "./consignmentFilenames.js";
import { accessAnswerRows } from "./freightQuotePdf.js";
import { LOST_FOUND_PHONE } from "./freightPalletize.js";

const NAVY = "#07127c";
const MUTED = "#555555";
const INK = "#111111";
const GOLD = "#8a6d12";
const BORDER = "#d4cfc3";
const MARGIN = 26;
const CONTENT_WIDTH = 560;
const FIELD_ROW = 26;

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const BRAND_DIR = path.join(__dirname, "..", "public", "brand");

function money(n) {
  const num = Number(n);
  if (!Number.isFinite(num)) return String(n ?? "—");
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(num);
}

function dims(w, d, h) {
  if (w == null || d == null || h == null) return "";
  return `${w}"×${d}"×${h}"`;
}

function formatPhone(raw) {
  const digits = String(raw || "").replace(/\D/g, "");
  if (digits.length === 11 && digits.startsWith("1")) {
    return `(${digits.slice(1, 4)}) ${digits.slice(4, 7)}-${digits.slice(7)}`;
  }
  if (digits.length === 10) {
    return `(${digits.slice(0, 3)}) ${digits.slice(3, 6)}-${digits.slice(6)}`;
  }
  return String(raw || "").trim() || "—";
}

function readBrand(filename) {
  try {
    const p = path.join(BRAND_DIR, filename);
    if (!fs.existsSync(p)) return null;
    return fs.readFileSync(p);
  } catch {
    return null;
  }
}

async function fetchImageBuffer(url) {
  if (!url) return null;
  try {
    const res = await fetch(url, { signal: AbortSignal.timeout(12000) });
    if (!res.ok) return null;
    return Buffer.from(await res.arrayBuffer());
  } catch {
    return null;
  }
}

/**
 * Printable form field: value sits above the rule; label sits below.
 * Avoids the old overlap where values sat on top of underlines.
 */
function drawLineField(doc, label, x, y, width, value = "") {
  const valueText = value == null ? "" : String(value).trim();
  if (valueText) {
    doc.font("Helvetica").fontSize(9).fillColor(INK).text(valueText, x, y, {
      width,
      lineBreak: false,
      ellipsis: true,
    });
  }
  const lineY = y + 12;
  doc
    .moveTo(x, lineY)
    .lineTo(x + width, lineY)
    .strokeColor(BORDER)
    .lineWidth(0.85)
    .stroke();
  doc.font("Helvetica").fontSize(6).fillColor(MUTED).text(String(label || ""), x, lineY + 2.5, {
    width,
    lineBreak: false,
  });
  return lineY + 12;
}

/** Clean navy box-truck icon (generic — not Cruz / CPL branded). */
function drawDeliveryTruckIcon(doc, x, y, w, h) {
  const scale = Math.min(w / 120, h / 64);
  const ox = x + (w - 120 * scale) / 2;
  const oy = y + (h - 64 * scale) / 2;
  const s = (n) => n * scale;
  const px = (n) => ox + s(n);
  const py = (n) => oy + s(n);

  doc.save();
  // Soft plate behind icon
  doc.roundedRect(x, y, w, h, 6).fill("#f7f4ee");

  // Cargo box
  doc
    .roundedRect(px(8), py(10), s(70), s(34), 2)
    .fill(NAVY);
  // Cab
  doc
    .path(
      `M ${px(78)} ${py(18)} L ${px(104)} ${py(18)} L ${px(112)} ${py(30)} L ${px(112)} ${py(44)} L ${px(78)} ${py(44)} Z`
    )
    .fill(NAVY);
  // Cab window
  doc
    .path(`M ${px(84)} ${py(22)} L ${px(100)} ${py(22)} L ${px(105)} ${py(30)} L ${px(84)} ${py(30)} Z`)
    .fill("#dce3f5");
  // Bumper / lower rail
  doc.rect(px(8), py(42), s(104), s(3)).fill("#0a0f3a");
  // Wheels
  doc.circle(px(28), py(50), s(7)).fill("#222");
  doc.circle(px(28), py(50), s(3)).fill("#bbb");
  doc.circle(px(92), py(50), s(7)).fill("#222");
  doc.circle(px(92), py(50), s(3)).fill("#bbb");
  // Accent stripe on box
  doc.rect(px(14), py(24), s(58), s(3)).fill("#c9a227");
  doc.restore();
}

export function buildLocalSignPdfFilename(submission = {}, ctx = {}) {
  const name = sanitizeFilenamePart(submission.customer_name || "Customer", "Customer");
  const stamp = String(ctx.requestId || new Date().toISOString().slice(0, 10))
    .replace(/[^a-zA-Z0-9._-]/g, "-")
    .slice(0, 40);
  return `${name}_Local-Delivery_Sign-Form_${stamp}.pdf`;
}

/**
 * Local delivery sign form only (not pickup / nationwide).
 * @returns {Promise<Buffer|null>}
 */
export async function generateLocalSignPdf(submission, ctx = {}) {
  if (submission.delivery_path !== "local_az") return null;

  const {
    requestId = "",
    submittedAt = "",
    route = null,
    localEstimate = null,
    display = null,
  } = ctx;

  const addr =
    submission.delivery_address?.full ||
    [submission.street, submission.unit, submission.city, submission.state, submission.zip]
      .filter(Boolean)
      .join(", ");

  const estimate =
    localEstimate?.estimated_price != null ? money(localEstimate.estimated_price) : "TBD";
  const oneWay = route?.drive_minutes ?? localEstimate?.drive_minutes ?? null;
  const roundTrip = oneWay != null ? Number(oneWay) * 2 : null;
  const miles = route?.distance_miles ?? localEstimate?.distance_miles ?? null;
  const gate =
    submission.access?.gate_code_or_instructions ||
    (submission.access?.gated_access ? "Yes (code TBD)" : "—");

  const accessRows = accessAnswerRows(submission.access || {}, {
    isPickup: false,
    includeLiftgate: false,
  });
  const items = submission.items || [];

  const mapUrl =
    route?.map_image_url ||
    localEstimate?.route?.map_image_url ||
    display?.map_image_url ||
    "";
  const directionsUrl =
    route?.directions_url ||
    localEstimate?.route?.directions_url ||
    display?.directions_url ||
    "";
  const mapBuf = await fetchImageBuffer(mapUrl);
  // Center header uses the LOST + FOUND wordmark (not the circular seal).
  const logoBuf =
    readBrand("logo-wordmark.png") ||
    readBrand("logo-rectangular.png") ||
    readBrand("seal.png") ||
    readBrand("logo-seal-card.png");
  const deliverySceneBuf = readBrand("delivery-scene.png");
  const storeBuf =
    readBrand("showroom-exterior-sign.png") ||
    readBrand("showroom-exterior-v2.png") ||
    readBrand("showroom-exterior.png");

  const rateNote = localEstimate?.oversize_confirm
    ? "$130/hr size-based 3-person estimate — confirm with delivery team; final may be lower if two people are enough (~$95/hr)"
    : Number(submission.access?.extra_people) >= 1
      ? "$130/hr 3-person crew — confirm before scheduling"
      : "$95/hr round-trip (stairs / oversized may add charges) — confirm before scheduling";

  return new Promise((resolve, reject) => {
    const doc = new PDFDocument({
      size: "LETTER",
      autoFirstPage: true,
      bufferPages: false,
      margins: { top: MARGIN, bottom: MARGIN, left: MARGIN, right: MARGIN },
      info: {
        Title: `Local delivery sign form — ${estimate}`,
        Author: "Lost & Found Resale Interiors",
      },
    });
    const chunks = [];
    doc.on("data", (c) => chunks.push(c));
    doc.on("end", () => resolve(Buffer.concat(chunks)));
    doc.on("error", reject);

    let y = MARGIN;

    // Header: delivery scene (left) · LF wordmark (center) · showroom (right)
    const headerH = 58;
    const logoW = 168;
    const logoH = 48;
    const sceneW = 118;
    const sceneH = 52;
    const storeW = 90;
    const storeH = 52;
    const headerMidY = y + headerH / 2;

    if (deliverySceneBuf) {
      try {
        doc.image(deliverySceneBuf, MARGIN, headerMidY - sceneH / 2, {
          width: sceneW,
          height: sceneH,
          fit: [sceneW, sceneH],
          align: "center",
          valign: "center",
        });
      } catch {
        drawDeliveryTruckIcon(doc, MARGIN, headerMidY - sceneH / 2, sceneW, sceneH);
      }
    } else {
      drawDeliveryTruckIcon(doc, MARGIN, headerMidY - sceneH / 2, sceneW, sceneH);
    }

    const logoX = MARGIN + (CONTENT_WIDTH - logoW) / 2;
    if (logoBuf) {
      try {
        doc.image(logoBuf, logoX, headerMidY - logoH / 2, {
          width: logoW,
          height: logoH,
          fit: [logoW, logoH],
          align: "center",
          valign: "center",
        });
      } catch {
        doc
          .font("Helvetica-Bold")
          .fontSize(11)
          .fillColor(NAVY)
          .text("LOST + FOUND", logoX, headerMidY - 6, { width: logoW, align: "center" });
      }
    } else {
      doc
        .font("Helvetica-Bold")
        .fontSize(11)
        .fillColor(NAVY)
        .text("LOST + FOUND", logoX, headerMidY - 6, { width: logoW, align: "center" });
    }

    if (storeBuf) {
      try {
        doc.image(storeBuf, MARGIN + CONTENT_WIDTH - storeW, headerMidY - storeH / 2, {
          width: storeW,
          height: storeH,
          fit: [storeW, storeH],
          align: "center",
          valign: "center",
        });
      } catch {
        /* ignore */
      }
    }
    y += headerH + 4;
    doc
      .moveTo(MARGIN, y)
      .lineTo(MARGIN + CONTENT_WIDTH, y)
      .strokeColor(NAVY)
      .lineWidth(1.25)
      .stroke();
    y += 8;

    doc
      .font("Helvetica-Bold")
      .fontSize(12)
      .fillColor(NAVY)
      .text("PRELIMINARY DELIVERY ESTIMATE — SIGN & INITIAL", MARGIN, y, {
        width: CONTENT_WIDTH,
        align: "center",
      });
    y = doc.y + 4;
    doc
      .font("Helvetica")
      .fontSize(7)
      .fillColor(GOLD)
      .text(
        "This is a preliminary estimate from our online tool. Pricing may change based on access, stairs, crew needs, item size, drive time, and day-of conditions. Confirm exact pricing before scheduling.",
        MARGIN,
        y,
        { width: CONTENT_WIDTH, align: "center", lineGap: 1.5 }
      );
    y = doc.y + 8;

    // Estimate strip
    doc.roundedRect(MARGIN, y, CONTENT_WIDTH, 30, 4).fill("#f4f1ea");
    doc
      .font("Helvetica-Bold")
      .fontSize(10)
      .fillColor(NAVY)
      .text(`Local Arizona delivery  ·  Estimate ${estimate}`, MARGIN + 10, y + 5, {
        width: CONTENT_WIDTH - 20,
        lineBreak: false,
      });
    doc
      .font("Helvetica")
      .fontSize(6.5)
      .fillColor(MUTED)
      .text(rateNote, MARGIN + 10, y + 17, { width: CONTENT_WIDTH - 20, lineBreak: false });
    y += 36;

    // Team fill-ins under estimate (room to write)
    const fillColW = (CONTENT_WIDTH - 12) / 2;
    const fillPanelH = 42;
    doc.roundedRect(MARGIN, y, CONTENT_WIDTH, fillPanelH, 4).lineWidth(1).strokeColor(BORDER).stroke();
    drawLineField(doc, "DELIVERY COMPANY", MARGIN + 10, y + 8, fillColW - 10, "");
    drawLineField(doc, "CONFIRMED RATE", MARGIN + fillColW + 12, y + 8, fillColW - 10, "");
    y += fillPanelH + 8;

    // Customer + logistics
    const colW = (CONTENT_WIDTH - 12) / 2;
    drawLineField(doc, "NAME", MARGIN, y, colW, submission.customer_name || "");
    drawLineField(doc, "PHONE", MARGIN + colW + 12, y, colW, formatPhone(submission.customer_phone));
    y += FIELD_ROW;
    drawLineField(doc, "EMAIL", MARGIN, y, colW, submission.customer_email || "");
    drawLineField(
      doc,
      "TYPE",
      MARGIN + colW + 12,
      y,
      colW,
      String(submission.destination_type || "residential").replace(/^\w/, (c) => c.toUpperCase())
    );
    y += FIELD_ROW;
    drawLineField(doc, "DELIVERY ADDRESS", MARGIN, y, CONTENT_WIDTH, addr);
    y += FIELD_ROW;
    drawLineField(doc, "GATE CODE / ACCESS", MARGIN, y, colW, gate === "—" ? "" : gate);
    const driveText =
      oneWay != null
        ? `${oneWay} min one way / ${roundTrip} min RT${miles != null ? ` · ${miles} mi` : ""}`
        : miles != null
          ? `${miles} mi`
          : "";
    drawLineField(doc, "DRIVE / DISTANCE", MARGIN + colW + 12, y, colW, driveText);
    y += FIELD_ROW;
    drawLineField(doc, "DATE OF DELIVERY", MARGIN, y, colW, "");
    drawLineField(doc, "TIME WINDOW", MARGIN + colW + 12, y, colW, "");
    y += FIELD_ROW + 4;

    // Access answers (compact)
    doc.font("Helvetica-Bold").fontSize(8).fillColor(NAVY).text("ACCESS ANSWERS (FROM FORM)", MARGIN, y);
    y = doc.y + 2;
    doc.moveTo(MARGIN, y).lineTo(MARGIN + CONTENT_WIDTH, y).strokeColor(NAVY).lineWidth(1).stroke();
    y += 4;

    const mid = Math.ceil(accessRows.length / 2);
    const left = accessRows.slice(0, mid);
    const right = accessRows.slice(mid);
    const rowCount = Math.max(left.length, right.length);
    const accessLabelW = 100;
    const accessValW = colW - accessLabelW;
    for (let i = 0; i < rowCount; i++) {
      const L = left[i];
      const R = right[i];
      if (L) {
        doc.font("Helvetica").fontSize(6.2).fillColor(MUTED).text(L[0], MARGIN, y, {
          width: accessLabelW,
          lineBreak: false,
        });
        doc
          .font("Helvetica-Bold")
          .fontSize(6.2)
          .fillColor(INK)
          .text(String(L[1] ?? "—"), MARGIN + accessLabelW, y, {
            width: accessValW,
            lineBreak: false,
          });
      }
      if (R) {
        const x = MARGIN + colW + 12;
        doc.font("Helvetica").fontSize(6.2).fillColor(MUTED).text(R[0], x, y, {
          width: accessLabelW,
          lineBreak: false,
        });
        doc
          .font("Helvetica-Bold")
          .fontSize(6.2)
          .fillColor(INK)
          .text(String(R[1] ?? "—"), x + accessLabelW, y, {
            width: accessValW,
            lineBreak: false,
          });
      }
      y += 9.2;
    }

    const preferredDaysWindows = String(submission.access?.preferred_days_windows || "").trim();
    if (preferredDaysWindows) {
      y += 3;
      doc
        .font("Helvetica")
        .fontSize(6.5)
        .fillColor(MUTED)
        .text("PREFERRED DAYS / WINDOWS", MARGIN, y, { width: 132, lineBreak: false });
      doc
        .font("Helvetica-Bold")
        .fontSize(6.5)
        .fillColor(INK)
        .text(preferredDaysWindows, MARGIN + 132, y, {
          width: CONTENT_WIDTH - 132,
        });
      y = Math.max(doc.y, y + 10);
    }
    y += 5;

    // Items (left) + route map (right)
    const mapW = 168;
    const itemsW = CONTENT_WIDTH - mapW - 10;
    const blockTop = y;
    const mapH = 110;

    doc.font("Helvetica-Bold").fontSize(8).fillColor(NAVY).text("ITEMS", MARGIN, y);
    doc
      .font("Helvetica-Bold")
      .fontSize(8)
      .fillColor(NAVY)
      .text("ROUTE MAP", MARGIN + itemsW + 10, y, { width: mapW });
    y = Math.max(doc.y, blockTop + 10);
    doc.moveTo(MARGIN, y).lineTo(MARGIN + itemsW, y).strokeColor(NAVY).lineWidth(1).stroke();
    doc
      .moveTo(MARGIN + itemsW + 10, y)
      .lineTo(MARGIN + CONTENT_WIDTH, y)
      .strokeColor(NAVY)
      .lineWidth(1)
      .stroke();
    y += 3;

    doc.font("Helvetica-Bold").fontSize(6.5).fillColor(MUTED).text("QTY", MARGIN, y, { width: 28 });
    doc
      .font("Helvetica-Bold")
      .fontSize(6.5)
      .fillColor(MUTED)
      .text("DESCRIPTION / ITEMS FROM LOST + FOUND", MARGIN + 30, y, { width: itemsW - 30 });
    let itemsY = y + 10;

    const maxItems = Math.min(items.length || 0, 5);
    if (!items.length) {
      doc.font("Helvetica").fontSize(7).fillColor(INK).text("No items listed", MARGIN + 30, itemsY);
      itemsY += 11;
    } else {
      for (let i = 0; i < maxItems; i++) {
        const row = items[i];
        const qty = Math.max(1, Number(row.quantity) || 1);
        const bits = [
          row.title || "Item",
          dims(row.width, row.depth, row.height),
          row.weight != null ? `${row.weight} lb` : "",
        ]
          .filter(Boolean)
          .join("  ·  ");
        doc.font("Helvetica-Bold").fontSize(7).fillColor(INK).text(String(qty), MARGIN, itemsY, {
          width: 28,
          lineBreak: false,
        });
        doc.font("Helvetica").fontSize(7).fillColor(INK).text(bits, MARGIN + 30, itemsY, {
          width: itemsW - 30,
          lineBreak: false,
        });
        itemsY += 11;
      }
      if (items.length > maxItems) {
        doc
          .font("Helvetica-Oblique")
          .fontSize(6.5)
          .fillColor(MUTED)
          .text(`+ ${items.length - maxItems} more — see summary PDF`, MARGIN + 30, itemsY);
        itemsY += 10;
      }
    }

    const mapX = MARGIN + itemsW + 10;
    const mapY = blockTop + 12;
    doc.roundedRect(mapX, mapY, mapW, mapH, 3).lineWidth(0.8).strokeColor(BORDER).stroke();
    if (mapBuf) {
      try {
        doc.image(mapBuf, mapX + 3, mapY + 3, {
          width: mapW - 6,
          height: mapH - 6,
          fit: [mapW - 6, mapH - 6],
          align: "center",
          valign: "center",
        });
      } catch {
        doc
          .font("Helvetica-Oblique")
          .fontSize(7)
          .fillColor(MUTED)
          .text("Map unavailable", mapX + 8, mapY + mapH / 2 - 6, { width: mapW - 16 });
      }
    } else {
      doc
        .font("Helvetica-Oblique")
        .fontSize(7)
        .fillColor(MUTED)
        .text(
          directionsUrl
            ? "Map image unavailable — open route link in email."
            : "Route map unavailable for this request.",
          mapX + 8,
          mapY + mapH / 2 - 10,
          { width: mapW - 16, align: "center" }
        );
    }

    y = Math.max(itemsY, mapY + mapH) + 8;

    // Disclosures
    const disclosureText = [
      "This is a preliminary estimate only. Final pricing must be confirmed before scheduling and may change based on access, stairs, crew size, item size/weight, distance, wait time, labor, special handling, and conditions on the day of service.",
      "When a third-party carrier or mover is used (for example, Cruz Pro Line), confirmed delivery and moving charges are paid directly to that company — not held as a prepaid Lost & Found delivery fee unless otherwise agreed in writing. Accepted payment methods include cash, check, or Zelle.",
      "Installation, assembly beyond basic placement, electrical, plumbing, gas, wall anchoring, and any utility or fixture work are NOT included and are NOT the responsibility or liability of Lost & Found.",
      "Customer confirms access details above are accurate, a clear path is available, and customer accepts responsibility for site conditions, building rules, parking, and any issues from inaccurate information or site limitations.",
      "By signing and initialing, customer agrees to pay all fees and understands the price is subject to change depending on conditions — this is a best-guess estimate, not a final locked rate.",
    ].join(" ");

    const disclosureTop = y;
    doc
      .font("Helvetica-Bold")
      .fontSize(7)
      .fillColor(NAVY)
      .text("IMPORTANT — PLEASE READ BEFORE SIGNING / INITIALING", MARGIN + 8, disclosureTop + 5, {
        width: CONTENT_WIDTH - 16,
      });
    doc
      .font("Helvetica")
      .fontSize(5.7)
      .fillColor(INK)
      .text(disclosureText, MARGIN + 8, disclosureTop + 16, {
        width: CONTENT_WIDTH - 16,
        align: "left",
        lineGap: 1.2,
      });
    const disclosureBottom = doc.y + 6;
    doc
      .roundedRect(MARGIN, disclosureTop, CONTENT_WIDTH, Math.max(68, disclosureBottom - disclosureTop), 4)
      .lineWidth(1)
      .strokeColor(BORDER)
      .stroke();
    y = Math.max(disclosureBottom, disclosureTop + 68) + 8;

    // Agreement + signature / initials
    doc
      .font("Helvetica-Bold")
      .fontSize(9)
      .fillColor(INK)
      .text("Customer agrees to pay all confirmed delivery fees", MARGIN, y, {
        width: CONTENT_WIDTH,
        align: "center",
      });
    y = doc.y + 10;

    drawLineField(doc, "CUSTOMER SIGNATURE", MARGIN, y, colW + 40, "");
    drawLineField(doc, "DATE", MARGIN + colW + 52, y, colW - 40, "");
    y += FIELD_ROW + 2;
    drawLineField(doc, "PRINT NAME", MARGIN, y, colW + 40, submission.customer_name || "");
    drawLineField(doc, "CUSTOMER INITIALS", MARGIN + colW + 52, y, colW - 40, "");
    y += FIELD_ROW + 2;

    const footerY = Math.min(y, doc.page.height - MARGIN - 10);
    doc
      .font("Helvetica")
      .fontSize(6)
      .fillColor(MUTED)
      .text(
        `Lost & Found Resale Interiors · Scottsdale · ${LOST_FOUND_PHONE} · ${requestId || ""}${
          submittedAt ? ` · ${submittedAt}` : ""
        } · Print for delivery clipboard / customer signature & initials`,
        MARGIN,
        footerY,
        { width: CONTENT_WIDTH, align: "center" }
      );

    doc.end();
  });
}
