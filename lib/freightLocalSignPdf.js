/**
 * Branded local delivery document for the customer to sign.
 * Local Arizona delivery only (not pickup — customer is not on-site for pickup).
 * Attached on internal emails. Prefer one page; overflow items continue on page 2.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import PDFDocument from "pdfkit";
import { sanitizeFilenamePart } from "./consignmentFilenames.js";
import { accessAnswerRows } from "./freightQuotePdf.js";
import { customerAssemblyTimeLines, LOST_FOUND_PHONE } from "./freightPalletize.js";
import { customerFacingTitle } from "./listingTitleDisplay.js";

const NAVY = "#07127c";
const MUTED = "#555555";
const INK = "#111111";
const RED = "#9c2f2f";
const BORDER = "#d4cfc3";
const MARGIN = 22;
const CONTENT_WIDTH = 568;
const FIELD_ROW = 28;

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

/** Keep delivery document to one page — prefer at most ~2 short sentences. */
function truncatePreferredSchedule(text, maxChars = 160) {
  const raw = String(text || "")
    .trim()
    .replace(/\s+/g, " ");
  if (!raw) return "";
  const sentences = raw.match(/[^.!?]+[.!?]+|[^.!?]+$/g) || [raw];
  let out = sentences
    .slice(0, 2)
    .join(" ")
    .trim();
  const clipped = sentences.length > 2 || out.length > maxChars;
  if (out.length > maxChars) {
    out = out.slice(0, maxChars);
    const cut = out.lastIndexOf(" ");
    if (cut > 80) out = out.slice(0, cut);
  }
  out = out.replace(/[.,;:\s]+$/g, "");
  return clipped ? `${out}…` : out;
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

/**
 * Printable form field: value sits above the rule; label sits below.
 * @param {{ valueSize?: number, labelSize?: number, valuePad?: number, boldLabel?: boolean }} [opts]
 */
function drawLineField(doc, label, x, y, width, value = "", opts = {}) {
  const valueSize = opts.valueSize ?? 12;
  const labelSize = opts.labelSize ?? 8;
  const valuePad = opts.valuePad ?? 15;
  const boldLabel = opts.boldLabel !== false;
  const valueText = value == null ? "" : String(value).trim();
  if (valueText) {
    doc.font("Helvetica-Bold").fontSize(valueSize).fillColor(INK).text(valueText, x, y, {
      width,
      lineBreak: false,
      ellipsis: true,
    });
  }
  const lineY = y + valuePad;
  doc
    .moveTo(x, lineY)
    .lineTo(x + width, lineY)
    .strokeColor(BORDER)
    .lineWidth(1.15)
    .stroke();
  doc
    .font(boldLabel ? "Helvetica-Bold" : "Helvetica")
    .fontSize(labelSize)
    .fillColor(boldLabel ? INK : MUTED)
    .text(String(label || ""), x, lineY + 3, {
      width,
      lineBreak: false,
    });
  return lineY + labelSize + 5;
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
  return `${name}_Local-Delivery_Document_${stamp}.pdf`;
}

/**
 * Local delivery document for customer signature (not pickup / nationwide).
 * @returns {Promise<Buffer|null>}
 */
export async function generateLocalSignPdf(submission, ctx = {}) {
  if (submission.delivery_path !== "local_az") return null;

  const {
    requestId = "",
    submittedAt = "",
    route = null,
    localEstimate = null,
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

  // Only show access answers that are Yes / affirmative (skip all Nos).
  const accessRows = accessAnswerRows(submission.access || {}, {
    isPickup: false,
    includeLiftgate: false,
  }).filter(([, v]) => {
    const s = String(v || "").trim();
    if (!s || s === "—" || s === "-") return false;
    if (/^no$/i.test(s)) return false;
    if (/^not sure$/i.test(s)) return false;
    return true;
  });
  const items = submission.items || [];

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

  const assemblyMinsAmt = Number(localEstimate?.assembly_extra_minutes) || 0;
  const assemblyLines = customerAssemblyTimeLines(assemblyMinsAmt);

  const estimateLine = `Local Arizona delivery  ·  Estimate ${estimate}  ·  ($95/hr)`;

  return new Promise((resolve, reject) => {
    const doc = new PDFDocument({
      size: "LETTER",
      autoFirstPage: true,
      bufferPages: false,
      margins: { top: MARGIN, bottom: MARGIN, left: MARGIN, right: MARGIN },
      info: {
        Title: `Local delivery document — ${estimate}`,
        Author: "Lost & Found Resale Interiors",
      },
    });
    const chunks = [];
    doc.on("data", (c) => chunks.push(c));
    doc.on("end", () => resolve(Buffer.concat(chunks)));
    doc.on("error", reject);

    let y = MARGIN;

    // Header: delivery scene (left) · LF wordmark (center) · showroom (right)
    const headerH = 48;
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
    y += headerH + 2;
    doc
      .moveTo(MARGIN, y)
      .lineTo(MARGIN + CONTENT_WIDTH, y)
      .strokeColor(NAVY)
      .lineWidth(1.25)
      .stroke();
    y += 4;

    doc
      .font("Helvetica-Bold")
      .fontSize(12)
      .fillColor(NAVY)
      .text("PRELIMINARY DELIVERY ESTIMATE — SIGN", MARGIN, y, {
        width: CONTENT_WIDTH,
        align: "center",
      });
    y = doc.y + 2;
    doc
      .font("Helvetica")
      .fontSize(7.5)
      .fillColor(RED)
      .text(
        "This is a preliminary estimate from our online tool. Pricing may change based on access, stairs, crew needs, item size, drive time, and day-of conditions. Confirm exact pricing before scheduling.",
        MARGIN,
        y,
        { width: CONTENT_WIDTH, align: "center", lineGap: 0.4 }
      );
    y = doc.y + 4;

    // Estimate strip
    const stripH = assemblyLines ? 44 : 28;
    doc.roundedRect(MARGIN, y, CONTENT_WIDTH, stripH, 4).fill("#f4f1ea");
    doc
      .font("Helvetica-Bold")
      .fontSize(12)
      .fillColor(NAVY)
      .text(estimateLine, MARGIN + 10, y + 6, {
        width: CONTENT_WIDTH - 20,
        lineBreak: false,
      });
    if (assemblyLines) {
      doc
        .font("Helvetica")
        .fontSize(8)
        .fillColor(MUTED)
        .text(
          `${assemblyLines.extraTitle} | ${assemblyLines.extraLabel}`,
          MARGIN + 10,
          y + 20,
          { width: CONTENT_WIDTH - 20, lineBreak: false }
        );
      doc
        .font("Helvetica-Bold")
        .fontSize(8.5)
        .fillColor(NAVY)
        .text(
          `${assemblyLines.totalTitle} | ${assemblyLines.totalLabel}`,
          MARGIN + 10,
          y + 31,
          { width: CONTENT_WIDTH - 20, lineBreak: false }
        );
    }
    y += stripH + 4;

    // Team fill-ins — write-in room; bold labels
    const fillColW = (CONTENT_WIDTH - 18) / 2;
    const writeInOpts = { valueSize: 12, labelSize: 9, valuePad: 16, boldLabel: true };
    const rateOpts = { valueSize: 12, labelSize: 9, valuePad: 16, boldLabel: true };
    const fillPanelH = 46;
    doc.roundedRect(MARGIN, y, CONTENT_WIDTH, fillPanelH, 4).lineWidth(1.1).strokeColor(BORDER).stroke();
    drawLineField(doc, "DELIVERY COMPANY", MARGIN + 12, y + 4, fillColW - 16, "", writeInOpts);
    drawLineField(doc, "CONFIRMED RATE", MARGIN + fillColW + 18, y + 4, fillColW - 16, "($95/hr)", rateOpts);
    y += fillPanelH + 2;
    doc
      .font("Helvetica")
      .fontSize(8)
      .fillColor(MUTED)
      .text(
        '($95/hr) is for delivery only. Additional "tasks" will be an additional charge.',
        MARGIN + 12,
        y,
        { width: CONTENT_WIDTH - 24 }
      );
    y = doc.y + 3;

    // Customer + logistics
    const colW = (CONTENT_WIDTH - 18) / 2;
    const infoOpts = { valueSize: 11, labelSize: 7.5, valuePad: 12, boldLabel: true };
    const infoRow = 24;
    drawLineField(doc, "NAME", MARGIN, y, colW, submission.customer_name || "", infoOpts);
    drawLineField(doc, "PHONE", MARGIN + colW + 18, y, colW, formatPhone(submission.customer_phone), infoOpts);
    y += infoRow;
    drawLineField(doc, "EMAIL", MARGIN, y, CONTENT_WIDTH, submission.customer_email || "", infoOpts);
    y += infoRow;
    drawLineField(doc, "DELIVERY ADDRESS", MARGIN, y, CONTENT_WIDTH, addr, infoOpts);
    y += infoRow;
    drawLineField(doc, "GATE CODE / ACCESS", MARGIN, y, colW, gate === "—" ? "" : gate, infoOpts);
    const driveText =
      oneWay != null
        ? `${oneWay} min one way / ${roundTrip} min RT${miles != null ? ` · ${miles} mi` : ""}`
        : miles != null
          ? `${miles} mi`
          : "";
    drawLineField(doc, "DRIVE / DISTANCE", MARGIN + colW + 18, y, colW, driveText, infoOpts);
    // Blank lines are for the driver team after they confirm — not the customer's request.
    y += infoRow + 4;
    const scheduleOpts = { valueSize: 12, labelSize: 7, valuePad: 16, boldLabel: true };
    drawLineField(doc, "DRIVER TEAM CONFIRMED DATE", MARGIN, y, colW, "", scheduleOpts);
    drawLineField(
      doc,
      "DRIVER TEAM CONFIRMED TIME WINDOW",
      MARGIN + colW + 18,
      y,
      colW,
      "",
      scheduleOpts
    );
    y += infoRow + 6;

    // Access answers — Yes answers only (section omitted when none)
    if (accessRows.length) {
      doc.font("Helvetica-Bold").fontSize(10).fillColor(NAVY).text("ACCESS (YES ANSWERS)", MARGIN, y);
      y = doc.y + 2;
      doc.moveTo(MARGIN, y).lineTo(MARGIN + CONTENT_WIDTH, y).strokeColor(NAVY).lineWidth(1).stroke();
      y += 5;

      const mid = Math.ceil(accessRows.length / 2);
      const left = accessRows.slice(0, mid);
      const right = accessRows.slice(mid);
      const rowCount = Math.max(left.length, right.length);
      const accessLabelW = 118;
      const accessValW = colW - accessLabelW;
      for (let i = 0; i < rowCount; i++) {
        const L = left[i];
        const R = right[i];
        if (L) {
          doc.font("Helvetica").fontSize(9).fillColor(MUTED).text(L[0], MARGIN, y, {
            width: accessLabelW,
            lineBreak: false,
          });
          doc
            .font("Helvetica-Bold")
            .fontSize(9)
            .fillColor(INK)
            .text(String(L[1] ?? ""), MARGIN + accessLabelW, y, {
              width: accessValW,
              lineBreak: false,
            });
        }
        if (R) {
          const x = MARGIN + colW + 14;
          doc.font("Helvetica").fontSize(9).fillColor(MUTED).text(R[0], x, y, {
            width: accessLabelW,
            lineBreak: false,
          });
          doc
            .font("Helvetica-Bold")
            .fontSize(9)
            .fillColor(INK)
            .text(String(R[1] ?? ""), x + accessLabelW, y, {
              width: accessValW,
              lineBreak: false,
            });
        }
        y += 11;
      }
      y += 2;
    }

    const preferredDaysWindows = truncatePreferredSchedule(
      submission.access?.preferred_days_windows
    );
    if (preferredDaysWindows) {
      y += 2;
      doc
        .font("Helvetica-Bold")
        .fontSize(8)
        .fillColor(RED)
        .text("CUSTOMER PREFERRED DAYS / WINDOW", MARGIN, y, {
          width: CONTENT_WIDTH,
          lineBreak: false,
        });
      y = doc.y + 1;
      doc.font("Helvetica-Bold").fontSize(14).fillColor(RED);
      const prefH = Math.min(32, doc.heightOfString(preferredDaysWindows, { width: CONTENT_WIDTH }));
      doc.text(preferredDaysWindows, MARGIN, y, {
        width: CONTENT_WIDTH,
        height: prefH,
        ellipsis: true,
      });
      y = y + prefH + 4;
    } else {
      y += 2;
    }

    // Pin the legal block and signature to the bottom of this page, then fit
    // items into the space above it. This sign is always one page.
    const absoluteBottom = doc.page.height - MARGIN;
    const footerY = absoluteBottom - 10;
    const bodySize = 8;
    const bodyGap = 0.5;
    const dw = CONTENT_WIDTH - 16;
    const dx = MARGIN + 8;
    const discPlainLead =
      "Preliminary estimate only — final price must be confirmed and may change based on access, stairs, item size/weight, and conditions on the day of service. Third-party movers (e.g. Cruz Pro Line) are paid directly (cash, check, or Zelle). Full furniture installation, ";
    const discBoldMid = "electrical, plumbing, gas, wall anchoring, and any utility or fixture work";
    const discPlainTail =
      " are NOT included and are NOT the responsibility or liability of Lost & Found. By signing, customer agrees to pay all fees and understands this is not a final locked rate.";
    doc.font("Helvetica").fontSize(bodySize);
    const discBodyH = doc.heightOfString(discPlainLead + discBoldMid + discPlainTail, {
      width: dw,
      lineGap: bodyGap,
    });
    const discBoxH = Math.max(48, 6 + 12 + discBodyH + 6);
    const agreeH = 18;
    const fieldsH = 70;
    const bottomStackH = discBoxH + 6 + agreeH + fieldsH + 12;
    const disclosureTop = footerY - bottomStackH;

    const qtyColW = 36;
    const textWidth = CONTENT_WIDTH - qtyColW - 4;
    const itemsHeaderH = 28;
    const itemCount = Math.max(items.length, 1);
    const rowsBudget = Math.max(itemCount * 11, disclosureTop - 4 - y - itemsHeaderH);
    const rowH = Math.min(16, Math.floor(rowsBudget / itemCount));
    const itemFont = rowH >= 15 ? 11 : rowH >= 13 ? 10 : 9;

    doc.font("Helvetica-Bold").fontSize(10).fillColor(NAVY).text("ITEMS", MARGIN, y, { lineBreak: false });
    y = doc.y + 1;
    doc.moveTo(MARGIN, y).lineTo(MARGIN + CONTENT_WIDTH, y).strokeColor(NAVY).lineWidth(1.1).stroke();
    y += 3;
    doc.font("Helvetica-Bold").fontSize(8).fillColor(MUTED).text("QTY", MARGIN, y, {
      width: qtyColW,
      lineBreak: false,
    });
    doc
      .font("Helvetica-Bold")
      .fontSize(8)
      .fillColor(MUTED)
      .text("DESCRIPTION / ITEMS FROM LOST + FOUND", MARGIN + qtyColW, y, {
        width: textWidth,
        lineBreak: false,
      });
    let itemsY = y + 11;

    const itemRows = items.length
      ? items
      : [{ title: "No items listed", quantity: "", width: null, depth: null, height: null, weight: null }];
    for (const row of itemRows) {
      const qty = row.quantity === "" ? "" : String(Math.max(1, Number(row.quantity) || 1));
      const title = row.quantity === "" ? "No items listed" : customerFacingTitle(row, "Item");
      const meta =
        row.quantity === ""
          ? ""
          : [dims(row.width, row.depth, row.height), row.weight != null ? `${row.weight} lb` : ""]
              .filter(Boolean)
              .join("  ·  ");
      const desc = meta ? `${title}  ·  ${meta}` : title;
      doc.font("Helvetica-Bold").fontSize(itemFont).fillColor(INK).text(qty, MARGIN, itemsY, {
        width: qtyColW,
        lineBreak: false,
        ellipsis: true,
      });
      doc.font("Helvetica-Bold").fontSize(itemFont).fillColor(INK).text(desc, MARGIN + qtyColW, itemsY, {
        width: textWidth,
        height: rowH,
        lineBreak: false,
        ellipsis: true,
      });
      itemsY += rowH;
    }

    doc
      .font("Helvetica-Bold")
      .fontSize(9.5)
      .fillColor(NAVY)
      .text("IMPORTANT — PLEASE READ BEFORE SIGNING", MARGIN + 8, disclosureTop + 6, {
        width: CONTENT_WIDTH - 16,
      });

    let dy = disclosureTop + 18;
    // One continuous flowing paragraph (bold only the exclusion list).
    doc.font("Helvetica").fontSize(bodySize).fillColor(INK).text(discPlainLead, dx, dy, {
      width: dw,
      align: "left",
      lineGap: bodyGap,
      continued: true,
    });
    doc.font("Helvetica-Bold").fontSize(bodySize).fillColor(INK).text(discBoldMid, {
      width: dw,
      align: "left",
      lineGap: bodyGap,
      continued: true,
    });
    doc.font("Helvetica").fontSize(bodySize).fillColor(INK).text(discPlainTail, {
      width: dw,
      align: "left",
      lineGap: bodyGap,
    });
    const disclosureBottom = Math.max(doc.y + 4, disclosureTop + discBoxH);
    doc
      .roundedRect(MARGIN, disclosureTop, CONTENT_WIDTH, disclosureBottom - disclosureTop, 4)
      .lineWidth(1)
      .strokeColor(BORDER)
      .stroke();

    const sigY = disclosureBottom + 8;
    doc
      .font("Helvetica-Bold")
      .fontSize(10)
      .fillColor(INK)
      .text("Customer agrees to pay all fees and understands price is subject to change", MARGIN, sigY, {
        width: CONTENT_WIDTH,
        align: "center",
      });
    const fieldsY = doc.y + 8;

    const sigW = CONTENT_WIDTH * 0.42;
    const nameW = CONTENT_WIDTH * 0.34;
    const dateW = CONTENT_WIDTH - sigW - nameW - 16;
    drawLineField(doc, "CUSTOMER SIGNATURE", MARGIN, fieldsY, sigW, "", infoOpts);
    drawLineField(
      doc,
      "PRINT NAME",
      MARGIN + sigW + 8,
      fieldsY,
      nameW,
      submission.customer_name || "",
      infoOpts
    );
    drawLineField(doc, "DATE", MARGIN + sigW + nameW + 16, fieldsY, dateW, "", infoOpts);

    // Blank line for handwriting which L+F employee handled the delivery / form.
    const employeeY = fieldsY + 26;
    const empNameW = CONTENT_WIDTH * 0.62;
    const empDateW = CONTENT_WIDTH - empNameW - 8;
    drawLineField(doc, "L+F EMPLOYEE", MARGIN, employeeY, empNameW, "", infoOpts);
    drawLineField(doc, "DATE", MARGIN + empNameW + 8, employeeY, empDateW, "", infoOpts);

    doc
      .font("Helvetica")
      .fontSize(7.5)
      .fillColor(MUTED)
      .text(
        `Lost & Found Resale Interiors · Scottsdale · ${LOST_FOUND_PHONE} · Print for delivery clipboard`,
        MARGIN,
        footerY,
        { width: CONTENT_WIDTH, align: "center" }
      );

    doc.end();
  });
}
