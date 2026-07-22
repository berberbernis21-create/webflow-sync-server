/**
 * Printable internal PDF for freight / local delivery / consignor pickup quotes.
 * Target: fit on 1–2 pages (no blank trailing pages).
 */

import PDFDocument from "pdfkit";
import { sanitizeFilenamePart } from "./consignmentFilenames.js";

const NAVY = "#07127c";
const MUTED = "#555555";
const RED = "#9c2f2f";
const MARGIN = 36;
const CONTENT_WIDTH = 540;
const FOOTER_RESERVE = 28;

function money(n) {
  const num = Number(n);
  if (!Number.isFinite(num)) return String(n ?? "-");
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(num);
}

function dims(w, d, h) {
  if (w == null || d == null || h == null) return "-";
  return `${w}"×${d}"×${h}"`;
}

function yn(v) {
  if (v === true) return "Yes";
  if (v === false) return "No";
  return "—";
}

function formatDestinationType(raw) {
  const s = String(raw || "").trim();
  if (!s || s === "-") return "-";
  return s.charAt(0).toUpperCase() + s.slice(1).toLowerCase();
}

function formatPhone(raw) {
  const digits = String(raw || "").replace(/\D/g, "");
  if (digits.length === 11 && digits.startsWith("1")) {
    return `(${digits.slice(1, 4)}) ${digits.slice(4, 7)}-${digits.slice(7)}`;
  }
  if (digits.length === 10) {
    return `(${digits.slice(0, 3)}) ${digits.slice(3, 6)}-${digits.slice(6)}`;
  }
  return String(raw || "").trim() || "-";
}

function classLabel(p) {
  if (!p) return "-";
  return p.freight_class != null ? String(p.freight_class) : "-";
}

/**
 * Full Access-step answers for the PDF (Yes/No), not only “highlights”.
 * Liftgate is LTL-only — omit for local company-truck jobs.
 */
export function accessAnswerRows(access = {}, { isPickup = false, includeLiftgate = false } = {}) {
  const rows = [];
  rows.push([
    "Stairs",
    access.stairs
      ? `Yes — ${access.stair_flights || "?"} flight(s)${access.stair_notes ? ` — ${access.stair_notes}` : ""}`
      : "No",
  ]);
  rows.push([isPickup ? "Inside pickup" : "Inside delivery", yn(access.inside_delivery)]);
  rows.push([
    isPickup ? "Carry-out from room / inside" : "Room placement",
    yn(access.room_placement),
  ]);
  if (!isPickup) rows.push(["Unpacking / debris", yn(access.unpacking_or_debris_removal)]);
  rows.push([
    "Freight elevator",
    access.freight_elevator
      ? access.freight_elevator_notes
        ? `Yes — ${access.freight_elevator_notes}`
        : "Yes"
      : "No",
  ]);
  rows.push(["Loading dock", yn(access.dock)]);
  rows.push([
    "Gated access",
    access.gated_access
      ? access.gate_code_or_instructions
        ? `Yes — ${access.gate_code_or_instructions}`
        : "Yes"
      : "No",
  ]);
  rows.push(["Tight turns / narrow halls", yn(access.tight_turns_or_narrow_halls)]);
  rows.push(["Long carry", yn(access.long_carry)]);
  rows.push(["Parking / time limits", yn(access.parking_or_time_restrictions)]);
  rows.push(["Fragile / special handling", yn(access.fragile_or_special_handling)]);
  rows.push(["More than 2 people", yn(access.needs_more_than_two_people)]);
  rows.push([
    isPickup ? "Disassembly may be needed" : "Assembly / disassembly",
    yn(access.disassembly_or_assembly),
  ]);
  if (includeLiftgate) {
    rows.push(["Liftgate at pickup", yn(access.liftgate_pickup)]);
    rows.push(["Liftgate at delivery", yn(access.liftgate_delivery)]);
  }
  if (access.notes) rows.push(["Notes", access.notes]);
  return rows;
}

/** Notable yeses for the short internal email (no auto liftgate on local truck jobs). */
export function accessHighlightRows(access = {}, { isPickup = false, includeLiftgate = false } = {}) {
  return accessAnswerRows(access, { isPickup, includeLiftgate }).filter(([, v]) => {
    const s = String(v || "");
    return s.startsWith("Yes") || (s !== "No" && s !== "—" && s !== "-");
  });
}

export function nonAccessReviewReasons(reasons = []) {
  const accessLike =
    /stairs|movers|elevator|tight turns|narrow hall|long carry|inside delivery|room placement|unpacking|debris|disassembly|assembly|parking|fragile|special handling|gated access|liftgate/i;
  return (reasons || []).filter((r) => !accessLike.test(String(r)));
}

function pageBottom(doc) {
  return doc.page.height - MARGIN - FOOTER_RESERVE;
}

function ensureSpace(doc, needed) {
  if (doc.y + needed <= pageBottom(doc)) return;
  doc.addPage();
}

function drawSectionTitle(doc, title, color = NAVY) {
  ensureSpace(doc, 20);
  doc.moveDown(0.25);
  const y = doc.y;
  doc.font("Helvetica-Bold").fontSize(10).fillColor(color).text(title, MARGIN, y, {
    width: CONTENT_WIDTH,
  });
  doc
    .moveTo(MARGIN, doc.y + 1)
    .lineTo(MARGIN + CONTENT_WIDTH, doc.y + 1)
    .strokeColor(color)
    .lineWidth(1)
    .stroke();
  doc.y += 6;
}

function drawRow(doc, label, value, { valueColor = "#222", labelW = 128 } = {}) {
  const text = value == null || value === "" ? "—" : String(value);
  ensureSpace(doc, 13);
  const y = doc.y;
  doc.font("Helvetica").fontSize(8.5).fillColor(MUTED).text(`${label}`, MARGIN, y, {
    width: labelW,
    lineBreak: false,
  });
  doc
    .font("Helvetica-Bold")
    .fontSize(8.5)
    .fillColor(valueColor)
    .text(text, MARGIN + labelW, y, {
      width: CONTENT_WIDTH - labelW,
    });
  doc.y = Math.max(doc.y, y + 11);
}

/** Two compact columns of label/value pairs. */
function drawTwoColRows(doc, leftRows, rightRows, { valueColorRight = "#222" } = {}) {
  const colGap = 16;
  const colW = (CONTENT_WIDTH - colGap) / 2;
  const labelW = 108;
  const rowH = 11;
  const n = Math.max(leftRows.length, rightRows.length);
  ensureSpace(doc, n * rowH + 4);
  let y = doc.y;
  for (let i = 0; i < n; i++) {
    if (y + rowH > pageBottom(doc)) {
      doc.addPage();
      y = MARGIN;
    }
    const L = leftRows[i];
    const R = rightRows[i];
    if (L) {
      doc.font("Helvetica").fontSize(8).fillColor(MUTED).text(L[0], MARGIN, y, {
        width: labelW,
        lineBreak: false,
      });
      doc
        .font("Helvetica-Bold")
        .fontSize(8)
        .fillColor("#222")
        .text(String(L[1] ?? "—"), MARGIN + labelW, y, {
          width: colW - labelW,
          lineBreak: false,
        });
    }
    if (R) {
      const x = MARGIN + colW + colGap;
      const isYes = String(R[1] || "").startsWith("Yes");
      doc.font("Helvetica").fontSize(8).fillColor(MUTED).text(R[0], x, y, {
        width: labelW,
        lineBreak: false,
      });
      doc
        .font("Helvetica-Bold")
        .fontSize(8)
        .fillColor(isYes ? valueColorRight : "#222")
        .text(String(R[1] ?? "—"), x + labelW, y, {
          width: colW - labelW,
          lineBreak: false,
        });
    }
    y += rowH;
  }
  doc.y = y + 4;
}

async function fetchMapBuffer(url) {
  if (!url) return null;
  try {
    const res = await fetch(url, { signal: AbortSignal.timeout(12000) });
    if (!res.ok) return null;
    return Buffer.from(await res.arrayBuffer());
  } catch {
    return null;
  }
}

export function buildFreightQuotePdfFilename(submission = {}, ctx = {}) {
  const name = sanitizeFilenamePart(submission.customer_name || "Customer", "Customer");
  const path =
    submission.delivery_path === "pickup_az"
      ? "Pickup"
      : submission.delivery_path === "local_az"
        ? "Local-Delivery"
        : "Freight";
  const stamp = String(ctx.requestId || new Date().toISOString().slice(0, 10))
    .replace(/[^a-zA-Z0-9._-]/g, "-")
    .slice(0, 40);
  return `${name}_${path}_Estimate_${stamp}.pdf`;
}

/**
 * @returns {Promise<Buffer|null>}
 */
export async function generateFreightQuotePdf(submission, ctx = {}) {
  const {
    requestId = "",
    submittedAt = "",
    route = null,
    localEstimate = null,
    nationwideRate = null,
    reviewReasons = [],
  } = ctx;

  const isLocal =
    submission.delivery_path === "local_az" || submission.delivery_path === "pickup_az";
  const isPickup = submission.delivery_path === "pickup_az";
  const includeLiftgate = !isLocal;
  const pathLabel = isPickup
    ? "Consignor pickup"
    : isLocal
      ? "Local Arizona delivery"
      : "Nationwide freight";
  const modeLabel =
    submission.request_mode === "estimate" ? "Get an estimate" : "Have us quote it";
  const addr =
    submission.delivery_address?.full ||
    [submission.street, submission.unit, submission.city, submission.state, submission.zip]
      .filter(Boolean)
      .join(", ");

  const destType = formatDestinationType(
    submission.destination_type ||
      (submission.access?.residential
        ? "Residential"
        : submission.access?.commercial
          ? "Commercial"
          : "")
  );

  const oneWay = route?.drive_minutes ?? localEstimate?.drive_minutes ?? null;
  const roundTrip = oneWay != null ? Number(oneWay) * 2 : null;
  const miles = route?.distance_miles ?? nationwideRate?.distance_miles ?? null;
  const estimate =
    isLocal && localEstimate?.estimated_price != null
      ? money(localEstimate.estimated_price)
      : !isLocal && nationwideRate?.range_low != null && nationwideRate?.range_high != null
        ? `${money(nationwideRate.range_low)} - ${money(nationwideRate.range_high)}`
        : "Pending";

  const mapRoute =
    route?.map_image_url || route?.directions_url
      ? route
      : nationwideRate?.route?.map_image_url || nationwideRate?.route?.directions_url
        ? nationwideRate.route
        : null;

  const mapBuf = await fetchMapBuffer(mapRoute?.map_image_url);
  const accessRows = accessAnswerRows(submission.access, { isPickup, includeLiftgate });
  const extraReview = nonAccessReviewReasons(reviewReasons);

  const requestLeft = [
    ["Mode", modeLabel],
    ["Path", pathLabel],
    ["Type", destType],
    ["Estimate", estimate],
  ];
  if (oneWay != null) requestLeft.push(["Drive", `${oneWay} min / ${roundTrip} min RT`]);
  if (miles != null) requestLeft.push(["Distance", `${miles} mi`]);
  if (isLocal) requestLeft.push(["Rate", "$95/hr round-trip"]);

  const requestRight = [
    ["Name", submission.customer_name || "—"],
    ["Phone", formatPhone(submission.customer_phone)],
    ["Email", submission.customer_email || "—"],
    ["Address", addr || "—"],
  ];
  if (submission.multi_item_note) requestRight.push(["Multi-item", submission.multi_item_note]);

  // Split access into two columns for density
  const mid = Math.ceil(accessRows.length / 2);
  const accessLeft = accessRows.slice(0, mid);
  const accessRight = accessRows.slice(mid);

  return new Promise((resolve, reject) => {
    const doc = new PDFDocument({
      size: "LETTER",
      autoFirstPage: true,
      margins: { top: MARGIN, bottom: MARGIN + 12, left: MARGIN, right: MARGIN },
      info: {
        Title: `${pathLabel} estimate`,
        Author: "Lost & Found Resale Interiors",
      },
    });
    const chunks = [];
    doc.on("data", (c) => chunks.push(c));
    doc.on("end", () => resolve(Buffer.concat(chunks)));
    doc.on("error", reject);

    let pageNo = 0;
    const stampFooter = () => {
      pageNo += 1;
      const label = `${requestId || "Freight estimate"}  ·  p.${pageNo}`;
      doc
        .font("Helvetica")
        .fontSize(7)
        .fillColor(MUTED)
        .text(label, MARGIN, doc.page.height - 22, {
          width: CONTENT_WIDTH,
          align: "center",
          lineBreak: false,
        });
    };
    stampFooter();
    doc.on("pageAdded", stampFooter);

    doc.font("Helvetica-Bold").fontSize(13).fillColor(NAVY).text("Lost & Found Resale Interiors", {
      width: CONTENT_WIDTH,
      align: "center",
    });
    doc
      .font("Helvetica")
      .fontSize(8)
      .fillColor(MUTED)
      .text("DELIVERY — PICKUP — FREIGHT", { width: CONTENT_WIDTH, align: "center" });
    doc.moveDown(0.35);
    doc
      .font("Helvetica-Bold")
      .fontSize(11)
      .fillColor(NAVY)
      .text(`Internal: ${pathLabel}  ·  ${estimate}`, { width: CONTENT_WIDTH });
    doc
      .font("Helvetica")
      .fontSize(8)
      .fillColor(MUTED)
      .text(`${requestId}${submittedAt ? `  ·  ${submittedAt}` : ""}`, { width: CONTENT_WIDTH });
    if (oneWay != null || miles != null) {
      const bits = [];
      if (oneWay != null) bits.push(`${oneWay} min one way — ${roundTrip} min round trip`);
      if (miles != null) bits.push(`${miles} mi`);
      doc.font("Helvetica").fontSize(9).fillColor("#222").text(bits.join("  ·  "), {
        width: CONTENT_WIDTH,
      });
    }

    drawSectionTitle(doc, "Request & customer");
    drawTwoColRows(doc, requestLeft, requestRight);

    drawSectionTitle(doc, "Access answers (from form)", RED);
    drawTwoColRows(doc, accessLeft, accessRight, { valueColorRight: RED });

    if (extraReview.length) {
      drawSectionTitle(doc, "Other review notes", RED);
      for (const r of extraReview) drawRow(doc, "Note", r, { valueColor: RED });
    }

    if (mapBuf) {
      drawSectionTitle(doc, "Route map");
      try {
        const maxW = CONTENT_WIDTH;
        const maxH = 150;
        const img = doc.openImage(mapBuf);
        const scale = Math.min(maxW / img.width, maxH / img.height, 1);
        const w = img.width * scale;
        const h = img.height * scale;
        ensureSpace(doc, h + 6);
        doc.image(mapBuf, MARGIN, doc.y, { width: w, height: h });
        doc.y += h + 4;
      } catch {
        doc.font("Helvetica-Oblique").fontSize(8).fillColor(MUTED).text("(Map unavailable)");
      }
    } else if (mapRoute?.directions_url) {
      drawRow(doc, "Maps", "Open route link in the email (URL omitted to save space)");
    }

    drawSectionTitle(doc, "Items");
    const items = submission.items || [];
    if (!items.length) {
      drawRow(doc, "Items", "None listed");
    } else {
      for (const row of items) {
        const p = row.pallet;
        ensureSpace(doc, 36);
        doc
          .font("Helvetica-Bold")
          .fontSize(9)
          .fillColor(NAVY)
          .text(`${row.index}. ${row.title || "Item"}`, MARGIN, doc.y, { width: CONTENT_WIDTH });
        const bits = [
          dims(row.width, row.depth, row.height),
          row.weight != null ? `${row.weight} lb` : null,
          row.price != null ? money(row.price) : null,
          row.set_count > 1 ? `set of ${row.set_count}` : null,
        ]
          .filter(Boolean)
          .join("  ·  ");
        if (bits) {
          doc.font("Helvetica").fontSize(8).fillColor("#222").text(bits, { width: CONTENT_WIDTH });
        }
        if (p) {
          doc
            .font("Helvetica")
            .fontSize(8)
            .fillColor("#222")
            .text(
              `Freight ${dims(p.width, p.depth, p.height)}  ·  ${p.weight} lb  ·  class ${classLabel(p)}  ·  non-stackable ${yn(p.non_stackable)}`,
              { width: CONTENT_WIDTH }
            );
        }
        if (row.product_url) {
          doc.font("Helvetica").fontSize(7).fillColor(NAVY).text(row.product_url, {
            width: CONTENT_WIDTH,
            link: row.product_url,
          });
        }
        doc.moveDown(0.2);
      }
    }

    doc.moveDown(0.4);
    doc
      .font("Helvetica")
      .fontSize(7)
      .fillColor(MUTED)
      .text("Internal use only — print for driver / warehouse clipboard.", {
        width: CONTENT_WIDTH,
        align: "center",
      });

    doc.end();
  });
}
