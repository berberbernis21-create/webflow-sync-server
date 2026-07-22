/**
 * Printable internal PDF for freight / local delivery / consignor pickup quotes.
 * Attached to the ops email (same idea as consignment estimate PDFs).
 */

import PDFDocument from "pdfkit";
import { sanitizeFilenamePart } from "./consignmentFilenames.js";

const NAVY = "#07127c";
const MUTED = "#555555";
const RED = "#9c2f2f";
const MARGIN = 48;
const CONTENT_WIDTH = 516;
const FOOTER_RESERVE = 32;

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
  return `${w}" W x ${d}" D x ${h}" H`;
}

function yn(v) {
  if (v === true) return "Yes";
  if (v === false) return "No";
  return "-";
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

function accessHighlightRows(access = {}, { isPickup = false } = {}) {
  const rows = [];
  if (access.stairs) {
    rows.push([
      "Stairs",
      `${access.stair_flights || "?"} flight(s)${access.stair_notes ? ` | ${access.stair_notes}` : ""}`,
    ]);
  }
  if (access.dock) rows.push(["Loading dock", "Yes"]);
  if (access.freight_elevator) {
    rows.push([
      "Freight elevator",
      access.freight_elevator_notes ? `Yes | ${access.freight_elevator_notes}` : "Yes",
    ]);
  }
  if (access.gated_access) {
    rows.push([
      "Gated access",
      access.gate_code_or_instructions ? access.gate_code_or_instructions : "Yes",
    ]);
  }
  if (access.tight_turns_or_narrow_halls) rows.push(["Tight turns / narrow halls", "Yes"]);
  if (access.inside_delivery) rows.push([isPickup ? "Inside pickup" : "Inside delivery", "Yes"]);
  if (access.room_placement) {
    rows.push([isPickup ? "Carry-out from room / inside" : "Room placement", "Yes"]);
  }
  if (access.unpacking_or_debris_removal) rows.push(["Unpacking / debris", "Yes"]);
  if (access.needs_more_than_two_people) rows.push(["More than 2 people", "Yes"]);
  if (access.disassembly_or_assembly) {
    rows.push([isPickup ? "Disassembly may be needed" : "Assembly / disassembly", "Yes"]);
  }
  if (access.long_carry) rows.push(["Long carry", "Yes"]);
  if (access.parking_or_time_restrictions) rows.push(["Parking / time limits", "Yes"]);
  if (access.fragile_or_special_handling) rows.push(["Fragile / special handling", "Yes"]);
  if (access.liftgate_pickup) rows.push(["Liftgate at pickup", "Yes"]);
  if (access.liftgate_delivery) rows.push(["Liftgate at delivery", "Yes"]);
  if (access.notes) rows.push(["Notes", access.notes]);
  return rows;
}

/** Access-related review lines are already in Access highlights — keep only other notes. */
export function nonAccessReviewReasons(reasons = []) {
  const accessLike =
    /stairs|movers|elevator|tight turns|narrow hall|long carry|inside delivery|room placement|unpacking|debris|disassembly|assembly|parking|fragile|special handling|gated access/i;
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
  ensureSpace(doc, 28);
  doc.moveDown(0.4);
  doc.font("Helvetica-Bold").fontSize(12).fillColor(color).text(title, MARGIN, doc.y, {
    width: CONTENT_WIDTH,
  });
  doc
    .moveTo(MARGIN, doc.y + 2)
    .lineTo(MARGIN + CONTENT_WIDTH, doc.y + 2)
    .strokeColor(color)
    .lineWidth(1.2)
    .stroke();
  doc.y += 10;
}

function drawRow(doc, label, value, { valueColor = "#222" } = {}) {
  const text = value == null || value === "" ? "-" : String(value);
  ensureSpace(doc, 18);
  const y = doc.y;
  doc.font("Helvetica").fontSize(10).fillColor(MUTED).text(`${label}:`, MARGIN, y, {
    width: 150,
    continued: false,
  });
  doc.font("Helvetica-Bold").fontSize(10).fillColor(valueColor).text(text, MARGIN + 150, y, {
    width: CONTENT_WIDTH - 150,
  });
  doc.y = Math.max(doc.y, y + 14);
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
  const accessRows = accessHighlightRows(submission.access, { isPickup });
  const extraReview = nonAccessReviewReasons(reviewReasons);

  return new Promise((resolve, reject) => {
    const doc = new PDFDocument({
      size: "LETTER",
      bufferPages: true,
      margins: { top: MARGIN, bottom: MARGIN, left: MARGIN, right: MARGIN },
      info: {
        Title: `${pathLabel} estimate`,
        Author: "Lost & Found Resale Interiors",
      },
    });
    const chunks = [];
    doc.on("data", (c) => chunks.push(c));
    doc.on("end", () => resolve(Buffer.concat(chunks)));
    doc.on("error", reject);

    doc.font("Helvetica-Bold").fontSize(16).fillColor(NAVY).text("Lost & Found Resale Interiors", {
      width: CONTENT_WIDTH,
      align: "center",
    });
    doc
      .font("Helvetica")
      .fontSize(9)
      .fillColor(MUTED)
      .text("DELIVERY | PICKUP | FREIGHT", { width: CONTENT_WIDTH, align: "center" });
    doc.moveDown(0.6);
    doc
      .font("Helvetica-Bold")
      .fontSize(14)
      .fillColor(NAVY)
      .text(`Internal summary: ${pathLabel}`, { width: CONTENT_WIDTH });
    doc
      .font("Helvetica")
      .fontSize(9)
      .fillColor(MUTED)
      .text(`${requestId}${submittedAt ? `  |  ${submittedAt}` : ""}`, { width: CONTENT_WIDTH });
    doc.moveDown(0.5);

    doc
      .font("Helvetica-Bold")
      .fontSize(18)
      .fillColor(NAVY)
      .text(estimate, { width: CONTENT_WIDTH });
    const driveBits = [];
    if (oneWay != null) driveBits.push(`${oneWay} min one way`);
    if (roundTrip != null) driveBits.push(`${roundTrip} min round trip`);
    if (miles != null) driveBits.push(`${miles} mi`);
    if (driveBits.length) {
      doc.font("Helvetica").fontSize(11).fillColor("#222").text(driveBits.join("  |  "), {
        width: CONTENT_WIDTH,
      });
    }
    doc.moveDown(0.4);

    drawSectionTitle(doc, "Customer");
    drawRow(doc, "Name", submission.customer_name);
    drawRow(doc, "Email", submission.customer_email);
    drawRow(doc, "Phone", formatPhone(submission.customer_phone));

    drawSectionTitle(doc, "Request");
    drawRow(doc, "Mode", modeLabel);
    drawRow(doc, "Path", pathLabel);
    drawRow(doc, "Type", destType);
    drawRow(doc, "Address", addr);
    if (submission.unit || submission.delivery_address?.unit) {
      drawRow(doc, "Unit", submission.unit || submission.delivery_address.unit);
    }
    drawRow(doc, "Estimate", estimate);
    if (oneWay != null) {
      drawRow(doc, "Drive", `${oneWay} min one way | ${roundTrip} min round trip`);
    }
    if (miles != null) drawRow(doc, "Distance", `${miles} mi`);
    if (isLocal) drawRow(doc, "Rate", "$95/hour round-trip (not one way)");
    if (submission.multi_item_note) drawRow(doc, "Multi-item note", submission.multi_item_note);
    if (submission.page_url) drawRow(doc, "Page", submission.page_url);
    if (mapRoute?.directions_url) drawRow(doc, "Google Maps", mapRoute.directions_url);

    drawSectionTitle(doc, "Access highlights", RED);
    if (!accessRows.length) {
      drawRow(doc, "Notes", "None noted", { valueColor: RED });
    } else {
      for (const [k, v] of accessRows) {
        drawRow(doc, k, v, { valueColor: RED });
      }
    }

    if (extraReview.length) {
      drawSectionTitle(doc, "Other review notes", RED);
      for (const r of extraReview) {
        drawRow(doc, "Note", r, { valueColor: RED });
      }
    }

    if (mapBuf) {
      drawSectionTitle(doc, "Route map");
      try {
        const maxW = CONTENT_WIDTH;
        const maxH = 220;
        const img = doc.openImage(mapBuf);
        const scale = Math.min(maxW / img.width, maxH / img.height, 1);
        const w = img.width * scale;
        const h = img.height * scale;
        ensureSpace(doc, h + 8);
        doc.image(mapBuf, MARGIN, doc.y, { width: w, height: h });
        doc.y += h + 8;
      } catch {
        doc.font("Helvetica-Oblique").fontSize(9).fillColor(MUTED).text("(Map image could not be embedded)");
      }
    }

    drawSectionTitle(doc, "Items");
    const items = submission.items || [];
    if (!items.length) {
      drawRow(doc, "Items", "None listed");
    } else {
      for (const row of items) {
        const p = row.pallet;
        ensureSpace(doc, 56);
        doc
          .font("Helvetica-Bold")
          .fontSize(11)
          .fillColor(NAVY)
          .text(`${row.index}. ${row.title || "Item"}`, MARGIN, doc.y, { width: CONTENT_WIDTH });
        const bits = [
          dims(row.width, row.depth, row.height),
          row.weight != null ? `${row.weight} lb` : null,
          row.price != null ? money(row.price) : null,
          row.set_count > 1 ? `set of ${row.set_count}` : null,
        ]
          .filter(Boolean)
          .join(" | ");
        if (bits) {
          doc.font("Helvetica").fontSize(10).fillColor("#222").text(bits, { width: CONTENT_WIDTH });
        }
        if (p) {
          doc
            .font("Helvetica")
            .fontSize(10)
            .fillColor("#222")
            .text(
              `Freight: ${dims(p.width, p.depth, p.height)} | ${p.weight} lb | class ${classLabel(p)} | non-stackable: ${yn(p.non_stackable)}`,
              { width: CONTENT_WIDTH }
            );
          if (p.packing_notes?.length) {
            doc
              .font("Helvetica")
              .fontSize(9)
              .fillColor(MUTED)
              .text(`Packing: ${p.packing_notes.join(" ")}`, { width: CONTENT_WIDTH });
          }
        } else {
          doc.font("Helvetica").fontSize(10).fillColor(MUTED).text("Freight: incomplete", {
            width: CONTENT_WIDTH,
          });
        }
        if (row.product_url) {
          doc.font("Helvetica").fontSize(9).fillColor(NAVY).text(row.product_url, {
            width: CONTENT_WIDTH,
            link: row.product_url,
          });
        }
        doc.moveDown(0.35);
      }
    }

    doc.moveDown(0.8);
    doc
      .font("Helvetica")
      .fontSize(8)
      .fillColor(MUTED)
      .text("Internal use only | print this PDF for the driver / warehouse clipboard.", MARGIN, doc.y, {
        width: CONTENT_WIDTH,
        align: "center",
      });

    const range = doc.bufferedPageRange();
    for (let i = 0; i < range.count; i++) {
      doc.switchToPage(range.start + i);
      doc
        .font("Helvetica")
        .fontSize(8)
        .fillColor(MUTED)
        .text(`Page ${i + 1} of ${range.count}`, MARGIN, doc.page.height - 28, {
          width: CONTENT_WIDTH,
          align: "center",
        });
    }

    doc.end();
  });
}
