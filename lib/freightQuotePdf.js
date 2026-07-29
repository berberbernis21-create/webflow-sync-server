/**
 * Printable internal PDF for freight / local delivery / consignor pickup quotes.
 * Target: fit on 1–2 pages (no blank trailing pages).
 */

import PDFDocument from "pdfkit";
import { sanitizeFilenamePart } from "./consignmentFilenames.js";
import { formatInternalAssemblyFeeDetail, isArizonaStateOrZip } from "./freightPalletize.js";

const NAVY = "#07127c";
const MUTED = "#555555";
const RED = "#9c2f2f";
const MARGIN = 36;
const CONTENT_WIDTH = 540;
const FOOTER_RESERVE = 40;

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
  if (access.white_glove || access.white_glove_delivery) {
    rows.push(["White Glove", "Yes"]);
  }
  if (access.room_of_choice) rows.push(["Room of Choice", "Yes"]);
  if (access.warehouse_pickup) rows.push(["Warehouse / terminal pickup", "Yes — customer pickup"]);
  if (access.store_loading) rows.push(["Store loading (origin)", "Yes"]);
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
  rows.push([
    "More than 2 people (opinion)",
    access.needs_more_than_two_people
      ? `Yes — ${
          Number(access.extra_people) >= 2
            ? "2 extra people (4 total)"
            : Number(access.extra_people) === 1
              ? "1 extra person (3 total)"
              : "crew TBD"
        }${access.more_than_two_people_reason ? ` — ${access.more_than_two_people_reason}` : ""}`
      : "No",
  ]);
  rows.push([
    "Disassembly & reassembly",
    (() => {
      const req = String(access.assembly_required || "").toLowerCase();
      if (req === "idk") return "idk";
      if (req === "no") return "No";
      if (req === "yes") {
        const over = String(access.assembly_over_15 || "").toLowerCase();
        if (over === "yes") {
          const mins = Math.max(0, Math.floor(Number(access.assembly_extra_minutes) || 0));
          return mins > 0 ? `${mins} minutes` : "Yes";
        }
        if (over === "idk") return "idk";
        return "Within free 15 minutes";
      }
      return yn(access.disassembly_or_assembly);
    })(),
  ]);
  if (includeLiftgate) {
    rows.push([
      "Liftgate at pickup",
      access.liftgate_pickup_staff_to_confirm
        ? `Staff confirms with final quote (assume yes for now; form estimate: ${yn(access.liftgate_pickup)})`
        : yn(access.liftgate_pickup),
    ]);
    rows.push(["Liftgate at delivery (customer)", yn(access.liftgate_delivery)]);
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
    /stairs|movers|elevator|tight turns|narrow hall|long carry|inside delivery|room placement|unpacking|debris|disassembly|assembly|parking|fragile|special handling|gated access|liftgate|extra person|two trucks|oversized|round trip over|confirm stacking|flip\/stack/i;
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
  const valueW = Math.max(40, colW - labelW);
  const n = Math.max(leftRows.length, rightRows.length);
  let y = doc.y;
  for (let i = 0; i < n; i++) {
    const L = leftRows[i];
    const R = rightRows[i];
    const leftVal = L ? String(L[1] ?? "—") : "";
    const rightVal = R ? String(R[1] ?? "—") : "";
    // Measure wrapped height so long values cannot spill into the other column.
    doc.font("Helvetica-Bold").fontSize(8);
    const leftH = L ? doc.heightOfString(leftVal, { width: valueW }) : 0;
    const rightH = R ? doc.heightOfString(rightVal, { width: valueW }) : 0;
    const rowH = Math.max(11, leftH, rightH) + 2;
    if (y + rowH > pageBottom(doc)) {
      doc.addPage();
      y = MARGIN;
    }
    if (L) {
      doc.font("Helvetica").fontSize(8).fillColor(MUTED).text(L[0], MARGIN, y, {
        width: labelW,
        lineBreak: false,
      });
      doc
        .font("Helvetica-Bold")
        .fontSize(8)
        .fillColor("#222")
        .text(leftVal, MARGIN + labelW, y, {
          width: valueW,
        });
    }
    if (R) {
      const x = MARGIN + colW + colGap;
      const isYes = rightVal === "Yes" || rightVal.startsWith("Yes —") || rightVal.startsWith("Yes-");
      doc.font("Helvetica").fontSize(8).fillColor(MUTED).text(R[0], x, y, {
        width: labelW,
        lineBreak: false,
      });
      doc
        .font("Helvetica-Bold")
        .fontSize(8)
        .fillColor(isYes ? valueColorRight : "#222")
        .text(rightVal, x + labelW, y, {
          width: valueW,
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

export function buildFreightQuotePdfFilename(submission = {}, ctx = {}, options = {}) {
  const audience = options.audience === "customer" ? "customer" : "internal";
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
  const kind = audience === "customer" ? "Your-Summary" : "Estimate";
  return `${name}_${path}_${kind}_${stamp}.pdf`;
}

/**
 * @param {object} submission
 * @param {object} ctx
 * @param {{ audience?: "internal" | "customer" }} [options]
 * @returns {Promise<Buffer|null>}
 */
export async function generateFreightQuotePdf(submission, ctx = {}, options = {}) {
  const audience = options.audience === "customer" ? "customer" : "internal";
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
  const items = submission.items || [];
  const itemImageBufs = await Promise.all(
    items.map((row) => fetchMapBuffer(row.image_url || row.imageUrl || ""))
  );
  const accessRows = accessAnswerRows(submission.access, { isPickup, includeLiftgate });
  const extraReview = audience === "internal" ? nonAccessReviewReasons(reviewReasons) : [];

  const requestLeft = [
    ["Mode", modeLabel],
    ["Path", pathLabel],
    ["Type", destType],
    [!isLocal && audience === "customer" ? "Preliminary range" : "Estimate", estimate],
  ];
  if (oneWay != null && isLocal) requestLeft.push(["Drive", `${oneWay} min / ${roundTrip} min RT`]);
  if (miles != null) requestLeft.push(["Distance", `${miles} mi`]);
  if (isLocal) requestLeft.push(["Rate", "$95/hr round-trip"]);
  if (isLocal && Number(localEstimate?.assembly_fee) > 0) {
    requestLeft.push([
      "Additional Disassembly & Reassembly Time",
      audience === "internal"
        ? formatInternalAssemblyFeeDetail(localEstimate)
        : `${localEstimate.assembly_extra_minutes || 0} Minutes`,
    ]);
  } else if (isLocal && localEstimate?.assembly_required) {
    requestLeft.push(["Disassembly & Reassembly", "Within free 15 min (no extra charge)"]);
  }
  if (isLocal && localEstimate?.assembly_confirm_over_60) {
    requestLeft.push([
      "Confirm disassembly & reassembly",
      "Over 60 extra min — confirm with customer",
    ]);
  }

  const requestRight = [
    ["Name", submission.customer_name || "—"],
    ["Phone", formatPhone(submission.customer_phone)],
    ["Email", submission.customer_email || "—"],
    ["Address", addr || "—"],
  ];
  const preferredDaysWindows = String(submission.access?.preferred_days_windows || "").trim();
  if (preferredDaysWindows) {
    requestRight.push(["Preferred days / windows", preferredDaysWindows]);
  }
  if (submission.multi_item_note) requestRight.push(["Multi-item", submission.multi_item_note]);

  // Split access into two columns for density
  const mid = Math.ceil(accessRows.length / 2);
  const accessLeft = accessRows.slice(0, mid);
  const accessRight = accessRows.slice(mid);

  const destState = submission.state || submission.delivery_address?.state || "";
  const destZip = submission.zip || submission.delivery_address?.zip || "";
  const outOfAz = !isLocal && !isArizonaStateOrZip({ state: destState, zip: destZip });
  const isNationwideCustomer = !isLocal && audience === "customer";

  const headline =
    audience === "customer"
      ? `Your request summary  ·  ${pathLabel}  ·  ${!isLocal ? `Preliminary ${estimate}` : estimate}`
      : `Internal: ${pathLabel}  ·  ${estimate}`;
  const footerNote = isNationwideCustomer
    ? "Preliminary freight range only — confirmed carrier rates follow by email."
    : audience === "customer"
      ? "Estimate only — review your answers for accuracy when you call to confirm pricing."
      : "Internal use only — print for driver / warehouse clipboard.";
  const accessTitle =
    audience === "customer" ? "Your access answers" : "Access answers (from form)";
  const requestTitle =
    audience === "customer" ? "Your request" : "Request & customer";
  const pdfTitle =
    audience === "customer"
      ? `Your ${pathLabel} estimate summary`
      : `${pathLabel} estimate`;

  return new Promise((resolve, reject) => {
    const doc = new PDFDocument({
      size: "LETTER",
      autoFirstPage: true,
      bufferPages: true,
      margins: { top: MARGIN, bottom: MARGIN + 20, left: MARGIN, right: MARGIN },
      info: {
        Title: pdfTitle,
        Author: "Lost & Found Resale Interiors",
      },
    });
    const chunks = [];
    doc.on("data", (c) => chunks.push(c));
    doc.on("end", () => resolve(Buffer.concat(chunks)));
    doc.on("error", reject);

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
      .text(headline, { width: CONTENT_WIDTH });
    doc
      .font("Helvetica")
      .fontSize(8)
      .fillColor(MUTED)
      .text(`${requestId}${submittedAt ? `  ·  ${submittedAt}` : ""}`, { width: CONTENT_WIDTH });
    if (oneWay != null || miles != null) {
      const bits = [];
      if (isLocal && oneWay != null) bits.push(`${oneWay} min one way — ${roundTrip} min round trip`);
      if (miles != null) bits.push(`${miles} mi`);
      if (bits.length) {
        doc.font("Helvetica").fontSize(9).fillColor("#222").text(bits.join("  ·  "), {
          width: CONTENT_WIDTH,
        });
      }
    }

    if (isNationwideCustomer) {
      doc.moveDown(0.25);
      doc
        .font("Helvetica-Bold")
        .fontSize(9)
        .fillColor("#222")
        .text(
          "Our team will review your submitted freight details and follow up by email with confirmed rates and the best available carrier options.",
          { width: CONTENT_WIDTH }
        );
      doc.moveDown(0.15);
      doc
        .font("Helvetica")
        .fontSize(8)
        .fillColor("#333")
        .text(
          "This preliminary freight range is based on the destination, estimated pallet size and weight, and the selected freight services. Final pricing may move up or down based on carrier capacity, confirmed dimensions, terminal or residential service, liftgate requirements, and available carrier options.",
          { width: CONTENT_WIDTH }
        );
      doc.moveDown(0.12);
      doc.font("Helvetica-Bold").fontSize(8).fillColor("#222").text("Freight savings option: ", {
        width: CONTENT_WIDTH,
        continued: true,
      });
      doc
        .font("Helvetica")
        .fillColor("#333")
        .text(
          "Picking up from a nearby freight terminal or warehouse may significantly reduce the final freight cost compared with residential delivery. Available options will be provided with the confirmed rates.",
          { width: CONTENT_WIDTH }
        );
      if (outOfAz) {
        doc.moveDown(0.12);
        doc
          .font("Helvetica-Bold")
          .fontSize(8)
          .fillColor("#222")
          .text("Out-of-state purchase savings: ", {
            width: CONTENT_WIDTH,
            continued: true,
          });
        doc
          .font("Helvetica")
          .fillColor("#333")
          .text(
            "When the item is purchased online and shipped outside Arizona, no Arizona sales tax is charged on the merchandise purchase. This is approximately an 8% savings compared with an Arizona purchase.",
            { width: CONTENT_WIDTH }
          );
        doc
          .font("Helvetica")
          .fontSize(7)
          .fillColor(MUTED)
          .text(
            "This applies to the merchandise purchase. Freight and other service charges are separate.",
            { width: CONTENT_WIDTH }
          );
      }
      doc.moveDown(0.2);
    }

    drawSectionTitle(doc, requestTitle);
    drawTwoColRows(doc, requestLeft, requestRight);

    drawSectionTitle(doc, accessTitle, RED);
    drawTwoColRows(doc, accessLeft, accessRight, { valueColorRight: RED });

    if (extraReview.length) {
      drawSectionTitle(doc, "Other review notes", RED);
      for (const r of extraReview) drawRow(doc, "Note", r, { valueColor: RED });
    }

    if (mapBuf) {
      drawSectionTitle(doc, "Route map");
      try {
        const maxW = CONTENT_WIDTH;
        const maxH = isNationwideCustomer ? 95 : 150;
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
    if (!items.length) {
      drawRow(doc, "Items", "None listed");
    } else {
      for (let i = 0; i < items.length; i++) {
        const row = items[i];
        const p = row.pallet;
        const thumbBuf = itemImageBufs[i];
        let thumbW = 0;
        let thumbH = 0;
        const thumbMax = isNationwideCustomer ? 56 : 68;

        if (thumbBuf) {
          try {
            const img = doc.openImage(thumbBuf);
            const scale = Math.min(thumbMax / img.width, thumbMax / img.height, 1);
            thumbW = img.width * scale;
            thumbH = img.height * scale;
          } catch {
            /* ignore bad image */
          }
        }

        ensureSpace(doc, Math.max(40, thumbH + 10));
        const y0 = doc.y;
        const textX = thumbW ? MARGIN + thumbMax + 10 : MARGIN;
        const textW = thumbW ? CONTENT_WIDTH - thumbMax - 10 : CONTENT_WIDTH;

        if (thumbBuf && thumbW) {
          try {
            doc.image(thumbBuf, MARGIN, y0, { width: thumbW, height: thumbH });
          } catch {
            /* ignore */
          }
        }

        doc
          .font("Helvetica-Bold")
          .fontSize(9)
          .fillColor(NAVY)
          .text(`${row.index}. ${row.title || "Item"}`, textX, y0, { width: textW });
        const bits = [
          dims(row.width, row.depth, row.height),
          row.weight != null ? `${row.weight} lb` : null,
          !isPickup && row.price != null ? money(row.price) : null,
          row.set_count > 1 ? `set of ${row.set_count}` : null,
          isLocal
            ? (() => {
                const req = String(row.assembly_required || "").toLowerCase();
                if (req === "idk") return "Disassembly & reassembly: idk";
                if (req === "no") return "Disassembly & reassembly: no";
                if (req === "yes") {
                  const over = String(row.assembly_over_15 || "").toLowerCase();
                  if (over === "yes") {
                    const mins = Math.max(0, Math.floor(Number(row.assembly_extra_minutes) || 0));
                    return mins > 0
                      ? `Disassembly & reassembly: ${mins} minutes`
                      : "Disassembly & reassembly: yes";
                  }
                  if (over === "idk") return "Disassembly & reassembly: idk";
                  return "Disassembly & reassembly: within free 15 minutes";
                }
                return null;
              })()
            : null,
        ]
          .filter(Boolean)
          .join("  ·  ");
        if (bits) {
          doc.font("Helvetica").fontSize(8).fillColor("#222").text(bits, textX, doc.y, {
            width: textW,
          });
        }
        if (p) {
          doc
            .font("Helvetica")
            .fontSize(8)
            .fillColor("#222")
            .text(
              `Freight ${dims(p.width, p.depth, p.height)}  ·  ${p.weight} lb  ·  class ${classLabel(p)}  ·  non-stackable ${yn(p.non_stackable)}`,
              textX,
              doc.y,
              { width: textW }
            );
        }
        if (row.product_url) {
          doc.font("Helvetica").fontSize(7).fillColor(NAVY).text(row.product_url, textX, doc.y, {
            width: textW,
            link: row.product_url,
          });
        }
        doc.y = Math.max(doc.y, y0 + (thumbH || 0)) + 8;
      }
    }

    if (isNationwideCustomer) {
      // Keep Next step + footer note fully above the page stamp.
      ensureSpace(doc, 58);
      doc.moveDown(0.25);
      doc.font("Helvetica-Bold").fontSize(8.5).fillColor("#222").text("Next step: ", {
        width: CONTENT_WIDTH,
        continued: true,
      });
      doc
        .font("Helvetica")
        .fillColor("#333")
        .text(
          "Lost & Found will review the shipment details and follow up by email with final rates and the best available carrier options.",
          { width: CONTENT_WIDTH }
        );
    }

    ensureSpace(doc, 22);
    doc.moveDown(0.3);
    doc
      .font("Helvetica")
      .fontSize(7)
      .fillColor(MUTED)
      .text(footerNote, {
        width: CONTENT_WIDTH,
        align: "center",
      });

    // Stamp footers after content. Writing near the bottom during layout
    // overflows PDFKit's bottom margin and used to auto-insert a blank page 1.
    const range = doc.bufferedPageRange();
    for (let i = 0; i < range.count; i++) {
      doc.switchToPage(range.start + i);
      const savedBottom = doc.page.margins.bottom;
      doc.page.margins.bottom = 0;
      doc
        .font("Helvetica")
        .fontSize(7)
        .fillColor(MUTED)
        .text(`${requestId || "Freight estimate"}  ·  p.${i + 1}`, MARGIN, doc.page.height - 18, {
          width: CONTENT_WIDTH,
          align: "center",
          lineBreak: false,
        });
      doc.page.margins.bottom = savedBottom;
    }

    doc.end();
  });
}
