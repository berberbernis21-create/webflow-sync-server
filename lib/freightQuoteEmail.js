import { sendEmail, sendInternalNotification } from "../emailService.js";
import {
  FREIGHTCENTER_PHONE,
  LOST_FOUND_EMAIL,
  LOST_FOUND_PHONE,
} from "./freightPalletize.js";

const FONT = "Arial,Helvetica,sans-serif";

function escapeHtml(str) {
  return String(str ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function yn(v) {
  return v ? "Yes" : "No";
}

function dims(w, d, h) {
  if (w == null || d == null || h == null) return "—";
  return `${w}" W × ${d}" D × ${h}" H`;
}

function classLabel(pallet) {
  if (!pallet) return "—";
  if (pallet.freight_class == null) {
    return `To be confirmed${
      pallet.suggested_freight_class ? ` (staff hint: ${pallet.suggested_freight_class})` : ""
    }`;
  }
  return String(pallet.freight_class);
}

function tableHtml(rows) {
  return [
    `<table cellpadding="0" cellspacing="0" style="width:100%;border-collapse:collapse;font-size:14px;">`,
    ...rows.map(
      ([k, v]) =>
        `<tr><td style="padding:6px 10px 6px 0;color:#666;vertical-align:top;width:40%;">${escapeHtml(
          k
        )}</td><td style="padding:6px 0;color:#222;vertical-align:top;">${escapeHtml(
          String(v ?? "")
        )}</td></tr>`
    ),
    `</table>`,
  ].join("");
}

function accessRows(access = {}) {
  return [
    ["Residential", yn(access.residential)],
    ["Commercial", yn(access.commercial)],
    ["Dock", yn(access.dock)],
    ["Forklift", yn(access.forklift)],
    ["Freight elevator", yn(access.freight_elevator)],
    ["Elevator notes", access.freight_elevator_notes || "—"],
    ["Stairs", yn(access.stairs)],
    ["Flights of stairs", String(access.stair_flights ?? 0)],
    ["More than 2 people", yn(access.needs_more_than_two_people)],
    ["Tight turns / narrow halls", yn(access.tight_turns_or_narrow_halls)],
    ["Gated access", yn(access.gated_access)],
    ["Gate code / instructions", access.gate_code_or_instructions || "—"],
    ["Inside delivery", yn(access.inside_delivery)],
    ["Room placement", yn(access.room_placement)],
    ["Unpacking / debris removal", yn(access.unpacking_or_debris_removal)],
    ["Disassembly / assembly", yn(access.disassembly_or_assembly)],
    ["Long carry", yn(access.long_carry)],
    ["Parking / time restrictions", yn(access.parking_or_time_restrictions)],
    ["Fragile / special handling", yn(access.fragile_or_special_handling)],
    ["Liftgate pickup", yn(access.liftgate_pickup)],
    ["Liftgate delivery", yn(access.liftgate_delivery)],
    ["Access notes", access.notes || "—"],
  ];
}

function itemsHtml(items = []) {
  return items
    .map((row) => {
      const p = row.pallet;
      return [
        `<div style="margin:0 0 14px;padding:12px 14px;border:1px solid #e8e4dc;border-radius:6px;background:#fafaf8;">`,
        `<p style="margin:0 0 6px;font-weight:700;">${escapeHtml(row.index)}. ${escapeHtml(
          row.title
        )}</p>`,
        `<p style="margin:0;font-size:13px;color:#555;line-height:1.55;">`,
        `Source: ${escapeHtml(row.source)} · Qty: ${escapeHtml(row.quantity)}`,
        row.price ? `<br/>Listed price: ${escapeHtml(row.price)}` : "",
        `<br/>Product dims: ${escapeHtml(dims(row.width, row.depth, row.height))}`,
        row.weight != null ? ` · ${escapeHtml(row.weight)} lb` : "",
        p
          ? `<br/>Pallet entry: ${escapeHtml(dims(p.width, p.depth, p.height))} · ${escapeHtml(
              p.weight
            )} lb · Class ${escapeHtml(classLabel(p))} · Non-stackable: ${yn(p.non_stackable)}` +
            (p.set_count > 1
              ? `<br/>Set packing: ${escapeHtml(p.packing_mode || "")} · ${escapeHtml(
                  p.pieces_per_layer
                )}/layer × ${escapeHtml(p.layers)} layer(s) · stacked ~${escapeHtml(
                  p.stacked_height_in
                )}"`
              : "") +
            (Array.isArray(p.packing_notes) && p.packing_notes.length
              ? `<br/><span style="color:#555;">${escapeHtml(p.packing_notes.join(" "))}</span>`
              : "")
          : `<br/><span style="color:#b42318;">Missing dims/weight — cannot palletize yet</span>`,
        row.product_url
          ? `<br/><a href="${escapeHtml(row.product_url)}">${escapeHtml(row.product_url)}</a>`
          : "",
        `</p></div>`,
      ].join("");
    })
    .join("");
}

function itemsText(items = []) {
  const lines = [];
  for (const row of items) {
    const p = row.pallet;
    lines.push(
      "",
      `--- Item ${row.index}: ${row.title} ---`,
      `Source: ${row.source} · Qty: ${row.quantity}`,
      row.price ? `Price: ${row.price}` : null,
      `Product: ${dims(row.width, row.depth, row.height)}${row.weight != null ? ` · ${row.weight} lb` : ""}`,
      p
        ? `Pallet: ${dims(p.width, p.depth, p.height)} · ${p.weight} lb · Class ${classLabel(p)} · Non-stackable: ${yn(p.non_stackable)}`
        : "Pallet: incomplete",
      row.product_url ? `URL: ${row.product_url}` : null
    );
  }
  return lines.filter((l) => l != null).join("\n");
}

export function buildFreightQuoteEmails(submission, ctx = {}) {
  const {
    requestId,
    submittedAt,
    route = null,
    localEstimate = null,
    reviewReasons = [],
  } = ctx;

  const isLocal = submission.delivery_path === "local_az";
  const modeLabel = submission.request_mode === "estimate" ? "Estimate" : "Please quote";
  const addr = submission.delivery_address?.full || `${submission.street}, ${submission.city}, ${submission.state} ${submission.zip}`;

  const subject = isLocal
    ? `Local Delivery Estimate Request — ${submission.customer_name} — ${submission.zip}`
    : `Nationwide Freight Quote Request — ${submission.customer_name} — ${submission.zip}`;

  const localBlock =
    isLocal && localEstimate?.estimated_price != null
      ? [
          ["Preliminary route estimate", `$${localEstimate.estimated_price}`],
          ["One-way drive time", `${route?.drive_minutes ?? localEstimate.drive_minutes} minutes`],
          [
            "Distance",
            route?.distance_miles != null ? `${route.distance_miles} miles` : "—",
          ],
          ["Formula", "$95 for first 20 min, then +$15 per started 8-min block"],
        ]
      : isLocal
        ? [["Preliminary route estimate", "Pending manual review (route time unavailable)"]]
        : [];

  const internalHtml = [
    `<div style="font-family:${FONT};line-height:1.5;color:#222;max-width:740px;">`,
    `<h2 style="margin:0 0 8px;">${escapeHtml(subject)}</h2>`,
    `<p style="margin:0 0 14px;font-size:13px;color:#555;">Request ID: <strong>${escapeHtml(
      requestId
    )}</strong> · ${escapeHtml(submittedAt || "")}</p>`,
    tableHtml([
      ["Request mode", modeLabel],
      ["Delivery path", isLocal ? "Local Arizona" : "Nationwide freight"],
      ["Customer", submission.customer_name],
      ["Email", submission.customer_email],
      ["Phone", submission.customer_phone || "—"],
      ["Destination type", submission.destination_type],
      ["Full address", addr],
      ["Origin", submission.origin_address],
      ["Page URL", submission.page_url || "—"],
      ...localBlock,
      ["Manual review", reviewReasons.length ? "Yes" : "No"],
      ["Review reasons", reviewReasons.length ? reviewReasons.join("; ") : "—"],
      [
        "Multi-item note",
        submission.multi_item_note || "—",
      ],
    ]),
    `<h3 style="margin:22px 0 8px;">Access / accessorials</h3>`,
    tableHtml(accessRows(submission.access)),
    `<h3 style="margin:22px 0 8px;">Items (server-side SOP palletize)</h3>`,
    itemsHtml(submission.items),
    `<p style="margin:16px 0 0;font-size:12px;color:#777;">Lost &amp; Found does not book the shipment. Nationwide: provide FreightCenter quote + Shipment ID; customer books with FreightCenter (${FREIGHTCENTER_PHONE}).</p>`,
    `</div>`,
  ].join("");

  const internalText = [
    subject,
    `Request ID: ${requestId}`,
    `Submitted: ${submittedAt}`,
    `Mode: ${modeLabel}`,
    `Path: ${isLocal ? "local_az" : "nationwide"}`,
    `Customer: ${submission.customer_name}`,
    `Email: ${submission.customer_email}`,
    `Phone: ${submission.customer_phone || "—"}`,
    `Address: ${addr}`,
    ...localBlock.map(([k, v]) => `${k}: ${v}`),
    reviewReasons.length ? `Review: ${reviewReasons.join("; ")}` : null,
    "",
    "Access:",
    ...accessRows(submission.access).map(([k, v]) => `  ${k}: ${v}`),
    itemsText(submission.items),
    submission.page_url ? `Page: ${submission.page_url}` : null,
  ]
    .filter((l) => l != null)
    .join("\n");

  let customerHtml;
  let customerSubject;
  let customerText;

  if (isLocal) {
    const price =
      localEstimate?.estimated_price != null ? `$${localEstimate.estimated_price}` : "Pending confirmation";
    const mins = route?.drive_minutes ?? localEstimate?.drive_minutes;
    customerSubject = "Your Preliminary Local Delivery Estimate — Lost & Found Resale";
    customerHtml = [
      `<div style="font-family:${FONT};line-height:1.55;color:#222;max-width:640px;">`,
      `<h2 style="margin:0 0 12px;">Your Preliminary Local Delivery Estimate</h2>`,
      `<p style="margin:0 0 8px;font-size:22px;font-weight:700;">Estimated route price: ${escapeHtml(
        price
      )}</p>`,
      mins != null
        ? `<p style="margin:0 0 16px;font-size:16px;">Estimated one-way drive time: <strong>${escapeHtml(
            mins
          )} minutes</strong></p>`
        : `<p style="margin:0 0 16px;">We will confirm drive time and pricing shortly.</p>`,
      `<p style="margin:0 0 14px;font-size:14px;color:#444;">This is a <strong>best estimate</strong> for two movers and a box truck from our Scottsdale showroom. Access (stairs, elevators, tight turns, extra labor, inside placement, fragile handling, parking) may change the final price. Lost &amp; Found will confirm pricing before you book.</p>`,
      reviewReasons.length
        ? `<p style="margin:0 0 14px;padding:10px 12px;background:#fff8e8;border-left:3px solid #c9a227;font-size:13px;"><strong>May need manual review:</strong> ${escapeHtml(
            reviewReasons.join("; ")
          )}</p>`
        : "",
      `<h3 style="margin:18px 0 8px;">Delivery summary</h3>`,
      tableHtml([
        ["Name", submission.customer_name],
        ["Address", addr],
        ["Request ID", requestId],
      ]),
      `<h3 style="margin:18px 0 8px;">Access details you provided</h3>`,
      tableHtml(accessRows(submission.access)),
      `<h3 style="margin:18px 0 8px;">Items</h3>`,
      itemsHtml(submission.items),
      `<p style="margin:18px 0 0;font-size:13px;color:#555;">Questions? Call ${LOST_FOUND_PHONE} or email ${LOST_FOUND_EMAIL}.</p>`,
      `<p style="margin:10px 0 0;font-size:12px;color:#888;">Lost &amp; Found Resale · Scottsdale · lostandfoundresale.com</p>`,
      `</div>`,
    ].join("");
    customerText = [
      "Your Preliminary Local Delivery Estimate",
      `Estimated route price: ${price}`,
      mins != null ? `Estimated one-way drive time: ${mins} minutes` : null,
      "",
      "This is a best estimate for two movers and a box truck. Access and labor may change the final price.",
      reviewReasons.length ? `Review notes: ${reviewReasons.join("; ")}` : null,
      `Address: ${addr}`,
      `Request ID: ${requestId}`,
      itemsText(submission.items),
      "",
      `Lost & Found: ${LOST_FOUND_PHONE} · ${LOST_FOUND_EMAIL}`,
    ]
      .filter((l) => l != null)
      .join("\n");
  } else {
    customerSubject = "Your Nationwide Freight Request Is Ready for Review — Lost & Found Resale";
    customerHtml = [
      `<div style="font-family:${FONT};line-height:1.55;color:#222;max-width:640px;">`,
      `<h2 style="margin:0 0 12px;">Your Nationwide Freight Request Is Ready for Review</h2>`,
      `<p style="margin:0 0 14px;">Thanks, ${escapeHtml(
        submission.customer_name
      )}. We received your freight details. Confirmed carrier pricing and the Shipment ID will follow after our team reviews the shipment — Lost &amp; Found does not book the carrier for you.</p>`,
      `<h3 style="margin:16px 0 8px;">Freight-ready items</h3>`,
      itemsHtml(submission.items),
      `<h3 style="margin:16px 0 8px;">Accessorials</h3>`,
      tableHtml([
        ["Liftgate pickup", yn(submission.access.liftgate_pickup)],
        ["Liftgate delivery", yn(submission.access.liftgate_delivery)],
        ["Residential delivery", yn(submission.access.residential)],
      ]),
      `<h3 style="margin:16px 0 8px;">Access details</h3>`,
      tableHtml(accessRows(submission.access)),
      `<p style="margin:16px 0 0;font-size:14px;">FreightCenter: ${FREIGHTCENTER_PHONE}<br/>Lost &amp; Found: ${LOST_FOUND_PHONE}<br/>Email: ${LOST_FOUND_EMAIL}<br/>Request ID: ${escapeHtml(
        requestId
      )}</p>`,
      `<p style="margin:12px 0 0;font-size:12px;color:#777;">Freight quotes are typically valid about 24 hours. Delivery is usually curbside or dock unless inside services are quoted separately.</p>`,
      `</div>`,
    ].join("");
    customerText = [
      "Your Nationwide Freight Request Is Ready for Review",
      `Request ID: ${requestId}`,
      itemsText(submission.items),
      `Liftgate pickup: ${yn(submission.access.liftgate_pickup)}`,
      `Liftgate delivery: ${yn(submission.access.liftgate_delivery)}`,
      `Residential: ${yn(submission.access.residential)}`,
      "",
      `FreightCenter: ${FREIGHTCENTER_PHONE}`,
      `Lost & Found: ${LOST_FOUND_PHONE} · ${LOST_FOUND_EMAIL}`,
    ].join("\n");
  }

  return {
    internal: {
      subject,
      html: internalHtml,
      text: internalText,
      replyTo: submission.customer_email,
    },
    customer: {
      to: submission.customer_email,
      subject: customerSubject,
      html: customerHtml,
      text: customerText,
    },
  };
}

export async function sendFreightQuoteEmails(submission, ctx) {
  const built = buildFreightQuoteEmails(submission, ctx);
  await sendInternalNotification({
    subject: built.internal.subject,
    html: built.internal.html,
    text: built.internal.text,
    replyTo: built.internal.replyTo,
  });
  await sendEmail({
    to: built.customer.to,
    subject: built.customer.subject,
    html: built.customer.html,
    text: built.customer.text,
  });
  return built;
}
