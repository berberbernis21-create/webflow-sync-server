import { sendEmail, sendInternalNotification } from "../emailService.js";
import {
  FREIGHTCENTER_PHONE,
  LOCAL_AZ_HOURLY_RATE,
} from "./freightPalletize.js";

const FONT = "Arial,Helvetica,sans-serif";

function escapeHtml(str) {
  return String(str ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function yn(value) {
  if (value === true) return "Yes";
  if (value === false) return "No";
  return "Not answered";
}

function dimsLine(d) {
  if (!d) return "—";
  return `${d.widthIn}" W x ${d.depthIn}" D x ${d.heightIn}" H`;
}

function buildAccessRows(access = {}) {
  return [
    ["Delivery type", access.deliveryType === "commercial" ? "Commercial" : "Residential"],
    ["Residential delivery", yn(access.residentialDelivery)],
    ["Dock at destination", yn(access.hasDock)],
    ["Forklift at destination", yn(access.hasForklift)],
    ["Liftgate pickup", yn(access.needsLiftgatePickup)],
    ["Liftgate delivery", yn(access.needsLiftgateDelivery)],
    ["Freight elevator", yn(access.hasFreightElevator)],
    ["Stairs", yn(access.hasStairs)],
    ["Flights of stairs", access.hasStairs ? String(access.stairFlights || 0) : "N/A"],
    ["Needs more than 2 people", yn(access.needsMoreThanTwoPeople)],
    ["Tight turns / narrow halls", yn(access.hasTightTurns)],
    ["Inside delivery requested", yn(access.insideDeliveryRequested)],
    ["Unpacking requested", yn(access.unpackingRequested)],
  ];
}

function itemsHtml(palletized = []) {
  return palletized
    .map((row) => {
      const productDims = row.product
        ? dimsLine(row.product)
        : "Missing dimensions";
      const freightDims = row.freight ? dimsLine(row.freight) : "Cannot palletize yet";
      const freightWt = row.freight ? `${row.freight.weightLb} lb` : "—";
      const missing = row.missing?.length
        ? `<p style="margin:6px 0 0;color:#b42318;font-size:13px;">Missing: ${escapeHtml(
            row.missing.join(", ")
          )}</p>`
        : "";
      return [
        `<div style="margin:0 0 16px;padding:12px 14px;border:1px solid #e8e4dc;border-radius:6px;background:#fafaf8;">`,
        `<p style="margin:0 0 6px;font-size:15px;font-weight:700;color:#222;">${escapeHtml(
          row.index
        )}. ${escapeHtml(row.title)}</p>`,
        `<p style="margin:0;font-size:13px;color:#555;line-height:1.5;">`,
        `Source: ${escapeHtml(row.source || "manual")}<br/>`,
        row.price ? `Price: ${escapeHtml(row.price)}<br/>` : "",
        `Qty: ${escapeHtml(row.quantity)}<br/>`,
        `Product: ${escapeHtml(productDims)}${
          row.product?.weightLb != null ? ` · ${row.product.weightLb} lb` : ""
        }<br/>`,
        `Freight entry: ${escapeHtml(freightDims)} · ${escapeHtml(freightWt)}<br/>`,
        `Class: ${escapeHtml(row.freightClass)} · Non-stackable: ${yn(row.nonStackable)}`,
        row.productUrl
          ? `<br/><a href="${escapeHtml(row.productUrl)}">${escapeHtml(row.productUrl)}</a>`
          : "",
        `</p>`,
        missing,
        `</div>`,
      ].join("");
    })
    .join("");
}

function itemsText(palletized = []) {
  const lines = [];
  for (const row of palletized) {
    lines.push(
      "",
      `--- Item ${row.index}: ${row.title} ---`,
      `Source: ${row.source || "manual"}`,
      row.price ? `Price: ${row.price}` : null,
      `Qty: ${row.quantity}`,
      `Product: ${row.product ? dimsLine(row.product) : "missing"} ${
        row.product?.weightLb != null ? `· ${row.product.weightLb} lb` : ""
      }`,
      `Freight: ${row.freight ? dimsLine(row.freight) : "n/a"} · ${
        row.freight ? `${row.freight.weightLb} lb` : ""
      }`,
      `Class: ${row.freightClass} · Non-stackable: ${yn(row.nonStackable)}`,
      row.missing?.length ? `Missing: ${row.missing.join(", ")}` : null,
      row.productUrl ? `URL: ${row.productUrl}` : null
    );
  }
  return lines.filter((l) => l != null).join("\n");
}

function tableHtml(rows) {
  return [
    `<table cellpadding="0" cellspacing="0" style="width:100%;border-collapse:collapse;font-size:14px;">`,
    ...rows.map(
      ([k, v]) =>
        `<tr><td style="padding:6px 10px 6px 0;color:#666;vertical-align:top;width:42%;">${escapeHtml(
          k
        )}</td><td style="padding:6px 0;color:#222;vertical-align:top;">${escapeHtml(
          v
        )}</td></tr>`
    ),
    `</table>`,
  ].join("");
}

export function buildFreightQuoteEmails(submission, { submittedAt } = {}) {
  const modeLabel =
    submission.mode === "estimate" ? "Self-serve estimate request" : "Please quote for me";
  const pathLabel = submission.isLocalAz
    ? `Local Arizona delivery (~$${LOCAL_AZ_HOURLY_RATE}/hr)`
    : "Nationwide LTL freight (FreightCenter)";

  const address = `${submission.street}, ${submission.city}, ${submission.state} ${submission.zip}`;
  const accessRows = buildAccessRows(submission.access);
  const incomplete = (submission.palletized || []).filter((r) => !r.ok);

  const summaryRows = [
    ["Submitted", submittedAt || ""],
    ["Request type", modeLabel],
    ["Delivery path", pathLabel],
    ["Customer", submission.customerName],
    ["Email", submission.customerEmail],
    ["Phone", submission.customerPhone || "Not provided"],
    ["Delivery address", address],
    ["Items", String(submission.palletized?.length || 0)],
  ];

  const estimateNote = submission.isLocalAz
    ? `Local AZ estimate: third-party movers at $${LOCAL_AZ_HOURLY_RATE}/hr (same rate regardless of piece count). Final hours confirmed by our team.`
    : `Nationwide freight: we prepare palletized LTL details per our FreightCenter SOP (48x40 pallet when it fits, height +5", weight +30 lb, liftgate/residential as applicable). Confirmed $ and Shipment ID come from FreightCenter — quotes are typically valid ~24 hours. FreightCenter: ${FREIGHTCENTER_PHONE}.`;

  const disclaimer =
    "Estimates are not a booking. Lost & Found provides quote details; for nationwide freight the customer confirms/pays/coordinates with FreightCenter or the carrier. Curbside/dock unless inside delivery is quoted separately.";

  const internalSubject = `[Freight] ${submission.customerName} — ${
    submission.isLocalAz ? "AZ local" : "Nationwide"
  } — ${submission.palletized?.length || 0} item(s)`;

  const internalHtml = [
    `<div style="font-family:${FONT};line-height:1.5;color:#222;max-width:720px;">`,
    `<h2 style="margin:0 0 8px;color:#1a1a1a;">Freight / delivery quote request</h2>`,
    `<p style="margin:0 0 16px;font-size:14px;color:#555;">${escapeHtml(modeLabel)} · ${escapeHtml(
      pathLabel
    )}</p>`,
    incomplete.length
      ? `<p style="margin:0 0 16px;padding:10px 12px;background:#fff4ed;border-left:3px solid #e04f16;font-size:13px;color:#7a2e0e;"><strong>${incomplete.length} item(s) missing dims/weight</strong> — confirm before running FreightCenter.</p>`
      : "",
    `<h3 style="margin:0 0 8px;">Customer &amp; address</h3>`,
    tableHtml(summaryRows),
    submission.deliveryNotes
      ? `<p style="margin:12px 0 0;font-size:14px;"><strong>Notes:</strong> ${escapeHtml(
          submission.deliveryNotes
        )}</p>`
      : "",
    `<h3 style="margin:24px 0 8px;">Access / accessorials</h3>`,
    tableHtml(accessRows),
    `<h3 style="margin:24px 0 8px;">Items (product → freight entry)</h3>`,
    itemsHtml(submission.palletized),
    `<p style="margin:20px 0 0;font-size:13px;color:#555;">${escapeHtml(estimateNote)}</p>`,
    `<p style="margin:12px 0 0;font-size:12px;color:#777;">${escapeHtml(disclaimer)}</p>`,
    `</div>`,
  ].join("");

  const internalText = [
    "Freight / delivery quote request",
    modeLabel,
    pathLabel,
    "",
    ...summaryRows.map(([k, v]) => `${k}: ${v}`),
    submission.deliveryNotes ? `Notes: ${submission.deliveryNotes}` : null,
    "",
    "Access / accessorials:",
    ...accessRows.map(([k, v]) => `  ${k}: ${v}`),
    itemsText(submission.palletized),
    "",
    estimateNote,
    disclaimer,
  ]
    .filter((l) => l != null)
    .join("\n");

  const customerSubject =
    submission.mode === "please_quote"
      ? "We received your freight / delivery quote request — Lost & Found Resale"
      : "Your delivery / freight estimate request — Lost & Found Resale";

  const customerHtml = [
    `<div style="font-family:${FONT};line-height:1.55;color:#222;max-width:640px;">`,
    `<h2 style="margin:0 0 12px;">Thanks, ${escapeHtml(submission.customerName)}</h2>`,
    `<p style="margin:0 0 14px;font-size:15px;">We received your ${
      submission.mode === "please_quote"
        ? "request for our team to prepare a freight / delivery quote"
        : "delivery / freight estimate request"
    }. Here is a summary of what you submitted.</p>`,
    `<h3 style="margin:20px 0 8px;font-size:16px;">Delivery</h3>`,
    tableHtml([
      ["Address", address],
      ["Path", pathLabel],
      ["Request", modeLabel],
    ]),
    `<h3 style="margin:20px 0 8px;font-size:16px;">Access details</h3>`,
    tableHtml(accessRows),
    `<h3 style="margin:20px 0 8px;font-size:16px;">Items</h3>`,
    itemsHtml(submission.palletized),
    `<p style="margin:18px 0 0;font-size:14px;">${escapeHtml(estimateNote)}</p>`,
    submission.mode === "please_quote"
      ? `<p style="margin:12px 0 0;font-size:14px;">Our team will review this and follow up at <strong>${escapeHtml(
          submission.customerEmail
        )}</strong>. For nationwide freight, once we have a FreightCenter quote we will send the amount and Shipment ID — you confirm and book directly with FreightCenter (${FREIGHTCENTER_PHONE}).</p>`
      : `<p style="margin:12px 0 0;font-size:14px;">Reply to this email or call us if you want us to run a confirmed FreightCenter quote for you.</p>`,
    `<p style="margin:16px 0 0;font-size:12px;color:#777;">${escapeHtml(disclaimer)}</p>`,
    `<p style="margin:16px 0 0;font-size:13px;color:#555;">Lost &amp; Found Resale · Scottsdale<br/>lostandfoundresale.com</p>`,
    `</div>`,
  ].join("");

  const customerText = [
    `Thanks, ${submission.customerName}`,
    "",
    "We received your freight / delivery request. Summary:",
    `Address: ${address}`,
    `Path: ${pathLabel}`,
    `Request: ${modeLabel}`,
    "",
    "Access:",
    ...accessRows.map(([k, v]) => `  ${k}: ${v}`),
    itemsText(submission.palletized),
    "",
    estimateNote,
    disclaimer,
    "",
    "Lost & Found Resale — lostandfoundresale.com",
  ].join("\n");

  return {
    internal: {
      subject: internalSubject,
      html: internalHtml,
      text: internalText,
      replyTo: submission.customerEmail,
    },
    customer: {
      to: submission.customerEmail,
      subject: customerSubject,
      html: customerHtml,
      text: customerText,
    },
  };
}

export async function sendFreightQuoteEmails(submission, { submittedAt } = {}) {
  const built = buildFreightQuoteEmails(submission, { submittedAt });
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
