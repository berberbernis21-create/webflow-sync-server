import { sendEmail, sendInternalNotification } from "../emailService.js";
import {
  FREIGHTCENTER_PHONE,
  LOST_FOUND_EMAIL,
  LOST_FOUND_PHONE,
} from "./freightPalletize.js";

const FONT = "Arial,Helvetica,sans-serif";
const NAVY = "#07127c";
const MUTED = "#5c5c5c";
const BORDER = "#e5e1d8";
const CREAM = "#fbfaf6";

function escapeHtml(str) {
  return String(str ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function yn(v) {
  if (v === true) return "Yes";
  if (v === false) return "No";
  return "—";
}

function dims(w, d, h) {
  if (w == null || d == null || h == null) return "—";
  return `${w}" W × ${d}" D × ${h}" H`;
}

function money(n) {
  const num = Number(n);
  if (!Number.isFinite(num)) return String(n ?? "—");
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(num);
}

/** First name for greeting — skip junk / placeholder tokens like "Email". */
function customerGreetingName(fullName) {
  const first = String(fullName || "")
    .trim()
    .split(/\s+/)
    .filter(Boolean)[0];
  if (!first || first.length < 2) return null;
  if (/^(email|test|customer|user|name|n\/a|na|none|null|undefined)$/i.test(first)) {
    return null;
  }
  return first;
}

function classLabel(pallet) {
  if (!pallet) return "—";
  if (pallet.freight_class == null) {
    return `To be confirmed${
      pallet.suggested_freight_class ? ` (hint ${pallet.suggested_freight_class})` : ""
    }`;
  }
  return String(pallet.freight_class);
}

function sectionTitle(text) {
  return `<h3 style="margin:28px 0 10px;padding-bottom:6px;border-bottom:2px solid ${NAVY};color:${NAVY};font-size:15px;letter-spacing:0.02em;">${escapeHtml(
    text
  )}</h3>`;
}

function tableHtml(rows) {
  return [
    `<table cellpadding="0" cellspacing="0" style="width:100%;border-collapse:collapse;font-size:14px;">`,
    ...rows.map(([k, v], i) => {
      const bg = i % 2 === 0 ? CREAM : "#ffffff";
      return `<tr style="background:${bg};"><td style="padding:8px 12px;color:${MUTED};vertical-align:top;width:38%;border-bottom:1px solid ${BORDER};">${escapeHtml(
        k
      )}</td><td style="padding:8px 12px;color:#222;vertical-align:top;border-bottom:1px solid ${BORDER};font-weight:600;">${escapeHtml(
        String(v ?? "—")
      )}</td></tr>`;
    }),
    `</table>`,
  ].join("");
}

function formatPhoneForEmail(raw) {
  const digits = String(raw || "").replace(/\D/g, "");
  if (digits.length === 11 && digits.startsWith("1")) {
    return `(${digits.slice(1, 4)}) ${digits.slice(4, 7)}-${digits.slice(7)}`;
  }
  if (digits.length === 10) {
    return `(${digits.slice(0, 3)}) ${digits.slice(3, 6)}-${digits.slice(6)}`;
  }
  return String(raw || "").trim() || "—";
}

function accessRows(access = {}) {
  return [
    ["Residential", yn(access.residential)],
    ["Commercial", yn(access.commercial)],
    ["Dock at destination", yn(access.dock)],
    ["Forklift at destination", yn(access.forklift)],
    ["Freight elevator", yn(access.freight_elevator)],
    ["Elevator notes", access.freight_elevator_notes || "—"],
    ["Stairs", yn(access.stairs)],
    ["Flights of stairs", access.stairs ? String(access.stair_flights ?? 0) : "N/A"],
    ["Stair notes", access.stair_notes || "—"],
    ["Needs more than 2 people", yn(access.needs_more_than_two_people)],
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
    ["Customer access notes", access.notes || "—"],
  ];
}

function flaggedAccessSummary(access = {}) {
  const flags = [];
  if (access.stairs) {
    const parts = [`${access.stair_flights || "?"} flight(s) of stairs`];
    if (access.stair_notes) parts.push(access.stair_notes);
    flags.push(parts.join(" — "));
  }
  if (access.needs_more_than_two_people) flags.push("more than 2 movers");
  if (access.freight_elevator) flags.push("freight elevator");
  if (access.tight_turns_or_narrow_halls) flags.push("tight turns / narrow halls");
  if (access.gated_access) flags.push("gated access");
  if (access.inside_delivery) flags.push("inside delivery");
  if (access.room_placement) flags.push("room placement");
  if (access.unpacking_or_debris_removal) flags.push("unpacking / debris");
  if (access.disassembly_or_assembly) flags.push("assembly / disassembly");
  if (access.long_carry) flags.push("long carry");
  if (access.parking_or_time_restrictions) flags.push("parking / time limits");
  if (access.fragile_or_special_handling) flags.push("fragile / special handling");
  if (access.dock) flags.push("dock confirmed");
  if (access.forklift) flags.push("forklift confirmed");
  return flags.length ? flags.join(" · ") : "No special access flags";
}

function internalItemsHtml(items = []) {
  return items
    .map((row) => {
      const p = row.pallet;
      const setNote =
        row.set_count > 1
          ? `Set of ${row.set_count} · dims per piece · weight is TOTAL for set`
          : null;
      return [
        `<div style="margin:0 0 16px;padding:14px 16px;border:1px solid ${BORDER};border-radius:8px;background:#fff;">`,
        `<p style="margin:0 0 8px;font-size:15px;font-weight:800;color:${NAVY};">${escapeHtml(
          row.index
        )}. ${escapeHtml(row.title)}</p>`,
        `<p style="margin:0;font-size:13px;line-height:1.6;color:#333;">`,
        `<strong>Source:</strong> ${escapeHtml(row.source || "—")} &nbsp;|&nbsp; <strong>Listing qty:</strong> ${escapeHtml(
          row.quantity
        )}`,
        setNote ? `<br/><strong>Set:</strong> ${escapeHtml(setNote)}` : "",
        row.price ? `<br/><strong>Listed price:</strong> ${escapeHtml(money(row.price))}` : "",
        `<br/><strong>Product dims:</strong> ${escapeHtml(dims(row.width, row.depth, row.height))}`,
        row.weight != null ? ` &nbsp;·&nbsp; <strong>Weight:</strong> ${escapeHtml(row.weight)} lb` : "",
        p
          ? [
              `<br/><strong>Freight entry (SOP):</strong> ${escapeHtml(
                dims(p.width, p.depth, p.height)
              )} · ${escapeHtml(p.weight)} lb`,
              `<br/><strong>Class:</strong> ${escapeHtml(classLabel(p))} &nbsp;|&nbsp; <strong>Non-stackable:</strong> ${yn(
                p.non_stackable
              )}`,
              p.set_count > 1
                ? `<br/><strong>Packing:</strong> ${escapeHtml(p.packing_mode || "")} · ${escapeHtml(
                    p.pieces_per_layer
                  )}/layer × ${escapeHtml(p.layers)} layer(s) · stacked ~${escapeHtml(
                    p.stacked_height_in
                  )}"`
                : "",
              Array.isArray(p.packing_notes) && p.packing_notes.length
                ? `<br/><span style="color:${MUTED};">${escapeHtml(p.packing_notes.join(" "))}</span>`
                : "",
            ].join("")
          : `<br/><span style="color:#9c2f2f;"><strong>Incomplete</strong> — missing dims/weight for palletize</span>`,
        row.product_url
          ? `<br/><a href="${escapeHtml(row.product_url)}" style="color:${NAVY};">${escapeHtml(
              row.product_url
            )}</a>`
          : "",
        `</p></div>`,
      ].join("");
    })
    .join("");
}

function customerItemsHtml(items = []) {
  return [
    `<ul style="margin:0;padding-left:18px;font-size:14px;line-height:1.55;color:#333;">`,
    ...items.map((row) => {
      const bit = [
        `<strong>${escapeHtml(row.title || `Item ${row.index}`)}</strong>`,
        row.set_count > 1 ? `(set of ${escapeHtml(row.set_count)})` : null,
        row.weight != null ? `· ${escapeHtml(row.weight)} lb` : null,
        row.price ? `· ${escapeHtml(money(row.price))}` : null,
      ]
        .filter(Boolean)
        .join(" ");
      const link = row.product_url
        ? `<br/><a href="${escapeHtml(row.product_url)}" style="color:${NAVY};font-weight:700;">Buy this item →</a>`
        : "";
      return `<li style="margin:0 0 12px;">${bit}${link}</li>`;
    }),
    `</ul>`,
  ].join("");
}

function customerItemsText(items = []) {
  return (items || [])
    .map((row) => {
      const bits = [
        `- ${row.title || `Item ${row.index}`}`,
        row.set_count > 1 ? `(set of ${row.set_count})` : null,
        row.weight != null ? `· ${row.weight} lb` : null,
        row.price ? `· ${money(row.price)}` : null,
      ]
        .filter(Boolean)
        .join(" ");
      return row.product_url ? `${bits}\n  Buy: ${row.product_url}` : bits;
    })
    .join("\n");
}

function itemsText(items = []) {
  const lines = [];
  for (const row of items) {
    const p = row.pallet;
    lines.push(
      "",
      `--- Item ${row.index}: ${row.title} ---`,
      `Source: ${row.source} · Listing qty: ${row.quantity}`,
      row.set_count > 1 ? `Set count: ${row.set_count} (dims per piece, weight total)` : null,
      row.price ? `Price: ${row.price}` : null,
      `Product: ${dims(row.width, row.depth, row.height)}${row.weight != null ? ` · ${row.weight} lb` : ""}`,
      p
        ? `Pallet: ${dims(p.width, p.depth, p.height)} · ${p.weight} lb · Class ${classLabel(p)} · Non-stackable: ${yn(p.non_stackable)}`
        : "Pallet: incomplete",
      p?.packing_notes?.length ? `Packing: ${p.packing_notes.join(" ")}` : null,
      row.product_url ? `URL: ${row.product_url}` : null
    );
  }
  return lines.filter((l) => l != null).join("\n");
}

function wrapEmail(inner) {
  return [
    `<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head><body style="margin:0;padding:0;">`,
    `<div style="font-family:${FONT};background:#f0eee8;padding:24px 12px;">`,
    `<div style="max-width:680px;margin:0 auto;background:#ffffff;border:1px solid ${BORDER};border-radius:10px;overflow:hidden;">`,
    `<div style="background:${NAVY};padding:18px 22px;">`,
    `<p style="margin:0;color:#ffffff;font-size:11px;font-weight:800;letter-spacing:0.08em;text-transform:uppercase;">Lost &amp; Found Resale</p>`,
    `<p style="margin:4px 0 0;color:#c9d0ff;font-size:13px;">Delivery &amp; Freight</p>`,
    `</div>`,
    `<div style="padding:22px 24px 28px;color:#222;line-height:1.55;">`,
    inner,
    `</div>`,
    `</div></div>`,
    `</body></html>`,
  ].join("");
}

export function buildFreightQuoteEmails(submission, ctx = {}) {
  const {
    requestId,
    submittedAt,
    route = null,
    localEstimate = null,
    nationwideRate = null,
    reviewReasons = [],
  } = ctx;

  const isLocal = submission.delivery_path === "local_az";
  const modeLabel =
    submission.request_mode === "estimate" ? "Get an estimate" : "Have us quote it";
  const pathLabel = isLocal ? "Local Arizona delivery" : "Nationwide freight";
  const addr =
    submission.delivery_address?.full ||
    [submission.street, submission.unit, submission.city, submission.state, submission.zip]
      .filter(Boolean)
      .join(", ");
  const greetingName = customerGreetingName(submission.customer_name);
  const greetingHtml = greetingName
    ? `Thank you, ${escapeHtml(greetingName)}`
    : "Thank you for your request";
  const greetingText = greetingName
    ? `Thank you, ${greetingName}`
    : "Thank you for your request";

  const internalSubject = isLocal
    ? `Local Delivery Estimate — ${submission.customer_name} — ${submission.zip}`
    : `Nationwide Freight Quote — ${submission.customer_name} — ${submission.zip}`;

  const estimateRows =
    isLocal && localEstimate?.estimated_price != null
      ? [
          ["Preliminary route estimate", money(localEstimate.estimated_price)],
          [
            "One-way drive time",
            `${route?.drive_minutes ?? localEstimate.drive_minutes} minutes`,
          ],
          [
            "Round-trip drive time",
            `${
              Number(route?.drive_minutes ?? localEstimate.drive_minutes) * 2
            } minutes`,
          ],
          [
            "Distance",
            route?.distance_miles != null ? `${route.distance_miles} miles` : "—",
          ],
          ["Pricing formula", "$95 through 17 min, then +$15/8 per extra minute, round up to $5"],
        ]
      : isLocal
        ? [["Preliminary route estimate", "Pending — route time unavailable / manual review"]]
        : [
            [
              "Preliminary freight range",
              nationwideRate?.range_low != null && nationwideRate?.range_high != null
                ? `${money(nationwideRate.range_low)} – ${money(nationwideRate.range_high)}`
                : "Pending partner quote",
            ],
            [
              "Distance (showroom → destination)",
              nationwideRate?.distance_miles != null || route?.distance_miles != null
                ? `${nationwideRate?.distance_miles ?? route.distance_miles} miles`
                : "—",
            ],
            ["Floor", "Never below $350 for nationwide freight"],
            ["Carrier price", "Confirm with FreightCenter / partners — do not treat range as booked"],
            ["Shipment ID", "Provide after partner quote"],
          ];

  const oneWayMinutes =
    isLocal && Number.isFinite(Number(route?.drive_minutes ?? localEstimate?.drive_minutes))
      ? Math.ceil(Number(route?.drive_minutes ?? localEstimate.drive_minutes))
      : null;
  const roundTripMinutes = oneWayMinutes != null ? oneWayMinutes * 2 : null;
  const distanceMiles =
    route?.distance_miles != null && Number.isFinite(Number(route.distance_miles))
      ? Number(route.distance_miles)
      : null;
  const extraCrew = Boolean(submission.access?.needs_more_than_two_people);
  const estimateAmountLabel = extraCrew
    ? "Approximate two-person base"
    : "Preliminary local delivery estimate";

  const nationwideRangeText =
    !isLocal && nationwideRate?.range_low != null && nationwideRate?.range_high != null
      ? `${money(nationwideRate.range_low)} – ${money(nationwideRate.range_high)}`
      : null;
  const nationwideMiles =
    nationwideRate?.distance_miles ??
    nationwideRate?.route?.distance_miles ??
    route?.distance_miles ??
    null;

  const estimateLine =
    isLocal && localEstimate?.estimated_price != null
      ? `Your <strong>${escapeHtml(estimateAmountLabel)}</strong> is <strong>${escapeHtml(
          money(localEstimate.estimated_price)
        )}</strong>.`
      : isLocal
        ? `We received your local delivery request and will confirm timing and pricing with you shortly.`
        : nationwideRangeText
          ? `Your <strong>preliminary nationwide freight range</strong> is <strong>${escapeHtml(
              nationwideRangeText
            )}</strong>.`
          : `We received your nationwide freight request and will review the freight-ready details on our side.`;

  const driveExplainHtml =
    isLocal && oneWayMinutes != null
      ? `<p style="margin:0 0 14px;font-size:14px;color:#444;">This estimate considers <strong>${oneWayMinutes} minutes one way (${roundTripMinutes} minutes round trip)</strong>${
          distanceMiles != null ? ` · about <strong>${distanceMiles} miles</strong>` : ""
        } from our Scottsdale showroom to your location and back, wrapping and securing for transport, loading the item, unloading, and the expected time and labor.</p>`
      : isLocal
        ? `<p style="margin:0 0 14px;font-size:14px;color:#444;">This estimate considers the drive from our Scottsdale showroom to your location and back, wrapping and securing for transport, loading the item, unloading, and the expected time and labor.</p>`
        : `<p style="margin:0 0 14px;font-size:14px;color:#444;">${
            nationwideMiles != null
              ? `We measured about <strong>${nationwideMiles} miles</strong> from our Scottsdale showroom to your delivery address. `
              : "Distance will be confirmed when we review your request. "
          }This preliminary range is based on measured distance, pallet dimensions and weight, and the access details you provided. <strong>Final pricing depends on the type of carrier and service level</strong> — rates can come in lower than this range, and white-glove or specialty handling can push the total higher.</p>`;

  const driveExplainText =
    isLocal && oneWayMinutes != null
      ? `This estimate considers ${oneWayMinutes} minutes one way (${roundTripMinutes} minutes round trip)${
          distanceMiles != null ? ` · about ${distanceMiles} miles` : ""
        } from our Scottsdale showroom to your location and back, wrapping and securing for transport, loading the item, unloading, and the expected time and labor.`
      : isLocal
        ? "This estimate considers the drive from our Scottsdale showroom to your location and back, wrapping and securing for transport, loading the item, unloading, and the expected time and labor."
        : `${
            nationwideMiles != null
              ? `We measured about ${nationwideMiles} miles from our Scottsdale showroom to your delivery address. `
              : "Distance will be confirmed when we review your request. "
          }This preliminary range is based on measured distance, pallet dimensions and weight, and the access details you provided. Final pricing depends on the type of carrier and service level — rates can come in lower than this range, and white-glove or specialty handling can push the total higher.`;

  const buyPolicyHtml = [
    `<div style="margin:22px 0;padding:16px 18px;background:${CREAM};border:1px solid ${BORDER};border-radius:8px;">`,
    `<p style="margin:0 0 8px;font-size:14px;font-weight:800;color:${NAVY};">Ready to purchase?</p>`,
    `<p style="margin:0;font-size:14px;line-height:1.6;color:#333;">We do <strong>not hold items</strong> — we work for our consignors. If you are ready to buy, go ahead and purchase the item(s) and we can confirm delivery or freight rates afterward. If you would like us to confirm rates <em>before</em> you buy, just email us at <a href="mailto:${LOST_FOUND_EMAIL}" style="color:${NAVY};font-weight:700;">${LOST_FOUND_EMAIL}</a> or call <a href="tel:${LOST_FOUND_PHONE.replace(
      /\D/g,
      ""
    )}" style="color:${NAVY};font-weight:700;">${LOST_FOUND_PHONE}</a> — or reply to this email.</p>`,
    `</div>`,
  ].join("");

  const buyPolicyText = [
    "Ready to purchase?",
    "We do not hold items — we work for our consignors. If you are ready to buy, go ahead and purchase the item(s) and we can confirm delivery or freight rates afterward. If you would like us to confirm rates before you buy, email info@lostandfoundresale.com, call 480-588-7006, or reply to this email.",
  ].join("\n");

  const customerSubject = isLocal
    ? "Thanks — we received your delivery estimate request"
    : "Thanks — we received your freight quote request";

  const internalHtml = wrapEmail(
    [
      `<h2 style="margin:0 0 6px;color:${NAVY};font-size:22px;">New ${escapeHtml(
        pathLabel
      )} request</h2>`,
      `<p style="margin:0 0 18px;font-size:13px;color:${MUTED};">Request ID <strong style="color:#222;">${escapeHtml(
        requestId
      )}</strong> · ${escapeHtml(submittedAt || "")}</p>`,

      reviewReasons.length
        ? `<div style="margin:0 0 18px;padding:12px 14px;background:#fff8e8;border-left:4px solid #c9a227;font-size:13px;"><strong>Needs review:</strong> ${escapeHtml(
            reviewReasons.join("; ")
          )}</div>`
        : "",

      sectionTitle("1. Customer"),
      tableHtml([
        ["Name", submission.customer_name],
        ["Email", submission.customer_email],
        ["Phone", formatPhoneForEmail(submission.customer_phone)],
        ["Reply-To", "This email — reply goes to the customer"],
      ]),

      sectionTitle("2. Request"),
      tableHtml([
        ["Mode", modeLabel],
        ["Path", pathLabel],
        ["Destination type", submission.destination_type || "—"],
        ["Origin (showroom)", submission.origin_address || "—"],
        ["Delivery address", addr],
        ["Unit / suite", submission.unit || submission.delivery_address?.unit || "—"],
        ["Page URL", submission.page_url || "—"],
        ...estimateRows,
        ["Access flags summary", flaggedAccessSummary(submission.access)],
        ["Multi-item note", submission.multi_item_note || "—"],
      ]),

      sectionTitle("3. Access & handling (every answer)"),
      tableHtml(accessRows(submission.access)),

      sectionTitle("4. Items & SOP freight entries"),
      internalItemsHtml(submission.items || []),

      `<p style="margin:22px 0 0;font-size:12px;color:${MUTED};line-height:1.5;">Lost &amp; Found prepares the quote details only. For nationwide freight, confirm pricing and Shipment ID with FreightCenter (${FREIGHTCENTER_PHONE}); the customer books and pays the carrier. Do not send login credentials in any reply.</p>`,
    ].join("")
  );

  const internalText = [
    internalSubject,
    `Request ID: ${requestId}`,
    `Submitted: ${submittedAt}`,
    "",
    "CUSTOMER",
    `Name: ${submission.customer_name}`,
    `Email: ${submission.customer_email}`,
    `Phone: ${formatPhoneForEmail(submission.customer_phone)}`,
    "",
    "REQUEST",
    `Mode: ${modeLabel}`,
    `Path: ${pathLabel}`,
    `Address: ${addr}`,
    ...estimateRows.map(([k, v]) => `${k}: ${v}`),
    reviewReasons.length ? `Review: ${reviewReasons.join("; ")}` : null,
    "",
    "ACCESS",
    ...accessRows(submission.access).map(([k, v]) => `${k}: ${v}`),
    itemsText(submission.items),
    submission.page_url ? `Page: ${submission.page_url}` : null,
  ]
    .filter((l) => l != null)
    .join("\n");

  const customerHtml = wrapEmail(
    [
      `<h2 style="margin:0 0 12px;color:${NAVY};font-size:22px;">${greetingHtml}</h2>`,
      `<p style="margin:0 0 14px;font-size:15px;color:#333;">${estimateLine}</p>`,
      driveExplainHtml,
      `<p style="margin:0 0 14px;font-size:14px;color:#444;">This is a <strong>preliminary estimate only</strong> — not a booking and not a final quote. Access, labor, item handling, and scheduling can change the final amount.</p>`,

      (submission.items || []).length
        ? `<p style="margin:0 0 6px;font-size:13px;font-weight:800;color:${NAVY};text-transform:uppercase;letter-spacing:0.04em;">Your item(s)</p>${customerItemsHtml(
            submission.items
          )}<p style="margin:10px 0 0;font-size:13px;color:${MUTED};">Delivery: ${escapeHtml(addr)}</p>`
        : "",

      buyPolicyHtml,

      isLocal
        ? ""
        : `<p style="margin:0 0 12px;font-size:14px;color:#444;"><strong>This is an estimate only.</strong> Final pricing depends on the carrier and service level — it can be less than this range, and white-glove service can push it higher. We will follow up with better rates after we review your request and confirm an exact quote with our freight partners. You can also reach FreightCenter at ${FREIGHTCENTER_PHONE} once we provide a Shipment ID.</p>`,

      `<p style="margin:18px 0 0;font-size:12px;color:#999;">Reference: ${escapeHtml(
        requestId
      )} · Lost &amp; Found Resale · Scottsdale · lostandfoundresale.com</p>`,
    ].join("")
  );

  const customerText = [
    greetingText,
    "",
    isLocal && localEstimate?.estimated_price != null
      ? `Your ${estimateAmountLabel} is ${money(localEstimate.estimated_price)}.`
      : isLocal
        ? "We received your local delivery request and will confirm timing and pricing shortly."
        : nationwideRangeText
          ? `Your preliminary nationwide freight range is ${nationwideRangeText}.`
          : "We received your nationwide freight request and will review it on our side.",
    driveExplainText,
    "",
    "This is a preliminary estimate only — not a booking and not a final quote.",
    !isLocal
      ? "This is an estimate only. Final pricing depends on the carrier and service level — it can be less than this range, and white-glove service can push it higher. We will follow up with better rates after we review your request and confirm an exact quote with our freight partners."
      : null,
    "",
    "Your item(s):",
    customerItemsText(submission.items || []),
    `Delivery: ${addr}`,
    "",
    buyPolicyText,
    "",
    `Reference: ${requestId}`,
    "Lost & Found Resale · Scottsdale",
  ]
    .filter((l) => l != null && l !== "")
    .join("\n");

  return {
    internal: {
      subject: internalSubject,
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
  const internal = await sendInternalNotification({
    subject: built.internal.subject,
    html: built.internal.html,
    text: built.internal.text,
    replyTo: built.internal.replyTo,
  });
  const customer = await sendEmail({
    to: built.customer.to,
    subject: built.customer.subject,
    html: built.customer.html,
    text: built.customer.text,
    replyTo: LOST_FOUND_EMAIL,
  });
  return {
    internal: {
      sent: true,
      to: internal.to,
      resend_id: internal.id,
    },
    customer: {
      sent: true,
      to: customer.to,
      resend_id: customer.id,
    },
  };
}
