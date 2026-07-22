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

function routeMapHtml(route = null, { caption = "Route from Scottsdale showroom" } = {}) {
  const img = route?.map_image_url;
  const link = route?.directions_url;
  if (!img && !link) return "";
  const parts = [
    `<div style="margin:14px 0 18px;">`,
    img
      ? `<a href="${escapeHtml(link || img)}" style="display:block;text-decoration:none;"><img src="${escapeHtml(
          img
        )}" alt="${escapeHtml(caption)}" width="640" style="display:block;width:100%;max-width:640px;height:auto;border:1px solid ${BORDER};border-radius:8px;" /></a>`
      : "",
    link
      ? `<p style="margin:8px 0 0;font-size:13px;"><a href="${escapeHtml(
          link
        )}" style="color:${NAVY};font-weight:700;">Open route in Google Maps →</a></p>`
      : "",
    `</div>`,
  ];
  return parts.join("");
}

function quickStatsLine({ price, oneWay, roundTrip, miles, isPickup }) {
  const bits = [];
  if (price) bits.push(price);
  if (oneWay != null) bits.push(`${oneWay} min one-way`);
  if (roundTrip != null) bits.push(`${roundTrip} min round-trip`);
  if (miles != null) bits.push(`${miles} mi`);
  if (isPickup) bits.push("pickup");
  return bits.join(" · ");
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

  const isLocal =
    submission.delivery_path === "local_az" || submission.delivery_path === "pickup_az";
  const isPickup = submission.delivery_path === "pickup_az";
  const modeLabel =
    submission.request_mode === "estimate" ? "Get an estimate" : "Have us quote it";
  const pathLabel = isPickup
    ? "Consignor pickup"
    : isLocal
      ? "Local Arizona delivery"
      : "Nationwide freight";
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

  const internalSubject = isPickup
    ? `Consignor Pickup Estimate — ${submission.customer_name} — ${submission.zip}`
    : isLocal
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
          [
            "Pricing",
            "$95.00/hour based on round-trip time (not one way); includes wrapping, preparing, loading, securing, offloading, and placement",
          ],
          isPickup ? ["Direction", "Round-trip pickup from showroom to consignor and back"] : null,
        ].filter(Boolean)
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
    : isPickup
      ? "Preliminary consignor pickup estimate"
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

  const includedWorkHtml = isPickup
    ? "Total time is calculated as a <strong>round trip</strong>, not one way. The estimate also covers preparing the freight, loading and securing it on the truck, return to the showroom, offloading, and placement."
    : "Total time is calculated as a <strong>round trip</strong>, not one way. The estimate also covers wrapping and preparing the freight, loading and securing it on the truck, offloading, and placement.";
  const includedWorkText = isPickup
    ? "Total time is calculated as a round trip, not one way. The estimate also covers preparing the freight, loading and securing it on the truck, return to the showroom, offloading, and placement."
    : "Total time is calculated as a round trip, not one way. The estimate also covers wrapping and preparing the freight, loading and securing it on the truck, offloading, and placement.";

  const driveExplainHtml =
    isLocal && oneWayMinutes != null
      ? `<p style="margin:0 0 10px;font-size:14px;color:#444;"><strong>${oneWayMinutes} min</strong> one way · <strong>${roundTripMinutes} min</strong> round trip${
          distanceMiles != null ? ` · <strong>${distanceMiles} mi</strong>` : ""
        }. ${includedWorkHtml}</p>`
      : isLocal
        ? `<p style="margin:0 0 10px;font-size:14px;color:#444;">${includedWorkHtml}</p>`
        : `<p style="margin:0 0 10px;font-size:14px;color:#444;">${
            nationwideMiles != null
              ? `About <strong>${nationwideMiles} miles</strong> from our Scottsdale showroom. `
              : ""
          }Preliminary range from distance, pallet size/weight, and access. Final price depends on carrier and service level.</p>`;

  const driveExplainText =
    isLocal && oneWayMinutes != null
      ? `${oneWayMinutes} min one way · ${roundTripMinutes} min round trip${
          distanceMiles != null ? ` · ${distanceMiles} mi` : ""
        }. ${includedWorkText}`
      : isLocal
        ? includedWorkText
        : `${
            nationwideMiles != null
              ? `About ${nationwideMiles} miles from our Scottsdale showroom. `
              : ""
          }Preliminary range from distance, pallet size/weight, and access. Final price depends on carrier and service level.`;

  const mapRoute =
    route?.map_image_url || route?.directions_url
      ? route
      : nationwideRate?.route?.map_image_url || nationwideRate?.route?.directions_url
        ? nationwideRate.route
        : null;

  const localPriceText =
    isLocal && localEstimate?.estimated_price != null
      ? money(localEstimate.estimated_price)
      : null;
  const nationwidePriceText = nationwideRangeText;
  const snapshotLine = quickStatsLine({
    price: localPriceText || nationwidePriceText,
    oneWay: oneWayMinutes,
    roundTrip: roundTripMinutes,
    miles: distanceMiles ?? (nationwideMiles != null ? Number(nationwideMiles) : null),
    isPickup,
  });

  const buyPolicyHtml = isPickup
    ? [
        `<div style="margin:22px 0;padding:16px 18px;background:${CREAM};border:1px solid ${BORDER};border-radius:8px;">`,
        `<p style="margin:0 0 8px;font-size:14px;font-weight:800;color:${NAVY};">Next steps for pickup</p>`,
        `<p style="margin:0;font-size:14px;line-height:1.6;color:#333;">Preliminary estimate only. We will confirm scheduling and final pricing. Reply here or contact <a href="mailto:${LOST_FOUND_EMAIL}" style="color:${NAVY};font-weight:700;">${LOST_FOUND_EMAIL}</a> / <a href="tel:${LOST_FOUND_PHONE.replace(
          /\D/g,
          ""
        )}" style="color:${NAVY};font-weight:700;">${LOST_FOUND_PHONE}</a>.</p>`,
        `</div>`,
      ].join("")
    : [
        `<div style="margin:22px 0;padding:16px 18px;background:${CREAM};border:1px solid ${BORDER};border-radius:8px;">`,
        `<p style="margin:0 0 8px;font-size:14px;font-weight:800;color:${NAVY};">Ready to purchase?</p>`,
        `<p style="margin:0;font-size:14px;line-height:1.6;color:#333;">We do <strong>not hold items</strong> — we work for our consignors. Ready to buy? Purchase now and we can confirm delivery/freight after. Want rates first? Email <a href="mailto:${LOST_FOUND_EMAIL}" style="color:${NAVY};font-weight:700;">${LOST_FOUND_EMAIL}</a>, call <a href="tel:${LOST_FOUND_PHONE.replace(
          /\D/g,
          ""
        )}" style="color:${NAVY};font-weight:700;">${LOST_FOUND_PHONE}</a>, or reply here.</p>`,
        `</div>`,
      ].join("");

  const buyPolicyText = isPickup
    ? [
        "Next steps for pickup",
        "Preliminary estimate only. We will confirm scheduling and final pricing. Reply to this email or contact info@lostandfoundresale.com / 480-588-7006.",
      ].join("\n")
    : [
        "Ready to purchase?",
        "We do not hold items — we work for our consignors. Ready to buy? Purchase now and we can confirm delivery/freight after. Want rates first? Email info@lostandfoundresale.com, call 480-588-7006, or reply here.",
      ].join("\n");

  const customerSubject = isPickup
    ? "Thanks — we received your consignor pickup estimate request"
    : isLocal
      ? "Thanks — we received your delivery estimate request"
      : "Thanks — we received your freight quote request";

  const internalSnapshotHtml = [
    `<div style="margin:0 0 20px;padding:14px 16px;background:${CREAM};border:1px solid ${BORDER};border-radius:8px;">`,
    `<p style="margin:0 0 6px;font-size:11px;font-weight:800;letter-spacing:0.06em;text-transform:uppercase;color:${MUTED};">Quick summary</p>`,
    `<p style="margin:0;font-size:16px;font-weight:800;color:${NAVY};">${escapeHtml(
      snapshotLine || pathLabel
    )}</p>`,
    `<p style="margin:6px 0 0;font-size:13px;color:#333;">${escapeHtml(addr)}</p>`,
    `<p style="margin:4px 0 0;font-size:13px;color:${MUTED};">${escapeHtml(
      submission.customer_name
    )} · ${escapeHtml(formatPhoneForEmail(submission.customer_phone))} · ${escapeHtml(
      submission.customer_email
    )}</p>`,
    `<p style="margin:6px 0 0;font-size:13px;color:#333;"><strong>Access:</strong> ${escapeHtml(
      flaggedAccessSummary(submission.access)
    )}</p>`,
    reviewReasons.length
      ? `<p style="margin:8px 0 0;font-size:13px;color:#9c2f2f;"><strong>Review:</strong> ${escapeHtml(
          reviewReasons.join("; ")
        )}</p>`
      : "",
    `</div>`,
    routeMapHtml(mapRoute, {
      caption: isPickup ? "Pickup route" : "Delivery route",
    }),
  ].join("");

  const internalHtml = wrapEmail(
    [
      `<h2 style="margin:0 0 6px;color:${NAVY};font-size:22px;">New ${escapeHtml(
        pathLabel
      )} request</h2>`,
      `<p style="margin:0 0 14px;font-size:13px;color:${MUTED};">Request ID <strong style="color:#222;">${escapeHtml(
        requestId
      )}</strong> · ${escapeHtml(submittedAt || "")}</p>`,

      internalSnapshotHtml,

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
        ["Delivery / pickup address", addr],
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
    "QUICK SUMMARY",
    snapshotLine || pathLabel,
    `Address: ${addr}`,
    `Customer: ${submission.customer_name} · ${formatPhoneForEmail(submission.customer_phone)} · ${submission.customer_email}`,
    `Access: ${flaggedAccessSummary(submission.access)}`,
    mapRoute?.directions_url ? `Map: ${mapRoute.directions_url}` : null,
    reviewReasons.length ? `Review: ${reviewReasons.join("; ")}` : null,
    "",
    "REQUEST",
    `Mode: ${modeLabel}`,
    `Path: ${pathLabel}`,
    ...estimateRows.map(([k, v]) => `${k}: ${v}`),
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
      `<p style="margin:0 0 10px;font-size:15px;color:#333;">${estimateLine}</p>`,
      driveExplainHtml,
      routeMapHtml(mapRoute, {
        caption: isPickup ? "Your pickup route" : "Your delivery route",
      }),
      `<p style="margin:0 0 14px;font-size:14px;color:#444;">Preliminary estimate only — not a booking. Access, labor, and scheduling can change the final amount.</p>`,

      (submission.items || []).length
        ? `<p style="margin:0 0 6px;font-size:13px;font-weight:800;color:${NAVY};text-transform:uppercase;letter-spacing:0.04em;">Your item(s)</p>${customerItemsHtml(
            submission.items
          )}<p style="margin:10px 0 0;font-size:13px;color:${MUTED};">${
            isPickup ? "Pickup" : "Delivery"
          }: ${escapeHtml(addr)}</p>`
        : "",

      buyPolicyHtml,

      isLocal
        ? ""
        : `<p style="margin:0 0 12px;font-size:14px;color:#444;">Final pricing depends on carrier and service level — it can be lower than this range; white-glove can push it higher. We will follow up after partner review.</p>
           <p style="margin:0 0 12px;font-size:14px;color:#444;"><strong>FreightCenter</strong>: <a href="tel:${FREIGHTCENTER_PHONE.replace(/\D/g, "")}" style="color:${NAVY};font-weight:700;">${FREIGHTCENTER_PHONE}</a> — you can get a quote from them immediately.</p>`,

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
    mapRoute?.directions_url ? `Route map: ${mapRoute.directions_url}` : null,
    "",
    "Preliminary estimate only — not a booking.",
    !isLocal
      ? "Final pricing depends on carrier and service level. FreightCenter: 800-716-7608 — you can get a quote from them immediately."
      : null,
    "",
    "Your item(s):",
    customerItemsText(submission.items || []),
    `Delivery / pickup: ${addr}`,
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
