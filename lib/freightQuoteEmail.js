import { sendEmail, sendInternalNotification, sendInternalNotificationWithAttachments } from "../emailService.js";
import {
  FREIGHTCENTER_PHONE,
  LOST_FOUND_EMAIL,
  LOST_FOUND_PHONE,
} from "./freightPalletize.js";
import {
  buildFreightQuotePdfFilename,
  generateFreightQuotePdf,
  nonAccessReviewReasons,
  accessHighlightRows,
} from "./freightQuotePdf.js";
import { isFreightPdfS3Configured, uploadFreightQuotePdfToS3 } from "./freightQuoteS3.js";

const FONT = "Arial,Helvetica,sans-serif";
const NAVY = "#07127c";
const MUTED = "#5c5c5c";
const BORDER = "#e5e1d8";
const CREAM = "#fbfaf6";
const INK = "#111111";

const PUBLIC_API_BASE = String(
  process.env.PUBLIC_API_BASE || "https://webflow-sync-server.onrender.com"
).replace(/\/$/, "");

const FREIGHTCENTER_LUKE_EMAIL = "lrogers@freightcenter.com";
const FREIGHTCENTER_SOP_PATH =
  "Dropbox → SHOPIFY → Freight Shipping → Lost_and_Found_FreightCenter_Quote_SOP.pdf";

function exceedsStandardPallet(item = {}) {
  const w = Number(item?.pallet?.width ?? item?.width);
  const d = Number(item?.pallet?.depth ?? item?.depth);
  if (!Number.isFinite(w) || !Number.isFinite(d) || w <= 0 || d <= 0) return false;
  return !((w <= 48 && d <= 40) || (w <= 40 && d <= 48));
}

function hasOversizedPalletItems(items = []) {
  return (items || []).some(exceedsStandardPallet);
}

/** Fits on 48×40 but does not fill a full standard pallet — smaller pallets may apply. */
function isSmallerThanStandardPallet(item = {}) {
  const w = Number(item?.pallet?.width ?? item?.width);
  const d = Number(item?.pallet?.depth ?? item?.depth);
  if (!Number.isFinite(w) || !Number.isFinite(d) || w <= 0 || d <= 0) return false;
  if (exceedsStandardPallet(item)) return false;
  const long = Math.max(w, d);
  const short = Math.min(w, d);
  return long < 48 || short < 40;
}

function hasSmallerThanStandardPalletItems(items = []) {
  return (items || []).some(isSmallerThanStandardPallet);
}

const BRAND = {
  logo:
    "https://cdn.prod.website-files.com/5e8d436ca3f96345b47da055/6a0c6dda2c7a7a5fbc3d5f09_Logo%20.png",
  seal: `${PUBLIC_API_BASE}/brand/seal.png`,
  site: "https://www.lostandfoundresale.com/",
  furniture: "https://www.lostandfoundresale.com/",
  handbags: "https://www.lostandfoundresale.com/",
};

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
  return "-";
}

function dims(w, d, h) {
  if (w == null || d == null || h == null) return "-";
  return `${w}" W × ${d}" D × ${h}" H`;
}

function money(n) {
  const num = Number(n);
  if (!Number.isFinite(num)) return String(n ?? "-");
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(num);
}

/** First name for greeting | skip junk / placeholder tokens like "Email". */
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
  if (!pallet) return "-";
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
        String(v ?? "-")
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
  return String(raw || "").trim() || "-";
}

function compactTableHtml(rows, { valueColor = "#222" } = {}) {
  const cleaned = (rows || []).filter((r) => r && r[1] != null && r[1] !== "" && r[1] !== "-");
  if (!cleaned.length) {
    return `<p style="margin:0;font-size:13px;color:${MUTED};">None noted</p>`;
  }
  return [
    `<table cellpadding="0" cellspacing="0" style="width:100%;border-collapse:collapse;font-size:13px;">`,
    ...cleaned.map(([k, v], i) => {
      const bg = i % 2 === 0 ? CREAM : "#ffffff";
      return `<tr style="background:${bg};"><td style="padding:6px 8px;color:${MUTED};vertical-align:top;width:42%;border-bottom:1px solid ${BORDER};">${escapeHtml(
        k
      )}</td><td style="padding:6px 8px;color:${valueColor};vertical-align:top;border-bottom:1px solid ${BORDER};font-weight:600;">${escapeHtml(
        String(v)
      )}</td></tr>`;
    }),
    `</table>`,
  ].join("");
}

function formatDestinationType(raw) {
  const s = String(raw || "").trim();
  if (!s || s === "-") return "-";
  return s.charAt(0).toUpperCase() + s.slice(1).toLowerCase();
}

function ynLabel(v) {
  if (v === true) return "Yes";
  if (v === false) return "No";
  return "—";
}

/** Stacking / pallet summary lines for Luke + internal FreightCenter quoting. */
function packingForLukeLines(items = []) {
  return (items || []).map((row, i) => {
    const p = row.pallet || {};
    const stackable = !(row.non_stackable || p.non_stackable);
    const setBit =
      row.set_count > 1 || p.set_count > 1
        ? `set of ${row.set_count || p.set_count}`
        : "single / as listed";
    const stackBit = stackable
      ? "STACKABLE for freight (pair/set can stack — not double height when nested)"
      : "NON-STACKABLE (do not stack other freight on top)";
    return [
      `${i + 1}. ${row.title || "Item"}`,
      `   Piece dims: ${dims(row.width, row.depth, row.height)} · ${row.weight != null ? `${row.weight} lb` : "weight TBD"} · ${setBit}`,
      `   Freight-ready pallet entry: ${dims(p.width, p.depth, p.height)} · ${p.weight != null ? `${p.weight} lb` : "—"} · class ${classLabel(p)}`,
      `   Typical footprint: 48"×40" pallet (or oversized as shown) · ${stackBit}`,
      p.stack_note ? `   Stack note: ${p.stack_note}` : null,
      row.product_url ? `   Listing: ${row.product_url}` : null,
    ]
      .filter(Boolean)
      .join("\n");
  });
}

function nationwideInternalPlaybookHtml(submission = {}) {
  const a = submission.access || {};
  const items = submission.items || [];
  const oversized = hasOversizedPalletItems(items);
  const smaller = !oversized && hasSmallerThanStandardPalletItems(items);
  const packingHtml = packingForLukeLines(items)
    .map((block) =>
      `<pre style="margin:0 0 10px;padding:10px 12px;background:#fff;border:1px solid ${BORDER};border-radius:6px;font-size:11px;line-height:1.45;white-space:pre-wrap;font-family:Consolas,Monaco,monospace;color:#222;">${escapeHtml(
        block
      )}</pre>`
    )
    .join("");
  const optionsSaid = [
    a.white_glove ? "White Glove" : null,
    a.warehouse_pickup ? "Warehouse / terminal pickup" : null,
    `Liftgate at delivery: ${ynLabel(a.liftgate_delivery)}`,
    "Liftgate at pickup: Lost & Found to confirm",
    a.stairs
      ? `Stairs: Yes${a.stair_flights ? ` (${a.stair_flights} flight(s))` : ""}${
          a.stair_notes ? ` — ${a.stair_notes}` : ""
        }`
      : null,
    a.dock ? "Loading dock or forklift at delivery" : null,
    a.notes ? `Notes: ${a.notes}` : null,
  ]
    .filter(Boolean)
    .join(" · ");

  const palletFitHtml = oversized
    ? `<p style="margin:0 0 12px;font-size:14px;line-height:1.55;color:#333;"><strong>Pallet fit:</strong> This is larger than a standard <strong>48″×40″</strong> pallet. We may be able to stand it up differently, rotate, stack, or otherwise maneuver it to fit a standard pallet (often much lower cost). Please confirm the best realistic packing before finalizing rates.</p>`
    : smaller
      ? `<p style="margin:0 0 12px;font-size:14px;line-height:1.55;color:#333;"><strong>Pallet fit:</strong> This looks smaller than a full <strong>48″×40″</strong> pallet. We may have a smaller pallet available — please confirm the best pallet size for the rate.</p>`
      : `<p style="margin:0 0 12px;font-size:14px;line-height:1.55;color:#333;"><strong>Pallet fit:</strong> Standard <strong>48″×40″</strong> footprint unless packing notes below say otherwise.</p>`;

  return [
    `<div style="margin:16px 0;padding:16px 18px;background:${CREAM};border:1px solid ${BORDER};border-radius:8px;">`,
    `<p style="margin:0 0 6px;font-size:13px;font-weight:800;color:${NAVY};">Nationwide freight — next steps</p>`,
    `<p style="margin:0 0 12px;font-size:13px;line-height:1.5;color:${MUTED};">Ready to forward to our Freight Center contact (and CC the customer if helpful).</p>`,
    `<ol style="margin:0 0 14px;padding-left:18px;font-size:14px;line-height:1.55;color:#222;">`,
    `<li style="margin:0 0 8px;">FreightCenter quote SOP is in <strong>Dropbox</strong> (Shopify → Freight Shipping).</li>`,
    `<li style="margin:0 0 8px;"><strong>Forward this email to our Freight Center contact</strong> at <a href="mailto:${FREIGHTCENTER_LUKE_EMAIL}" style="color:${NAVY};font-weight:700;">${FREIGHTCENTER_LUKE_EMAIL}</a> and ask for options: cheapest, best/quality, and warehouse pickup. Stick with their recommendation when quoting the customer.</li>`,
    `</ol>`,
    palletFitHtml,
    `<p style="margin:0 0 6px;font-size:12px;font-weight:800;color:${INK};">Packing details</p>`,
    packingHtml || `<p style="margin:0 0 10px;font-size:12px;color:${MUTED};">No items listed.</p>`,
    `<p style="margin:0;font-size:13px;line-height:1.5;color:#333;"><strong>Options / access:</strong> ${escapeHtml(
      optionsSaid || "Standard residential freight options."
    )}</p>`,
    `</div>`,
  ].join("");
}

function nationwideInternalPlaybookText(submission = {}) {
  const a = submission.access || {};
  const items = submission.items || [];
  const oversized = hasOversizedPalletItems(items);
  const smaller = !oversized && hasSmallerThanStandardPalletItems(items);
  const packing = packingForLukeLines(items).join("\n\n");
  const palletFit = oversized
    ? "Pallet fit: Larger than a standard 48x40. We may be able to stand it up differently, rotate, stack, or maneuver it to fit a standard pallet (often much lower cost). Please confirm best packing before finalizing rates."
    : smaller
      ? "Pallet fit: Smaller than a full 48x40. We may have a smaller pallet available — please confirm the best pallet size for the rate."
      : "Pallet fit: Standard 48x40 footprint unless packing notes say otherwise.";
  return [
    "NATIONWIDE FREIGHT — NEXT STEPS",
    "Ready to forward to our Freight Center contact (and CC the customer if helpful).",
    "1) FreightCenter quote SOP is in Dropbox (Shopify → Freight Shipping).",
    `2) Forward this email to our Freight Center contact <${FREIGHTCENTER_LUKE_EMAIL}> and ask for options: cheapest, best/quality, and warehouse pickup. Stick with their recommendation when quoting the customer.`,
    "",
    palletFit,
    "",
    "PACKING DETAILS",
    packing || "(no items)",
    "",
    "OPTIONS / ACCESS",
    [
      a.white_glove ? "White Glove" : null,
      a.warehouse_pickup ? "Warehouse / terminal pickup" : null,
      `Liftgate at delivery: ${ynLabel(a.liftgate_delivery)}`,
      "Liftgate at pickup: Lost & Found to confirm",
      a.notes ? `Notes: ${a.notes}` : null,
    ]
      .filter(Boolean)
      .join(" | ") || "Standard residential freight options.",
  ].join("\n");
}

function internalItemsHtml(items = []) {
  return items
    .map((row) => {
      const p = row.pallet;
      const title = escapeHtml(row.title || `Item ${row.index}`);
      const bits = [
        dims(row.width, row.depth, row.height),
        row.weight != null ? `${row.weight} lb` : null,
        row.price ? money(row.price) : null,
        p
          ? `Freight ${dims(p.width, p.depth, p.height)} | ${p.weight} lb | class ${classLabel(p)}${
              p.non_stackable ? " | non-stackable" : ""
            }`
          : "Freight incomplete",
      ]
        .filter(Boolean)
        .join(" | ");
      const url = row.product_url ? String(row.product_url) : "";
      const img = row.image_url ? String(row.image_url) : "";
      const imgBlock = img
        ? url
          ? `<a href="${escapeHtml(url)}" style="display:block;line-height:0;text-decoration:none;"><img src="${escapeHtml(
              img
            )}" alt="${title}" width="96" height="96" style="display:block;width:96px;height:96px;object-fit:cover;border-radius:6px;border:1px solid ${BORDER};background:#fff;" /></a>`
          : `<img src="${escapeHtml(
              img
            )}" alt="${title}" width="96" height="96" style="display:block;width:96px;height:96px;object-fit:cover;border-radius:6px;border:1px solid ${BORDER};background:#fff;" />`
        : "";
      return [
        `<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin:0 0 12px;background:#fff;border:1px solid ${BORDER};border-radius:8px;">`,
        `<tr>`,
        imgBlock
          ? `<td valign="top" width="108" style="padding:10px 0 10px 10px;">${imgBlock}</td>`
          : "",
        `<td valign="middle" style="padding:10px 12px;font-size:12px;line-height:1.45;color:#333;">`,
        `<p style="margin:0 0 4px;font-size:14px;font-weight:800;color:${NAVY};">${escapeHtml(
          row.index
        )}. ${title}</p>`,
        `<p style="margin:0;font-size:12px;line-height:1.45;color:#333;">${escapeHtml(bits)}</p>`,
        url
          ? `<p style="margin:6px 0 0;font-size:12px;"><a href="${escapeHtml(
              url
            )}" style="color:${NAVY};font-weight:700;">View listing</a></p>`
          : "",
        `</td></tr></table>`,
      ].join("");
    })
    .join("");
}

function buyNowCtaHtml(items = []) {
  const withUrl = (items || []).filter((row) => row.product_url);
  if (!withUrl.length) return "";
  if (withUrl.length === 1) {
    return [
      `<a href="${escapeHtml(withUrl[0].product_url)}" style="display:inline-block;min-width:132px;padding:14px 18px;background:${NAVY};color:#ffffff;text-decoration:none;font-weight:800;font-size:15px;letter-spacing:0.03em;border-radius:8px;text-align:center;">Buy Now</a>`,
      `<p style="margin:8px 0 0;font-size:11px;line-height:1.4;color:${MUTED};">Opens the product page</p>`,
    ].join("");
  }
  return withUrl
    .map(
      (row, i) =>
        `<a href="${escapeHtml(row.product_url)}" style="display:block;margin:${i ? "8px" : "0"} 0 0;padding:12px 14px;background:${NAVY};color:#ffffff;text-decoration:none;font-weight:800;font-size:13px;border-radius:8px;text-align:center;">Buy Now${
          withUrl.length > 1 ? ` · Item ${i + 1}` : ""
        }</a>`
    )
    .join("");
}

function customerItemsHtml(items = []) {
  return (items || [])
    .map((row) => {
      const title = escapeHtml(row.title || `Item ${row.index}`);
      const meta = [
        row.set_count > 1 ? `Set of ${escapeHtml(row.set_count)}` : null,
        row.weight != null ? `${escapeHtml(row.weight)} lb` : null,
        row.price ? money(row.price) : null,
      ]
        .filter(Boolean)
        .join(" · ");
      const url = row.product_url ? String(row.product_url) : "";
      const img = row.image_url ? String(row.image_url) : "";
      const imgBlock = img
        ? url
          ? `<a href="${escapeHtml(url)}" style="display:block;line-height:0;text-decoration:none;"><img src="${escapeHtml(
              img
            )}" alt="${title}" width="108" height="108" style="display:block;width:108px;height:108px;object-fit:cover;border-radius:8px;border:1px solid ${BORDER};background:#fff;" /></a>`
          : `<img src="${escapeHtml(
              img
            )}" alt="${title}" width="108" height="108" style="display:block;width:108px;height:108px;object-fit:cover;border-radius:8px;border:1px solid ${BORDER};background:#fff;" />`
        : "";
      const buyLabel = url
        ? img
          ? `<div style="margin:10px 0 0;line-height:1.4;"><a href="${escapeHtml(url)}" style="color:${NAVY};font-weight:700;font-size:13px;text-decoration:underline;">View &amp; buy on our site</a></div>`
          : `<div style="margin:10px 0 0;"><a href="${escapeHtml(url)}" style="display:inline-block;padding:8px 12px;border:1px solid ${NAVY};border-radius:6px;color:${NAVY};font-weight:700;font-size:13px;text-decoration:none;">View &amp; buy on our site</a></div>`
        : "";
      return [
        `<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin:0 0 14px;background:${CREAM};border:1px solid ${BORDER};border-radius:8px;">`,
        `<tr>`,
        imgBlock
          ? `<td valign="top" width="120" style="padding:12px 0 12px 12px;">${imgBlock}</td>`
          : "",
        `<td valign="middle" style="padding:12px 14px;font-size:14px;line-height:1.55;color:#333;">`,
        `<div style="margin:0 0 4px;"><strong style="color:${INK};font-size:15px;line-height:1.35;">${title}</strong></div>`,
        meta ? `<div style="margin:0;color:${MUTED};font-size:13px;line-height:1.4;">${meta}</div>` : "",
        buyLabel,
        `</td></tr></table>`,
      ].join("");
    })
    .join("");
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

/** Fill missing product_url / image_url from listing lookup so emails stay useful. */
async function enrichSubmissionItemMedia(submission) {
  const items = Array.isArray(submission?.items) ? submission.items : [];
  if (!items.length) return submission;
  const enriched = await Promise.all(
    items.map(async (item) => {
      if (item?.image_url && item?.product_url) return item;
      const title = String(item?.title || "").trim();
      if (!title) return item;
      try {
        const res = await fetch(
          `${PUBLIC_API_BASE}/api/listing?name=${encodeURIComponent(title)}`,
          { headers: { Accept: "application/json" } }
        );
        if (!res.ok) return item;
        const data = await res.json();
        const listing = data?.listing || {};
        const image_url =
          item.image_url ||
          listing.image_url ||
          (Array.isArray(data.images) && data.images[0] ? String(data.images[0]) : "") ||
          "";
        const product_url =
          item.product_url ||
          listing.product_url ||
          data.productUrl ||
          data.shopifyOnlineUrl ||
          "";
        return {
          ...item,
          image_url: image_url || item.image_url || "",
          product_url: product_url || item.product_url || "",
        };
      } catch {
        return item;
      }
    })
  );
  return { ...submission, items: enriched };
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
        )}" alt="${escapeHtml(caption)}" width="840" style="display:block;width:100%;max-width:840px;height:auto;border:1px solid ${BORDER};border-radius:8px;" /></a>`
      : "",
    link
      ? `<p style="margin:8px 0 0;font-size:13px;"><a href="${escapeHtml(
          link
        )}" style="color:${NAVY};font-weight:700;">Open route in Google Maps</a></p>`
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

const SOCIAL = {
  resaleFacebook: "https://www.facebook.com/LostAndFoundResale/",
  resaleInstagram: "https://www.instagram.com/lostandfoundresale/",
  luxuryShop: "https://www.lostandfoundresale.com/",
  luxuryFacebook: "https://www.facebook.com/people/Lost-and-Found-Luxury-Resale/61584002517357/",
  luxuryInstagram: "https://www.instagram.com/lostandfoundhandbags/",
  iconFacebook: `${PUBLIC_API_BASE}/brand/icon-facebook.png`,
  iconInstagram: `${PUBLIC_API_BASE}/brand/icon-instagram.png`,
};

function socialLinkButton(href, label, { bg = "#ffffff", color = NAVY, border = BORDER } = {}) {
  return `<a href="${escapeHtml(href)}" style="display:inline-block;margin:0 8px 8px 0;padding:10px 14px;border:1px solid ${border};border-radius:999px;background:${bg};color:${color};font-size:12px;font-weight:800;text-decoration:none;letter-spacing:0.02em;">${escapeHtml(
    label
  )}</a>`;
}

function socialIconButton(href, label, iconUrl, { bg = "#1877F2", margin = "0 10px 10px 0" } = {}) {
  return [
    `<a href="${escapeHtml(href)}" style="display:inline-block;margin:${margin};text-decoration:none;vertical-align:middle;">`,
    `<table role="presentation" cellpadding="0" cellspacing="0" style="border-collapse:separate;border-radius:999px;background:${bg};">`,
    `<tr>`,
    `<td style="padding:8px 14px 8px 8px;vertical-align:middle;">`,
    `<img src="${escapeHtml(iconUrl)}" width="22" height="22" alt="" style="display:block;width:22px;height:22px;border:0;border-radius:6px;" />`,
    `</td>`,
    `<td style="padding:8px 16px 8px 0;vertical-align:middle;color:#ffffff;font-size:13px;font-weight:800;letter-spacing:0.02em;white-space:nowrap;line-height:22px;">${escapeHtml(
      label
    )}</td>`,
    `</tr>`,
    `</table>`,
    `</a>`,
  ].join("");
}

function socialOutlineButton(href, label, { color = NAVY, border = NAVY, margin = "0 10px 10px 0" } = {}) {
  return [
    `<a href="${escapeHtml(href)}" style="display:inline-block;margin:${margin};text-decoration:none;vertical-align:middle;">`,
    `<table role="presentation" cellpadding="0" cellspacing="0" style="border-collapse:separate;border-radius:999px;background:#ffffff;border:1px solid ${border};">`,
    `<tr>`,
    `<td style="padding:8px 18px;vertical-align:middle;color:${color};font-size:13px;font-weight:800;letter-spacing:0.02em;white-space:nowrap;line-height:22px;">${escapeHtml(
      label
    )}</td>`,
    `</tr>`,
    `</table>`,
    `</a>`,
  ].join("");
}

function socialFollowHtml({ isPickup }) {
  const intro = isPickup
    ? "Once your item is priced and live, it will be featured on our pages and across many of our other sales channels. Follow, like, subscribe, and share so you catch it when it goes live."
    : "Stay in the loop. Lots of new pieces and items show up and get featured here, and we post multiple times a day.";

  return [
    `<div style="margin:22px 0 0;padding:0;border:1px solid ${BORDER};border-radius:14px;overflow:hidden;background:${CREAM};">`,
    `<div style="padding:18px 20px 8px;">`,
    `<p style="margin:0 0 6px;font-size:11px;font-weight:800;letter-spacing:0.12em;text-transform:uppercase;color:${NAVY};">Follow Lost &amp; Found</p>`,
    `<p style="margin:0 0 16px;font-size:14px;line-height:1.55;color:#333;">${escapeHtml(intro)}</p>`,
    `</div>`,

    `<div style="margin:0 16px 14px;padding:14px 16px;background:#ffffff;border:1px solid ${BORDER};border-radius:12px;">`,
    `<p style="margin:0 0 4px;font-size:14px;font-weight:800;color:${NAVY};">Lost &amp; Found Resale</p>`,
    `<p style="margin:0 0 12px;font-size:12px;line-height:1.45;color:#666;">Furniture, finds, and daily drops from the Scottsdale showroom.</p>`,
    socialIconButton(SOCIAL.resaleFacebook, "Facebook", SOCIAL.iconFacebook, { bg: "#1877F2" }),
    socialIconButton(SOCIAL.resaleInstagram, "Instagram", SOCIAL.iconInstagram, {
      bg: "#dd2a7b",
    }),
    `</div>`,

    `<div style="margin:0 16px 18px;padding:14px 16px;background:#ffffff;border:1px solid ${BORDER};border-radius:12px;">`,
    `<p style="margin:0 0 4px;font-size:14px;font-weight:800;color:${NAVY};">Luxury division</p>`,
    `<p style="margin:0 0 12px;font-size:12px;line-height:1.45;color:#666;">Designer handbags and accessories.</p>`,
    `<table role="presentation" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">`,
    `<tr>`,
    `<td style="padding:0 10px 0 0;vertical-align:middle;">`,
    socialOutlineButton(SOCIAL.luxuryShop, "Shop", { margin: "0" }),
    `</td>`,
    `<td style="padding:0 10px 0 0;vertical-align:middle;">`,
    socialIconButton(SOCIAL.luxuryFacebook, "Facebook", SOCIAL.iconFacebook, {
      bg: "#1877F2",
      margin: "0",
    }),
    `</td>`,
    `<td style="padding:0;vertical-align:middle;">`,
    socialIconButton(SOCIAL.luxuryInstagram, "Instagram", SOCIAL.iconInstagram, {
      bg: "#dd2a7b",
      margin: "0",
    }),
    `</td>`,
    `</tr>`,
    `</table>`,
    `</div>`,
    `</div>`,
  ].join("");
}

function socialFollowText({ isPickup }) {
  const intro = isPickup
    ? "Once your item is priced and live, it will be featured on our pages and across many of our other sales channels. Follow, like, subscribe, and share so you catch it when it goes live."
    : "Stay in the loop. Lots of new pieces and items show up and get featured here, and we post multiple times a day.";
  return [
    "Follow Lost & Found",
    intro,
    `Lost & Found Resale Facebook: ${SOCIAL.resaleFacebook}`,
    `Lost & Found Resale Instagram: ${SOCIAL.resaleInstagram}`,
    "Luxury division:",
    `Shop: ${SOCIAL.luxuryShop}`,
    `Luxury Facebook: ${SOCIAL.luxuryFacebook}`,
    `Luxury Instagram: ${SOCIAL.luxuryInstagram}`,
  ].join("\n");
}

function brandHeaderHtml() {
  return [
    `<div style="background:#ffffff;padding:18px 22px 14px;border-bottom:1px solid ${BORDER};text-align:center;">`,
    `<a href="${BRAND.site}" style="text-decoration:none;"><img src="${BRAND.logo}" alt="Lost + Found Resale Interiors" width="280" style="display:block;margin:0 auto;width:280px;max-width:80%;height:auto;" /></a>`,
    `<p style="margin:10px 0 0;font-size:11px;letter-spacing:0.14em;text-transform:uppercase;color:${MUTED};">Delivery — Pickup — Freight</p>`,
    `</div>`,
  ].join("");
}

function showroomStripHtml() {
  return [
    `<div style="margin:18px 0 0;">`,
    `<p style="margin:0 0 10px;font-size:11px;font-weight:800;letter-spacing:0.08em;text-transform:uppercase;color:${INK};">Shop Lost &amp; Found</p>`,
    `<div>`,
    socialLinkButton(BRAND.furniture, "Furniture", { bg: "#ffffff", color: INK, border: "#222" }),
    socialLinkButton(BRAND.handbags, "Handbags", { bg: "#ffffff", color: INK, border: "#222" }),
    `</div>`,
    `<p style="margin:10px 0 0;font-size:12px;color:${MUTED};">15530 N Greenway Hayden Loop Ste 100, Scottsdale | Mon-Sat 10-5, Sun 12-4</p>`,
    `</div>`,
  ].join("");
}

function wrapEmail(inner, { includeShopLinks = false } = {}) {
  return [
    `<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head><body style="margin:0;padding:0;">`,
    `<div style="font-family:${FONT};background:#f3f1ec;padding:24px 12px;">`,
    `<div style="max-width:880px;margin:0 auto;background:#ffffff;border:1px solid ${BORDER};border-radius:10px;overflow:hidden;">`,
    brandHeaderHtml(),
    `<div style="padding:22px 24px 28px;color:#222;line-height:1.55;">`,
    inner,
    includeShopLinks ? showroomStripHtml() : "",
    `<div style="margin:18px 0 0;padding-top:14px;border-top:1px solid ${BORDER};text-align:center;">`,
    `<img src="${BRAND.seal}" alt="Lost + Found seal" width="72" style="display:inline-block;width:72px;height:auto;opacity:0.9;" />`,
    `<p style="margin:8px 0 0;font-size:11px;color:${MUTED};">Lost &amp; Found Resale Interiors | Scottsdale, Arizona</p>`,
    `</div>`,
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
    "Estimate / quote request";
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
    ? `Consignor Pickup Estimate | ${submission.customer_name} | ${submission.zip}`
    : isLocal
      ? `Local Delivery Estimate | ${submission.customer_name} | ${submission.zip}`
      : `Nationwide Freight Quote | ${submission.customer_name} | ${submission.zip}`;

  const estimateRows =
    isLocal && localEstimate?.estimated_price != null
      ? [
          ["Estimate", money(localEstimate.estimated_price)],
          [
            "Drive",
            `${route?.drive_minutes ?? localEstimate.drive_minutes} min one way — ${
              Number(route?.drive_minutes ?? localEstimate.drive_minutes) * 2
            } min round trip`,
          ],
          [
            "Distance",
            route?.distance_miles != null ? `${route.distance_miles} mi` : "-",
          ],
          ["Rate", "$95/hour round-trip (not one way)"],
        ]
      : isLocal
        ? [["Estimate", "Pending | route unavailable / manual review"]]
        : [
            [
              "Range",
              nationwideRate?.range_low != null && nationwideRate?.range_high != null
                ? `${money(nationwideRate.range_low)} - ${money(nationwideRate.range_high)}`
                : "Pending partner quote",
            ],
            [
              "Distance",
              nationwideRate?.distance_miles != null || route?.distance_miles != null
                ? `${nationwideRate?.distance_miles ?? route.distance_miles} mi`
                : "-",
            ],
            [
              "Note",
              `Forward to our Freight Center contact (${FREIGHTCENTER_LUKE_EMAIL}) · SOP in Dropbox · floor $350`,
            ],
          ];

  const requestCompactRows = [
    ["Mode", modeLabel],
    ["Path", pathLabel],
    [
      "Type",
      formatDestinationType(
        submission.destination_type ||
          (submission.access?.residential
            ? "Residential"
            : submission.access?.commercial
              ? "Commercial"
              : "")
      ),
    ],
    ["Address", addr],
    submission.unit || submission.delivery_address?.unit
      ? ["Unit", submission.unit || submission.delivery_address?.unit]
      : null,
    ...estimateRows,
    submission.multi_item_note ? ["Multi-item note", submission.multi_item_note] : null,
    submission.page_url ? ["Page", submission.page_url] : null,
  ].filter(Boolean);

  const accessCompactRows = accessHighlightRows(submission.access, {
    isPickup,
    includeLiftgate: !isLocal,
  });

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
      ? `${money(nationwideRate.range_low)} - ${money(nationwideRate.range_high)}`
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
            )}</strong>. <strong>We will get you a quote</strong> — our team will review this and email you full rates.`
          : `We received your nationwide freight request. <strong>We will get you a quote</strong> — our team will review this and email you full rates.`;

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
          }This is a preliminary range from distance, pallet size/weight, and the freight options you selected — every carrier prices differently.</p>`;

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
          }This is a preliminary range from distance, pallet size/weight, and the freight options you selected — every carrier prices differently.`;

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

  const buyNowHtml = buyNowCtaHtml(submission.items || []);
  const buyUrlsText = (submission.items || [])
    .filter((row) => row.product_url)
    .map((row, i) => `Buy Now${(submission.items || []).filter((r) => r.product_url).length > 1 ? ` (item ${i + 1})` : ""}: ${row.product_url}`)
    .join("\n");

  const buyPolicyHtml = isPickup
    ? [
        `<div style="margin:22px 0;padding:16px 18px;background:${CREAM};border:1px solid ${BORDER};border-radius:8px;">`,
        `<p style="margin:0 0 8px;font-size:14px;font-weight:800;color:${INK};">Next steps for pickup</p>`,
        `<p style="margin:0;font-size:14px;line-height:1.6;color:#333;">Final pricing will need to be confirmed directly with the driver team. If you need final confirmed pricing, please email <a href="mailto:${LOST_FOUND_EMAIL}" style="color:${INK};font-weight:700;">${LOST_FOUND_EMAIL}</a> or call <a href="tel:${LOST_FOUND_PHONE.replace(
          /\D/g,
          ""
        )}" style="color:${INK};font-weight:700;">${LOST_FOUND_PHONE}</a>.</p>`,
        `</div>`,
      ].join("")
    : [
        `<div style="margin:22px 0;padding:16px 18px;background:${CREAM};border:1px solid ${BORDER};border-radius:8px;">`,
        `<table role="presentation" width="100%" cellpadding="0" cellspacing="0"><tr>`,
        `<td valign="top" style="padding-right:${buyNowHtml ? "16px" : "0"};">`,
        `<p style="margin:0 0 8px;font-size:14px;font-weight:800;color:${INK};">Ready to purchase?</p>`,
        `<p style="margin:0;font-size:14px;line-height:1.6;color:#333;">We do <strong>not hold items</strong> — we work for our consignors. Ready to buy? Purchase now and we can confirm delivery/freight after. Want rates first? Email <a href="mailto:${LOST_FOUND_EMAIL}" style="color:${INK};font-weight:700;">${LOST_FOUND_EMAIL}</a>, call <a href="tel:${LOST_FOUND_PHONE.replace(
          /\D/g,
          ""
        )}" style="color:${INK};font-weight:700;">${LOST_FOUND_PHONE}</a>, or reply here.</p>`,
        `</td>`,
        buyNowHtml
          ? `<td valign="middle" width="168" style="text-align:center;white-space:nowrap;">${buyNowHtml}</td>`
          : "",
        `</tr></table>`,
        `</div>`,
      ].join("");

  const buyPolicyText = isPickup
    ? [
        "Next steps for pickup",
        "Final pricing will need to be confirmed directly with the driver team. If you need final confirmed pricing, please email info@lostandfoundresale.com or call 480-588-7006.",
      ].join("\n")
    : [
        "Ready to purchase?",
        "We do not hold items — we work for our consignors. Ready to buy? Purchase now and we can confirm delivery/freight after. Want rates first? Email info@lostandfoundresale.com, call 480-588-7006, or reply here.",
        buyUrlsText || null,
      ]
        .filter(Boolean)
        .join("\n");

  const customerSubject = isPickup
    ? "Thanks, we received your consignor pickup estimate request"
    : isLocal
      ? "Thanks, we received your delivery estimate request"
      : "Thanks, we received your freight quote request";

  const extraReviewReasons = nonAccessReviewReasons(reviewReasons);

  const internalSnapshotHtml = [
    `<div style="margin:0 0 14px;padding:12px 14px;background:${CREAM};border:1px solid ${BORDER};border-radius:8px;">`,
    `<p style="margin:0;font-size:16px;font-weight:800;color:${NAVY};">${escapeHtml(
      snapshotLine || pathLabel
    )}</p>`,
    `<p style="margin:6px 0 0;font-size:13px;color:#333;">${escapeHtml(addr)}</p>`,
    `<p style="margin:4px 0 0;font-size:13px;color:${MUTED};"><a href="mailto:${escapeHtml(
      submission.customer_email
    )}" style="color:${NAVY};font-weight:700;">${escapeHtml(
      submission.customer_name
    )}</a> | ${escapeHtml(formatPhoneForEmail(submission.customer_phone))} | ${escapeHtml(
      submission.customer_email
    )}</p>`,
    extraReviewReasons.length
      ? `<p style="margin:6px 0 0;font-size:13px;color:#9c2f2f;"><strong>Review:</strong> ${escapeHtml(
          extraReviewReasons.join("; ")
        )}</p>`
      : "",
    mapRoute?.directions_url
      ? `<p style="margin:8px 0 0;font-size:13px;"><a href="${escapeHtml(
          mapRoute.directions_url
        )}" style="color:${NAVY};font-weight:700;">Open route in Google Maps</a></p>`
      : "",
    `</div>`,
    // Map image only (link already above) to keep the email shorter when image fails
    mapRoute?.map_image_url
      ? `<div style="margin:0 0 14px;"><a href="${escapeHtml(
          mapRoute.directions_url || mapRoute.map_image_url
        )}" style="display:block;text-decoration:none;"><img src="${escapeHtml(
          mapRoute.map_image_url
        )}" alt="${isPickup ? "Pickup route" : "Delivery route"}" width="840" style="display:block;width:100%;max-width:840px;height:auto;border:1px solid ${BORDER};border-radius:8px;" /></a></div>`
      : "",
  ].join("");

  const ACCESS_RED = "#9c2f2f";
  const twoColHtml = [
    `<table role="presentation" cellpadding="0" cellspacing="0" style="width:100%;border-collapse:collapse;margin:0 0 14px;">`,
    `<tr>`,
    `<td style="width:50%;vertical-align:top;padding:0 8px 0 0;">`,
    `<p style="margin:0 0 8px;padding-bottom:4px;border-bottom:2px solid ${NAVY};color:${NAVY};font-size:13px;font-weight:800;">Request</p>`,
    compactTableHtml(requestCompactRows),
    `</td>`,
    `<td style="width:50%;vertical-align:top;padding:0 0 0 8px;">`,
    `<p style="margin:0 0 8px;padding-bottom:4px;border-bottom:2px solid ${ACCESS_RED};color:${ACCESS_RED};font-size:13px;font-weight:800;">Access highlights</p>`,
    compactTableHtml(accessCompactRows, { valueColor: ACCESS_RED }),
    `</td>`,
    `</tr>`,
    `</table>`,
  ].join("");

  const localInternalNextStepsHtml = isLocal
    ? [
        `<div style="margin:0 0 14px;padding:14px 16px;background:#eef1ff;border:1px solid ${BORDER};border-left:4px solid ${NAVY};border-radius:8px;">`,
        `<p style="margin:0 0 4px;font-size:11px;font-weight:800;letter-spacing:0.1em;text-transform:uppercase;color:${NAVY};">Next steps</p>`,
        `<p style="margin:0;font-size:15px;line-height:1.5;font-weight:700;color:${INK};">Confirm with delivery team and let client know.</p>`,
        `</div>`,
      ].join("")
    : "";

  const localInternalNextStepsText = isLocal
    ? ["Next steps", "Confirm with delivery team and let client know.", ""].join("\n")
    : "";

  const internalHtml = wrapEmail(
    [
      `<h2 style="margin:0 0 4px;color:${NAVY};font-size:20px;">New ${escapeHtml(
        pathLabel
      )}</h2>`,
      `<p style="margin:0 0 12px;font-size:12px;color:${MUTED};">${escapeHtml(
        requestId
      )} | ${escapeHtml(submittedAt || "")} | Reply goes to customer</p>`,
      localInternalNextStepsHtml,
      `<p style="margin:0 0 12px;font-size:12px;color:${MUTED};">{{PDF_NOTICE}}</p>`,

      internalSnapshotHtml,
      twoColHtml,

      `<p style="margin:0 0 8px;padding-bottom:4px;border-bottom:2px solid ${NAVY};color:${NAVY};font-size:13px;font-weight:800;">Items</p>`,
      internalItemsHtml(submission.items || []),

      !isLocal ? nationwideInternalPlaybookHtml(submission) : "",
    ].join("")
  );

  const internalText = [
    internalSubject,
    `Request ID: ${requestId}`,
    `Submitted: ${submittedAt}`,
    "",
    localInternalNextStepsText || null,
    snapshotLine || pathLabel,
    `Address: ${addr}`,
    `Customer: ${submission.customer_name} | ${formatPhoneForEmail(submission.customer_phone)} | ${submission.customer_email}`,
    mapRoute?.directions_url ? `Map: ${mapRoute.directions_url}` : null,
    extraReviewReasons.length ? `Review: ${extraReviewReasons.join("; ")}` : null,
    "",
    "REQUEST",
    ...requestCompactRows.map(([k, v]) => `${k}: ${v}`),
    "",
    "ACCESS HIGHLIGHTS",
    ...accessCompactRows.map(([k, v]) => `${k}: ${v}`),
    itemsText(submission.items),
    !isLocal ? "" : null,
    !isLocal ? nationwideInternalPlaybookText(submission) : null,
  ]
    .filter((l) => l != null)
    .join("\n");

  const oversizedNote = !isLocal && hasOversizedPalletItems(submission.items || []);
  const oversizedHtml = oversizedNote
    ? `<div style="margin:14px 0;padding:14px 16px;background:#fff8e8;border:1px solid #e6d7a8;border-radius:8px;">
        <p style="margin:0;font-size:14px;line-height:1.55;color:#333;"><strong>Oversized for a standard 48″×40″ pallet.</strong> We may be able to maneuver or re-orient this so it fits a standard pallet, which can bring the freight cost down a lot. We will follow up after we review.</p>
      </div>`
    : "";
  const oversizedText = oversizedNote
    ? "Oversized for a standard 48x40 pallet: we may be able to maneuver or re-orient this so it fits a standard pallet, which can bring the freight cost down a lot. We will follow up after we review."
    : null;

  const nationwideNextStepsHtml = isLocal
    ? ""
    : [
        `<div style="margin:18px 0 0;padding:16px 18px;background:${CREAM};border:1px solid ${BORDER};border-radius:8px;">`,
        `<p style="margin:0 0 10px;font-size:14px;font-weight:800;color:${INK};">What happens next</p>`,
        `<p style="margin:0 0 10px;font-size:14px;line-height:1.55;color:#333;"><strong>We will get you a confirmed quote</strong> by email. Carrier rates vary — you can also call FreightCenter anytime at <a href="tel:${FREIGHTCENTER_PHONE.replace(/\D/g, "")}" style="color:${NAVY};font-weight:700;">${FREIGHTCENTER_PHONE}</a>.</p>`,
        submission.access?.white_glove
          ? `<p style="margin:0 0 10px;font-size:14px;line-height:1.55;color:#333;"><strong>White Glove selected</strong> — that raises the quote versus standard threshold / curbside freight.</p>`
          : "",
        submission.access?.warehouse_pickup
          ? `<p style="margin:0 0 10px;font-size:14px;line-height:1.55;color:#333;"><strong>Warehouse / terminal pickup selected</strong> — that usually saves about $150–$200.</p>`
          : `<p style="margin:0 0 10px;font-size:14px;line-height:1.55;color:#333;">Tip: warehouse/terminal pickup near you usually saves about <strong>$150–$200</strong>. We will mention this with your quote.</p>`,
        `<p style="margin:0;font-size:14px;line-height:1.55;color:#333;"><strong>Liftgate at delivery:</strong> ${
          submission.access?.liftgate_delivery ? "Yes" : "No"
        } (your choice). <strong>Pickup loading:</strong> Lost &amp; Found will confirm whether it can load without a liftgate — most items cannot, because they ship on a pallet. Hours: <strong>10am–5pm, Monday–Saturday</strong>.</p>`,
        `</div>`,
      ].join("");

  const nationwideNextStepsText = isLocal
    ? null
    : [
        "What happens next",
        "We will get you a confirmed quote by email. Carrier rates vary — you can also call FreightCenter anytime at 800-716-7608.",
        submission.access?.white_glove
          ? "White Glove selected — that raises the quote versus standard threshold / curbside freight."
          : null,
        submission.access?.warehouse_pickup
          ? "Warehouse / terminal pickup selected — that usually saves about $150–$200."
          : "Tip: warehouse/terminal pickup near you usually saves about $150–$200. We will mention this with your quote.",
        `Liftgate at delivery: ${
          submission.access?.liftgate_delivery ? "Yes" : "No"
        } (your choice). Pickup loading: Lost & Found will confirm whether it can load without a liftgate — most items cannot, because they ship on a pallet. Hours: 10am–5pm, Monday–Saturday.`,
      ]
        .filter(Boolean)
        .join("\n");

  const customerHtml = wrapEmail(
    [
      `<h2 style="margin:0 0 12px;color:${NAVY};font-size:22px;">${greetingHtml}</h2>`,
      `<p style="margin:0 0 10px;font-size:15px;color:#333;">${estimateLine}</p>`,
      driveExplainHtml,
      oversizedHtml,
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
      nationwideNextStepsHtml,

      socialFollowHtml({ isPickup }),

      `<p style="margin:18px 0 0;font-size:12px;color:#999;">Reference: ${escapeHtml(
        requestId
      )} · Lost &amp; Found Resale · Scottsdale · lostandfoundresale.com</p>`,
    ].join(""),
    { includeShopLinks: true }
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
    oversizedText,
    mapRoute?.directions_url ? `Route map: ${mapRoute.directions_url}` : null,
    "",
    "Preliminary estimate only — not a booking.",
    nationwideNextStepsText,
    "",
    "Your item(s):",
    customerItemsText(submission.items || []),
    `Delivery / pickup: ${addr}`,
    "",
    buyPolicyText,
    "",
    socialFollowText({ isPickup }),
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

function applyPdfNotice({ html, text, attached, downloadUrl, filename }) {
  let noticeHtml = "";
  let noticeText = "";
  if (attached && downloadUrl) {
    noticeHtml = `Printable PDF is attached, and also available to download: <a href="${escapeHtml(
      downloadUrl
    )}" style="color:${NAVY};font-weight:700;">${escapeHtml(filename || "Download PDF")}</a>.`;
    noticeText = `Printable PDF attached. Download: ${downloadUrl}`;
  } else if (attached) {
    noticeHtml = "Printable PDF with the full summary is attached.";
    noticeText = "Printable PDF with the full summary is attached.";
  } else if (downloadUrl) {
    noticeHtml = `Printable PDF: <a href="${escapeHtml(
      downloadUrl
    )}" style="color:${NAVY};font-weight:700;">${escapeHtml(
      filename || "Download PDF"
    )}</a> (AWS backup link).`;
    noticeText = `Printable PDF download: ${downloadUrl}`;
  } else {
    noticeHtml = "Printable PDF was not generated for this request.";
    noticeText = "Printable PDF was not generated for this request.";
  }

  return {
    html: String(html || "").replace("{{PDF_NOTICE}}", noticeHtml),
    text: `${noticeText}\n\n${String(text || "").replace(/\{\{PDF_NOTICE\}\}\n?/g, "")}`.trim(),
  };
}

export async function sendFreightQuoteEmails(submission, ctx) {
  const enrichedSubmission = await enrichSubmissionItemMedia(submission);
  const built = buildFreightQuoteEmails(enrichedSubmission, ctx);

  let pdfBuffer = null;
  let pdfFilename = buildFreightQuotePdfFilename(enrichedSubmission, ctx);
  try {
    pdfBuffer = await generateFreightQuotePdf(enrichedSubmission, ctx);
    if (!pdfBuffer?.length) pdfBuffer = null;
  } catch (pdfErr) {
    console.error(
      "[freight-quote] internal PDF generation failed (continuing):",
      pdfErr?.message || pdfErr
    );
    pdfBuffer = null;
  }

  let s3Url = null;
  let s3Meta = null;
  if (pdfBuffer && isFreightPdfS3Configured()) {
    const uploaded = await uploadFreightQuotePdfToS3({
      buffer: pdfBuffer,
      filename: pdfFilename,
      requestId: ctx?.requestId || "",
    });
    if (uploaded.ok) {
      s3Url = uploaded.url;
      s3Meta = { bucket: uploaded.bucket, key: uploaded.key, expiresIn: uploaded.expiresIn };
    }
  }

  const withAttach = applyPdfNotice({
    html: built.internal.html,
    text: built.internal.text,
    attached: Boolean(pdfBuffer),
    downloadUrl: s3Url,
    filename: pdfFilename,
  });

  const replyTo = built.internal.replyTo;
  const subject = built.internal.subject;
  const attachments = pdfBuffer
    ? [
        {
          filename: pdfFilename,
          content: pdfBuffer,
          contentType: "application/pdf",
        },
      ]
    : [];

  let internal;
  let pdfAttached = false;
  let usedS3Fallback = false;

  try {
    if (attachments.length) {
      internal = await sendInternalNotificationWithAttachments({
        subject,
        html: withAttach.html,
        text: withAttach.text,
        replyTo,
        attachments,
      });
      pdfAttached = true;
    } else {
      internal = await sendInternalNotification({
        subject,
        html: withAttach.html,
        text: withAttach.text,
        replyTo,
      });
    }
  } catch (attachErr) {
    console.error(
      "[freight-quote] internal email with PDF attachment failed:",
      attachErr?.message || attachErr
    );

    // If attach failed and we don't have S3 yet, try uploading now as backup.
    if (pdfBuffer && !s3Url && isFreightPdfS3Configured()) {
      const uploaded = await uploadFreightQuotePdfToS3({
        buffer: pdfBuffer,
        filename: pdfFilename,
        requestId: ctx?.requestId || "",
      });
      if (uploaded.ok) {
        s3Url = uploaded.url;
        s3Meta = { bucket: uploaded.bucket, key: uploaded.key, expiresIn: uploaded.expiresIn };
      }
    }

    const fallback = applyPdfNotice({
      html: built.internal.html,
      text: built.internal.text,
      attached: false,
      downloadUrl: s3Url,
      filename: pdfFilename,
    });

    if (!s3Url && !isFreightPdfS3Configured()) {
      console.warn(
        "[freight-quote] PDF attachment failed and S3 is not configured (set FREIGHT_PDF_S3_BUCKET + AWS creds)."
      );
    }

    internal = await sendInternalNotification({
      subject,
      html: fallback.html,
      text: fallback.text,
      replyTo,
    });
    usedS3Fallback = Boolean(s3Url);
    pdfAttached = false;
  }

  const internalId = String(internal?.data?.id || internal?.id || "");
  const internalTo = Array.isArray(internal?.to)
    ? internal.to
    : String(process.env.INTERNAL_NOTIFY_EMAIL || "")
        .split(",")
        .map((e) => e.trim())
        .filter(Boolean);

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
      to: internalTo,
      resend_id: internalId,
      pdf_attached: pdfAttached,
      pdf_s3_url: s3Url || null,
      pdf_s3_fallback: usedS3Fallback,
      pdf_s3: s3Meta,
    },
    customer: {
      sent: true,
      to: customer.to,
      resend_id: customer.id,
    },
  };
}
