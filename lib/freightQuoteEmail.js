import { sendEmail, sendEmailWithAttachments, sendInternalNotification, sendInternalNotificationWithAttachments } from "../emailService.js";
import {
  FREIGHTCENTER_PHONE,
  LOST_FOUND_EMAIL,
  LOST_FOUND_PHONE,
  CRUZ_PRO_LINE_NAME,
  CRUZ_PRO_LINE_PHONE,
  isArizonaStateOrZip,
  describeStackConfirmNeed,
} from "./freightPalletize.js";
import {
  buildFreightQuotePdfFilename,
  generateFreightQuotePdf,
  nonAccessReviewReasons,
  accessHighlightRows,
} from "./freightQuotePdf.js";
import {
  buildLocalSignPdfFilename,
  generateLocalSignPdf,
} from "./freightLocalSignPdf.js";
import { isFreightPdfS3Configured, uploadFreightQuotePdfToS3 } from "./freightQuoteS3.js";

const FONT = "Arial,Helvetica,sans-serif";
const NAVY = "#07127c";
const MUTED = "#5c5c5c";
const BORDER = "#e5e1d8";
const CREAM = "#fbfaf6";
const INK = "#111111";
const SHOP_SEARCH_BASE = "https://www.lostandfoundresale.com/search?query=";

function shopSearchUrl(title) {
  const q = encodeURIComponent(String(title || "").trim()).replace(/%20/g, "+");
  return `${SHOP_SEARCH_BASE}${q}`;
}

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
  cruzLogo: `${PUBLIC_API_BASE}/brand/cruz-pro-line-truck.png`,
  storePhoto: `${PUBLIC_API_BASE}/brand/showroom-exterior-v2.png`,
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
  if (pallet.freight_class == null) return "To be confirmed";
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
    const formSaidNonStack = !!(row.non_stackable || p.non_stackable);
    const setBit =
      row.set_count > 1 || p.set_count > 1
        ? `set of ${row.set_count || p.set_count}`
        : "single / as listed";
    return [
      `${i + 1}. ${row.title || "Item"}`,
      `   Piece dims: ${dims(row.width, row.depth, row.height)} · ${row.weight != null ? `${row.weight} lb` : "weight TBD"} · ${setBit}`,
      `   Freight-ready pallet entry: ${dims(p.width, p.depth, p.height)} · ${p.weight != null ? `${p.weight} lb` : "—"} · class ${classLabel(p)}`,
      `   STACKING: Confirm with warehouse whether freight can be stacked on this — do not assume. (Form said: ${
        formSaidNonStack ? "non-stackable / fragile" : "not marked non-stackable"
      } — ignore for quoting until confirmed.)`,
      p.stack_note ? `   Form stack note (unconfirmed): ${p.stack_note}` : null,
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
  const destState = submission.state || submission.delivery_address?.state || "";
  const destZip = submission.zip || submission.delivery_address?.zip || "";
  const outOfAz = !isArizonaStateOrZip({ state: destState, zip: destZip });
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
    "Liftgate at pickup: Lost & Found will determine with final quote (assume yes for now)",
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
    `<p style="margin:0 0 12px;font-size:13px;line-height:1.5;color:${MUTED};">Ready to forward to our FreightCenter contact for confirmed carrier options.</p>`,
    `<p style="margin:0 0 12px;font-size:14px;line-height:1.55;color:#222;">Forward this request to our FreightCenter contact and ask for the best available carrier options, including the lowest rate, best overall service, residential delivery, and nearby freight-terminal or warehouse-pickup options. Review all recommendations internally before sending confirmed options to the customer.</p>`,
    `<p style="margin:0 0 10px;font-size:14px;line-height:1.55;color:#333;"><strong>Customer follow-up:</strong> After reviewing the carrier responses, send the customer the best available options and clearly explain any differences in price, transit time, residential service, liftgate service, and terminal pickup.</p>`,
    `<p style="margin:0 0 10px;font-size:14px;line-height:1.55;color:#333;"><strong>Freight savings note:</strong> Always request nearby freight-terminal or warehouse-pickup options. Customer pickup from a local terminal can significantly reduce the final freight cost compared with residential delivery.</p>`,
    outOfAz
      ? [
          `<p style="margin:0 0 4px;font-size:14px;line-height:1.55;color:#333;"><strong>Online purchase tax note:</strong> Because the item is being shipped outside Arizona, no Arizona sales tax is charged on the merchandise purchase when the customer buys the item online. This is approximately an 8% savings compared with an Arizona purchase.</p>`,
          `<p style="margin:0 0 12px;font-size:12px;line-height:1.45;color:${MUTED};">This applies to the item purchase. Freight, delivery, or other service charges are handled separately.</p>`,
        ].join("")
      : "",
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
  const destState = submission.state || submission.delivery_address?.state || "";
  const destZip = submission.zip || submission.delivery_address?.zip || "";
  const outOfAz = !isArizonaStateOrZip({ state: destState, zip: destZip });
  const palletFit = oversized
    ? "Pallet fit: Larger than a standard 48x40. We may be able to stand it up differently, rotate, stack, or maneuver it to fit a standard pallet (often much lower cost). Please confirm best packing before finalizing rates."
    : smaller
      ? "Pallet fit: Smaller than a full 48x40. We may have a smaller pallet available — please confirm the best pallet size for the rate."
      : "Pallet fit: Standard 48x40 footprint unless packing notes say otherwise.";
  return [
    "NATIONWIDE FREIGHT — NEXT STEPS",
    "Ready to forward to our FreightCenter contact for confirmed carrier options.",
    "Forward this request to our FreightCenter contact and ask for the best available carrier options, including the lowest rate, best overall service, residential delivery, and nearby freight-terminal or warehouse-pickup options. Review all recommendations internally before sending confirmed options to the customer.",
    "",
    "Customer follow-up: After reviewing the carrier responses, send the customer the best available options and clearly explain any differences in price, transit time, residential service, liftgate service, and terminal pickup.",
    "Freight savings note: Always request nearby freight-terminal or warehouse-pickup options. Customer pickup from a local terminal can significantly reduce the final freight cost compared with residential delivery.",
    outOfAz
      ? "Online purchase tax note: Because the item is being shipped outside Arizona, no Arizona sales tax is charged on the merchandise purchase when the customer buys the item online. This is approximately an 8% savings compared with an Arizona purchase.\nThis applies to the item purchase. Freight, delivery, or other service charges are handled separately."
      : null,
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
      "Liftgate at pickup: Lost & Found will determine with final quote (assume yes for now)",
      a.stairs
        ? `Stairs: Yes${a.stair_flights ? ` (${a.stair_flights} flight(s))` : ""}`
        : null,
      a.dock ? "Loading dock or forklift at delivery" : null,
      a.notes ? `Notes: ${a.notes}` : null,
    ]
      .filter(Boolean)
      .join(" · ") || "Standard residential freight options.",
  ]
    .filter((l) => l != null)
    .join("\n");
}

function internalItemsHtml(items = [], { showPrice = true } = {}) {
  return items
    .map((row) => {
      const p = row.pallet;
      const title = escapeHtml(row.title || `Item ${row.index}`);
      const bits = [
        dims(row.width, row.depth, row.height),
        row.weight != null ? `${row.weight} lb` : null,
        showPrice && row.price ? money(row.price) : null,
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
    return `<a href="${escapeHtml(withUrl[0].product_url)}" style="display:inline-block;padding:10px 16px;background:${NAVY};color:#ffffff;text-decoration:none;font-weight:800;font-size:13px;letter-spacing:0.02em;border-radius:8px;white-space:nowrap;">Buy Now</a>`;
  }
  return withUrl
    .map(
      (row, i) =>
        `<a href="${escapeHtml(row.product_url)}" style="display:inline-block;margin:${i ? "6px" : "0"} 0 0;padding:10px 14px;background:${NAVY};color:#ffffff;text-decoration:none;font-weight:800;font-size:13px;border-radius:8px;white-space:nowrap;">Buy Now${
          withUrl.length > 1 ? ` · ${i + 1}` : ""
        }</a>`
    )
    .join("<br/>");
}

function customerItemsHtml(items = [], { showBuyLinks = true } = {}) {
  return (items || [])
    .map((row) => {
      const title = escapeHtml(row.title || `Item ${row.index}`);
      const dimBits =
        row.width != null && row.depth != null && row.height != null
          ? `${escapeHtml(row.width)}X${escapeHtml(row.depth)}X${escapeHtml(row.height)}H`
          : "";
      const titleAlreadyHasDims = /\d+\s*[xX×]\s*\d+/.test(String(row.title || ""));
      const titleLine = dimBits && !titleAlreadyHasDims ? `${title} · ${dimBits}` : title;
      const meta = [
        row.set_count > 1 ? `Set of ${escapeHtml(row.set_count)}` : null,
        row.weight != null ? `${escapeHtml(row.weight)} lb` : null,
        row.price ? money(row.price) : null,
      ]
        .filter(Boolean)
        .join(" · ");
      const hasListing = Boolean(String(row.product_url || "").trim());
      const url = showBuyLinks
        ? hasListing
          ? String(row.product_url)
          : shopSearchUrl(row.title || "")
        : "";
      const ctaLabel = hasListing ? "Buy Now" : "Find Item";
      const img = row.image_url ? String(row.image_url) : "";
      const imgBlock = img
        ? url
          ? `<a href="${escapeHtml(url)}" style="display:block;line-height:0;text-decoration:none;"><img src="${escapeHtml(
              img
            )}" alt="${title}" width="72" height="72" style="display:block;width:72px;height:72px;object-fit:cover;border-radius:8px;border:1px solid ${BORDER};background:#fff;" /></a>`
          : `<img src="${escapeHtml(
              img
            )}" alt="${title}" width="72" height="72" style="display:block;width:72px;height:72px;object-fit:cover;border-radius:8px;border:1px solid ${BORDER};background:#fff;" />`
        : "";
      const buyCell = url
        ? `<td valign="middle" align="right" style="padding:10px 12px 10px 8px;white-space:nowrap;"><a href="${escapeHtml(
            url
          )}" style="display:inline-block;padding:10px 14px;background:${NAVY};color:#ffffff;text-decoration:none;font-weight:800;font-size:13px;border-radius:8px;">${ctaLabel}</a></td>`
        : "";
      return [
        `<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin:0 0 10px;background:${CREAM};border:1px solid ${BORDER};border-radius:8px;">`,
        `<tr>`,
        imgBlock
          ? `<td valign="middle" width="84" style="padding:10px 0 10px 10px;">${imgBlock}</td>`
          : "",
        `<td valign="middle" style="padding:10px 12px;font-size:14px;line-height:1.4;color:#333;text-align:left;">`,
        `<div style="margin:0 0 2px;text-align:left;"><strong style="color:${INK};font-size:14px;line-height:1.3;">${titleLine}</strong></div>`,
        meta
          ? `<div style="margin:0;color:${MUTED};font-size:12px;line-height:1.35;text-align:left;">${meta}</div>`
          : "",
        `</td>`,
        buyCell,
        `</tr></table>`,
      ].join("");
    })
    .join("");
}

function customerItemsText(items = [], { showBuyLinks = true } = {}) {
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
      return showBuyLinks
        ? `${bits}\n  ${row.product_url ? "Buy" : "Find"}: ${
            row.product_url ? row.product_url : shopSearchUrl(row.title || "")
          }`
        : bits;
    })
    .join("\n");
}

function normalizeListingTitle(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

/**
 * Optionally fill missing product_url / image_url from listing lookup.
 * Never invent shop links for consignor pickups or fuzzy title matches
 * (e.g. "Bed" matching a random live listing).
 */
async function enrichSubmissionItemMedia(submission) {
  const items = Array.isArray(submission?.items) ? submission.items : [];
  if (!items.length) return submission;

  if (submission?.delivery_path === "pickup_az") {
    return {
      ...submission,
      items: items.map((item) => ({
        ...item,
        product_url: "",
        image_url: "",
      })),
    };
  }

  const enriched = await Promise.all(
    items.map(async (item) => {
      if (item?.image_url && item?.product_url) return item;
      const title = String(item?.title || "").trim();
      if (!title) return item;
      const source = String(item?.source || "").toLowerCase();
      // Manual entries without a URL should stay manual — do not invent a listing.
      if (source === "manual" && !item?.product_url) return item;
      try {
        const res = await fetch(
          `${PUBLIC_API_BASE}/api/listing?name=${encodeURIComponent(title)}`,
          { headers: { Accept: "application/json" } }
        );
        if (!res.ok) return item;
        const data = await res.json();
        const listing = data?.listing || {};
        const listingTitle = String(listing.title || data.title || "").trim();
        const titlesMatch =
          normalizeListingTitle(title) &&
          normalizeListingTitle(title) === normalizeListingTitle(listingTitle);
        if (!titlesMatch) return item;
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
  return `<a href="${escapeHtml(href)}" style="display:block;width:100%;box-sizing:border-box;margin:0 0 8px 0;padding:12px 14px;border:1px solid ${border};border-radius:999px;background:${bg};color:${color};font-size:13px;font-weight:800;text-decoration:none;letter-spacing:0.02em;text-align:center;">${escapeHtml(
    label
  )}</a>`;
}

function socialIconButton(href, label, iconUrl, { bg = "#1877F2", margin = "0 0 10px 0" } = {}) {
  return [
    `<a href="${escapeHtml(href)}" style="display:block;width:100%;max-width:100%;box-sizing:border-box;margin:${margin};text-decoration:none;">`,
    `<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border-collapse:separate;border-radius:999px;background:${bg};width:100%;">`,
    `<tr>`,
    `<td style="padding:11px 14px 11px 10px;vertical-align:middle;width:34px;">`,
    `<img src="${escapeHtml(iconUrl)}" width="22" height="22" alt="" style="display:block;width:22px;height:22px;border:0;border-radius:6px;" />`,
    `</td>`,
    `<td style="padding:11px 16px 11px 0;vertical-align:middle;color:#ffffff;font-size:14px;font-weight:800;letter-spacing:0.02em;line-height:22px;">${escapeHtml(
      label
    )}</td>`,
    `</tr>`,
    `</table>`,
    `</a>`,
  ].join("");
}

function socialOutlineButton(href, label, { color = NAVY, border = NAVY, margin = "0 0 10px 0" } = {}) {
  return [
    `<a href="${escapeHtml(href)}" style="display:block;width:100%;max-width:100%;box-sizing:border-box;margin:${margin};text-decoration:none;">`,
    `<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border-collapse:separate;border-radius:999px;background:#ffffff;border:1px solid ${border};width:100%;">`,
    `<tr>`,
    `<td style="padding:11px 18px;vertical-align:middle;color:${color};font-size:14px;font-weight:800;letter-spacing:0.02em;line-height:22px;text-align:center;">${escapeHtml(
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
    `<div style="margin:18px 0 0;padding:0;border:1px solid ${BORDER};border-radius:14px;overflow:hidden;background:${CREAM};">`,
    `<div style="padding:16px 16px 10px;text-align:center;">`,
    `<p style="margin:0 0 6px;font-size:11px;font-weight:800;letter-spacing:0.12em;text-transform:uppercase;color:${NAVY};text-align:center;">Follow Lost &amp; Found</p>`,
    `<p style="margin:0 auto 14px;max-width:420px;font-size:14px;line-height:1.55;color:#333;text-align:center;">${escapeHtml(
      intro
    )}</p>`,
    `</div>`,

    `<div style="margin:0 auto 12px;max-width:420px;padding:14px;background:#ffffff;border:1px solid ${BORDER};border-radius:12px;">`,
    `<p style="margin:0 0 4px;font-size:14px;font-weight:800;color:${NAVY};">Lost &amp; Found Resale</p>`,
    `<p style="margin:0 0 12px;font-size:12px;line-height:1.45;color:#666;">Furniture, finds, and daily drops from the Scottsdale showroom.</p>`,
    socialIconButton(SOCIAL.resaleFacebook, "Facebook", SOCIAL.iconFacebook, { bg: "#1877F2" }),
    socialIconButton(SOCIAL.resaleInstagram, "Instagram", SOCIAL.iconInstagram, {
      bg: "#dd2a7b",
      margin: "0",
    }),
    `</div>`,

    `<div style="margin:0 auto 16px;max-width:420px;padding:14px;background:#ffffff;border:1px solid ${BORDER};border-radius:12px;">`,
    `<p style="margin:0 0 4px;font-size:14px;font-weight:800;color:${NAVY};">Luxury division</p>`,
    `<p style="margin:0 0 12px;font-size:12px;line-height:1.45;color:#666;">Designer handbags and accessories.</p>`,
    socialOutlineButton(SOCIAL.luxuryShop, "Shop"),
    socialIconButton(SOCIAL.luxuryFacebook, "Facebook", SOCIAL.iconFacebook, {
      bg: "#1877F2",
    }),
    socialIconButton(SOCIAL.luxuryInstagram, "Instagram", SOCIAL.iconInstagram, {
      bg: "#dd2a7b",
      margin: "0",
    }),
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
    `<div style="background:#ffffff;padding:16px 22px 10px;border-bottom:1px solid ${BORDER};text-align:center;">`,
    `<a href="${BRAND.site}" style="text-decoration:none;"><img src="${BRAND.logo}" alt="Lost + Found Resale Interiors" width="280" style="display:block;margin:0 auto;width:280px;max-width:80%;height:auto;" /></a>`,
    `<p style="margin:8px 0 0;font-size:11px;letter-spacing:0.14em;text-transform:uppercase;color:${MUTED};text-align:center;">Delivery — Pickup — Freight</p>`,
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
    `<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><style type="text/css">@media only screen and (max-width:620px){.lf-email-pad{padding:16px 14px 22px!important}.lf-email-shell{padding:12px 8px!important}}</style></head><body style="margin:0;padding:0;-webkit-text-size-adjust:100%;">`,
    `<div class="lf-email-shell" style="font-family:${FONT};background:#f3f1ec;padding:24px 12px;">`,
    `<div style="max-width:880px;margin:0 auto;background:#ffffff;border:1px solid ${BORDER};border-radius:10px;overflow:hidden;">`,
    brandHeaderHtml(),
    `<div class="lf-email-pad" style="padding:22px 24px 28px;color:#222;line-height:1.55;">`,
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
          ["Estimate", `${money(localEstimate.estimated_price)} (Estimate)`],
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
          [
            "Rate",
            localEstimate.rate_label ||
              (localEstimate.hourly_rate
                ? `$${localEstimate.hourly_rate}/hour round-trip (not one way)`
                : "$95/hour round-trip (not one way)"),
          ],
          Number(localEstimate.stair_fee) > 0
            ? ["Stairs fee", money(localEstimate.stair_fee)]
            : null,
        ].filter(Boolean)
      : isLocal
        ? [["Estimate", "Pending | route unavailable / manual review"]]
        : (() => {
            const notes = [];
            if (hasOversizedPalletItems(submission.items || [])) {
              notes.push(
                "Item may be oversized for a standard 48×40 pallet — confirm fit / re-orient before final quote"
              );
            } else if (hasSmallerThanStandardPalletItems(submission.items || [])) {
              notes.push("Item is under a full 48×40 footprint — a smaller pallet may reduce cost");
            }
            notes.push("Confirm stackability with warehouse before final quote — do not assume from the form");
            notes.push(
              submission.access?.liftgate_delivery
                ? "Customer requested liftgate at delivery"
                : "No liftgate requested at delivery"
            );
            notes.push("Liftgate at Lost & Found pickup: assume yes until staff confirms");
            if (submission.access?.white_glove) notes.push("White Glove selected");
            if (submission.access?.warehouse_pickup) notes.push("Warehouse / terminal pickup selected");
            return [
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
              ["Staff checklist", notes.join(" · ")],
            ];
          })();

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
  const callOutRows = [...accessCompactRows];
  if (isLocal && Boolean(localEstimate?.oversize_confirm)) {
    callOutRows.unshift([
      "Confirm crew size",
      "Confirm crew size — size hit 3-person / $130/hr threshold; final may be lower if two people are enough (~$95/hr)",
    ]);
  } else if (isLocal && (Number(submission.access?.extra_people) || 0) >= 1) {
    const n = Number(submission.access.extra_people) || 0;
    callOutRows.unshift([
      "Confirm crew size",
      n >= 2
        ? "Confirm number of people / trucks — customer selected 2 extra people"
        : "Confirm number of people required — customer selected 1 extra person (3-person crew)",
    ]);
  }

  const oneWayMinutes =
    isLocal && Number.isFinite(Number(route?.drive_minutes ?? localEstimate?.drive_minutes))
      ? Math.ceil(Number(route?.drive_minutes ?? localEstimate.drive_minutes))
      : null;
  const roundTripMinutes = oneWayMinutes != null ? oneWayMinutes * 2 : null;
  const distanceMiles =
    route?.distance_miles != null && Number.isFinite(Number(route.distance_miles))
      ? Number(route.distance_miles)
      : null;
  const extraPeople = Number(submission.access?.extra_people) || 0;
  const extraCrew = Boolean(submission.access?.needs_more_than_two_people) || extraPeople > 0;
  const oversizeConfirm = Boolean(localEstimate?.oversize_confirm);
  const longHaulConfirm = Boolean(localEstimate?.long_haul);
  const estimateAmountLabel = oversizeConfirm
    ? "Preliminary estimate (size-based 3-person — confirm; final may be lower)"
    : longHaulConfirm
      ? "Best-guess estimate (out of town 100+ mi — callout)"
      : extraPeople >= 2
        ? "Your two-truck estimate"
        : extraPeople === 1 || extraCrew
          ? "Your 3-person crew estimate"
          : isPickup
            ? "Your preliminary consignor pickup estimate"
            : "Your preliminary local delivery estimate";

  const nationwideRangeText =
    !isLocal && nationwideRate?.range_low != null && nationwideRate?.range_high != null
      ? `${money(nationwideRate.range_low)} - ${money(nationwideRate.range_high)}`
      : null;
  const nationwideMiles =
    nationwideRate?.distance_miles ??
    nationwideRate?.route?.distance_miles ??
    route?.distance_miles ??
    null;
  const destState = submission.state || submission.delivery_address?.state || "";
  const destZip = submission.zip || submission.delivery_address?.zip || "";
  const outOfAz = !isLocal && !isArizonaStateOrZip({ state: destState, zip: destZip });

  const localPriceDisplay =
    isLocal && localEstimate?.estimated_price != null
      ? money(localEstimate.estimated_price)
      : null;

  const cruzTel = CRUZ_PRO_LINE_PHONE.replace(/\D/g, "");
  const shopTel = LOST_FOUND_PHONE.replace(/\D/g, "");

  function localRateConfirmHtml({ strong = false } = {}) {
    const border = strong ? "#c9a227" : "#d4cfc3";
    const bg = strong ? "#fff8e8" : "#fbfaf6";
    const intro = isPickup
      ? `This <strong>preliminary estimate</strong> is based on the information you submitted. A PDF of your submitted details is attached for your records. Contact <strong>${CRUZ_PRO_LINE_NAME}</strong> directly to confirm availability and <strong>final pricing</strong>, then let the shop know the confirmed pickup date by calling or emailing <a href="mailto:${LOST_FOUND_EMAIL}" style="color:${NAVY};font-weight:700;">${LOST_FOUND_EMAIL}</a>.`
      : `This <strong>preliminary estimate</strong> is based on the information you submitted. A PDF of your submitted details is attached for your records. Review the information for accuracy, then contact the delivery partner or Lost &amp; Found Resale to confirm availability and <strong>final pricing</strong>.`;
    const partnerKicker = isPickup ? "Confirm pricing" : "Delivery partner";
    const partnerName = CRUZ_PRO_LINE_NAME;
    const partnerSub = "";
    const shopKicker = isPickup ? "Notify the shop of your date" : "Call the shop";
    const shopExtra = isPickup
      ? `<p style="margin:4px 0 0;font-size:13px;font-weight:700;line-height:1.3;text-align:left;"><a href="mailto:${LOST_FOUND_EMAIL}" style="color:${NAVY};text-decoration:underline;">${LOST_FOUND_EMAIL}</a></p>`
      : "";
    return [
      `<div style="margin:10px 0 14px;padding:20px 22px;background:${bg};border:2px solid ${border};border-radius:12px;">`,
      `<p style="margin:0 0 10px;font-size:14px;font-weight:800;color:#8a6d12;letter-spacing:0.03em;text-transform:uppercase;">Next steps</p>`,
      `<p style="margin:0 0 16px;font-size:14px;line-height:1.5;color:#333;">${intro}</p>`,
      `<table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="width:100%;border-collapse:separate;border-spacing:0 12px;">`,
      `<tr><td style="padding:10px 12px;background:#ffffff;border:1px solid #e5e1d8;border-radius:10px;">`,
      `<table role="presentation" width="100%" cellpadding="0" cellspacing="0"><tr>`,
      `<td style="vertical-align:middle;width:64px;">`,
      `<a href="tel:+1${cruzTel}" style="text-decoration:none;"><img src="${BRAND.cruzLogo}" alt="${partnerName}" width="56" height="56" style="display:block;width:56px;height:56px;border-radius:8px;object-fit:cover;border:1px solid #e5e1d8;" /></a>`,
      `</td>`,
      `<td style="vertical-align:middle;padding-left:12px;text-align:left;">`,
      `<p style="margin:0 0 1px;font-size:10px;font-weight:800;letter-spacing:0.06em;text-transform:uppercase;color:#8a6d12;text-align:left;">${partnerKicker}</p>`,
      `<p style="margin:0;font-size:14px;font-weight:800;color:${NAVY};line-height:1.25;text-align:left;"><a href="tel:+1${cruzTel}" style="color:${NAVY};text-decoration:none;">${partnerName}</a></p>`,
      partnerSub,
      `<p style="margin:2px 0 0;font-size:15px;font-weight:800;line-height:1.2;text-align:left;"><a href="tel:+1${cruzTel}" style="color:${INK};text-decoration:underline;">${CRUZ_PRO_LINE_PHONE}</a></p>`,
      `</td></tr></table>`,
      `</td></tr>`,
      `<tr><td style="padding:10px 12px;background:#ffffff;border:1px solid #e5e1d8;border-radius:10px;">`,
      `<table role="presentation" width="100%" cellpadding="0" cellspacing="0"><tr>`,
      `<td style="vertical-align:middle;width:64px;">`,
      `<a href="${isPickup ? `mailto:${LOST_FOUND_EMAIL}` : `tel:+1${shopTel}`}" style="text-decoration:none;"><img src="${BRAND.storePhoto}" alt="Lost &amp; Found Scottsdale showroom" width="56" height="56" style="display:block;width:56px;height:56px;border-radius:8px;object-fit:cover;border:1px solid #e5e1d8;" /></a>`,
      `</td>`,
      `<td style="vertical-align:middle;padding-left:12px;text-align:left;">`,
      `<p style="margin:0 0 1px;font-size:10px;font-weight:800;letter-spacing:0.06em;text-transform:uppercase;color:#8a6d12;text-align:left;">${shopKicker}</p>`,
      `<p style="margin:0;font-size:14px;font-weight:800;color:${NAVY};line-height:1.25;text-align:left;"><a href="tel:+1${shopTel}" style="color:${NAVY};text-decoration:none;">Lost &amp; Found Resale</a></p>`,
      `<p style="margin:2px 0 0;font-size:15px;font-weight:800;line-height:1.2;text-align:left;"><a href="tel:+1${shopTel}" style="color:${INK};text-decoration:underline;">${LOST_FOUND_PHONE}</a></p>`,
      shopExtra,
      `</td></tr></table>`,
      `</td></tr>`,
      `</table>`,
      `</div>`,
    ].join("");
  }

  function localRateConfirmText() {
    if (isPickup) {
      return [
        "Next steps",
        `This preliminary estimate is based on the information you submitted. A PDF of your submitted details is attached for your records. Contact ${CRUZ_PRO_LINE_NAME} directly to confirm availability and final pricing, then let the shop know the confirmed pickup date by calling or emailing ${LOST_FOUND_EMAIL}.`,
        `${CRUZ_PRO_LINE_NAME}: ${CRUZ_PRO_LINE_PHONE}`,
        `Lost & Found Resale (notify of confirmed date): ${LOST_FOUND_PHONE} · ${LOST_FOUND_EMAIL}`,
      ].join("\n");
    }
    return [
      "Next steps",
      "This preliminary estimate is based on the information you submitted. A PDF of your submitted details is attached for your records. Review the information for accuracy, then contact the delivery partner or Lost & Found Resale to confirm availability and final pricing.",
      `Cruz Pro Line: ${CRUZ_PRO_LINE_PHONE}`,
      `Lost & Found Resale (shop): ${LOST_FOUND_PHONE}`,
    ].join("\n");
  }

  const confirmDueSizeHtml = oversizeConfirm
    ? `<p style="margin:0 0 10px;padding:12px 14px;background:#fff8e8;border-left:4px solid #c9a227;font-size:14px;line-height:1.55;color:#333;"><strong>Size-based 3-person estimate (confirm).</strong> This quote assumes three people may be needed because at least one item is 299+ lb and over 72&quot; high, or over 550 lb — even if you did not select extra crew. Final rates are often lower if the delivery team confirms two people are enough (typically closer to our $95/hour rate). <strong>Please confirm with the delivery team before scheduling. This is an estimate only.</strong></p>`
    : "";

  const confirmDueDistanceHtml = longHaulConfirm
    ? `<p style="margin:0 0 10px;padding:12px 14px;background:#fff8e8;border-left:4px solid #c9a227;font-size:14px;line-height:1.55;color:#333;"><strong>Out of town callout (100+ mile round trip).</strong> Longer Arizona trips can still work with our local truck, but confirm pricing before scheduling. <strong>This is an estimate only.</strong></p>`
    : "";

  // Shared customer hero: label + large bold amount (local, pickup, and nationwide).
  const estimateHtml = localPriceDisplay
    ? [
        `<p style="margin:0 0 4px;font-size:15px;line-height:1.4;color:#333;">${escapeHtml(
          estimateAmountLabel
        )}</p>`,
        `<p style="margin:0 0 8px;font-size:28px;line-height:1.15;font-weight:800;color:${NAVY};">Preliminary Estimate: ${escapeHtml(
          localPriceDisplay
        )}</p>`,
        confirmDueSizeHtml,
        confirmDueDistanceHtml,
      ].join("")
    : isLocal
      ? `<p style="margin:0 0 10px;font-size:15px;line-height:1.55;color:#333;">We received your ${
          isPickup ? "consignor pickup" : "local delivery"
        } request and will confirm timing and pricing with you shortly.</p>`
      : nationwideRangeText
        ? [
            `<p style="margin:0 0 6px;font-size:14px;line-height:1.4;color:${MUTED};letter-spacing:0.01em;">Your preliminary nationwide freight range</p>`,
            `<p style="margin:0 0 10px;font-size:32px;line-height:1.1;font-weight:800;color:${NAVY};">${escapeHtml(
              nationwideRangeText
            )}</p>`,
            `<p style="margin:0 0 14px;padding:10px 12px;background:#fff5f5;border-left:4px solid #c62828;border-radius:4px;font-size:14px;line-height:1.45;color:#c62828;font-weight:700;text-align:left;">Our team will review your request and follow up with confirmed rates and the best available options for you.</p>`,
          ].join("")
        : `<p style="margin:0 0 10px;font-size:15px;line-height:1.55;color:#333;">We received your nationwide freight request. Our team will review your request and follow up with confirmed rates and the best available options for you.</p>`;

  const includedWorkHtml = isPickup
    ? "Your estimated time is round-trip and includes preparation, wrapping, loading and securing, return to the showroom, unloading, and placement."
    : "Your estimated time is round-trip and includes wrapping and preparation, loading and securing, delivery, unloading, and placement.";
  const includedWorkText = includedWorkHtml.replace(/<[^>]+>/g, "");

  const nationwideExplainHtml = !isLocal
    ? [
        nationwideMiles != null
          ? `<p style="margin:0 0 14px;font-size:13px;line-height:1.45;color:${MUTED};">About <strong style="color:#333;">${nationwideMiles} miles</strong> from our Scottsdale showroom</p>`
          : "",
        `<div style="margin:0 0 14px;padding:14px 16px;background:${CREAM};border:1px solid ${BORDER};border-radius:8px;">`,
        `<p style="margin:0;font-size:14px;line-height:1.55;color:#444;">This preliminary range is based on the destination, estimated pallet size and weight, and the freight services selected. Final pricing may move up or down based on carrier capacity, confirmed dimensions, residential or terminal service, liftgate requirements, and available carrier options.</p>`,
        `</div>`,
        `<div style="margin:0 0 14px;padding:14px 16px;background:#fff;border:1px solid ${BORDER};border-left:4px solid ${NAVY};border-radius:8px;">`,
        `<p style="margin:0;font-size:14px;line-height:1.55;color:#333;"><strong style="color:${NAVY};">Freight savings tip:</strong> Picking up from a nearby freight terminal or warehouse instead of choosing residential delivery can significantly reduce the final shipping cost. Available pickup options will be included with your confirmed rates.</p>`,
        `</div>`,
        outOfAz
          ? [
              `<div style="margin:0 0 16px;padding:14px 16px;background:#f7fbf5;border:1px solid #cfe0c8;border-left:4px solid #2f5a32;border-radius:8px;">`,
              `<p style="margin:0 0 6px;font-size:14px;line-height:1.55;color:#333;"><strong style="color:#2f5a32;">Out-of-state purchase savings:</strong> When you purchase the item online and it is shipped outside Arizona, no Arizona sales tax is charged on the item purchase at checkout. That is approximately an 8% savings compared with an Arizona purchase.</p>`,
              `<p style="margin:0;font-size:12px;line-height:1.45;color:${MUTED};">This applies to the merchandise purchase. Freight and other service charges are separate.</p>`,
              `</div>`,
            ].join("")
          : "",
      ].join("")
    : "";

  const nationwideExplainText = !isLocal
    ? [
        nationwideMiles != null
          ? `About ${nationwideMiles} miles from our Scottsdale showroom.`
          : null,
        "This preliminary range is based on the destination, estimated pallet size and weight, and the freight services selected. Final pricing may move up or down based on carrier capacity, confirmed dimensions, residential or terminal service, liftgate requirements, and available carrier options.",
        "Freight savings tip: Picking up from a nearby freight terminal or warehouse instead of choosing residential delivery can significantly reduce the final shipping cost. Available pickup options will be included with your confirmed rates.",
        outOfAz
          ? "Out-of-state purchase savings: When you purchase the item online and it is shipped outside Arizona, no Arizona sales tax is charged on the item purchase at checkout. That is approximately an 8% savings compared with an Arizona purchase.\nThis applies to the merchandise purchase. Freight and other service charges are separate."
          : null,
      ]
        .filter(Boolean)
        .join("\n")
    : "";

  const driveExplainHtml =
    isLocal && oneWayMinutes != null
      ? [
          `<p style="margin:8px 0 4px;font-size:14px;font-weight:700;color:${NAVY};text-align:center;">Estimated drive time: about ${oneWayMinutes} minutes each way · ${roundTripMinutes} minutes round trip${
            distanceMiles != null ? ` · ${distanceMiles} miles each way` : ""
          }</p>`,
          `<p style="margin:0 0 8px;font-size:12px;line-height:1.45;color:${MUTED};text-align:center;">Distance and time are based on the mapped route, not live traffic.</p>`,
          Number(localEstimate?.multi_item_adder) > 0
            ? `<p style="margin:0 0 8px;font-size:13px;color:#444;text-align:center;">Includes multi-item handling for <strong>${escapeHtml(
                localEstimate.item_count
              )}</strong> items.</p>`
            : "",
        ].join("")
      : isLocal
        ? ""
        : "";

  const driveExplainText =
    isLocal && oneWayMinutes != null
      ? [
          `Estimated drive time: about ${oneWayMinutes} minutes each way · ${roundTripMinutes} minutes round trip${
            distanceMiles != null ? ` · ${distanceMiles} miles each way` : ""
          }`,
          "Distance and time are based on the mapped route, not live traffic.",
          Number(localEstimate?.multi_item_adder) > 0
            ? `Includes multi-item handling for ${localEstimate.item_count} items.`
            : null,
        ]
          .filter(Boolean)
          .join("\n")
      : isLocal
        ? ""
        : "";

  const laborLineHtml = isLocal
    ? `<p style="margin:8px 0;font-size:13px;line-height:1.5;color:#333;text-align:center;">${includedWorkHtml}</p>`
    : "";
  const laborLineText = isLocal ? includedWorkText : "";

  const confirmOnceHtml = isLocal
    ? `<p style="margin:8px 0 12px;font-size:13px;line-height:1.5;color:#333;text-align:center;"><strong>Confirm pricing before scheduling.</strong> Review your emailed PDF and make sure all item, delivery, and access details are accurate when you call.</p>`
    : "";
  const confirmOnceText = isLocal
    ? "Confirm pricing before scheduling. Review your emailed PDF and make sure all item, delivery, and access details are accurate when you call."
    : "";

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

  const nationwideBuyTipHtml = !isLocal && outOfAz
    ? `<p style="margin:10px 0 0;font-size:13px;line-height:1.5;color:#333;">Purchase online to secure the item and receive the out-of-state no-sales-tax benefit on the merchandise purchase.</p>`
    : "";

  function buildBuyItemsBlock({ includeTip = false, includeAddress = false, margin = "0 0 16px" } = {}) {
    if (isPickup) {
      if (!(submission.items || []).length) return "";
      return [
        `<div style="margin:${margin};">`,
        `<p style="margin:0 0 10px;font-size:14px;font-weight:800;color:${NAVY};">Your item(s)</p>`,
        customerItemsHtml(submission.items || [], { showBuyLinks: false }),
        includeAddress
          ? `<p style="margin:8px 0 0;font-size:12px;color:${MUTED};">Pickup: ${escapeHtml(addr)}</p>`
          : "",
        `</div>`,
      ].join("");
    }
    if (!(submission.items || []).length) return "";
    return [
      `<div style="margin:${margin};padding:16px;background:#fff;border:1px solid ${BORDER};border-radius:10px;">`,
      `<p style="margin:0 0 12px;font-size:14px;font-weight:800;color:${NAVY};">Buy your item(s)</p>`,
      customerItemsHtml(submission.items || [], { showBuyLinks: true }),
      includeTip ? nationwideBuyTipHtml : "",
      includeAddress
        ? `<p style="margin:10px 0 0;font-size:12px;color:${MUTED};">Delivery: ${escapeHtml(addr)}</p>`
        : "",
      `</div>`,
    ].join("");
  }

  const buyItemsAboveMapHtml = !isLocal
    ? buildBuyItemsBlock({ includeTip: true, includeAddress: false, margin: "0 0 18px" })
    : "";
  const buyItemsHtml = buildBuyItemsBlock({
    includeTip: false,
    includeAddress: true,
    margin: isLocal ? "10px 0 8px" : "18px 0 8px",
  });

  // Nationwide: compact contact only (tax / savings already shown above the product card).
  const buyPolicyHtml = !isLocal
    ? [
        `<div style="margin:14px 0;padding:14px 16px;background:${CREAM};border:1px solid ${BORDER};border-radius:8px;">`,
        `<p style="margin:0;font-size:14px;line-height:1.55;color:#333;">Questions? Reach Lost &amp; Found at <a href="mailto:${LOST_FOUND_EMAIL}" style="color:${INK};font-weight:700;">${LOST_FOUND_EMAIL}</a> or <a href="tel:${LOST_FOUND_PHONE.replace(
          /\D/g,
          ""
        )}" style="color:${INK};font-weight:700;">${LOST_FOUND_PHONE}</a>.</p>`,
        !buyItemsHtml && !buyItemsAboveMapHtml && buyNowHtml
          ? `<div style="margin-top:12px;">${buyNowHtml}${nationwideBuyTipHtml}</div>`
          : "",
        `</div>`,
      ].join("")
    : "";

  const buyPolicyText = !isLocal
    ? [
        "Questions? Reach Lost & Found at info@lostandfoundresale.com or 480-588-7006.",
        outOfAz
          ? "Purchase online to secure the item and receive the out-of-state no-sales-tax benefit on the merchandise purchase."
          : null,
        buyUrlsText || null,
      ]
        .filter(Boolean)
        .join("\n")
    : "";

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
    `<p style="margin:0 0 8px;padding-bottom:4px;border-bottom:2px solid ${ACCESS_RED};color:${ACCESS_RED};font-size:13px;font-weight:800;">Call out items</p>`,
    compactTableHtml(callOutRows, { valueColor: ACCESS_RED }),
    `</td>`,
    `</tr>`,
    `</table>`,
  ].join("");

  const localInternalNextStepsHtml = isLocal
    ? [
        `<div style="margin:0 0 14px;padding:14px 16px;background:#eef1ff;border:1px solid ${BORDER};border-left:4px solid ${NAVY};border-radius:8px;">`,
        `<p style="margin:0 0 4px;font-size:11px;font-weight:800;letter-spacing:0.1em;text-transform:uppercase;color:${NAVY};">Next steps</p>`,
        `<p style="margin:0;font-size:15px;line-height:1.5;font-weight:700;color:${INK};">${
          isPickup
            ? "Be on the lookout for final confirmation of when the item(s) are coming in."
            : `Confirm with delivery team and let client know. Print the <strong>Sign Form</strong> PDF (attached) for customer signature &amp; initials on delivery day.`
        }</p>`,
        `</div>`,
      ].join("")
    : "";

  const stackConfirmItems =
    !isLocal
      ? (submission.items || [])
          .map((it) => ({ it, need: describeStackConfirmNeed(it) }))
          .filter((row) => row.need)
      : [];
  const hasCustomerQty = stackConfirmItems.some((row) => row.need.kind === "customer_qty");
  const hasSetListing = stackConfirmItems.some((row) => row.need.kind === "set_listing");
  const stackIntroHtml = hasCustomerQty
    ? hasSetListing
      ? `At least one line has quantity 2+ (customer flip/stack answer) and/or a multi-piece set listing (qty 1). <strong>Do not book final freight until the warehouse confirms packing.</strong>`
      : `Customer selected quantity 2+ and answered whether pieces can be flipped/stacked. <strong>Do not book final freight until the warehouse confirms packing.</strong>`
    : hasSetListing
      ? `This listing is a multi-piece set with quantity 1 — the customer was <strong>not</strong> asked about flip/stack. Confirm the nested/stack packing plan before booking final freight.`
      : `Warehouse must confirm packing before final freight rates.`;
  const stackIntroText = hasCustomerQty
    ? hasSetListing
      ? "Quantity 2+ and/or multi-piece set listing. Do not book final freight until warehouse confirms packing."
      : "Customer selected quantity 2+. Do not book final freight until warehouse confirms packing."
    : hasSetListing
      ? "Multi-piece set with qty 1 (customer was not asked about flip/stack). Confirm packing before final freight."
      : "Warehouse must confirm packing before final freight rates.";

  const stackConfirmHtml = stackConfirmItems.length
    ? [
        `<div style="margin:0 0 14px;padding:14px 16px;background:#fff8e8;border:2px solid #c9a227;border-left:4px solid #c0392b;border-radius:8px;">`,
        `<p style="margin:0 0 6px;font-size:12px;font-weight:800;letter-spacing:0.08em;text-transform:uppercase;color:#8a6d12;">Warehouse warning — confirm stacking</p>`,
        `<p style="margin:0 0 8px;font-size:14px;line-height:1.5;color:#333;">${stackIntroHtml}</p>`,
        `<ul style="margin:0;padding-left:18px;font-size:13px;line-height:1.5;color:#333;">`,
        ...stackConfirmItems.map(({ it, need }) => {
          const detail =
            need.kind === "set_listing"
              ? `qty ${need.quantity} · set of ${need.set_count} · customer not asked (set listing)`
              : need.kind === "customer_qty"
                ? `qty ${need.quantity}${need.set_count > 1 ? ` · set of ${need.set_count}` : ""} · customer answer: <strong>${escapeHtml(
                    need.claim || "unanswered"
                  )}</strong>`
                : `qty ${need.quantity} · packing confirm`;
          return `<li style="margin:0 0 4px;"><strong>${escapeHtml(it.title || "Item")}</strong> · ${detail}${
            it.pallet?.packing_notes?.length
              ? ` — ${escapeHtml(it.pallet.packing_notes.join(" "))}`
              : ""
          }</li>`;
        }),
        `</ul>`,
        `</div>`,
      ].join("")
    : "";

  const stackConfirmText = stackConfirmItems.length
    ? [
        "WAREHOUSE WARNING — CONFIRM STACKING",
        stackIntroText,
        ...stackConfirmItems.map(({ it, need }) => {
          if (need.kind === "set_listing") {
            return `- ${it.title || "Item"} · qty ${need.quantity} · set of ${need.set_count} · customer not asked (set listing)`;
          }
          if (need.kind === "customer_qty") {
            return `- ${it.title || "Item"} · qty ${need.quantity} · customer answer: ${need.claim || "unanswered"}`;
          }
          return `- ${it.title || "Item"} · qty ${need.quantity} · packing confirm`;
        }),
        "",
      ].join("\n")
    : "";

  const localInternalNextStepsText = isLocal
    ? [
        "Next steps",
        isPickup
          ? "Be on the lookout for final confirmation of when the item(s) are coming in."
          : "Confirm with delivery team and let client know. Print the Sign Form PDF (attached) for customer signature & initials on delivery day.",
        "",
      ].join("\n")
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
      stackConfirmHtml,
      `<p style="margin:0 0 12px;font-size:12px;color:${MUTED};">{{PDF_NOTICE}}</p>`,

      internalSnapshotHtml,
      twoColHtml,

      `<p style="margin:0 0 8px;padding-bottom:4px;border-bottom:2px solid ${NAVY};color:${NAVY};font-size:13px;font-weight:800;">Items</p>`,
      internalItemsHtml(submission.items || [], { showPrice: !isPickup }),

      !isLocal ? nationwideInternalPlaybookHtml(submission) : "",
    ].join("")
  );

  const internalText = [
    internalSubject,
    `Request ID: ${requestId}`,
    `Submitted: ${submittedAt}`,
    "",
    localInternalNextStepsText || null,
    stackConfirmText || null,
    snapshotLine || pathLabel,
    `Address: ${addr}`,
    `Customer: ${submission.customer_name} | ${formatPhoneForEmail(submission.customer_phone)} | ${submission.customer_email}`,
    mapRoute?.directions_url ? `Map: ${mapRoute.directions_url}` : null,
    extraReviewReasons.length ? `Review: ${extraReviewReasons.join("; ")}` : null,
    "",
    "REQUEST",
    ...requestCompactRows.map(([k, v]) => `${k}: ${v}`),
    "",
    "CALL OUT ITEMS",
    ...(callOutRows.length
      ? callOutRows.map(([k, v]) => `${k}: ${v}`)
      : ["None noted"]),
    itemsText(submission.items),
    !isLocal ? "" : null,
    !isLocal ? nationwideInternalPlaybookText(submission) : null,
  ]
    .filter((l) => l != null)
    .join("\n");

  const oversizedNote = !isLocal && hasOversizedPalletItems(submission.items || []);
  const oversizedHtml = oversizedNote
    ? `<div style="margin:14px 0;padding:14px 16px;background:#fff8e8;border:1px solid #e6d7a8;border-radius:8px;">
        <p style="margin:0;font-size:14px;line-height:1.55;color:#333;"><strong>Oversized for a standard 48″×40″ pallet.</strong> We may be able to adjust the item orientation or pallet configuration to reduce the final freight cost. Our team will review the details before requesting confirmed rates.</p>
      </div>`
    : "";
  const oversizedText = oversizedNote
    ? "Oversized for a standard 48x40 pallet. We may be able to adjust the item orientation or pallet configuration to reduce the final freight cost. Our team will review the details before requesting confirmed rates."
    : null;

  const nationwideNextStepsHtml = isLocal
    ? ""
    : [
        `<div style="margin:18px 0 0;padding:16px 18px;background:${CREAM};border:1px solid ${BORDER};border-radius:8px;">`,
        `<p style="margin:0 0 10px;font-size:14px;font-weight:800;color:${INK};">Freight details</p>`,
        submission.access?.white_glove
          ? `<p style="margin:0 0 10px;font-size:14px;line-height:1.55;color:#333;"><strong>White Glove selected</strong> — that raises the quote versus standard threshold / curbside freight.</p>`
          : "",
        `<p style="margin:0 0 10px;font-size:14px;line-height:1.55;color:#333;"><strong>Liftgate at delivery:</strong> ${
          submission.access?.liftgate_delivery ? "Yes" : "No"
        } (your choice). <strong>Pickup loading:</strong> Lost &amp; Found will determine if a liftgate is needed when we send your confirmed rates.</p>`,
        `<p style="margin:0;font-size:14px;line-height:1.55;color:#333;"><strong>Pickup / loading hours:</strong> 10am–5pm, Monday–Saturday.</p>`,
        `</div>`,
      ].join("");

  const nationwideNextStepsText = isLocal
    ? null
    : [
        "Freight details",
        submission.access?.white_glove
          ? "White Glove selected — that raises the quote versus standard threshold / curbside freight."
          : null,
        `Liftgate at delivery: ${
          submission.access?.liftgate_delivery ? "Yes" : "No"
        } (your choice). Pickup loading: Lost & Found will determine if a liftgate is needed when we send your confirmed rates.`,
        "Pickup / loading hours: 10am–5pm, Monday–Saturday.",
      ]
        .filter(Boolean)
        .join("\n");

  const customerHtml = wrapEmail(
    [
      `<h2 style="margin:0 0 14px;color:${NAVY};font-size:26px;line-height:1.25;font-weight:800;">${greetingHtml}</h2>`,
      estimateHtml,
      isLocal ? localRateConfirmHtml({ strong: true }) : "",
      isLocal ? driveExplainHtml : nationwideExplainHtml,
      oversizedHtml,
      buyItemsAboveMapHtml,
      routeMapHtml(mapRoute, {
        caption: isPickup ? "Your pickup route" : "Your delivery route",
      }),
      laborLineHtml,
      confirmOnceHtml,
      buyItemsHtml,
      buyPolicyHtml,
      nationwideNextStepsHtml,
      socialFollowHtml({ isPickup }),
      `<p style="margin:16px 0 0;font-size:12px;color:#999;">Reference: ${escapeHtml(
        requestId
      )} · Lost &amp; Found Resale · Scottsdale · lostandfoundresale.com</p>`,
    ].join(""),
    { includeShopLinks: true }
  );

  const customerText = [
    greetingText,
    "",
    localPriceDisplay
      ? `${estimateAmountLabel}\nPreliminary Estimate: ${localPriceDisplay}${
          oversizeConfirm
            ? "\nSize-based 3-person estimate (confirm). This quote assumes three people may be needed due to item size/weight (299+ lb and over 72\" H, or over 550 lb) — even if you did not select extra crew. Final rates are often lower if two people are enough (typically closer to $95/hour). Confirm with the delivery team before scheduling. ESTIMATE ONLY."
            : ""
        }${
          longHaulConfirm
            ? "\nOut of town callout (100+ mile round trip). Longer Arizona trips can still work with our local truck — confirm pricing before scheduling. ESTIMATE ONLY."
            : ""
        }`
      : isLocal
        ? `We received your ${isPickup ? "consignor pickup" : "local delivery"} request and will confirm timing and pricing shortly.`
        : nationwideRangeText
          ? `Your preliminary nationwide freight range\n${nationwideRangeText}\nOur team will review your request and follow up with confirmed rates and the best available options for you.`
          : "We received your nationwide freight request. Our team will review your request and follow up with confirmed rates and the best available options for you.",
    isLocal ? localRateConfirmText() : null,
    isLocal ? driveExplainText || null : nationwideExplainText || null,
    oversizedText,
    !isLocal && !isPickup
      ? [
          "",
          "Buy your item(s):",
          customerItemsText(submission.items || [], { showBuyLinks: true }),
          outOfAz
            ? "Purchase online to secure the item and receive the out-of-state no-sales-tax benefit on the merchandise purchase."
            : null,
        ]
          .filter(Boolean)
          .join("\n")
      : null,
    mapRoute?.directions_url ? `Route map: ${mapRoute.directions_url}` : null,
    laborLineText || null,
    confirmOnceText || null,
    "",
    isPickup ? "Your item(s):" : "Buy your item(s):",
    customerItemsText(submission.items || [], { showBuyLinks: !isPickup }),
    `${isPickup ? "Pickup" : "Delivery"}: ${addr}`,
    "",
    buyPolicyText || null,
    nationwideNextStepsText,
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

function applyPdfNotice({ html, text, attached, downloadUrl, filename, signFormAttached = false }) {
  let noticeHtml = "";
  let noticeText = "";

  // Local delivery: internal team gets the sign form only (no separate summary PDF).
  if (signFormAttached && !attached && !downloadUrl) {
    noticeHtml =
      "<strong>Sign Form PDF</strong> is attached — one-page delivery estimate for print, with route map, customer signature &amp; initials.";
    noticeText =
      "Sign Form PDF attached — print for customer signature & initials on delivery day (includes route map).";
  } else if (attached && downloadUrl) {
    noticeHtml = `Printable summary PDF is attached, and also available to download: <a href="${escapeHtml(
      downloadUrl
    )}" style="color:${NAVY};font-weight:700;">${escapeHtml(filename || "Download PDF")}</a>.`;
    noticeText = `Printable summary PDF attached. Download: ${downloadUrl}`;
  } else if (attached) {
    noticeHtml = "Printable summary PDF with the full request is attached.";
    noticeText = "Printable summary PDF with the full request is attached.";
  } else if (downloadUrl) {
    noticeHtml = `Printable summary PDF: <a href="${escapeHtml(
      downloadUrl
    )}" style="color:${NAVY};font-weight:700;">${escapeHtml(
      filename || "Download PDF"
    )}</a> (AWS backup link).`;
    noticeText = `Printable summary PDF download: ${downloadUrl}`;
  } else {
    noticeHtml = "Printable summary PDF was not generated for this request.";
    noticeText = "Printable summary PDF was not generated for this request.";
  }

  if (signFormAttached && (attached || downloadUrl)) {
    noticeHtml +=
      " <strong>Sign Form PDF</strong> is also attached — one-page branded delivery estimate for print, with route map, customer signature &amp; initials.";
    noticeText +=
      "\nSign Form PDF also attached — print for customer signature & initials on delivery day (includes route map).";
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
  let customerPdfBuffer = null;
  let customerPdfFilename = buildFreightQuotePdfFilename(enrichedSubmission, ctx, {
    audience: "customer",
  });
  const isLocalDelivery = enrichedSubmission.delivery_path === "local_az";

  // Local delivery: internal team only needs the sign form (skip internal summary PDF).
  if (!isLocalDelivery) {
    try {
      pdfBuffer = await generateFreightQuotePdf(enrichedSubmission, ctx, {
        audience: "internal",
      });
      if (!pdfBuffer?.length) pdfBuffer = null;
    } catch (pdfErr) {
      console.error(
        "[freight-quote] internal PDF generation failed (continuing):",
        pdfErr?.message || pdfErr
      );
      pdfBuffer = null;
    }
  }
  try {
    customerPdfBuffer = await generateFreightQuotePdf(enrichedSubmission, ctx, {
      audience: "customer",
    });
    if (!customerPdfBuffer?.length) customerPdfBuffer = null;
  } catch (pdfErr) {
    console.error(
      "[freight-quote] customer PDF generation failed (continuing):",
      pdfErr?.message || pdfErr
    );
    customerPdfBuffer = null;
  }

  let signPdfBuffer = null;
  let signPdfFilename = buildLocalSignPdfFilename(enrichedSubmission, ctx);
  if (isLocalDelivery) {
    try {
      signPdfBuffer = await generateLocalSignPdf(enrichedSubmission, ctx);
      if (!signPdfBuffer?.length) signPdfBuffer = null;
    } catch (pdfErr) {
      console.error(
        "[freight-quote] local sign PDF generation failed (continuing):",
        pdfErr?.message || pdfErr
      );
      signPdfBuffer = null;
    }
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
    signFormAttached: Boolean(signPdfBuffer),
  });

  const replyTo = built.internal.replyTo;
  const subject = built.internal.subject;
  const attachments = [];
  if (pdfBuffer) {
    attachments.push({
      filename: pdfFilename,
      content: pdfBuffer,
      contentType: "application/pdf",
    });
  }
  if (signPdfBuffer) {
    attachments.push({
      filename: signPdfFilename,
      content: signPdfBuffer,
      contentType: "application/pdf",
    });
  }

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
      signFormAttached: false,
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

  let customer;
  let customerPdfAttached = false;
  const customerTo = built.customer.to;
  try {
    if (customerPdfBuffer) {
      const sent = await sendEmailWithAttachments({
        to: customerTo,
        subject: built.customer.subject,
        html: built.customer.html,
        text: built.customer.text,
        replyTo: LOST_FOUND_EMAIL,
        attachments: [
          {
            filename: customerPdfFilename,
            content: customerPdfBuffer,
            contentType: "application/pdf",
          },
        ],
      });
      customer = {
        to: Array.isArray(customerTo) ? customerTo : [customerTo],
        id: String(sent?.data?.id || sent?.id || ""),
      };
      customerPdfAttached = true;
    } else {
      customer = await sendEmail({
        to: customerTo,
        subject: built.customer.subject,
        html: built.customer.html,
        text: built.customer.text,
        replyTo: LOST_FOUND_EMAIL,
      });
    }
  } catch (customerAttachErr) {
    console.error(
      "[freight-quote] customer email with PDF failed, retrying without attachment:",
      customerAttachErr?.message || customerAttachErr
    );
    customer = await sendEmail({
      to: customerTo,
      subject: built.customer.subject,
      html: built.customer.html,
      text: built.customer.text,
      replyTo: LOST_FOUND_EMAIL,
    });
    customerPdfAttached = false;
  }
  return {
    internal: {
      sent: true,
      to: internalTo,
      resend_id: internalId,
      pdf_attached: pdfAttached,
      sign_pdf_attached: Boolean(signPdfBuffer) && pdfAttached,
      pdf_s3_url: s3Url || null,
      pdf_s3_fallback: usedS3Fallback,
      pdf_s3: s3Meta,
    },
    customer: {
      sent: true,
      to: customer.to,
      resend_id: customer.id,
      pdf_attached: customerPdfAttached,
    },
  };
}
