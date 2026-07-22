/**
 * Lost & Found FreightCenter Quote SOP — palletize item dims/weight for LTL.
 * Pallet footprint 48x40 unless oversized; height +5"; weight +30 lb; class 150/175.
 */

export const STANDARD_PALLET_W = 48;
export const STANDARD_PALLET_D = 40;
export const PALLET_HEIGHT_IN = 5;
export const PALLET_WEIGHT_LB = 30;
export const FREIGHTCENTER_PHONE = "800-716-7608";
export const LOCAL_AZ_HOURLY_RATE = 95;

/** Rough AZ ZIP range used for "local Arizona delivery" messaging. */
export function isArizonaDestination({ state, zip } = {}) {
  const st = String(state || "").trim().toUpperCase();
  if (st === "AZ" || st === "ARIZONA") return true;
  const z = String(zip || "").replace(/\D/g, "").slice(0, 5);
  if (!/^\d{5}$/.test(z)) return false;
  const n = Number(z);
  return n >= 85001 && n <= 86556;
}

function num(value) {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? n : null;
}

function roundUpInches(value) {
  const n = num(value);
  if (n == null) return null;
  return Math.ceil(n);
}

/**
 * Infer freight class from SOP examples (default 150; glass/tables/consoles 175).
 */
export function inferFreightClass(item = {}) {
  if (item.freightClass != null && Number(item.freightClass) > 0) {
    return Number(item.freightClass);
  }
  const blob = [
    item.title,
    item.itemName,
    item.category,
    item.notes,
    item.nonStackable ? "fragile glass" : "",
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();

  if (
    /\b(glass\s*top|glass\s+desk|mirror|mirrored)\b/.test(blob) ||
    /\b(console|dining\s+table|dining\s+tables|coffee\s+table)\b/.test(blob)
  ) {
    return 175;
  }
  return 150;
}

export function shouldMarkNonStackable(item = {}) {
  if (item.nonStackable === true || item.nonStackable === "true" || item.nonStackable === "yes") {
    return true;
  }
  if (item.nonStackable === false || item.nonStackable === "false" || item.nonStackable === "no") {
    return false;
  }
  const blob = [item.title, item.itemName, item.category, item.notes].filter(Boolean).join(" ").toLowerCase();
  return /\b(glass|mirror|artwork|wicker|cane|rattan|antique|fragile|marble\s+top)\b/.test(blob);
}

/**
 * Parse W x D x H from a title suffix like "68x20x36H" when listing has no structured dims.
 */
export function parseDimsFromTitle(title) {
  const t = String(title || "");
  let m = t.match(/(\d+(?:\.\d+)?)\s*[xX×]\s*(\d+(?:\.\d+)?)\s*[xX×]\s*(\d+(?:\.\d+)?)\s*H?\b/);
  if (m) {
    const width = num(m[1]);
    const depth = num(m[2]);
    const height = num(m[3]);
    if (width && depth && height) return { widthIn: width, depthIn: depth, heightIn: height };
  }
  m = t.match(/(\d+(?:\.\d+)?)\s*[xX×]\s*(\d+(?:\.\d+)?)\s*H\b/i);
  if (m) {
    const a = num(m[1]);
    const b = num(m[2]);
    if (a && b) return { widthIn: a, depthIn: Math.min(a, b), heightIn: b };
  }
  return null;
}

/**
 * @param {{ widthIn?: number, depthIn?: number, heightIn?: number, weightLb?: number, quantity?: number, stackedHeightIn?: number, title?: string }} item
 */
export function palletizeItem(item = {}) {
  const qty = Math.max(1, Math.floor(num(item.quantity) || 1));
  let widthIn = num(item.widthIn);
  let depthIn = num(item.depthIn);
  let heightIn = num(item.stackedHeightIn) || num(item.heightIn);
  let weightLb = num(item.weightLb);

  if ((widthIn == null || depthIn == null || heightIn == null) && item.title) {
    const parsed = parseDimsFromTitle(item.title);
    if (parsed) {
      widthIn = widthIn ?? parsed.widthIn;
      depthIn = depthIn ?? parsed.depthIn;
      heightIn = heightIn ?? parsed.heightIn;
    }
  }

  const missing = [];
  if (widthIn == null) missing.push("width");
  if (depthIn == null) missing.push("depth");
  if (heightIn == null) missing.push("height");
  if (weightLb == null) missing.push("weight");

  if (missing.length) {
    return {
      ok: false,
      missing,
      quantity: qty,
      product: { widthIn, depthIn, heightIn, weightLb },
      freight: null,
      freightClass: inferFreightClass(item),
      nonStackable: shouldMarkNonStackable(item),
    };
  }

  // SOP: 48x40 if it fits; only increase the side that exceeds.
  let freightW = STANDARD_PALLET_W;
  let freightD = STANDARD_PALLET_D;
  if (widthIn > STANDARD_PALLET_W) freightW = roundUpInches(widthIn);
  if (depthIn > STANDARD_PALLET_D) freightD = roundUpInches(depthIn);

  // If the item is oriented with the long edge as depth, allow swapping so "oversized side" is fair.
  // Prefer mapping larger product face onto W when both exceed — keep simple: use as entered.
  const freightH = roundUpInches(heightIn + PALLET_HEIGHT_IN);
  const freightWeight = Math.ceil(weightLb * qty + PALLET_WEIGHT_LB);

  return {
    ok: true,
    missing: [],
    quantity: qty,
    product: {
      widthIn,
      depthIn,
      heightIn,
      weightLb,
    },
    freight: {
      widthIn: freightW,
      depthIn: freightD,
      heightIn: freightH,
      weightLb: freightWeight,
      packaging: "Pallet(s)",
      mode: "LTL",
    },
    freightClass: inferFreightClass(item),
    nonStackable: shouldMarkNonStackable(item),
    notes: [
      widthIn <= STANDARD_PALLET_W && depthIn <= STANDARD_PALLET_D
        ? `Fits standard ${STANDARD_PALLET_W}" x ${STANDARD_PALLET_D}" pallet footprint`
        : `Oversized: used ${freightW}" W x ${freightD}" D (only enlarged sides over ${STANDARD_PALLET_W}"/${STANDARD_PALLET_D}")`,
      `Height = item ${heightIn}" + ${PALLET_HEIGHT_IN}" pallet`,
      `Weight = product ${weightLb} lb x ${qty} + ${PALLET_WEIGHT_LB} lb pallet`,
    ],
  };
}

export function palletizeItems(items = []) {
  return (Array.isArray(items) ? items : []).map((item, index) => {
    const result = palletizeItem(item);
    return {
      index: index + 1,
      title: String(item.title || item.itemName || `Item ${index + 1}`).trim(),
      price: item.price != null ? String(item.price) : "",
      source: item.source || (item.lookedUp ? "listing_lookup" : "manual"),
      productUrl: item.productUrl || "",
      ...result,
    };
  });
}
