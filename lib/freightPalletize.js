/**
 * Lost & Found FreightCenter Quote SOP — server-side palletize (never trust browser alone).
 */

export const SHOWROOM_ORIGIN =
  "15530 N Greenway Hayden Loop Ste 100, Scottsdale, AZ 85260";
export const STANDARD_PALLET_W = 48;
export const STANDARD_PALLET_D = 40;
export const PALLET_HEIGHT_IN = 5;
export const PALLET_WEIGHT_LB = 30;
export const FREIGHTCENTER_PHONE = "800-716-7608";
export const LOST_FOUND_PHONE = "480-588-7006";
export const LOST_FOUND_EMAIL = "info@lostandfoundresale.com";
export const LOCAL_AZ_BASE_PRICE = 95;
export const LOCAL_AZ_BASE_MINUTES = 20;
export const LOCAL_AZ_BLOCK_MINUTES = 8;
export const LOCAL_AZ_BLOCK_PRICE = 15;
export const MAX_FREIGHT_ITEMS = 10;
export const MAX_DIM_IN = 240;
export const MAX_WEIGHT_LB = 5000;
export const MAX_STAIR_FLIGHTS = 20;

export function isArizonaStateOrZip({ state, zip } = {}) {
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

/** SOP / Webflow calculator: oversized sides round up to the next 5" (e.g. 55.6 → 60). */
function roundUpTo5(value) {
  const n = num(value);
  if (n == null) return null;
  return Math.ceil(n / 5) * 5;
}

export function inferFreightClass(item = {}) {
  if (item.freight_class != null && item.freight_class !== "" && Number(item.freight_class) > 0) {
    return Number(item.freight_class);
  }
  if (item.freightClass != null && item.freightClass !== "" && Number(item.freightClass) > 0) {
    return Number(item.freightClass);
  }
  const blob = [item.title, item.category, item.notes].filter(Boolean).join(" ").toLowerCase();
  if (
    /\b(glass\s*top|glass\s+desk|mirror|mirrored)\b/.test(blob) ||
    /\b(console|dining\s+table|dining\s+tables|coffee\s+table)\b/.test(blob)
  ) {
    return 175;
  }
  return 150;
}

/** Explicit class from customer, or null when "Not sure — Lost & Found will confirm". */
export function resolveFreightClass(item = {}) {
  const raw = item.freight_class ?? item.freightClass;
  if (raw === null || raw === undefined || raw === "") return null;
  const n = Number(raw);
  return Number.isFinite(n) && n > 0 ? n : null;
}

export function shouldMarkNonStackable(item = {}) {
  if (
    item.non_stackable === true ||
    item.nonStackable === true ||
    item.non_stackable === "true" ||
    item.non_stackable === "yes"
  ) {
    return true;
  }
  if (
    item.non_stackable === false ||
    item.nonStackable === false ||
    item.non_stackable === "false" ||
    item.non_stackable === "no"
  ) {
    return false;
  }
  const blob = [item.title, item.category, item.notes].filter(Boolean).join(" ").toLowerCase();
  return /\b(glass|mirror|artwork|wicker|cane|rattan|antique|fragile|marble|stone|sculpture|high-?end)\b/.test(
    blob
  );
}

export function parseDimsFromTitle(title) {
  const t = String(title || "");
  let m = t.match(/(\d+(?:\.\d+)?)\s*[xX×]\s*(\d+(?:\.\d+)?)\s*[xX×]\s*(\d+(?:\.\d+)?)\s*H?\b/);
  if (m) {
    const width = num(m[1]);
    const depth = num(m[2]);
    const height = num(m[3]);
    if (width && depth && height) return { width, depth, height };
  }
  m = t.match(/(\d+(?:\.\d+)?)\s*[xX×]\s*(\d+(?:\.\d+)?)\s*H\b/i);
  if (m) {
    const a = num(m[1]);
    const b = num(m[2]);
    if (a && b) return { width: a, depth: Math.min(a, b), height: b };
  }
  return null;
}

/**
 * Recalculate SOP freight entry for one item. One pallet entry per item (no fake consolidation).
 */
export function palletizeItem(item = {}) {
  const qty = Math.max(1, Math.min(99, Math.floor(num(item.quantity) || 1)));
  let width = num(item.width ?? item.widthIn);
  let depth = num(item.depth ?? item.depthIn ?? item.length);
  let height = num(item.height ?? item.heightIn ?? item.stacked_height ?? item.stackedHeightIn);
  let weight = num(item.weight ?? item.weightLb);

  if ((width == null || depth == null || height == null) && item.title) {
    const parsed = parseDimsFromTitle(item.title);
    if (parsed) {
      width = width ?? parsed.width;
      depth = depth ?? parsed.depth;
      height = height ?? parsed.height;
    }
  }

  const missing = [];
  if (width == null) missing.push("width");
  if (depth == null) missing.push("depth");
  if (height == null) missing.push("height");
  if (weight == null) missing.push("weight");

  const freightClass = resolveFreightClass(item);
  const suggestedFreightClass = freightClass ?? inferFreightClass({ ...item, freight_class: undefined });
  const nonStackable = shouldMarkNonStackable(item);

  if (missing.length) {
    return {
      ok: false,
      missing,
      quantity: qty,
      product: { width, depth, height, weight },
      pallet: null,
      freight_class: freightClass,
      suggested_freight_class: suggestedFreightClass,
      non_stackable: nonStackable,
    };
  }

  if (width > MAX_DIM_IN || depth > MAX_DIM_IN || height > MAX_DIM_IN) {
    return {
      ok: false,
      missing: ["dimensions_out_of_range"],
      quantity: qty,
      product: { width, depth, height, weight },
      pallet: null,
      freight_class: freightClass,
      suggested_freight_class: suggestedFreightClass,
      non_stackable: nonStackable,
    };
  }
  if (weight > MAX_WEIGHT_LB) {
    return {
      ok: false,
      missing: ["weight_out_of_range"],
      quantity: qty,
      product: { width, depth, height, weight },
      pallet: null,
      freight_class: freightClass,
      suggested_freight_class: suggestedFreightClass,
      non_stackable: nonStackable,
    };
  }

  let freightW = STANDARD_PALLET_W;
  let freightD = STANDARD_PALLET_D;
  if (width > STANDARD_PALLET_W) freightW = roundUpTo5(width);
  if (depth > STANDARD_PALLET_D) freightD = roundUpTo5(depth);

  const freightH = roundUpInches(height + PALLET_HEIGHT_IN);
  const freightWeight = Math.ceil(weight * qty + PALLET_WEIGHT_LB);

  const pallet = {
    width: freightW,
    depth: freightD,
    height: freightH,
    weight: freightWeight,
    freight_class: freightClass,
    suggested_freight_class: suggestedFreightClass,
    packaging_type: "Pallet(s)",
    non_stackable: nonStackable,
  };

  return {
    ok: true,
    missing: [],
    quantity: qty,
    product: { width, depth, height, weight },
    pallet,
    freight_class: freightClass,
    suggested_freight_class: suggestedFreightClass,
    non_stackable: nonStackable,
  };
}

export function palletizeItems(items = []) {
  return (Array.isArray(items) ? items : []).map((item, index) => {
    const result = palletizeItem(item);
    const priceNum = Number(item.price);
    return {
      index: index + 1,
      source: String(item.source || "manual").trim() || "manual",
      title: String(item.title || `Item ${index + 1}`).trim(),
      price: Number.isFinite(priceNum) ? priceNum : item.price || 0,
      product_url: String(item.product_url || item.productUrl || "").trim(),
      quantity: result.quantity,
      width: result.product?.width ?? null,
      depth: result.product?.depth ?? null,
      height: result.product?.height ?? null,
      weight: result.product?.weight ?? null,
      freight_class: result.freight_class,
      suggested_freight_class: result.suggested_freight_class,
      non_stackable: result.non_stackable,
      pallet: result.pallet,
      ok: result.ok,
      missing: result.missing,
    };
  });
}

/** Local AZ preliminary route price from one-way drive minutes. */
export function calculateLocalRouteEstimate(driveMinutes) {
  const mins = Number(driveMinutes);
  if (!Number.isFinite(mins) || mins < 0) return null;
  const extraBlocks = Math.max(0, Math.ceil((mins - LOCAL_AZ_BASE_MINUTES) / LOCAL_AZ_BLOCK_MINUTES));
  const estimatedPrice = LOCAL_AZ_BASE_PRICE + extraBlocks * LOCAL_AZ_BLOCK_PRICE;
  return {
    base_minutes: LOCAL_AZ_BASE_MINUTES,
    base_price: LOCAL_AZ_BASE_PRICE,
    block_minutes: LOCAL_AZ_BLOCK_MINUTES,
    block_price: LOCAL_AZ_BLOCK_PRICE,
    drive_minutes: Math.round(mins),
    extra_blocks: extraBlocks,
    estimated_price: estimatedPrice,
  };
}
