/**
 * Lost & Found FreightCenter Quote SOP — server-side palletize (never trust browser alone).
 *
 * Listing rules for sets (e.g. "Set of 4" chairs):
 * - Form quantity stays 1 (one listing / one SKU line).
 * - Title/description dims are PER PIECE.
 * - Listing weight is TOTAL for the set.
 * - Pallet plan stacks/layers pieces (SOP nested-stack example for chairs).
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
/** Covered by the base $95 — matches Webflow calculator. */
export const LOCAL_AZ_BASE_MINUTES = 17;
/** Reference rate: $15 per 8 minutes → applied per extra minute, then round up to $5. */
export const LOCAL_AZ_REF_MINUTES = 8;
export const LOCAL_AZ_REF_PRICE = 15;
/** @deprecated alias — same as LOCAL_AZ_REF_MINUTES */
export const LOCAL_AZ_BLOCK_MINUTES = LOCAL_AZ_REF_MINUTES;
/** @deprecated alias — same as LOCAL_AZ_REF_PRICE */
export const LOCAL_AZ_BLOCK_PRICE = LOCAL_AZ_REF_PRICE;
export const MAX_FREIGHT_ITEMS = 10;
export const MAX_DIM_IN = 240;
export const MAX_WEIGHT_LB = 5000;
export const MAX_STAIR_FLIGHTS = 20;

const SET_WORD_COUNTS = {
  two: 2,
  three: 3,
  four: 4,
  five: 5,
  six: 6,
  seven: 7,
  eight: 8,
  nine: 9,
  ten: 10,
  twelve: 12,
};

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

/**
 * How many pieces are in this listing (Set of 4 → 4). Defaults to 1.
 * Does NOT change form quantity — qty stays 1 for a single set listing.
 */
export function parseSetCountFromTitle(title) {
  const t = String(title || "");
  let m = t.match(/\bsets?\s+of\s+(\d{1,2})\b/i);
  if (m) return Math.min(24, parseInt(m[1], 10));
  m = t.match(/\bsets?\s+of\s+(two|three|four|five|six|seven|eight|nine|ten|twelve)\b/i);
  if (m) return SET_WORD_COUNTS[m[1].toLowerCase()] || 1;
  if (/\bpair\b/i.test(t)) return 2;
  m = t.match(/\b(\d{1,2})\s*-?\s*(?:pc|pcs|piece|pieces)\b/i);
  if (m) return Math.min(24, parseInt(m[1], 10));
  m = t.match(/\b(\d{1,2})\s+chairs?\b/i);
  if (m) return Math.min(24, parseInt(m[1], 10));
  m = t.match(/\b(\d{1,2})\s+stools?\b/i);
  if (m) return Math.min(24, parseInt(m[1], 10));
  return 1;
}

/** Chairs/stools that typically nest; swivel / barrel rarely nest tightly. */
export function canNestStackPieces(title, { nonStackable = false } = {}) {
  if (nonStackable) return false;
  const t = String(title || "").toLowerCase();
  if (/\b(swivel|barrel|recliner|sofa|loveseat|sectional|chaise|bench)\b/.test(t)) return false;
  return /\b(dining\s+chairs?|side\s+chairs?|chairs?|stools?|bar\s*stools?|barstools?)\b/.test(t);
}

function estimateNestAddIn(pieceHeight) {
  // Nested dining chairs: each additional piece typically adds ~3–6".
  return Math.max(3, Math.min(6, Math.round(Number(pieceHeight) * 0.12) || 4));
}

/** Max pieces of WxD that fit on a 48x40 footprint (tries both rotations). */
export function piecesPerPalletLayer(width, depth, palletW = STANDARD_PALLET_W, palletD = STANDARD_PALLET_D) {
  const w = num(width);
  const d = num(depth);
  if (!w || !d) return 1;
  let best = 0;
  for (const [pw, pd] of [
    [w, d],
    [d, w],
  ]) {
    const across = Math.floor(palletW / pw);
    const deep = Math.floor(palletD / pd);
    best = Math.max(best, Math.max(0, across) * Math.max(0, deep));
  }
  return Math.max(1, best);
}

/**
 * Plan how a multi-piece set sits on pallet(s) using individual dims + total weight.
 */
export function planSetPacking({
  title,
  width,
  depth,
  height,
  setCount,
  nonStackable = false,
}) {
  const count = Math.max(1, Math.floor(Number(setCount) || 1));
  const notes = [];

  if (count <= 1) {
    return {
      set_count: 1,
      packing_mode: "single",
      pieces_per_layer: 1,
      layers: 1,
      stacked_height_in: height,
      pallet_count: 1,
      notes: ["Single piece — standard SOP pallet entry."],
    };
  }

  notes.push(
    `Listing is a set of ${count}: dims are per piece; weight is the TOTAL for the set.`
  );

  if (canNestStackPieces(title, { nonStackable })) {
    const nestAdd = estimateNestAddIn(height);
    const stackedHeight = height + (count - 1) * nestAdd;
    notes.push(
      `Nest/stack plan (SOP-style): ${count} pieces nested — est. stack height ${stackedHeight}" (${height}" + ${(count - 1) * nestAdd}" nest).`
    );
    return {
      set_count: count,
      packing_mode: "nested_stack",
      pieces_per_layer: 1,
      layers: count,
      stacked_height_in: stackedHeight,
      nest_add_in: nestAdd,
      pallet_count: 1,
      notes,
    };
  }

  const perLayer = piecesPerPalletLayer(width, depth);
  const layers = Math.ceil(count / perLayer);
  const stackedHeight = layers * height;
  // If one piece already exceeds footprint, footprint grows via oversized rules later.
  const needsExtraPallets = perLayer === 1 && count > 1 && width > STANDARD_PALLET_W && depth > STANDARD_PALLET_D;
  const palletCount = needsExtraPallets ? count : 1;

  if (perLayer >= count) {
    notes.push(
      `Side-by-side on one pallet: all ${count} pieces fit in one layer on 48"×40" (no nesting).`
    );
  } else {
    notes.push(
      `Layered on pallet: ${perLayer} piece(s)/layer × ${layers} layer(s) (does not nest well — e.g. swivel/barrel). Est. stack height ${stackedHeight}".`
    );
  }
  if (palletCount > 1) {
    notes.push(`Oversized pieces may need ${palletCount} pallets — staff should confirm.`);
  }

  return {
    set_count: count,
    packing_mode: "layered",
    pieces_per_layer: perLayer,
    layers,
    stacked_height_in: stackedHeight,
    pallet_count: palletCount,
    notes,
  };
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
  // 60X30H = footprint × height (round tables: diameter × height)
  m = t.match(/(\d+(?:\.\d+)?)\s*[xX×]\s*(\d+(?:\.\d+)?)\s*H\b/i);
  if (m) {
    const a = num(m[1]);
    const b = num(m[2]);
    if (a && b) {
      if (/\bround\b/i.test(t)) return { width: a, depth: a, height: b };
      return { width: a, depth: Math.min(a, b), height: b };
    }
  }
  return null;
}

/**
 * Recalculate SOP freight entry for one listing line.
 * Sets: qty=1, per-piece dims, TOTAL weight, stacked/layered pallet height.
 * @param {object} item
 * @param {{ allowTitleDimFallback?: boolean }} [opts]
 *   When false (Webflow form submit), never invent dims from the title —
 *   use only the width/depth/height/weight the customer entered.
 */
export function palletizeItem(item = {}, { allowTitleDimFallback = true } = {}) {
  const title = String(item.title || item.itemName || "").trim();
  // Form quantity = how many of this listing line (almost always 1 for a set).
  const listingQty = Math.max(1, Math.min(99, Math.floor(num(item.quantity) || 1)));
  const setCount =
    num(item.set_count) || num(item.setCount) || parseSetCountFromTitle(title) || 1;

  let width = num(item.width ?? item.widthIn);
  let depth = num(item.depth ?? item.depthIn ?? item.length);
  let height = num(item.height ?? item.heightIn);
  // Explicit stacked height from staff overrides estimate.
  const explicitStacked = num(item.stacked_height ?? item.stackedHeightIn ?? item.stacked_height_in);
  // Weight is TOTAL for the listing/set — never multiply by set_count.
  let weight = num(item.weight ?? item.weightLb);

  // Title parse is ONLY a last resort when the client sent no dims at all.
  if (
    allowTitleDimFallback &&
    width == null &&
    depth == null &&
    height == null &&
    title
  ) {
    const parsed = parseDimsFromTitle(title);
    if (parsed) {
      width = parsed.width;
      depth = parsed.depth;
      height = parsed.height;
    }
  }

  const missing = [];
  if (width == null) missing.push("width");
  if (depth == null) missing.push("depth");
  if (height == null) missing.push("height");
  if (weight == null) missing.push("weight");

  const freightClass = resolveFreightClass(item);
  const suggestedFreightClass = freightClass ?? inferFreightClass({ ...item, title, freight_class: undefined });
  const nonStackable = shouldMarkNonStackable({ ...item, title });

  const baseFail = {
    quantity: listingQty,
    set_count: setCount,
    product: { width, depth, height, weight },
    pallet: null,
    freight_class: freightClass,
    suggested_freight_class: suggestedFreightClass,
    non_stackable: nonStackable,
    packing: null,
  };

  if (missing.length) {
    return { ok: false, missing, ...baseFail };
  }

  if (width > MAX_DIM_IN || depth > MAX_DIM_IN || height > MAX_DIM_IN) {
    return { ok: false, missing: ["dimensions_out_of_range"], ...baseFail };
  }
  if (weight > MAX_WEIGHT_LB) {
    return { ok: false, missing: ["weight_out_of_range"], ...baseFail };
  }

  const packing = planSetPacking({
    title,
    width,
    depth,
    height,
    setCount: setCount * listingQty,
    nonStackable,
  });
  if (explicitStacked) {
    packing.stacked_height_in = explicitStacked;
    packing.packing_mode = "explicit_stacked_height";
    packing.notes = [
      ...(packing.notes || []),
      `Using provided stacked height ${explicitStacked}".`,
    ];
  }

  // Footprint: based on one piece (or layer arrangement still uses standard 48x40 when pieces fit).
  let freightW = STANDARD_PALLET_W;
  let freightD = STANDARD_PALLET_D;
  if (width > STANDARD_PALLET_W) freightW = roundUpTo5(width);
  if (depth > STANDARD_PALLET_D) freightD = roundUpTo5(depth);

  const freightH = roundUpInches(packing.stacked_height_in + PALLET_HEIGHT_IN);
  // Total listing weight × listing lines only (set weight already totals the pieces).
  const freightWeight = Math.ceil(weight * listingQty + PALLET_WEIGHT_LB * (packing.pallet_count || 1));

  const pallet = {
    width: freightW,
    depth: freightD,
    height: freightH,
    weight: freightWeight,
    freight_class: freightClass,
    suggested_freight_class: suggestedFreightClass,
    packaging_type: packing.pallet_count > 1 ? "Pallet(s)" : "Pallet(s)",
    non_stackable: nonStackable,
    set_count: packing.set_count,
    packing_mode: packing.packing_mode,
    pieces_per_layer: packing.pieces_per_layer,
    layers: packing.layers,
    stacked_height_in: packing.stacked_height_in,
    pallet_count: packing.pallet_count || 1,
    packing_notes: packing.notes,
  };

  return {
    ok: true,
    missing: [],
    quantity: listingQty,
    set_count: setCount,
    product: {
      width,
      depth,
      height,
      weight,
      dims_are: setCount > 1 ? "per_piece" : "as_listed",
      weight_is: setCount > 1 ? "total_for_set" : "as_listed",
    },
    packing,
    pallet,
    freight_class: freightClass,
    suggested_freight_class: suggestedFreightClass,
    non_stackable: nonStackable,
  };
}

/** Exact Webflow Part 2 palletize — single source of truth for the calculator. */
export function webflowSimplePalletize(item = {}) {
  const w = num(item.width ?? item.widthIn);
  const d = num(item.depth ?? item.depthIn ?? item.length);
  const h = num(item.height ?? item.heightIn);
  const wt = num(item.weight ?? item.weightLb);
  const selectedClass =
    item.freight_class === null || item.freight_class === ""
      ? null
      : num(item.freight_class ?? item.freightClass);
  if (w == null || d == null || h == null || wt == null) return null;
  return {
    width: w <= STANDARD_PALLET_W ? STANDARD_PALLET_W : roundUpTo5(w),
    depth: d <= STANDARD_PALLET_D ? STANDARD_PALLET_D : roundUpTo5(d),
    height: Math.ceil(h + PALLET_HEIGHT_IN),
    weight: Math.ceil(wt + PALLET_WEIGHT_LB),
    freight_class: selectedClass,
    packaging_type: "Pallet(s)",
    non_stackable: Boolean(item.non_stackable ?? item.nonStackable),
    set_count: 1,
    packing_mode: "webflow_simple",
    pieces_per_layer: 1,
    layers: 1,
    stacked_height_in: h,
    pallet_count: 1,
    packing_notes: ["Webflow calculator SOP entry (48×40 when fits, +5 in H, +30 lb)."],
  };
}

export function palletizeItems(items = [], { allowTitleDimFallback = true, preferClientPallet = false } = {}) {
  return (Array.isArray(items) ? items : []).map((item, index) => {
    const result = palletizeItem(item, { allowTitleDimFallback });
    const priceNum = Number(item.price);

    // Always echo the customer's entered product dims (not title-inferred).
    const width = num(item.width ?? item.widthIn) ?? result.product?.width ?? null;
    const depth = num(item.depth ?? item.depthIn ?? item.length) ?? result.product?.depth ?? null;
    const height = num(item.height ?? item.heightIn) ?? result.product?.height ?? null;
    const weight = num(item.weight ?? item.weightLb) ?? result.product?.weight ?? null;

    // Single calculator: prefer posted pallet, else Webflow-simple formula from entered dims.
    let pallet = result.pallet;
    if (preferClientPallet) {
      const clientPallet = item.pallet && typeof item.pallet === "object" ? item.pallet : null;
      const cw = clientPallet ? num(clientPallet.width) : null;
      const cd = clientPallet ? num(clientPallet.depth) : null;
      const ch = clientPallet ? num(clientPallet.height) : null;
      const cwt = clientPallet ? num(clientPallet.weight) : null;
      if (cw && cd && ch && cwt) {
        pallet = {
          ...webflowSimplePalletize({
            width,
            depth,
            height,
            weight,
            freight_class: clientPallet.freight_class,
            non_stackable: clientPallet.non_stackable,
          }),
          width: cw,
          depth: cd,
          height: ch,
          weight: cwt,
          freight_class:
            clientPallet.freight_class === null || clientPallet.freight_class === ""
              ? null
              : num(clientPallet.freight_class) ?? result.freight_class,
          non_stackable: Boolean(clientPallet.non_stackable ?? result.non_stackable),
          packing_mode: "client_summary",
          packing_notes: ["Matches calculator summary pallet entry."],
        };
      } else {
        pallet =
          webflowSimplePalletize({
            width,
            depth,
            height,
            weight,
            freight_class: item.freight_class,
            non_stackable: item.non_stackable ?? item.nonStackable,
          }) || result.pallet;
      }
    }

    return {
      index: index + 1,
      source: String(item.source || "manual").trim() || "manual",
      title: String(item.title || `Item ${index + 1}`).trim(),
      price: Number.isFinite(priceNum) ? priceNum : item.price || 0,
      product_url: String(item.product_url || item.productUrl || "").trim(),
      quantity: result.quantity,
      set_count: preferClientPallet ? 1 : result.set_count,
      width,
      depth,
      height,
      weight,
      dims_are: "as_entered",
      weight_is: "as_entered",
      freight_class: result.freight_class,
      suggested_freight_class: result.suggested_freight_class,
      non_stackable: Boolean(item.non_stackable ?? item.nonStackable ?? result.non_stackable),
      packing: preferClientPallet
        ? {
            set_count: 1,
            packing_mode: "webflow_simple",
            pieces_per_layer: 1,
            layers: 1,
            stacked_height_in: height,
            pallet_count: 1,
            notes: ["Product dims as entered on the calculator form."],
          }
        : result.packing,
      pallet,
      ok: result.ok && width != null && depth != null && height != null && weight != null,
      missing: result.missing,
    };
  });
}

/** Local AZ preliminary route price from one-way drive minutes (matches Webflow). */
export function calculateLocalRouteEstimate(driveMinutes) {
  const mins = Number(driveMinutes);
  if (!Number.isFinite(mins) || mins < 0) return null;
  const drive = Math.max(0, Math.ceil(mins));
  const extraMinutes = Math.max(0, drive - LOCAL_AZ_BASE_MINUTES);
  const perMinuteRate = LOCAL_AZ_REF_PRICE / LOCAL_AZ_REF_MINUTES;
  const rawEstimate = LOCAL_AZ_BASE_PRICE + extraMinutes * perMinuteRate;
  const estimatedPrice = Math.ceil(rawEstimate / 5) * 5;
  return {
    base_minutes: LOCAL_AZ_BASE_MINUTES,
    base_price: LOCAL_AZ_BASE_PRICE,
    block_minutes: LOCAL_AZ_REF_MINUTES,
    block_price: LOCAL_AZ_REF_PRICE,
    per_minute_rate: perMinuteRate,
    drive_minutes: Math.round(mins) || drive,
    extra_minutes: extraMinutes,
    raw_estimate: rawEstimate,
    estimated_price: estimatedPrice,
  };
}
