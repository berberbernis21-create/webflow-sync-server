/**
 * Nationwide freight preliminary range (not a booked carrier quote).
 *
 * Smart inputs:
 * - Google Routes distance (miles) from Scottsdale showroom → destination
 * - Total pallet weight + cube (from SOP pallet dims)
 * - Accessorials / white-glove / warehouse pickup / liftgate flags
 *
 * Floor is never below $350. Always labeled as preliminary — staff + trusted
 * partners confirm the exact carrier quote (human in the loop).
 */

import { lookupDriveTimeMinutes } from "./freightLocalEstimate.js";

export const NATIONWIDE_FLOOR_USD = 350;
/** Flat nationwide adder when any item is marked non-stackable. */
export const NON_STACKABLE_ADDER_USD = 50;
/** Warehouse / terminal pickup discount band. */
export const WAREHOUSE_PICKUP_DISCOUNT_LOW = 150;
export const WAREHOUSE_PICKUP_DISCOUNT_HIGH = 200;

function num(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

function moneyRound(n) {
  return Math.max(NATIONWIDE_FLOOR_USD, Math.ceil(num(n) / 25) * 25);
}

function totalPalletWeight(items = []) {
  return (Array.isArray(items) ? items : []).reduce((sum, it) => {
    const w = num(it?.pallet?.weight ?? it?.weight);
    const qty = Math.max(1, num(it?.quantity) || 1);
    return sum + w * (it?.pallet?.weight != null ? 1 : qty);
  }, 0);
}

/** Rough freight cube in cubic feet from pallet W×D×H. */
function totalPalletCubeFt(items = []) {
  return (Array.isArray(items) ? items : []).reduce((sum, it) => {
    const p = it?.pallet || {};
    const w = num(p.width ?? it?.width);
    const d = num(p.depth ?? it?.depth);
    const h = num(p.height ?? it?.height);
    if (w > 0 && d > 0 && h > 0) return sum + (w * d * h) / 1728;
    return sum;
  }, 0);
}

/**
 * Distance-driven base band.
 * Soft scale so small/light freight stays near the floor; long hauls still rise.
 */
function baseBandFromMiles(miles) {
  const m = num(miles);
  if (!m || m <= 0) {
    return { low: 350, high: 650, measured: false };
  }

  const low = 350 + m * 0.045 + Math.max(0, m - 1000) * 0.015;
  const high = 425 + m * 0.1 + Math.max(0, m - 1000) * 0.03;
  return { low, high, measured: true };
}

function weightAndCubeAdders(totalLb, cubeFt) {
  const w = num(totalLb);
  const c = num(cubeFt);
  let low = 0;
  let high = 0;

  if (w > 200) {
    low += (w - 200) * 0.1;
    high += (w - 200) * 0.3;
  }
  if (c > 50) {
    low += (c - 50) * 1.0;
    high += (c - 50) * 2.75;
  }
  if (w > 0 && w <= 220 && c > 0 && c <= 55) {
    low -= 25;
    high -= 50;
  }
  return { low, high };
}

function accessorialAdders(access = {}, items = []) {
  let low = 0;
  let high = 0;
  const bump = (l, h) => {
    low += l;
    high += h;
  };

  if (access.residential) bump(25, 60);

  // Liftgate: only when still required at delivery.
  if (access.liftgate_delivery) bump(30, 70);
  if (access.liftgate_pickup) bump(15, 40);

  // White glove / room of choice — large carrier service upgrades.
  if (access.white_glove || access.white_glove_delivery) bump(175, 425);
  if (access.room_of_choice || access.room_placement) bump(100, 250);
  // Avoid double-counting older inside/unpack flags when WG already selected.
  if (!access.white_glove && !access.white_glove_delivery) {
    if (access.inside_delivery) bump(60, 150);
    if (access.unpacking_or_debris_removal) bump(60, 150);
  }

  if (access.stairs) bump(50, 125);
  if (access.disassembly_or_assembly) bump(80, 250);
  if (access.fragile_or_special_handling) bump(40, 140);
  if (access.needs_more_than_two_people || access.crew_review_required) bump(125, 300);
  if (access.long_carry) bump(50, 125);
  if (access.tight_turns_or_narrow_halls) bump(30, 100);
  if (access.gated_access) bump(15, 50);
  if (access.parking_or_time_restrictions) bump(20, 65);

  // Cost savers (nationwide).
  if (access.warehouse_pickup) {
    bump(-WAREHOUSE_PICKUP_DISCOUNT_LOW, -WAREHOUSE_PICKUP_DISCOUNT_HIGH);
  }
  if (access.store_loading) bump(-40, -25);
  if (access.dock || access.forklift) bump(-20, -10);

  const nonStack = (Array.isArray(items) ? items : []).some(
    (it) => it?.non_stackable || it?.pallet?.non_stackable
  );
  if (nonStack) bump(NON_STACKABLE_ADDER_USD, NON_STACKABLE_ADDER_USD);

  const extraItems = Math.max(0, (Array.isArray(items) ? items.length : 1) - 1);
  if (extraItems > 0) bump(extraItems * 30, extraItems * 90);

  return { low, high };
}

function followUpMessage({ miles, measured }) {
  const distanceBit =
    measured && miles != null
      ? ` We measured about ${miles} miles from our Scottsdale showroom to your delivery address.`
      : " Distance will be confirmed when we review your request.";
  return (
    `Preliminary nationwide freight range based on distance, pallet size/weight, and the service options you selected.${distanceBit}` +
    " Every carrier prices freight differently, so ranges are intentionally wide." +
    " You can contact FreightCenter directly at 800-716-7608 for a same-day check, or we will email you full rates once our team reviews your request with our trusted human partner in the loop." +
    " White Glove and Room of Choice raise the quote significantly; picking up from a freight warehouse/terminal near you typically lowers it by about $150–$200; no liftgate at delivery and store loading at our end also help." +
    " If you follow through with pickup or loading at Lost & Found, our hours are 10am–5pm, Monday–Saturday."
  );
}

/**
 * Build preliminary nationwide range from measured miles + weight/cube + access.
 */
export function estimateNationwideRange({ miles = null, items = [], access = {} } = {}) {
  const band = baseBandFromMiles(miles);
  const totalLb = totalPalletWeight(items);
  const cubeFt = totalPalletCubeFt(items);
  const size = weightAndCubeAdders(totalLb, cubeFt);
  const accessorials = accessorialAdders(access, items);

  let rangeLow = moneyRound(band.low + size.low + accessorials.low);
  let rangeHigh = moneyRound(band.high + size.high + accessorials.high);

  if (rangeHigh < rangeLow + 75) rangeHigh = moneyRound(rangeLow + 100);
  rangeLow = Math.max(NATIONWIDE_FLOOR_USD, rangeLow);
  if (rangeHigh < rangeLow) rangeHigh = moneyRound(rangeLow + 100);

  const whiteGloveLikely = Boolean(
    access.white_glove ||
      access.white_glove_delivery ||
      access.room_of_choice ||
      access.inside_delivery ||
      access.room_placement ||
      access.unpacking_or_debris_removal ||
      access.disassembly_or_assembly ||
      access.needs_more_than_two_people
  );

  const measuredMiles =
    band.measured && miles != null && Number.isFinite(Number(miles)) ? Number(miles) : null;

  return {
    status: "estimated_range",
    amount: null,
    range_low: rangeLow,
    range_high: rangeHigh,
    currency: "USD",
    floor_usd: NATIONWIDE_FLOOR_USD,
    white_glove_likely: whiteGloveLikely,
    warehouse_pickup: Boolean(access.warehouse_pickup),
    distance_miles: measuredMiles,
    distance_measured: Boolean(band.measured && measuredMiles != null),
    total_pallet_weight_lb: totalLb || null,
    total_pallet_cube_ft: cubeFt ? Math.round(cubeFt * 10) / 10 : null,
    message: followUpMessage({ miles: measuredMiles, measured: band.measured }),
  };
}

/**
 * Measure route distance, then compute the preliminary customer range.
 */
export async function fetchNationwideLiveRate(submission = {}) {
  const destination =
    submission.delivery_address?.full ||
    [submission.street, submission.unit, submission.city, submission.state, submission.zip]
      .filter(Boolean)
      .join(", ");

  const route = await lookupDriveTimeMinutes(destination);
  const miles = route.ok ? route.distance_miles : null;
  const range = estimateNationwideRange({
    miles,
    items: submission.items || [],
    access: submission.access || {},
  });

  const provider = String(process.env.FREIGHT_RATING_PROVIDER || "")
    .trim()
    .toLowerCase();

  const routeBlock = route.ok
    ? {
        ok: true,
        distance_miles: route.distance_miles,
        drive_minutes: route.drive_minutes,
        map_image_url: route.map_image_url || null,
        directions_url: route.directions_url || null,
        provider: route.provider,
      }
    : {
        ok: false,
        error: route.error || "maps_unavailable",
        provider: route.provider || "none",
      };

  return {
    ...range,
    carrier: null,
    transit_days: null,
    quote_id: null,
    expires_at: null,
    provider:
      !provider || provider === "none" || provider === "manual" ? "distance_range_model" : provider,
    route: routeBlock,
    requires_partner_quote: true,
    follow_up:
      "Contact FreightCenter at 800-716-7608 for a same-day check, or we will email you full rates after our team reviews. Lost & Found pickup/loading hours: 10am–5pm, Monday–Saturday. Warehouse/terminal pickup usually saves about $150–$200.",
  };
}
