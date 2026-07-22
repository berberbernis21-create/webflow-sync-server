/**
 * Nationwide freight preliminary range (not a booked carrier quote).
 *
 * Smart inputs:
 * - Google Routes distance (miles) from Scottsdale showroom → destination
 * - Total pallet weight + cube (from SOP pallet dims)
 * - Accessorials / white-glove flags
 *
 * Floor is never below $350. Always labeled as preliminary — staff follow up
 * with better rates after review + partner quotes.
 */

import { lookupDriveTimeMinutes } from "./freightLocalEstimate.js";

export const NATIONWIDE_FLOOR_USD = 350;

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
 * Short hauls sit near the $350 floor; long hauls scale continuously.
 */
function baseBandFromMiles(miles) {
  const m = num(miles);
  if (!m || m <= 0) {
    // No measured miles — conservative mid-country band until Maps succeeds.
    return { low: 425, high: 850, measured: false };
  }

  // Continuous model: floor + per-mile component (LTL furniture-ish).
  const low = 350 + m * 0.09 + Math.max(0, m - 500) * 0.02;
  const high = 475 + m * 0.2 + Math.max(0, m - 500) * 0.04;
  return { low, high, measured: true };
}

function weightAndCubeAdders(totalLb, cubeFt) {
  const w = num(totalLb);
  const c = num(cubeFt);
  let low = 0;
  let high = 0;

  if (w > 150) {
    low += (w - 150) * 0.15;
    high += (w - 150) * 0.45;
  }
  if (c > 40) {
    low += (c - 40) * 1.5;
    high += (c - 40) * 4;
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

  if (access.residential) bump(40, 90);
  if (access.liftgate_delivery) bump(45, 95);
  if (access.liftgate_pickup) bump(25, 55);
  if (access.stairs) bump(60, 150);
  if (access.inside_delivery) bump(75, 175);
  if (access.room_placement) bump(100, 225);
  if (access.unpacking_or_debris_removal) bump(75, 175);
  if (access.disassembly_or_assembly) bump(100, 275);
  if (access.fragile_or_special_handling) bump(50, 175);
  if (access.needs_more_than_two_people || access.crew_review_required) bump(150, 350);
  if (access.long_carry) bump(60, 150);
  if (access.tight_turns_or_narrow_halls) bump(40, 120);
  if (access.gated_access) bump(20, 70);
  if (access.parking_or_time_restrictions) bump(25, 80);

  const nonStack = (Array.isArray(items) ? items : []).some(
    (it) => it?.non_stackable || it?.pallet?.non_stackable
  );
  if (nonStack) bump(40, 140);

  const extraItems = Math.max(0, (Array.isArray(items) ? items.length : 1) - 1);
  if (extraItems > 0) bump(extraItems * 40, extraItems * 120);

  return { low, high };
}

function followUpMessage({ miles, measured }) {
  const distanceBit =
    measured && miles != null
      ? ` We measured about ${miles} miles from our Scottsdale showroom to your delivery address.`
      : " Distance will be confirmed when we review your request.";
  return (
    `Preliminary nationwide freight range based on distance, pallet size/weight, and access needs.${distanceBit}` +
    " Final pricing depends on the carrier, service level, and partner rates — it can come in lower than this range, and white-glove or specialty handling can push it higher." +
    " This is an estimate only. We will follow up with better rates after we review your request and confirm an exact quote with our freight partners."
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

  if (rangeHigh < rangeLow + 125) rangeHigh = moneyRound(rangeLow + 175);
  rangeLow = Math.max(NATIONWIDE_FLOOR_USD, rangeLow);
  if (rangeHigh < rangeLow) rangeHigh = moneyRound(rangeLow + 175);

  const whiteGloveLikely = Boolean(
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
      "We will follow up with better rates after we review your request and confirm pricing with our freight partners.",
  };
}
