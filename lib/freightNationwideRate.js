/**
 * Nationwide freight preliminary range (not a booked carrier quote).
 * Floor is never below $350. White-glove / accessorials push the high end up.
 * Uses Google Routes distance when available; otherwise zip-distance bands fall back safely.
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

function baseBandFromMiles(miles) {
  const m = num(miles);
  if (!m || m <= 0) return { low: 400, high: 750 }; // unknown distance — conservative mid band
  if (m <= 400) return { low: 350, high: 575 };
  if (m <= 800) return { low: 425, high: 750 };
  if (m <= 1200) return { low: 525, high: 925 };
  if (m <= 1800) return { low: 625, high: 1100 };
  if (m <= 2400) return { low: 725, high: 1300 };
  return { low: 825, high: 1550 };
}

function weightAdders(totalLb) {
  const w = num(totalLb);
  if (w <= 150) return { low: 0, high: 25 };
  if (w <= 300) return { low: 25, high: 100 };
  if (w <= 500) return { low: 75, high: 200 };
  if (w <= 800) return { low: 125, high: 300 };
  return { low: 200, high: 450 };
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
  if (access.inside_delivery) bump(75, 175); // toward white glove
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

/**
 * Build preliminary nationwide range from miles + weight + access.
 */
export function estimateNationwideRange({ miles = null, items = [], access = {} } = {}) {
  const band = baseBandFromMiles(miles);
  const weight = weightAdders(totalPalletWeight(items));
  const accessorials = accessorialAdders(access, items);

  let rangeLow = moneyRound(band.low + weight.low + accessorials.low);
  let rangeHigh = moneyRound(band.high + weight.high + accessorials.high);

  if (rangeHigh < rangeLow + 100) rangeHigh = moneyRound(rangeLow + 150);
  rangeLow = Math.max(NATIONWIDE_FLOOR_USD, rangeLow);
  if (rangeHigh < rangeLow) rangeHigh = moneyRound(rangeLow + 150);

  const whiteGloveLikely = Boolean(
    access.inside_delivery ||
      access.room_placement ||
      access.unpacking_or_debris_removal ||
      access.disassembly_or_assembly ||
      access.needs_more_than_two_people
  );

  return {
    status: "estimated_range",
    amount: null,
    range_low: rangeLow,
    range_high: rangeHigh,
    currency: "USD",
    floor_usd: NATIONWIDE_FLOOR_USD,
    white_glove_likely: whiteGloveLikely,
    distance_miles: miles != null && Number.isFinite(Number(miles)) ? Number(miles) : null,
    total_pallet_weight_lb: totalPalletWeight(items) || null,
    message:
      "Preliminary nationwide freight range only. Lost & Found will follow up with a direct quote and contact our freight partners for an exact quote.",
  };
}

/**
 * Live provider hook + always compute a preliminary range for the customer.
 */
export async function fetchNationwideLiveRate(submission = {}) {
  const destination =
    submission.delivery_address?.full ||
    [submission.street, submission.city, submission.state, submission.zip]
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

  // No live broker API yet — still return estimated_range (never invent a fake booked rate).
  if (!provider || provider === "none" || provider === "manual") {
    return {
      ...range,
      carrier: null,
      transit_days: null,
      quote_id: null,
      expires_at: null,
      provider: provider || "range_model",
      route: route.ok
        ? {
            distance_miles: route.distance_miles,
            drive_minutes: route.drive_minutes,
            provider: route.provider,
          }
        : { ok: false, error: route.error || "maps_unavailable" },
      requires_partner_quote: true,
    };
  }

  return {
    ...range,
    status: "estimated_range",
    carrier: null,
    transit_days: null,
    quote_id: null,
    expires_at: null,
    provider,
    route: route.ok
      ? {
          distance_miles: route.distance_miles,
          drive_minutes: route.drive_minutes,
          provider: route.provider,
        }
      : { ok: false, error: route.error || "maps_unavailable" },
    requires_partner_quote: true,
    message: `Preliminary range calculated. Live provider "${provider}" is not fully wired — staff will confirm with partners.`,
  };
}
