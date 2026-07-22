import {
  SHOWROOM_ORIGIN,
  LOCAL_AZ_BASE_MINUTES,
  LOCAL_AZ_BASE_PRICE,
  LOCAL_AZ_REF_MINUTES,
  LOCAL_AZ_REF_PRICE,
  calculateLocalRouteEstimate,
  isArizonaStateOrZip,
} from "./freightPalletize.js";
import { attachRouteMapFields } from "./freightRouteMap.js";

const ROUTE_TIMEOUT_MS = Math.max(
  3000,
  parseInt(process.env.GOOGLE_ROUTES_TIMEOUT_MS || "8000", 10) || 8000
);

/**
 * Google Routes API — computeRoutes (server-side only).
 * Env: GOOGLE_MAPS_API_KEY (must allow Routes API; Distance Matrix is not used).
 * @returns {{ ok: boolean, drive_minutes?: number, distance_miles?: number, encoded_polyline?: string, map_image_url?: string, directions_url?: string, error?: string, provider?: string }}
 */
export async function lookupDriveTimeMinutes(destinationAddress) {
  const dest = String(destinationAddress || "").trim();
  if (!dest) {
    return { ok: false, error: "missing_destination" };
  }

  const key = String(process.env.GOOGLE_MAPS_API_KEY || process.env.GOOGLE_ROUTES_API_KEY || "").trim();
  if (!key) {
    return { ok: false, error: "maps_not_configured", provider: "none" };
  }

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), ROUTE_TIMEOUT_MS);
  try {
    const res = await fetch("https://routes.googleapis.com/directions/v2:computeRoutes", {
      method: "POST",
      signal: controller.signal,
      headers: {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": key,
        "X-Goog-FieldMask":
          "routes.duration,routes.distanceMeters,routes.polyline.encodedPolyline",
      },
      body: JSON.stringify({
        origin: { address: SHOWROOM_ORIGIN },
        destination: { address: dest },
        travelMode: "DRIVE",
        routingPreference: "TRAFFIC_AWARE",
        languageCode: "en-US",
        units: "IMPERIAL",
      }),
    });

    const data = await res.json().catch(() => null);
    if (!res.ok) {
      const apiMsg = data?.error?.message || data?.error?.status || `http_${res.status}`;
      return {
        ok: false,
        error: String(apiMsg).slice(0, 160),
        provider: "google_routes",
      };
    }

    const route = data?.routes?.[0];
    if (!route) {
      return { ok: false, error: "maps_no_route", provider: "google_routes" };
    }

    // duration is like "1234s"
    const durationStr = String(route.duration || "");
    const seconds = Number(durationStr.replace(/s$/i, ""));
    const meters = Number(route.distanceMeters);
    if (!Number.isFinite(seconds) || seconds < 0) {
      return { ok: false, error: "maps_invalid_duration", provider: "google_routes" };
    }

    const encodedPolyline = String(route.polyline?.encodedPolyline || "").trim() || null;

    return attachRouteMapFields(
      {
        ok: true,
        drive_minutes: Math.max(1, Math.round(seconds / 60)),
        distance_miles: Number.isFinite(meters)
          ? Math.round((meters / 1609.344) * 10) / 10
          : null,
        encoded_polyline: encodedPolyline,
        provider: "google_routes",
      },
      dest
    );
  } catch (err) {
    const aborted = err?.name === "AbortError";
    return {
      ok: false,
      error: aborted ? "maps_timeout" : "maps_request_failed",
      provider: "google_routes",
    };
  } finally {
    clearTimeout(timer);
  }
}

/**
 * Build manual-review reasons from access + route (no auto surcharges).
 */
export function buildManualReviewReasons(access = {}, { driveMinutes = null, isLocalAz = false } = {}) {
  const reasons = [];
  const flights = Number(access.stair_flights) || 0;
  if (access.stairs) {
    if (flights >= 2) reasons.push(`${flights} flights of stairs`);
    else if (flights === 1) reasons.push("One flight of stairs");
    else reasons.push("Stairs at destination");
  }
  if (access.needs_more_than_two_people) reasons.push("More than two movers requested");
  if (access.freight_elevator) reasons.push("Freight elevator / elevator restrictions");
  if (access.tight_turns_or_narrow_halls) reasons.push("Tight turns or narrow hallways");
  if (access.long_carry) reasons.push("Long carry");
  if (access.inside_delivery) reasons.push("Inside delivery requested");
  if (access.room_placement) reasons.push("Room placement requested");
  if (access.unpacking_or_debris_removal) reasons.push("Unpacking or debris removal requested");
  if (access.disassembly_or_assembly) reasons.push("Disassembly or assembly requested");
  if (access.parking_or_time_restrictions) reasons.push("Parking or time restrictions");
  if (access.fragile_or_special_handling) reasons.push("Fragile or special handling");
  if (access.gated_access) reasons.push("Gated access");
  if (isLocalAz && driveMinutes != null && driveMinutes > 75) {
    reasons.push("Long drive from Scottsdale showroom — confirm scheduling");
  }
  return reasons;
}

/**
 * Full local estimate block for preview/submit responses.
 */
export async function buildLocalEstimateForDestination({
  deliveryPath,
  state,
  zip,
  destinationFull,
  access,
}) {
  const inAz = isArizonaStateOrZip({ state, zip });

  if ((deliveryPath === "local_az" || deliveryPath === "pickup_az") && !inAz) {
    return {
      ok: false,
      status: 400,
      error:
        deliveryPath === "pickup_az"
          ? "Consignor pickup must be within Arizona. Out-of-state items need a different arrangement."
          : "This destination is outside Arizona. Please switch to nationwide freight for an out-of-state quote.",
    };
  }

  if (deliveryPath !== "local_az" && deliveryPath !== "pickup_az") {
    return { ok: true, skip: true };
  }

  const routeLookup = await lookupDriveTimeMinutes(destinationFull);
  const reviewReasons = buildManualReviewReasons(access, {
    driveMinutes: routeLookup.ok ? routeLookup.drive_minutes : null,
    isLocalAz: true,
  });

  if (!routeLookup.ok) {
    reviewReasons.push("Route time unavailable — manual review required");
    return {
      ok: true,
      delivery_path: deliveryPath,
      route: {
        drive_minutes: null,
        distance_miles: null,
        error: routeLookup.error || "maps_failed",
      },
      local_estimate: {
        base_minutes: LOCAL_AZ_BASE_MINUTES,
        base_price: LOCAL_AZ_BASE_PRICE,
        block_minutes: LOCAL_AZ_REF_MINUTES,
        block_price: LOCAL_AZ_REF_PRICE,
        estimated_price: null,
        drive_minutes: null,
        requires_manual_review: true,
        review_reasons: [...new Set(reviewReasons)],
      },
      requires_manual_review: true,
      review_reasons: [...new Set(reviewReasons)],
    };
  }

  const priced = calculateLocalRouteEstimate(routeLookup.drive_minutes);
  const requiresManualReview = reviewReasons.length > 0;

  return {
    ok: true,
    delivery_path: deliveryPath,
    route: {
      drive_minutes: routeLookup.drive_minutes,
      distance_miles: routeLookup.distance_miles,
      map_image_url: routeLookup.map_image_url || null,
      directions_url: routeLookup.directions_url || null,
    },
    local_estimate: {
      base_minutes: priced.base_minutes,
      base_price: priced.base_price,
      block_minutes: priced.block_minutes,
      block_price: priced.block_price,
      estimated_price: priced.estimated_price,
      drive_minutes: priced.drive_minutes,
      requires_manual_review: requiresManualReview,
      review_reasons: requiresManualReview ? reviewReasons : [],
    },
    requires_manual_review: requiresManualReview,
    review_reasons: reviewReasons,
  };
}
