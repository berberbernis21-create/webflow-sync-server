/**
 * Route map helpers for the freight calculator.
 * Static map images are proxied through our API so GOOGLE_MAPS_API_KEY
 * is never embedded in customer emails or the Webflow page.
 *
 * Signed URLs carry origin + destination only. The proxy renders a Static Map
 * with labeled S/D markers (auto-fit zoom so both ends are always visible).
 */

import crypto from "crypto";
import { SHOWROOM_ORIGIN } from "./freightPalletize.js";

const PUBLIC_API_BASE = String(
  process.env.PUBLIC_API_BASE || "https://webflow-sync-server.onrender.com"
).replace(/\/$/, "");

const MAP_TTL_MS = Math.max(
  60 * 60 * 1000,
  parseInt(process.env.FREIGHT_MAP_TTL_MS || String(7 * 24 * 60 * 60 * 1000), 10) ||
    7 * 24 * 60 * 60 * 1000
);

function mapsApiKey() {
  return String(process.env.GOOGLE_MAPS_API_KEY || process.env.GOOGLE_ROUTES_API_KEY || "").trim();
}

function hmacSecret() {
  return String(process.env.FREIGHT_MAP_HMAC_SECRET || mapsApiKey() || "freight-map-dev").trim();
}

function sign(payload) {
  return crypto.createHmac("sha256", hmacSecret()).update(payload).digest("base64url");
}

function payloadString({ exp, origin, destination }) {
  return `${exp}|${origin}|${destination}`;
}

/** Public Google Maps directions link (no API key). */
export function buildDirectionsUrl(origin, destination) {
  const o = encodeURIComponent(String(origin || SHOWROOM_ORIGIN).trim());
  const d = encodeURIComponent(String(destination || "").trim());
  return `https://www.google.com/maps/dir/?api=1&origin=${o}&destination=${d}&travelmode=driving`;
}

/**
 * Signed URL pointing at our map proxy (safe to put in emails / page HTML).
 * Short by design: origin + destination only (no polyline in the query).
 */
export function buildSignedMapImageUrl({
  origin = SHOWROOM_ORIGIN,
  destination,
} = {}) {
  const dest = String(destination || "").trim();
  if (!dest) return null;

  const exp = Date.now() + MAP_TTL_MS;
  const o = String(origin || SHOWROOM_ORIGIN).trim();
  const sig = sign(payloadString({ exp, origin: o, destination: dest }));
  const params = new URLSearchParams({
    exp: String(exp),
    o,
    d: dest,
    sig,
  });
  return `${PUBLIC_API_BASE}/api/freight-quote/map?${params.toString()}`;
}

export function attachRouteMapFields(routeLookup, destination, origin = SHOWROOM_ORIGIN) {
  if (!routeLookup?.ok) return routeLookup;
  const dest = String(destination || "").trim();
  return {
    ...routeLookup,
    map_image_url: buildSignedMapImageUrl({
      origin,
      destination: dest,
    }),
    directions_url: buildDirectionsUrl(origin, dest),
  };
}

/**
 * Validate signed query params from GET /api/freight-quote/map.
 */
export function verifyMapRequest(query = {}) {
  const exp = Number(query.exp);
  const origin = String(query.o || "").trim();
  const destination = String(query.d || "").trim();
  const sig = String(query.sig || "").trim();

  if (!Number.isFinite(exp) || !origin || !destination || !sig) {
    return { ok: false, status: 400, error: "missing_params" };
  }
  if (Date.now() > exp) {
    return { ok: false, status: 410, error: "map_link_expired" };
  }

  const expected = sign(payloadString({ exp, origin, destination }));
  const a = Buffer.from(sig);
  const b = Buffer.from(expected);
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) {
    return { ok: false, status: 403, error: "bad_signature" };
  }

  return { ok: true, origin, destination };
}

/**
 * Build the Google Static Maps URL (server-side only).
 * Markers only (no zoom, no path): Google auto-fits both endpoints with padding
 * so the full to/from route is visible — same framing as the customer email.
 */
export function buildGoogleStaticMapUrl({ origin, destination }) {
  const key = mapsApiKey();
  if (!key) return null;

  const params = new URLSearchParams({
    size: "640x400",
    scale: "2",
    maptype: "roadmap",
    key,
  });

  // Explicit size + labeled markers; omit zoom so both pins stay in frame.
  params.append("markers", `size:mid|color:0x07127c|label:S|${origin}`);
  params.append("markers", `size:mid|color:0xc0392b|label:D|${destination}`);

  return `https://maps.googleapis.com/maps/api/staticmap?${params.toString()}`;
}

/**
 * Fetch Static Maps bytes for the signed request (proxy).
 */
export async function fetchStaticMapBytes(verified) {
  const url = buildGoogleStaticMapUrl(verified);
  if (!url) {
    return { ok: false, status: 503, error: "maps_not_configured" };
  }

  const res = await fetch(url);
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    console.error("[freight-quote] static map failed", res.status, String(body).slice(0, 200));
    return { ok: false, status: 502, error: `static_map_http_${res.status}` };
  }
  const buf = Buffer.from(await res.arrayBuffer());
  const contentType = res.headers.get("content-type") || "image/png";
  return { ok: true, buf, contentType };
}
