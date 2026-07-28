/**
 * US ZIP → city/state lookup for the freight estimator.
 * Uses Zippopotam.us (no API key). Hawaii and Alaska are excluded from nationwide quotes.
 */

const ZIP_RE = /^\d{5}$/;

/** Alaska ZIP ranges commonly used for destination checks. */
function zipLooksLikeAlaska(zip5) {
  const n = Number(zip5);
  return Number.isFinite(n) && n >= 99501 && n <= 99950;
}

/** Hawaii ZIP ranges. */
function zipLooksLikeHawaii(zip5) {
  const n = Number(zip5);
  return Number.isFinite(n) && n >= 96701 && n <= 96898;
}

export function normalizeUsZip(raw) {
  const digits = String(raw || "").replace(/\D/g, "");
  if (digits.length < 5) return "";
  return digits.slice(0, 5);
}

/**
 * Nationwide freight quotes: contiguous US + DC only (not HI / AK).
 * @returns {{ excluded: boolean, state?: string, reason?: string }}
 */
export function nationwideDestinationExclusion({ state, zip } = {}) {
  const st = String(state || "")
    .trim()
    .toUpperCase();
  const zip5 = normalizeUsZip(zip);

  if (st === "HI" || st === "HAWAII" || zipLooksLikeHawaii(zip5)) {
    return {
      excluded: true,
      state: "HI",
      reason:
        "Nationwide freight quotes are not available for Hawaii. Please contact Lost & Found for shipping options to Hawaii.",
    };
  }
  if (st === "AK" || st === "ALASKA" || zipLooksLikeAlaska(zip5)) {
    return {
      excluded: true,
      state: "AK",
      reason:
        "Nationwide freight quotes are not available for Alaska. Please contact Lost & Found for shipping options to Alaska.",
    };
  }
  return { excluded: false };
}

/**
 * @param {string} zip
 * @returns {Promise<{ ok: true, zip: string, city: string, state: string, state_name: string } | { ok: false, error: string, status?: number }>}
 */
export async function lookupUsZipCityState(zip) {
  const zip5 = normalizeUsZip(zip);
  if (!ZIP_RE.test(zip5)) {
    return { ok: false, error: "Enter a valid 5-digit ZIP code.", status: 400 };
  }

  try {
    const res = await fetch(`https://api.zippopotam.us/us/${zip5}`, {
      headers: { Accept: "application/json" },
      signal: AbortSignal.timeout(8000),
    });
    if (res.status === 404) {
      return { ok: false, error: "We could not find that ZIP code.", status: 404 };
    }
    if (!res.ok) {
      return { ok: false, error: "ZIP lookup is temporarily unavailable.", status: 502 };
    }
    const data = await res.json();
    const place = Array.isArray(data?.places) ? data.places[0] : null;
    const city = String(place?.["place name"] || "").trim();
    const state = String(place?.["state abbreviation"] || "").trim().toUpperCase();
    const stateName = String(place?.state || "").trim();
    if (!city || !/^[A-Z]{2}$/.test(state)) {
      return { ok: false, error: "We could not find that ZIP code.", status: 404 };
    }
    return {
      ok: true,
      zip: zip5,
      city,
      state,
      state_name: stateName,
    };
  } catch (err) {
    console.error("[freight-zip] lookup failed:", err?.message || err);
    return { ok: false, error: "ZIP lookup is temporarily unavailable.", status: 502 };
  }
}
