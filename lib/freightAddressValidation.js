/**
 * Google Address Validation API (server-side only).
 * Env: GOOGLE_MAPS_API_KEY (Address Validation API enabled) or GOOGLE_ADDRESS_VALIDATION_API_KEY.
 * Soft-fails: if unavailable, returns original address parts unchanged.
 */

const TIMEOUT_MS = Math.max(
  2000,
  parseInt(process.env.GOOGLE_ADDRESS_VALIDATION_TIMEOUT_MS || "6000", 10) || 6000
);

function mapsKey() {
  return String(
    process.env.GOOGLE_ADDRESS_VALIDATION_API_KEY ||
      process.env.GOOGLE_MAPS_API_KEY ||
      process.env.GOOGLE_ROUTES_API_KEY ||
      ""
  ).trim();
}

/**
 * @param {{ street?: string, unit?: string, city?: string, state?: string, zip?: string, full?: string }} addr
 */
export async function validateAndStandardizeAddress(addr = {}) {
  const input = {
    street: String(addr.street || "").trim(),
    unit: String(addr.unit || "").trim(),
    city: String(addr.city || "").trim(),
    state: String(addr.state || "").trim().toUpperCase(),
    zip: String(addr.zip || "").trim(),
    full: String(addr.full || "").trim(),
  };
  const regionCode = "US";
  const addressLines = [input.street, input.unit].filter(Boolean);
  const key = mapsKey();
  if (!key || !input.street || !input.city || !input.state || !input.zip) {
    return {
      ok: Boolean(input.street && input.city && input.state && input.zip),
      standardized: false,
      provider: key ? "skipped_incomplete" : "none",
      delivery_address: {
        ...input,
        full:
          input.full ||
          [input.street, input.unit, input.city, input.state, input.zip].filter(Boolean).join(", "),
      },
    };
  }

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);
  try {
    const res = await fetch(
      `https://addressvalidation.googleapis.com/v1:validateAddress?key=${encodeURIComponent(key)}`,
      {
        method: "POST",
        signal: controller.signal,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          address: {
            regionCode,
            locality: input.city,
            administrativeArea: input.state,
            postalCode: input.zip,
            addressLines,
          },
        }),
      }
    );
    const data = await res.json().catch(() => null);
    if (!res.ok) {
      return {
        ok: true,
        standardized: false,
        provider: "google_address_validation",
        error: String(data?.error?.message || `http_${res.status}`).slice(0, 160),
        delivery_address: {
          ...input,
          full:
            input.full ||
            [input.street, input.unit, input.city, input.state, input.zip]
              .filter(Boolean)
              .join(", "),
        },
      };
    }

    const result = data?.result;
    const postal = result?.address?.postalAddress || {};
    const formatted = String(result?.address?.formattedAddress || "").trim();
    const street =
      [postal.addressLines?.[0], postal.addressLines?.[1]].filter(Boolean).join(", ") ||
      input.street;
    const city = postal.locality || input.city;
    const state = String(postal.administrativeArea || input.state).trim().toUpperCase();
    const zip = String(postal.postalCode || input.zip).replace(/\s+/g, "");
    const unit = input.unit;
    const full =
      formatted ||
      [street, unit, city, state, zip].filter(Boolean).join(", ");

    return {
      ok: true,
      standardized: true,
      provider: "google_address_validation",
      verdict: result?.verdict || null,
      delivery_address: { street, unit, city, state, zip, full },
    };
  } catch (err) {
    return {
      ok: true,
      standardized: false,
      provider: "google_address_validation",
      error: err?.name === "AbortError" ? "timeout" : "request_failed",
      delivery_address: {
        ...input,
        full:
          input.full ||
          [input.street, input.unit, input.city, input.state, input.zip].filter(Boolean).join(", "),
      },
    };
  } finally {
    clearTimeout(timer);
  }
}
