/**
 * Nationwide freight live-rate hook.
 * No provider credentials are exposed to Webflow.
 * When FREIGHT_RATING_PROVIDER is unset or call fails → pending_manual_review (no fake $).
 */

export async function fetchNationwideLiveRate(_submission) {
  const provider = String(process.env.FREIGHT_RATING_PROVIDER || "")
    .trim()
    .toLowerCase();

  // Placeholder for future FreightCenter / broker API integration.
  if (!provider || provider === "none" || provider === "manual") {
    return {
      status: "pending_manual_review",
      amount: null,
      currency: "USD",
      carrier: null,
      transit_days: null,
      quote_id: null,
      expires_at: null,
      provider: provider || "none",
      message:
        "Freight-ready details prepared. Confirmed carrier pricing and Shipment ID follow after review.",
    };
  }

  // Unknown provider configured — do not invent a rate.
  return {
    status: "pending_manual_review",
    amount: null,
    currency: "USD",
    carrier: null,
    transit_days: null,
    quote_id: null,
    expires_at: null,
    provider,
    message: `Live rating provider "${provider}" is not implemented; manual FreightCenter review required.`,
  };
}
