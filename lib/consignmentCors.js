import cors from "cors";

/** Production Webflow site origins for consignment + freight calculator. */
export const CONSIGNMENT_CORS_ORIGINS = [
  "https://www.lostandfoundresale.com",
  "https://lostandfoundresale.com",
  "https://www.lostandfoundhandbags.com",
  "https://lostandfoundhandbags.com",
  ...String(process.env.CONSIGNMENT_CORS_EXTRA_ORIGINS || "")
    .split(",")
    .map((o) => o.trim())
    .filter(Boolean),
];

/**
 * Browser origins allowed for freight/consignment forms.
 * Includes Webflow staging / designer preview and localhost (dev only).
 */
export function isAllowedConsignmentOrigin(origin) {
  if (!origin || typeof origin !== "string") return false;
  if (CONSIGNMENT_CORS_ORIGINS.includes(origin)) return true;
  if (/^https:\/\/[a-z0-9-]+\.webflow\.io$/i.test(origin)) return true;
  if (/^https:\/\/([a-z0-9-]+\.)?webflow\.com$/i.test(origin)) return true;
  // Local development only
  if (/^http:\/\/(localhost|127\.0\.0\.1)(:\d+)?$/i.test(origin)) return true;
  return false;
}

/**
 * Ensure ACAO is set on JSON responses (including 400/413/500).
 */
export function applyConsignmentCorsHeaders(req, res) {
  const origin = req.headers.origin;
  if (origin && isAllowedConsignmentOrigin(origin)) {
    res.setHeader("Access-Control-Allow-Origin", origin);
    res.setHeader("Vary", "Origin");
    res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type, Accept, Origin, X-Requested-With");
  } else if (!origin) {
    // Server-to-server / curl — no browser CORS needed
    res.setHeader("Access-Control-Allow-Origin", "*");
  }
}

/** Reflect allowed storefront origins only (no unrestricted production CORS). */
export const consignmentCorsOptions = {
  origin(origin, callback) {
    if (!origin) return callback(null, true);
    if (isAllowedConsignmentOrigin(origin)) return callback(null, origin);
    return callback(null, false);
  },
  credentials: false,
  methods: ["GET", "HEAD", "POST", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Accept", "Origin", "X-Requested-With"],
  optionsSuccessStatus: 204,
  maxAge: 86400,
};

export function createConsignmentCorsMiddleware() {
  return cors(consignmentCorsOptions);
}
