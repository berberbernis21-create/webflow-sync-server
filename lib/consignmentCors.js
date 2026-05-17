import cors from "cors";

/** Production Webflow site origins for the consignment form. */
export const CONSIGNMENT_CORS_ORIGINS = [
  "https://www.lostandfoundresale.com",
  "https://lostandfoundresale.com",
  ...String(process.env.CONSIGNMENT_CORS_EXTRA_ORIGINS || "")
    .split(",")
    .map((o) => o.trim())
    .filter(Boolean),
];

/**
 * Browser origins allowed to POST multipart consignment submissions.
 * Webflow staging / designer preview hosts are included.
 */
export function isAllowedConsignmentOrigin(origin) {
  if (!origin || typeof origin !== "string") return false;
  if (CONSIGNMENT_CORS_ORIGINS.includes(origin)) return true;
  if (/^https:\/\/[a-z0-9-]+\.webflow\.io$/i.test(origin)) return true;
  if (/^https:\/\/([a-z0-9-]+\.)?webflow\.com$/i.test(origin)) return true;
  return false;
}

/** Global CORS: reflect allowed storefront origins; allow no-origin (webhooks, curl). */
export const consignmentCorsOptions = {
  origin(origin, callback) {
    if (!origin) return callback(null, true);
    if (isAllowedConsignmentOrigin(origin)) return callback(null, origin);
    return callback(null, true);
  },
  credentials: false,
  methods: ["GET", "HEAD", "POST", "OPTIONS"],
  optionsSuccessStatus: 204,
  maxAge: 86400,
};

export function createConsignmentCorsMiddleware() {
  return cors(consignmentCorsOptions);
}
