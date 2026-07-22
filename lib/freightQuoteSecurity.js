import crypto from "crypto";

const RATE_WINDOW_MS = 60_000;
const RATE_MAX = Math.max(5, parseInt(process.env.FREIGHT_QUOTE_RATE_MAX || "20", 10) || 20);
const IDEMPOTENCY_TTL_MS = 10 * 60_000;

/** @type {Map<string, number[]>} */
const hitsByIp = new Map();
/** @type {Map<string, { at: number, body: object }>} */
const idempotencyCache = new Map();

function clientIp(req) {
  const xf = String(req.headers["x-forwarded-for"] || "")
    .split(",")[0]
    .trim();
  return xf || req.ip || req.socket?.remoteAddress || "unknown";
}

export function freightRateLimit(req, res, next) {
  const ip = clientIp(req);
  const now = Date.now();
  const prev = (hitsByIp.get(ip) || []).filter((t) => now - t < RATE_WINDOW_MS);
  prev.push(now);
  hitsByIp.set(ip, prev);
  if (prev.length > RATE_MAX) {
    return res.status(429).json({
      success: false,
      error: "Too many requests. Please wait a minute and try again.",
    });
  }
  next();
}

export function makeRequestId() {
  const d = new Date();
  const stamp = d.toISOString().replace(/[-:TZ.]/g, "").slice(0, 14);
  return `FQ-${stamp}-${crypto.randomBytes(3).toString("hex").toUpperCase()}`;
}

export function getIdempotentResponse(key) {
  if (!key) return null;
  const row = idempotencyCache.get(key);
  if (!row) return null;
  if (Date.now() - row.at > IDEMPOTENCY_TTL_MS) {
    idempotencyCache.delete(key);
    return null;
  }
  return row.body;
}

export function setIdempotentResponse(key, body) {
  if (!key) return;
  idempotencyCache.set(key, { at: Date.now(), body });
  if (idempotencyCache.size > 500) {
    const cutoff = Date.now() - IDEMPOTENCY_TTL_MS;
    for (const [k, v] of idempotencyCache) {
      if (v.at < cutoff) idempotencyCache.delete(k);
    }
  }
}

export function buildIdempotencyKey(submission) {
  if (submission.idempotency_key) return submission.idempotency_key;
  const basis = [
    submission.customer_email,
    submission.zip,
    submission.request_mode,
    submission.delivery_path,
    (submission.items || []).map((i) => `${i.title}:${i.width}x${i.depth}x${i.height}:${i.weight}`).join("|"),
  ].join("::");
  return crypto.createHash("sha256").update(basis).digest("hex").slice(0, 32);
}
