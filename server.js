import express from "express";
import axios from "axios";
import cors from "cors";
import crypto from "crypto";
import dotenv from "dotenv";
import fs from "fs";
import nodemailer from "nodemailer";
import twilio from "twilio";
import { CATEGORY_KEYWORDS } from "./categoryKeywords.js";
import { CATEGORY_KEYWORDS_FURNITURE, CATEGORY_KEYWORDS_FURNITURE_WEAK } from "./categoryKeywordsFurniture.js";
import { detectBrandFromProduct } from "./brand.js";
import { detectBrandFromProductFurniture } from "./brandFurniture.js";
import { classifyWithLLM } from "./llmVerticalClassifier.js";
import { classifyCategoryWithLLM } from "./llmCategoryClassifier.js";

dotenv.config();

/* ======================================================
   DUPLICATE PLACEMENT — Email alert (Gmail SMTP)
   Env: GMAIL_SMTP_USER, GMAIL_SMTP_PASSWORD, REPORT_EMAIL_TO (comma-separated)
====================================================== */
/** @param {Set<string>} [duplicateEmailSentFor] - Per-run dedupe: only one email per shopifyProductId per run. We also persist sent IDs so we never send again for the same item unless a significant change happens. */
async function sendDuplicatePlacementEmail(conflictLog, duplicateEmailSentFor) {
  const {
    productTitle,
    shopifyProductId,
    previousVertical,
    detectedVertical,
    webflowItemIdRemoved,
    webflowIdArchived,
  } = conflictLog;
  const removedId = webflowItemIdRemoved || webflowIdArchived;
  const id = String(shopifyProductId ?? "");
  if (duplicateEmailSentFor && duplicateEmailSentFor.has(id)) {
    webflowLog("info", { event: "duplicate_placement.email_skipped", reason: "already_sent_this_run", shopifyProductId: id });
    return;
  }
  const sentPreviously = loadDuplicatePlacementSentIds();
  if (sentPreviously.has(id)) {
    webflowLog("info", { event: "duplicate_placement.email_skipped", reason: "already_sent_previous_run", shopifyProductId: id });
    return;
  }
  const to = process.env.REPORT_EMAIL_TO;
  if (!to || !process.env.GMAIL_SMTP_USER || !process.env.GMAIL_SMTP_PASSWORD) {
    webflowLog("warn", { event: "duplicate_placement.email_skipped", reason: "missing_env", REPORT_EMAIL_TO: !!to });
    return;
  }
  const recipients = to.split(",").map((e) => e.trim()).filter(Boolean);
  const subject = `[Backend / Webflow sync] Product was on the wrong site (we fixed it)`;
  const prev = String(previousVertical || "").toLowerCase();
  const prevLabel =
    prev === "luxury"
      ? "Luxury / Handbags"
      : prev === "furniture"
        ? "Furniture & Home"
        : String(previousVertical || "Unknown");
  const detectedLabel =
    String(detectedVertical || "").toLowerCase() === "luxury"
      ? "Luxury / Handbags"
      : String(detectedVertical || "").toLowerCase() === "furniture"
        ? "Furniture & Home"
        : String(detectedVertical || "Unknown");
  const intro =
    prev === "luxury"
      ? "It was listed on Luxury / Handbags, but we think it belongs on Furniture & Home. We removed the Handbags copy so it only shows in one place for now."
      : prev === "furniture"
        ? "It was listed on Furniture & Home, but we think it belongs on Luxury / Handbags. We removed the Furniture copy so it only shows in one place for now."
        : "It was listed on one site, but we think it belongs on the other site. We removed the extra copy so it only shows in one place for now.";
  const wrongGuess =
    "If we got it wrong: update the product in Traxia. Change the title so it clearly says what the item is (lamp, vase, tray, handbag, wallet, etc.). Fix tags and product type if they do not match. Then run your next sync so we can place it correctly.";
  const body = [
    `${productTitle || "(none)"}`,
    "",
    intro,
    "",
    wrongGuess,
    "",
    "Details:",
    `  Product: ${productTitle || "(none)"}`,
    `  Store product ID: ${shopifyProductId}`,
    `  Used to be on: ${prevLabel}`,
    `  We think it belongs on: ${detectedLabel}`,
    `  Backend / Webflow ID we removed (for support logs only, search by name): ${removedId || "n/a"}`,
  ].join("\n");
  const htmlBody = `
    <div style="font-family: Arial, sans-serif; line-height: 1.45; color: #111;">
      <p><strong>${String(productTitle || "(none)")}</strong></p>
      <p>${intro}</p>
      <p>${wrongGuess}</p>
      <p><strong>Details:</strong><br/>
      Product: ${String(productTitle || "(none)")}<br/>
      Store product ID: ${String(shopifyProductId || "n/a")}<br/>
      Used to be on: ${prevLabel}<br/>
      We think it belongs on: ${detectedLabel}<br/>
      Backend / Webflow ID we removed (for support logs only, search by name): ${String(removedId || "n/a")}
      </p>
    </div>
  `;

  try {
    const transporter = nodemailer.createTransport({
      host: "smtp.gmail.com",
      port: 587,
      secure: false,
      auth: {
        user: process.env.GMAIL_SMTP_USER,
        pass: process.env.GMAIL_SMTP_PASSWORD,
      },
    });
    await transporter.sendMail({
      from: process.env.GMAIL_SMTP_USER,
      to: recipients,
      subject,
      text: body,
      html: htmlBody,
    });
    webflowLog("info", { event: "duplicate_placement.email_sent", to: recipients, shopifyProductId });
    if (duplicateEmailSentFor) duplicateEmailSentFor.add(id);
    saveDuplicatePlacementSentId(id);
  } catch (err) {
    webflowLog("error", { event: "duplicate_placement.email_failed", shopifyProductId, message: err.message });
  }
}

function shopifyWriteFailureEmailText(err) {
  const status = err?.response?.status;
  const url = err?.config?.url;
  const method = String(err?.config?.method || "").toUpperCase();
  let responseBody = err?.response?.data;
  if (typeof responseBody !== "string") {
    try {
      responseBody = JSON.stringify(responseBody);
    } catch {
      responseBody = String(responseBody ?? "");
    }
  }
  return {
    status: status ?? null,
    url: url || null,
    method: method || null,
    message: String(err?.message || "Unknown error"),
    responseBody: String(responseBody || "").slice(0, 4000),
  };
}

/** Email when Shopify write retries are exhausted; same SMTP/REPORT_EMAIL_TO setup as duplicate-placement alerts. */
async function sendShopifyWriteFailureEmail(detail, perRunDedupeSet) {
  const productId = String(detail?.shopifyProductId ?? "");
  const op = String(detail?.op ?? "shopify_write");
  const dedupeKey = `${productId}:${op}`;
  if (perRunDedupeSet?.has(dedupeKey)) return;

  const to = process.env.REPORT_EMAIL_TO;
  const user = process.env.GMAIL_SMTP_USER;
  const pass = process.env.GMAIL_SMTP_PASSWORD;
  if (!to || !user || !pass) {
    webflowLog("warn", { event: "shopify_write.email_skipped", reason: "missing_env", shopifyProductId: productId || null, op });
    return;
  }

  const recipients = to.split(",").map((e) => e.trim()).filter(Boolean);
  const body = [
    "Shopify write failed after automatic retries.",
    "",
    `Operation: ${op}`,
    `Shopify product ID: ${productId || "n/a"}`,
    detail?.productTitle ? `Product title: ${detail.productTitle}` : "",
    detail?.vertical ? `Vertical: ${detail.vertical}` : "",
    detail?.attempts ? `Attempts: ${detail.attempts}` : "",
    detail?.status != null ? `HTTP status: ${detail.status}` : "",
    detail?.method ? `HTTP method: ${detail.method}` : "",
    detail?.url ? `URL: ${detail.url}` : "",
    "",
    "Error message:",
    detail?.message || "(none)",
    "",
    "Response body (truncated):",
    detail?.responseBody || "(none)",
    "",
    "This product continued through sync, but Shopify write did not complete.",
    "— Lost & Found Webflow Sync",
  ]
    .filter(Boolean)
    .join("\n");

  try {
    const transporter = nodemailer.createTransport({
      host: "smtp.gmail.com",
      port: 587,
      secure: false,
      auth: { user, pass },
    });
    await transporter.sendMail({
      from: user,
      to: recipients.join(", "),
      subject: `[Backend / Webflow sync] Shopify write failed after retries — ${productId || "unknown product"}`,
      text: body,
    });
    if (perRunDedupeSet) perRunDedupeSet.add(dedupeKey);
    webflowLog("info", { event: "shopify_write.email_sent", shopifyProductId: productId || null, op, to: recipients });
  } catch (err) {
    webflowLog("error", { event: "shopify_write.email_failed", shopifyProductId: productId || null, op, message: err.message });
  }
}

/* ======================================================
   [WEBFLOW] CENTRALIZED LOGGING — Single-line JSON, timestamp, optional requestId/elapsedMs
   LOG_LEVEL: "error" | "warn" | "info" — defaults to "info". "error" cuts most I/O to speed up frequent syncs.
====================================================== */
const LOG_LEVEL = (process.env.LOG_LEVEL || "info").toLowerCase();
const LOG_ERROR = LOG_LEVEL === "error" || LOG_LEVEL === "warn" || LOG_LEVEL === "info";
const LOG_WARN = LOG_LEVEL === "warn" || LOG_LEVEL === "info";
const LOG_INFO = LOG_LEVEL === "info";
const LOG_REQUESTS = LOG_INFO; // skip per-request logs unless info
// Default strict: do not touch Webflow when Shopify snapshot is unchanged.
const WEBFLOW_STRICT_NOOP_UPDATES = process.env.WEBFLOW_STRICT_NOOP_UPDATES !== "false";

let syncRequestId = null;
let syncStartTime = null;

function webflowLog(level, payload) {
  if (level === "error" && !LOG_ERROR) return;
  if (level === "warn" && !LOG_WARN) return;
  if (level === "info" && !LOG_INFO) return;
  const base = {
    ts: new Date().toISOString(),
    level,
    ...(syncRequestId != null && { requestId: syncRequestId }),
    ...(syncStartTime != null && { elapsedMs: Date.now() - syncStartTime }),
    ...(typeof payload === "object" && payload !== null ? payload : { message: String(payload) }),
  };
  const msg = JSON.stringify(base);
  if (level === "warn") console.warn(msg);
  else if (level === "error") console.error(msg);
  else console.log(msg);
}

function webflowRequestLog(method, url, body) {
  if (!LOG_REQUESTS) return;
  // avoid logging full request body — it can be huge and slows down sync
  webflowLog("info", { event: "request", method, url });
}

function webflowFailureLog(method, url, status, responseBody, requestBody) {
  if (!LOG_ERROR) return;
  const truncate = (o, max = 500) => {
    if (o == null) return null;
    const s = typeof o === "string" ? o : JSON.stringify(o);
    return s.length <= max ? s : s.slice(0, max) + "...";
  };
  webflowLog("error", {
    event: "failure",
    method,
    url,
    status,
    responseBody: truncate(responseBody),
    requestBody: truncate(requestBody),
  });
}

// Axios interceptors: only for api.webflow.com
const WEBFLOW_ORIGIN = "https://api.webflow.com";
const WEBFLOW_MIN_DELAY_MS = Math.max(500, parseInt(process.env.WEBFLOW_MIN_DELAY_MS || "1000", 10)); // ~60 req/min; use 600 for CMS (120/min)
const WEBFLOW_429_MAX_RETRIES = Math.min(5, Math.max(1, parseInt(process.env.WEBFLOW_429_MAX_RETRIES || "3", 10)));

let lastWebflowRequestTime = 0;

axios.interceptors.request.use(async (config) => {
  if (!config.url || !String(config.url).startsWith(WEBFLOW_ORIGIN)) return config;
  const now = Date.now();
  const elapsed = now - lastWebflowRequestTime;
  if (elapsed < WEBFLOW_MIN_DELAY_MS) {
    await new Promise((r) => setTimeout(r, WEBFLOW_MIN_DELAY_MS - elapsed));
  }
  lastWebflowRequestTime = Date.now();
  if (LOG_REQUESTS) {
    webflowRequestLog(config.method?.toUpperCase() ?? "GET", config.url, config.data);
  }
  return config;
});
axios.interceptors.response.use(
  (response) => response,
  async (err) => {
    const config = err.config;
    const url = config?.url;
    if (!url || !String(url).startsWith(WEBFLOW_ORIGIN)) return Promise.reject(err);

    const status = err.response?.status;
    const retryCount = config.__webflowRetryCount ?? 0;

    if (status === 429 && retryCount < WEBFLOW_429_MAX_RETRIES) {
      const retryAfter = err.response?.headers?.["retry-after"];
      const waitSec = retryAfter != null ? parseInt(String(retryAfter), 10) : 60;
      const waitMs = Math.min(120000, Math.max(5000, (Number.isNaN(waitSec) ? 60 : waitSec) * 1000));
      webflowLog("warn", {
        event: "webflow.429_retry",
        url: config.url,
        retryCount: retryCount + 1,
        maxRetries: WEBFLOW_429_MAX_RETRIES,
        waitMs,
        retryAfter: retryAfter ?? "default 60s",
      });
      await new Promise((r) => setTimeout(r, waitMs));
      config.__webflowRetryCount = retryCount + 1;
      return axios(config);
    }

    webflowFailureLog(
      config?.method?.toUpperCase() ?? "?",
      url,
      status,
      err.response?.data,
      config?.data
    );
    return Promise.reject(err);
  }
);

/* ======================================================
   DUAL-PIPELINE: WEBFLOW ENV CONFIG
   Luxury = existing. Furniture = new env vars.
====================================================== */
function getWebflowConfig(vertical) {
  if (vertical === "furniture") {
    return {
      collectionId: process.env.RESALE_Products_Collection_ID,
      skuCollectionId: process.env.RESALE_SKUs_Collection_ID,
      token: process.env.RESALE_TOKEN,
      siteId: process.env.RESALE_WEBFLOW_SITE_ID,
    };
  }
  return {
    collectionId: process.env.WEBFLOW_COLLECTION_ID,
    skuCollectionId: null,
    token: process.env.WEBFLOW_TOKEN,
    siteId: null,
  };
}

const app = express();
app.use(cors());
app.use(
  express.json({
    verify: (req, res, buf) => {
      req.rawBody = buf;
    },
  })
);

/**
 * When SHOPIFY_WEBHOOK_SECRET is set (Custom app → API secret key), enforces Shopify HMAC.
 * If unset, requests are accepted and a warn is logged — fine for first-time wiring; set the secret for production.
 */
function verifyShopifyHmac(req, res, next) {
  const secret = process.env.SHOPIFY_WEBHOOK_SECRET?.trim();
  if (!secret) {
    webflowLog("warn", {
      event: "shopify.webhook.hmac_skipped",
      path: req.path,
      message: "Set SHOPIFY_WEBHOOK_SECRET to enforce verification",
    });
    return next();
  }
  const hmacHeader = req.get("X-Shopify-Hmac-Sha256");
  const raw = req.rawBody;
  if (!raw || !Buffer.isBuffer(raw)) {
    webflowLog("error", { event: "shopify.webhook.bad_body", path: req.path });
    return res.status(400).send("Bad request");
  }
  if (!hmacHeader) {
    return res.status(401).send("Unauthorized");
  }
  const digest = crypto.createHmac("sha256", secret).update(raw).digest("base64");
  const a = Buffer.from(digest, "utf8");
  const b = Buffer.from(String(hmacHeader), "utf8");
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) {
    webflowLog("warn", { event: "shopify.webhook.hmac_invalid", path: req.path });
    return res.status(401).send("Unauthorized");
  }
  next();
}

/* ======================================================
   SHOPIFY ORDER WEBHOOK — Twilio SMS to all 3 team numbers
   Env: TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE
====================================================== */
const twilioClient =
  process.env.TWILIO_ACCOUNT_SID && process.env.TWILIO_AUTH_TOKEN
    ? twilio(process.env.TWILIO_ACCOUNT_SID, process.env.TWILIO_AUTH_TOKEN)
    : null;

const teamNumbers = ["+14807727004", "+14802098133", "+14807726962"];

app.post("/shopify/order", async (req, res) => {
  res.status(200).send("Webhook received");

  const order = req.body;
  if (!order || typeof order !== "object") return;

  // From Shopify Order webhook payload (see https://shopify.dev/docs/api/admin-rest/latest/resources/order)
  const orderName = order.name ?? "";
  const totalPrice = order.total_price ?? "";
  const contactEmail = order.email ?? order.contact_email ?? order.customer?.email ?? "";
  const firstName = order.customer?.first_name ?? order.shipping_address?.first_name ?? "";
  const lastName = order.customer?.last_name ?? order.shipping_address?.last_name ?? "";
  const lineItems = order.line_items ?? [];
  const firstItem = lineItems[0];
  const firstItemTitle = firstItem?.title ?? "(no item)";
  const firstItemSku = firstItem?.sku ?? "";
  const itemLine = firstItemSku ? `${firstItemTitle} (SKU: ${firstItemSku})` : firstItemTitle;
  const moreItems = lineItems.length > 1 ? ` +${lineItems.length - 1} more — see email` : "";
  const shippingAddr = order.shipping_address;
  const shippingLine = shippingAddr
    ? [shippingAddr.address1, shippingAddr.city, shippingAddr.province_code || shippingAddr.province, shippingAddr.zip].filter(Boolean).join(", ")
    : "";
  const sourceName = order.source_name ?? "";

  const message = [
    "NEW ONLINE ORDER",
    "",
    `Order: ${orderName}`,
    `Items: ${itemLine}${moreItems}`,
    `Customer: ${[firstName, lastName].filter(Boolean).join(" ").trim() || "(see email)"}`,
    `Email: ${contactEmail || "—"}`,
    shippingLine ? `Ship to: ${shippingLine}` : "",
    `Total: $${totalPrice}`,
    sourceName ? `Channel: ${sourceName}` : "",
    "",
    "Review Shopify and the Flow email for full details (shipping type, pickup vs ship, etc.). Confirm requirements and process ASAP.",
    "",
    "Validate customer ID and legitimacy before releasing. Do not ship or release until validation is complete. Questions: Jill 480-209-8133",
  ]
    .filter(Boolean)
    .join("\n");

  if (!twilioClient || !process.env.TWILIO_PHONE) {
    console.warn("Twilio not configured; order SMS not sent.", { orderName });
    return;
  }

  for (const number of teamNumbers) {
    try {
      await twilioClient.messages.create({
        body: message,
        from: process.env.TWILIO_PHONE,
        to: number,
      });
    } catch (err) {
      console.error(`Twilio SMS failed to ${number}:`, err.message);
    }
  }
});

/* ======================================================
   SHOPIFY PRODUCT WEBHOOKS — creation / update → full syncSingleProduct path (same as sync-all per SKU)
   HTTP 200 first (Shopify timeout); work continues in background.
   Debounce: Shopify sends products/create and products/update back-to-back for new SKUs; one wait collapses them
   into a single sync so we do not run two full passes (major cause of duplicate Webflow rows).
   WEBHOOK_PRODUCT_DEBOUNCE_MS: default 3500; set 0 to disable debounce.
====================================================== */
const WEBHOOK_PRODUCT_DEBOUNCE_MS = Math.max(
  0,
  parseInt(process.env.WEBHOOK_PRODUCT_DEBOUNCE_MS || "3500", 10) || 3500
);
const productWebhookDebounceTimers = new Map();

function scheduleDebouncedProductWebhookSync(shopifyProductId, triggerPath) {
  const id = String(shopifyProductId ?? "").trim();
  if (!id) return;
  if (WEBHOOK_PRODUCT_DEBOUNCE_MS <= 0) {
    void runWebhookSingleProductSync(id, triggerPath);
    return;
  }
  const prev = productWebhookDebounceTimers.get(id);
  if (prev?.timer) clearTimeout(prev.timer);
  const paths = prev?.paths ? [...prev.paths, triggerPath] : [triggerPath];
  const timer = setTimeout(() => {
    productWebhookDebounceTimers.delete(id);
    const uniquePaths = [...new Set(paths)];
    const pathLabel = uniquePaths.length > 1 ? uniquePaths.join("+") : uniquePaths[0];
    webflowLog("info", {
      event: "shopify.webhook.product_debounced",
      shopifyProductId: id,
      paths: uniquePaths,
      waitMs: WEBHOOK_PRODUCT_DEBOUNCE_MS,
    });
    void runWebhookSingleProductSync(id, pathLabel);
  }, WEBHOOK_PRODUCT_DEBOUNCE_MS);
  productWebhookDebounceTimers.set(id, { timer, paths });
}

function scheduleProductWebhookSync(req, res) {
  res.status(200).send("ok");
  const id = req.body?.id != null ? String(req.body.id) : null;
  const topic = req.get("X-Shopify-Topic") ?? "";
  const shop = req.get("X-Shopify-Shop-Domain") ?? "";
  if (!id) {
    webflowLog("warn", {
      event: "shopify.webhook.product",
      path: req.path,
      topic,
      shop,
      reason: "missing_product_id",
    });
    return;
  }
  webflowLog("info", {
    event: "shopify.webhook.product",
    path: req.path,
    topic,
    shop,
    shopifyProductId: id,
    productTitle: req.body?.title != null ? String(req.body.title).slice(0, 200) : null,
  });
  scheduleDebouncedProductWebhookSync(id, req.path);
}

app.post("/webhook/products", verifyShopifyHmac, scheduleProductWebhookSync);
app.post("/webhook/products/update", verifyShopifyHmac, scheduleProductWebhookSync);

/* ======================================================
   PATHS / CACHE SETUP
====================================================== */
// Persist across deploys (e.g. mount a volume at DATA_DIR) so we don't re-call the LLM for every product after a new build.
const DATA_DIR = process.env.DATA_DIR || "./data";
const CACHE_FILE = `${DATA_DIR}/lastSync.json`;
const DUPLICATE_EMAIL_SENT_FILE = `${DATA_DIR}/duplicate_placement_emails_sent.json`;
const WEIGHT_MISSING_EMAIL_SENT_FILE = `${DATA_DIR}/weight_missing_emails_sent.json`;
const SKU_IMAGE_FAIL_EMAIL_LAST_FILE = `${DATA_DIR}/sku_image_fail_email_last.json`;
const WEBFLOW_SKU_IMAGE_MAX_ATTEMPTS = 5;
const WEBFLOW_SKU_IMAGE_BACKOFF_MS = 5000;
const SKU_IMAGE_FAIL_EMAIL_COOLDOWN_MS = 24 * 60 * 60 * 1000;
/** One-time sold backfill marker (delete file to re-run archive for on/before cutoff). */
const SOLD_BACKFILL_DONE_FILE =
  process.env.SOLD_BACKFILL_DONE_FILE || `${DATA_DIR}/sold_retention_backfill_2026-04-02.done`;

/** Appended to Shopify/Webflow description when Furniture product has no weight in Shopify. */
const WEIGHT_VALIDATE_NOTE_HTML = '<br><br><em>(Please validate weight if weight is ever missing.)</em>';

function ensureDataDir() {
  if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
  }
}

/** IDs we've already sent a duplicate-placement email for (across runs). If they don't fix it, we won't email again unless a significant change happens. */
function loadDuplicatePlacementSentIds() {
  try {
    if (!fs.existsSync(DUPLICATE_EMAIL_SENT_FILE)) return new Set();
    const raw = fs.readFileSync(DUPLICATE_EMAIL_SENT_FILE, "utf8");
    const arr = JSON.parse(raw);
    return new Set(Array.isArray(arr) ? arr : []);
  } catch (err) {
    return new Set();
  }
}

function saveDuplicatePlacementSentId(id) {
  try {
    ensureDataDir();
    const set = loadDuplicatePlacementSentIds();
    set.add(String(id));
    fs.writeFileSync(DUPLICATE_EMAIL_SENT_FILE, JSON.stringify([...set], null, 2), "utf8");
  } catch (err) {
    webflowLog("error", { event: "duplicate_placement_sent.save_failed", message: err.message });
  }
}

function loadWeightMissingEmailSentIds() {
  try {
    if (!fs.existsSync(WEIGHT_MISSING_EMAIL_SENT_FILE)) return new Set();
    const raw = fs.readFileSync(WEIGHT_MISSING_EMAIL_SENT_FILE, "utf8");
    const arr = JSON.parse(raw);
    return new Set(Array.isArray(arr) ? arr : []);
  } catch {
    return new Set();
  }
}

function saveWeightMissingEmailSentIds(set) {
  try {
    ensureDataDir();
    fs.writeFileSync(WEIGHT_MISSING_EMAIL_SENT_FILE, JSON.stringify([...set], null, 2), "utf8");
  } catch (err) {
    webflowLog("error", { event: "weight_missing_sent.save_failed", message: err.message });
  }
}

function clearWeightMissingEmailSentId(shopifyProductId) {
  const set = loadWeightMissingEmailSentIds();
  if (set.delete(String(shopifyProductId))) saveWeightMissingEmailSentIds(set);
}

/** One email per product until weight is added; uses same Gmail SMTP as duplicate-placement alerts. */
async function sendMissingWeightAlertEmail(product, dimensions, verticalLabel) {
  const shopifyProductId = String(product?.id ?? "");
  if (!shopifyProductId) return;
  const sent = loadWeightMissingEmailSentIds();
  if (sent.has(shopifyProductId)) return;

  const user = process.env.GMAIL_SMTP_USER;
  const pass = process.env.GMAIL_SMTP_PASSWORD;
  if (!user || !pass) {
    webflowLog("warn", { event: "weight_missing.email_skipped", reason: "missing_smtp", shopifyProductId });
    return;
  }

  const store = process.env.SHOPIFY_STORE || "";
  const adminUrl = store ? `https://admin.shopify.com/store/${store}/products/${shopifyProductId}` : "";
  const title = product?.title || "(no title)";
  const dims = dimensions || {};
  const dimSummary = ["width", "height", "length", "weight"]
    .map((k) => (dims[k] != null && !Number.isNaN(dims[k]) ? `${k}: ${dims[k]}` : null))
    .filter(Boolean)
    .join(", ");

  const label = verticalLabel || "Product";
  const recipients = ["info@lostandfoundresale.com", "berberbernis21@gmail.com"];
  const body = [
    `A ${label} product is missing weight in Shopify (variant weight / tags / metafields).`,
    "Please add weight (variant or tags, e.g. Weight: 2 lb) so Webflow sync and listings stay complete.",
    "",
    `Product: ${title}`,
    `Shopify product ID: ${shopifyProductId}`,
    adminUrl ? `Admin: ${adminUrl}` : "",
    dimSummary ? `Parsed dimensions: ${dimSummary}` : "Parsed dimensions: (none or incomplete)",
    "",
    "The product description will include: (Please validate weight if weight is ever missing.) until weight is set.",
    "",
    "— Lost & Found Webflow Sync",
  ]
    .filter(Boolean)
    .join("\n");

  try {
    const transporter = nodemailer.createTransport({
      host: "smtp.gmail.com",
      port: 587,
      secure: false,
      auth: { user, pass },
    });
    await transporter.sendMail({
      from: user,
      to: recipients.join(", "),
      subject: `[Webflow Sync] Missing weight — ${title.slice(0, 60)}${title.length > 60 ? "…" : ""}`,
      text: body,
    });
    sent.add(shopifyProductId);
    saveWeightMissingEmailSentIds(sent);
    webflowLog("info", { event: "weight_missing.email_sent", shopifyProductId, to: recipients });
  } catch (err) {
    webflowLog("error", { event: "weight_missing.email_failed", shopifyProductId, message: err.message });
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function loadSkuImageFailEmailLast() {
  try {
    ensureDataDir();
    if (!fs.existsSync(SKU_IMAGE_FAIL_EMAIL_LAST_FILE)) return {};
    return JSON.parse(fs.readFileSync(SKU_IMAGE_FAIL_EMAIL_LAST_FILE, "utf8"));
  } catch {
    return {};
  }
}

function saveSkuImageFailEmailLast(map) {
  try {
    ensureDataDir();
    fs.writeFileSync(SKU_IMAGE_FAIL_EMAIL_LAST_FILE, JSON.stringify(map, null, 2), "utf8");
  } catch (err) {
    webflowLog("error", { event: "sku_image_fail_email_last.save_failed", message: err.message });
  }
}

function skuImageFailEmailKey(siteId, productId) {
  return `${siteId || "?"}:${productId || "?"}`;
}

/** After repeated failures to push SKU images to Webflow, alert once per cooldown per product. */
async function sendSkuImageImportFailureEmail({ siteId, productId, skuId, op, productTitle, attempts, lastError, dedupeKey }) {
  const key = dedupeKey || skuImageFailEmailKey(siteId, productId);
  const lastMap = loadSkuImageFailEmailLast();
  const lastSent = lastMap[key];
  if (typeof lastSent === "number" && Date.now() - lastSent < SKU_IMAGE_FAIL_EMAIL_COOLDOWN_MS) {
    webflowLog("info", {
      event: "sku_image_import.email_skipped",
      reason: "cooldown_24h",
      key,
      productId,
    });
    return;
  }

  const to = process.env.REPORT_EMAIL_TO;
  const user = process.env.GMAIL_SMTP_USER;
  const pass = process.env.GMAIL_SMTP_PASSWORD;
  if (!to || !user || !pass) {
    webflowLog("warn", {
      event: "sku_image_import.email_skipped",
      reason: "missing_env",
      REPORT_EMAIL_TO: !!to,
    });
    return;
  }

  const recipients = to.split(",").map((e) => e.trim()).filter(Boolean);
  const msg = webflowApiErrorText(lastError).slice(0, 2000);
  const status = lastError?.response?.status;
  const body = [
    "Webflow could not attach SKU images after multiple automatic retries.",
    "",
    `Operation: ${op || "unknown"}`,
    `Attempts with images: ${attempts} (max ${WEBFLOW_SKU_IMAGE_MAX_ATTEMPTS})`,
    `Site ID: ${siteId || "n/a"}`,
    `Webflow product ID: ${productId || "n/a"}`,
    skuId ? `SKU ID: ${skuId}` : "",
    productTitle ? `Title (if known): ${productTitle}` : "",
    status != null ? `Last HTTP status: ${status}` : "",
    "",
    "Last error (truncated):",
    msg || "(no message)",
    "",
    "The sync continued by saving price/text without main-image / more-images / download-files for this update so the product is not stuck.",
    "You can re-upload images in Webflow or fix the source URLs in Shopify and run sync again.",
    "",
    "— Lost & Found Webflow Sync",
  ]
    .filter(Boolean)
    .join("\n");

  try {
    const transporter = nodemailer.createTransport({
      host: "smtp.gmail.com",
      port: 587,
      secure: false,
      auth: { user, pass },
    });
    await transporter.sendMail({
      from: user,
      to: recipients,
      subject: `[Webflow Sync] SKU images failed after ${attempts} tries — ${(productTitle || productId || "product").toString().slice(0, 55)}`,
      text: body,
    });
    lastMap[key] = Date.now();
    saveSkuImageFailEmailLast(lastMap);
    webflowLog("info", { event: "sku_image_import.email_sent", to: recipients, productId, siteId });
  } catch (err) {
    webflowLog("error", { event: "sku_image_import.email_failed", productId, message: err.message });
  }
}

function webflowRetryDelayMs(err, attemptIndex) {
  const ra = err?.response?.headers;
  const raw = ra?.["retry-after"] ?? ra?.["Retry-After"];
  if (raw != null) {
    const sec = parseInt(String(raw).trim(), 10);
    if (Number.isFinite(sec) && sec > 0) return Math.min(sec * 1000, 120_000);
  }
  return Math.min(WEBFLOW_SKU_IMAGE_BACKOFF_MS * attemptIndex, 30_000);
}

/** Retry same payload: remote asset import errors, rate limits, short gateway blips. */
function isWebflowSkuImagePayloadRetryableError(err) {
  if (isWebflowRemoteAssetImportError(err)) return true;
  const s = err?.response?.status;
  return s === 429 || s === 502 || s === 503;
}

/** Load cache from disk. Must persist across deploys so we only call LLM for new products or when name/description change. */
function loadCache() {
  try {
    ensureDataDir();
    if (!fs.existsSync(CACHE_FILE)) return {};
    const raw = fs.readFileSync(CACHE_FILE, "utf8");
    return JSON.parse(raw);
  } catch (err) {
    webflowLog("error", { event: "cache.load_failed", message: err.message });
    return {};
  }
}

function saveCache(cache) {
  try {
    ensureDataDir();
    fs.writeFileSync(CACHE_FILE, JSON.stringify(cache, null, 2), "utf8");
  } catch (err) {
    webflowLog("error", { event: "cache.save_failed", message: err.message });
  }
}

/**
 * Normalize old cache entries (legacy hash-only or without vertical)
 */
function getCacheEntry(cache, idStr) {
  const entry = cache[idStr];
  if (!entry) return null;

  if (typeof entry === "object" && entry.hash) {
    return { ...entry, vertical: entry.vertical ?? "luxury" };
  }

  return { hash: entry, webflowId: null, lastQty: null, vertical: "luxury" };
}

/* ======================================================
   WEBFLOW DIRECT LOOKUP (parameterized by collection)
   CMS only. For ecommerce (Furniture) use getWebflowEcommerceProductById.
====================================================== */
async function getWebflowItemById(itemId, config) {
  if (!itemId || !config?.collectionId || !config?.token) return null;
  try {
    const url = `https://api.webflow.com/v2/collections/${config.collectionId}/items/${itemId}`;
    const response = await axios.get(url, {
      headers: {
        Authorization: `Bearer ${config.token}`,
        accept: "application/json",
      },
    });
    return response.data;
  } catch (err) {
    if (err.response?.status === 404) {
      console.warn(`⚠️ Webflow item ${itemId} not found by ID.`);
      return null;
    }
    console.error("⚠️ getWebflowItemById error:", err.toString());
    return null;
  }
}

/* ======================================================
   WEBFLOW ECOMMERCE API (Furniture — uses site_id, not collection_id)
   Required: ecommerce:write scope. Products collection is ecommerce.
====================================================== */
async function getWebflowEcommerceProductById(siteId, productId, token) {
  if (!siteId || !productId || !token) return null;
  try {
    const url = `https://api.webflow.com/v2/sites/${siteId}/products/${productId}`;
    const response = await axios.get(url, {
      headers: { Authorization: `Bearer ${token}`, accept: "application/json" },
    });
    const data = response.data;
    // API returns { product, skus }; normalize so caller always has .id and .skus
    const product = data.product ?? data;
    const skus = data.skus ?? product.skus ?? [];
    return { ...product, id: product.id ?? productId, skus };
  } catch (err) {
    if (err.response?.status === 404) return null;
    webflowLog("error", { event: "getWebflowEcommerceProductById.error", siteId, productId, message: err.message, responseData: err.response?.data });
    return null;
  }
}

/** Run-scoped index: Furniture ecommerce products. Populated at sync start for O(1) lookup. */
let furnitureProductIndex = null;

async function findExistingWebflowEcommerceProduct(shopifyProductId, slug, config, productNameForFallback = null) {
  if (!config?.siteId || !config?.token) return null;
  const slugNorm = slug ? String(slug).trim() : null;
  const ensureLive = async (entry, source) => {
    if (!entry?.id) return null;
    const live = await getWebflowEcommerceProductById(config.siteId, entry.id, config.token);
    if (live) return entry;
    if (furnitureProductIndex) {
      try {
        const fd = entry.fieldData || {};
        const byIdKey = fd["shopify-product-id"] ? String(fd["shopify-product-id"]) : null;
        const bySlugKey = (fd["slug"] || fd["shopify-slug-2"])
          ? String(fd["slug"] || fd["shopify-slug-2"]).trim()
          : null;
        if (byIdKey) furnitureProductIndex.byShopifyId?.delete(byIdKey);
        if (bySlugKey) furnitureProductIndex.bySlug?.delete(bySlugKey);
        const nm = normalizeProductNameForIndex(fd.name ?? entry.name ?? "");
        if (nm) furnitureProductIndex.byName?.delete(nm);
      } catch {}
    }
    webflowLog("warn", {
      event: "furniture_find.index_stale_entry",
      source,
      shopifyProductId,
      staleWebflowId: entry.id,
      message: "Indexed furniture entry was missing live; purged stale index mapping and falling back to live scan",
    });
    return null;
  };

  // Use pre-loaded index when available; if Shopify ID not there, still scan API (index can be stale vs other workers / new rows).
  if (furnitureProductIndex) {
    const byId = furnitureProductIndex.byShopifyId?.get(String(shopifyProductId));
    if (byId) {
      const live = await ensureLive(byId, "byShopifyId");
      if (live) return live;
    }
    if (slugNorm) {
      const bySlug = furnitureProductIndex.bySlug?.get(slugNorm);
      if (bySlug) {
        const live = await ensureLive(bySlug, "bySlug");
        if (live) return live;
      }
    }
    if (productNameForFallback && furnitureProductIndex.byName) {
      const nameKey = normalizeProductNameForIndex(productNameForFallback);
      const byName = furnitureProductIndex.byName.get(nameKey);
      if (byName) {
        const live = await ensureLive(byName, "byName");
        if (live) {
          webflowLog("info", { event: "furniture_find_by_name", shopifyProductId, webflowId: byName.id, name: productNameForFallback });
          return live;
        }
      }
    }
    webflowLog("info", {
      event: "furniture_find.index_miss_live_scan",
      shopifyProductId,
      message: "Shopify ID not in run index; paginating Webflow products API",
    });
  }

  let offset = 0;
  const limit = 100;
  while (true) {
    const url = `https://api.webflow.com/v2/sites/${config.siteId}/products?limit=${limit}&offset=${offset}`;
    let response;
    try {
      response = await axios.get(url, {
        headers: { Authorization: `Bearer ${config.token}`, accept: "application/json" },
      });
    } catch (err) {
      webflowLog("error", { event: "ecommerce_list.error", siteId: config.siteId, message: err.message, responseData: err.response?.data });
      return null;
    }
    const raw = response.data.products ?? response.data.items ?? [];
    const listItems = Array.isArray(raw) ? raw : [];
    for (const listItem of listItems) {
      const product = listItem.product ?? listItem;
      const skus = listItem.skus ?? product.skus ?? [];
      const fd = product.fieldData || {};
      const wfId = fd["shopify-product-id"] ? String(fd["shopify-product-id"]) : null;
      const wfSlug = (fd["slug"] || fd["shopify-slug-2"]) ? String(fd.slug || fd["shopify-slug-2"]).trim() : null;
      if (wfId && String(wfId) === String(shopifyProductId)) return { ...product, skus };
      if (slugNorm && wfSlug && wfSlug === slugNorm) return { ...product, skus };
    }
    if (listItems.length < limit) break;
    offset += limit;
  }
  return null;
}

const WEBFLOW_ITEM_REF_REGEX = /^[a-f0-9]{24}$/i;

async function createWebflowEcommerceProduct(siteId, productFieldData, skuFieldData, token) {
  const url = `https://api.webflow.com/v2/sites/${siteId}/products`;
  const productData = sanitizeCategoryForWebflow({ ...productFieldData });
  const skuData = sanitizeCategoryForWebflow({ ...skuFieldData });
  const headers = { Authorization: `Bearer ${token}`, "Content-Type": "application/json" };
  const productTitle = productFieldData?.name || productFieldData?.["name"] || "";
  const post = (skuFd) => {
    const payload = {
      product: { fieldData: productData },
      sku: { fieldData: sanitizeSkuNumericFields(sanitizeCategoryForWebflow({ ...skuFd })) },
      publishStatus: "staging",
    };
    return axios.post(url, payload, { headers });
  };

  if (!skuFieldDataHasRemoteAssetFields(skuData)) {
    const response = await post(skuData);
    return response.data;
  }

  let lastErr = null;
  for (let attempt = 1; attempt <= WEBFLOW_SKU_IMAGE_MAX_ATTEMPTS; attempt++) {
    try {
      const response = await post(skuData);
      return response.data;
    } catch (err) {
      lastErr = err;
      const retryable = isWebflowSkuImagePayloadRetryableError(err);
      if (retryable && attempt < WEBFLOW_SKU_IMAGE_MAX_ATTEMPTS) {
        const waitMs = webflowRetryDelayMs(err, attempt);
        webflowLog("warn", {
          event: "create.ecommerce.image_retry",
          siteId,
          attempt,
          waitMs,
          status: err.response?.status,
          message: (err.response?.data?.message || err.message || "").slice(0, 400),
        });
        await sleep(waitMs);
        continue;
      }
      if (retryable && attempt === WEBFLOW_SKU_IMAGE_MAX_ATTEMPTS) break;
      throw err;
    }
  }

  const slugForDedupe = productData?.slug || productData?.["slug"] || String(productTitle || "unknown").slice(0, 80);
  await sendSkuImageImportFailureEmail({
    siteId,
    productId: "(create)",
    skuId: null,
    op: "POST /products (create with SKU images)",
    productTitle,
    attempts: WEBFLOW_SKU_IMAGE_MAX_ATTEMPTS,
    lastError: lastErr,
    dedupeKey: `${siteId}:create:${slugForDedupe}`,
  });
  webflowLog("warn", {
    event: "create.ecommerce.remote_import_fallback_without_sku_images",
    siteId,
    message: lastErr?.response?.data?.message || lastErr?.message,
  });
  const response = await post(stripRemoteImageFieldsFromSkuFieldData(skuData));
  return response.data;
}

function sanitizeCategoryForWebflow(fieldData) {
  if (!fieldData || typeof fieldData !== "object") return fieldData;
  const out = { ...fieldData };
  const cat = out.category;
  const validRef = typeof cat === "string" && WEBFLOW_ITEM_REF_REGEX.test(cat);
  if ("category" in out && !validRef) {
    delete out.category;
  }
  return out;
}

/** Webflow rejects null for sku.fieldData.weight (and other dimension numbers); omit invalid values. */
function sanitizeSkuNumericFields(fieldData) {
  if (!fieldData || typeof fieldData !== "object") return fieldData;
  const out = { ...fieldData };
  for (const key of ["weight", "width", "height", "length"]) {
    const v = out[key];
    if (v === null || v === undefined || (typeof v === "number" && Number.isNaN(v))) {
      delete out[key];
    }
  }
  return out;
}

/** SKU fields that make Webflow fetch remote URLs (Shopify CDN often fails their importer). */
const WEBFLOW_SKU_REMOTE_ASSET_KEYS = ["main-image", "more-images", "download-files"];

function stripRemoteImageFieldsFromSkuFieldData(fieldData) {
  if (!fieldData || typeof fieldData !== "object") return fieldData;
  const out = { ...fieldData };
  for (const k of WEBFLOW_SKU_REMOTE_ASSET_KEYS) {
    delete out[k];
  }
  return out;
}

function skuFieldDataHasRemoteAssetFields(fieldData) {
  if (!fieldData || typeof fieldData !== "object") return false;
  return WEBFLOW_SKU_REMOTE_ASSET_KEYS.some((k) => fieldData[k] != null);
}

function webflowApiErrorText(err) {
  const d = err?.response?.data;
  if (d == null) return String(err?.message || "");
  if (typeof d === "string") return d;
  if (typeof d.message === "string") return d.message;
  try {
    return JSON.stringify(d);
  } catch {
    return String(d);
  }
}

/** Webflow returns 400 when their servers cannot pull a remote image into assets. */
function isWebflowRemoteAssetImportError(err) {
  const status = err.response?.status;
  if (status !== 400 && status !== 422) return false;
  const msg = webflowApiErrorText(err).toLowerCase();
  return (
    msg.includes("remote file failed") ||
    msg.includes("failed to import") ||
    msg.includes("remote asset") ||
    msg.includes("failed to fetch") ||
    msg.includes("cdn.shopify.com")
  );
}

function isWebflowDuplicateSlugError(err) {
  const status = err?.response?.status;
  const details = err?.response?.data?.details;
  if (status !== 400 || !Array.isArray(details)) return false;
  return details.some(
    (d) =>
      d?.param === "slug" &&
      /unique value is already in database/i.test(String(d?.description || ""))
  );
}

async function updateWebflowEcommerceProduct(siteId, productId, fieldData, token, _existingProduct = null) {
  const url = `https://api.webflow.com/v2/sites/${siteId}/products/${productId}`;
  let data = sanitizeCategoryForWebflow({ ...fieldData });
  // Always load current product before PATCH: list/index payloads can omit isArchived, and a body without
  // isArchived can clear archive / disturb publish state when Webflow merges the update.
  webflowLog("info", { event: "product.patch.prefetch", productId, reason: "authoritative sku + isArchived" });
  const current = await getWebflowEcommerceProductById(siteId, productId, token);
  const currentProductFieldData =
    current?.fieldData && typeof current.fieldData === "object" ? current.fieldData : {};
  if (fieldDataEffectivelyEqual(data, currentProductFieldData)) {
    webflowLog("info", {
      event: "product.patch.skipped_unchanged",
      productId,
      message: "No product field changes detected; skipping Webflow PATCH",
    });
    return;
  }
  let skuFieldData = current?.skus?.[0]?.fieldData;
  if (skuFieldData == null || typeof skuFieldData !== "object") {
    webflowLog("info", { event: "product.patch.sku_empty_after_prefetch", productId });
    skuFieldData = {};
  }
  skuFieldData = sanitizeSkuNumericFields(sanitizeCategoryForWebflow({ ...skuFieldData }));
  const preserveArchived = current?.isArchived === true;
  const body = {
    product: { fieldData: data, ...(preserveArchived ? { isArchived: true } : {}) },
    sku: { fieldData: skuFieldData },
  };
  body.product.fieldData = sanitizeCategoryForWebflow(body.product.fieldData);
  body.sku.fieldData = sanitizeSkuNumericFields(sanitizeCategoryForWebflow(body.sku.fieldData));
  const productTitle = data?.name || data?.["name"] || "";
  webflowLog("info", {
    event: "product.patch.calling",
    method: "PATCH",
    url,
    productId,
    bodyKeys: ["product", "sku"],
    preserveArchived,
  });
  const patchHeaders = { Authorization: `Bearer ${token}`, "Content-Type": "application/json" };
  const skuPrepared = body.sku.fieldData;
  const patchCombined = (skuFd) =>
    axios.patch(
      url,
      {
        product: body.product,
        sku: { fieldData: sanitizeSkuNumericFields(sanitizeCategoryForWebflow({ ...skuFd })) },
      },
      { headers: patchHeaders }
    );

  if (!skuFieldDataHasRemoteAssetFields(skuPrepared)) {
    await patchCombined(skuPrepared);
    return;
  }

  let lastErr = null;
  for (let attempt = 1; attempt <= WEBFLOW_SKU_IMAGE_MAX_ATTEMPTS; attempt++) {
    try {
      await patchCombined(skuPrepared);
      return;
    } catch (err) {
      lastErr = err;
      const retryable = isWebflowSkuImagePayloadRetryableError(err);
      if (retryable && attempt < WEBFLOW_SKU_IMAGE_MAX_ATTEMPTS) {
        const waitMs = webflowRetryDelayMs(err, attempt);
        webflowLog("warn", {
          event: "product.patch.image_retry",
          productId,
          attempt,
          waitMs,
          status: err.response?.status,
          message: (err.response?.data?.message || err.message || "").slice(0, 400),
        });
        await sleep(waitMs);
        continue;
      }
      if (retryable && attempt === WEBFLOW_SKU_IMAGE_MAX_ATTEMPTS) break;
      throw err;
    }
  }

  const defaultSkuId = current?.skus?.[0]?.id;
  await sendSkuImageImportFailureEmail({
    siteId,
    productId,
    skuId: defaultSkuId,
    op: "PATCH /products/{id} (combined product + SKU)",
    productTitle,
    attempts: WEBFLOW_SKU_IMAGE_MAX_ATTEMPTS,
    lastError: lastErr,
  });
  webflowLog("warn", {
    event: "product.patch.remote_import_fallback_without_sku_images",
    productId,
    message: lastErr?.response?.data?.message || lastErr?.message,
  });
  const strippedSku = stripRemoteImageFieldsFromSkuFieldData({ ...skuPrepared });
  await patchCombined(strippedSku);
}

async function updateWebflowEcommerceSku(siteId, productId, skuId, fieldData, token) {
  if (!siteId || !productId || !skuId || !token) {
    webflowLog("warn", { event: "sku.patch.skipped", reason: "missing_params", siteId: !!siteId, productId: !!productId, skuId: !!skuId, token: !!token });
    return;
  }
  if (fieldData == null || typeof fieldData !== "object") {
    webflowLog("warn", { event: "sku.patch.skipped", reason: "invalid_fieldData", productId, skuId });
    return;
  }
  const url = `https://api.webflow.com/v2/sites/${siteId}/products/${productId}/skus/${skuId}`;
  const fullFd = sanitizeSkuNumericFields({ ...fieldData });
  const patchHeaders = { Authorization: `Bearer ${token}`, "Content-Type": "application/json" };
  webflowLog("info", { event: "sku.patch.calling", method: "PATCH", url, productId, skuId, bodyKeys: ["sku"] });

  if (!skuFieldDataHasRemoteAssetFields(fullFd)) {
    await axios.patch(url, { sku: { fieldData: fullFd } }, { headers: patchHeaders });
    return;
  }

  let lastErr = null;
  for (let attempt = 1; attempt <= WEBFLOW_SKU_IMAGE_MAX_ATTEMPTS; attempt++) {
    try {
      await axios.patch(url, { sku: { fieldData: fullFd } }, { headers: patchHeaders });
      return;
    } catch (err) {
      lastErr = err;
      const retryable = isWebflowSkuImagePayloadRetryableError(err);
      if (retryable && attempt < WEBFLOW_SKU_IMAGE_MAX_ATTEMPTS) {
        const waitMs = webflowRetryDelayMs(err, attempt);
        webflowLog("warn", {
          event: "sku.patch.image_retry",
          productId,
          skuId,
          attempt,
          waitMs,
          status: err.response?.status,
          message: (err.response?.data?.message || err.message || "").slice(0, 400),
        });
        await sleep(waitMs);
        continue;
      }
      if (retryable && attempt === WEBFLOW_SKU_IMAGE_MAX_ATTEMPTS) break;
      throw err;
    }
  }

  await sendSkuImageImportFailureEmail({
    siteId,
    productId,
    skuId,
    op: "PATCH /products/{id}/skus/{skuId}",
    productTitle: fieldData?.name || fieldData?.["name"] || "",
    attempts: WEBFLOW_SKU_IMAGE_MAX_ATTEMPTS,
    lastError: lastErr,
  });
  webflowLog("warn", {
    event: "sku.patch.remote_import_fallback_without_images",
    productId,
    skuId,
    message: lastErr?.response?.data?.message || lastErr?.message,
  });
  const stripped = sanitizeSkuNumericFields(stripRemoteImageFieldsFromSkuFieldData({ ...fullFd }));
  await axios.patch(url, { sku: { fieldData: stripped } }, { headers: patchHeaders });
}

/** Sync default SKU for ecommerce product (price, images, weight, dimensions). */
async function syncFurnitureEcommerceSku(product, webflowProductId, config) {
  webflowLog("info", { event: "syncFurnitureEcommerceSku.entry", shopifyProductId: product?.id, webflowProductId, hasSiteId: !!config?.siteId, hasToken: !!config?.token });
  if (!config?.siteId || !config?.token) {
    webflowLog("warn", { event: "syncFurnitureEcommerceSku.skipped", reason: "missing_config", webflowProductId });
    return;
  }
  const full = await getWebflowEcommerceProductById(config.siteId, webflowProductId, config.token);
  if (full?.isArchived === true) {
    webflowLog("info", { event: "syncFurnitureEcommerceSku.skipped", reason: "product_archived", webflowProductId });
    return;
  }
  const skus = full?.skus ?? [];
  const defaultSku = skus[0];
  if (!defaultSku?.id) {
    webflowLog("warn", { event: "syncFurnitureEcommerceSku.skipped", reason: "no_default_sku", webflowProductId, skusCount: skus.length });
    return;
  }
  const price = product.variants?.[0]?.price;
  const priceCents = price != null && price !== "" ? Math.round(parseFloat(price) * 100) : null;
  const dimensions = getDimensionsFromProduct(product);
  const allImages = (product.images || []).map((img) => img.src);
  const mainImageUrl = allImages[0] || null;
  const moreImagesUrls = allImages.slice(1);

  const existingFd = defaultSku.fieldData || {};
  const previousPriceCents = webflowSkuMoneyFieldToCents(existingFd.price);
  const compareSlug = getFurnitureSkuCompareAtSlug();

  const fieldData = {
    price: priceCents != null ? { value: priceCents, unit: "USD" } : null,
    ...skuDimensionFields(dimensions),
    "main-image": mainImageUrl ? { url: mainImageUrl } : null,
    "more-images": moreImagesUrls.length > 0 ? moreImagesUrls.slice(0, 10).map((url) => (url ? { url } : null)).filter(Boolean) : null,
  };

  // Compare-at is markdown-only: set when price goes down; never clear on price increases.
  if (priceCents != null && previousPriceCents != null && previousPriceCents > 0 && priceCents < previousPriceCents) {
    fieldData[compareSlug] = { value: previousPriceCents, unit: "USD" };
    webflowLog("info", {
      event: "syncFurnitureEcommerceSku.price_drop_compare_at",
      webflowProductId,
      shopifyProductId: product?.id,
      previousPriceCents,
      newPriceCents: priceCents,
    });
  }

  await updateWebflowEcommerceSku(config.siteId, webflowProductId, defaultSku.id, { ...defaultSku.fieldData, ...fieldData }, config.token);
  webflowLog("info", { event: "syncFurnitureEcommerceSku.exit", webflowProductId, skuId: defaultSku.id });
}

/** Archive an ecommerce product (soft-delete) so it no longer appears in the furniture store. */
async function archiveWebflowEcommerceProduct(siteId, productId, token) {
  if (!siteId || !productId || !token) return;
  const full = await getWebflowEcommerceProductById(siteId, productId, token);
  if (!full) return;
  if (full.isArchived === true) {
    webflowLog("info", { event: "archive.ecommerce_skip_already_archived", productId });
    return;
  }
  const productFieldData = full.fieldData || {};
  const skuFieldData = full?.skus?.[0]?.fieldData ?? {};
  const url = `https://api.webflow.com/v2/sites/${siteId}/products/${productId}`;
  const body = {
    product: { fieldData: productFieldData, isArchived: true },
    sku: { fieldData: skuFieldData },
  };
  webflowLog("info", { event: "archive.ecommerce", productId, message: "Archiving mistaken furniture product" });
  await axios.patch(url, body, {
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
  });
}

/**
 * Remove a Furniture ecommerce product (duplicate / wrong-vertical cleanup).
 * Tries DELETE first; on failure (405/501/network, etc.) falls back to archive (known to work on Webflow).
 * 404 on DELETE = already removed.
 */
async function deleteWebflowEcommerceProduct(siteId, productId, token) {
  if (!siteId || !productId || !token) return;
  const url = `https://api.webflow.com/v2/sites/${siteId}/products/${productId}`;
  try {
    await axios.delete(url, {
      headers: { Authorization: `Bearer ${token}`, accept: "application/json" },
    });
    webflowLog("info", { event: "delete.ecommerce_product", productId, message: "Furniture ecommerce product deleted" });
  } catch (err) {
    const status = err.response?.status;
    if (status === 404) {
      // Webflow can return 404 RouteNotFound on DELETE while product still exists.
      // Verify with GET before deciding it's gone.
      const stillExists = await getWebflowEcommerceProductById(siteId, productId, token);
      if (!stillExists) {
        webflowLog("info", {
          event: "delete.ecommerce_already_gone",
          productId,
          message: "Product already deleted or missing",
        });
        return;
      }
      webflowLog("warn", {
        event: "delete.ecommerce_delete_404_but_exists",
        productId,
        message: "DELETE returned 404 but product still exists; falling back to archive",
      });
      await archiveWebflowEcommerceProduct(siteId, productId, token);
      return;
    }
    const msg = err.response?.data?.message || err.message || String(err);
    webflowLog("warn", {
      event: "delete.ecommerce_fallback_archive",
      productId,
      status: status ?? null,
      message: msg,
    });
    await archiveWebflowEcommerceProduct(siteId, productId, token);
  }
}

/** Delete a CMS collection item (e.g. remove from Luxury when product is actually furniture). */
async function deleteWebflowCollectionItem(collectionId, itemId, token) {
  if (!collectionId || !itemId || !token) return;
  const url = `https://api.webflow.com/v2/collections/${collectionId}/items/${itemId}`;
  webflowLog("info", { event: "delete.cms_item", collectionId, itemId, message: "Removing duplicate from other vertical" });
  await axios.delete(url, {
    headers: { Authorization: `Bearer ${token}`, accept: "application/json" },
  });
}

/* ======================================================
   SHOPIFY — AUTO PUBLISH TO CHANNELS
====================================================== */

const SHOPIFY_GRAPHQL_URL = `https://${process.env.SHOPIFY_STORE}.myshopify.com/admin/api/2024-01/graphql.json`;
const SHOPIFY_WRITE_MAX_ATTEMPTS = Math.max(5, parseInt(process.env.SHOPIFY_WRITE_MAX_ATTEMPTS || "5", 10) || 5);
const SHOPIFY_WRITE_BASE_BACKOFF_MS = Math.max(500, parseInt(process.env.SHOPIFY_WRITE_BASE_BACKOFF_MS || "2000", 10) || 2000);

function isRetryableShopifyWriteError(err) {
  const status = err?.response?.status;
  if ([429, 500, 502, 503, 504].includes(status)) return true;
  const code = String(err?.code || "").toUpperCase();
  return (
    code === "ECONNABORTED" ||
    code === "ETIMEDOUT" ||
    code === "ECONNRESET" ||
    code === "EAI_AGAIN" ||
    code === "ENOTFOUND"
  );
}

function shopifyRetryDelayMs(err, attemptIndex) {
  const headers = err?.response?.headers;
  const raw = headers?.["retry-after"] ?? headers?.["Retry-After"];
  if (raw != null) {
    const sec = parseInt(String(raw).trim(), 10);
    if (Number.isFinite(sec) && sec > 0) return Math.min(sec * 1000, 120_000);
  }
  return Math.min(SHOPIFY_WRITE_BASE_BACKOFF_MS * attemptIndex, 30_000);
}

async function postShopifyGraphqlWithRetry(body, op, meta = {}) {
  let lastErr = null;
  for (let attempt = 1; attempt <= SHOPIFY_WRITE_MAX_ATTEMPTS; attempt++) {
    try {
      return await axios.post(
        SHOPIFY_GRAPHQL_URL,
        body,
        {
          headers: {
            "X-Shopify-Access-Token": process.env.SHOPIFY_ACCESS_TOKEN,
            "Content-Type": "application/json",
          },
        }
      );
    } catch (err) {
      lastErr = err;
      const retryable = isRetryableShopifyWriteError(err);
      if (retryable && attempt < SHOPIFY_WRITE_MAX_ATTEMPTS) {
        const waitMs = shopifyRetryDelayMs(err, attempt);
        webflowLog("warn", {
          event: "shopify.graphql.retry",
          op,
          attempt,
          waitMs,
          status: err?.response?.status ?? null,
          message: (err?.message || "").slice(0, 280),
          ...meta,
        });
        await sleep(waitMs);
        continue;
      }
      err._retryAttempts = attempt;
      throw err;
    }
  }
  if (lastErr) lastErr._retryAttempts = SHOPIFY_WRITE_MAX_ATTEMPTS;
  throw lastErr || new Error(`Shopify GraphQL failed for ${op}`);
}

let cachedPublicationIds = null;

async function getPublicationIds() {
  if (cachedPublicationIds) return cachedPublicationIds;

  const query = `
    {
      publications(first: 20) {
        edges {
          node {
            id
            name
          }
        }
      }
    }
  `;

  const resp = await axios.post(
    SHOPIFY_GRAPHQL_URL,
    { query },
    {
      headers: {
        "X-Shopify-Access-Token": process.env.SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json",
      },
    }
  );

  const all = resp.data?.data?.publications?.edges || [];

  cachedPublicationIds = all
    .filter((e) => {
      const n = e.node.name.toLowerCase();
      return (
        n.includes("online") ||
        n.includes("instagram") ||
        n.includes("facebook") ||
        n.includes("shop") ||
        n.includes("buy button")
      );
    })
    .map((e) => e.node.id);

  return cachedPublicationIds;
}

async function publishToSalesChannels(productId) {
  const pubIds = await getPublicationIds();
  for (const publicationId of pubIds) {
    const mutation = `
      mutation {
        publishablePublish(
          id: "gid://shopify/Product/${productId}"
          input: { publicationId: "${publicationId}" }
        ) {
          userErrors { message }
        }
      }
    `;

    try {
      await axios.post(
        SHOPIFY_GRAPHQL_URL,
        { query: mutation },
        {
          headers: {
            "X-Shopify-Access-Token": process.env.SHOPIFY_ACCESS_TOKEN,
            "Content-Type": "application/json",
          },
        }
      );
    } catch (err) {
      webflowLog("error", { event: "publishing.error", message: err.message, responseData: err.response?.data });
    }
  }
}

/* ======================================================
   SHOPIFY — WRITE METAFIELDS (department, category, vertical, dimensions_status)
   Department = parent (Furniture & Home | Luxury Goods). Category = child (Living Room, Handbags, etc.).
   Furniture & Home and Luxury Goods are mutually exclusive: furniture gets furniture_and_home = category, luxury_goods = "";
   luxury gets furniture_and_home = "", luxury_goods = category. Collection rules use these.
   Override Furniture & Home metafield: FURNITURE_AND_HOME_METAFIELD_NAMESPACE, FURNITURE_AND_HOME_METAFIELD_KEY
====================================================== */
const FURNITURE_AND_HOME_NAMESPACE = (process.env.FURNITURE_AND_HOME_METAFIELD_NAMESPACE || "custom").trim() || "custom";
const FURNITURE_AND_HOME_KEY = (process.env.FURNITURE_AND_HOME_METAFIELD_KEY || "furniture_category").trim() || "furniture_category";

async function updateShopifyMetafields(productId, { department, category, vertical, dimensionsStatus }) {
  const ownerId = `gid://shopify/Product/${productId}`;
  const dept = department ?? "";
  const cat = category ?? "";
  const vert = vertical ?? "luxury";
  const isFurniture = dept === "Furniture & Home";
  // "category" metafield only accepts luxury options (Handbags, Totes, etc.). Furniture uses furniture_and_home only.
  const metafields = [
    { ownerId, key: "department", namespace: "custom", type: "single_line_text_field", value: dept },
    ...(!isFurniture ? [{ ownerId, key: "category", namespace: "custom", type: "single_line_text_field", value: cat || "Other " }] : []),
    { ownerId, key: "vertical", namespace: "custom", type: "single_line_text_field", value: vert },
    { ownerId, key: "product_type_group", namespace: "custom", type: "single_line_text_field", value: dept },
    // Furniture & Home: Living Room, Bedroom, Accessories, etc. (use env vars if your store's definition differs)
    ...(isFurniture ? [{ ownerId, key: FURNITURE_AND_HOME_KEY, namespace: FURNITURE_AND_HOME_NAMESPACE, type: "single_line_text_field", value: cat || "Accessories" }] : []),
    // luxury_goods: Handbags, Other , etc. (Luxury Goods dropdown)
    ...(!isFurniture ? [{ ownerId, key: "luxury_goods", namespace: "custom", type: "single_line_text_field", value: cat || "Other " }] : []),
  ].filter((m) => m.value != null && String(m.value).trim() !== "");
  if (dimensionsStatus != null && String(dimensionsStatus).trim() !== "") {
    metafields.push({
      ownerId,
      key: "dimensions_status",
      namespace: "custom",
      type: "single_line_text_field",
      value: dimensionsStatus,
    });
  }

  const mutation = `
    mutation MetafieldsSet($metafields: [MetafieldsSetInput!]!) {
      metafieldsSet(metafields: $metafields) {
        metafields { key value }
        userErrors { field message }
      }
    }
  `;
  const res = await postShopifyGraphqlWithRetry(
    { query: mutation, variables: { metafields } },
    "metafieldsSet",
    { productId }
  );
  const errors = res.data?.data?.metafieldsSet?.userErrors ?? [];
  if (errors.length > 0) {
    const msg = errors.map((e) => `${e.field}: ${e.message}`).join("; ");
    webflowLog("error", {
      event: "metafields_set.user_errors",
      productId,
      userErrors: errors,
      furnitureMetafield: isFurniture ? { namespace: FURNITURE_AND_HOME_NAMESPACE, key: FURNITURE_AND_HOME_KEY, value: cat || "Accessories" } : null,
      hint: "If Furniture & Home fails: check Shopify Settings → Custom data → Products for the metafield's exact namespace and key, then set FURNITURE_AND_HOME_METAFIELD_NAMESPACE and FURNITURE_AND_HOME_METAFIELD_KEY.",
    });
    throw new Error(`Shopify metafieldsSet failed: ${msg}`);
  }
}
/* ======================================================
   SHOPIFY — WRITE our logic TO Shopify (vendor, productType, tags)
   We decide vertical/category; we WRITE that to Shopify so Shopify matches Webflow. Tags = department + category.
====================================================== */
const SYNC_DEPARTMENT_TAGS = ["Furniture & Home", "Luxury Goods"];
/** Shorthand tags merchants or old setups use — we strip these whenever we rewrite department/category so they do not fight automation (e.g. "Luxury" vs "Luxury Goods"). */
const LEGACY_VERTICAL_SHORTHAND_TAGS = ["Luxury", "Furniture"];
const SYNC_CATEGORY_TAGS = [
  "Living Room", "Dining Room", "Office Den", "Rugs", "Art / Mirrors", "Bedroom", "Accessories", "Outdoor / Patio", "Lighting",
  "Handbags", "Totes", "Crossbody", "Wallets", "Backpacks", "Luggage", "Scarves", "Belts", "Small Bags", "Other ", "Other",
  "Recently Sold",
];
function mergeProductTagsForSync(existingTags, department, category) {
  const existing = Array.isArray(existingTags) ? existingTags : (typeof existingTags === "string" ? existingTags.split(",").map((s) => s.trim()).filter(Boolean) : []);
  const toRemove = new Set([...SYNC_DEPARTMENT_TAGS, ...SYNC_CATEGORY_TAGS].map((t) => t.trim()).filter(Boolean));
  const shorthandRemoveLower = new Set(LEGACY_VERTICAL_SHORTHAND_TAGS.map((t) => String(t).trim().toLowerCase()).filter(Boolean));
  const kept = existing.filter((t) => {
    const s = String(t).trim();
    if (toRemove.has(s)) return false;
    if (shorthandRemoveLower.has(s.toLowerCase())) return false;
    return true;
  });
  const toAdd = [department, category].filter((v) => v != null && String(v).trim() !== "");
  const combined = [...kept];
  for (const tag of toAdd) {
    const t = String(tag).trim();
    if (t && !combined.includes(t)) combined.push(t);
  }
  return combined;
}

async function updateShopifyVendorAndType(productId, brandValue, productType, existingTags, department, category, descriptionHtml) {
  const mutation = `
    mutation UpdateProduct($input: ProductInput!) {
      productUpdate(input: $input) {
        product {
          id
          vendor
          productType
          tags
          descriptionHtml
        }
        userErrors {
          field
          message
        }
      }
    }
  `;

  const input = {
    id: `gid://shopify/Product/${productId}`,
    vendor: brandValue || "Unknown",
  };
  if (productType != null && String(productType).trim() !== "") {
    input.productType = String(productType).trim();
  }
  if (department != null && category != null) {
    input.tags = mergeProductTagsForSync(existingTags ?? [], department, category);
  }
  // Only update description if explicitly provided (not null and not empty)
  if (descriptionHtml != null && String(descriptionHtml).trim() !== "") {
    input.descriptionHtml = String(descriptionHtml).trim();
  }
  const variables = { input };

  const res = await postShopifyGraphqlWithRetry(
    { query: mutation, variables },
    "productUpdate",
    { productId }
  );
  const errors = res.data?.data?.productUpdate?.userErrors ?? [];
  if (errors.length > 0) {
    const msg = errors.map((e) => `${e.field}: ${e.message}`).join("; ");
    webflowLog("error", { event: "product_update.user_errors", productId, userErrors: errors });
    throw new Error(`Shopify productUpdate failed: ${msg}`);
  }
}

/* ======================================================
   SHOPIFY — REPLACE "CONDITION" WITH "TITLE" (Furniture only)
   Replaces "Condition" option with "Title: Default Title" for Furniture & Home products.
   This satisfies Shopify's requirement for at least one option while being eBay-compatible.
====================================================== */
async function removeConditionOptionIfFurniture(product) {
  const productId = String(product.id);
  
  // Check if product has options
  if (!product.options || !Array.isArray(product.options) || product.options.length === 0) {
    webflowLog("info", { event: "condition_option.skip", shopifyProductId: productId, reason: "no_options" });
    return;
  }

  // Find "Condition" option
  const conditionOption = product.options.find(opt => 
    opt && opt.name && String(opt.name).toLowerCase() === "condition"
  );

  if (!conditionOption || !conditionOption.id) {
    webflowLog("info", { event: "condition_option.skip", shopifyProductId: productId, reason: "condition_not_found" });
    return;
  }

  // First, query the product to get the ProductOptionValue IDs
  const queryProduct = `
    query getProduct($id: ID!) {
      product(id: $id) {
        options {
          id
          name
          optionValues {
            id
            name
          }
        }
      }
    }
  `;

  let optionValueIds = [];
  try {
    const queryRes = await axios.post(
      SHOPIFY_GRAPHQL_URL,
      { 
        query: queryProduct, 
        variables: { id: `gid://shopify/Product/${productId}` } 
      },
      {
        headers: {
          "X-Shopify-Access-Token": process.env.SHOPIFY_ACCESS_TOKEN,
          "Content-Type": "application/json",
        },
      }
    );

    const productData = queryRes.data?.data?.product;
    const conditionOptionData = productData?.options?.find(opt => 
      opt.name && String(opt.name).toLowerCase() === "condition"
    );

    if (conditionOptionData?.optionValues) {
      optionValueIds = conditionOptionData.optionValues.map(ov => ov.id);
    }
  } catch (err) {
    webflowLog("error", {
      event: "condition_option.query_error",
      shopifyProductId: productId,
      message: err.message
    });
    return;
  }

  if (optionValueIds.length === 0) {
    webflowLog("warn", {
      event: "condition_option.no_values",
      shopifyProductId: productId,
      optionId: conditionOption.id
    });
    return;
  }

  // Build the optionValuesToUpdate array with actual IDs
  const optionValuesToUpdate = optionValueIds.map(id => ({
    id,
    name: "Default Title"
  }));

  // Strategy: Use productOptionUpdate to rename option and update all its values
  const mutation = `
    mutation productOptionUpdate(
      $productId: ID!,
      $option: OptionUpdateInput!,
      $optionValuesToUpdate: [OptionValueUpdateInput!]
    ) {
      productOptionUpdate(
        productId: $productId,
        option: $option,
        optionValuesToUpdate: $optionValuesToUpdate
      ) {
        product {
          id
          options {
            id
            name
            values
          }
        }
        userErrors {
          field
          message
          code
        }
      }
    }
  `;

  const variables = {
    productId: `gid://shopify/Product/${productId}`,
    option: {
      id: `gid://shopify/ProductOption/${conditionOption.id}`,
      name: "Title"
    },
    optionValuesToUpdate
  };

  try {
    const res = await axios.post(
      SHOPIFY_GRAPHQL_URL,
      { query: mutation, variables },
      {
        headers: {
          "X-Shopify-Access-Token": process.env.SHOPIFY_ACCESS_TOKEN,
          "Content-Type": "application/json",
        },
      }
    );

    const data = res.data?.data?.productOptionUpdate;
    const userErrors = data?.userErrors ?? [];

    if (userErrors.length > 0) {
      webflowLog("warn", {
        event: "condition_option.update_errors",
        shopifyProductId: productId,
        optionId: conditionOption.id,
        userErrors
      });
      return;
    }

    const updatedProduct = data?.product;
    if (updatedProduct) {
      webflowLog("info", {
        event: "condition_option.replaced_with_title",
        shopifyProductId: productId,
        oldOptionId: conditionOption.id,
        newOptions: updatedProduct.options
      });
    } else {
      webflowLog("info", {
        event: "condition_option.update_success_no_return",
        shopifyProductId: productId,
        optionId: conditionOption.id,
        message: "Update likely succeeded but Shopify didn't return product data"
      });
    }
  } catch (err) {
    // Log but don't throw - don't fail the entire sync for this
    webflowLog("error", {
      event: "condition_option.update_error",
      shopifyProductId: productId,
      optionId: conditionOption.id,
      message: err.message,
      status: err.response?.status,
      responseData: err.response?.data
    });
  }
}

/* ======================================================
   HASH FOR CHANGE DETECTION
   Includes dimensions (variant + metafields + tag lines) so dimension changes still invalidate the fast path.
   body_html is normalized (collapse whitespace) so Shopify formatting drift doesn't cause false "changed".
   taxonomyVersion: bump this when category/vertical logic changes so all items resync once.
====================================================== */
function normalizeHtmlForHash(html) {
  if (html == null || typeof html !== "string") return html;
  return html.replace(/\s+/g, " ").trim();
}

/** Shopify often sends inventory as number or string; null = unknown / not tracked on variant. */
function normalizeInventoryQty(raw) {
  if (raw == null || raw === "") return null;
  const n = Number(raw);
  return Number.isNaN(n) ? null : n;
}

/** First variant only (same as rest of sync). Used for sold detection and shopifyHash. */
function getPrimaryVariantInventoryQuantity(product) {
  return normalizeInventoryQty(product?.variants?.[0]?.inventory_quantity);
}

function shopifyHash(product) {
  const dimensions = getDimensionsFromProduct(product);
  return {
    title: product.title,
    vendor: product.vendor,
    body_html: normalizeHtmlForHash(product.body_html),
    price: product.variants?.[0]?.price || null,
    qty: getPrimaryVariantInventoryQuantity(product),
    images: (product.images || []).map((i) => i.src),
    slug: product.handle,
    dimensions: { width: dimensions.width, height: dimensions.height, length: dimensions.length, weight: dimensions.weight },
    taxonomyVersion: 10,
  };
}

/** Hash of only name + description; used to decide if we call the LLM (only when this changes). */
function contentHashForLLM(product) {
  return {
    title: product.title || "",
    body_html: normalizeHtmlForHash(product.body_html),
    taxonomyVersion: 10,
  };
}

/* ======================================================
   CATEGORY DETECTOR
====================================================== */
function detectCategory(title) {
  if (!title) return "Other ";
  const normalized = title.toLowerCase();
  for (const [category, keywords] of Object.entries(CATEGORY_KEYWORDS)) {
    for (const kw of keywords) {
      if (normalized.includes(kw.toLowerCase())) return category;
    }
  }
  return "Other ";
}

/* ======================================================
   MAP CATEGORY TO SHOPIFY METAFIELD VALUES
   (Shopify dropdown requires exact matches)
====================================================== */
function mapCategoryForShopify(category) {
  const map = {
    Handbags: "Handbags",
    Totes: "Totes",
    Crossbody: "Crossbody",
    Wallets: "Wallets",
    Backpacks: "Backpacks",
    Luggage: "Luggage",
    Scarves: "Scarves",
    Belts: "Belts",
    Accessories: "Accessories",
    "Small Bags": "Small Bags",

    // EVERYTHING ELSE must map to "Other " WITH TRAILING SPACE
    default: "Other ",
  };

  return map[category] || map.default;
}

/* ======================================================
   FURNITURE CATEGORY — detect + map to Shopify display
   Fallback: Accessories when no keyword matches.
   Uses word-boundary matching so "art" doesn't match "smart", "print" doesn't match "blueprint", "desk" doesn't match "desktop".
====================================================== */
function matchFurnitureKeyword(normalized, keyword) {
  const k = keyword.trim().toLowerCase();
  if (!k) return false;
  const escaped = k.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const pattern = "\\b" + escaped + "\\b";
  try {
    return new RegExp(pattern, "i").test(normalized);
  } catch {
    return normalized.includes(k);
  }
}

/**
 * Apply dimension-based rules for tables. Returns overridden category or null if no override.
 * Rules (height/depth/width in inches):
 * - Dining: Height ≥ 28 AND Depth ≥ 30 AND Width ≥ 40 → Dining Room
 * - Not dining: Height ≤ 22 OR Depth ≤ 24 → Living Room (disqualifies Dining)
 */
function applyTableDimensionRules(dims, name, descAndTags) {
  const h = dims?.height != null && !Number.isNaN(dims.height) ? Number(dims.height) : null;
  const w = dims?.width != null && !Number.isNaN(dims.width) ? Number(dims.width) : null;
  const d = dims?.length != null && !Number.isNaN(dims.length) ? Number(dims.length) : null; // length = depth
  if (h == null || w == null || d == null) return null;
  const text = (name + " " + (descAndTags || "")).toLowerCase();
  if (!/\btable\b/.test(text)) return null; // only for table-like products

  // "Table lamp" / "desk lamp" contain "table" but are Lighting — shallow depth would wrongly force Living Room
  if (/\btable lamps?\b/.test(text) || /\bdesk lamps?\b/.test(text)) return null;

  // Books: "coffee table book", encyclopedia copy, or "table of contents" are not case-goods tables
  if (/\bcoffee table books?\b/.test(text)) return null;
  if (/\btable of contents\b/.test(text)) return null;
  if (/\bencyclopedia\b/.test(text)) return null;

  if (h <= 22 || d <= 24) return "LivingRoom"; // cannot be dining
  if (h >= 28 && d >= 30 && w >= 40) return "DiningRoom"; // dining table dimensions
  return null;
}

/**
 * Collapse Shopify typography so accessory title overrides still match (ZWSP, fancy hyphens, accented Latin).
 */
function normalizeTitleForFurnitureAccessoryMatch(raw) {
  return (raw || "")
    .trim()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[\u200B-\u200D\uFEFF]/g, "")
    .replace(/[\u2010-\u2015\u2212\uFF0D]/g, "-")
    .toLowerCase();
}

/**
 * Furniture subcategory: LLM often returns LivingRoom when copy mentions dining/coffee tables or “living space”.
 * When title is unambiguous, override LLM so keyword truth wins (we still call LLM for audit; cost already paid).
 */
function furnitureAccessoryCategoryOverrideTitle(title) {
  const t = normalizeTitleForFurnitureAccessoryMatch(title);
  if (!t) return null;
  // candlestick(s), candle stick(s), candle-stick(s); NFKC typography handled above
  if (/\bcandle[\s-]*sticks?\b/.test(t)) return "Accessories";
  if (/\bcandle-?holders?\b/.test(t) || /\bcandle holders?\b/.test(t)) return "Accessories";
  if (/\bpedestal bowls?\b/.test(t)) return "Accessories";
  const bowlIsChair = /\bbowl chairs?\b/.test(t);
  if (!bowlIsChair && /\bbowls?\b/.test(t)) return "Accessories";
  if (/\bvases?\b/.test(t)) return "Accessories";
  if (/\btable lamps?\b/.test(t) || /\bdesk lamps?\b/.test(t) || /\bbedside lamps?\b/.test(t)) return "Accessories";
  const trayIsFurnitureTable =
    /\btray tables?\b/.test(t) ||
    /\btv tray tables?\b/.test(t) ||
    /\bfolding tray tables?\b/.test(t) ||
    /\bbutlers? trays? tables?\b/.test(t) ||
    /\bbutler'?s trays? tables?\b/.test(t);
  if (!trayIsFurnitureTable && /\btrays?\b/.test(t)) return "Accessories";
  if (/\bdecanters?\b/.test(t) || /\bcarafes?\b/.test(t)) return "Accessories";
  return null;
}

/** Mattresses / sleep surfaces: title + description + tags. Overrides LLM "Accessories" when copy matches "pillow"/"box" from pillow-top or box spring. */
function furnitureSleepSurfaceIndicatesBedroom(title, descriptionHtml, tags) {
  const stripHtml = (html) => (html || "").replace(/<[^>]*>/g, " ").replace(/\s+/g, " ").trim();
  const name = ((title || "").trim()).toLowerCase();
  const descText = stripHtml(descriptionHtml || "").trim().toLowerCase();
  const tagsStr = Array.isArray(tags) ? tags.join(" ").toLowerCase() : typeof tags === "string" ? tags.toLowerCase() : "";
  const hay = `${name} ${descText} ${tagsStr}`;
  if (!hay.trim()) return false;
  if (/\bmattresses?\b/i.test(hay)) return true;
  if (/\bbox[\s-]?springs?\b/i.test(hay)) return true;
  if (/\bbox[\s-]?foundations?\b/i.test(hay)) return true;
  if (/\bpillow[\s-]?top\b/i.test(hay)) return true;
  if (/\beuro[\s-]?top\b/i.test(hay)) return true;
  if (/\badjustable[\s-]?(base|bed)s?\b/i.test(hay)) return true;
  if (/\bsplit[\s-]?box\b/i.test(hay)) return true;
  return false;
}

function detectCategoryFurniture(title, descriptionHtml, tags, dimensions) {
  const stripHtml = (html) => (html || "").replace(/<[^>]*>/g, " ").replace(/\s+/g, " ").trim();
  const descText = stripHtml(descriptionHtml || "").trim();
  const tagsStr = Array.isArray(tags) ? tags.join(" ") : typeof tags === "string" ? tags : "";
  const name = ((title || "").trim()).toLowerCase();
  const descAndTags = descText ? [descText, tagsStr].filter(Boolean).join(" ").toLowerCase() : "";
  const hasDesc = !!descText;

  if (!name && !descAndTags) return "Accessories";

  if (furnitureSleepSurfaceIndicatesBedroom(title, descriptionHtml, tags)) return "Bedroom";

  // Hard guard: if title/description clearly says art, never classify as Rugs.
  const artSignals = [
    " art ",
    " artwork",
    " painting",
    " paintings",
    " wall art",
    " canvas",
    " tapestry",
    " print",
    " prints",
    " lithograph",
    " giclee",
    " framed art",
    " sculpture",
  ];
  const paddedText = ` ${[name, descAndTags].filter(Boolean).join(" ")} `;
  if (artSignals.some((s) => paddedText.includes(s))) return "ArtMirrors";

  // Dimension-based override for tables (height/depth/width rules)
  const dimOverride = applyTableDimensionRules(dimensions, name, descAndTags);
  if (dimOverride != null) return dimOverride;

  const scores = {};

  for (const [category, keywords] of Object.entries(CATEGORY_KEYWORDS_FURNITURE)) {
    scores[category] = 0;
    for (const kw of keywords) {
      const isMultiWord = kw.trim().includes(" ");
      const inName = matchFurnitureKeyword(name, kw);
      const inDesc = hasDesc && matchFurnitureKeyword(descAndTags, kw);
      if (inName) scores[category] += isMultiWord ? 3 : 2;
      else if (inDesc) scores[category] += isMultiWord ? 1.5 : 1;
    }
  }

  if (CATEGORY_KEYWORDS_FURNITURE_WEAK) {
    for (const [category, weakKws] of Object.entries(CATEGORY_KEYWORDS_FURNITURE_WEAK)) {
      for (const kw of weakKws) {
        const inName = matchFurnitureKeyword(name, kw);
        const inDesc = hasDesc && matchFurnitureKeyword(descAndTags, kw);
        if (inName || inDesc) scores[category] = (scores[category] || 0) + 0.5;
      }
    }
  }

  let bestCategory = "Accessories";
  let bestScore = 0;
  for (const [category, score] of Object.entries(scores)) {
    if (score > bestScore) {
      bestScore = score;
      bestCategory = category;
    }
  }
  return bestScore > 0 ? bestCategory : "Accessories";
}

const FURNITURE_CATEGORY_TO_SHOPIFY = {
  LivingRoom: "Living Room",
  DiningRoom: "Dining Room",
  OfficeDen: "Office Den",
  Rugs: "Rugs",
  ArtMirrors: "Art / Mirrors",
  Bedroom: "Bedroom",
  Accessories: "Accessories",
  OutdoorPatio: "Outdoor / Patio",
  Lighting: "Lighting",
};

function mapFurnitureCategoryForShopify(category) {
  return FURNITURE_CATEGORY_TO_SHOPIFY[category] ?? "Accessories";
}

/* ======================================================
   DEPARTMENT & METAFIELDS FROM TYPE (authoritative)
   Do NOT invent categorization. Type is correct; derive Department and metafield from Type only.
   Priority: Department → department-specific metafield → tags. Never populate both metafields.
====================================================== */
function normalizeTypeForMatch(s) {
  if (s == null || typeof s !== "string") return "";
  return s.trim().toLowerCase().replace(/\s+/g, " ");
}

// Type signals: if Type contains these → Furniture & Home
const FURNITURE_TYPE_SIGNALS = [
  "furniture", "table", "tables", "seating", "case goods", "lighting", "decor", "mirrors", "mirror",
  "rugs", "rug", "home accessories", "living room", "dining room", "office", "bedroom",
  "art", "painting", "paintings", "outdoor", "patio", "lamps", "lamp",
];

// Type signals: if Type contains these → Luxury Goods
const LUXURY_TYPE_SIGNALS = [
  "handbag", "handbags", "bag", "bags", "wallet", "wallets", "belt", "belts",
  "scarf", "scarves", "fashion accessories", "wearable", "accessories", "tote", "totes",
  "crossbody", "backpack", "backpacks", "luggage", "clutch", "small bag",
];

/** Returns "Furniture & Home" | "Luxury Goods" | null. (Legacy; vertical is now from LLM classifier.) */
function getDepartmentFromType(productType) {
  const n = normalizeTypeForMatch(productType);
  if (!n) return null;
  for (const s of FURNITURE_TYPE_SIGNALS) {
    if (n.includes(s)) return "Furniture & Home";
  }
  for (const s of LUXURY_TYPE_SIGNALS) {
    if (n.includes(s)) return "Luxury Goods";
  }
  return null;
}

/** True when product_type is only a generic luxury bucket (accessories/wearable) with no specific bag/scarf/belt etc. */
function isGenericLuxuryTypeOnly(productType) {
  const n = normalizeTypeForMatch(productType);
  if (!n) return false;
  const generic = ["accessories", "wearable", "fashion accessories"];
  const specific = ["handbag", "handbags", "bag", "bags", "wallet", "wallets", "belt", "belts", "scarf", "scarves", "tote", "totes", "crossbody", "backpack", "backpacks", "luggage", "clutch", "small bag"];
  const hasGeneric = generic.some((s) => n.includes(s));
  const hasSpecific = specific.some((s) => n.includes(s));
  return hasGeneric && !hasSpecific;
}

/** True when title or description clearly indicate art or furniture (painting, canvas, art, mirror, rug, etc.). */
function hasFurnitureOrArtSignals(product) {
  const title = (product.title || "").toLowerCase();
  const desc = (product.body_html || "").replace(/<[^>]*>/g, " ").toLowerCase();
  const combined = [title, desc].join(" ");
  const signals = ["painting", "paintings", "canvas", "acrylic on canvas", "art", "mirror", "mirrors", "rug", "rugs", "furniture", "decor", "lamp", "lamps", "seating", "case goods", "living room", "dining room", "bedroom", "outdoor", "patio", "pillow", "pillows"];
  return signals.some((w) => combined.includes(w));
}

const LUXURY_WEARABLE_CUES = [
  "backpack", "backpacks", "belt", "belts", "wallet", "wallets", "card holder", "cardholder",
  "handbag", "handbags", "bag", "bags", "crossbody", "clutch", "purse", "tote", "wristlet",
  "shoulder bag", "satchel", "luggage", "duffle", "duffel", "briefcase",
  "bangle", "bangles", "bracelet", "bracelets",
  "necklace", "necklaces", "earring", "earrings", "brooch", "brooches",
];
const ART_CUES_STRONG = [
  "signed art", "wall art", "framed art", "painting", "paintings", "lithograph", "giclee",
  "serigraph", "sculpture", "acrylic on canvas", "oil on canvas", "mixed media", "original art",
];
const FURNITURE_HOME_CUES = [
  "lamp", "lamps", "chandelier", "sconce", "mirror", "mirrors", "rug", "rugs", "dining table",
  "coffee table", "side table", "nightstand", "dresser", "chair", "chairs", "sofa", "sectional",
  "console", "bookcase", "bedroom", "living room", "dining room", "outdoor", "patio", "decor",
];
const LIGHTING_HARD_CUES = [
  "table lamp", "floor lamp", "desk lamp", "lamp", "lamps", "chandelier", "sconce", "torchiere",
];

function textHasAnyWordCue(text, cues) {
  if (!text) return false;
  for (const cue of cues) {
    if (matchWordBoundary(text, cue)) return true;
  }
  return false;
}

/**
 * Resolve final vertical from combined evidence (name + description + dimensions + weight).
 * This runs as a guard over LLM output to prevent one-word misroutes.
 */
function resolveVerticalFromEvidence(product, llmDetectedVertical) {
  const title = (product?.title || "").trim().toLowerCase();
  const desc = (product?.body_html || "").replace(/<[^>]*>/g, " ").trim().toLowerCase();
  const text = [title, desc].filter(Boolean).join(" ");
  const dims = getDimensionsFromProduct(product || {});
  const hasDimsOrWeight = hasAnyDimensions(dims);
  const hasWearableCue = textHasAnyWordCue(text, LUXURY_WEARABLE_CUES);
  const hasStrongArtCue = textHasAnyWordCue(text, ART_CUES_STRONG);
  const hasFurnitureCue = textHasAnyWordCue(text, FURNITURE_HOME_CUES);
  const hasLightingHardCue = textHasAnyWordCue(text, LIGHTING_HARD_CUES);
  const hasGenericArtWord = matchWordBoundary(text, "art");

  // Hard-stop for obvious lighting/home terms so "ring"/"canvas" wording doesn't misroute lamps.
  if (hasLightingHardCue) {
    return { vertical: "furniture", reason: "evidence_lighting_hard_cue" };
  }

  // Wearables always stay Luxury, even if words like "canvas" appear.
  if (hasWearableCue) {
    return { vertical: "luxury", reason: "evidence_wearable_cue" };
  }
  // Art/furniture only wins when supported by meaningful context, not one isolated token.
  if (hasStrongArtCue || (hasGenericArtWord && hasFurnitureCue)) {
    return { vertical: "furniture", reason: "evidence_art_home_cues" };
  }
  if (hasFurnitureCue && hasDimsOrWeight) {
    return { vertical: "furniture", reason: "evidence_furniture_plus_dimensions" };
  }
  return { vertical: llmDetectedVertical, reason: "llm" };
}

/** True when product is obviously luxury (jewelry, earring, bracelet, pouch, or clearly a luxury accessory).
 *  IMPORTANT: Brand alone is NOT enough — we require accessory/jewelry cues so furniture/housewares from luxury brands stay in Furniture & Home.
 */
function isClearlyLuxury(product) {
  const title = (product.title || "").toLowerCase();
  const tagsStr = getProductTagsArray(product).join(" ").toLowerCase();
  const desc = (product.body_html || "").replace(/<[^>]*>/g, " ").toLowerCase();
  const combined = [title, tagsStr, desc].join(" ");
  const clearlyLuxuryWords = [
    "jewelry", "earring", "earrings", "bracelet", "bracelets", "necklace", "necklaces",
    "pouch", "barrette", "barrettes", "designer accessories", "statement jewelry", "costume jewelry", "luxury-collection"
  ];
  if (clearlyLuxuryWords.some((w) => combined.includes(w))) return true;
  // Brand-based override ONLY when it looks like a wearable/accessory (bag, clutch, scarf, wallet, belt, etc.).
  const isLuxuryBrand = detectBrandFromProduct(product.title, product.vendor);
  if (isLuxuryBrand) {
    const accessoryWords = [
      "bag", "bags", "handbag", "handbags", "tote", "totes", "crossbody",
      "wallet", "wallets", "clutch", "clutches", "backpack", "backpacks",
      "scarf", "scarves", "belt", "belts", "pouch", "small bag"
    ];
    if (accessoryWords.some((w) => combined.includes(w))) return true;
  }
  return false;
}

// Existing taxonomy: Furniture & Home metafield values (exact)
const FURNITURE_TAXONOMY = ["Living Room", "Dining Room", "Office Den", "Rugs", "Art / Mirrors", "Bedroom", "Accessories", "Outdoor / Patio", "Lighting"];
// Type → Furniture & Home metafield value (normalized type key → taxonomy value)
const TYPE_TO_FURNITURE_CATEGORY = {
  "living room": "Living Room", "dining room": "Dining Room", "office den": "Office Den", "office": "Office Den",
  "rugs": "Rugs", "rug": "Rugs", "art / mirrors": "Art / Mirrors", "art mirrors": "Art / Mirrors", "art": "Art / Mirrors", "mirrors": "Art / Mirrors", "mirror": "Art / Mirrors", "painting": "Art / Mirrors", "paintings": "Art / Mirrors",
  "bedroom": "Bedroom", "accessories": "Accessories", "outdoor / patio": "Outdoor / Patio", "outdoor": "Outdoor / Patio", "patio": "Outdoor / Patio",
  "lighting": "Lighting", "lamps": "Lighting", "lamp": "Lighting", "table": "Living Room", "tables": "Living Room", "seating": "Living Room", "decor": "Accessories", "case goods": "Living Room", "home accessories": "Accessories",
};

// Existing taxonomy: Luxury Goods metafield values (exact)
const LUXURY_TAXONOMY = ["Handbags", "Totes", "Crossbody", "Small Bags", "Backpacks", "Wallets", "Luggage", "Scarves", "Belts", "Accessories", "Other ", "Recently Sold"];
const TYPE_TO_LUXURY_CATEGORY = {
  "handbag": "Handbags", "handbags": "Handbags", "tote": "Totes", "totes": "Totes", "crossbody": "Crossbody",
  "small bag": "Small Bags", "backpack": "Backpacks", "backpacks": "Backpacks", "wallet": "Wallets", "wallets": "Wallets",
  "luggage": "Luggage", "scarf": "Scarves", "scarves": "Scarves", "belt": "Belts", "belts": "Belts",
  "clutch": "Small Bags", "bag": "Handbags", "bags": "Handbags", "fashion accessories": "Accessories", "wearable": "Accessories", "accessories": "Accessories",
  "jewelry": "Accessories", "jewellery": "Accessories", "earring": "Accessories", "earrings": "Accessories",
  "bracelet": "Accessories", "bracelets": "Accessories", "necklace": "Accessories", "necklaces": "Accessories",
  "ring": "Accessories", "rings": "Accessories", "pendant": "Accessories", "pendants": "Accessories",
  "brooch": "Accessories", "brooches": "Accessories", "barrette": "Accessories", "barrettes": "Accessories",
};

function getFurnitureCategoryFromType(productType) {
  const n = normalizeTypeForMatch(productType);
  if (!n) return "Accessories";
  return TYPE_TO_FURNITURE_CATEGORY[n] ?? "Accessories";
}

function getLuxuryCategoryFromType(productType, soldNow) {
  if (soldNow) return "Recently Sold";
  const n = normalizeTypeForMatch(productType);
  if (!n) return "Other ";
  return TYPE_TO_LUXURY_CATEGORY[n] ?? "Other ";
}

/** Word-boundary match for keyword in text. */
function matchWordBoundary(text, keyword) {
  if (!text || !keyword) return false;
  const k = String(keyword).trim().toLowerCase();
  if (!k) return false;
  const escaped = k.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  try {
    return new RegExp("\\b" + escaped + "\\b", "i").test(String(text));
  } catch {
    return String(text).toLowerCase().includes(k);
  }
}

/** Shoe keywords — explicitly route to Other (no Handbags/Totes/etc. category for footwear). */
const SHOE_KEYWORDS = [
  "sneakers", "sneaker", "pumps", "pump", "heels", "heel", "flats", "flat",
  "boots", "boot", "sandals", "sandal", "loafers", "loafer", "mules", "mule",
  "slides", "slide", "ballerina", "oxfords", "oxford", "footwear", "shoes", "shoe",
];

/** True if title/description indicate footwear — checked before product_type so shoes with type "Accessories" become Other. */
function isShoeProduct(title, descriptionHtml) {
  const stripHtml = (html) => (html || "").replace(/<[^>]*>/g, " ").replace(/\s+/g, " ").trim();
  const text = [(title || "").trim(), stripHtml(descriptionHtml || "")].filter(Boolean).join(" ").toLowerCase();
  return text && SHOE_KEYWORDS.some((kw) => matchWordBoundary(text, kw));
}

/** Jewelry and accessory keywords — title + description only: force Accessories. */
const JEWELRY_KEYWORDS = [
  "jewelry", "jewellery", "jewel", "earring", "earrings", "bracelet", "bracelets",
  "necklace", "necklaces", "ring", "rings", "pendant", "pendants", "brooch", "brooches",
  "barrette", "barrettes", "statement jewelry", "costume jewelry",
];

/** Accessory-only terms (keychains, purse hooks, bag charms) — title + description: force Accessories. */
const ACCESSORY_KEYWORDS = [
  "keychain", "keychains", "key ring", "key rings", "bag charm", "bag charms",
  "purse hook", "purse hooks", "bag hook", "bag hooks",
  "attache purse hook", "attache hook",
];

/** Belt terms — title + description: force Belts (chain belt, belt accessory, etc.). */
const BELT_KEYWORDS = [
  "belt", "belts", "chain belt", "belt accessory", "belt accessories", "waist belt", "leather belt",
];

/** Bag/agenda terms — if present, never force to Accessories; use real category (Crossbody, Handbags, etc.) or Other. */
const BAG_AGENDA_KEYWORDS = [
  "crossbody", "handbag", "handbags", "tote", "totes", "wallet", "wallets", "clutch", "backpack", "backpacks",
  "luggage", "satchel", "shoulder bag", "small bag", "pochette", "agenda", "agenda cover", "notepad", "notebook",
  "document holder", "folio", "business card case",
];

/** True if title or description indicate a bag or agenda — do NOT categorize as Accessories. */
function isBagOrAgendaProduct(title, descriptionHtml) {
  const stripHtml = (html) => (html || "").replace(/<[^>]*>/g, " ").replace(/\s+/g, " ").trim();
  const desc = stripHtml(descriptionHtml || "").trim();
  const text = [(title || "").trim(), desc].filter(Boolean).join(" ").toLowerCase();
  if (!text) return false;
  return BAG_AGENDA_KEYWORDS.some((kw) => matchWordBoundary(text, kw));
}

/** True if title or description indicate a belt — force category Belts. */
function isBeltProduct(title, descriptionHtml) {
  const stripHtml = (html) => (html || "").replace(/<[^>]*>/g, " ").replace(/\s+/g, " ").trim();
  const desc = stripHtml(descriptionHtml || "").trim();
  const text = [(title || "").trim(), desc].filter(Boolean).join(" ").toLowerCase();
  if (!text) return false;
  return BELT_KEYWORDS.some((kw) => matchWordBoundary(text, kw));
}

/** True if title or description indicate jewelry or accessory (keychain, key ring, bag charm) — force Accessories. */
function isJewelryOrAccessoryProduct(title, descriptionHtml) {
  const stripHtml = (html) => (html || "").replace(/<[^>]*>/g, " ").replace(/\s+/g, " ").trim();
  const desc = stripHtml(descriptionHtml || "").trim();
  const text = [(title || "").trim(), desc].filter(Boolean).join(" ").toLowerCase();
  if (!text) return false;
  if (JEWELRY_KEYWORDS.some((kw) => matchWordBoundary(text, kw))) return true;
  if (ACCESSORY_KEYWORDS.some((kw) => matchWordBoundary(text, kw))) return true;
  return false;
}

/** Detect luxury category from title/description when product_type is empty or unmatched. Title-first: match on title before description so accessory mentions (e.g. "comes with clutch") don't override the main product. */
function detectLuxuryCategoryFromTitle(title, descriptionHtml) {
  const stripHtml = (html) => (html || "").replace(/<[^>]*>/g, " ").replace(/\s+/g, " ").trim();
  const titleText = (title || "").trim().toLowerCase();
  const descText = stripHtml(descriptionHtml || "").trim().toLowerCase();
  const combined = [titleText, descText].filter(Boolean).join(" ");
  if (!combined) return null;
  if (SHOE_KEYWORDS.some((kw) => matchWordBoundary(combined, kw))) return null;
  if (JEWELRY_KEYWORDS.some((kw) => matchWordBoundary(combined, kw))) return "Accessories";
  if (ACCESSORY_KEYWORDS.some((kw) => matchWordBoundary(combined, kw))) return "Accessories";
  // Document holders, agendas, folios, business card cases → Other (stationery/office, not handbags).
  if (matchWordBoundary(combined, "document holder") || matchWordBoundary(combined, "agenda") || matchWordBoundary(combined, "folio") || matchWordBoundary(combined, "business card case")) {
    return "Other ";
  }
  const tryMatch = (text) => {
    if (!text) return null;
    for (const [category, keywords] of Object.entries(CATEGORY_KEYWORDS)) {
      if (!Array.isArray(keywords)) continue;
      for (const kw of keywords) {
        if (matchWordBoundary(text, kw)) return category;
      }
    }
    return null;
  };
  return tryMatch(titleText) ?? tryMatch(descText);
}

/** In-memory map: display name (and slug) -> Webflow category item ID. Filled by loadFurnitureCategoryMap(). */
let furnitureCategoryMapCache = null;

/** Fetch Categories collection from Webflow and build name/slug -> item ID map so we don't need env vars. */
async function loadFurnitureCategoryMap() {
  if (furnitureCategoryMapCache) return furnitureCategoryMapCache;
  const siteId = process.env.RESALE_WEBFLOW_SITE_ID;
  const token = process.env.RESALE_TOKEN;
  if (!siteId || !token) {
    webflowLog("info", { event: "furniture_categories.skip", reason: "missing RESALE_WEBFLOW_SITE_ID or RESALE_TOKEN" });
    return {};
  }
  try {
    const collResp = await axios.get(`https://api.webflow.com/v2/sites/${siteId}/collections`, {
      headers: { Authorization: `Bearer ${token}`, accept: "application/json" },
    });
    const collections = collResp.data?.collections ?? [];
    const categoriesColl = collections.find(
      (c) => (c.displayName && c.displayName.toLowerCase() === "categories") || (c.slug && c.slug.toLowerCase() === "category")
    );
    if (!categoriesColl?.id) {
      webflowLog("info", { event: "furniture_categories.skip", reason: "no Categories collection found" });
      return {};
    }
    const map = {};
    let offset = 0;
    const limit = 100;
    while (true) {
      const itemsResp = await axios.get(
        `https://api.webflow.com/v2/collections/${categoriesColl.id}/items`,
        { params: { limit, offset }, headers: { Authorization: `Bearer ${token}`, accept: "application/json" } }
      );
      const items = itemsResp.data?.items ?? [];
      for (const item of items) {
        const id = item.id;
        if (!id || !/^[a-f0-9]{24}$/i.test(id)) continue;
        const name = item.fieldData?.name ?? item.name;
        if (name && typeof name === "string") map[name.trim()] = id;
        const slug = item.fieldData?.slug ?? item.slug;
        if (slug && typeof slug === "string") map[slug.trim()] = id;
      }
      if (items.length < limit) break;
      offset += limit;
    }
    furnitureCategoryMapCache = map;
    webflowLog("info", { event: "furniture_categories.loaded", count: Object.keys(map).length });
    return map;
  } catch (err) {
    webflowLog("warn", { event: "furniture_categories.load_failed", message: err.message });
    return {};
  }
}

/** Pre-load Luxury CMS items once per sync → O(1) lookup instead of N×page scans. */
async function loadLuxuryItemIndex() {
  const config = getWebflowConfig("luxury");
  if (!config?.collectionId || !config?.token) return;
  const byShopifyId = new Map();
  const bySlug = new Map();
  const byUrl = new Map();
  let offset = 0;
  const limit = 100;
  while (true) {
    const url = `https://api.webflow.com/v2/collections/${config.collectionId}/items?limit=${limit}&offset=${offset}`;
    const resp = await axios.get(url, {
      headers: { Authorization: `Bearer ${config.token}`, accept: "application/json" },
    });
    const items = resp.data?.items ?? [];
    for (const item of items) {
      const fd = item.fieldData || {};
      const wfId = fd["shopify-product-id"] ? String(fd["shopify-product-id"]) : null;
      const wfUrl = fd["shopify-url"] ? String(fd["shopify-url"]).trim() : null;
      const wfSlug = (fd["slug"] || fd["shopify-slug-2"]) ? String(fd["slug"] || fd["shopify-slug-2"]).trim() : null;
      if (wfId) byShopifyId.set(wfId, item);
      if (wfUrl) byUrl.set(wfUrl, item);
      if (wfSlug) bySlug.set(wfSlug, item);
    }
    if (items.length < limit) break;
    offset += limit;
  }
  luxuryItemIndex = { byShopifyId, bySlug, byUrl };
  webflowLog("info", { event: "luxury_item_index.loaded", count: byShopifyId.size });
}

/** Normalize product name for index (so we can find by name and avoid duplicate creates). */
function normalizeProductNameForIndex(name) {
  return (name ?? "").trim().toLowerCase().replace(/\s+/g, " ");
}

/** Pre-load Furniture ecommerce products once per sync → O(1) lookup. */
async function loadFurnitureProductIndex() {
  const config = getWebflowConfig("furniture");
  if (!config?.siteId || !config?.token) return;
  const byShopifyId = new Map();
  const bySlug = new Map();
  const byName = new Map();
  let offset = 0;
  const limit = 100;
  while (true) {
    const url = `https://api.webflow.com/v2/sites/${config.siteId}/products?limit=${limit}&offset=${offset}`;
    const resp = await axios.get(url, {
      headers: { Authorization: `Bearer ${config.token}`, accept: "application/json" },
    });
    const raw = resp.data?.products ?? resp.data?.items ?? [];
    const list = Array.isArray(raw) ? raw : [];
    for (const listItem of list) {
      const product = listItem.product ?? listItem;
      const skus = listItem.skus ?? product.skus ?? [];
      const fd = product.fieldData || {};
      const wfId = fd["shopify-product-id"] ? String(fd["shopify-product-id"]) : null;
      const wfSlug = (fd["slug"] || fd["shopify-slug-2"]) ? String(fd["slug"] || fd["shopify-slug-2"]).trim() : null;
      const wfName = fd.name ?? product.name;
      const entry = { ...product, skus };
      if (wfId) byShopifyId.set(wfId, entry);
      if (wfSlug) bySlug.set(wfSlug, entry);
      const nameKey = normalizeProductNameForIndex(wfName);
      if (nameKey && !byName.has(nameKey)) byName.set(nameKey, entry);
    }
    if (list.length < limit) break;
    offset += limit;
  }
  furnitureProductIndex = { byShopifyId, bySlug, byName };
  webflowLog("info", { event: "furniture_product_index.loaded", count: byShopifyId.size, byName: byName.size });
}

/** Pre-load Furniture SKUs by product ID once per sync → O(1) lookup. */
async function loadFurnitureSkuIndex() {
  const config = getWebflowConfig("furniture");
  if (!config?.skuCollectionId || !config?.token) return;
  const byProductId = new Map();
  let offset = 0;
  const limit = 100;
  while (true) {
    const url = `https://api.webflow.com/v2/collections/${config.skuCollectionId}/items?limit=${limit}&offset=${offset}`;
    const resp = await axios.get(url, {
      headers: { Authorization: `Bearer ${config.token}`, accept: "application/json" },
    });
    const items = resp.data?.items ?? [];
    for (const item of items) {
      const productRef = item.fieldData?.product;
      if (productRef) byProductId.set(String(productRef), item);
    }
    if (items.length < limit) break;
    offset += limit;
  }
  furnitureSkuIndex = byProductId;
  webflowLog("info", { event: "furniture_sku_index.loaded", count: byProductId.size });
}

/** Ecommerce category must be an ItemRef (Webflow collection item ID). Uses Webflow Categories if loaded; else env vars. */
function resolveFurnitureCategoryRef(displayCategory) {
  if (!displayCategory || typeof displayCategory !== "string") return null;
  if (furnitureCategoryMapCache && furnitureCategoryMapCache[displayCategory]) {
    return furnitureCategoryMapCache[displayCategory];
  }
  const key = displayCategory.replace(/\s*\/\s*/g, "_").replace(/\s+/g, "_").toUpperCase().replace(/[^A-Z0-9_]/g, "");
  const envKey = `FURNITURE_CATEGORY_${key}`;
  const id = process.env[envKey];
  const trimmed = id && String(id).trim();
  if (!trimmed) return null;
  if (!/^[a-f0-9]{24}$/i.test(trimmed)) return null;
  return trimmed;
}

/* ======================================================
   DIMENSIONS — extract from Shopify (Furniture)
   Native: weight, width, height, length on variant.
   Metafields: custom.width, custom.height, custom.length.
   Tags: "Width: 48", "Height: 18", "Depth: 18", "Weight: 10" (ecommerce tags; used as fallback).
   If missing → dimensions_status = "missing".
====================================================== */
function getProductTagsArray(product) {
  const t = product.tags;
  if (Array.isArray(t)) return t;
  if (typeof t === "string") return t.split(",").map((s) => s.trim()).filter(Boolean);
  return [];
}

/** Parse dimensions from Shopify product tags, e.g. "Width: 48", "Height: 18", "Depth: 18", "Weight: 10". */
function parseDimensionsFromTags(product) {
  const tags = getProductTagsArray(product);
  let width = null, height = null, length = null, weight = null;
  const num = "(\\d+(?:\\.\\d+)?)";
  for (const tag of tags) {
    const t = String(tag).trim();
    const wMatch = t.match(new RegExp("^Width:\\s*" + num + "$", "i"));
    if (wMatch) width = parseFloat(wMatch[1]);
    const hMatch = t.match(new RegExp("^Height:\\s*" + num + "$", "i"));
    if (hMatch) height = parseFloat(hMatch[1]);
    const dMatch = t.match(new RegExp("^Depth:\\s*" + num + "$", "i"));
    if (dMatch) length = parseFloat(dMatch[1]);
    const wtMatch = t.match(new RegExp("^Weight:\\s*" + num + "$", "i"));
    if (wtMatch) weight = parseFloat(wtMatch[1]);
  }
  return { width, height, length, weight };
}

function getDimensionsFromProduct(product) {
  const v = product.variants?.[0];
  let weight = v?.weight != null && v.weight > 0 ? Number(v.weight) : null;
  let width = null, height = null, length = null;
  const metafields = Array.isArray(product.metafields) ? product.metafields : [];
  for (const m of metafields) {
    if (m.namespace === "custom" && m.key === "width" && m.value) width = parseFloat(m.value);
    if (m.namespace === "custom" && m.key === "height" && m.value) height = parseFloat(m.value);
    if (m.namespace === "custom" && m.key === "length" && m.value) length = parseFloat(m.value);
  }
  const fromTags = parseDimensionsFromTags(product);
  if (width == null && fromTags.width != null && !Number.isNaN(fromTags.width)) width = fromTags.width;
  if (height == null && fromTags.height != null && !Number.isNaN(fromTags.height)) height = fromTags.height;
  if (length == null && fromTags.length != null && !Number.isNaN(fromTags.length)) length = fromTags.length;
  if (weight == null && fromTags.weight != null && !Number.isNaN(fromTags.weight) && fromTags.weight > 0) weight = fromTags.weight;
  return { weight, width, height, length };
}

function hasAnyDimensions(dims) {
  return (
    (dims.weight != null && dims.weight > 0) ||
    (dims.width != null && !Number.isNaN(dims.width)) ||
    (dims.height != null && !Number.isNaN(dims.height)) ||
    (dims.length != null && !Number.isNaN(dims.length))
  );
}

/** Format dimensions for description. Dimensions on one line, weight on the next. Uses <br> for HTML line breaks. Only called when hasAnyDimensions. */
function formatDimensionsForDescription(dims) {
  if (!dims || !hasAnyDimensions(dims)) return "";
  const sizeParts = [];
  if (dims.width != null && !Number.isNaN(dims.width)) sizeParts.push(`Width: ${dims.width}"`);
  if (dims.length != null && !Number.isNaN(dims.length)) sizeParts.push(`Depth: ${dims.length}"`);
  if (dims.height != null && !Number.isNaN(dims.height)) sizeParts.push(`Height: ${dims.height}"`);
  const hasWeight = dims.weight != null && !Number.isNaN(dims.weight) && dims.weight > 0;
  const sizeLine = sizeParts.length ? `Dimensions: ${sizeParts.join(" × ")}.` : "";
  const weightLine = hasWeight ? `Weight: ${dims.weight} lb.` : "";
  if (sizeLine && weightLine) return `${sizeLine}<br>${weightLine}`;
  return sizeLine || weightLine || "";
}

/** Strip existing dimensions/weight block(s) from description to prevent duplication.
 * Must match BOTH formats we emit:
 * 1) Dimensions: ... Weight: ... (furniture with size + weight)
 * 2) Weight: N lb. (handbags with weight only - was not matched before, causing repeated appends)
 * Also strips any number of trailing duplicate blocks from prior bug.
 */
function stripExistingDimensions(descriptionHtml) {
  if (!descriptionHtml || typeof descriptionHtml !== "string") return "";
  let s = descriptionHtml;
  // Strip trailing blocks in a loop (handles multiple duplicates from prior bug)
  for (let prev = ""; prev !== s; ) {
    prev = s;
    // Pattern 1: <br><br>Dimensions: ... (optional Weight: ...)
    s = s.replace(/(<br\s*\/?>\s*){2,}Dimensions:[\s\S]*?Weight:\s*[\d.]+?\s*lb\.?$/i, "").trim();
    s = s.replace(/(<br\s*\/?>\s*){2,}Dimensions:[\s\S]*?$/i, "").trim();
    // Pattern 2: <br><br>Weight: N lb. (weight-only — handbags)
    s = s.replace(/(<br\s*\/?>\s*){2,}Weight:\s*[\d.]+?\s*lb\.?$/i, "").trim();
    s = s.replace(/(<br\s*\/?>\s*)+Weight:\s*[\d.]+?\s*lb\.?$/i, "").trim();
  }
  return s.trim();
}

/** Remove the sync-appended weight validation note so we can replace it cleanly. */
function stripWeightValidateNote(descriptionHtml) {
  if (!descriptionHtml || typeof descriptionHtml !== "string") return "";
  return descriptionHtml
    .replace(/(?:<br\s*\/?>\s*)*<em>\s*\(\s*Please validate weight if weight is ever missing\.\s*\)\s*<\/em>/gi, "")
    .trim();
}

/**
 * Strip trailing "Tags: …" blocks mistakenly auto-appended to descriptions (never re-add; tags live on Shopify only).
 * Runs for every vertical so furniture/luxury listings clean up even when dimensions are not rebuilt.
 */
function stripLegacyAutomatedTagsParagraph(descriptionHtml) {
  let s = descriptionHtml || "";
  if (typeof s !== "string") return "";
  for (let prev = ""; prev !== s; ) {
    prev = s;
    s = s.replace(/(<br\s*\/?>\s*)+\s*Tags:\s*[\s\S]*$/i, "").trim();
  }
  return s.trim();
}

/** Strip sync-appended footer: weight note, automated Tags: line, dimensions/weight block(s). */
function stripAppendedSyncFooter(descriptionHtml) {
  let s = descriptionHtml || "";
  if (typeof s !== "string") return "";
  s = stripWeightValidateNote(s);
  s = stripLegacyAutomatedTagsParagraph(s);
  s = stripExistingDimensions(s);
  return s.trim();
}

/** Webflow SKU dimension fields must be numbers; omit keys when value is null/NaN. */
function skuDimensionFields(dimensions) {
  const d = dimensions || {};
  const out = {};
  if (d.weight != null && !Number.isNaN(d.weight) && d.weight > 0) out.weight = Number(d.weight);
  if (d.width != null && !Number.isNaN(d.width)) out.width = Number(d.width);
  if (d.height != null && !Number.isNaN(d.height)) out.height = Number(d.height);
  if (d.length != null && !Number.isNaN(d.length)) out.length = Number(d.length);
  return out;
}

/* ======================================================
   SHOPIFY — FETCH ALL PRODUCTS (cursor-based pagination)
   Uses Link header / page_info per Shopify REST docs so all
   pages are fetched (e.g. 406 products), not just the first 250.
   https://shopify.dev/docs/api/usage/pagination-rest
====================================================== */
function parseNextPageUrl(linkHeader) {
  if (!linkHeader || typeof linkHeader !== "string") return null;
  const parts = linkHeader.split(",");
  for (const part of parts) {
    const trimmed = part.trim();
    if (!/rel=(\s*)?["']?next["']?/i.test(trimmed)) continue;
    const match = trimmed.match(/<([^>]+)>/);
    if (match && match[1]) return match[1].trim();
  }
  return null;
}

async function fetchAllShopifyProducts() {
  const store = process.env.SHOPIFY_STORE;
  const token = process.env.SHOPIFY_ACCESS_TOKEN;
  const headers = {
    "X-Shopify-Access-Token": token,
    "Content-Type": "application/json",
  };

  const allProducts = [];
  let url = `https://${store}.myshopify.com/admin/api/2024-01/products.json?limit=250`;

  while (url) {
    const response = await axios.get(url, { headers });
    const products = response.data.products || [];
    if (products.length) allProducts.push(...products);

    const link = response.headers.link || response.headers.Link;
    const nextUrl = parseNextPageUrl(link);
    url = nextUrl || null;
  }

  return allProducts;
}

/* ======================================================
   SHOPIFY — FETCH SINGLE PRODUCT (confirm status before touching Webflow)
   Returns { status: 'active'|'archived'|'draft'|'gone' } or null on request failure (don't assume).
====================================================== */
async function fetchShopifyProductStatus(productId) {
  const store = process.env.SHOPIFY_STORE;
  const token = process.env.SHOPIFY_ACCESS_TOKEN;
  if (!store || !token) return null;
  const url = `https://${store}.myshopify.com/admin/api/2024-01/products/${productId}.json`;
  try {
    const response = await axios.get(url, {
      headers: {
        "X-Shopify-Access-Token": token,
        "Content-Type": "application/json",
      },
    });
    const product = response.data?.product;
    if (!product) return null;
    const status = (product.status || "").toLowerCase();
    if (status === "active") return { status: "active", product };
    if (status === "archived") return { status: "archived", product };
    if (status === "draft") return { status: "draft", product };
    return { status: status || "unknown", product };
  } catch (err) {
    if (err.response?.status === 404) return { status: "gone" };
    webflowLog("info", {
      event: "shopify_fetch_one.failed",
      shopifyProductId: productId,
      status: err.response?.status,
      message: err.message,
    });
    return null; // couldn't confirm — do not touch Webflow
  }
}

/** Full Admin API product shape for syncSingleProduct (same as fetchAllShopifyProducts list items). */
async function fetchShopifyProductById(productId) {
  const store = process.env.SHOPIFY_STORE;
  const token = process.env.SHOPIFY_ACCESS_TOKEN;
  if (!store || !token) return null;
  const id = String(productId).trim();
  if (!id) return null;
  const url = `https://${store}.myshopify.com/admin/api/2024-01/products/${id}.json`;
  try {
    const response = await axios.get(url, {
      headers: {
        "X-Shopify-Access-Token": token,
        "Content-Type": "application/json",
      },
    });
    return response.data?.product ?? null;
  } catch (err) {
    webflowLog("info", {
      event: "shopify.fetch_product.failed",
      shopifyProductId: id,
      status: err.response?.status,
      message: err.message,
    });
    return null;
  }
}

/* ======================================================
   WEBFLOW — STRONG MATCHER (ID / URL / SLUG)
   Uses config (collectionId + token) for target collection.
====================================================== */
/** Run-scoped index: Luxury CMS items. Populated at sync start for O(1) lookup. */
let luxuryItemIndex = null;

async function findExistingWebflowItem(shopifyProductId, shopifyUrl, slug, config) {
  if (!config?.collectionId || !config?.token) return null;
  const shopifyUrlNorm = shopifyUrl ? String(shopifyUrl).trim() : null;
  const slugNorm = slug ? String(slug).trim() : null;

  webflowLog("info", { event: "match_scan.start", shopifyProductId, shopifyUrl: shopifyUrlNorm, slug: slugNorm });

  // Use pre-loaded index when available; if no hit, still paginate CMS (index can be stale — avoids duplicate creates).
  if (luxuryItemIndex) {
    const byId = luxuryItemIndex.byShopifyId?.get(String(shopifyProductId));
    if (byId) {
      webflowLog("info", { event: "match_scan.found", shopifyProductId, webflowItemId: byId.id, source: "index" });
      return byId;
    }
    if (shopifyUrlNorm) {
      const byUrl = luxuryItemIndex.byUrl?.get(shopifyUrlNorm);
      if (byUrl) {
        webflowLog("info", { event: "match_scan.found", shopifyProductId, webflowItemId: byUrl.id, source: "index" });
        return byUrl;
      }
    }
    if (slugNorm) {
      const bySlug = luxuryItemIndex.bySlug?.get(slugNorm);
      if (bySlug) {
        webflowLog("info", { event: "match_scan.found", shopifyProductId, webflowItemId: bySlug.id, source: "index" });
        return bySlug;
      }
    }
    webflowLog("info", {
      event: "match_scan.index_miss_live_scan",
      shopifyProductId,
      message: "No match in run index; paginating CMS collection",
    });
  }

  let offset = 0;
  const limit = 100;
  while (true) {
    const url = `https://api.webflow.com/v2/collections/${config.collectionId}/items?limit=${limit}&offset=${offset}`;
    let response;
    try {
      response = await axios.get(url, {
        headers: { Authorization: `Bearer ${config.token}`, accept: "application/json" },
      });
    } catch (err) {
      webflowLog("error", { event: "match_scan.error", shopifyProductId, offset, message: err.message, responseData: err.response?.data });
      return null;
    }
    const items = response.data.items || [];
    webflowLog("info", { event: "match_scan.page", shopifyProductId, offset, itemCount: items.length });
    for (const item of items) {
      const fd = item.fieldData || {};
      const wfIdRaw = fd["shopify-product-id"] || null;
      const wfUrlRaw = fd["shopify-url"] || null;
      const wfSlugRaw = fd["slug"] || null;
      const wfSlug2Raw = fd["shopify-slug-2"] || null;
      const wfId = wfIdRaw ? String(wfIdRaw) : null;
      const wfUrl = wfUrlRaw ? String(wfUrlRaw).trim() : null;
      const wfSlug = (wfSlugRaw ? String(wfSlugRaw).trim() : null) || (wfSlug2Raw ? String(wfSlug2Raw).trim() : null);
      const idMatch = wfId && String(wfId) === String(shopifyProductId);
      const urlMatch = wfUrl && shopifyUrlNorm && wfUrl === shopifyUrlNorm;
      const slugMatch = wfSlug && slugNorm && wfSlug === slugNorm;
      if (idMatch || urlMatch || slugMatch) {
        webflowLog("info", { event: "match_scan.found", shopifyProductId, webflowItemId: item.id });
        return item;
      }
    }
    if (items.length < limit) break;
    offset += limit;
  }
  webflowLog("info", { event: "match_scan.not_found", shopifyProductId });
  return null;
}

/* ======================================================
   FURNITURE SKU — find by product reference (required for PATCH)
====================================================== */
/** Run-scoped index: Furniture SKUs by product ID. Populated at sync start for O(1) lookup. */
let furnitureSkuIndex = null;

async function findExistingSkuByProductId(skuCollectionId, webflowProductId, token) {
  if (!skuCollectionId || !token || !webflowProductId) return null;

  // Use pre-loaded index when available
  if (furnitureSkuIndex) {
    return furnitureSkuIndex.get(String(webflowProductId)) ?? null;
  }

  let offset = 0;
  const limit = 100;
  while (true) {
    const url = `https://api.webflow.com/v2/collections/${skuCollectionId}/items?limit=${limit}&offset=${offset}`;
    let response;
    try {
      response = await axios.get(url, {
        headers: { Authorization: `Bearer ${token}`, accept: "application/json" },
      });
    } catch (err) {
      console.error("⚠️ SKU list error:", err.response?.data || err.message);
      return null;
    }
    const items = response.data.items || [];
    for (const item of items) {
      const productRef = item.fieldData?.product;
      if (productRef && String(productRef) === String(webflowProductId)) return item;
    }
    if (items.length < limit) break;
    offset += limit;
  }
  return null;
}

/* ======================================================
   FURNITURE SKU — sync default SKU (price, images, weight, dimensions)
   Products collection = metadata only. SKU = commerce + images.
   Every SKU PATCH/CREATE MUST include "product": "<WEBFLOW_PRODUCT_ID>".
====================================================== */
async function syncFurnitureSku(product, webflowProductId, config) {
  if (!config?.skuCollectionId || !config?.token || !webflowProductId) return;
  const price = product.variants?.[0]?.price;
  const priceDollars = price != null && price !== "" ? parseFloat(price) : null;
  const priceCents =
    priceDollars != null && !Number.isNaN(priceDollars) ? Math.round(priceDollars * 100) : null;
  const dimensions = getDimensionsFromProduct(product);
  const allImages = (product.images || []).map((img) => img.src);
  const mainImageUrl = allImages[0] || null;
  const moreImagesUrls = allImages.slice(1);

  const skuFieldData = {
    product: webflowProductId,
    price: priceCents != null ? { value: priceCents, unit: "USD" } : null,
    ...skuDimensionFields(dimensions),
    "main-image": mainImageUrl ? { url: mainImageUrl } : null,
    "more-images":
      moreImagesUrls.length > 0
        ? moreImagesUrls.slice(0, 10).map((url) => (url ? { url } : null)).filter(Boolean)
        : null,
  };

  const existingSku = await findExistingSkuByProductId(
    config.skuCollectionId,
    webflowProductId,
    config.token
  );

  const headers = {
    Authorization: `Bearer ${config.token}`,
    "Content-Type": "application/json",
  };

  if (existingSku) {
    webflowLog("info", { event: "furniture_sku.patch", shopifyProductId: product.id, webflowProductId, skuId: existingSku.id });
    const fd = existingSku.fieldData || {};
    await axios.patch(
      `https://api.webflow.com/v2/collections/${config.skuCollectionId}/items/${existingSku.id}`,
      {
        fieldData: {
          ...skuFieldData,
          slug: fd.slug ?? existingSku.slug,
        },
      },
      { headers }
    );
  } else {
    const name = product.title ? `${product.title} - Default` : "Default SKU";
    const slug = product.handle ? `${product.handle}-default-sku` : `sku-${webflowProductId}`;
    webflowLog("info", { event: "furniture_sku.create", shopifyProductId: product.id, webflowProductId });
    await axios.post(
      `https://api.webflow.com/v2/collections/${config.skuCollectionId}/items`,
      { fieldData: { ...skuFieldData, name, slug } },
      { headers }
    );
  }
}

/** Furniture ecommerce DateTime slug for “sold at” (retention + UI). Override with FURNITURE_SOLD_SINCE_FIELD_SLUG. */
function getFurnitureSoldSinceFieldSlug() {
  const t = (process.env.FURNITURE_SOLD_SINCE_FIELD_SLUG || "date-sold").trim();
  return t || "date-sold";
}

/** Luxury CMS DateTime slug for “Date sold”. Matches Webflow field “Date Sold” → `date-sold`. Override with LUXURY_SOLD_SINCE_FIELD_SLUG. */
function getLuxurySoldSinceFieldSlug() {
  const t = (process.env.LUXURY_SOLD_SINCE_FIELD_SLUG || "date-sold").trim();
  return t || "date-sold";
}

/** Business timezone for sold-date stamping (default Arizona). */
function getSalesTimezone() {
  const t = (process.env.SALES_TIMEZONE || "America/Phoenix").trim();
  return t || "America/Phoenix";
}

/** Date-only YYYY-MM-DD string in business timezone for Date Sold fields. */
function getBusinessDateSoldString(now = new Date()) {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: getSalesTimezone(),
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(now);
  const byType = Object.fromEntries(parts.map((p) => [p.type, p.value]));
  return `${byType.year}-${byType.month}-${byType.day}`;
}

/** Webflow ecommerce SKU slug for compare-at (API: `compare-at-price`). Override if your site renamed the field. */
function getFurnitureSkuCompareAtSlug() {
  const t = (process.env.FURNITURE_SKU_COMPARE_AT_SLUG || "compare-at-price").trim();
  return t || "compare-at-price";
}

/** Cents from Webflow SKU `price` / `compare-at-price` object `{ value, unit }`. */
function webflowSkuMoneyFieldToCents(field) {
  if (field == null || typeof field !== "object") return null;
  const n = Number(field.value);
  if (!Number.isFinite(n)) return null;
  return Math.round(n);
}

/** Suffix for furniture ecommerce product name when marked sold (appended once). */
const FURNITURE_NO_LONGER_AVAILABLE_SUFFIX = " (No Longer Available)";

function appendFurnitureNoLongerAvailableToTitle(currentTitle) {
  if (currentTitle == null) return null;
  const s = String(currentTitle);
  if (!s.trim()) return null;
  if (s.includes("(No Longer Available)")) return s;
  return s + FURNITURE_NO_LONGER_AVAILABLE_SUFFIX;
}

/* ======================================================
   MARK AS SOLD — per vertical
   Luxury: CMS PATCH. Furniture: ecommerce PATCH (siteId).
====================================================== */
async function markAsSold(existing, vertical, config) {
  if (!existing || !config?.token) return;
  if (vertical === "furniture" && config?.siteId) {
    if (existing.isArchived === true) {
      webflowLog("info", {
        event: "markAsSold.skip_archived_furniture",
        webflowId: existing.id,
        source: "existing_flag",
      });
      return;
    }
    if (!Object.prototype.hasOwnProperty.call(existing, "isArchived")) {
      const live = await getWebflowEcommerceProductById(config.siteId, existing.id, config.token);
      if (live?.isArchived === true) {
        webflowLog("info", {
          event: "markAsSold.skip_archived_furniture",
          webflowId: existing.id,
          source: "prefetch",
        });
        return;
      }
    }
  }
  const alreadySoldInWebflow = webflowListingLooksSold(existing, vertical);
  const base = { ...(existing.fieldData || {}) };
  const fieldData =
    vertical === "furniture"
      ? { ...base, sold: true }
      : { ...base, category: "Recently Sold", "show-on-webflow": false };

  const luxurySoldSinceSlug = getLuxurySoldSinceFieldSlug();
  const furnitureSoldSinceSlug = getFurnitureSoldSinceFieldSlug();
  const soldDate = getBusinessDateSoldString();
  // Luxury: fill Date sold when missing or unparsable (same idea as furniture), plus category + show-on-webflow above.
  if (vertical === "luxury" && luxurySoldSinceSlug) {
    if (parseSoldTimestampMsFromWebflowField(fieldData, luxurySoldSinceSlug) == null) {
      fieldData[luxurySoldSinceSlug] = soldDate;
    }
  }
  // Furniture: every sold listing must have a parseable `date-sold` for retention; keep existing if coerce succeeds.
  if (vertical === "furniture" && furnitureSoldSinceSlug) {
    if (parseSoldTimestampMsFromWebflowField(fieldData, furnitureSoldSinceSlug) == null) {
      fieldData[furnitureSoldSinceSlug] = soldDate;
    }
  }
  // Furniture: append "(No Longer Available)" to product name once (same PATCH as sold + date-sold).
  if (vertical === "furniture") {
    const withSuffix = appendFurnitureNoLongerAvailableToTitle(fieldData.name);
    if (withSuffix != null) fieldData.name = withSuffix;
  }

  if (vertical === "furniture" && config.siteId) {
    // Ecommerce PATCH requires { product: { fieldData }, sku: { fieldData } }; reuse shared updater
    await updateWebflowEcommerceProduct(config.siteId, existing.id, fieldData, config.token, existing);
    return;
  }
  await axios.patch(
    `https://api.webflow.com/v2/collections/${config.collectionId}/items/${existing.id}`,
    { fieldData },
    {
      headers: {
        Authorization: `Bearer ${config.token}`,
        "Content-Type": "application/json",
      },
    }
  );
}

/**
 * Same path as sync-all "disappeared" handling: confirm Shopify is not active, mark Webflow sold, remove cache.
 * @param {string} goneId - Shopify product id
 * @param {Record<string, any>} cache - mutable cache from loadCache()
 * @param {{ trigger?: string }} [options]
 * @returns {Promise<"marked_sold"|"no_webflow"|"skip_unconfirmed"|"skip_still_active"|"skip_unknown_status">}
 */
async function processDisappearedShopifyProduct(goneId, cache, options = {}) {
  const trigger = options.trigger || "sync-all.disappeared";
  const confirmed = await fetchShopifyProductStatus(goneId);
  if (confirmed === null || confirmed === undefined) {
    webflowLog("info", {
      event: "sync-all.disappeared_skip_unconfirmed",
      shopifyProductId: goneId,
      reason: "fetch_failed_or_unknown",
      trigger,
    });
    return "skip_unconfirmed";
  }
  if (confirmed.status === "active") {
    webflowLog("info", {
      event: "sync-all.disappeared_skip_still_active",
      shopifyProductId: goneId,
      reason: "product_still_active_in_shopify",
      trigger,
    });
    return "skip_still_active";
  }
  if (confirmed.status !== "gone" && confirmed.status !== "archived" && confirmed.status !== "draft") {
    webflowLog("info", {
      event: "sync-all.disappeared_skip_unconfirmed",
      shopifyProductId: goneId,
      reason: "shopify_status_unknown",
      status: confirmed.status,
      trigger,
    });
    return "skip_unknown_status";
  }
  const entry = getCacheEntry(cache, goneId);
  // No cache (or unknown vertical): do not assume luxury — otherwise delete/disappeared only scans the
  // luxury CMS and misses Furniture ecommerce (match_scan on CMS → not_found → no Webflow sold).
  let vertical = entry?.vertical ?? null;
  let config = vertical ? getWebflowConfig(vertical) : null;
  let existing = null;

  if (entry?.webflowId && config) {
    if (vertical === "furniture" && config.siteId) {
      existing = await getWebflowEcommerceProductById(config.siteId, entry.webflowId, config.token);
    } else if (vertical === "luxury" && config.collectionId) {
      existing = await getWebflowItemById(entry.webflowId, config);
    }
  }
  if (!existing && vertical === "furniture" && config?.siteId) {
    existing = await findExistingWebflowEcommerceProduct(goneId, null, config);
  }
  if (!existing && vertical === "luxury" && config?.collectionId) {
    existing = await findExistingWebflowItem(goneId, null, null, config);
  }
  if (!existing) {
    const furn = getWebflowConfig("furniture");
    if (furn?.siteId && furn?.token && vertical !== "furniture") {
      const e = await findExistingWebflowEcommerceProduct(goneId, null, furn);
      if (e) {
        existing = e;
        vertical = "furniture";
        config = furn;
      }
    }
  }
  if (!existing) {
    const lux = getWebflowConfig("luxury");
    if (lux?.collectionId && lux?.token && vertical !== "luxury") {
      const e = await findExistingWebflowItem(goneId, null, null, lux);
      if (e) {
        existing = e;
        vertical = "luxury";
        config = lux;
      }
    }
  }

  if (existing) {
    webflowLog("info", {
      event: "sync-all.disappeared_mark_sold_confirmed",
      shopifyProductId: goneId,
      webflowId: existing.id,
      vertical,
      shopifyStatus: confirmed.status,
      trigger,
    });
    await markAsSold(existing, vertical, config);
  } else {
    webflowLog("info", {
      event: "sync-all.disappeared_no_webflow",
      shopifyProductId: goneId,
      trigger,
    });
  }

  delete cache[goneId];
  webflowLog("info", {
    event: "cache.mutated",
    shopifyProductId: goneId,
    op: "deleted",
    reason: "disappeared_confirmed",
    trigger,
  });
  return existing ? "marked_sold" : "no_webflow";
}

function webflowListingLooksSold(existing, vertical) {
  const fd = existing?.fieldData || {};
  if (vertical === "furniture") {
    const s = fd.sold;
    return s === true || s === 1 || s === "1" || (typeof s === "string" && s.toLowerCase() === "true");
  }
  if (fd["show-on-webflow"] === false) return true;
  const cat = fd.category;
  if (typeof cat === "string" && cat.replace(/\s+$/, "") === "Recently Sold") return true;
  return false;
}

function shopifyQtySaysSold(qty) {
  if (qty == null) return false;
  const n = Number(qty);
  if (Number.isNaN(n)) return false;
  return n <= 0;
}

/** Shopify says qty 0 but Webflow is not in sold state — must PATCH, never skip_unchanged. */
function needsWebflowSoldRepair(existing, vertical, qty) {
  return shopifyQtySaysSold(qty) && !webflowListingLooksSold(existing, vertical);
}

/**
 * Mark sold when Shopify first variant is out of stock (0 or negative), including 1 → 0.
 * previousQty null = first time we stored qty / legacy cache → still mark if Shopify is sold out.
 */
function shouldMarkSoldTransition(previousQty, qty) {
  if (!shopifyQtySaysSold(qty)) return false;
  const pq =
    previousQty == null || previousQty === "" ? null : Number(previousQty);
  if (pq == null || Number.isNaN(pq)) return true;
  return pq > 0;
}

/** Persist when listing is sold (qty <= 0) for SOLD_RETENTION_DAYS furniture removal sweep; omit when back in stock. */
function soldMarkedAtPayload(cacheEntry, lastQty) {
  if (shopifyQtySaysSold(lastQty)) {
    return { soldMarkedAt: cacheEntry?.soldMarkedAt || new Date().toISOString() };
  }
  return {};
}

function getSoldRetentionMs() {
  const n = parseInt(process.env.SOLD_RETENTION_DAYS || "4", 10);
  return Math.max(1, Number.isFinite(n) ? n : 4) * 86400000;
}

/** Webflow DateTime fields may be ISO strings or nested objects (`date`, `value`, etc.). */
function coerceWebflowDateTimeToMs(value, depth = 0) {
  if (value == null || depth > 5) return null;
  if (typeof value === "number") {
    if (!Number.isFinite(value)) return null;
    if (value > 1e15) return Math.round(value);
    if (value > 1e12) return Math.round(value);
    if (value > 1e9) return Math.round(value * 1000);
    return null;
  }
  if (typeof value === "string") {
    const trim = value.trim();
    if (!trim) return null;
    const t = Date.parse(trim);
    return Number.isNaN(t) ? null : t;
  }
  if (typeof value === "object") {
    const nested =
      value.date ??
      value.value ??
      value.datetime ??
      value.iso ??
      value.timestamp ??
      value.start;
    if (nested !== undefined && nested !== value) {
      const inner = coerceWebflowDateTimeToMs(nested, depth + 1);
      if (inner != null) return inner;
    }
  }
  const fallback = Date.parse(String(value));
  return Number.isNaN(fallback) ? null : fallback;
}

function parseSoldTimestampMsFromWebflowField(fieldData, slug) {
  if (!slug || !fieldData || fieldData[slug] == null) return null;
  return coerceWebflowDateTimeToMs(fieldData[slug]);
}

/** Anchor instant for “how long has this been sold”. Furniture: only Webflow `date-sold` (default slug) — no cache fallback so retention matches the field you set in Webflow. */
function getSoldInstantMs(webflowEntity, cacheEntry, vertical) {
  const fd = webflowEntity?.fieldData || {};
  const luxSlug = getLuxurySoldSinceFieldSlug();
  const furnSlug = getFurnitureSoldSinceFieldSlug();
  if (vertical === "luxury" && luxSlug) {
    const ms = parseSoldTimestampMsFromWebflowField(fd, luxSlug);
    if (ms != null) return ms;
  }
  if (vertical === "furniture" && furnSlug) {
    const ms = parseSoldTimestampMsFromWebflowField(fd, furnSlug);
    if (ms != null) return ms;
    return null;
  }
  if (cacheEntry?.soldMarkedAt) {
    const ms = Date.parse(cacheEntry.soldMarkedAt);
    if (!Number.isNaN(ms)) return ms;
  }
  return null;
}

/** Default April 2, 2026 end-of-day UTC; override with SOLD_BACKFILL_BEFORE_DATE=YYYY-MM-DD. */
function getSoldBackfillCutoffEndMs() {
  const raw = (process.env.SOLD_BACKFILL_BEFORE_DATE || "2026-04-02").trim();
  if (!raw) return Date.UTC(2026, 3, 2, 23, 59, 59, 999);
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    const t = Date.parse(`${raw}T23:59:59.999Z`);
    return Number.isNaN(t) ? Date.UTC(2026, 3, 2, 23, 59, 59, 999) : t;
  }
  const t = Date.parse(raw);
  return Number.isNaN(t) ? Date.UTC(2026, 3, 2, 23, 59, 59, 999) : t;
}

function webflowEntityAnchorMs(entity) {
  if (!entity || typeof entity !== "object") return null;
  const candidates = [
    entity.lastUpdated,
    entity.updatedOn,
    entity.lastPublished,
    entity.createdOn,
    entity.created,
  ];
  for (const c of candidates) {
    if (c == null) continue;
    const t = Date.parse(String(c));
    if (!Number.isNaN(t)) return t;
  }
  return null;
}

/**
 * For backfill: getSoldInstantMs first (furniture: date-sold only; luxury: optional slug; other: cache soldMarkedAt),
 * else Webflow item timestamps (lastUpdated, etc.). Ongoing retention uses getSoldInstantMs only for furniture.
 */
function getSoldRetentionAnchorMs(entity, cacheEntry, vertical) {
  const explicit = getSoldInstantMs(entity, cacheEntry, vertical);
  if (explicit != null) return explicit;
  return webflowEntityAnchorMs(entity);
}

/** Luxury CMS: archive (same v2 item PATCH as updates; removes from site when published). */
async function archiveWebflowCollectionItem(collectionId, itemId, token) {
  if (!collectionId || !itemId || !token) return;
  const url = `https://api.webflow.com/v2/collections/${collectionId}/items/${itemId}`;
  webflowLog("info", { event: "archive.cms_item", collectionId, itemId });
  await axios.patch(
    url,
    { isArchived: true },
    {
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
        accept: "application/json",
      },
    }
  );
}

/**
 * (1) One-time backfill: **delete** sold **furniture** (ecommerce) where anchor ≤ SOLD_BACKFILL_BEFORE_DATE (default 2026-04-02).
 *     Runs once until SOLD_BACKFILL_DONE_FILE exists (delete file to re-run). Archive only if DELETE fails (same as duplicate cleanup).
 *     Luxury is not touched here — sold luxury stays in Recently Sold (CMS category + hidden).
 * (2) Ongoing: furniture sold ≥ SOLD_RETENTION_DAYS from date-sold — **delete** ecommerce product; archive only as fallback.
 *     Never skip a row because the *list* says isArchived — only GET /products/{id} decides.
 */
async function archiveLongSoldWebflowListings(cache) {
  if (process.env.SOLD_RETENTION_DISABLE === "1" || process.env.SOLD_RETENTION_DISABLE === "true") {
    return { archived: 0, soldBackfillArchived: 0 };
  }
  ensureDataDir();
  const retentionMs = getSoldRetentionMs();
  const now = Date.now();
  let archived = 0;
  let soldBackfillArchived = 0;
  /** Furniture Webflow product ids archived this run (in-memory index is stale until next sync). */
  const skipRetentionFurnitureIds = new Set();
  const furnitureConfig = getWebflowConfig("furniture");

  const touchSoldClock = (shopifyId) => {
    const idStr = String(shopifyId).trim();
    if (!idStr) return;
    const cur = cache[idStr];
    if (cur && typeof cur === "object") {
      if (!cur.soldMarkedAt) cur.soldMarkedAt = new Date().toISOString();
      return;
    }
    if (typeof cur === "string") {
      cache[idStr] = {
        hash: cur,
        webflowId: null,
        lastQty: 0,
        vertical: "luxury",
        soldMarkedAt: new Date().toISOString(),
      };
      return;
    }
    webflowLog("info", {
      event: "sold_retention.no_cache_skip_clock",
      shopifyProductId: idStr,
      message: "No cache row yet; furniture retention needs date-sold on Webflow (and next index load)",
    });
  };

  const runOneTimeBackfill =
    process.env.SOLD_BACKFILL_DISABLE !== "1" &&
    process.env.SOLD_BACKFILL_DISABLE !== "true" &&
    !fs.existsSync(SOLD_BACKFILL_DONE_FILE);

  if (runOneTimeBackfill) {
    const cutoffEndMs = getSoldBackfillCutoffEndMs();
    webflowLog("info", {
      event: "sold_retention.backfill_start",
      cutoffEndMs,
      cutoffLabel: new Date(cutoffEndMs).toISOString(),
      doneFile: SOLD_BACKFILL_DONE_FILE,
      message: "One-time removal: sold furniture only (delete, archive fallback; luxury unchanged)",
    });

    if (furnitureProductIndex?.byShopifyId && furnitureConfig?.siteId && furnitureConfig?.token) {
      const seenBf = new Set();
      for (const [shopifyId, product] of furnitureProductIndex.byShopifyId) {
        const pid = product?.id;
        if (!pid || seenBf.has(pid)) continue;
        seenBf.add(pid);
        if (!webflowListingLooksSold(product, "furniture")) continue;
        let entityBf = product;
        try {
          const full = await getWebflowEcommerceProductById(furnitureConfig.siteId, pid, furnitureConfig.token);
          if (full) entityBf = full;
        } catch (err) {
          webflowLog("warn", {
            event: "sold_retention.backfill_full_product_fetch_failed",
            shopifyProductId: shopifyId,
            webflowId: pid,
            message: err.message,
          });
        }
        // Do not trust list `isArchived` — stale true skipped re-archive after wrongful unarchive.
        if (entityBf.isArchived) continue;
        if (!webflowListingLooksSold(entityBf, "furniture")) continue;
        const cacheEntry = getCacheEntry(cache, shopifyId);
        const anchorMs = getSoldRetentionAnchorMs(entityBf, cacheEntry, "furniture");
        if (anchorMs == null) {
          webflowLog("info", {
            event: "sold_retention.backfill_skip_no_anchor",
            vertical: "furniture",
            shopifyProductId: shopifyId,
            webflowId: pid,
          });
          continue;
        }
        if (anchorMs > cutoffEndMs) continue;
        try {
          await deleteWebflowEcommerceProduct(furnitureConfig.siteId, pid, furnitureConfig.token);
          delete cache[shopifyId];
          skipRetentionFurnitureIds.add(String(pid));
          soldBackfillArchived++;
          archived++;
          webflowLog("info", {
            event: "sold_retention.backfill_removed",
            vertical: "furniture",
            shopifyProductId: shopifyId,
            webflowId: pid,
            anchorMs,
            message: "delete first, archive if delete unsupported",
          });
        } catch (err) {
          webflowLog("error", {
            event: "sold_retention.backfill_remove_failed",
            vertical: "furniture",
            shopifyProductId: shopifyId,
            webflowId: pid,
            message: err.message,
          });
        }
      }
    }

    try {
      fs.writeFileSync(
        SOLD_BACKFILL_DONE_FILE,
        JSON.stringify(
          {
            completedAt: new Date().toISOString(),
            cutoffEndMs,
            archivedInBackfill: soldBackfillArchived,
          },
          null,
          2
        ),
        "utf8"
      );
      webflowLog("info", {
        event: "sold_retention.backfill_marker_written",
        path: SOLD_BACKFILL_DONE_FILE,
        soldBackfillArchived,
      });
    } catch (err) {
      webflowLog("error", { event: "sold_retention.backfill_marker_failed", message: err.message });
    }
  }

  if (furnitureProductIndex?.byShopifyId && furnitureConfig?.siteId && furnitureConfig?.token) {
    const seenProd = new Set();
    for (const [shopifyId, product] of furnitureProductIndex.byShopifyId) {
      const pid = product?.id;
      if (!pid || seenProd.has(pid)) continue;
      seenProd.add(pid);
      if (skipRetentionFurnitureIds.has(String(pid))) continue;
      if (!webflowListingLooksSold(product, "furniture")) continue;

      let entityForRetention = product;
      try {
        const full = await getWebflowEcommerceProductById(furnitureConfig.siteId, pid, furnitureConfig.token);
        if (full) entityForRetention = full;
      } catch (err) {
        webflowLog("warn", {
          event: "sold_retention.full_product_fetch_failed",
          shopifyProductId: shopifyId,
          webflowId: pid,
          message: err.message,
        });
      }
      if (entityForRetention.isArchived) continue;
      if (!webflowListingLooksSold(entityForRetention, "furniture")) continue;

      const cacheEntry = getCacheEntry(cache, shopifyId);
      const soldAtMs = getSoldInstantMs(entityForRetention, cacheEntry, "furniture");
      if (soldAtMs == null) {
        touchSoldClock(shopifyId);
        continue;
      }
      if (now - soldAtMs < retentionMs) continue;

      try {
        await deleteWebflowEcommerceProduct(furnitureConfig.siteId, pid, furnitureConfig.token);
        delete cache[shopifyId];
        archived++;
        webflowLog("info", {
          event: "sold_retention.removed",
          vertical: "furniture",
          shopifyProductId: shopifyId,
          webflowId: pid,
          message: "delete first, archive if delete unsupported",
        });
      } catch (err) {
        webflowLog("error", {
          event: "sold_retention.remove_failed",
          vertical: "furniture",
          shopifyProductId: shopifyId,
          webflowId: pid,
          message: err.message,
        });
      }
    }
  }

  return { archived, soldBackfillArchived };
}

function firstVariantInventoryQty(product) {
  return getPrimaryVariantInventoryQuantity(product);
}

/**
 * Every Webflow listing with a Shopify product id: if Shopify says 0 inventory (first variant), mark sold.
 * If that id is not in this run's product.json crawl, GET the product once — gone/archived/draft → mark sold;
 * still active with stock → skip (id was missing from crawl for another reason).
 */
async function sweepWebflowOrphansAgainstShopifyCatalog(products, cache) {
  const productById = new Map((products || []).map((p) => [String(p.id), p]));
  /** @type {{ shopifyId: string, vertical: string, existing: object }[]} */
  const jobs = [];

  const pushJob = (shopifyId, vertical, existing) => {
    const id = shopifyId != null ? String(shopifyId).trim() : "";
    if (!id || !existing?.id) return;
    if (existing.isArchived) return;
    if (webflowListingLooksSold(existing, vertical)) return;
    jobs.push({ shopifyId: id, vertical, existing });
  };

  if (furnitureProductIndex?.byShopifyId) {
    for (const [sid, entry] of furnitureProductIndex.byShopifyId) {
      pushJob(sid, "furniture", entry);
    }
  }
  if (luxuryItemIndex?.byShopifyId) {
    for (const [sid, item] of luxuryItemIndex.byShopifyId) {
      pushJob(sid, "luxury", item);
    }
  }

  if (jobs.length === 0) {
    webflowLog("info", { event: "sync-all.webflow_sold_sweep", candidates: 0, markedSold: 0 });
    return 0;
  }

  webflowLog("info", {
    event: "sync-all.webflow_sold_sweep",
    candidates: jobs.length,
    message: "Webflow ↔ Shopify: mark sold when qty 0 or product gone/archived/draft",
  });

  const sweepConc = Math.min(10, Math.max(3, jobs.length));
  let markedSold = 0;

  for (let i = 0; i < jobs.length; i += sweepConc) {
    const chunk = jobs.slice(i, i + sweepConc);
    const results = await Promise.all(
      chunk.map(async ({ shopifyId, vertical, existing }) => {
        const config = getWebflowConfig(vertical);
        const p = productById.get(shopifyId);

        if (p) {
          const qty = firstVariantInventoryQty(p);
          if (qty !== null && qty <= 0) {
            try {
              webflowLog("info", {
                event: "sync-all.webflow_sold_sweep_qty_zero",
                shopifyProductId: shopifyId,
                webflowId: existing.id,
                vertical,
                qty,
              });
              await markAsSold(existing, vertical, config);
              if (vertical === "furniture" && config.siteId) {
                await syncFurnitureEcommerceSku(p, existing.id, config);
              }
              const prevEntry = getCacheEntry(cache, shopifyId) || {};
              cache[shopifyId] = {
                hash: shopifyHash(p),
                contentHash: contentHashForLLM(p),
                webflowId: existing.id,
                lastQty: qty,
                vertical,
                ...soldMarkedAtPayload(prevEntry, qty),
              };
              webflowLog("info", {
                event: "cache.mutated",
                shopifyProductId: shopifyId,
                op: "sold",
                webflowId: existing.id,
                vertical,
                reason: "webflow_sold_sweep_qty_zero",
              });
              return 1;
            } catch (err) {
              webflowLog("error", {
                event: "sync-all.webflow_sold_sweep_qty_zero_failed",
                shopifyProductId: shopifyId,
                webflowId: existing.id,
                vertical,
                message: err.message,
              });
              return 0;
            }
          }
          return 0;
        }

        const confirmed = await fetchShopifyProductStatus(shopifyId);
        if (confirmed == null) {
          webflowLog("info", {
            event: "sync-all.webflow_orphan_skip_unconfirmed",
            shopifyProductId: shopifyId,
            vertical,
            webflowId: existing.id,
            reason: "fetch_failed_or_unknown",
          });
          return 0;
        }
        if (confirmed.status === "active") {
          const pq = firstVariantInventoryQty(confirmed.product);
          if (pq !== null && pq <= 0) {
            try {
              webflowLog("info", {
                event: "sync-all.webflow_sold_sweep_qty_zero",
                shopifyProductId: shopifyId,
                webflowId: existing.id,
                vertical,
                qty: pq,
                source: "shopify_fetch_one",
              });
              await markAsSold(existing, vertical, config);
              if (vertical === "furniture" && config.siteId && confirmed.product) {
                await syncFurnitureEcommerceSku(confirmed.product, existing.id, config);
              }
              const prevEntry = getCacheEntry(cache, shopifyId) || {};
              cache[shopifyId] = {
                hash: shopifyHash(confirmed.product),
                contentHash: contentHashForLLM(confirmed.product),
                webflowId: existing.id,
                lastQty: pq,
                vertical,
                ...soldMarkedAtPayload(prevEntry, pq),
              };
              webflowLog("info", {
                event: "cache.mutated",
                shopifyProductId: shopifyId,
                op: "sold",
                webflowId: existing.id,
                vertical,
                reason: "webflow_sold_sweep_qty_zero_not_in_bulk_list",
              });
              return 1;
            } catch (err) {
              webflowLog("error", {
                event: "sync-all.webflow_sold_sweep_qty_zero_failed",
                shopifyProductId: shopifyId,
                webflowId: existing.id,
                vertical,
                message: err.message,
              });
              return 0;
            }
          }
          webflowLog("info", {
            event: "sync-all.webflow_orphan_skip_still_active",
            shopifyProductId: shopifyId,
            vertical,
            webflowId: existing.id,
            reason: "product_still_active_in_shopify",
          });
          return 0;
        }
        if (confirmed.status !== "gone" && confirmed.status !== "archived" && confirmed.status !== "draft") {
          webflowLog("info", {
            event: "sync-all.webflow_orphan_skip_unconfirmed",
            shopifyProductId: shopifyId,
            vertical,
            webflowId: existing.id,
            status: confirmed.status,
          });
          return 0;
        }
        try {
          webflowLog("info", {
            event: "sync-all.webflow_orphan_mark_sold",
            shopifyProductId: shopifyId,
            webflowId: existing.id,
            vertical,
            shopifyStatus: confirmed.status,
          });
          await markAsSold(existing, vertical, config);
          delete cache[shopifyId];
          webflowLog("info", {
            event: "cache.mutated",
            shopifyProductId: shopifyId,
            op: "deleted",
            reason: "webflow_orphan_shopify_inactive",
          });
          return 1;
        } catch (err) {
          webflowLog("error", {
            event: "sync-all.webflow_orphan_mark_sold_failed",
            shopifyProductId: shopifyId,
            webflowId: existing.id,
            vertical,
            message: err.message,
          });
          return 0;
        }
      })
    );
    markedSold += results.reduce((a, b) => a + b, 0);
  }

  webflowLog("info", { event: "sync-all.webflow_sold_sweep_done", candidates: jobs.length, markedSold });
  return markedSold;
}

/* ======================================================
   ⭐ CORE SYNC LOGIC — DUAL PIPELINE, NO DUPLICATES ⭐
   Per–Shopify-id queue: concurrent webhook + sync-all cannot both CREATE the same Webflow product.
====================================================== */
const shopifyProductSyncChains = new Map();

function runSerializedByShopifyProductId(shopifyProductId, fn) {
  const id = String(shopifyProductId ?? "").trim();
  if (!id) return fn();
  const prev = shopifyProductSyncChains.get(id) ?? Promise.resolve();
  const next = prev.catch(() => {}).then(() => fn());
  const tail = next.catch((err) => {
    webflowLog("error", {
      event: "serialized_product_sync.failed",
      shopifyProductId: id,
      message: err?.message ?? String(err),
    });
  });
  shopifyProductSyncChains.set(id, tail);
  next.finally(() => {
    if (shopifyProductSyncChains.get(id) === tail) shopifyProductSyncChains.delete(id);
  });
  return next;
}

async function syncSingleProductCore(product, cache, options = {}) {
  const shopifyProductId = String(product.id);
  const cacheEntry = getCacheEntry(cache, shopifyProductId);
  const duplicateEmailSentFor = options.duplicateEmailSentFor ?? null;
  const shopifyWriteEmailSentFor = options.shopifyWriteEmailSentFor ?? null;

  const currentHash = shopifyHash(product);
  const currentContentHash = contentHashForLLM(product);
  const previousContentHash = cacheEntry?.contentHash ?? null;
  const previousHashForEarlyExit = cacheEntry?.hash ?? null;
  const shopifyDataUnchangedForCache =
    previousHashForEarlyExit != null &&
    JSON.stringify(currentHash) === JSON.stringify(previousHashForEarlyExit);
  const previousQty = cacheEntry?.lastQty ?? null;
  const qty = getPrimaryVariantInventoryQuantity(product);
  // Skip LLM only when: (1) we have same name/description as last time, OR (2) we have a cached webflowId but no contentHash (legacy) — treat as unchanged to avoid cost.
  const nameOrDescriptionUnchanged =
    (previousContentHash && JSON.stringify(currentContentHash) === JSON.stringify(previousContentHash)) ||
    (cacheEntry?.webflowId && previousContentHash == null);
  const forceReclassify = options.forceReclassify === true;
  const cachedVerticalForSkip = cacheEntry?.vertical ?? "luxury";
  const evidenceVsCache = resolveVerticalFromEvidence(product, cachedVerticalForSkip);
  const verticalNeedsCorrection = evidenceVsCache.vertical !== cachedVerticalForSkip;

  // Do NOT lock vertical from cache when webflowId is missing and qty is 0 — wrong vertical (e.g. luxury) blocked
  // Webflow index lookup + sold heuristic, causing skip_create_sold while the live listing stays on Furniture unpublished/sold.

  let recoveredFromWebflow = null;

  if (
    WEBFLOW_STRICT_NOOP_UPDATES &&
    !recoveredFromWebflow &&
    nameOrDescriptionUnchanged &&
    shopifyDataUnchangedForCache &&
    cacheEntry?.webflowId &&
    !verticalNeedsCorrection &&
    !forceReclassify
  ) {
    const vertical = cachedVerticalForSkip;
    cache[shopifyProductId] = {
      hash: currentHash,
      contentHash: currentContentHash,
      webflowId: cacheEntry.webflowId,
      lastQty: qty,
      vertical,
      ...soldMarkedAtPayload(cacheEntry, qty),
    };
    webflowLog("info", {
      event: "sync_product.skip_unchanged_no_touch",
      shopifyProductId,
      productTitle: product.title,
      webflowId: cacheEntry.webflowId,
      vertical,
      message: "Shopify snapshot unchanged; skipping all Webflow read/write touch",
    });
    return { operation: "skip", id: cacheEntry.webflowId };
  }

  if (
    !recoveredFromWebflow &&
    nameOrDescriptionUnchanged &&
    shopifyDataUnchangedForCache &&
    cacheEntry?.webflowId &&
    !forceReclassify
  ) {
    const vertical = cacheEntry.vertical ?? "luxury";
    const config = getWebflowConfig(vertical);
    let existing = null;
    if (vertical === "furniture" && config?.siteId && config?.token) {
      existing = await getWebflowEcommerceProductById(config.siteId, cacheEntry.webflowId, config.token);
    } else if (vertical === "luxury" && config?.collectionId && config?.token) {
      existing = await getWebflowItemById(cacheEntry.webflowId, config);
    }
    if (existing) {
      if (vertical === "furniture" && existing.isArchived === true) {
        cache[shopifyProductId] = {
          hash: currentHash,
          contentHash: currentContentHash,
          webflowId: existing.id,
          lastQty: qty,
          vertical,
          ...soldMarkedAtPayload(cacheEntry, qty),
        };
        webflowLog("info", {
          event: "sync_product.skip_unchanged_archived",
          shopifyProductId,
          productTitle: product.title,
          webflowId: existing.id,
          message: "Furniture listing is archived; no Webflow PATCH",
        });
        return { operation: "skip", id: existing.id };
      }
      const repairSold = needsWebflowSoldRepair(existing, vertical, qty);
      const mustMarkSold = shouldMarkSoldTransition(previousQty, qty) || repairSold;
      if (mustMarkSold) {
        const fromQtyDrop =
          !repairSold &&
          previousQty != null &&
          Number(previousQty) > 0 &&
          shopifyQtySaysSold(qty);
        webflowLog("info", {
          event: repairSold ? "sync_product.repair_sold" : "sync_product.newly_sold",
          shopifyProductId,
          productTitle: product.title,
          webflowId: existing.id,
          vertical,
          previousQty,
          currentQty: qty,
          ...(fromQtyDrop ? { reason: "inventory_1_to_0_or_in_stock_to_sold" } : {}),
        });
        await markAsSold(existing, vertical, config);
        if (vertical === "furniture" && config?.siteId) {
          await syncFurnitureEcommerceSku(product, existing.id, config);
        }
        cache[shopifyProductId] = {
          hash: currentHash,
          contentHash: currentContentHash,
          webflowId: existing.id,
          lastQty: qty,
          vertical,
          ...soldMarkedAtPayload(cacheEntry, qty),
        };
        webflowLog("info", { event: "cache.mutated", shopifyProductId, op: "sold", webflowId: existing.id, vertical });
        return { operation: "sold", id: existing.id };
      }
      // Already sold in Webflow + Shopify qty 0, but we never PATCH date-sold (e.g. manual SOLD toggle or pre–date-sold deploy) → retention sees no clock; fix without full LLM/write.
      if (
        vertical === "furniture" &&
        config?.siteId &&
        webflowListingLooksSold(existing, "furniture") &&
        shopifyQtySaysSold(qty)
      ) {
        const furnSlug = getFurnitureSoldSinceFieldSlug();
        if (parseSoldTimestampMsFromWebflowField(existing.fieldData || {}, furnSlug) == null) {
          webflowLog("info", {
            event: "sync_product.skip_unchanged.patch_missing_date_sold",
            shopifyProductId,
            productTitle: product.title,
            webflowId: existing.id,
            message: "Sold in Webflow but date-sold empty; patching for retention",
          });
          await markAsSold(existing, vertical, config);
          await syncFurnitureEcommerceSku(product, existing.id, config);
          cache[shopifyProductId] = {
            hash: currentHash,
            contentHash: currentContentHash,
            webflowId: existing.id,
            lastQty: qty,
            vertical,
            ...soldMarkedAtPayload(cacheEntry, qty),
          };
          return { operation: "update", id: existing.id };
        }
      }
      if (vertical === "furniture" && config?.siteId && furnitureEcommerceNeedsImageRepairFromShopify(product, existing)) {
        webflowLog("info", {
          event: "sync_product.skip_unchanged.repair_missing_images",
          shopifyProductId,
          productTitle: product.title,
          webflowId: existing.id,
          message: "Shopify has images but Webflow SKU has none; forcing SKU sync",
        });
        try {
          await syncFurnitureEcommerceSku(product, existing.id, config);
        } catch (err) {
          webflowLog("error", {
            event: "sync_product.skip_unchanged.repair_missing_images_failed",
            shopifyProductId,
            webflowId: existing.id,
            message: err?.message ?? String(err),
          });
          throw err;
        }
        cache[shopifyProductId] = {
          hash: currentHash,
          contentHash: currentContentHash,
          webflowId: existing.id,
          lastQty: qty,
          vertical,
          ...soldMarkedAtPayload(cacheEntry, qty),
        };
        webflowLog("info", { event: "cache.mutated", shopifyProductId, op: "repair_images", webflowId: existing.id, vertical });
        return { operation: "update", id: existing.id };
      }
      if (vertical === "luxury" && config?.collectionId && luxuryCmsNeedsImageRepairFromShopify(product, existing)) {
        webflowLog("info", {
          event: "sync_product.skip_unchanged.repair_missing_images",
          shopifyProductId,
          productTitle: product.title,
          webflowId: existing.id,
          message: "Shopify has images but Luxury CMS has none; patching images",
        });
        try {
          await patchLuxuryCmsImagesFromShopify(product, existing, config);
        } catch (err) {
          webflowLog("error", {
            event: "sync_product.skip_unchanged.repair_missing_images_failed",
            shopifyProductId,
            webflowId: existing.id,
            message: err?.message ?? String(err),
          });
          throw err;
        }
        cache[shopifyProductId] = {
          hash: currentHash,
          contentHash: currentContentHash,
          webflowId: existing.id,
          lastQty: qty,
          vertical,
          ...soldMarkedAtPayload(cacheEntry, qty),
        };
        webflowLog("info", { event: "cache.mutated", shopifyProductId, op: "repair_images", webflowId: existing.id, vertical });
        return { operation: "update", id: existing.id };
      }
      cache[shopifyProductId] = {
        hash: currentHash,
        contentHash: currentContentHash,
        webflowId: existing.id,
        lastQty: qty,
        vertical,
        ...soldMarkedAtPayload(cacheEntry, qty),
      };
      webflowLog("info", { event: "sync_product.skip_unchanged", shopifyProductId, productTitle: product.title, webflowId: existing.id, message: "Name/description/shopify snapshot unchanged; skipped LLM and write" });
      return { operation: "skip", id: existing.id };
    }
  }

  // When cache is missing (e.g. didn't persist after deploy), don't call LLM — check Webflow first. If item exists there, use it and repopulate cache.
  if (!recoveredFromWebflow) {
    const noCacheEntry = !cacheEntry?.webflowId;
    const inFurniture = noCacheEntry && !forceReclassify ? furnitureProductIndex?.byShopifyId?.get(shopifyProductId) : null;
    const inLuxury = noCacheEntry && !forceReclassify ? luxuryItemIndex?.byShopifyId?.get(shopifyProductId) : null;
    recoveredFromWebflow = inFurniture ? { vertical: "furniture", fromWebflowIndex: true } : inLuxury ? { vertical: "luxury", fromWebflowIndex: true } : null;
  }
  if (recoveredFromWebflow?.fromWebflowIndex) {
    webflowLog("info", { event: "sync_product.skip_llm_cache_missing_found_in_webflow", shopifyProductId, vertical: recoveredFromWebflow.vertical, message: "Cache missing but item exists in Webflow; skipping LLM" });
  }

  // Sold items (qty 0): never call the LLM — use product type + title hints for vertical (index lookup above may already have set it).
  if (!recoveredFromWebflow && qty !== null && qty <= 0 && !forceReclassify) {
    const pt = (product.product_type ?? "").toLowerCase();
    const title = (product.title ?? "").toLowerCase();
    const furnitureTitleHints = [
      "dresser",
      "chair",
      "table",
      "cabinet",
      "bed",
      "console",
      "sectional",
      "sofa",
      "loveseat",
      "ottoman",
      "credenza",
      "buffet",
      "nightstand",
      "desk",
      "hutch",
      "recliner",
      "bookcase",
      "bench",
      "chaise",
      "sideboard",
      "armoire",
      "wardrobe",
      "etagere",
      "headboard",
      "footboard",
      "vanity",
      "stool",
      "lamp",
      "lampshade",
      "chandelier",
      "sconce",
      "torchiere",
      "lighting",
    ];
    const titleLooksFurniture = furnitureTitleHints.some((w) => title.includes(w));
    const soldVertical =
      pt.includes("furniture") || pt.includes("home") || titleLooksFurniture ? "furniture" : "luxury";
    recoveredFromWebflow = { vertical: soldVertical, soldNoLlm: true };
    webflowLog("info", {
      event: "sync_product.skip_llm_sold",
      shopifyProductId,
      vertical: soldVertical,
      message: "Sold item (qty 0); skipping LLM, using heuristic vertical",
    });
  }

  let vertical, detectedVertical, verticalCorrected;
  if (!recoveredFromWebflow) {
    const llmLogPayload = {};
    const llmResult = await classifyWithLLM(product, llmLogPayload, webflowLog);
    const llmDetectedVertical = llmResult.category === "LUXURY" ? "luxury" : "furniture";
    const evidenceVertical = resolveVerticalFromEvidence(product, llmDetectedVertical);
    detectedVertical = evidenceVertical.vertical;
    const correctedToLuxury = cacheEntry?.vertical === "furniture" && detectedVertical === "luxury";
    const correctedToFurniture = cacheEntry?.vertical === "luxury" && detectedVertical === "furniture";
    vertical = correctedToLuxury
      ? "luxury"
      : correctedToFurniture
        ? "furniture"
        : (cacheEntry?.vertical ?? detectedVertical);
    verticalCorrected = correctedToLuxury;
    webflowLog("info", {
    event: "vertical.resolved",
    shopifyProductId,
    detectedVertical,
    cacheVertical: cacheEntry?.vertical ?? null,
    vertical,
    corrected: verticalCorrected,
    llmConfidence: llmResult.confidence,
    llmReasoning: llmResult.reasoning?.slice(0, 120),
    llmOverride: llmLogPayload.override ?? null,
    });
    if (evidenceVertical.reason !== "llm") {
      webflowLog("info", {
        event: "vertical.override_evidence",
        shopifyProductId,
        productTitle: product.title || "",
        reason: evidenceVertical.reason,
        llmDetectedVertical,
        finalVertical: detectedVertical,
        message: "Evidence guard overrode LLM vertical using title/description/dimensions/weight",
      });
    }
    if (llmLogPayload.raw != null || llmLogPayload.override) {
      webflowLog("info", { event: "llm_vertical.audit", shopifyProductId, ...llmLogPayload });
    }

    // When we correct furniture → luxury, remove the mistaken product from Webflow (archive) and clear cache so we create in luxury.
    // If it's already archived, don't re-archive and don't send duplicate email.
    if (verticalCorrected && cacheEntry?.webflowId && vertical === "luxury") {
    const furnitureConfig = getWebflowConfig("furniture");
    let alreadyArchived = false;
    if (furnitureConfig?.siteId && furnitureConfig?.token) {
      const full = await getWebflowEcommerceProductById(furnitureConfig.siteId, cacheEntry.webflowId, furnitureConfig.token);
      alreadyArchived = full?.isArchived === true;
      if (alreadyArchived) {
        webflowLog("info", { event: "vertical.corrected.skipped_already_archived", shopifyProductId, webflowId: cacheEntry.webflowId });
        saveDuplicatePlacementSentId(shopifyProductId);
      } else {
        try {
          await deleteWebflowEcommerceProduct(furnitureConfig.siteId, cacheEntry.webflowId, furnitureConfig.token);
          webflowLog("info", { event: "vertical.corrected.removed", shopifyProductId, webflowId: cacheEntry.webflowId });
        } catch (err) {
          webflowLog("error", { event: "vertical.corrected.archive_failed", shopifyProductId, webflowId: cacheEntry.webflowId, message: err.message });
        }
      }
    }
    const duplicateLog = {
      productTitle: product.title || "",
      shopifyProductId,
      previousVertical: "furniture",
      detectedVertical: "luxury",
      webflowItemIdRemoved: cacheEntry.webflowId,
    };
    delete cache[shopifyProductId];
    // Same as luxury→furniture below: must force LLM + skip stale index recovery — otherwise
    // furnitureProductIndex still has this Shopify id and we re-sync as furniture without creating luxury.
    const result = await syncSingleProductCore(product, cache, { ...options, forceReclassify: true });
    return { ...result, duplicateCorrected: !alreadyArchived, duplicateLog };
  }

  // When we correct luxury → furniture (e.g. masquerade mask was in Luxury, classifier now says Furniture), remove from Luxury and create in Furniture.
  if (correctedToFurniture && cacheEntry?.webflowId && vertical === "furniture") {
    const luxuryConfig = getWebflowConfig("luxury");
    if (luxuryConfig?.collectionId && luxuryConfig?.token) {
      try {
        await deleteWebflowCollectionItem(luxuryConfig.collectionId, cacheEntry.webflowId, luxuryConfig.token);
        webflowLog("info", { event: "vertical.corrected_luxury_to_furniture.removed", shopifyProductId, webflowId: cacheEntry.webflowId });
      } catch (err) {
        webflowLog("error", { event: "vertical.corrected_luxury_to_furniture.delete_failed", shopifyProductId, webflowId: cacheEntry.webflowId, message: err.message });
      }
    }
    delete cache[shopifyProductId];
    const result = await syncSingleProductCore(product, cache, { ...options, forceReclassify: true });
    return {
      ...result,
      duplicateCorrected: true,
      duplicateLog: {
        productTitle: product.title || "",
        shopifyProductId,
        previousVertical: "luxury",
        detectedVertical: "furniture",
        webflowItemIdRemoved: cacheEntry.webflowId,
      },
    };
  }

  // Cleanup: ensure this product exists only in the current vertical. Remove from the other vertical if found.
  const slugForCleanup = product.handle || "";
  const shopifyUrlForCleanup = `https://${process.env.SHOPIFY_STORE || ""}.myshopify.com/products/${slugForCleanup}`;

  if (vertical === "luxury") {
    const furnitureConfig = getWebflowConfig("furniture");
    if (furnitureConfig?.siteId && furnitureConfig?.token) {
      const existingInFurniture = await findExistingWebflowEcommerceProduct(
        shopifyProductId,
        slugForCleanup,
        furnitureConfig,
        product.title || null
      );
      if (existingInFurniture) {
        const full = await getWebflowEcommerceProductById(furnitureConfig.siteId, existingInFurniture.id, furnitureConfig.token);
        const alreadyArchived = full?.isArchived === true;
        if (alreadyArchived) {
          webflowLog("info", { event: "cleanup.skipped_already_archived", shopifyProductId, webflowId: existingInFurniture.id });
          saveDuplicatePlacementSentId(shopifyProductId);
        } else {
          webflowLog("info", {
            event: "cleanup.found_in_other_vertical",
            shopifyProductId,
            currentVertical: "luxury",
            otherVertical: "furniture",
            webflowId: existingInFurniture.id,
            productTitle: product.title,
          });
          try {
            await deleteWebflowEcommerceProduct(furnitureConfig.siteId, existingInFurniture.id, furnitureConfig.token);
            webflowLog("info", { event: "cleanup.removed_from_furniture", shopifyProductId, webflowId: existingInFurniture.id });
            await sendDuplicatePlacementEmail(
              {
                productTitle: product.title || "",
                shopifyProductId,
                previousVertical: "furniture",
                detectedVertical: "luxury",
                webflowItemIdRemoved: existingInFurniture.id,
                sweep: true,
              },
              duplicateEmailSentFor
            );
          } catch (err) {
            webflowLog("error", { event: "cleanup.remove_furniture_failed", shopifyProductId, webflowId: existingInFurniture.id, message: err.message });
          }
        }
      }
    }
  }

  if (vertical === "furniture") {
    const luxuryConfig = getWebflowConfig("luxury");
    if (luxuryConfig?.collectionId && luxuryConfig?.token) {
      const existingInLuxury = await findExistingWebflowItem(shopifyProductId, shopifyUrlForCleanup, slugForCleanup, luxuryConfig);
      if (existingInLuxury) {
        webflowLog("info", {
          event: "cleanup.found_in_other_vertical",
          shopifyProductId,
          currentVertical: "furniture",
          otherVertical: "luxury",
          webflowId: existingInLuxury.id,
          productTitle: product.title,
        });
        try {
          await deleteWebflowCollectionItem(luxuryConfig.collectionId, existingInLuxury.id, luxuryConfig.token);
          webflowLog("info", { event: "cleanup.deleted_from_luxury", shopifyProductId, webflowId: existingInLuxury.id });
          await sendDuplicatePlacementEmail(
            {
              productTitle: product.title || "",
              shopifyProductId,
              previousVertical: "luxury",
              detectedVertical: "furniture",
              webflowItemIdRemoved: existingInLuxury.id,
              sweep: true,
            },
            duplicateEmailSentFor
          );
        } catch (err) {
          webflowLog("error", { event: "cleanup.delete_luxury_failed", shopifyProductId, webflowId: existingInLuxury.id, message: err.message });
        }
      }
    }
  }
  } else {
    vertical = recoveredFromWebflow.vertical;
    detectedVertical = vertical;
    const evidenceVertical = resolveVerticalFromEvidence(product, detectedVertical);
    if (evidenceVertical.vertical !== vertical) {
      vertical = evidenceVertical.vertical;
      detectedVertical = evidenceVertical.vertical;
      webflowLog("info", {
        event: "vertical.override_evidence_recovered",
        shopifyProductId,
        productTitle: product.title || "",
        reason: evidenceVertical.reason,
        message: "Recovered vertical was overridden by evidence guard",
      });
    }
  }

  const config = getWebflowConfig(vertical);

  // Use name + description FROM Shopify to decide; then write our decision back to Shopify and update the correct Webflow collection (Luxury or Furniture).
  let name = product.title;
  let description = product.body_html;
  let price = product.variants?.[0]?.price || null;
  let slug = product.handle;

  let detectedBrand =
    vertical === "furniture"
      ? detectBrandFromProductFurniture(product.title, product.body_html, product.vendor)
      : detectBrandFromProduct(product.title, product.vendor);
  const brand = detectedBrand || "Unknown";

  const allImages = (product.images || []).map((img) => img.src);
  const featuredImage = allImages[0] || null;
  const gallery = allImages.slice(1);

  const soldNow = shopifyQtySaysSold(qty);

  // Single source of truth: resolved VERTICAL. Department and category are derived from it everywhere (Webflow + Shopify).
  const productType = (product.product_type ?? "").trim();
  const department = vertical === "furniture" ? "Furniture & Home" : "Luxury Goods";
  let dimensionsStatus = null;
  const dimensions = getDimensionsFromProduct(product);
  let categoryForMetafield;
  if (recoveredFromWebflow) {
    // Cache-missing path: no LLM; use keyword-only category.
    if (vertical === "furniture") {
      const forcedCat = furnitureAccessoryCategoryOverrideTitle(name);
      let resolved = forcedCat ?? detectCategoryFurniture(name, description, getProductTagsArray(product), dimensions);
      if (furnitureSleepSurfaceIndicatesBedroom(name, description, getProductTagsArray(product))) resolved = "Bedroom";
      categoryForMetafield = mapFurnitureCategoryForShopify(resolved);
    } else {
      if (soldNow) categoryForMetafield = "Recently Sold";
      else {
        if (isShoeProduct(name, description)) categoryForMetafield = "Other ";
        else categoryForMetafield = detectLuxuryCategoryFromTitle(name, description) ?? "Other ";
        if (isJewelryOrAccessoryProduct(name, description) && !isBagOrAgendaProduct(name, description)) categoryForMetafield = "Accessories";
        if (isBeltProduct(name, description)) categoryForMetafield = "Belts";
        if (categoryForMetafield === "Accessories" && isBagOrAgendaProduct(name, description)) categoryForMetafield = detectLuxuryCategoryFromTitle(name, description) ?? "Other ";
      }
    }
  } else if (vertical === "furniture") {
    const llmCategory = await classifyCategoryWithLLM(product, "furniture", {}, webflowLog);
    let resolved = llmCategory?.category ?? detectCategoryFurniture(name, description, getProductTagsArray(product), dimensions);
    const forcedCat = furnitureAccessoryCategoryOverrideTitle(name);
    if (forcedCat) resolved = forcedCat;
    if (furnitureSleepSurfaceIndicatesBedroom(name, description, getProductTagsArray(product))) resolved = "Bedroom";
    categoryForMetafield = mapFurnitureCategoryForShopify(resolved);
  } else {
    if (soldNow) {
      categoryForMetafield = "Recently Sold";
    } else {
      const llmCategory = await classifyCategoryWithLLM(product, "luxury", {}, webflowLog);
      if (llmCategory?.category) {
        categoryForMetafield = mapCategoryForShopify(llmCategory.category);
      } else {
        if (isShoeProduct(name, description)) categoryForMetafield = "Other ";
        else {
          const fromTitle = detectLuxuryCategoryFromTitle(name, description);
          categoryForMetafield = fromTitle ?? "Other ";
        }
      }
      if (isJewelryOrAccessoryProduct(name, description) && !isBagOrAgendaProduct(name, description)) categoryForMetafield = "Accessories";
      if (isBeltProduct(name, description)) categoryForMetafield = "Belts";
      if (categoryForMetafield === "Accessories" && isBagOrAgendaProduct(name, description)) {
        const fromTitle = detectLuxuryCategoryFromTitle(name, description);
        categoryForMetafield = fromTitle ?? "Other ";
      }
    }
  }
  const shopifyDepartment = department;
  const shopifyCategoryValue = categoryForMetafield;
  const category = shopifyCategoryValue;

  const showOnWebflow = vertical === "luxury" ? !soldNow : true;
  const shopifyUrl = `https://${process.env.SHOPIFY_STORE}.myshopify.com/products/${slug}`;

  // Dimensions status + append to description when present (never append Shopify tag lists — Tags: legacy is stripped below always).
  const originalDescription = product.body_html || "";
  let descriptionChanged = false;

  const descWithoutLegacyTags = stripLegacyAutomatedTagsParagraph(description || "");
  if (descWithoutLegacyTags !== (description || "")) {
    description = descWithoutLegacyTags;
    descriptionChanged = true;
  }

  if (vertical === "furniture") {
    dimensionsStatus = hasAnyDimensions(dimensions) ? "present" : "missing";
  }
  if (hasAnyDimensions(dimensions)) {
    const dimStr = formatDimensionsForDescription(dimensions);
    if (dimStr) {
      const body = stripAppendedSyncFooter(description || "").trim();
      const newDescription = (body + "<br><br>" + dimStr).trim();

      if (newDescription !== originalDescription) {
        description = newDescription;
        descriptionChanged = true;
      }
    }
  }

  // Furniture only: missing weight — email once per product; append validation note to Shopify/Webflow description.
  // Luxury: no missing-weight emails or notes; strip any legacy note and clear dedupe id so a future furniture sync can alert if reclassified.
  const weightMissing =
    dimensions.weight == null || Number.isNaN(dimensions.weight) || dimensions.weight <= 0;
  const notSold = !soldNow && shopifyCategoryValue !== "Recently Sold";
  const trackMissingWeight = notSold && vertical === "furniture";

  if (soldNow || shopifyCategoryValue === "Recently Sold") {
    const cleaned = stripWeightValidateNote(description || "").trimEnd();
    if (cleaned !== (description || "")) {
      description = cleaned;
      descriptionChanged = cleaned !== originalDescription;
    }
  }

  if (vertical === "luxury") {
    clearWeightMissingEmailSentId(shopifyProductId);
    const cleanedLux = stripWeightValidateNote(description || "").trimEnd();
    if (cleanedLux !== (description || "")) {
      description = cleanedLux;
      descriptionChanged = cleanedLux !== originalDescription || descriptionChanged;
    }
  }

  if (trackMissingWeight) {
    if (weightMissing) {
      await sendMissingWeightAlertEmail(product, dimensions, "Furniture & Home");
      const withoutNote = stripWeightValidateNote(description || "").trimEnd();
      const withNote = withoutNote + WEIGHT_VALIDATE_NOTE_HTML;
      if (withNote !== (description || "")) {
        description = withNote;
        descriptionChanged = withNote !== originalDescription;
      }
    } else {
      clearWeightMissingEmailSentId(shopifyProductId);
      const cleaned = stripWeightValidateNote(description || "").trimEnd();
      if (cleaned !== (description || "")) {
        description = cleaned;
        descriptionChanged = cleaned !== originalDescription;
      }
    }
  }

  try {
    // Remove "Condition" option only for products we're actually syncing as Furniture.
    if (vertical === "furniture") {
      await removeConditionOptionIfFurniture(product);
    }

    // Write metafields + vendor/type/tags to Shopify so Shopify matches the vertical we're syncing to Webflow.
    if (shopifyCategoryValue !== "Recently Sold") {
      await updateShopifyMetafields(shopifyProductId, {
        department: shopifyDepartment,
        category: shopifyCategoryValue,
        vertical: vertical === "furniture" ? "furniture" : "luxury",
        dimensionsStatus: vertical === "furniture" ? dimensionsStatus : undefined,
      });
      // Only pass description if it changed
      await updateShopifyVendorAndType(
        shopifyProductId,
        brand,
        shopifyCategoryValue,
        getProductTagsArray(product),
        shopifyDepartment,
        shopifyCategoryValue,
        descriptionChanged ? description : null
      );
    }
  } catch (err) {
    // Do not fail the whole product sync on transient Shopify write errors; continue to Webflow + cache update.
    const detail = shopifyWriteFailureEmailText(err);
    await sendShopifyWriteFailureEmail(
      {
        op: "shopify_metafields_or_product_update",
        shopifyProductId,
        productTitle: name,
        vertical,
        attempts: err?._retryAttempts || SHOPIFY_WRITE_MAX_ATTEMPTS,
        ...detail,
      },
      shopifyWriteEmailSentFor
    );
    webflowLog("error", {
      event: "sync_product.shopify_write_failed_continue",
      shopifyProductId,
      productTitle: name,
      vertical,
      message: err?.message ?? String(err),
      status: err?.response?.status ?? null,
      url: err?.config?.url ?? null,
      method: err?.config?.method ?? null,
    });
  }

  // RULE: Only touch Webflow when (1) item removed from Shopify, or (2) item changed in Shopify from previous run.
  // Otherwise we do not call Webflow at all. When we do go to Webflow we retrieve the item, compare to what we'd send, and only update if there is a difference.
  const previousHash = cacheEntry?.hash || null;
  const hashUnchanged = previousHash && JSON.stringify(currentHash) === JSON.stringify(previousHash);
  const newlySoldCheck = shouldMarkSoldTransition(previousQty, qty);
  const shopifyQtySoldBlocksEarlySkip = shopifyQtySaysSold(qty);
  if (cacheEntry?.webflowId && hashUnchanged && !newlySoldCheck && !shopifyQtySoldBlocksEarlySkip) {
    // Verify cached item still exists in this vertical (fixes stale cache when item was archived or ID was wrong vertical)
    let cachedExists = false;
    let cachedFurnitureProduct = null;
    let cachedLuxuryItem = null;
    if (vertical === "furniture" && config?.siteId) {
      cachedFurnitureProduct = await getWebflowEcommerceProductById(config.siteId, cacheEntry.webflowId, config.token);
      cachedExists = cachedFurnitureProduct != null && !cachedFurnitureProduct.isArchived;
    } else if (vertical === "luxury" && config?.collectionId) {
      cachedLuxuryItem = await getWebflowItemById(cacheEntry.webflowId, config);
      cachedExists = cachedLuxuryItem != null;
    }
    if (!cachedExists) {
      webflowLog("info", {
        event: "sync_product.cache_stale",
        shopifyProductId,
        productTitle: name,
        webflowId: cacheEntry.webflowId,
        vertical,
        reason: "item_not_found_or_archived",
      });
      delete cache[shopifyProductId];
      // fall through to normal lookup/create
    } else {
      if (
        vertical === "furniture" &&
        cachedFurnitureProduct &&
        furnitureEcommerceNeedsImageRepairFromShopify(product, cachedFurnitureProduct)
      ) {
        webflowLog("info", {
          event: "sync_product.skip_early.repair_missing_images",
          shopifyProductId,
          productTitle: name,
          webflowId: cacheEntry.webflowId,
          message: "Shopify has images but Webflow SKU has none; forcing SKU sync",
        });
        await syncFurnitureEcommerceSku(product, cachedFurnitureProduct.id, config);
        cache[shopifyProductId] = {
          hash: currentHash,
          contentHash: currentContentHash,
          webflowId: cacheEntry.webflowId,
          lastQty: qty,
          vertical,
          ...soldMarkedAtPayload(cacheEntry, qty),
        };
        webflowLog("info", { event: "cache.mutated", shopifyProductId, op: "repair_images", webflowId: cacheEntry.webflowId, vertical });
        return { operation: "update", id: cacheEntry.webflowId };
      }
      if (vertical === "luxury" && cachedLuxuryItem && luxuryCmsNeedsImageRepairFromShopify(product, cachedLuxuryItem)) {
        webflowLog("info", {
          event: "sync_product.skip_early.repair_missing_images",
          shopifyProductId,
          productTitle: name,
          webflowId: cacheEntry.webflowId,
          message: "Shopify has images but Luxury CMS has none; patching images",
        });
        await patchLuxuryCmsImagesFromShopify(product, cachedLuxuryItem, config);
        cache[shopifyProductId] = {
          hash: currentHash,
          contentHash: currentContentHash,
          webflowId: cacheEntry.webflowId,
          lastQty: qty,
          vertical,
          ...soldMarkedAtPayload(cacheEntry, qty),
        };
        webflowLog("info", { event: "cache.mutated", shopifyProductId, op: "repair_images", webflowId: cacheEntry.webflowId, vertical });
        return { operation: "update", id: cacheEntry.webflowId };
      }
      webflowLog("info", { event: "sync_product.skip_early_no_webflow", shopifyProductId, productTitle: name, webflowId: cacheEntry.webflowId, reason: "shopify_unchanged" });
      cache[shopifyProductId] = {
        hash: currentHash,
        contentHash: currentContentHash,
        webflowId: cacheEntry.webflowId,
        lastQty: qty,
        vertical,
        ...soldMarkedAtPayload(cacheEntry, qty),
      };
      webflowLog("info", { event: "cache.mutated", shopifyProductId, op: "skip", webflowId: cacheEntry.webflowId, vertical });
      return { operation: "skip", id: cacheEntry.webflowId };
    }
  }

  webflowLog("info", {
    event: "sync_product.entry",
    shopifyProductId,
    productTitle: name,
    vertical,
    cacheWebflowId: cacheEntry?.webflowId ?? null,
    previousQty,
    currentQty: qty,
    department: shopifyDepartment,
    category,
    categoryChild: shopifyCategoryValue,
    soldNow,
    shopifyUrl,
    slug,
    ...(vertical === "furniture" && { dimensionsStatus }),
  });

  // Retrieve from Webflow only when we might need to update (hash changed or no cache). Then compare; only PATCH if different.
  let existing = null;
  if (vertical === "furniture" && config.siteId) {
    if (cacheEntry?.webflowId) {
      webflowLog("info", { event: "sync_product.try_cache", shopifyProductId, cacheWebflowId: cacheEntry.webflowId, target: "ecommerce" });
      existing = await getWebflowEcommerceProductById(config.siteId, cacheEntry.webflowId, config.token);
      if (!existing) webflowLog("info", { event: "sync_product.cache_miss", shopifyProductId, reason: "ecommerce_not_found" });
    }
    if (!existing) {
      existing = await findExistingWebflowEcommerceProduct(shopifyProductId, slug, config, name);
    }
  } else {
    if (cacheEntry?.webflowId) {
      webflowLog("info", { event: "sync_product.try_cache", shopifyProductId, cacheWebflowId: cacheEntry.webflowId, target: "cms" });
      existing = await getWebflowItemById(cacheEntry.webflowId, config);
      if (!existing) webflowLog("info", { event: "sync_product.cache_miss", shopifyProductId, reason: "cms_not_found" });
    }
    if (!existing) {
      existing = await findExistingWebflowItem(shopifyProductId, shopifyUrl, slug, config);
    }
  }

  // RULE: If an item exists in Webflow with this Shopify product ID anywhere, never create a new one.
  // Final lookup by Shopify ID across both verticals so we never duplicate (e.g. cache/slug mismatch).
  if (!existing) {
    const inFurniture = furnitureProductIndex?.byShopifyId?.get(String(shopifyProductId));
    const inLuxury = luxuryItemIndex?.byShopifyId?.get(String(shopifyProductId));
    if (vertical === "furniture" && inFurniture) {
      existing = inFurniture;
      webflowLog("info", { event: "sync_product.found_by_shopify_id", shopifyProductId, productTitle: name, webflowId: existing.id, vertical, source: "furniture_index" });
    } else if (vertical === "luxury" && inLuxury) {
      existing = inLuxury;
      webflowLog("info", { event: "sync_product.found_by_shopify_id", shopifyProductId, productTitle: name, webflowId: existing.id, vertical, source: "luxury_index" });
    }
  }

  if (existing) {
    webflowLog("info", { event: "sync_product.linked", shopifyProductId, productTitle: name, webflowId: existing.id });

    if (vertical === "furniture" && config?.siteId) {
      if (!Object.prototype.hasOwnProperty.call(existing, "isArchived")) {
        const live = await getWebflowEcommerceProductById(config.siteId, existing.id, config.token);
        if (live) existing = live;
      }
      if (existing.isArchived === true) {
        cache[shopifyProductId] = {
          hash: currentHash,
          contentHash: currentContentHash,
          webflowId: existing.id,
          lastQty: qty,
          vertical,
          ...soldMarkedAtPayload(cacheEntry, qty),
        };
        webflowLog("info", {
          event: "sync_product.skip_archived_furniture",
          shopifyProductId,
          productTitle: name,
          webflowId: existing.id,
          message: "Listing archived; skipping mark-sold and product updates",
        });
        return { operation: "skip", id: existing.id };
      }
    }

    const repairSold = needsWebflowSoldRepair(existing, vertical, qty);
    const mustMarkSold =
      shouldMarkSoldTransition(previousQty, qty) || repairSold;

    if (mustMarkSold) {
      const fromQtyDrop =
        !repairSold &&
        previousQty != null &&
        Number(previousQty) > 0 &&
        shopifyQtySaysSold(qty);
      webflowLog("info", {
        event: repairSold ? "sync_product.repair_sold" : "sync_product.newly_sold",
        shopifyProductId,
        productTitle: name,
        webflowId: existing.id,
        vertical,
        previousQty,
        currentQty: qty,
        ...(fromQtyDrop ? { reason: "inventory_1_to_0_or_in_stock_to_sold" } : {}),
      });
      await markAsSold(existing, vertical, config);
      if (vertical === "furniture" && config.siteId) {
        await syncFurnitureEcommerceSku(product, existing.id, config);
      }
      cache[shopifyProductId] = {
        hash: currentHash,
        contentHash: currentContentHash,
        webflowId: existing.id,
        lastQty: qty,
        vertical,
        ...soldMarkedAtPayload(cacheEntry, qty),
      };
      webflowLog("info", { event: "cache.mutated", shopifyProductId, op: "sold", webflowId: existing.id, vertical });
      return { operation: "sold", id: existing.id };
    }

    const previousHash = cacheEntry?.hash || null;
    const changed =
      !previousHash ||
      JSON.stringify(currentHash) !== JSON.stringify(previousHash);

    if (changed) {
      const fieldData = buildWebflowFieldData({
        vertical,
        name,
        brand,
        price,
        description,
        shopifyProductId,
        shopifyUrl,
        shopifySlug: slug,
        productType: product.product_type,
        category,
        featuredImage,
        gallery,
        showOnWebflow,
        soldNow,
        dimensions,
        dimensionsStatus,
        existingSlug: existing.fieldData?.slug,
        newSlug: slug,
        existingFieldData: existing.fieldData || null,
      });
      const existingFD = existing.fieldData || {};
      if (fieldDataEffectivelyEqual(fieldData, existingFD)) {
        if (vertical === "furniture" && config?.siteId && furnitureEcommerceNeedsImageRepairFromShopify(product, existing)) {
          webflowLog("info", {
            event: "sync_product.skip_webflow_unchanged.repair_missing_images",
            shopifyProductId,
            productTitle: name,
            webflowId: existing.id,
            message: "Product fieldData matches but Webflow SKU has no images; forcing SKU sync",
          });
          await syncFurnitureEcommerceSku(product, existing.id, config);
          cache[shopifyProductId] = {
            hash: currentHash,
            contentHash: currentContentHash,
            webflowId: existing.id,
            lastQty: qty,
            vertical,
            ...soldMarkedAtPayload(cacheEntry, qty),
          };
          webflowLog("info", { event: "cache.mutated", shopifyProductId, op: "repair_images", webflowId: existing.id, vertical });
          return { operation: "update", id: existing.id };
        }
        if (vertical === "luxury" && config?.collectionId && luxuryCmsNeedsImageRepairFromShopify(product, existing)) {
          webflowLog("info", {
            event: "sync_product.skip_webflow_unchanged.repair_missing_images",
            shopifyProductId,
            productTitle: name,
            webflowId: existing.id,
            message: "CMS fieldData matches text but images missing; patching images",
          });
          await patchLuxuryCmsImagesFromShopify(product, existing, config);
          cache[shopifyProductId] = {
            hash: currentHash,
            contentHash: currentContentHash,
            webflowId: existing.id,
            lastQty: qty,
            vertical,
            ...soldMarkedAtPayload(cacheEntry, qty),
          };
          webflowLog("info", { event: "cache.mutated", shopifyProductId, op: "repair_images", webflowId: existing.id, vertical });
          return { operation: "update", id: existing.id };
        }
        webflowLog("info", { event: "sync_product.skip_webflow_unchanged", shopifyProductId, productTitle: name, webflowId: existing.id, message: "Retrieved from Webflow; same as Shopify would send; not touching" });
        cache[shopifyProductId] = {
          hash: currentHash,
          contentHash: currentContentHash,
          webflowId: existing.id,
          lastQty: qty,
          vertical,
          ...soldMarkedAtPayload(cacheEntry, qty),
        };
        webflowLog("info", { event: "cache.mutated", shopifyProductId, op: "skip", webflowId: existing.id, vertical });
        return { operation: "skip", id: existing.id };
      }
      webflowLog("info", { event: "sync_product.updating", shopifyProductId, productTitle: name, webflowId: existing.id, reason: "shopify_changed_and_webflow_differs" });
      if (vertical === "furniture" && config.siteId) {
        await updateWebflowEcommerceProduct(config.siteId, existing.id, fieldData, config.token, existing);
        await syncFurnitureEcommerceSku(product, existing.id, config);
      } else {
        await axios.patch(
          `https://api.webflow.com/v2/collections/${config.collectionId}/items/${existing.id}`,
          { fieldData },
          {
            headers: {
              Authorization: `Bearer ${config.token}`,
              "Content-Type": "application/json",
            },
          }
        );
      }
      cache[shopifyProductId] = {
        hash: currentHash,
        contentHash: currentContentHash,
        webflowId: existing.id,
        lastQty: qty,
        vertical,
        ...soldMarkedAtPayload(cacheEntry, qty),
      };
      webflowLog("info", { event: "cache.mutated", shopifyProductId, op: "update", webflowId: existing.id, vertical });
      return { operation: "update", id: existing.id };
    }

    if (!changed && WEBFLOW_STRICT_NOOP_UPDATES) {
      webflowLog("info", {
        event: "sync_product.skip_no_changes_no_touch",
        shopifyProductId,
        productTitle: name,
        webflowId: existing.id,
        vertical,
        message: "No Shopify changes and strict no-op mode enabled; skipping image/sync repair touches",
      });
      cache[shopifyProductId] = {
        hash: currentHash,
        contentHash: currentContentHash,
        webflowId: existing.id,
        lastQty: qty,
        vertical,
        ...soldMarkedAtPayload(cacheEntry, qty),
      };
      webflowLog("info", { event: "cache.mutated", shopifyProductId, op: "skip", webflowId: existing.id, vertical });
      return { operation: "skip", id: existing.id };
    }

    if (!changed && vertical === "furniture" && config?.siteId && furnitureEcommerceNeedsImageRepairFromShopify(product, existing)) {
      webflowLog("info", {
        event: "sync_product.skip_no_changes.repair_missing_images",
        shopifyProductId,
        productTitle: name,
        webflowId: existing.id,
        message: "Shopify snapshot unchanged but Webflow SKU has no images; forcing SKU sync",
      });
      await syncFurnitureEcommerceSku(product, existing.id, config);
      cache[shopifyProductId] = {
        hash: currentHash,
        contentHash: currentContentHash,
        webflowId: existing.id,
        lastQty: qty,
        vertical,
        ...soldMarkedAtPayload(cacheEntry, qty),
      };
      webflowLog("info", { event: "cache.mutated", shopifyProductId, op: "repair_images", webflowId: existing.id, vertical });
      return { operation: "update", id: existing.id };
    }
    if (!changed && vertical === "luxury" && config?.collectionId && luxuryCmsNeedsImageRepairFromShopify(product, existing)) {
      webflowLog("info", {
        event: "sync_product.skip_no_changes.repair_missing_images",
        shopifyProductId,
        productTitle: name,
        webflowId: existing.id,
        message: "Shopify snapshot unchanged but Luxury CMS has no images; patching images",
      });
      await patchLuxuryCmsImagesFromShopify(product, existing, config);
      cache[shopifyProductId] = {
        hash: currentHash,
        contentHash: currentContentHash,
        webflowId: existing.id,
        lastQty: qty,
        vertical,
        ...soldMarkedAtPayload(cacheEntry, qty),
      };
      webflowLog("info", { event: "cache.mutated", shopifyProductId, op: "repair_images", webflowId: existing.id, vertical });
      return { operation: "update", id: existing.id };
    }

    webflowLog("info", { event: "sync_product.skip_no_changes", shopifyProductId, productTitle: name, webflowId: existing.id });
    cache[shopifyProductId] = {
      hash: currentHash,
      contentHash: currentContentHash,
      webflowId: existing.id,
      lastQty: qty,
      vertical,
      ...soldMarkedAtPayload(cacheEntry, qty),
    };
    webflowLog("info", { event: "cache.mutated", shopifyProductId, op: "skip", webflowId: existing.id, vertical });
    return { operation: "skip", id: existing.id };
  }

  // Create when no existing item found — either no cache, or cache pointed to deleted Webflow item
  if (!existing) {
    // Don't recreate sold items (qty 0). If you deleted them from Webflow, we won't push them back.
    if (soldNow) {
      // Write cache so next run skips LLM for this already-classified sold item.
      cache[shopifyProductId] = {
        hash: currentHash,
        contentHash: currentContentHash,
        webflowId: null,
        lastQty: 0,
        vertical,
        ...soldMarkedAtPayload(cacheEntry, 0),
      };
      webflowLog("info", { event: "cache.mutated", shopifyProductId, op: "skip_sold", webflowId: null, vertical });
      webflowLog("info", {
        event: "sync_product.skip_create_sold",
        shopifyProductId,
        productTitle: name,
        reason: "qty 0 — not recreating deleted sold items",
      });
      return { operation: "skip", id: null };
    }

    webflowLog("info", {
      event: "sync_product.create_path",
      shopifyProductId,
      productTitle: name,
      message: cacheEntry ? "CACHE MISS (Webflow item deleted) → Creating new" : "NO CACHE + NO MATCH → Creating new Webflow item",
    });

    // RULE: Never create if an item with this Shopify product ID already exists in the TARGET vertical. Update the existing one; only PATCH if something changed.
    const guardFurnitureCfg = getWebflowConfig("furniture");
    const guardLuxuryCfg = getWebflowConfig("luxury");
    let alreadyInFurniture = furnitureProductIndex?.byShopifyId?.get(String(shopifyProductId)) ?? null;
    let alreadyInLuxury = luxuryItemIndex?.byShopifyId?.get(String(shopifyProductId)) ?? null;
    if (!alreadyInFurniture && guardFurnitureCfg?.siteId && guardFurnitureCfg?.token) {
      alreadyInFurniture = await findExistingWebflowEcommerceProduct(
        shopifyProductId,
        slug,
        guardFurnitureCfg,
        name
      );
    }
    if (!alreadyInLuxury && guardLuxuryCfg?.collectionId && guardLuxuryCfg?.token) {
      alreadyInLuxury = await findExistingWebflowItem(shopifyProductId, shopifyUrl, slug, guardLuxuryCfg);
    }
    const existingFromGuard = (detectedVertical === "furniture" && alreadyInFurniture) ? alreadyInFurniture : (detectedVertical === "luxury" && alreadyInLuxury) ? alreadyInLuxury : null;
    if (existingFromGuard) {
      webflowLog("info", {
        event: "sync_product.found_existing_by_shopify_id",
        shopifyProductId,
        productTitle: name,
        webflowId: existingFromGuard.id,
        vertical: detectedVertical,
        message: "Item already exists; updating instead of creating. Will only PATCH if data differs.",
      });
      const guardConfig = getWebflowConfig(detectedVertical);
      let guardExisting = existingFromGuard;
      if (detectedVertical === "furniture" && guardConfig?.siteId) {
        if (!Object.prototype.hasOwnProperty.call(guardExisting, "isArchived")) {
          const live = await getWebflowEcommerceProductById(guardConfig.siteId, guardExisting.id, guardConfig.token);
          if (live) guardExisting = live;
        }
        if (guardExisting.isArchived === true) {
          cache[shopifyProductId] = {
            hash: currentHash,
            contentHash: currentContentHash,
            webflowId: guardExisting.id,
            lastQty: qty,
            vertical: detectedVertical,
            ...soldMarkedAtPayload(cacheEntry, qty),
          };
          webflowLog("info", {
            event: "sync_product.skip_create_guard_archived",
            shopifyProductId,
            productTitle: name,
            webflowId: guardExisting.id,
            message: "Furniture listing is archived; no create-guard PATCH",
          });
          return { operation: "skip", id: guardExisting.id };
        }
      }
      const guardRepair = needsWebflowSoldRepair(guardExisting, detectedVertical, qty);
      const guardMustSold = shouldMarkSoldTransition(previousQty, qty) || guardRepair;
      if (guardMustSold) {
        const fromQtyDrop =
          !guardRepair &&
          previousQty != null &&
          Number(previousQty) > 0 &&
          shopifyQtySaysSold(qty);
        webflowLog("info", {
          event: guardRepair ? "sync_product.repair_sold" : "sync_product.newly_sold",
          shopifyProductId,
          productTitle: name,
          webflowId: guardExisting.id,
          vertical: detectedVertical,
          source: "create_guard",
          previousQty,
          currentQty: qty,
          ...(fromQtyDrop ? { reason: "inventory_1_to_0_or_in_stock_to_sold" } : {}),
        });
        await markAsSold(guardExisting, detectedVertical, guardConfig);
        if (detectedVertical === "furniture" && guardConfig.siteId) {
          await syncFurnitureEcommerceSku(product, guardExisting.id, guardConfig);
        }
        cache[shopifyProductId] = {
          hash: currentHash,
          contentHash: currentContentHash,
          webflowId: guardExisting.id,
          lastQty: qty,
          vertical: detectedVertical,
          ...soldMarkedAtPayload(cacheEntry, qty),
        };
        webflowLog("info", { event: "cache.mutated", shopifyProductId, op: "sold", webflowId: guardExisting.id, vertical: detectedVertical });
        return { operation: "sold", id: guardExisting.id };
      }
      const fieldData = buildWebflowFieldData({
        vertical: detectedVertical,
        name,
        brand,
        price,
        description,
        shopifyProductId,
        shopifyUrl,
        shopifySlug: slug,
        productType: product.product_type,
        category,
        featuredImage,
        gallery,
        showOnWebflow,
        soldNow,
        dimensions,
        dimensionsStatus,
        existingSlug: guardExisting.fieldData?.slug,
        newSlug: slug,
        existingFieldData: guardExisting.fieldData || null,
      });
      const existingFD = guardExisting.fieldData || {};
      if (fieldDataEffectivelyEqual(fieldData, existingFD)) {
        webflowLog("info", { event: "sync_product.skip_webflow_unchanged", shopifyProductId, productTitle: name, webflowId: guardExisting.id, message: "Existing item matches; not touching" });
        cache[shopifyProductId] = {
          hash: currentHash,
          contentHash: currentContentHash,
          webflowId: guardExisting.id,
          lastQty: qty,
          vertical: detectedVertical,
          ...soldMarkedAtPayload(cacheEntry, qty),
        };
        webflowLog("info", { event: "cache.mutated", shopifyProductId, op: "skip", webflowId: guardExisting.id, vertical: detectedVertical });
        return { operation: "skip", id: guardExisting.id };
      }
      webflowLog("info", { event: "sync_product.updating", shopifyProductId, productTitle: name, webflowId: guardExisting.id, reason: "shopify_changed_and_webflow_differs" });
      if (detectedVertical === "furniture" && guardConfig.siteId) {
        await updateWebflowEcommerceProduct(guardConfig.siteId, guardExisting.id, fieldData, guardConfig.token, guardExisting);
        await syncFurnitureEcommerceSku(product, guardExisting.id, guardConfig);
      } else {
        await axios.patch(
          `https://api.webflow.com/v2/collections/${guardConfig.collectionId}/items/${guardExisting.id}`,
          { fieldData },
          {
            headers: {
              Authorization: `Bearer ${guardConfig.token}`,
              "Content-Type": "application/json",
            },
          }
        );
      }
      cache[shopifyProductId] = {
        hash: currentHash,
        contentHash: currentContentHash,
        webflowId: guardExisting.id,
        lastQty: qty,
        vertical: detectedVertical,
        ...soldMarkedAtPayload(cacheEntry, qty),
      };
      webflowLog("info", { event: "cache.mutated", shopifyProductId, op: "update", webflowId: guardExisting.id, vertical: detectedVertical });
      return { operation: "update", id: guardExisting.id };
    }

    // Sweep: if we're creating in Furniture, remove from Luxury so we never have the same Shopify ID in both.
    if (detectedVertical === "furniture" && alreadyInLuxury) {
      const luxuryConfig = getWebflowConfig("luxury");
      if (luxuryConfig?.collectionId && luxuryConfig?.token) {
        try {
          await deleteWebflowCollectionItem(luxuryConfig.collectionId, alreadyInLuxury.id, luxuryConfig.token);
          webflowLog("info", { event: "sweep.removed_from_luxury", shopifyProductId, webflowId: alreadyInLuxury.id, productTitle: name });
        } catch (err) {
          webflowLog("error", { event: "sweep.remove_luxury_failed", shopifyProductId, webflowId: alreadyInLuxury.id, message: err.message });
        }
      }
    }

    // Sweep: if we're creating in Luxury, check if this product wrongly exists in Furniture (e.g. no cache / cache lost). Archive it so it doesn't stay in both places.
    if (detectedVertical === "luxury") {
      const furnitureConfig = getWebflowConfig("furniture");
      if (furnitureConfig?.siteId && furnitureConfig?.token) {
        const existingInFurniture = await findExistingWebflowEcommerceProduct(shopifyProductId, slug, furnitureConfig, name);
        if (existingInFurniture) {
          const full = await getWebflowEcommerceProductById(furnitureConfig.siteId, existingInFurniture.id, furnitureConfig.token);
          const alreadyArchived = full?.isArchived === true;
          if (alreadyArchived) {
            webflowLog("info", { event: "sweep.skipped_already_archived", shopifyProductId, webflowId: existingInFurniture.id });
            saveDuplicatePlacementSentId(shopifyProductId);
          } else {
            webflowLog("info", {
              event: "sweep.found_in_furniture",
              shopifyProductId,
              webflowId: existingInFurniture.id,
              productTitle: name,
              message: "Archiving from Furniture before creating in Luxury",
            });
            try {
              await deleteWebflowEcommerceProduct(furnitureConfig.siteId, existingInFurniture.id, furnitureConfig.token);
              webflowLog("info", { event: "sweep.removed_from_furniture", shopifyProductId, webflowId: existingInFurniture.id });
              await sendDuplicatePlacementEmail(
                {
                  productTitle: name,
                  shopifyProductId,
                  previousVertical: "furniture",
                  detectedVertical: "luxury",
                  webflowItemIdRemoved: existingInFurniture.id,
                  sweep: true,
                },
                duplicateEmailSentFor
              );
            } catch (err) {
              webflowLog("error", { event: "sweep.remove_furniture_failed", shopifyProductId, webflowId: existingInFurniture.id, message: err.message });
            }
          }
        }
      }
    }

    const createConfig = getWebflowConfig(detectedVertical);
    const productFieldData = buildWebflowFieldData({
      vertical: detectedVertical,
      name,
      brand,
      price,
      description,
      shopifyProductId,
      shopifyUrl,
      shopifySlug: slug,
      productType: product.product_type,
      category,
      featuredImage,
      gallery,
      showOnWebflow,
      soldNow,
      dimensions,
      dimensionsStatus,
      existingSlug: null,
      newSlug: slug,
      existingFieldData: null,
    });

    let newId;

    if (detectedVertical === "furniture" && createConfig.siteId) {
      const priceCents = price != null && price !== "" ? Math.round(parseFloat(price) * 100) : null;
      const dims = dimensions || getDimensionsFromProduct(product);
      const skuFieldData = {
        name: name ? `${name} - Default` : "Default SKU",
        slug: slug ? `${slug}-default-sku` : `sku-${shopifyProductId}`,
        price: priceCents != null ? { value: priceCents, unit: "USD" } : null,
        ...skuDimensionFields(dims),
        "main-image": featuredImage ? { url: featuredImage } : null,
        "more-images": (gallery || []).slice(0, 10).map((url) => (url ? { url } : null)).filter(Boolean),
      };
      webflowLog("info", { event: "create.ecommerce.start", shopifyProductId, productTitle: name, siteId: createConfig.siteId });
      try {
        const createResp = await createWebflowEcommerceProduct(
          createConfig.siteId,
          productFieldData,
          skuFieldData,
          createConfig.token
        );
        newId = createResp.product?.id;
        if (!newId) throw new Error("No product.id in ecommerce create response");
        webflowLog("info", { event: "create.ecommerce.ok", shopifyProductId, productTitle: name, webflowId: newId });
      } catch (createErr) {
        webflowLog("error", {
          event: "create.ecommerce.failed",
          shopifyProductId,
          productTitle: name,
          message: createErr.message,
          status: createErr.response?.status,
          responseBody: createErr.response?.data,
        });
        if (isWebflowDuplicateSlugError(createErr)) {
          const existingBySlug = await findExistingWebflowEcommerceProduct(
            shopifyProductId,
            slug,
            createConfig,
            name
          );
          if (existingBySlug?.id) {
            newId = existingBySlug.id;
            webflowLog("warn", {
              event: "create.ecommerce.slug_collision_recovered",
              shopifyProductId,
              productTitle: name,
              slug,
              recoveredWebflowId: newId,
              message:
                "Create hit duplicate slug; recovered by linking to existing ecommerce product",
            });
          } else {
            throw createErr;
          }
        } else {
          throw createErr;
        }
      }
    } else {
      webflowLog("info", { event: "create.cms.start", shopifyProductId, productTitle: name, collectionId: createConfig.collectionId });
      let resp;
      try {
        resp = await axios.post(
          `https://api.webflow.com/v2/collections/${createConfig.collectionId}/items`,
          { fieldData: productFieldData },
          {
            headers: {
              Authorization: `Bearer ${createConfig.token}`,
              "Content-Type": "application/json",
            },
          }
        );
      } catch (createErr) {
        webflowLog("error", {
          event: "create.cms.failed",
          shopifyProductId,
          productTitle: name,
          message: createErr.message,
          status: createErr.response?.status,
          responseBody: createErr.response?.data,
        });
        if (isWebflowDuplicateSlugError(createErr)) {
          const existingBySlug = await findExistingWebflowItem(
            shopifyProductId,
            shopifyUrl,
            slug,
            createConfig
          );
          if (existingBySlug?.id) {
            newId = existingBySlug.id;
            webflowLog("warn", {
              event: "create.cms.slug_collision_recovered",
              shopifyProductId,
              productTitle: name,
              slug,
              recoveredWebflowId: newId,
              message:
                "Create hit duplicate slug; recovered by linking to existing CMS item",
            });
          } else {
            throw createErr;
          }
        } else {
          throw createErr;
        }
      }
      if (!newId) newId = resp?.data?.id;
      webflowLog("info", { event: "create.cms.ok", shopifyProductId, productTitle: name, webflowId: newId });
    }
    cache[shopifyProductId] = {
      hash: currentHash,
      contentHash: currentContentHash,
      webflowId: newId,
      lastQty: qty,
      vertical: detectedVertical,
      ...soldMarkedAtPayload(cacheEntry, qty),
    };
    webflowLog("info", { event: "cache.mutated", shopifyProductId, op: "create", webflowId: newId, vertical: detectedVertical });
    return { operation: "create", id: newId };
  }

  // Should not reach: existing was non-null but we didn't update/skip/sold
  return { operation: "skip", id: null };
}

async function syncSingleProduct(product, cache, options = {}) {
  const id = String(product?.id ?? "").trim();
  if (!id) return syncSingleProductCore(product, cache, options);
  return runSerializedByShopifyProductId(id, () => syncSingleProductCore(product, cache, options));
}

/**
 * One-product sync with the same indexes + cache as sync-all (webhooks: create/update).
 * ACK HTTP before calling; this can take tens of seconds (LLM, Webflow).
 */
async function runWebhookSingleProductSync(shopifyProductId, triggerPath) {
  const id = String(shopifyProductId ?? "").trim();
  if (!id) return;
  syncRequestId = crypto.randomUUID().slice(0, 8);
  syncStartTime = Date.now();
  try {
    await loadFurnitureCategoryMap();
    luxuryItemIndex = null;
    furnitureProductIndex = null;
    furnitureSkuIndex = null;
    await Promise.all([
      loadLuxuryItemIndex(),
      loadFurnitureProductIndex(),
      loadFurnitureSkuIndex(),
    ]);
    const product = await fetchShopifyProductById(id);
    if (!product) {
      webflowLog("warn", {
        event: "shopify.webhook.sync_skip",
        path: triggerPath,
        shopifyProductId: id,
        reason: "product_not_found_or_fetch_failed",
      });
      return;
    }
    const cache = loadCache();
    const duplicateEmailSentFor = new Set();
    const shopifyWriteEmailSentFor = new Set();
    const result = await syncSingleProduct(product, cache, { duplicateEmailSentFor, shopifyWriteEmailSentFor });
    saveCache(cache);
    if (result?.duplicateCorrected && result?.duplicateLog) {
      await sendDuplicatePlacementEmail(result.duplicateLog, duplicateEmailSentFor);
    }
    webflowLog("info", {
      event: "shopify.webhook.sync_done",
      path: triggerPath,
      shopifyProductId: id,
      operation: result?.operation ?? null,
      webflowId: result?.id ?? null,
      ...(result?.duplicateCorrected && { duplicateCorrected: true }),
    });
  } catch (err) {
    webflowLog("error", {
      event: "shopify.webhook.sync_error",
      path: triggerPath,
      shopifyProductId: id,
      message: err.message,
    });
  } finally {
    syncRequestId = null;
    syncStartTime = null;
    luxuryItemIndex = null;
    furnitureProductIndex = null;
    furnitureSkuIndex = null;
  }
}

/* ======================================================
   COMPARE FIELD DATA — skip PATCH when nothing actually changed
   Avoids touching Webflow (no Modified bump, no unpublished changes).
====================================================== */
function fieldDataEffectivelyEqual(newFD, existingFD) {
  if (!newFD || typeof newFD !== "object") return !existingFD;
  if (!existingFD || typeof existingFD !== "object") return false;
  const strNorm = (v) => (v == null ? "" : String(v).replace(/\s+/g, " ").trim());
  const priceNorm = (v) => {
    if (v == null) return null;
    const num = parseFloat(String(v).replace(/[^0-9.-]/g, ""));
    return Number.isNaN(num) ? String(v) : num;
  };
  for (const key of Object.keys(newFD)) {
    let n = newFD[key];
    let e = existingFD[key];
    if (n === e) continue;
    if (n == null && e == null) continue;
    if (typeof n === "object" && n !== null && typeof e === "object" && e !== null) {
      if (n.url != null && e.url != null && n.url === e.url) continue;
      if (JSON.stringify(n) === JSON.stringify(e)) continue;
    }
    if (key === "description" || key === "body_html") {
      if (strNorm(n) === strNorm(e)) continue;
    }
    if (key === "price" && priceNorm(n) === priceNorm(e)) continue;
    if (["name", "brand", "slug", "shopify-product-id", "shopify-url"].includes(key) && strNorm(n) === strNorm(e)) continue;
    if (key === "category") {
      const nRef = typeof n === "string" && WEBFLOW_ITEM_REF_REGEX.test(n);
      const eRef = typeof e === "string" && WEBFLOW_ITEM_REF_REGEX.test(e);
      if (nRef && eRef && n === e) continue;
      if ((nRef && typeof e === "string" && e.trim()) || (eRef && typeof n === "string" && n.trim())) continue;
    }
    if (key === getFurnitureSoldSinceFieldSlug()) {
      const tn = coerceWebflowDateTimeToMs(n);
      const te = coerceWebflowDateTimeToMs(e);
      if (tn != null && te != null && Math.abs(tn - te) < 2000) continue;
      if (tn == null && te == null) continue;
    }
    if (String(n) === String(e)) continue;
    return false;
  }
  return true;
}

/* ======================================================
   BUILD WEBFLOW fieldData BY VERTICAL
   Luxury: featured-image, image-1..5, show-on-webflow, brand, price, shopify-url.
   Furniture: slugs match CMS — name, slug, description, category, sold,
   shopify-product-id, shopify-slug-2, main-description-2, ec-product-type, shippable.
   (Images live on SKU; no dimension fields on this collection.)
====================================================== */
function buildWebflowFieldData(opts) {
  const {
    vertical,
    name,
    brand,
    price,
    description,
    shopifyProductId,
    shopifyUrl,
    shopifySlug,
    productType,
    category,
    featuredImage,
    gallery,
    showOnWebflow,
    soldNow,
    dimensions,
    dimensionsStatus,
    existingSlug,
    newSlug,
    existingFieldData,
  } = opts;

  const slug = existingSlug ?? newSlug ?? "";

  if (vertical === "furniture") {
    // Ecommerce product category must be an ItemRef (collection item ID), never a display string.
    const categoryRef = resolveFurnitureCategoryRef(category);
    if (categoryRef == null) {
      const envKey = `FURNITURE_CATEGORY_${category.replace(/\s*\/\s*/g, "_").replace(/\s+/g, "_").toUpperCase().replace(/[^A-Z0-9_]/g, "")}`;
      webflowLog("warn", { event: "build_field_data.category_unassigned", category, envKey, message: `Set ${envKey} to Webflow category item ID` });
    }
    const out = {
      name,
      slug,
      description: description ?? "",
      sold: !!soldNow,
      "shopify-product-id": shopifyProductId,
      "shopify-slug-2": shopifySlug ?? newSlug ?? "",
      "main-description-2": description ?? null,
      "ec-product-type": productType ?? null,
      shippable: true,
    };
    if (categoryRef != null && WEBFLOW_ITEM_REF_REGEX.test(String(categoryRef))) out.category = categoryRef;
    const soldDateSlug = getFurnitureSoldSinceFieldSlug();
    const ex = existingFieldData && typeof existingFieldData === "object" ? existingFieldData : null;
    if (soldDateSlug) {
      if (soldNow) {
        const wasSold = webflowListingLooksSold({ fieldData: ex || {} }, "furniture");
        const missingDate = parseSoldTimestampMsFromWebflowField(ex || {}, soldDateSlug) == null;
        if (!wasSold || missingDate) {
          out[soldDateSlug] = getBusinessDateSoldString();
        }
      } else if (ex != null && parseSoldTimestampMsFromWebflowField(ex, soldDateSlug) != null) {
        out[soldDateSlug] = null;
      }
    }
    return out;
  }

  // Webflow rejects "Other " (trailing space); use "Other" for Luxury CMS.
  // Only allow known luxury taxonomy values; everything else (including furniture-only categories like "Living Room") becomes "Other ".
  const isLuxuryCategory = category && LUXURY_TAXONOMY.includes(category);
  const luxuryCategory = isLuxuryCategory ? category : "Other ";
  const webflowCategory = (luxuryCategory && luxuryCategory.trimEnd() === "Other") ? "Other" : (luxuryCategory ?? "");
  const base = {
    name,
    brand,
    price,
    description,
    "shopify-product-id": shopifyProductId,
    "shopify-url": shopifyUrl,
    category: webflowCategory,
    slug,
  };
  return {
    ...base,
    "featured-image": featuredImage ? { url: featuredImage } : null,
    "image-1": gallery?.[0] ? { url: gallery[0] } : null,
    "image-2": gallery?.[1] ? { url: gallery[1] } : null,
    "image-3": gallery?.[2] ? { url: gallery[2] } : null,
    "image-4": gallery?.[3] ? { url: gallery[3] } : null,
    "image-5": gallery?.[4] ? { url: gallery[4] } : null,
    "show-on-webflow": showOnWebflow,
  };
}

/* ======================================================
   FACEBOOK MARKETPLACE — GET /api/listing?name=...
   Default: Webflow (Luxury CMS + Furniture ecommerce). Env: WEBFLOW_* / RESALE_*.
   Luxury productUrl: handbags storefront — https://www.lostandfoundhandbags.com/shop/{item-slug}
     (data still from luxury Webflow CMS; public shop is separate domain). Override: LISTING_LUXURY_URL_PREFIX.
     JSON includes vertical: "luxury" | "furniture" | "shopify" for clients (e.g. Chrome footer).
   Furniture productUrl: LISTING_PRODUCT_URL_PREFIX + /{slug} (default …lostandfoundresale.com/product).
   Optional: ?source=shopify — Shopify Admin GraphQL (SHOPIFY_STORE + SHOPIFY_ACCESS_TOKEN).
   POST /api/listing-blurb — Facebook-friendly body via OpenAI (OPENAI_API_KEY; optional OPENAI_LISTING_MODEL=gpt-4o-mini).
====================================================== */
function stripListingDescriptionHtml(html) {
  if (!html || typeof html !== "string") return "";
  return html.replace(/<[^>]*>/g, " ").replace(/\s+/g, " ").trim();
}

function shopifyAdminGraphqlListingUrl() {
  const raw = (process.env.SHOPIFY_STORE || "")
    .trim()
    .replace(/^https?:\/\//, "")
    .replace(/\/$/, "");
  if (!raw) return null;
  const host = raw.includes(".") ? raw : `${raw}.myshopify.com`;
  return `https://${host}/admin/api/2024-01/graphql.json`;
}

/** Loosen punctuation so "Table-79X39X43H" and "Table 79 x 39" score the same for listing match. */
function normalizeProductTitleForLooseMatch(name) {
  let s = String(name ?? "")
    .trim()
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[–—]/g, "-");
  s = s.replace(/[-_/]+/g, " ");
  s = s.replace(/\s+/g, " ").trim();
  return s;
}

/** Try several Admin search strings; long titles with & and hyphenated dims often miss on the first query. */
function shopifyAdminListingSearchVariants(raw) {
  const base = String(raw || "").trim();
  if (!base) return [];
  const uniq = [];
  const add = (s) => {
    const t = String(s || "").trim();
    if (t && !uniq.includes(t)) uniq.push(t);
  };
  add(base);
  add(base.replace(/\s*&\s*/g, " and "));
  add(base.replace(/\s*&\s*/g, " "));
  add(base.replace(/\s*-\s*as\s+is\b\s*$/i, "").trim());
  add(base.replace(/\s+as\s+is\s*$/i, "").trim());
  add(base.replace(/\s*-\s*2\s+leaves\b/i, " ").replace(/\s+/g, " ").trim());
  add(
    base
      .replace(/\s*-\s*\d{1,3}\s*x\s*\d{1,3}(\s*x\s*\d{1,3})?\s*h?\b/i, " ")
      .replace(/\s+/g, " ")
      .trim()
  );
  const words = base.split(/\s+/).filter(Boolean);
  if (words.length >= 8) add(words.slice(0, 8).join(" "));
  if (words.length >= 6) add(words.slice(0, 6).join(" "));
  if (words.length >= 4) add(words.slice(0, 4).join(" "));
  if (words.length >= 2) add(words.slice(0, 2).join(" "));
  return uniq;
}

/**
 * @param {string} name Search string for Shopify `products(query: ...)`
 * @returns {Promise<{ title: string, price: string, description: string, images: string[], vendor: string | null, handle: string | null, productUrl: string | null, shopifyOnlineUrl: string | null, vertical: string } | null>}
 */
async function searchShopifyProducts(name) {
  const token = (process.env.SHOPIFY_ACCESS_TOKEN || "").trim();
  const url = shopifyAdminGraphqlListingUrl();
  if (!url || !token) {
    throw new Error("Missing SHOPIFY_STORE or SHOPIFY_ACCESS_TOKEN");
  }

  const query = `
    query ProductSearch($q: String!) {
      products(first: 15, query: $q) {
        edges {
          node {
            title
            handle
            vendor
            onlineStoreUrl
            descriptionHtml
            images(first: 5) {
              edges {
                node {
                  url
                }
              }
            }
            variants(first: 1) {
              edges {
                node {
                  price
                }
              }
            }
          }
        }
      }
    }
  `;

  const rawName = String(name || "").trim();
  const variants = shopifyAdminListingSearchVariants(rawName);
  let edges = [];
  for (const qTry of variants) {
    const res = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": token,
      },
      body: JSON.stringify({
        query,
        variables: { q: qTry },
      }),
    });

    if (!res.ok) {
      const text = await res.text();
      throw new Error(`Shopify HTTP ${res.status}: ${text.slice(0, 300)}`);
    }

    const json = await res.json();
    if (json.errors?.length) {
      throw new Error(json.errors.map((e) => e.message).join("; "));
    }

    const batch = json.data?.products?.edges || [];
    if (batch.length) {
      edges = batch;
      break;
    }
  }

  if (!edges.length) return null;

  let bestNode = null;
  let bestScore = -1;
  for (const edge of edges) {
    const cand = edge?.node;
    if (!cand) continue;
    const title = String(cand.title || "").trim();
    const score = listingTitleSearchScore(rawName, title);
    if (score <= 0) continue;
    const prevTitle = bestNode ? String(bestNode.title || "").trim() : "";
    if (score > bestScore || (score === bestScore && listingSearchTiePrefer(rawName, title, prevTitle))) {
      bestScore = score;
      bestNode = cand;
    }
  }
  if (!bestNode && edges.length === 1) {
    bestNode = edges[0]?.node || null;
  }
  if (!bestNode) return null;

  const node = bestNode;
  const images = (node.images?.edges || [])
    .map((e) => e?.node?.url)
    .filter(Boolean);

  const priceRaw = node.variants?.edges?.[0]?.node?.price;
  let price = "";
  if (priceRaw != null) {
    if (typeof priceRaw === "object" && priceRaw.amount != null) {
      price = String(priceRaw.amount);
    } else {
      price = String(priceRaw);
    }
  }

  const handle = (node.handle || "").trim();
  const prefix = (process.env.LISTING_PRODUCT_URL_PREFIX || "https://www.lostandfoundresale.com/product")
    .trim()
    .replace(/\/$/, "");
  let shopifyOnlineUrl = typeof node.onlineStoreUrl === "string" ? node.onlineStoreUrl.trim() : "";
  let productUrl = "";
  if (shopifyOnlineUrl && /lostandfoundresale\.com/i.test(shopifyOnlineUrl)) {
    productUrl = shopifyOnlineUrl;
  } else if (handle) {
    productUrl = `${prefix}/${handle}`;
  }

  const vendor = String(node.vendor || "").trim();
  return {
    title: node.title || "",
    price,
    description: stripListingDescriptionHtml(node.descriptionHtml || ""),
    images,
    handle: handle || null,
    productUrl: productUrl || null,
    shopifyOnlineUrl: shopifyOnlineUrl || null,
    vertical: "shopify",
    vendor: vendor || null,
  };
}

/** Image field from Webflow CMS or ecommerce SKU: `{ url }` or raw URL string. */
function webflowListingAssetUrl(field) {
  if (field == null) return null;
  if (typeof field === "string") {
    const t = field.trim();
    return /^https?:\/\//i.test(t) ? t : null;
  }
  if (typeof field === "object" && field.url) {
    const t = String(field.url).trim();
    return t || null;
  }
  return null;
}

function formatLuxuryListingPrice(val) {
  if (val == null || val === "") return "";
  if (typeof val === "number" && Number.isFinite(val)) return val.toFixed(2);
  const n = parseFloat(String(val).trim().replace(/[^0-9.]/g, ""));
  return Number.isFinite(n) ? n.toFixed(2) : String(val).trim();
}

function formatSkuCentsAsListingPrice(cents) {
  if (cents == null || !Number.isFinite(cents)) return "";
  return (cents / 100).toFixed(2);
}

function listingFurnitureProductUrlFromSlug(slug) {
  const s = String(slug || "").trim();
  if (!s) return null;
  const prefix = (process.env.LISTING_PRODUCT_URL_PREFIX || "https://www.lostandfoundresale.com/product")
    .trim()
    .replace(/\/$/, "");
  return `${prefix}/${s}`;
}

/**
 * Public base URL for luxury/handbag product links (no trailing slash).
 * Matches the live Shopify/handbags storefront, e.g.
 * https://www.lostandfoundhandbags.com/shop/gucci-sherry-line-vintage-brown-leather-portfolio-clutch-as-is-a6066
 * Override with LISTING_LUXURY_URL_PREFIX (no trailing slash), e.g. https://www.lostandfoundhandbags.com/shop
 */
function getLuxuryListingPublicBaseUrl() {
  const fromEnv = (process.env.LISTING_LUXURY_URL_PREFIX || "").trim().replace(/\/$/, "");
  if (fromEnv) return fromEnv;
  return "https://www.lostandfoundhandbags.com/shop";
}

/** When luxury + furniture both match, nudge toward luxury CMS for handbag-like search text. */
function luxuryListingWinnerBoost(rawQuery) {
  const q = normalizeProductNameForIndex(rawQuery);
  if (!q) return 0;
  const hints = [
    "handbag",
    "handbags",
    "clutch",
    "wallet",
    "wallets",
    "tote",
    "totes",
    "crossbody",
    "hobo",
    "satchel",
    "duffel",
    "luggage",
    "backpack",
    "backpacks",
    "gucci",
    "chanel",
    "prada",
    "fendi",
    "dior",
    "ysl",
    "versace",
    "burberry",
    "vuitton",
    "louis",
    "goyard",
    "balenciaga",
    "hermes",
    "celine",
    "bottega",
    "chloe",
    "miumiu",
    "valentino",
    "saint laurent",
    "louboutin",
    "michael kors",
    "kate spade",
  ];
  for (const t of hints) {
    if (q.includes(t.trim())) return 200;
  }
  return 0;
}

/** Query tokens this long must appear as substrings in the candidate title (unless stopword), or score is 0. Prevents shared "Design Within Reach Roll & Hill …" from always picking the first catalog hit (e.g. pendant vs chandelier). Override with LISTING_SEARCH_REQUIRED_TOKEN_LEN (3–8). */
const LISTING_SEARCH_REQUIRED_TOKEN_LEN = (() => {
  const n = parseInt(process.env.LISTING_SEARCH_REQUIRED_TOKEN_LEN || "4", 10);
  if (!Number.isFinite(n)) return 4;
  return Math.min(8, Math.max(3, n));
})();

const LISTING_SEARCH_QUERY_TOKEN_STOP = new Set([
  "with",
  "from",
  "that",
  "this",
  "your",
  "and",
  "for",
  "are",
  "was",
  "were",
  "the",
  "you",
  "our",
  "inch",
  "inches",
  "wide",
  "deep",
  "tall",
  "high",
  "long",
  "each",
  "sale",
]);

function listingQueryTokenRequiresNameMatch(t) {
  if (!t || t.length < LISTING_SEARCH_REQUIRED_TOKEN_LEN) return false;
  if (LISTING_SEARCH_QUERY_TOKEN_STOP.has(t)) return false;
  if (/^\d+$/.test(t)) return false;
  if (/^\d+x\d+$/i.test(t)) return false;
  if (/^\d+x\d+x\d+[a-z0-9]*$/i.test(t)) return false;
  if (/^\d{1,3}x\d{1,3}/i.test(t)) return false;
  return true;
}

/** Longest run of matching characters from the start of q against n (case-normalized strings). */
function listingLongestPrefixCharMatch(q, n) {
  const lim = Math.min(q.length, n.length);
  let k = 0;
  while (k < lim && q[k] === n[k]) k += 1;
  return k;
}

/** True if newName should replace oldName when scores are equal (Webflow scan tie-break). */
function listingSearchTiePrefer(q, newName, oldName) {
  const n0 = normalizeProductNameForIndex(newName);
  const n1 = normalizeProductNameForIndex(oldName);
  const p0 = listingLongestPrefixCharMatch(q, n0);
  const p1 = listingLongestPrefixCharMatch(q, n1);
  if (p0 !== p1) return p0 > p1;
  if (n0.length !== n1.length) return n0.length > n1.length;
  return n0 < n1;
}

/**
 * Score 0 = no match. Higher = better. 1000 = exact normalized title match.
 * Used to pick one listing when searching Webflow luxury CMS + furniture ecommerce.
 */
function listingTitleSearchScore(rawQuery, rawName) {
  const qStrict = normalizeProductNameForIndex(rawQuery);
  const nStrict = normalizeProductNameForIndex(rawName);
  if (!qStrict || !nStrict) return 0;
  if (qStrict === nStrict) return 1000;

  const q = normalizeProductTitleForLooseMatch(rawQuery);
  const n = normalizeProductTitleForLooseMatch(rawName);
  if (!q || !n) return 0;
  if (q === n) return 1000;

  const qTokens = q.split(/\s+/).filter(Boolean);
  const required = qTokens.filter(listingQueryTokenRequiresNameMatch);
  if (required.length > 0) {
    for (const t of required) {
      if (!n.includes(t)) return 0;
    }
  }

  let base;
  if (n.includes(q) || q.includes(n)) {
    base = 850;
    if (n.includes(q) && q.length >= 8) {
      base += Math.min(50, Math.floor(q.length / 5));
    }
  } else {
    const qt = qTokens;
    if (!qt.length) return 0;
    let hits = 0;
    for (const t of qt) {
      if (n.split(" ").includes(t)) hits += 1;
      else if (n.includes(t)) hits += 0.75;
    }
    if (hits <= 0) return 0;
    const ratio = hits / qt.length;
    base = Math.round(200 + hits * 100 + ratio * 400);
  }

  const prefix = listingLongestPrefixCharMatch(q, n);
  const tie = Math.min(55, Math.floor(prefix / 3));
  return Math.min(999, base + tie);
}

function luxuryFieldDataImageUrls(fd) {
  if (!fd || typeof fd !== "object") return [];
  const urls = [];
  const u0 = webflowListingAssetUrl(fd["featured-image"]);
  if (u0) urls.push(u0);
  for (let i = 1; i <= 5; i++) {
    const u = webflowListingAssetUrl(fd[`image-${i}`]);
    if (u) urls.push(u);
  }
  return urls;
}

function furnitureSkuFieldDataImageUrls(skuFd) {
  if (!skuFd || typeof skuFd !== "object") return [];
  const urls = [];
  const main = webflowListingAssetUrl(skuFd["main-image"]);
  if (main) urls.push(main);
  const more = skuFd["more-images"];
  if (Array.isArray(more)) {
    for (const m of more) {
      const u = webflowListingAssetUrl(m);
      if (u) urls.push(u);
    }
  }
  return urls;
}

function shopifyProductHasDisplayImages(product) {
  const imgs = product?.images;
  if (!Array.isArray(imgs) || imgs.length === 0) return false;
  return imgs.some((i) => i?.src && String(i.src).trim());
}

/** Shopify has images but Furniture ecommerce default SKU has no image URLs Webflow can use. */
function furnitureEcommerceNeedsImageRepairFromShopify(product, ecommerceProduct) {
  if (!shopifyProductHasDisplayImages(product)) return false;
  const skuFd = ecommerceProduct?.skus?.[0]?.fieldData;
  return furnitureSkuFieldDataImageUrls(skuFd).length === 0;
}

/** Shopify has images but Luxury CMS item has no featured / gallery image URLs. */
function luxuryCmsNeedsImageRepairFromShopify(product, cmsItem) {
  if (!shopifyProductHasDisplayImages(product)) return false;
  return luxuryFieldDataImageUrls(cmsItem?.fieldData).length === 0;
}

/** PATCH luxury CMS item: (re)push featured-image + image-1..5 from Shopify only where URLs exist. */
async function patchLuxuryCmsImagesFromShopify(product, existing, config) {
  if (!config?.collectionId || !config?.token || !existing?.id) return;
  const allImages = (product.images || []).map((img) => img?.src).filter((u) => u && String(u).trim());
  if (!allImages.length) return;
  const fd = { ...(existing.fieldData || {}) };
  fd["featured-image"] = { url: allImages[0] };
  for (let slot = 1; slot <= 5; slot++) {
    const src = allImages[slot];
    if (src) fd[`image-${slot}`] = { url: src };
  }
  const url = `https://api.webflow.com/v2/collections/${config.collectionId}/items/${existing.id}`;
  await axios.patch(
    url,
    { fieldData: fd },
    { headers: { Authorization: `Bearer ${config.token}`, "Content-Type": "application/json" } }
  );
}

function getListingSearchMinScore() {
  const n = parseInt(process.env.LISTING_SEARCH_MIN_SCORE || "350", 10);
  return Number.isFinite(n) && n > 0 ? n : 350;
}

async function scanLuxuryCmsForListingSearch(query) {
  const config = getWebflowConfig("luxury");
  if (!config.collectionId || !config.token) return null;
  let best = null;
  let offset = 0;
  const limit = 100;
  while (true) {
    const url = `https://api.webflow.com/v2/collections/${config.collectionId}/items?limit=${limit}&offset=${offset}`;
    const resp = await axios.get(url, {
      headers: { Authorization: `Bearer ${config.token}`, accept: "application/json" },
    });
    const items = resp.data?.items ?? [];
    for (const item of items) {
      if (item.isArchived === true) continue;
      const fd = item.fieldData || {};
      if (webflowListingLooksSold({ fieldData: fd }, "luxury")) continue;
      const name = fd.name ?? "";
      const score = listingTitleSearchScore(query, name);
      if (score <= 0) continue;
      if (
        !best ||
        score > best.score ||
        (score === best.score && listingSearchTiePrefer(query, name, best.name || ""))
      ) {
        best = { score, fd, name, item };
      }
      if (score >= 1000) return best;
    }
    if (items.length < limit) break;
    offset += limit;
  }
  return best;
}

async function scanFurnitureEcommerceForListingSearch(query) {
  const config = getWebflowConfig("furniture");
  if (!config.siteId || !config.token) return null;
  let best = null;
  let offset = 0;
  const limit = 100;
  while (true) {
    const url = `https://api.webflow.com/v2/sites/${config.siteId}/products?limit=${limit}&offset=${offset}`;
    const resp = await axios.get(url, {
      headers: { Authorization: `Bearer ${config.token}`, accept: "application/json" },
    });
    const raw = resp.data?.products ?? resp.data?.items ?? [];
    const list = Array.isArray(raw) ? raw : [];
    for (const listItem of list) {
      const product = listItem.product ?? listItem;
      if (product.isArchived === true) continue;
      const fd = product.fieldData || {};
      const skus = listItem.skus ?? product.skus ?? [];
      const merged = { ...product, fieldData: fd, skus };
      if (webflowListingLooksSold(merged, "furniture")) continue;
      const name = fd.name ?? product.name ?? "";
      const score = listingTitleSearchScore(query, name);
      if (score <= 0) continue;
      if (
        !best ||
        score > best.score ||
        (score === best.score && listingSearchTiePrefer(query, name, best.name || ""))
      ) {
        best = { score, product, fd, skus, name };
      }
      if (score >= 1000) return best;
    }
    if (list.length < limit) break;
    offset += limit;
  }
  return best;
}

function mapLuxuryListingSearchHit(hit) {
  const fd = hit.fd;
  const itemFd = hit.item?.fieldData && typeof hit.item.fieldData === "object" ? hit.item.fieldData : {};
  const slug = String(
    itemFd.slug || fd.slug || hit.item?.slug || fd["shopify-slug-2"] || ""
  ).trim();
  const title = String(fd.name || "").trim();
  const price = formatLuxuryListingPrice(fd.price);
  const description = stripListingDescriptionHtml(String(fd.description || ""));
  const images = luxuryFieldDataImageUrls(fd);
  const base = getLuxuryListingPublicBaseUrl();
  const productUrl = slug ? `${base}/${slug}` : null;
  const vendor = String(fd.brand || itemFd.brand || "").trim();
  return {
    title,
    price,
    description,
    images,
    handle: slug || null,
    productUrl,
    shopifyOnlineUrl: null,
    vertical: "luxury",
    vendor: vendor || null,
  };
}

async function mapFurnitureListingSearchHit(hit, config) {
  const { product, fd } = hit;
  let skus = hit.skus ?? [];
  let skuFd = skus[0]?.fieldData;
  let images = furnitureSkuFieldDataImageUrls(skuFd);
  if (images.length === 0 && product?.id) {
    const full = await getWebflowEcommerceProductById(config.siteId, product.id, config.token);
    if (full) {
      skus = full.skus ?? [];
      skuFd = skus[0]?.fieldData;
      images = furnitureSkuFieldDataImageUrls(skuFd);
    }
  }
  const slug = String(fd.slug || fd["shopify-slug-2"] || "").trim();
  const title = String(fd.name || product.name || "").trim();
  const description = stripListingDescriptionHtml(String(fd["main-description-2"] || fd.description || ""));
  const cents = webflowSkuMoneyFieldToCents(skuFd?.price);
  const price = formatSkuCentsAsListingPrice(cents);
  const vendor = String(fd.brand || "").trim();
  return {
    title,
    price,
    description,
    images,
    handle: slug || null,
    productUrl: listingFurnitureProductUrlFromSlug(slug),
    shopifyOnlineUrl: null,
    vertical: "furniture",
    vendor: vendor || null,
  };
}

/**
 * Best-effort title search across Luxury CMS + Furniture ecommerce (same JSON shape as Shopify listing).
 * @param {string} name
 */
async function searchWebflowListing(name) {
  const q = String(name || "").trim();
  if (!q) return null;
  const luxCfg = getWebflowConfig("luxury");
  const furnCfg = getWebflowConfig("furniture");
  const canLux = !!(luxCfg.collectionId && luxCfg.token);
  const canFurn = !!(furnCfg.siteId && furnCfg.token);
  if (!canLux && !canFurn) {
    throw new Error(
      "Webflow listing search not configured (set WEBFLOW_COLLECTION_ID + WEBFLOW_TOKEN and/or RESALE_WEBFLOW_SITE_ID + RESALE_TOKEN)"
    );
  }
  const minScore = getListingSearchMinScore();
  const [luxRes, furnRes] = await Promise.allSettled([
    canLux ? scanLuxuryCmsForListingSearch(q) : Promise.resolve(null),
    canFurn ? scanFurnitureEcommerceForListingSearch(q) : Promise.resolve(null),
  ]);
  const luxHit = luxRes.status === "fulfilled" ? luxRes.value : null;
  const furnHit = furnRes.status === "fulfilled" ? furnRes.value : null;
  if (luxRes.status === "rejected") {
    webflowLog("warn", {
      event: "listing_search.webflow_luxury_failed",
      query: q.slice(0, 120),
      message: luxRes.reason?.message || String(luxRes.reason || "unknown"),
    });
  }
  if (furnRes.status === "rejected") {
    webflowLog("warn", {
      event: "listing_search.webflow_furniture_failed",
      query: q.slice(0, 120),
      message: furnRes.reason?.message || String(furnRes.reason || "unknown"),
    });
  }
  if (luxRes.status === "rejected" && furnRes.status === "rejected") {
    throw new Error("Webflow listing search failed (luxury + furniture endpoints unavailable)");
  }
  const luxOk = luxHit && luxHit.score >= minScore;
  const furnOk = furnHit && furnHit.score >= minScore;
  if (!luxOk && !furnOk) return null;
  const luxAdj = luxOk ? luxHit.score + luxuryListingWinnerBoost(q) : 0;
  if (luxOk && furnOk) {
    if (furnHit.score > luxAdj) return await mapFurnitureListingSearchHit(furnHit, furnCfg);
    if (luxAdj > furnHit.score) return mapLuxuryListingSearchHit(luxHit);
    return mapLuxuryListingSearchHit(luxHit);
  }
  if (luxOk) return mapLuxuryListingSearchHit(luxHit);
  return await mapFurnitureListingSearchHit(furnHit, furnCfg);
}

/* ======================================================
   ROUTES
====================================================== */
app.get("/", (req, res) => {
  res.send(
    "Lost & Found — Clean Sync Server (No Duplicates, Sold Logic Fixed, Deep Scan Matcher + Logging)"
  );
});

app.get("/api/listing", async (req, res) => {
  const name = req.query.name;
  if (name === undefined || String(name).trim() === "") {
    return res.status(400).json({ error: "Missing required query param: name" });
  }
  const source = String(req.query.source || "webflow").trim().toLowerCase();
  try {
    let listing = null;
    if (source === "shopify") {
      listing = await searchShopifyProducts(String(name));
      if (!listing) {
        listing = await searchWebflowListing(String(name));
      }
    } else {
      try {
        listing = await searchWebflowListing(String(name));
      } catch (webflowErr) {
        webflowLog("warn", {
          event: "api.listing.webflow_fallback_shopify",
          message: webflowErr?.message || "webflow listing search failed",
        });
      }
      if (!listing) {
        listing = await searchShopifyProducts(String(name));
      }
    }
    if (!listing) {
      return res.status(404).json({ error: "No products found" });
    }
    return res.json({
      title: listing.title,
      price: listing.price,
      description: listing.description,
      images: listing.images,
      handle: listing.handle,
      productUrl: listing.productUrl,
      shopifyOnlineUrl: listing.shopifyOnlineUrl,
      vertical: listing.vertical,
      vendor: listing.vendor != null && String(listing.vendor).trim() !== "" ? String(listing.vendor).trim() : null,
    });
  } catch (err) {
    webflowLog("error", { event: "api.listing", message: err?.message, source });
    return res.status(500).json({
      error: err?.message || (source === "shopify" ? "Shopify request failed" : "Webflow request failed"),
    });
  }
});

/**
 * POST /api/listing-blurb — Short listing copy via OpenAI (OPENAI_API_KEY).
 * Body JSON: title, price, vertical, productDescription, pickupAddress, pickupHours, contactEmail,
 *   isLuxury (bool, optional), outputChannel (optional: "facebook" default | "craigslist").
 * Returns { text } plain text (no URLs for Facebook; Craigslist is standalone body).
 * Model: OPENAI_LISTING_MODEL (default gpt-4o-mini).
 */
app.post("/api/listing-blurb", async (req, res) => {
  const key = (process.env.OPENAI_API_KEY || "").trim();
  if (!key) {
    return res.status(503).json({
      error: "OPENAI_API_KEY is not set on this server",
      code: "openai_missing",
    });
  }

  const body = req.body && typeof req.body === "object" ? req.body : {};
  const title = String(body.title || "").trim();
  const price = body.price != null ? String(body.price).trim() : "";
  const vertical = String(body.vertical || "furniture").trim().toLowerCase();
  const catalog = String(body.productDescription || "").trim().slice(0, 2200);
  const pickupAddress = String(body.pickupAddress || "").trim();
  const pickupHours = String(body.pickupHours || "").trim();
  const contactEmail = String(body.contactEmail || "info@lostandfoundresale.com").trim();
  const isLuxury = body.isLuxury === true || vertical === "luxury";
  const outputChannel = String(body.outputChannel || "facebook").trim().toLowerCase();
  const isCraigslist = outputChannel === "craigslist";

  if (!title && !catalog) {
    return res.status(400).json({ error: "Provide at least title or productDescription" });
  }

  const model = (process.env.OPENAI_LISTING_MODEL || "gpt-4o-mini").trim();
  const variationHint = Math.random().toString(36).slice(2, 11);
  const openingStyles = [
    "direct seller opener",
    "friendly conversational opener",
    "feature-first opener",
    "condition-first opener",
  ];
  const flowStyles = [
    "two short paragraphs",
    "single flowing paragraph",
    "short sentence chain with natural pauses",
    "statement then detail then logistics",
  ];
  const closingStyles = [
    "link-first close",
    "showroom-invite close",
    "questions-then-link close",
    "checkout-and-details close",
  ];
  const pick = (arr) => arr[Math.floor(Math.random() * arr.length)];
  const variationProfile = {
    openingStyle: pick(openingStyles),
    flowStyle: pick(flowStyles),
    closingStyle: pick(closingStyles),
  };
  const audienceText = `${title} ${catalog}`.toLowerCase();
  const hasAny = (terms = []) => terms.some((t) => audienceText.includes(t));
  const heavyWeightMatch = audienceText.match(/(\d{2,4}(?:\.\d+)?)\s*(?:lb|lbs|pounds?)\b/i);
  const heavyByWeight = heavyWeightMatch ? Number(heavyWeightMatch[1]) > 50 : false;
  const largeByKeywords = hasAny([
    "oversized",
    "over sized",
    "large",
    "extra large",
    "xl",
    "sectional",
    "armoire",
    "china cabinet",
    "king size",
    "king-size",
    "dining table",
    "72 in",
    "84 in",
    "96 in",
    "7 ft",
    "8 ft",
  ]);
  const moverAssistEligible = heavyByWeight || largeByKeywords;
  const moverAssistLine =
    "The movers we use charge $95/hr and are a great group, and we can help set delivery up for you.";
  let audienceLane = "general_local";
  if (
    hasAny([
      "vintage",
      "mid century",
      "mid-century",
      "antique",
      "rare",
      "signed",
      "artist",
      "original",
      "collectible",
      "limited edition",
      "estate",
      "museum",
    ])
  ) {
    audienceLane = "collector";
  } else if (
    hasAny([
      "project",
      "restore",
      "restoration",
      "as is",
      "as-is",
      "repair",
      "needs work",
      "flipper",
      "reseller",
      "deal",
      "priced to move",
      "clearance",
    ])
  ) {
    audienceLane = "deal_hunter";
  } else if (
    hasAny([
      "dining",
      "sofa",
      "sectional",
      "bedroom",
      "dresser",
      "nightstand",
      "coffee table",
      "entryway",
      "home office",
      "patio",
      "kitchen",
      "family room",
    ])
  ) {
    audienceLane = "home_practical";
  }
  const audienceGuidance = {
    collector:
      "Subtle collector-aware language: mention craftsmanship, provenance-style cues, or era only when explicitly supported by title/catalog. Keep it grounded and concise.",
    deal_hunter:
      "Subtle value-oriented language: straightforward condition and usefulness, with practical tone for bargain-minded or project buyers. No hype.",
    home_practical:
      "Subtle practical-home language: focus on fit/function in real spaces, comfort, and everyday use when supported by facts.",
    general_local:
      "Neutral local resale language: plain and direct, balanced for mixed buyers.",
  };
  const retailToneBlock =
    " Avoid catalog or showroom tone: no introducing, curated, elevate your space, showcase, stunning, visit our website, SKU dumps, pasted spec tables, or pasted legal blocks.";

  let structureGuide;
  let toneGuide;
  let avoidPhraseGuide;
  let logisticsGuide;
  let maxBodyChars;

  if (isCraigslist) {
    maxBodyChars = 1400;
    const clAvoidStorePitch =
      "Do NOT use or echo: consignment (as a store label), Discover, stunning, gorgeous, masterpiece, don't miss out, perfect for anyone, beautifully balances, elevate your space, timeless appeal, artisanal flair, captures the essence, anyone looking to add, yours for just, act fast, limited opportunity, shop with confidence.";
    structureGuide = isLuxury
      ? "Write a natural Craigslist for-sale body from title + catalogDescription. Sound like a real person: a few short paragraphs or flowing sentences, not a catalog paste. Keep concrete details only when supported (materials, wear, hardware, size). Never use explicit brand or trademark names. If AS IS appears in the source, describe condition plainly. Weave in one plain pickup sentence that also mentions shipping options are available and full details are on the link below: you can pick it up right by Scottsdale Quarter at Lost and Found Resale Interiors (wayfinding, not a sales pitch). Close by steering questions, purchase, and contact through the store link at the bottom of this posting (do not type URLs in your body), or say they are welcome to come in. Do not ask people to reply here on Craigslist to coordinate. No URLs or phone numbers in your body. No markdown bullets or numbered lists. No em dash."
      : "Write a natural Craigslist for-sale body from title + catalogDescription. Casual and plain, like a local seller: what it is, honest condition, size or material only if stated, one plain pickup sentence that also notes shipping options are available and details are on the link below (pickup right by Scottsdale Quarter at Lost and Found Resale Interiors). End by pointing them to the link at the bottom of this post for item details and to contact or buy through the site, or invite them to stop in. Do not ask people to reply here on Craigslist to coordinate. Strip retail or catalog voice down to human language. No URLs or phone numbers in your body. No markdown bullets or numbered lists. No em dash.";
    toneGuide = isLuxury
      ? "Collector-aware Craigslist voice: knowledgeable but still local and grounded. Avoid posh showroom tone. Sound like someone talking to collectors, resellers, and value-minded buyers."
      : "Friendly, plain Craigslist seller for mixed local buyers: collectors, yard-sale/value shoppers, and practical home buyers. Not corporate, not showroom.";
    avoidPhraseGuide = isLuxury
      ? `${clAvoidStorePitch} Do not use explicit brand/trademark names from title or catalogDescription in output. Do not use em-dash punctuation (Unicode U+2014) or en-dash as a clause dash (U+2013); use commas, periods, or 'and'. Do not say: reply here, reply on Craigslist, coordinate through Craigslist, message me here to schedule, or similar.`
      : `${clAvoidStorePitch} Do not use em-dash punctuation (Unicode U+2014) or en-dash as a clause dash (U+2013); use commas, periods, or 'and'. Do not say: reply here, reply on Craigslist, coordinate through Craigslist, message me here to schedule, or similar.`;
    avoidPhraseGuide += retailToneBlock;
    logisticsGuide =
      "Keep logistics human and short. Include exactly one pickup line that names Scottsdale Quarter and Lost and Found Resale Interiors for directions (casual wording, not a brochure), and weave into that same line that shipping options are available with details on the link below. Do not paste the full street address, store hours, URLs, shipping policy, freight brokers, or phone numbers from JSON; a separate block after your text will have address and links. For next steps: tell readers to use the link at the bottom of this posting for full item details and to reach out or purchase through the site, or to come into the showroom. Never ask them to reply here on Craigslist or to coordinate only through Craigslist email.";
  } else {
    maxBodyChars = 420;
    structureGuide = isLuxury
      ? "Open with the item type and standout style details in premium but natural Marketplace wording. Do NOT include explicit brand names/trademarks. Use 2-4 short lines grounded in title + catalogDescription: condition callout, materials, hardware/finish, silhouette/style, and practical use if supported by catalog facts. Mention authentication documentation is available. Keep pickup in Scottsdale (near Scottsdale Quarter is fine) and note shipping options are available with full logistics on the site link below. End with a confident natural call to action to message now or email for details. Never use an em dash; use commas or periods."
      : "Open on the item in normal Marketplace wording (e.g. 'Check out...', 'Selling...') using title + catalogDescription as the factual base: same claims, tight paraphrase; never invent brands, damage, dimensions, or materials not supported by catalog/title. No shop name or consignment pitch up front. 1-3 short lines: what it is, condition/size only if catalog says so, casual price, Scottsdale-area pickup near Scottsdale Quarter if you mention area, and that shipping options are available (which service applies is on the site; do not hedge with 'might' / 'maybe' / 'might be available'). End with one short line: full pickup/shipping/freight/checkout wording is on the website at the link below; email for questions; do not type the email address. Never use an em dash (long dash) in your output; use commas, periods, or 'and' instead.";
    toneGuide = isLuxury
      ? "Facebook voice for collectors and smart deal seekers: informed and trustworthy, but not posh, not boutique, not corporate. Short lines, no hype."
      : "Sounds like a real person on Facebook Marketplace speaking to collectors, yard-sale/value shoppers, flippers, and practical home buyers. Short, plain, conversational.";
    avoidPhraseGuide = isLuxury
      ? "Do NOT use or echo: Lost & Found, Lost and Found, consignment (as a store label), Discover, stunning, gorgeous, masterpiece, don't miss out, perfect for anyone, beautifully balances, elevate your space, timeless appeal, artisanal flair, captures the essence, anyone looking to add, yours for just, act fast, limited opportunity, shop with confidence. Do not use explicit brand/trademark names from sourceTitle or catalogDescription in output. Do not use em-dash punctuation (Unicode U+2014) or en-dash as a clause dash (U+2013); use commas, periods, or 'and'."
      : "Do NOT use or echo: Lost & Found, Lost and Found, consignment (as a store label), Discover, stunning, gorgeous, masterpiece, don't miss out, perfect for anyone, beautifully balances, elevate your space, timeless appeal, artisanal flair, captures the essence, anyone looking to add, yours for just, act fast, limited opportunity, shop with confidence. Do not use em-dash punctuation (Unicode U+2014) or en-dash as a clause dash (U+2013); use commas, periods, or 'and'.";
    logisticsGuide = isLuxury
      ? "Use storePolicyInternalOnly so you do not invent carriers, rates, or guarantees. Keep logistics short: shipping options are available and full rules are on the site link below. Never add phone numbers, dollar amounts, time windows, storage/freight numbers, or broker names in the body."
      : "Use storePolicyInternalOnly so you do not invent carriers, rates, or guarantees. Say shipping options are available and spelled out on the site link below. Never 'shipping might be available' or similar hedging. Never put phone numbers, dollar amounts, time windows, storage/freight numbers, or broker names in your body.";
  }
  if (moverAssistEligible) {
    logisticsGuide += ` Include one short line exactly like this meaning: "${moverAssistLine}"`;
  } else {
    logisticsGuide += " Do not mention mover hourly rates or delivery setup services unless the item is clearly heavy/large in JSON.";
  }

  const storePolicyInternalOnly = [
    "MODEL REFERENCE ONLY: do not paste, quote, bullet, or summarize this in your output. It exists so you never contradict checkout reality.",
    "Site policy (Lost & Found Resale): eligible items get shipping at checkout from size/weight/handling; larger, fragile, or long-distance may need freight preparation or pickup instead of standard shipping.",
    "After purchase customers should call 480-588-7006 to confirm pickup/delivery; nights/weekends/holidays may mean slower callback.",
    "Pickup vs freight prep, local third-party delivery coordination, 72-hour coordination window, storage fees, freight prep fee, 48-hour staging notice, broker names (FreightCenter, FreightQuote, uShip), liftgate/no dock, all sales final, and carrier responsibility after pickup: all detailed on the website; your Facebook body must not restate them.",
  ].join(" ");

  const facts = {
    variationHint,
    outputChannel: isCraigslist ? "craigslist" : "facebook",
    sellerContext: isCraigslist
      ? "Scottsdale area Craigslist listing for mixed local buyer types. Standalone body text only (no separate footer). Audience can include collectors, resellers/flippers, and value-first shoppers, so keep language practical and credible. You may use one short pickup wayfinding sentence that names Scottsdale Quarter and Lost and Found Resale Interiors. Do not stack store slogans, consignment pitch, or ‘we are Lost & Found’ branding. Do not instruct readers to reply here or coordinate through Craigslist; contact and checkout are through the link that appears after your text, or they can visit the showroom."
      : "Scottsdale resale listing for Facebook Marketplace's mixed crowd: collectors, deal hunters, flippers, and everyday home buyers. Keep voice practical and human, not posh. Do NOT name the business (no ‘Lost & Found’, no store name, no ‘furniture & home consignment’ tagline) anywhere in your text. That branding lives in the fixed block after your copy.",
    itemCategoryHint: isLuxury ? "handbags/luxury accessory vibe if it fits the title" : "furniture/home/decor vibe if it fits the title",
    title: title || "(no title)",
    askingPriceFacebook: price || null,
    inventoryKind: isLuxury ? "luxury_handbags_accessories" : "furniture_and_home_resale",
    pickupArea: isCraigslist
      ? "Right by Scottsdale Quarter; storefront pickup at Lost and Found Resale Interiors (north Scottsdale / Hayden)."
      : "Near Scottsdale Quarter (north Scottsdale / Hayden).",
    pickupAddress: pickupAddress || "15530 N Greenway Hayden Loop Suite 100, Scottsdale, AZ 85260",
    pickupHours: pickupHours || "MON - SAT 10-5, SUN 12-4",
    contactEmail,
    catalogDescription: catalog || "(none supplied)",
    moverAssistEligible,
    moverAssistLine: moverAssistEligible ? moverAssistLine : "",
    storePolicyInternalOnly,
    structure: structureGuide,
    toneTarget: toneGuide,
    audienceLane,
    audienceGuidance: audienceGuidance[audienceLane],
    avoidPhrases: avoidPhraseGuide,
    logisticsHint: logisticsGuide,
    variationProfile,
    maxBodyChars,
  };

  const system = isCraigslist
    ? `You write ONLY the body text for a Craigslist for-sale post (plain text for the description box).

Output rules:
- No markdown, bullets, numbers, emojis. No URLs or domains. Do not type an email address.
- Do NOT open with a store name or consignment pitch. Sound like a normal Craigslist seller.
- Write for a mixed local crowd (collectors, value shoppers, and resellers). Keep it practical and grounded, not posh or luxury-ad voice.
- Use audienceLane and audienceGuidance as a light touch only. One or two subtle cues are enough; never sound over-the-top, salesy, or role-played.
- If moverAssistEligible is true in JSON, include one short delivery-help line using moverAssistLine naturally. If false, do not mention mover hourly rates.
- Do not ask readers to reply here on Craigslist or to coordinate pickup only through Craigslist. Point them to the link block below this text for item details, shipping options, web contact, and checkout, or to visit in person near Scottsdale Quarter at Lost and Found Resale Interiors.
- Natural length: aim ~400-900 characters unless catalog needs a bit more; respect maxBodyChars hard cap.
- Only paraphrase catalog facts; never invent damage or brands.
- Treat logistics and policy facts as hard constraints: do not change or contradict shipping availability, pickup location, AS IS condition language, or checkout/contact direction implied by JSON guidance.
- If inventoryKind is luxury_handbags_accessories: never include explicit brand or trademark names in output text.
- JSON may include storePolicyInternalOnly: treat it as silent context only. Never repeat or summarize it in your reply.
- Never use an em dash in your output (Unicode U+2014). Use a comma, a period, or the word 'and' instead. Same for en dash (U+2013) as a sentence dash; for number ranges a plain hyphen is OK (e.g. 16-29).
- At most one exclamation mark in the whole post (usually none).`
    : `You write ONLY the short main body for a Facebook Marketplace item (plain text before a separate block with links and store info).

Output rules:
- No markdown, bullets, numbers, emojis. No URLs or domains. Do not type an email address.
- Do NOT open with or include the business name or a ‘we are a consignment shop’ line. Jump straight into the item like a normal FB seller.
- Facebook casual: short, direct. Write for collectors and deal-minded buyers, not a posh boutique audience.
- Use audienceLane and audienceGuidance as a light touch only. Keep language natural and restrained, not over-the-top.
- If moverAssistEligible is true in JSON, include one short delivery-help line using moverAssistLine naturally. If false, do not mention mover hourly rates.
- Lead with the product; ground specifics in catalogDescription + title. Close by nudging them to the site link below for logistics and email for questions.
- HARD LENGTH: aim ~180-340 characters; max 420 characters. Trim fluff if long.
- Only paraphrase catalog facts; never invent damage or brands.
- Treat logistics and policy facts as hard constraints: do not change or contradict shipping availability, pickup location, AS IS condition language, or checkout/contact direction implied by JSON guidance.
- If inventoryKind is luxury_handbags_accessories: never include explicit brand or trademark names in output text.
- JSON may include storePolicyInternalOnly: treat it as silent context only. Never repeat or summarize it in your reply.
- For shipping: prefer confident wording like ‘shipping options are available’ or ‘shipping’s on the site’. Do not say they might or may be available.
- Never use an em dash in your output (Unicode U+2014, often shown as a long dash between clauses). Use a comma, a period, or the word ‘and’ instead. Same for en dash (U+2013) used as a sentence dash; for number ranges a plain hyphen is OK (e.g. 16-29).
- At most one exclamation mark in the whole post (usually none).`;

  const userMsg = isCraigslist
    ? `Write the Craigslist body using ONLY the JSON. Obey sellerContext and avoidPhrases strictly. Follow structure, toneTarget, logisticsHint. Respect maxBodyChars.
Use variationHint and variationProfile to make wording and sentence rhythm different from typical prior outputs, while keeping all facts consistent.
Do not reuse the same opener/closer phrasing every time.
Apply audienceLane and audienceGuidance with restraint: keep it subtle and natural.

Example vibe (do not copy): "Selling a solid wood dining table we used in our dining room for a few years. Seats six comfortably, a few normal scuffs on the legs. Measures about 60 by 36. You can pick it up right by Scottsdale Quarter at Lost and Found Resale Interiors, and shipping options are in the full details on the link at the bottom of this post. You can also swing by the store."

Facts JSON:\n${JSON.stringify(facts)}`
    : `Write the body using ONLY the JSON. Obey sellerContext and avoidPhrases strictly. Follow structure, toneTarget, logisticsHint. Respect maxBodyChars.
Use variationHint and variationProfile to keep phrasing fresh between runs (new opener and close style), but do not alter factual meaning.
Do not reuse the same opener/closer phrasing every time.
Apply audienceLane and audienceGuidance with restraint: keep it subtle and natural.

Example vibe (do not copy): "Check out this Canyon de Chelly print by Wilson Hurley, framed, about 35.5 x 30.5. Asking $199. Local pickup in Scottsdale, shipping options are on the link below. Email if you have questions."

Facts JSON:\n${JSON.stringify(facts)}`;

  try {
    const resp = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${key}`,
      },
      body: JSON.stringify({
        model,
        temperature: isCraigslist ? 0.88 : 0.92,
        max_tokens: isCraigslist ? 700 : 220,
        messages: [
          { role: "system", content: system },
          { role: "user", content: userMsg },
        ],
      }),
    });

    const raw = await resp.text();
    if (!resp.ok) {
      webflowLog("error", { event: "api.listing_blurb.openai_http", status: resp.status, body: raw.slice(0, 400) });
      return res.status(502).json({
        error: `OpenAI request failed (${resp.status})`,
        detail: raw.slice(0, 200),
      });
    }

    let json;
    try {
      json = JSON.parse(raw);
    } catch {
      return res.status(502).json({ error: "OpenAI returned non-JSON" });
    }

    let text = String(json?.choices?.[0]?.message?.content || "").trim();
    if (!text) {
      return res.status(502).json({ error: "OpenAI returned empty text" });
    }
    text = text
      .replace(/\*\*([^*]+)\*\*/g, "$1")
      .replace(/\*([^*]+)\*/g, "$1")
      .replace(/^#{1,6}\s+/gm, "")
      .replace(/\s*\u2014\s*/g, ", ")
      .replace(/\u2013/g, "-")
      .replace(/,\s*,/g, ",")
      .trim();

    const cap = isCraigslist ? 1500 : 440;
    if (text.length > cap) {
      const slice = text.slice(0, cap);
      const breakAt = Math.max(slice.lastIndexOf("."), slice.lastIndexOf("!"), slice.lastIndexOf("?"));
      text = (breakAt > 80 ? slice.slice(0, breakAt + 1) : slice.replace(/\s+\S*$/, "")).trim();
    }

    webflowLog("info", {
      event: "api.listing_blurb.ok",
      model,
      titleLen: title.length,
      catalogLen: catalog.length,
      outputChannel: isCraigslist ? "craigslist" : "facebook",
    });
    return res.json({ text, model });
  } catch (err) {
    webflowLog("error", { event: "api.listing_blurb.error", message: err?.message });
    return res.status(500).json({ error: err?.message || "listing-blurb failed" });
  }
});

/**
 * POST /clear-cache — Remove cache entries for given Shopify product IDs so the next sync will
 * re-resolve vertical and create/update in the correct collection (fixes items stuck as wrong vertical or archived).
 * Body: { "shopifyProductIds": ["9319055327491", "9319054213379", ...] }
 */
app.post("/clear-cache", (req, res) => {
  try {
    const ids = req.body?.shopifyProductIds;
    if (!Array.isArray(ids) || ids.length === 0) {
      return res.status(400).json({
        error: "Missing or empty shopifyProductIds",
        usage: "POST /clear-cache with body: { \"shopifyProductIds\": [\"id1\", \"id2\", ...] }",
      });
    }
    const cache = loadCache();
    let cleared = 0;
    for (const id of ids) {
      const key = String(id).trim();
      if (key && cache[key] !== undefined) {
        delete cache[key];
        cleared++;
      }
    }
    saveCache(cache);
    webflowLog("info", { event: "clear_cache", cleared, requested: ids.length, shopifyProductIds: ids });
    res.json({ cleared, totalRequested: ids.length });
  } catch (err) {
    webflowLog("error", { event: "clear_cache.error", message: err.message });
    res.status(500).json({ error: err.message });
  }
});

/**
 * POST /webhook/products/delete — Same path as sync-all “disappeared”: confirm Shopify not active, Webflow sold, cache row removed.
 */
app.post("/webhook/products/delete", verifyShopifyHmac, async (req, res) => {
  res.status(200).send("ok");
  try {
    const id = req.body?.id != null ? String(req.body.id) : null;
    if (!id) {
      webflowLog("warn", {
        event: "shopify.webhook.product_delete",
        path: "/webhook/products/delete",
        reason: "missing_product_id",
      });
      return;
    }
    webflowLog("info", {
      event: "shopify.webhook.product_delete",
      path: "/webhook/products/delete",
      shopifyProductId: id,
      topic: req.get("X-Shopify-Topic") ?? "",
      shop: req.get("X-Shopify-Shop-Domain") ?? "",
    });
    const cache = loadCache();
    const outcome = await processDisappearedShopifyProduct(id, cache, { trigger: "webhook.products_delete" });
    saveCache(cache);
    webflowLog("info", {
      event: "shopify.webhook.product_delete.done",
      shopifyProductId: id,
      outcome,
    });
  } catch (err) {
    webflowLog("error", {
      event: "shopify.webhook.product_delete.error",
      message: err.message,
    });
  }
});

app.post("/sync-all", async (req, res) => {
  syncRequestId = crypto.randomUUID().slice(0, 8);
  syncStartTime = Date.now();
  webflowLog("info", { event: "sync-all.entry", message: "sync-all started" });
  try {
    // Optional: force LLM reclassification for this run. "all" = every product; or array of Shopify product IDs.
    const reclassify = req.body?.reclassify;
    const reclassifyAll = reclassify === "all" || reclassify === true;
    const reclassifyIdsSet =
      Array.isArray(reclassify) && reclassify.length > 0
        ? new Set(reclassify.map((id) => String(id)))
        : null;
    if (reclassifyAll || reclassifyIdsSet) {
      webflowLog("info", { event: "sync-all.reclassify", reclassifyAll, reclassifyCount: reclassifyIdsSet?.size ?? "all" });
    }

    const products = await fetchAllShopifyProducts();
    webflowLog("info", { event: "sync-all.fetched_shopify", productCount: products?.length ?? 0 });
    const cache = loadCache();
    webflowLog("info", { event: "sync-all.loaded", productCount: products?.length ?? 0, cacheKeys: Object.keys(cache).length });

    await loadFurnitureCategoryMap();

    // Pre-load Webflow indexes once → O(1) lookups instead of N×page API scans per product
    luxuryItemIndex = null;
    furnitureProductIndex = null;
    furnitureSkuIndex = null;
    await Promise.all([
      loadLuxuryItemIndex(),
      loadFurnitureProductIndex(),
      loadFurnitureSkuIndex(),
    ]);

    let created = 0,
      updated = 0,
      skipped = 0,
      sold = 0,
      failed = 0,
      orphanMarkedSold = 0,
      archivedLongSold = 0,
      soldBackfillArchived = 0;

    // Disappeared: in cache but not in this run's product list. Only touch Webflow when we've confirmed in Shopify that the product is not active.
    const previousIds = Object.keys(cache);
    const currentIds = products.map((p) => String(p.id));
    const disappeared = previousIds.filter((id) => !currentIds.includes(id));

    const disappearedConcurrency = Math.min(10, Math.max(3, disappeared.length));
    webflowLog("info", {
      event: "sync-all.disappeared_check",
      previousIds: previousIds.length,
      currentIds: currentIds.length,
      disappeared: disappeared.length,
      disappearedConcurrency,
    });

    // Run Shopify status checks in parallel (capped) so many disappeared don't slow the run.
    const confirmedById = {};
    for (let i = 0; i < disappeared.length; i += disappearedConcurrency) {
      const chunk = disappeared.slice(i, i + disappearedConcurrency);
      const results = await Promise.all(chunk.map((id) => fetchShopifyProductStatus(id)));
      chunk.forEach((id, idx) => {
        confirmedById[id] = results[idx];
      });
    }

    for (const goneId of disappeared) {
      const confirmed = confirmedById[goneId];
      if (confirmed === null || confirmed === undefined) {
        webflowLog("info", {
          event: "sync-all.disappeared_skip_unconfirmed",
          shopifyProductId: goneId,
          reason: "fetch_failed_or_unknown",
        });
        continue;
      }
      if (confirmed.status === "active") {
        webflowLog("info", {
          event: "sync-all.disappeared_skip_still_active",
          shopifyProductId: goneId,
          reason: "product_still_active_in_shopify",
        });
        continue;
      }
      if (confirmed.status !== "gone" && confirmed.status !== "archived" && confirmed.status !== "draft") {
        webflowLog("info", {
          event: "sync-all.disappeared_skip_unconfirmed",
          shopifyProductId: goneId,
          reason: "shopify_status_unknown",
          status: confirmed.status,
        });
        continue;
      }
      const outcome = await processDisappearedShopifyProduct(goneId, cache, { trigger: "sync-all.disappeared" });
      if (outcome === "marked_sold") sold++;
    }

    orphanMarkedSold = await sweepWebflowOrphansAgainstShopifyCatalog(products, cache);
    sold += orphanMarkedSold;

    const duplicateEmailSentFor = new Set();
    const shopifyWriteEmailSentFor = new Set();
    const concurrency = Math.min(Math.max(1, parseInt(process.env.SYNC_CONCURRENCY || "3", 10) || 1), 15);

    for (let i = 0; i < products.length; i += concurrency) {
      const chunk = products.slice(i, i + concurrency);
      const settled = await Promise.allSettled(
        chunk.map((p) =>
          syncSingleProduct(p, cache, {
            duplicateEmailSentFor,
            shopifyWriteEmailSentFor,
            forceReclassify: reclassifyAll || (reclassifyIdsSet != null && reclassifyIdsSet.has(String(p.id))),
          })
        )
      );
      const results = settled.map((r) => (r.status === "fulfilled" ? r.value : null));
      for (let j = 0; j < settled.length; j++) {
        const s = settled[j];
        if (s.status === "rejected") {
          failed++;
          const p = chunk[j];
          webflowLog("error", {
            event: "sync-all.product_failed_continue",
            shopifyProductId: p?.id != null ? String(p.id) : null,
            productTitle: p?.title ?? null,
            message: s.reason?.message ?? String(s.reason),
          });
          continue;
        }
        const result = results[j];
        if (result.duplicateCorrected && result.duplicateLog) {
          webflowLog("info", {
            event: "sync-all.duplicate_placement",
            message: "Item was in Furniture but detected as Luxury; furniture listing deleted and re-synced to Luxury. Run continues so cache is saved.",
            ...result.duplicateLog,
          });
          await sendDuplicatePlacementEmail(result.duplicateLog, duplicateEmailSentFor);
          // Do NOT throw: we already created/updated in Luxury and updated cache. Throwing prevented saveCache(), so next run had no cache and the item stayed in limbo (re-archiving / duplicate emails every run).
        }
        if (result.operation === "create") created++;
        else if (result.operation === "update") updated++;
        else if (result.operation === "sold") sold++;
        else skipped++;
      }
    }

    const retentionOut = await archiveLongSoldWebflowListings(cache);
    archivedLongSold = retentionOut.archived;
    soldBackfillArchived = retentionOut.soldBackfillArchived;

    saveCache(cache);

    const durationMs = Date.now() - syncStartTime;
    webflowLog("info", {
      event: "sync-all.exit",
      created,
      updated,
      skipped,
      sold,
      failed,
      orphanMarkedSold,
      archivedLongSold,
      soldBackfillArchived,
      total: products.length,
      durationMs,
    });
    res.json({
      status: "ok",
      total: products.length,
      created,
      updated,
      skipped,
      sold,
      failed,
      orphanMarkedSold,
      archivedLongSold,
      soldBackfillArchived,
      durationMs,
    });
  } catch (err) {
    const status = err.response?.status;
    const body = err.response?.data;
    webflowLog("error", {
      event: "sync-all.error",
      message: err.message,
      status: status ?? null,
      responseBody: body ?? null,
      url: err.config?.url ?? null,
      method: err.config?.method ?? null,
    });
    const detail = body?.message || body?.err || err.message;
    res.status(status && status >= 400 ? status : 500).json({
      error: err.message,
      ...(detail && detail !== err.message && { detail: String(detail) }),
      ...(body && typeof body === "object" && { webflowResponse: body }),
    });
  } finally {
    syncRequestId = null;
    luxuryItemIndex = null;
    furnitureProductIndex = null;
    furnitureSkuIndex = null;
    syncStartTime = null;
  }
});

/* ======================================================
   SERVER
====================================================== */
const PORT = process.env.PORT || 4000;
app.listen(PORT, () => {
  console.log(`🔥 Sync server running on ${PORT}`);
  const host = process.env.RENDER_EXTERNAL_HOSTNAME || `localhost:${PORT}`;
  const scheme = process.env.RENDER_EXTERNAL_HOSTNAME ? "https" : "http";
  console.log(`Shopify order webhook: ${scheme}://${host}/shopify/order`);
  console.log(
    `Shopify product webhooks: ${scheme}://${host}/webhook/products (create), ${scheme}://${host}/webhook/products/update (update), ${scheme}://${host}/webhook/products/delete (delete)`
  );
  console.log(`Facebook listing helper (Webflow default): ${scheme}://${host}/api/listing?name=...`);
  console.log(`  Shopify mode: ${scheme}://${host}/api/listing?name=...&source=shopify`);
  console.log(`  Facebook copy (OpenAI): POST ${scheme}://${host}/api/listing-blurb (needs OPENAI_API_KEY)`);
});



