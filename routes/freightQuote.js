import express from "express";
import { isResendConfigured, parseRecipients } from "../emailService.js";
import { applyConsignmentCorsHeaders } from "../lib/consignmentCors.js";
import { validateFreightQuoteRequest } from "../lib/freightQuoteValidation.js";
import { sendFreightQuoteEmails } from "../lib/freightQuoteEmail.js";
import { buildLocalEstimateForDestination } from "../lib/freightLocalEstimate.js";
import { validateAndStandardizeAddress } from "../lib/freightAddressValidation.js";
import { fetchNationwideLiveRate } from "../lib/freightNationwideRate.js";
import {
  freightRateLimit,
  makeRequestId,
  buildIdempotencyKey,
  getIdempotentResponse,
  setIdempotentResponse,
} from "../lib/freightQuoteSecurity.js";
import { verifyMapRequest, fetchStaticMapBytes } from "../lib/freightRouteMap.js";

const router = express.Router();
const jsonParser = express.json({ limit: "1mb" });

router.use((req, res, next) => {
  applyConsignmentCorsHeaders(req, res);
  next();
});

function formatSubmittedAt(date = new Date()) {
  return date.toLocaleString("en-US", {
    timeZone: "America/Phoenix",
    dateStyle: "full",
    timeStyle: "short",
  });
}

function money(n) {
  const num = Number(n);
  if (!Number.isFinite(num)) return null;
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(num);
}

function accessorialsFromAccess(access = {}) {
  return {
    liftgate_pickup: Boolean(access.liftgate_pickup),
    liftgate_delivery: Boolean(access.liftgate_delivery),
    residential_delivery: Boolean(access.residential),
  };
}

function buildLocalDisplay(submission, local) {
  const extraCrew = Boolean(submission.access?.needs_more_than_two_people);
  const isPickup = submission.delivery_path === "pickup_az";
  const amount = local.local_estimate?.estimated_price ?? null;
  const label = extraCrew
    ? "Approximate Two-Person Base:"
    : "Your Preliminary Estimate Is";
  return {
    estimate_label: label,
    display_amount: amount,
    display_amount_formatted: amount != null ? money(amount) : null,
    headline: amount != null ? `${label} ${money(amount)}` : label,
    drive_minutes: local.route?.drive_minutes ?? local.local_estimate?.drive_minutes ?? null,
    distance_miles: local.route?.distance_miles ?? null,
    map_image_url: local.route?.map_image_url ?? null,
    directions_url: local.route?.directions_url ?? null,
    currency: "USD",
    path: submission.delivery_path,
    is_pickup: isPickup,
    requires_manual_review: Boolean(local.requires_manual_review),
    review_reasons: local.review_reasons || [],
    extra_crew: extraCrew,
  };
}

function mapItemsForClient(items = []) {
  return items.map((it) => ({
    index: it.index,
    source: it.source,
    title: it.title,
    width: it.width,
    depth: it.depth,
    height: it.height,
    weight: it.weight,
    quantity: it.quantity,
    price: it.price,
    product_url: it.product_url,
    freight_class: it.freight_class,
    freight_class_display:
      it.freight_class == null || it.freight_class === ""
        ? "To be confirmed"
        : String(it.freight_class),
    non_stackable: it.non_stackable,
    pallet: it.pallet,
    ok: it.ok,
    missing: it.missing,
  }));
}

/**
 * Sole calculator context: address standardize → path rules → palletize → price/rate.
 */
async function buildQuoteContext(submission) {
  const addrResult = await validateAndStandardizeAddress(submission.delivery_address || {});
  if (addrResult.delivery_address) {
    submission.delivery_address = addrResult.delivery_address;
    submission.street = addrResult.delivery_address.street;
    submission.unit = addrResult.delivery_address.unit;
    submission.city = addrResult.delivery_address.city;
    submission.state = addrResult.delivery_address.state;
    submission.zip = addrResult.delivery_address.zip;
  }

  const local = await buildLocalEstimateForDestination({
    deliveryPath: submission.delivery_path,
    state: submission.state,
    zip: submission.zip,
    destinationFull: submission.delivery_address.full,
    access: submission.access,
    itemCount: Array.isArray(submission.items) ? submission.items.length : 0,
  });

  if (local.ok === false) {
    return { error: local };
  }

  const items = mapItemsForClient(submission.items);
  const accessorials = accessorialsFromAccess(submission.access);

  if (submission.delivery_path === "nationwide") {
    const nationwide_rate = await fetchNationwideLiveRate(submission);
    const low = nationwide_rate.range_low;
    const high = nationwide_rate.range_high;
    const rangeFormatted =
      low != null && high != null ? `${money(low)} - ${money(high)}` : null;
    return {
      delivery_path: "nationwide",
      freight_ready: true,
      address: {
        standardized: Boolean(addrResult.standardized),
        provider: addrResult.provider,
        delivery_address: submission.delivery_address,
      },
      route: nationwide_rate.route?.distance_miles != null
        ? {
            distance_miles: nationwide_rate.route.distance_miles,
            drive_minutes: nationwide_rate.route.drive_minutes ?? null,
            map_image_url: nationwide_rate.route.map_image_url ?? null,
            directions_url: nationwide_rate.route.directions_url ?? null,
          }
        : null,
      items,
      accessorials,
      nationwide_rate,
      requires_confirmed_quote: true,
      requires_manual_review: true,
      review_reasons: [
        "Nationwide preliminary range only | confirm exact quote with freight partners",
      ],
      multi_item_note: submission.multi_item_note || undefined,
      display: {
        headline: "Your Preliminary Nationwide Freight Range",
        estimate_label: "Estimated freight range",
        display_amount: null,
        display_amount_formatted: rangeFormatted,
        range_low: low,
        range_high: high,
        distance_miles: nationwide_rate.distance_miles ?? nationwide_rate.route?.distance_miles ?? null,
        distance_measured: Boolean(nationwide_rate.distance_measured),
        map_image_url: nationwide_rate.route?.map_image_url ?? null,
        directions_url: nationwide_rate.route?.directions_url ?? null,
        status: nationwide_rate.status,
        message: nationwide_rate.message,
        follow_up: nationwide_rate.follow_up,
        white_glove_likely: Boolean(nationwide_rate.white_glove_likely),
      },
      message: nationwide_rate.message,
    };
  }

  const display = buildLocalDisplay(submission, local);
  return {
    delivery_path: submission.delivery_path || local.delivery_path || "local_az",
    address: {
      standardized: Boolean(addrResult.standardized),
      provider: addrResult.provider,
      delivery_address: submission.delivery_address,
    },
    route: local.route,
    local_estimate: local.local_estimate,
    requires_manual_review: display.requires_manual_review,
    review_reasons: display.review_reasons,
    items,
    accessorials,
    multi_item_note: submission.multi_item_note || undefined,
    display,
  };
}

router.options("/freight-quote", (req, res) => {
  applyConsignmentCorsHeaders(req, res);
  res.sendStatus(204);
});

router.options("/freight-quote/preview", (req, res) => {
  applyConsignmentCorsHeaders(req, res);
  res.sendStatus(204);
});

router.options("/freight-quote/map", (req, res) => {
  applyConsignmentCorsHeaders(req, res);
  res.sendStatus(204);
});

/**
 * GET /api/freight-quote/map | signed Static Maps proxy (no API key in emails/pages).
 */
router.get("/freight-quote/map", async (req, res) => {
  try {
    applyConsignmentCorsHeaders(req, res);
    const verified = verifyMapRequest(req.query || {});
    if (!verified.ok) {
      return res.status(verified.status || 400).send(verified.error || "bad_request");
    }
    const image = await fetchStaticMapBytes(verified);
    if (!image.ok) {
      return res.status(image.status || 502).send(image.error || "map_failed");
    }
    res.setHeader("Content-Type", image.contentType);
    res.setHeader("Cache-Control", "public, max-age=86400");
    return res.send(image.buf);
  } catch (err) {
    console.error("[freight-quote] map proxy failed:", err?.message || err);
    return res.status(500).send("map_failed");
  }
});

/**
 * POST /api/freight-quote/preview | validate + calculate only (no email, no booking).
 */
router.post("/freight-quote/preview", freightRateLimit, jsonParser, async (req, res) => {
  try {
    const validation = validateFreightQuoteRequest(req.body || {});
    if (!validation.ok) {
      return res.status(validation.status || 400).json({
        success: false,
        error: validation.error,
      });
    }

    const ctx = await buildQuoteContext(validation.submission);
    if (ctx.error) {
      return res.status(ctx.error.status || 400).json({
        success: false,
        error: ctx.error.error,
      });
    }

    return res.json({
      success: true,
      emails_sent: false,
      ...ctx,
    });
  } catch (err) {
    console.error("[freight-quote] preview failed:", err?.message || err);
    return res.status(500).json({ success: false, error: "Preview failed. Please try again." });
  }
});

/**
 * POST /api/freight-quote | recalculate + exactly one customer + one internal email.
 */
router.post("/freight-quote", freightRateLimit, jsonParser, async (req, res) => {
  try {
    if (!isResendConfigured()) {
      console.error("[freight-quote] Resend not configured");
      return res.status(500).json({
        success: false,
        error: "Submission failed. Please try again.",
      });
    }

    const validation = validateFreightQuoteRequest(req.body || {});
    if (!validation.ok) {
      if (validation.honeypot) {
        console.warn("[freight-quote] honeypot filled | rejecting");
        return res.status(400).json({
          success: false,
          error: "Submission blocked by spam filter. Please try again.",
        });
      }
      return res.status(validation.status || 400).json({
        success: false,
        error: validation.error,
      });
    }

    const submission = validation.submission;
    const idemKey = buildIdempotencyKey(submission);
    const cached = getIdempotentResponse(idemKey);
    if (cached) {
      console.log("[freight-quote] duplicate submit within a few seconds | skipped second email", {
        requestId: cached.request_id,
        email: submission.customer_email.replace(/(.{2}).+(@.+)/, "$1***$2"),
      });
      return res.json({
        ...cached,
        duplicate: true,
      });
    }

    const ctx = await buildQuoteContext(submission);
    if (ctx.error) {
      return res.status(ctx.error.status || 400).json({
        success: false,
        error: ctx.error.error,
      });
    }

    const requestId = makeRequestId();
    const submittedAt = formatSubmittedAt();

    let emails = {
      customer: { sent: false, to: submission.customer_email },
      internal: { sent: false, to: parseRecipients(process.env.INTERNAL_NOTIFY_EMAIL) },
    };

    try {
      const mailResult = await sendFreightQuoteEmails(submission, {
        requestId,
        submittedAt,
        route: ctx.route || null,
        localEstimate: ctx.local_estimate || null,
        nationwideRate: ctx.nationwide_rate || null,
        reviewReasons: ctx.review_reasons || [],
      });
      emails = {
        customer: {
          sent: true,
          to: submission.customer_email,
          resend_id: mailResult.customer.resend_id,
        },
        internal: {
          sent: true,
          to: mailResult.internal.to,
          resend_id: mailResult.internal.resend_id,
        },
      };
      console.log("[freight-quote] emails accepted by Resend", {
        requestId,
        customer_resend_id: mailResult.customer.resend_id,
        internal_resend_id: mailResult.internal.resend_id,
        customer_to: submission.customer_email.replace(/(.{2}).+(@.+)/, "$1***$2"),
        internal_to: (mailResult.internal.to || []).map((e) =>
          String(e).replace(/(.{2}).+(@.+)/, "$1***$2")
        ),
      });
    } catch (mailErr) {
      console.error("[freight-quote] email failed:", mailErr?.message || mailErr);
      return res.status(500).json({
        success: false,
        error: "Estimate calculated but email failed. Please try again or call 480-588-7006.",
        request_id: requestId,
        emails: {
          customer: { sent: false, to: submission.customer_email, fail: String(mailErr?.message || mailErr) },
          internal: { sent: false, fail: String(mailErr?.message || mailErr) },
        },
      });
    }

    const responseBody = {
      success: true,
      message:
        submission.request_mode === "please_quote"
          ? "Request received. Check your email for a summary | our team will follow up."
          : "Estimate request received. Check your email for a summary.",
      request_id: requestId,
      emails,
      emails_sent: true,
      ...ctx,
      redirect_faq: "https://www.lostandfoundresale.com/faq",
      redirect_shop_all: "https://www.lostandfoundresale.com/all-for-sale",
    };

    setIdempotentResponse(idemKey, responseBody);
    console.log("[freight-quote] submitted", {
      requestId,
      path: submission.delivery_path,
      mode: submission.request_mode,
      items: submission.items.length,
      email: submission.customer_email.replace(/(.{2}).+(@.+)/, "$1***$2"),
      customer_resend_id: emails.customer.resend_id,
      internal_resend_id: emails.internal.resend_id,
    });

    return res.json(responseBody);
  } catch (err) {
    console.error("[freight-quote] submission failed:", err?.message || err);
    return res.status(500).json({
      success: false,
      error: "Submission failed. Please try again.",
    });
  }
});

export default router;
