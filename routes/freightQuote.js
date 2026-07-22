import express from "express";
import { isResendConfigured } from "../emailService.js";
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
    currency: "USD",
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
  });

  if (local.ok === false) {
    return { error: local };
  }

  const items = mapItemsForClient(submission.items);
  const accessorials = accessorialsFromAccess(submission.access);

  if (submission.delivery_path === "nationwide") {
    const nationwide_rate = await fetchNationwideLiveRate(submission);
    const pending = nationwide_rate.status !== "quoted";
    return {
      delivery_path: "nationwide",
      freight_ready: true,
      address: {
        standardized: Boolean(addrResult.standardized),
        provider: addrResult.provider,
        delivery_address: submission.delivery_address,
      },
      items,
      accessorials,
      nationwide_rate,
      requires_confirmed_quote: true,
      requires_manual_review: pending || Boolean(nationwide_rate.status === "pending_manual_review"),
      review_reasons: pending
        ? ["Nationwide carrier rate pending FreightCenter / staff confirmation"]
        : [],
      multi_item_note: submission.multi_item_note || undefined,
      display: {
        headline: pending ? "Freight-Ready — Rate Pending Review" : "Nationwide Freight Quote",
        estimate_label: null,
        display_amount: nationwide_rate.amount,
        display_amount_formatted:
          nationwide_rate.amount != null ? money(nationwide_rate.amount) : null,
        status: nationwide_rate.status,
        message: nationwide_rate.message,
      },
      message: nationwide_rate.message,
    };
  }

  const display = buildLocalDisplay(submission, local);
  return {
    delivery_path: "local_az",
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

/**
 * POST /api/freight-quote/preview — validate + calculate only (no email, no booking).
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
 * POST /api/freight-quote — recalculate + exactly one customer + one internal email.
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
        console.warn("[freight-quote] honeypot filled — rejecting");
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
      console.log("[freight-quote] duplicate submit within a few seconds — skipped second email", {
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
      customer: { sent: false, to: submission.customer_email, error: null },
      internal: { sent: false, to: "INTERNAL_NOTIFY_EMAIL", error: null },
    };

    try {
      await sendFreightQuoteEmails(submission, {
        requestId,
        submittedAt,
        route: ctx.route || null,
        localEstimate: ctx.local_estimate || null,
        reviewReasons: ctx.review_reasons || [],
      });
      emails = {
        customer: { sent: true, to: submission.customer_email, error: null },
        internal: { sent: true, to: "INTERNAL_NOTIFY_EMAIL", error: null },
      };
    } catch (mailErr) {
      console.error("[freight-quote] email failed:", mailErr?.message || mailErr);
      return res.status(500).json({
        success: false,
        error: "Estimate calculated but email failed. Please try again or call 480-588-7006.",
        request_id: requestId,
        emails: {
          customer: { sent: false, to: submission.customer_email, error: String(mailErr?.message || mailErr) },
          internal: { sent: false, error: String(mailErr?.message || mailErr) },
        },
      });
    }

    const responseBody = {
      success: true,
      message:
        submission.request_mode === "please_quote"
          ? "Request received. Check your email for a summary — our team will follow up."
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
      emails,
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
