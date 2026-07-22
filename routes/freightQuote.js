import express from "express";
import { isResendConfigured } from "../emailService.js";
import { applyConsignmentCorsHeaders } from "../lib/consignmentCors.js";
import { validateFreightQuoteRequest } from "../lib/freightQuoteValidation.js";
import { sendFreightQuoteEmails } from "../lib/freightQuoteEmail.js";
import { buildLocalEstimateForDestination } from "../lib/freightLocalEstimate.js";
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

function accessorialsFromAccess(access = {}) {
  return {
    liftgate_pickup: Boolean(access.liftgate_pickup),
    liftgate_delivery: Boolean(access.liftgate_delivery),
    residential_delivery: Boolean(access.residential),
  };
}

async function buildQuoteContext(submission) {
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

  if (submission.delivery_path === "nationwide") {
    return {
      delivery_path: "nationwide",
      freight_ready: true,
      items: submission.items.map((it) => ({
        title: it.title,
        pallet: it.pallet,
        missing: it.missing,
        ok: it.ok,
      })),
      accessorials: accessorialsFromAccess(submission.access),
      requires_confirmed_quote: true,
      requires_manual_review: false,
      review_reasons: [],
      message:
        "Lost & Found will review the shipment and provide confirmed FreightCenter pricing.",
      multi_item_note: submission.multi_item_note || undefined,
    };
  }

  return {
    delivery_path: "local_az",
    route: local.route,
    local_estimate: local.local_estimate,
    requires_manual_review: Boolean(local.requires_manual_review),
    review_reasons: local.review_reasons || [],
    items: submission.items,
    accessorials: accessorialsFromAccess(submission.access),
    multi_item_note: submission.multi_item_note || undefined,
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
 * POST /api/freight-quote/preview — validate + calculate only (no email).
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
      ...ctx,
    });
  } catch (err) {
    console.error("[freight-quote] preview failed:", err?.message || err);
    return res.status(500).json({ success: false, error: "Preview failed. Please try again." });
  }
});

/**
 * POST /api/freight-quote — recalculate + email internal + customer.
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
        return res.json({ success: true, message: "Request received." });
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
      return res.json(cached);
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

    await sendFreightQuoteEmails(submission, {
      requestId,
      submittedAt,
      route: ctx.route || null,
      localEstimate: ctx.local_estimate || null,
      reviewReasons: ctx.review_reasons || [],
    });

    const responseBody = {
      success: true,
      message:
        submission.request_mode === "please_quote"
          ? "Request received. Check your email for a summary — our team will follow up."
          : "Estimate request received. Check your email for a summary.",
      request_id: requestId,
      delivery_path: ctx.delivery_path,
      route: ctx.route || undefined,
      local_estimate: ctx.local_estimate || undefined,
      freight_ready: ctx.freight_ready || undefined,
      requires_confirmed_quote: ctx.requires_confirmed_quote || undefined,
      requires_manual_review: ctx.requires_manual_review || false,
      review_reasons: ctx.review_reasons || [],
      redirect_faq: "https://www.lostandfoundresale.com/faq",
    };

    setIdempotentResponse(idemKey, responseBody);
    console.log("[freight-quote] submitted", {
      requestId,
      path: submission.delivery_path,
      mode: submission.request_mode,
      items: submission.items.length,
      email: submission.customer_email.replace(/(.{2}).+(@.+)/, "$1***$2"),
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
