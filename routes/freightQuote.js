import express from "express";
import { isResendConfigured } from "../emailService.js";
import { applyConsignmentCorsHeaders } from "../lib/consignmentCors.js";
import { validateFreightQuoteRequest } from "../lib/freightQuoteValidation.js";
import { sendFreightQuoteEmails } from "../lib/freightQuoteEmail.js";
import { LOCAL_AZ_HOURLY_RATE, palletizeItems } from "../lib/freightPalletize.js";

const router = express.Router();

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

router.options("/freight-quote", (req, res) => {
  applyConsignmentCorsHeaders(req, res);
  res.sendStatus(204);
});

router.options("/freight-quote/preview", (req, res) => {
  applyConsignmentCorsHeaders(req, res);
  res.sendStatus(204);
});

/**
 * POST /api/freight-quote/preview
 * Palletize + path label only (no email). Useful for Webflow estimate UI.
 */
router.post("/freight-quote/preview", express.json({ limit: "1mb" }), (req, res) => {
  try {
    const validation = validateFreightQuoteRequest(req.body || {});
    if (!validation.ok) {
      return res.status(400).json({ success: false, error: validation.error });
    }
    const { submission } = validation;
    return res.json({
      success: true,
      mode: submission.mode,
      isLocalAz: submission.isLocalAz,
      localHourlyRate: submission.isLocalAz ? LOCAL_AZ_HOURLY_RATE : null,
      path: submission.isLocalAz ? "local_arizona" : "nationwide_freight",
      items: submission.palletized,
      access: submission.access,
    });
  } catch (err) {
    console.error("[freight-quote] preview failed:", err?.message || err);
    return res.status(500).json({ success: false, error: "Preview failed. Please try again." });
  }
});

/**
 * POST /api/freight-quote
 * JSON body from Webflow freight calculator / contact-for-quote form.
 * Emails INTERNAL_NOTIFY_EMAIL + customer summary (requires valid email + full address + access Qs).
 *
 * Item lookup: Webflow should call GET /api/listing?name=Exact+Title first, then include
 * width/depth/height/weight/price/productUrl on each item (or item_N_* form fields).
 */
router.post("/freight-quote", express.json({ limit: "1mb" }), async (req, res) => {
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
      return res.status(400).json({ success: false, error: validation.error });
    }

    const { submission } = validation;
    const submittedAt = formatSubmittedAt();

    await sendFreightQuoteEmails(submission, { submittedAt });

    return res.json({
      success: true,
      message:
        submission.mode === "please_quote"
          ? "Request received. Check your email for a summary — our team will follow up."
          : "Estimate request received. Check your email for a summary.",
      mode: submission.mode,
      isLocalAz: submission.isLocalAz,
      path: submission.isLocalAz ? "local_arizona" : "nationwide_freight",
      localHourlyRate: submission.isLocalAz ? LOCAL_AZ_HOURLY_RATE : null,
      itemCount: submission.palletized.length,
    });
  } catch (err) {
    console.error("[freight-quote] submission failed:", err?.message || err);
    return res.status(500).json({
      success: false,
      error: "Submission failed. Please try again.",
    });
  }
});

/** Lightweight palletize helper without access validation (debug / tools). */
router.post("/freight-quote/palletize", express.json({ limit: "500kb" }), (req, res) => {
  const items = Array.isArray(req.body?.items) ? req.body.items : [];
  return res.json({ success: true, items: palletizeItems(items) });
});

export default router;
