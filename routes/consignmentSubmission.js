import express from "express";
import multer from "multer";
import { isResendConfigured, sendInternalNotificationWithAttachments } from "../emailService.js";
import { buildPdfFilename } from "../lib/consignmentFilenames.js";
import { buildConsignmentEmail, sendCustomerConfirmationEmail } from "../lib/consignmentEmail.js";
import { analyzeConsignmentItemsPricing } from "../lib/consignmentPricingAnalysis.js";
import { generateConsignmentPdf } from "../lib/consignmentPdf.js";
import {
  groupPhotosByItemNumber,
  validateConsignmentSubmission,
} from "../lib/consignmentValidation.js";

const router = express.Router();

const MAX_FILE_SIZE_BYTES = 10 * 1024 * 1024;
const MAX_FILES = 30;

const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: MAX_FILE_SIZE_BYTES,
    files: MAX_FILES,
  },
});

function formatSubmittedAt(date = new Date()) {
  return date.toLocaleString("en-US", {
    timeZone: "America/Phoenix",
    dateStyle: "full",
    timeStyle: "short",
  });
}

function multerErrorMessage(err) {
  if (err.code === "LIMIT_FILE_SIZE") {
    return "Each photo must be 10 MB or smaller.";
  }
  if (err.code === "LIMIT_FILE_COUNT" || err.code === "LIMIT_UNEXPECTED_FILE") {
    return `You can upload at most ${MAX_FILES} photos per submission.`;
  }
  if (err.code === "LIMIT_PART_COUNT") {
    return "Too many form fields in this submission.";
  }
  return "Invalid file upload.";
}

/**
 * POST /api/consignment-submission
 * multipart/form-data from Webflow; photos grouped by item_N_photos field names.
 */
router.post(
  "/consignment-submission",
  (req, res, next) => {
    upload.any()(req, res, (err) => {
      if (err) {
        const message =
          err instanceof multer.MulterError ? multerErrorMessage(err) : "Invalid file upload.";
        return res.status(400).json({ success: false, error: message });
      }
      next();
    });
  },
  async (req, res) => {
    try {
      if (!isResendConfigured()) {
        console.error("[consignment] Resend not configured");
        return res.status(500).json({
          success: false,
          error: "Submission failed. Please try again.",
        });
      }

      const body = req.body || {};
      const files = req.files || [];

      // Group uploads by item number from field names item_1_photos, item_2_photos, etc.
      const photoGroups = groupPhotosByItemNumber(files);

      const validation = validateConsignmentSubmission(body, photoGroups);
      if (!validation.ok) {
        return res.status(400).json({ success: false, error: validation.error });
      }

      const items = validation.items;
      const submittedAt = formatSubmittedAt();

      // PDF generation: professional layout with images under each item section
      const pdfBuffer = await generateConsignmentPdf({
        body,
        items,
        photoGroups,
        submittedAt,
      });
      const pdfFilename = buildPdfFilename(body.customerName);

      let pricingResults = null;
      let pricingModelsUsed = [];
      try {
        const pricing = await analyzeConsignmentItemsPricing({ items, photoGroups });
        pricingResults = pricing.results;
        pricingModelsUsed = pricing.modelsUsed || [];
        if (pricing.configured && pricingResults?.length) {
          console.log("[consignment] pricing analysis complete", {
            items: pricingResults.length,
            models: pricingModelsUsed,
          });
        }
      } catch (pricingErr) {
        console.error(
          "[consignment] pricing analysis failed (continuing):",
          pricingErr?.message || pricingErr
        );
      }

      // Email: summary + per-item sections; inline CID photos + renamed attachments
      const emailPayload = buildConsignmentEmail({
        body,
        items,
        photoGroups,
        pdfBuffer,
        pdfFilename,
        submittedAt,
        pricingResults,
      });

      await sendInternalNotificationWithAttachments({
        subject: emailPayload.subject,
        html: emailPayload.html,
        text: emailPayload.text,
        replyTo: String(body.customerEmail || "").trim() || undefined,
        attachments: emailPayload.attachments,
      });

      try {
        await sendCustomerConfirmationEmail(body, items, photoGroups, { submittedAt });
      } catch (customerErr) {
        console.error(
          "[consignment] customer confirmation email failed:",
          customerErr?.message || customerErr
        );
      }

      // Memory storage only — buffers released after response; no disk temp files
      return res.json({
        success: true,
        message: "Submission received successfully.",
      });
    } catch (err) {
      console.error("[consignment] submission failed:", err?.message || err);
      return res.status(500).json({
        success: false,
        error: "Submission failed. Please try again.",
      });
    }
  }
);

export default router;
