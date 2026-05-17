import express from "express";
import multer from "multer";
import { isResendConfigured, sendInternalNotificationWithAttachments } from "../emailService.js";
import { buildPdfFilename } from "../lib/consignmentFilenames.js";
import { buildConsignmentEmail, sendCustomerConfirmationEmail } from "../lib/consignmentEmail.js";
import {
  analyzeConsignmentItemsPricingWithBudget,
  getPricingConfigStatus,
} from "../lib/consignmentPricingAnalysis.js";
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

async function generateInternalPdfSafe({ body, items, photoGroups, submittedAt }) {
  try {
    return await generateConsignmentPdf({ body, items, photoGroups, submittedAt });
  } catch (pdfErr) {
    console.error(
      "[consignment] internal PDF generation failed (continuing):",
      pdfErr?.message || pdfErr
    );
    return null;
  }
}

async function runPricingSafe({ items, photoGroups }) {
  try {
    const pricing = await analyzeConsignmentItemsPricingWithBudget({ items, photoGroups });
    const pricingResults = pricing.results;
    const pricingModelsUsed = pricing.modelsUsed || [];

    if (pricing.skipped) {
      console.warn("[consignment] pricing disabled via CONSIGNMENT_PRICING_ENABLED");
    } else if (pricing.timedOut) {
      console.warn("[consignment] pricing budget exceeded — internal email sent without full comps", {
        itemCount: items.length,
      });
    } else if (!pricing.configured) {
      console.warn("[consignment] pricing skipped — not fully configured", {
        config: pricing.configStatus || getPricingConfigStatus(),
      });
    } else if (pricingResults?.length) {
      console.log("[consignment] pricing analysis complete", {
        items: pricingResults.length,
        models: pricingModelsUsed,
        available: pricingResults.filter((r) => r.available).length,
        timedOut: Boolean(pricing.timedOut),
        reasons: pricingResults
          .filter((r) => !r.available)
          .map((r) => ({ item: r.itemNumber, reason: r.reason })),
      });
    }

    return { pricingResults, pricingModelsUsed };
  } catch (pricingErr) {
    console.error(
      "[consignment] pricing analysis failed (continuing):",
      pricingErr?.message || pricingErr
    );
    return { pricingResults: null, pricingModelsUsed: [] };
  }
}

/**
 * PDF, pricing, and emails run after the HTTP response so Render/browser timeouts
 * (often ~30s) do not surface as net::ERR_FAILED on the Webflow form.
 */
async function processConsignmentSubmission({ body, items, photoGroups, submittedAt }) {
  const startedMs = Date.now();

  const [pdfBuffer, { pricingResults }] = await Promise.all([
    generateInternalPdfSafe({ body, items, photoGroups, submittedAt }),
    runPricingSafe({ items, photoGroups }),
  ]);

  const pdfFilename = buildPdfFilename(body.customerName);

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

  console.log("[consignment] background processing complete", {
    ms: Date.now() - startedMs,
    itemCount: items.length,
    photoCount: [...photoGroups.values()].reduce((n, p) => n + p.length, 0),
  });
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

      const photoGroups = groupPhotosByItemNumber(files);

      const validation = validateConsignmentSubmission(body, photoGroups);
      if (!validation.ok) {
        return res.status(400).json({ success: false, error: validation.error });
      }

      const items = validation.items;
      const submittedAt = formatSubmittedAt();

      res.json({
        success: true,
        message: "Submission received successfully.",
      });

      void processConsignmentSubmission({ body, items, photoGroups, submittedAt }).catch((err) => {
        console.error("[consignment] background processing failed:", err?.message || err);
      });
    } catch (err) {
      console.error("[consignment] submission failed:", err?.message || err);
      if (!res.headersSent) {
        return res.status(500).json({
          success: false,
          error: "Submission failed. Please try again.",
        });
      }
    }
  }
);

export default router;
