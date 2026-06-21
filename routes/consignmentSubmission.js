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
import { applyConsignmentCorsHeaders } from "../lib/consignmentCors.js";
import { resolveConsignmentBrand } from "../lib/consignmentBrand.js";
import { MAX_CONSIGNMENT_PHOTOS, MAX_UPLOAD_FILES } from "../lib/consignmentLimits.js";
import { preparePhotoGroupsForConsignment } from "../lib/consignmentImageNormalize.js";
import {
  archiveConsignmentIfNeeded,
  archiveConsignmentSubmission,
  removeConsignmentIntakeArchive,
} from "../lib/consignmentFailureArchive.js";
import {
  groupPhotosByItemNumber,
  validateConsignmentSubmission,
} from "../lib/consignmentValidation.js";

const router = express.Router();
const BACKGROUND_MAX_ATTEMPTS = 2;
const EMAIL_SEND_MAX_ATTEMPTS = 3;

router.use((req, res, next) => {
  applyConsignmentCorsHeaders(req, res);
  next();
});

const MAX_FILE_SIZE_BYTES = 10 * 1024 * 1024;
const MAX_FILES = MAX_UPLOAD_FILES;
const MAX_TOTAL_UPLOAD_BYTES = MAX_FILE_SIZE_BYTES * MAX_FILES;

const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: MAX_FILE_SIZE_BYTES,
    files: MAX_FILES,
    fieldSize: 2 * 1024 * 1024,
    fields: 64,
    parts: MAX_FILES + 64,
  },
});

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function rejectOversizedUpload(req, res, next) {
  const contentLength = parseInt(req.headers["content-length"] || "0", 10);
  if (Number.isFinite(contentLength) && contentLength > MAX_TOTAL_UPLOAD_BYTES) {
    return res.status(413).json({
      success: false,
      error: "Upload is too large. Please use fewer or smaller photos (10 MB max each).",
    });
  }
  next();
}

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
    return `You can upload at most ${MAX_CONSIGNMENT_PHOTOS} photos per submission (10 items max). Please remove extra photos or submit in a second request.`;
  }
  if (err.code === "LIMIT_PART_COUNT") {
    return "Too many form fields in this submission.";
  }
  return "Invalid file upload.";
}

async function generateInternalPdfSafe({ body, items, photoGroups, submittedAt, pricingResults }) {
  try {
    return await generateConsignmentPdf({
      body,
      items,
      photoGroups,
      submittedAt,
      pricingResults,
    });
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
    const pricing = await analyzeConsignmentItemsPricingWithBudget({
      items,
      photoGroups,
      background: true,
    });
    const pricingResults = pricing.results;
    const pricingModelsUsed = pricing.modelsUsed || [];

    if (pricing.skipped) {
      console.warn("[consignment] pricing disabled via CONSIGNMENT_PRICING_ENABLED");
    } else if (pricing.timedOut) {
      console.warn("[consignment] pricing budget exceeded — email will note partial comps", {
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

    return {
      pricingResults,
      pricingModelsUsed,
      pricingTimedOut: Boolean(pricing.timedOut),
      pricingConfigured: Boolean(pricing.configured),
      pricingSkipped: Boolean(pricing.skipped),
    };
  } catch (pricingErr) {
    console.error(
      "[consignment] pricing analysis failed (continuing):",
      pricingErr?.message || pricingErr
    );
    return {
      pricingResults: null,
      pricingModelsUsed: [],
      pricingTimedOut: false,
      pricingConfigured: false,
      pricingSkipped: false,
      pricingError: pricingErr?.message || String(pricingErr),
    };
  }
}

async function sendInternalEmailWithRetry(emailPayload) {
  let lastErr;
  for (let attempt = 1; attempt <= EMAIL_SEND_MAX_ATTEMPTS; attempt++) {
    try {
      await sendInternalNotificationWithAttachments({
        subject: emailPayload.subject,
        html: emailPayload.html,
        text: emailPayload.text,
        replyTo: emailPayload.replyTo,
        attachments: emailPayload.attachments,
      });
      if (attempt > 1) {
        console.log("[consignment] internal email sent after retry", { attempt });
      }
      return;
    } catch (err) {
      lastErr = err;
      console.error(
        `[consignment] internal email send failed (attempt ${attempt}/${EMAIL_SEND_MAX_ATTEMPTS}):`,
        err?.message || err
      );
      if (attempt < EMAIL_SEND_MAX_ATTEMPTS) {
        await sleep(2000 * attempt);
      }
    }
  }
  throw lastErr;
}

/**
 * PDF, pricing, and emails run after the HTTP response so Render/browser timeouts
 * (often ~30s) do not surface as net::ERR_FAILED on the Webflow form.
 */
async function processConsignmentSubmission({ body, items, photoGroups, submittedAt }) {
  const startedMs = Date.now();
  const brandKey = resolveConsignmentBrand(body, items);
  const uploadedPhotoCount = [...photoGroups.values()].reduce((n, p) => n + p.length, 0);

  const intakeArchivePath = archiveConsignmentSubmission({
    body,
    items,
    photoGroups,
    submittedAt,
    stage: "intake",
    emailSent: false,
  });

  console.log("[consignment] processing submission", {
    brand: brandKey,
    source: body?.source || null,
    submissionCategory: body?.submissionCategory || null,
    itemCount: items.length,
    photoCount: uploadedPhotoCount,
  });

  const processingWarnings = [];
  let preparedPhotoGroups = photoGroups;
  let photoFailures = [];

  try {
    const prepared = await preparePhotoGroupsForConsignment(photoGroups);
    preparedPhotoGroups = prepared.photoGroups;
    photoFailures = prepared.failures || [];
    if (photoFailures.length) {
      console.warn("[consignment] photo normalization failures", {
        failed: photoFailures.length,
        uploaded: uploadedPhotoCount,
      });
    }
  } catch (normalizeErr) {
    const message = normalizeErr?.message || String(normalizeErr);
    processingWarnings.push(`Photo normalization failed: ${message}`);
    console.error("[consignment] photo normalization failed (continuing without images):", message);
    preparedPhotoGroups = new Map();
    for (const [itemNumber, photos] of photoGroups.entries()) {
      for (const file of photos || []) {
        photoFailures.push({
          itemNumber,
          originalname: String(file?.originalname || "photo"),
          mimetype: String(file?.mimetype || "unknown"),
          size: Number(file?.size) || 0,
          message,
        });
      }
    }
  }

  const pricing = await runPricingSafe({ items, photoGroups: preparedPhotoGroups });
  const { pricingResults } = pricing;
  if (pricing.pricingError) {
    processingWarnings.push(`Pricing analysis failed: ${pricing.pricingError}`);
  } else if (pricing.pricingSkipped) {
    processingWarnings.push("Pricing analysis was disabled (CONSIGNMENT_PRICING_ENABLED).");
  } else if (!pricing.pricingConfigured) {
    processingWarnings.push("Pricing analysis skipped — API keys not fully configured.");
  } else if (pricing.pricingTimedOut) {
    processingWarnings.push("Pricing analysis timed out — comps may be partial or missing.");
  }

  const pdfBuffer = await generateInternalPdfSafe({
    body,
    items,
    photoGroups: preparedPhotoGroups,
    submittedAt,
    pricingResults,
  });

  if (!pdfBuffer?.length) {
    processingWarnings.push("Internal PDF was not generated.");
  }

  const archiveContext = () => ({
    body,
    items,
    photoGroups,
    submittedAt,
    photoFailures,
    processingWarnings: [...processingWarnings],
    pricingResults,
  });

  const savedArchivePath = archiveConsignmentIfNeeded({
    ...archiveContext(),
    stage: "pre_email",
    emailSent: false,
  });
  if (savedArchivePath) {
    processingWarnings.push(
      "Customer and item details were saved to the server failure archive."
    );
  }

  const pdfFilename = buildPdfFilename(body.customerName);
  const emailPayload = buildConsignmentEmail({
    body,
    items,
    photoGroups: preparedPhotoGroups,
    originalPhotoGroups: photoGroups,
    photoFailures,
    processingWarnings,
    pdfBuffer,
    pdfFilename,
    submittedAt,
    pricingResults,
  });
  emailPayload.replyTo = String(body.customerEmail || "").trim() || undefined;

  let emailSent = false;
  try {
    await sendInternalEmailWithRetry(emailPayload);
    emailSent = true;
  } catch (emailErr) {
    archiveConsignmentSubmission({
      ...archiveContext(),
      stage: "email_failed",
      error: emailErr?.message || String(emailErr),
      emailSent: false,
    });
    throw emailErr;
  }

  try {
    await sendCustomerConfirmationEmail(body, items, preparedPhotoGroups, { submittedAt });
  } catch (customerErr) {
    console.error(
      "[consignment] customer confirmation email failed:",
      customerErr?.message || customerErr
    );
    processingWarnings.push(
      `Customer confirmation email failed: ${customerErr?.message || customerErr}`
    );
    archiveConsignmentIfNeeded({
      ...archiveContext(),
      stage: "customer_email_failed",
      emailSent: true,
    });
  }

  if (emailSent) {
    archiveConsignmentIfNeeded({
      ...archiveContext(),
      stage: "delivered_with_issues",
      emailSent: true,
    });
  }

  if (emailSent && !photoFailures.length && !processingWarnings.length) {
    removeConsignmentIntakeArchive(intakeArchivePath);
  }

  console.log("[consignment] background processing complete", {
    ms: Date.now() - startedMs,
    itemCount: items.length,
    photoCount: uploadedPhotoCount,
    photosAttached: [...preparedPhotoGroups.values()].reduce((n, p) => n + p.length, 0),
    photoFailures: photoFailures.length,
    warnings: processingWarnings.length,
  });
}

async function processConsignmentSubmissionWithRetry(args, attempt = 1) {
  try {
    await processConsignmentSubmission(args);
  } catch (err) {
    console.error(
      `[consignment] background processing failed (attempt ${attempt}/${BACKGROUND_MAX_ATTEMPTS}):`,
      err?.message || err
    );
    if (attempt < BACKGROUND_MAX_ATTEMPTS) {
      await sleep(5000 * attempt);
      return processConsignmentSubmissionWithRetry(args, attempt + 1);
    }

    archiveConsignmentSubmission({
      body: args.body,
      items: args.items,
      photoGroups: args.photoGroups,
      submittedAt: args.submittedAt,
      photoFailures: [...args.photoGroups.entries()].flatMap(([itemNumber, photos]) =>
        (photos || []).map((file) => ({
          itemNumber,
          originalname: String(file?.originalname || "photo"),
          mimetype: String(file?.mimetype || "unknown"),
          size: Number(file?.size) || 0,
          message: "Processing failed before photos could be converted.",
        }))
      ),
      processingWarnings: [
        `Submission processing failed after ${BACKGROUND_MAX_ATTEMPTS} attempts.`,
      ],
      error: err?.message || String(err),
      stage: "processing_failed",
      emailSent: false,
      pricingResults: null,
    });

    try {
      const fallbackPayload = buildConsignmentEmail({
        body: args.body,
        items: args.items,
        photoGroups: new Map(),
        originalPhotoGroups: args.photoGroups,
        photoFailures: [...args.photoGroups.entries()].flatMap(([itemNumber, photos]) =>
          (photos || []).map((file) => ({
            itemNumber,
            originalname: String(file?.originalname || "photo"),
            mimetype: String(file?.mimetype || "unknown"),
            size: Number(file?.size) || 0,
            message: "Background processing failed before images could be attached.",
          }))
        ),
        processingWarnings: [
          `Submission processing failed after ${BACKGROUND_MAX_ATTEMPTS} attempts: ${err?.message || err}`,
          "Form data is included below. Photos were not attached.",
        ],
        pdfBuffer: null,
        pdfFilename: buildPdfFilename(args.body?.customerName),
        submittedAt: args.submittedAt,
        pricingResults: null,
      });
      fallbackPayload.replyTo = String(args.body?.customerEmail || "").trim() || undefined;
      await sendInternalEmailWithRetry(fallbackPayload);
      console.log("[consignment] fallback internal email sent after processing failure");
    } catch (fallbackErr) {
      console.error(
        "[consignment] fallback internal email failed:",
        fallbackErr?.message || fallbackErr
      );
      archiveConsignmentSubmission({
        body: args.body,
        items: args.items,
        photoGroups: args.photoGroups,
        submittedAt: args.submittedAt,
        photoFailures: [...args.photoGroups.entries()].flatMap(([itemNumber, photos]) =>
          (photos || []).map((file) => ({
            itemNumber,
            originalname: String(file?.originalname || "photo"),
            mimetype: String(file?.mimetype || "unknown"),
            size: Number(file?.size) || 0,
            message: "Processing and fallback email both failed.",
          }))
        ),
        processingWarnings: [
          "Internal email failed after processing failure.",
          "Fallback email also failed.",
        ],
        error: fallbackErr?.message || String(fallbackErr),
        stage: "email_and_fallback_failed",
        emailSent: false,
        pricingResults: null,
      });
    }
  }
}

router.options("/consignment-submission", (req, res) => {
  applyConsignmentCorsHeaders(req, res);
  res.sendStatus(204);
});

/**
 * POST /api/consignment-submission
 * multipart/form-data from Webflow; photos grouped by item_N_photos field names.
 */
router.post(
  "/consignment-submission",
  rejectOversizedUpload,
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

      void processConsignmentSubmissionWithRetry({ body, items, photoGroups, submittedAt });
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
