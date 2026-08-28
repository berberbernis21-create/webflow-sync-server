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
import {
  MAX_CONSIGNMENT_PHOTOS,
  MAX_PRICING_ITEMS,
  MAX_UPLOAD_FILES,
  isHeavyConsignmentSubmission,
} from "../lib/consignmentLimits.js";
import { preparePhotoGroupsForConsignment } from "../lib/consignmentImageNormalize.js";
import { expandConfidentMultiPieceItems } from "../lib/consignmentMultiPiece.js";
import { hostConsignmentPhotosForLens, readHostedConsignmentPhoto, buildGoogleLensUrlForImage, buildBingVisualSearchUrlForImage, buildSafariHttpsUrl, getPublicBaseUrl } from "../lib/consignmentPhotoHost.js";
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
  const heavySubmission = isHeavyConsignmentSubmission(items.length, uploadedPhotoCount);

  if (heavySubmission) {
    processingWarnings.push(
      `Large submission (${items.length} items, ${uploadedPhotoCount} photos): AI pricing and PDF generation were skipped to keep the server stable. Photos are still attached in this email when conversion succeeds.`
    );
    console.warn("[consignment] heavy submission — skipping pricing and PDFs", {
      itemCount: items.length,
      photoCount: uploadedPhotoCount,
    });
  }

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

  // When confident a form line is multiple distinct pieces, split for internal analysis only.
  const expanded = expandConfidentMultiPieceItems(items, preparedPhotoGroups, {
    maxItems: MAX_PRICING_ITEMS,
  });
  const analysisItems = expanded.items;
  const analysisPhotoGroups = expanded.photoGroups;
  if (expanded.splitCount > 0) {
    processingWarnings.push(...expanded.notes);
    console.log("[consignment] split multi-piece submission lines", {
      originalItems: items.length,
      analysisItems: analysisItems.length,
      splitLines: expanded.splitCount,
    });
  }

  // Host photos publicly so Google Lens buttons can reverse-search the actual image.
  try {
    const lensHost = hostConsignmentPhotosForLens(analysisPhotoGroups);
    console.log("[consignment] hosted photos for Google Lens", lensHost);
    if (lensHost.hosted === 0 && lensHost.failed > 0) {
      processingWarnings.push("Google Lens photo links could not be created for this submission.");
    }
  } catch (lensErr) {
    processingWarnings.push(`Google Lens photo hosting failed: ${lensErr?.message || lensErr}`);
    console.warn("[consignment] lens photo host failed", lensErr?.message || lensErr);
  }

  let pricingResults = null;
  if (!heavySubmission) {
    const pricing = await runPricingSafe({
      items: analysisItems,
      photoGroups: analysisPhotoGroups,
    });
    pricingResults = pricing.pricingResults;
    if (pricing.pricingError) {
      processingWarnings.push(`Pricing analysis failed: ${pricing.pricingError}`);
    } else if (pricing.pricingSkipped) {
      processingWarnings.push("Pricing analysis was disabled (CONSIGNMENT_PRICING_ENABLED).");
    } else if (!pricing.pricingConfigured) {
      processingWarnings.push("Pricing analysis skipped — API keys not fully configured.");
    } else if (pricing.pricingTimedOut) {
      processingWarnings.push("Pricing analysis timed out — comps may be partial or missing.");
    }
  }

  let pdfBuffer = null;
  if (!heavySubmission) {
    pdfBuffer = await generateInternalPdfSafe({
      body,
      items: analysisItems,
      photoGroups: analysisPhotoGroups,
      submittedAt,
      pricingResults,
    });

    if (!pdfBuffer?.length) {
      processingWarnings.push("Internal PDF was not generated.");
    }
  } else {
    processingWarnings.push("Internal PDF skipped for large submission.");
  }

  const archiveContext = () => ({
    body,
    items,
    analysisItems,
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
    items: analysisItems,
    photoGroups: analysisPhotoGroups,
    originalPhotoGroups: photoGroups,
    originalItemCount: items.length,
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
    await sendCustomerConfirmationEmail(body, items, preparedPhotoGroups, {
      submittedAt,
      skipPdf: heavySubmission,
    });
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
 * GET /api/consignment-photo/:token
 * Public image host for Google Lens reverse-image search (unguessable token).
 */
router.get("/consignment-photo/:token", (req, res) => {
  const photo = readHostedConsignmentPhoto(req.params.token);
  if (!photo?.buffer?.length) {
    return res.status(404).type("text").send("Photo not found or expired.");
  }
  res.setHeader("Content-Type", photo.mimetype || "image/jpeg");
  res.setHeader("Cache-Control", "public, max-age=86400");
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Cross-Origin-Resource-Policy", "cross-origin");
  return res.status(200).send(photo.buffer);
});

/**
 * GET /api/consignment-lens/:token
 * Short launch URL for email/PDF → visual search.
 * Desktop: immediate 302 to Google Lens (working path).
 * Mobile: bridge page — Outlook in-app browser cannot finish Google Lens, so we offer
 * Bing visual search (works there) + Open Lens in Safari.
 */
router.get("/consignment-lens/:token", (req, res) => {
  const token = String(req.params.token || "").trim();
  const photo = readHostedConsignmentPhoto(token);
  if (!photo?.buffer?.length) {
    return res.status(404).type("text").send("Photo not found or expired.");
  }
  const imageUrl = `${getPublicBaseUrl()}/api/consignment-photo/${token}`;
  const lensUrl = buildGoogleLensUrlForImage(imageUrl);
  const bingUrl = buildBingVisualSearchUrlForImage(imageUrl);
  const safariLensUrl = buildSafariHttpsUrl(lensUrl);
  if (!lensUrl) {
    return res.status(500).type("text").send("Could not build Google Lens link.");
  }

  const ua = String(req.get("user-agent") || "");
  const isMobile = /iPhone|iPad|iPod|Android|Mobile/i.test(ua);
  const isOutlook = /Outlook|MSAppHost|Word|Office/i.test(ua);
  const wantBridge = String(req.query.bridge || "") === "1" || isMobile;

  // Default / desktop: go straight to Lens (matches the path that works in Chrome).
  if (!wantBridge) {
    res.setHeader("Cache-Control", "no-store");
    return res.redirect(302, lensUrl);
  }

  const safeImage = escapeHtmlAttr(imageUrl);
  const safeLens = escapeHtmlAttr(lensUrl);
  const safeBing = escapeHtmlAttr(bingUrl || "");
  const safeSafariLens = escapeHtmlAttr(safariLensUrl || lensUrl);

  const outlookNote = isOutlook
    ? `<p style="background:#fff4ed;border-left:3px solid #e04f16;padding:10px 12px;text-align:left;font-size:14px;"><strong>Outlook on iPhone:</strong> Google Lens usually shows the photo but returns blank results in this browser. Use Bing below (works here), or open Lens in Safari.</p>`
    : `<p>Google Lens often returns blank results inside email apps. Use Bing below, or open Lens in Safari.</p>`;

  res.setHeader("Cache-Control", "no-store");
  res.setHeader("Content-Type", "text/html; charset=utf-8");
  return res.status(200).send(`<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Search this photo</title>
  <style>
    body{margin:0;font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Arial,sans-serif;background:#f6f2ea;color:#111;}
    .wrap{max-width:440px;margin:0 auto;padding:28px 18px 40px;text-align:center;}
    img{max-width:100%;height:auto;border:1px solid #ddd;border-radius:10px;background:#fff;}
    h1{font-size:22px;margin:18px 0 8px;}
    p{font-size:15px;line-height:1.5;color:#333;margin:0 0 14px;}
    .btn{display:block;width:100%;box-sizing:border-box;padding:14px 16px;margin:10px 0;border-radius:10px;font-size:16px;font-weight:700;text-decoration:none;}
    .primary{background:#00809d;color:#fff;}
    .secondary{background:#1a73e8;color:#fff;}
    .tertiary{background:#fff;color:#111;border:1px solid #ccc;}
    .tip{font-size:13px;color:#666;margin-top:16px;text-align:left;}
  </style>
</head>
<body>
  <div class="wrap">
    <img src="${safeImage}" alt="Consignment photo" />
    <h1>Search this photo</h1>
    ${outlookNote}
    ${
      bingUrl
        ? `<a class="btn primary" href="${safeBing}" rel="noopener noreferrer">Search similar images (works in Outlook)</a>`
        : ""
    }
    <a class="btn secondary" href="${safeSafariLens}" rel="noopener noreferrer">Open Google Lens in Safari</a>
    <a class="btn tertiary" href="${safeLens}" rel="noopener noreferrer">Try Google Lens here</a>
    <a class="btn tertiary" href="${safeImage}" rel="noopener noreferrer">Open photo only</a>
    <p class="tip"><strong>Why:</strong> Desktop Chrome can finish Google Lens. Outlook’s built-in browser usually cannot — you’ll see the furniture photo with no matches. Bing visual search works in Outlook; Safari works for Google Lens.</p>
  </div>
</body>
</html>`);
});

function escapeHtmlAttr(str) {
  return String(str ?? "")
    .replace(/&/g, "&amp;")
    .replace(/"/g, "&quot;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

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
