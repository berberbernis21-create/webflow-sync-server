/**
 * After a Render OOM/restart, intake-only archives still hold customer + item JSON.
 * On startup, email the team for any intake snapshot older than a short grace period.
 */
import fs from "fs";
import path from "path";
import { buildPdfFilename } from "./consignmentFilenames.js";
import { buildConsignmentEmail } from "./consignmentEmail.js";
import { isResendConfigured, sendInternalNotificationWithAttachments } from "../emailService.js";

const DATA_DIR = process.env.DATA_DIR || "./data";
const ARCHIVE_DIR = path.join(DATA_DIR, "failed-consignment-submissions");
const RECOVERY_GRACE_MS = Math.max(
  60_000,
  parseInt(process.env.CONSIGNMENT_INTAKE_RECOVERY_GRACE_MS || "120000", 10) || 120_000
);

function listArchiveJsonFiles() {
  if (!fs.existsSync(ARCHIVE_DIR)) return [];
  return fs
    .readdirSync(ARCHIVE_DIR)
    .filter((name) => name.endsWith(".json") && name !== "index.json");
}

function payloadFromArchive(filePath) {
  const raw = fs.readFileSync(filePath, "utf8");
  return JSON.parse(raw);
}

function rebuildBodyFromArchive(payload) {
  const raw = payload?.rawBody && typeof payload.rawBody === "object" ? payload.rawBody : {};
  const customer = payload?.customer || {};
  return {
    ...raw,
    customerName: customer.name ?? raw.customerName,
    customerEmail: customer.email ?? raw.customerEmail,
    customerPhone: customer.phone ?? raw.customerPhone,
    customerStreetAddress: customer.streetAddress ?? raw.customerStreetAddress,
    customerCity: customer.city ?? raw.customerCity,
    customerState: customer.state ?? raw.customerState,
    customerZip: customer.zip ?? raw.customerZip,
    preferredSubmissionType: customer.preferredSubmissionType ?? raw.preferredSubmissionType,
    sameItemLocation: customer.sameItemLocation ?? raw.sameItemLocation,
    pickupNotes: customer.pickupNotes ?? raw.pickupNotes,
    submissionCategory: customer.submissionCategory ?? raw.submissionCategory,
    source: customer.source ?? raw.source,
  };
}

function markArchiveRecovered(filePath, stage) {
  try {
    const payload = payloadFromArchive(filePath);
    payload.stage = stage;
    payload.recoveredAt = new Date().toISOString();
    fs.writeFileSync(filePath, JSON.stringify(payload, null, 2), "utf8");
  } catch (err) {
    console.error("[consignment] intake recovery mark failed:", err?.message || err);
  }
}

/**
 * @returns {Promise<number>} count of recovery emails sent
 */
export async function recoverStaleConsignmentIntakes() {
  if (!isResendConfigured()) {
    console.warn("[consignment] intake recovery skipped — Resend not configured");
    return 0;
  }

  const now = Date.now();
  let sent = 0;

  for (const filename of listArchiveJsonFiles()) {
    const filePath = path.join(ARCHIVE_DIR, filename);
    let payload;
    try {
      payload = payloadFromArchive(filePath);
    } catch {
      continue;
    }

    if (payload?.stage !== "intake" || payload?.emailSent) continue;

    const archivedMs = Date.parse(payload.archivedAt || "");
    if (!Number.isFinite(archivedMs) || now - archivedMs < RECOVERY_GRACE_MS) continue;

    const body = rebuildBodyFromArchive(payload);
    const items = Array.isArray(payload.items) ? payload.items : [];
    const submittedAt = payload.submittedAt || payload.archivedAt;
    const processingWarnings = [
      "Server restarted while this submission was processing (likely out of memory during photo conversion).",
      "Customer and item details below were recovered automatically from the intake archive.",
      "Photos were not recovered — ask the customer to resend photos if needed.",
    ];

    try {
      const emailPayload = buildConsignmentEmail({
        body,
        items,
        photoGroups: new Map(),
        originalPhotoGroups: new Map(),
        photoFailures: (payload.photoFailures || []).map((row) => ({
          ...row,
          message: row?.message || "Lost when server restarted during processing.",
        })),
        processingWarnings,
        pdfBuffer: null,
        pdfFilename: buildPdfFilename(body.customerName),
        submittedAt,
        pricingResults: null,
      });
      emailPayload.replyTo = String(body.customerEmail || "").trim() || undefined;

      await sendInternalNotificationWithAttachments({
        subject: `[Recovered] ${emailPayload.subject}`,
        html: emailPayload.html,
        text: emailPayload.text,
        replyTo: emailPayload.replyTo,
        attachments: emailPayload.attachments,
      });

      markArchiveRecovered(filePath, "recovered_after_restart");
      sent += 1;
      console.log("[consignment] intake recovery email sent", {
        file: filename,
        customerEmail: body.customerEmail || null,
        itemCount: items.length,
      });
    } catch (err) {
      console.error("[consignment] intake recovery email failed", {
        file: filename,
        message: err?.message || err,
      });
    }
  }

  if (sent) {
    console.log("[consignment] intake recovery complete", { emailsSent: sent });
  }

  return sent;
}
