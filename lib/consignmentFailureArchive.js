/**
 * Persist consignment customer + item data when processing or email delivery fails.
 * Files live under DATA_DIR/failed-consignment-submissions/ (mount a Render disk on DATA_DIR).
 */
import fs from "fs";
import path from "path";
import { resolveItemNumber } from "./consignmentValidation.js";

const DATA_DIR = process.env.DATA_DIR || "./data";
const ARCHIVE_DIR = path.join(DATA_DIR, "failed-consignment-submissions");
const INDEX_FILE = path.join(ARCHIVE_DIR, "index.json");

function ensureArchiveDir() {
  if (!fs.existsSync(ARCHIVE_DIR)) {
    fs.mkdirSync(ARCHIVE_DIR, { recursive: true });
  }
}

function slugify(text) {
  return String(text || "unknown")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 48) || "unknown";
}

function serializePhotoGroups(photoGroups) {
  const out = {};
  for (const [itemNumber, photos] of (photoGroups || new Map()).entries()) {
    out[String(itemNumber)] = (photos || []).map((file) => ({
      originalname: file?.originalname || null,
      mimetype: file?.mimetype || null,
      size: file?.size ?? null,
      fieldname: file?.fieldname || null,
    }));
  }
  return out;
}

function loadIndex() {
  try {
    if (!fs.existsSync(INDEX_FILE)) return [];
    const raw = fs.readFileSync(INDEX_FILE, "utf8");
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function appendIndex(entry) {
  const index = loadIndex();
  index.unshift(entry);
  fs.writeFileSync(INDEX_FILE, JSON.stringify(index.slice(0, 500), null, 2), "utf8");
}

/**
 * @param {{
 *   body: object,
 *   items: object[],
 *   photoGroups?: Map<number, import('multer').File[]>,
 *   submittedAt?: string,
 *   photoFailures?: object[],
 *   processingWarnings?: string[],
 *   error?: string | null,
 *   stage?: string,
 *   pricingResults?: object[] | null,
 *   emailSent?: boolean,
 * }} record
 * @returns {string | null} saved file path
 */
export function archiveConsignmentSubmission(record) {
  try {
    ensureArchiveDir();

    const body = record?.body || {};
    const items = Array.isArray(record?.items) ? record.items : [];
    const customerName = String(body.customerName || "").trim();
    const customerEmail = String(body.customerEmail || "").trim();
    const ts = new Date().toISOString().replace(/[:.]/g, "-");
    const filename = `${ts}_${slugify(customerName || customerEmail)}.json`;
    const filePath = path.join(ARCHIVE_DIR, filename);

    const payload = {
      archivedAt: new Date().toISOString(),
      stage: record?.stage || "unknown",
      emailSent: Boolean(record?.emailSent),
      error: record?.error ? String(record.error) : null,
      submittedAt: record?.submittedAt || null,
      customer: {
        name: customerName || null,
        email: customerEmail || null,
        phone: String(body.customerPhone || "").trim() || null,
        streetAddress: body.customerStreetAddress ?? null,
        city: body.customerCity ?? null,
        state: body.customerState ?? null,
        zip: body.customerZip ?? null,
        preferredSubmissionType:
          body.preferredSubmissionType ?? body.submissionType ?? body.preferredSubmission ?? null,
        sameItemLocation: body.sameItemLocation ?? null,
        pickupNotes: body.pickupNotes ?? body.pickupLocation ?? null,
        submissionCategory: body.submissionCategory ?? null,
        source: body.source ?? null,
      },
      items: items.map((item, index) => ({
        itemNumber: resolveItemNumber(item, index),
        ...item,
      })),
      photos: serializePhotoGroups(record?.photoGroups),
      photoFailures: record?.photoFailures || [],
      processingWarnings: record?.processingWarnings || [],
      pricingSummary: Array.isArray(record?.pricingResults)
        ? record.pricingResults.map((row) => ({
            itemNumber: row?.itemNumber,
            itemName: row?.itemName,
            available: row?.available,
            reason: row?.reason,
          }))
        : null,
      rawBody: { ...body },
    };

    fs.writeFileSync(filePath, JSON.stringify(payload, null, 2), "utf8");

    appendIndex({
      archivedAt: payload.archivedAt,
      file: filename,
      stage: payload.stage,
      emailSent: payload.emailSent,
      customerName: customerName || null,
      customerEmail: customerEmail || null,
      itemCount: items.length,
      photoFailureCount: (record?.photoFailures || []).length,
      warningCount: (record?.processingWarnings || []).length,
    });

    console.log("[consignment] failure archive saved", {
      file: filename,
      stage: payload.stage,
      customerEmail: customerEmail || null,
    });

    return filePath;
  } catch (err) {
    console.error("[consignment] failure archive save failed:", err?.message || err);
    return null;
  }
}

/**
 * Remove the intake snapshot when processing completed with no issues.
 */
export function removeConsignmentIntakeArchive(filePath) {
  if (!filePath) return;
  try {
    if (fs.existsSync(filePath)) {
      fs.unlinkSync(filePath);
      console.log("[consignment] intake archive removed after successful processing");
    }
  } catch (err) {
    console.error("[consignment] intake archive cleanup failed:", err?.message || err);
  }
}

/**
 * Save when anything went wrong but we still want the customer payload on disk.
 */
export function archiveConsignmentIfNeeded({
  body,
  items,
  photoGroups,
  submittedAt,
  photoFailures = [],
  processingWarnings = [],
  error = null,
  stage = "partial",
  pricingResults = null,
  emailSent = false,
}) {
  const hasIssue =
    Boolean(error) ||
    photoFailures.length > 0 ||
    processingWarnings.length > 0;

  if (!hasIssue) return null;

  return archiveConsignmentSubmission({
    body,
    items,
    photoGroups,
    submittedAt,
    photoFailures,
    processingWarnings,
    error,
    stage,
    pricingResults,
    emailSent,
  });
}
