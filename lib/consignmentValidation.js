import {
  MAX_CONSIGNMENT_ITEMS,
  MAX_CONSIGNMENT_PHOTOS,
  SUBMIT_FORM_CLIENT_GUIDANCE,
} from "./consignmentLimits.js";

/** Blank optional fields → "Not provided" in email/PDF output. */
export function displayValue(value) {
  if (value === null || value === undefined || String(value).trim() === "") {
    return "Not provided";
  }
  return String(value).trim();
}

/** Step 1 dropdown: Consignment vs Buy-out option (Webflow: preferredSubmissionType). */
export function resolvePreferredSubmissionType(body) {
  const raw =
    body?.preferredSubmissionType ??
    body?.submissionType ??
    body?.preferredSubmission ??
    body?.submissionPreference;
  return displayValue(raw);
}

export function isTermsAcknowledged(value) {
  if (value === true || value === 1) return true;
  const s = String(value ?? "")
    .trim()
    .toLowerCase();
  return s === "true" || s === "on" || s === "1";
}

export function parseItemsJson(raw) {
  if (raw == null || String(raw).trim() === "") {
    return { ok: false, error: "At least one item is required." };
  }
  let parsed;
  try {
    parsed = typeof raw === "string" ? JSON.parse(raw) : raw;
  } catch {
    return { ok: false, error: "Invalid items data." };
  }
  if (!Array.isArray(parsed) || parsed.length === 0) {
    return { ok: false, error: "At least one item is required." };
  }
  return { ok: true, items: parsed };
}

/** Resolve 1-based item number from JSON item or array index. */
export function resolveItemNumber(item, index) {
  const n = parseInt(item?.itemNumber, 10);
  if (Number.isFinite(n) && n > 0) return n;
  return index + 1;
}

/**
 * Group multer files by item number from field names `item_N_photos`.
 * Webflow sends one array field per item (e.g. item_1_photos, item_2_photos).
 */
export function groupPhotosByItemNumber(files) {
  const groups = new Map();
  for (const file of files || []) {
    const match = /^item_(\d+)_photos$/i.exec(String(file.fieldname || ""));
    if (!match) continue;
    const itemNumber = parseInt(match[1], 10);
    if (!Number.isFinite(itemNumber) || itemNumber < 1) continue;
    if (!groups.has(itemNumber)) groups.set(itemNumber, []);
    groups.get(itemNumber).push(file);
  }
  return groups;
}

export function countSubmissionPhotos(photoGroups) {
  let total = 0;
  for (const photos of photoGroups.values()) total += photos.length;
  return total;
}

export function formatDimensions(item) {
  const w = displayValue(item?.width);
  const d = displayValue(item?.depth);
  const h = displayValue(item?.height);
  if (w === "Not provided" && d === "Not provided" && h === "Not provided") {
    return "Not provided";
  }
  const fmt = (v) => (v === "Not provided" ? "—" : v);
  return `${fmt(w)} × ${fmt(d)} × ${fmt(h)}`;
}

/**
 * Required: customerName, customerEmail, customerPhone, terms, ≥1 item,
 * each item itemName/category/condition, ≥1 photo per item number.
 */
export function validateConsignmentSubmission(body, photoGroups) {
  const customerName = body?.customerName;
  const customerEmail = body?.customerEmail;
  const customerPhone = body?.customerPhone;

  if (!customerName || String(customerName).trim() === "") {
    return { ok: false, error: "Customer name is required." };
  }
  if (!customerEmail || String(customerEmail).trim() === "") {
    return { ok: false, error: "Customer email is required." };
  }
  if (!customerPhone || String(customerPhone).trim() === "") {
    return { ok: false, error: "Customer phone is required." };
  }
  if (!isTermsAcknowledged(body?.termsAcknowledged)) {
    return { ok: false, error: "You must acknowledge the terms to submit." };
  }

  const itemsResult = parseItemsJson(body?.items);
  if (!itemsResult.ok) return itemsResult;

  const items = itemsResult.items;
  if (items.length > MAX_CONSIGNMENT_ITEMS) {
    return {
      ok: false,
      error: SUBMIT_FORM_CLIENT_GUIDANCE.overItemLimit,
    };
  }

  const totalPhotos = countSubmissionPhotos(photoGroups);
  if (totalPhotos > MAX_CONSIGNMENT_PHOTOS) {
    return {
      ok: false,
      error: SUBMIT_FORM_CLIENT_GUIDANCE.overPhotoLimit,
    };
  }

  for (let i = 0; i < items.length; i++) {
    const item = items[i] || {};
    if (!item.itemName || String(item.itemName).trim() === "") {
      return { ok: false, error: `Item ${i + 1}: name is required.` };
    }
    if (!item.category || String(item.category).trim() === "") {
      return { ok: false, error: `Item ${i + 1}: category is required.` };
    }
    if (!item.condition || String(item.condition).trim() === "") {
      return { ok: false, error: `Item ${i + 1}: condition is required.` };
    }
    const itemNumber = resolveItemNumber(item, i);
    const photos = photoGroups.get(itemNumber) || [];
    if (photos.length === 0) {
      return {
        ok: false,
        error: `Item ${itemNumber}: at least one photo is required.`,
      };
    }
  }

  return { ok: true, items };
}
