/**
 * Shared caps for consignment submissions (handbags + furniture).
 * Every item in a submission is stored in PDFs/emails for team review.
 * Override on Render: CONSIGNMENT_MAX_ITEMS, CONSIGNMENT_MAX_PHOTOS.
 */

export const MAX_CONSIGNMENT_ITEMS = Math.max(
  1,
  Math.min(10, parseInt(process.env.CONSIGNMENT_MAX_ITEMS || "10", 10) || 10)
);

export const MAX_CONSIGNMENT_PHOTOS = Math.max(
  1,
  Math.min(30, parseInt(process.env.CONSIGNMENT_MAX_PHOTOS || "30", 10) || 30)
);

/** Multer file cap — keep in sync with MAX_CONSIGNMENT_PHOTOS (+ optional documents). */
export const MAX_UPLOAD_FILES = Math.max(
  MAX_CONSIGNMENT_PHOTOS,
  Math.min(40, parseInt(process.env.CONSIGNMENT_MAX_UPLOAD_FILES || "30", 10) || 30)
);

export const MAX_PRICING_ITEMS = MAX_CONSIGNMENT_ITEMS;

/** Large submissions skip AI pricing + PDF generation to avoid Render OOM (10 items / 23 HEIC photos). */
export const HEAVY_CONSIGNMENT_MIN_PHOTOS = Math.max(
  12,
  parseInt(process.env.CONSIGNMENT_HEAVY_PHOTO_COUNT || "16", 10) || 16
);

export const HEAVY_CONSIGNMENT_MIN_ITEMS = Math.max(
  6,
  parseInt(process.env.CONSIGNMENT_HEAVY_ITEM_COUNT || "8", 10) || 8
);

export function isHeavyConsignmentSubmission(itemCount, photoCount) {
  const items = Number(itemCount) || 0;
  const photos = Number(photoCount) || 0;
  return photos >= HEAVY_CONSIGNMENT_MIN_PHOTOS || items >= HEAVY_CONSIGNMENT_MIN_ITEMS;
}

/** Client-facing copy for Webflow submit pages (handbags + furniture). */
export const SUBMIT_FORM_CLIENT_GUIDANCE = {
  limitsTitle: "Submission limits",
  limitsSummary: "Up to 10 items and 30 photos per submission.",
  limitsDetail:
    "Add one item at a time and tap Save before adding the next. Each submission can include up to 10 saved items and 30 photos total across all items (about 3 photos per item on average).",
  overItemLimit:
    "You can save up to 10 items in this submission. Please submit now, then start a new submission on the same page for any additional items.",
  overPhotoLimit:
    "This submission can include up to 30 photos total. Please remove some photos or submit your first 10 items, then send another submission for the rest.",
  moreThanTenItems:
    "If you have more than 10 items, complete and submit this form first, then open the submission page again for your next group (up to 10 items and 30 photos each time).",
  saveBeforeContinue:
    "Each item must be saved before you continue. Every saved item—including photos and details—is included in what our team receives for review.",
  photosTip:
    "Clear photos help us review faster. Include front, back, details, wear, labels, and any documentation you have.",
  reviewNote:
    `Submitting does not guarantee acceptance. Our team reviews every item and photo you send and will follow up promptly.`,
};
