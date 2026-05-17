/** Sanitize for attachment/PDF filenames: no slashes, safe chars, spaces → hyphens. */
export function sanitizeFilenamePart(value, fallback = "Unknown") {
  const raw = value == null || String(value).trim() === "" ? fallback : String(value).trim();
  return raw
    .replace(/[/\\?%*:|"<>]/g, "")
    .replace(/\s+/g, "-")
    .replace(/[^a-zA-Z0-9._-]/g, "")
    .replace(/-+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 80) || fallback;
}

export function consignorLabel(customerName) {
  const name = customerName == null ? "" : String(customerName).trim();
  return name ? sanitizeFilenamePart(name, "UnknownConsignor") : "UnknownConsignor";
}

/** ConsignorName_Item-1_ItemName_Photo-1.jpg */
export function buildPhotoFilename({ consignorName, itemNumber, itemName, photoIndex, mimetype }) {
  const consignor = consignorLabel(consignorName);
  const itemNum = sanitizeFilenamePart(`Item-${itemNumber}`, `Item-${itemNumber}`);
  const itemPart = sanitizeFilenamePart(itemName || "Unnamed", "Unnamed");
  const ext =
    mimetype === "image/png"
      ? "png"
      : mimetype === "image/webp"
        ? "webp"
        : mimetype === "image/gif"
          ? "gif"
          : "jpg";
  return `${consignor}_${itemNum}_${itemPart}_Photo-${photoIndex}.${ext}`;
}

export function buildPdfFilename(consignorName) {
  const consignor = consignorLabel(consignorName);
  const stamp = new Date().toISOString().slice(0, 10);
  return `${consignor}_Consignment-Submission_${stamp}.pdf`;
}

/** Customer submission summary PDF attached to confirmation email. */
export function buildCustomerPdfFilename(consignorName, date = new Date()) {
  const consignor = consignorLabel(consignorName);
  const stamp = date.toISOString().slice(0, 10);
  return `${consignor}_Submission-Summary_${stamp}.pdf`;
}
