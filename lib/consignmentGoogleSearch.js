/**
 * Google Lens buttons for consignment photos (image search only).
 * Requires each photo to be hosted with file.googleLensUrl / file.publicImageUrl
 * (see hostConsignmentPhotosForLens).
 */

function escapeHtml(str) {
  return String(str ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

/**
 * Compact Lens-style control for HTML email, next to a photo filename.
 * Uses the hosted photo URL → Google Lens reverse image search.
 */
export function buildGoogleItemSearchButtonHtml(_item, { photoIndex = 1, lensUrl = null } = {}) {
  const url = String(lensUrl || "").trim();
  if (!url.startsWith("http://") && !url.startsWith("https://")) {
    return `<span style="font-size:11px;color:#999;">Lens link unavailable</span>`;
  }
  const label = photoIndex > 1 ? "Google Lens this photo" : "Google Lens this item";

  return [
    `<a href="${escapeHtml(url)}" target="_blank" rel="noopener noreferrer" `,
    `style="display:inline-block;padding:5px 12px;font-family:Arial,Helvetica,sans-serif;font-size:12px;font-weight:700;`,
    `color:#1a73e8;text-decoration:none;background:#fff;border:1px solid #dadce0;border-radius:16px;`,
    `box-shadow:0 1px 2px rgba(60,64,67,0.12);line-height:1.2;" `,
    `title="Search this photo with Google Lens">`,
    `<span style="display:inline-block;width:10px;height:10px;border-radius:50%;margin-right:6px;`,
    `background:conic-gradient(#4285f4 0 25%,#ea4335 0 50%,#fbbc05 0 75%,#34a853 0);vertical-align:-1px;"></span>`,
    `${escapeHtml(label)}</a>`,
  ].join("");
}

/**
 * Draw a clickable Google Lens link under a PDF photo (image search only).
 */
export function drawPdfGoogleItemSearchLinks(doc, _item, { margin, contentWidth, photoIndex = 1, lensUrl = null } = {}) {
  const url = String(lensUrl || "").trim();
  const label = photoIndex > 1 ? "Google Lens this photo →" : "Google Lens this item →";

  if (!url.startsWith("http://") && !url.startsWith("https://")) {
    doc.font("Helvetica").fontSize(8).fillColor("#999").text("Google Lens link unavailable", margin, doc.y, {
      width: contentWidth,
    });
    doc.moveDown(0.2);
    return;
  }

  doc.font("Helvetica-Bold").fontSize(9).fillColor("#1a73e8");
  doc.text(label, margin, doc.y, {
    width: contentWidth,
    link: url,
    underline: true,
  });
  doc.moveDown(0.2);
}
