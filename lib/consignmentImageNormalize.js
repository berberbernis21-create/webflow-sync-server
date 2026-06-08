/**
 * Normalize consignment upload buffers to JPEG for PDFKit embedding.
 * iPhone HEIC/HEIF and some WebP uploads pass multer but PDFKit cannot embed them raw.
 */
import sharp from "sharp";

function isJpeg(buffer) {
  return buffer?.length >= 2 && buffer[0] === 0xff && buffer[1] === 0xd8;
}

function isPng(buffer) {
  return buffer?.length >= 8 && buffer[0] === 0x89 && buffer[1] === 0x50 && buffer[2] === 0x4e && buffer[3] === 0x47;
}

/**
 * @param {Buffer} buffer
 * @param {string} [mimetype]
 * @returns {Promise<Buffer>}
 */
export async function normalizePhotoBufferForPdf(buffer, mimetype = "") {
  if (!buffer?.length) {
    throw new Error("Photo buffer is empty.");
  }

  const mime = String(mimetype || "").toLowerCase();
  const likelyHeic =
    mime.includes("heic") ||
    mime.includes("heif") ||
    mime.includes("avif") ||
    (buffer.length >= 12 && buffer.toString("ascii", 4, 8) === "ftyp" && /heic|heix|hevc|mif1/i.test(buffer.toString("ascii", 8, 16)));

  if (!likelyHeic && (isJpeg(buffer) || isPng(buffer))) {
    return buffer;
  }

  return sharp(buffer, { failOn: "none" })
    .rotate()
    .jpeg({ quality: 88, mozjpeg: true })
    .toBuffer();
}

/**
 * @param {Map<number, import('multer').File[]>} photoGroups
 * @returns {Promise<Map<number, import('multer').File[]>>}
 */
export async function preparePhotoGroupsForPdf(photoGroups) {
  const prepared = new Map();

  for (const [itemNumber, photos] of photoGroups.entries()) {
    const next = [];
    for (const file of photos || []) {
      try {
        const buffer = await normalizePhotoBufferForPdf(file.buffer, file.mimetype);
        next.push({
          ...file,
          buffer,
          mimetype: "image/jpeg",
          originalMimetype: file.mimetype,
        });
      } catch (err) {
        console.warn("[consignment] pdf photo normalize failed", {
          itemNumber,
          originalname: file?.originalname,
          mimetype: file?.mimetype,
          size: file?.size,
          message: err?.message || String(err),
        });
        next.push(file);
      }
    }
    prepared.set(itemNumber, next);
  }

  return prepared;
}
