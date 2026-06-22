/**
 * Normalize consignment upload buffers to JPEG for Vision, OpenAI, and PDFKit.
 * iPhone HEIC/HEIF uploads pass multer but most APIs and PDFKit cannot use them raw.
 * Render's sharp build often lacks libheif — heic-convert is the primary HEIC path.
 *
 * All outputs are downscaled (default max 1280px) so 20+ iPhone photos do not OOM the server.
 */
import sharp from "sharp";

let heicConvertLoader = null;

const MAX_IMAGE_EDGE = Math.min(
  2048,
  Math.max(800, parseInt(process.env.CONSIGNMENT_MAX_IMAGE_EDGE || "1280", 10) || 1280)
);

async function loadHeicConvert() {
  if (heicConvertLoader !== null) return heicConvertLoader;
  try {
    const mod = await import("heic-convert");
    heicConvertLoader = mod.default || mod;
  } catch (err) {
    console.warn("[consignment] heic-convert unavailable:", err?.message || err);
    heicConvertLoader = false;
  }
  return heicConvertLoader;
}

export function isJpegBuffer(buffer) {
  return buffer?.length >= 2 && buffer[0] === 0xff && buffer[1] === 0xd8;
}

export function isPngBuffer(buffer) {
  return (
    buffer?.length >= 8 &&
    buffer[0] === 0x89 &&
    buffer[1] === 0x50 &&
    buffer[2] === 0x4e &&
    buffer[3] === 0x47
  );
}

export function isLikelyHeic(buffer, mimetype = "") {
  const mime = String(mimetype || "").toLowerCase();
  if (mime.includes("heic") || mime.includes("heif") || mime.includes("avif")) return true;
  return (
    buffer?.length >= 12 &&
    buffer.toString("ascii", 4, 8) === "ftyp" &&
    /heic|heix|hevc|mif1|avif/i.test(buffer.toString("ascii", 8, 16))
  );
}

async function resizeConsignmentBuffer(buffer) {
  return sharp(buffer, { failOn: "none" })
    .rotate()
    .resize({
      width: MAX_IMAGE_EDGE,
      height: MAX_IMAGE_EDGE,
      fit: "inside",
      withoutEnlargement: true,
    })
    .jpeg({ quality: 82, mozjpeg: true })
    .toBuffer();
}

async function convertHeicToJpeg(buffer) {
  const convert = await loadHeicConvert();
  if (!convert) {
    throw new Error("HEIC decoder not available on server.");
  }
  const out = await convert({
    buffer,
    format: "JPEG",
    quality: 0.82,
  });
  return resizeConsignmentBuffer(Buffer.from(out));
}

async function convertWithSharp(buffer) {
  return resizeConsignmentBuffer(buffer);
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

  if (!isLikelyHeic(buffer, mimetype) && isJpegBuffer(buffer)) {
    return resizeConsignmentBuffer(buffer);
  }

  if (!isLikelyHeic(buffer, mimetype) && isPngBuffer(buffer)) {
    return convertWithSharp(buffer);
  }

  if (isLikelyHeic(buffer, mimetype)) {
    try {
      return await convertHeicToJpeg(buffer);
    } catch (heicErr) {
      console.warn("[consignment] heic-convert failed, trying sharp:", heicErr?.message || heicErr);
    }
  }

  return convertWithSharp(buffer);
}

function normalizedMulterFile(file, buffer) {
  return {
    ...file,
    buffer,
    mimetype: "image/jpeg",
    originalMimetype: file.mimetype,
    consignmentPhotoNormalized: true,
  };
}

/**
 * @param {Map<number, import('multer').File[]>} photoGroups
 * @returns {Promise<{ photoGroups: Map<number, import('multer').File[]>, failures: Array<{ itemNumber: number, originalname: string, mimetype: string, size: number, message: string }> }>}
 */
export async function preparePhotoGroupsForConsignment(photoGroups) {
  const prepared = new Map();
  const failures = [];
  let processed = 0;
  const total = [...photoGroups.values()].reduce((n, photos) => n + (photos?.length || 0), 0);

  for (const [itemNumber, photos] of photoGroups.entries()) {
    const next = [];
    for (const file of photos || []) {
      processed += 1;
      if (file?.consignmentPhotoNormalized && isJpegBuffer(file.buffer)) {
        next.push(file);
        continue;
      }

      try {
        const buffer = await normalizePhotoBufferForPdf(file.buffer, file.mimetype);
        next.push(normalizedMulterFile(file, buffer));
        if (file.buffer) file.buffer = null;
        console.log("[consignment] photo normalized", {
          itemNumber,
          index: processed,
          total,
          originalname: file?.originalname || null,
          bytes: buffer.length,
        });
      } catch (err) {
        const failure = {
          itemNumber,
          originalname: String(file?.originalname || "photo"),
          mimetype: String(file?.mimetype || "unknown"),
          size: Number(file?.size) || 0,
          message: err?.message || String(err),
        };
        failures.push(failure);
        console.warn("[consignment] photo normalize failed", failure);
      }
    }
    prepared.set(itemNumber, next);
  }

  return { photoGroups: prepared, failures };
}

/**
 * @param {Map<number, import('multer').File[]>} photoGroups
 * @returns {Promise<Map<number, import('multer').File[]>>}
 */
export async function preparePhotoGroupsForPdf(photoGroups) {
  const { photoGroups: prepared } = await preparePhotoGroupsForConsignment(photoGroups);
  return prepared;
}
