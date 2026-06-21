/**
 * Normalize consignment upload buffers to JPEG for Vision, OpenAI, and PDFKit.
 * iPhone HEIC/HEIF uploads pass multer but most APIs and PDFKit cannot use them raw.
 * Render's sharp build often lacks libheif — heic-convert is the primary HEIC path.
 */
import sharp from "sharp";

let heicConvertLoader = null;

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

async function convertHeicToJpeg(buffer) {
  const convert = await loadHeicConvert();
  if (!convert) {
    throw new Error("HEIC decoder not available on server.");
  }
  const out = await convert({
    buffer,
    format: "JPEG",
    quality: 0.88,
  });
  return Buffer.from(out);
}

async function convertWithSharp(buffer) {
  return sharp(buffer, { failOn: "none" })
    .rotate()
    .jpeg({ quality: 88, mozjpeg: true })
    .toBuffer();
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

  if (!isLikelyHeic(buffer, mimetype) && (isJpegBuffer(buffer) || isPngBuffer(buffer))) {
    return isPngBuffer(buffer) ? convertWithSharp(buffer) : buffer;
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

  for (const [itemNumber, photos] of photoGroups.entries()) {
    const next = [];
    for (const file of photos || []) {
      if (file?.consignmentPhotoNormalized && isJpegBuffer(file.buffer)) {
        next.push(file);
        continue;
      }

      try {
        const buffer = await normalizePhotoBufferForPdf(file.buffer, file.mimetype);
        next.push(normalizedMulterFile(file, buffer));
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
