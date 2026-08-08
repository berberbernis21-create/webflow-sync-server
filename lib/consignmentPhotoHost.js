/**
 * Host consignment photo buffers so Google Lens can fetch them by URL.
 * Files live under DATA_DIR/consignment-lens-photos/ (use a Render disk on DATA_DIR).
 */
import crypto from "crypto";
import fs from "fs";
import path from "path";

const DATA_DIR = process.env.DATA_DIR || "./data";
const PHOTO_DIR = path.join(DATA_DIR, "consignment-lens-photos");
const DEFAULT_TTL_MS = Math.max(
  24 * 60 * 60 * 1000,
  parseInt(process.env.CONSIGNMENT_LENS_PHOTO_TTL_MS || String(30 * 24 * 60 * 60 * 1000), 10) ||
    30 * 24 * 60 * 60 * 1000
);

function ensurePhotoDir() {
  if (!fs.existsSync(PHOTO_DIR)) {
    fs.mkdirSync(PHOTO_DIR, { recursive: true });
  }
}

export function getPublicBaseUrl() {
  const explicit = String(
    process.env.PUBLIC_BASE_URL ||
      process.env.RENDER_EXTERNAL_URL ||
      process.env.CONSIGNMENT_PUBLIC_BASE_URL ||
      ""
  )
    .trim()
    .replace(/\/$/, "");
  if (explicit) return explicit;
  const host = String(process.env.RENDER_EXTERNAL_HOSTNAME || "").trim();
  if (host) return `https://${host.replace(/^https?:\/\//, "")}`;
  return "https://webflow-sync-server.onrender.com";
}

function extForMime(mimetype = "") {
  const m = String(mimetype || "").toLowerCase();
  if (m.includes("png")) return "png";
  if (m.includes("webp")) return "webp";
  return "jpg";
}

function mimeFromExt(ext) {
  if (ext === "png") return "image/png";
  if (ext === "webp") return "image/webp";
  return "image/jpeg";
}

/** Google Lens reverse-image search for a publicly reachable image URL. */
export function buildGoogleLensUrlForImage(publicImageUrl) {
  const url = String(publicImageUrl || "").trim();
  if (!url.startsWith("http://") && !url.startsWith("https://")) return null;
  return `https://lens.google.com/uploadbyurl?url=${encodeURIComponent(url)}`;
}

/**
 * Persist each photo buffer and attach publicImageUrl + googleLensUrl on the file objects.
 * @param {Map<number, object[]>} photoGroups
 * @returns {{ hosted: number, failed: number }}
 */
export function hostConsignmentPhotosForLens(photoGroups) {
  ensurePhotoDir();
  const base = getPublicBaseUrl();
  const expiresAt = Date.now() + DEFAULT_TTL_MS;
  let hosted = 0;
  let failed = 0;

  for (const photos of (photoGroups || new Map()).values()) {
    for (const file of photos || []) {
      if (!file?.buffer?.length) {
        failed += 1;
        continue;
      }
      try {
        const token = crypto.randomBytes(24).toString("hex");
        const ext = extForMime(file.mimetype);
        const binPath = path.join(PHOTO_DIR, `${token}.${ext}`);
        const metaPath = path.join(PHOTO_DIR, `${token}.json`);
        fs.writeFileSync(binPath, file.buffer);
        fs.writeFileSync(
          metaPath,
          JSON.stringify({
            token,
            ext,
            mimetype: file.mimetype || mimeFromExt(ext),
            originalname: file.originalname || null,
            size: file.buffer.length,
            createdAt: new Date().toISOString(),
            expiresAt,
          }),
          "utf8"
        );
        const publicImageUrl = `${base}/api/consignment-photo/${token}`;
        file.publicImageUrl = publicImageUrl;
        file.googleLensUrl = buildGoogleLensUrlForImage(publicImageUrl);
        hosted += 1;
      } catch (err) {
        failed += 1;
        console.warn("[consignment-lens] host failed", err?.message || err);
      }
    }
  }

  maybeCleanupExpiredPhotos();
  return { hosted, failed };
}

export function readHostedConsignmentPhoto(token) {
  const safe = String(token || "").trim().toLowerCase();
  if (!/^[a-f0-9]{32,64}$/.test(safe)) return null;

  ensurePhotoDir();
  const metaPath = path.join(PHOTO_DIR, `${safe}.json`);
  if (!fs.existsSync(metaPath)) return null;

  let meta;
  try {
    meta = JSON.parse(fs.readFileSync(metaPath, "utf8"));
  } catch {
    return null;
  }

  if (meta?.expiresAt && Number(meta.expiresAt) < Date.now()) {
    try {
      fs.unlinkSync(metaPath);
      const ext = meta.ext || "jpg";
      const bin = path.join(PHOTO_DIR, `${safe}.${ext}`);
      if (fs.existsSync(bin)) fs.unlinkSync(bin);
    } catch {
      /* ignore */
    }
    return null;
  }

  const ext = String(meta.ext || "jpg").replace(/[^a-z0-9]/gi, "") || "jpg";
  const binPath = path.join(PHOTO_DIR, `${safe}.${ext}`);
  if (!fs.existsSync(binPath)) return null;

  return {
    buffer: fs.readFileSync(binPath),
    mimetype: meta.mimetype || mimeFromExt(ext),
    originalname: meta.originalname || `photo.${ext}`,
  };
}

function maybeCleanupExpiredPhotos() {
  // Light opportunistic cleanup (skip most calls).
  if (Math.random() > 0.08) return;
  try {
    ensurePhotoDir();
    const now = Date.now();
    for (const name of fs.readdirSync(PHOTO_DIR)) {
      if (!name.endsWith(".json")) continue;
      const metaPath = path.join(PHOTO_DIR, name);
      let meta;
      try {
        meta = JSON.parse(fs.readFileSync(metaPath, "utf8"));
      } catch {
        continue;
      }
      if (!meta?.expiresAt || Number(meta.expiresAt) >= now) continue;
      const token = name.replace(/\.json$/, "");
      const ext = meta.ext || "jpg";
      try {
        fs.unlinkSync(metaPath);
      } catch {
        /* ignore */
      }
      try {
        fs.unlinkSync(path.join(PHOTO_DIR, `${token}.${ext}`));
      } catch {
        /* ignore */
      }
    }
  } catch {
    /* ignore */
  }
}
