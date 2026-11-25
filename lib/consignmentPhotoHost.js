/**
 * Host consignment photo buffers so Google Lens can fetch them by URL.
 * Local files: DATA_DIR/consignment-lens-photos/ (prefer a Render disk on DATA_DIR).
 * When AWS_S3_BUCKET (or FREIGHT_PDF_S3_BUCKET / CONSIGNMENT_LENS_S3_BUCKET) is set,
 * also store on S3 so Lens links survive ephemeral Render disks / redeploys.
 */
import crypto from "crypto";
import fs from "fs";
import path from "path";
import { PutObjectCommand, GetObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";

const DATA_DIR = process.env.DATA_DIR || "./data";
const PHOTO_DIR = path.join(DATA_DIR, "consignment-lens-photos");
const DEFAULT_TTL_MS = Math.max(
  24 * 60 * 60 * 1000,
  parseInt(process.env.CONSIGNMENT_LENS_PHOTO_TTL_MS || String(30 * 24 * 60 * 60 * 1000), 10) ||
    30 * 24 * 60 * 60 * 1000
);

let cachedS3 = null;

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

function s3Bucket() {
  return String(
    process.env.CONSIGNMENT_LENS_S3_BUCKET ||
      process.env.FREIGHT_PDF_S3_BUCKET ||
      process.env.AWS_S3_BUCKET ||
      ""
  ).trim();
}

function s3Region() {
  return String(process.env.AWS_REGION || process.env.AWS_DEFAULT_REGION || "us-west-2").trim();
}

function s3KeyPrefix() {
  const raw = String(process.env.CONSIGNMENT_LENS_S3_PREFIX || "consignment-lens/").trim();
  return raw.endsWith("/") ? raw : `${raw}/`;
}

function s3UrlTtlSeconds() {
  const fromMs = Math.floor(DEFAULT_TTL_MS / 1000);
  const n = parseInt(process.env.CONSIGNMENT_LENS_S3_URL_TTL_SECONDS || String(fromMs), 10);
  const capped = Number.isFinite(n) && n > 60 ? n : fromMs;
  // Presigned GET URLs commonly max out around 7 days with temporary creds.
  return Math.min(capped, 7 * 24 * 60 * 60);
}

function getS3Client() {
  if (cachedS3) return cachedS3;
  const opts = { region: s3Region() };
  const accessKeyId = String(process.env.AWS_ACCESS_KEY_ID || "").trim();
  const secretAccessKey = String(process.env.AWS_SECRET_ACCESS_KEY || "").trim();
  if (accessKeyId && secretAccessKey) {
    opts.credentials = { accessKeyId, secretAccessKey };
  }
  cachedS3 = new S3Client(opts);
  return cachedS3;
}

export function isConsignmentLensS3Configured() {
  return Boolean(s3Bucket());
}

async function uploadLensPhotoToS3({ token, buffer, ext, mimetype }) {
  const bucket = s3Bucket();
  if (!bucket) return null;
  const key = `${s3KeyPrefix()}${token}.${ext}`;
  const client = getS3Client();
  await client.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: key,
      Body: buffer,
      ContentType: mimetype || mimeFromExt(ext),
      CacheControl: "public, max-age=86400",
    })
  );
  const expiresIn = s3UrlTtlSeconds();
  const signedUrl = await getSignedUrl(
    client,
    new GetObjectCommand({ Bucket: bucket, Key: key }),
    { expiresIn }
  );
  return { key, bucket, signedUrl, expiresIn };
}

async function readLensPhotoFromS3(meta) {
  const bucket = meta?.s3Bucket || s3Bucket();
  const key = meta?.s3Key;
  if (!bucket || !key) return null;
  try {
    const out = await getS3Client().send(new GetObjectCommand({ Bucket: bucket, Key: key }));
    const bytes = await out.Body?.transformToByteArray?.();
    if (!bytes?.length) return null;
    return {
      buffer: Buffer.from(bytes),
      mimetype: meta.mimetype || out.ContentType || mimeFromExt(meta.ext || "jpg"),
      originalname: meta.originalname || `photo.${meta.ext || "jpg"}`,
    };
  } catch (err) {
    console.warn("[consignment-lens] S3 read failed", err?.message || err);
    return null;
  }
}

/** Google Lens reverse-image search for a publicly reachable image URL. */
export function buildGoogleLensUrlForImage(publicImageUrl) {
  const url = String(publicImageUrl || "").trim();
  if (!url.startsWith("http://") && !url.startsWith("https://")) return null;
  return `https://lens.google.com/uploadbyurl?url=${encodeURIComponent(url)}`;
}

/** Bing visual search — more reliable inside Outlook iOS in-app browser than Google Lens. */
export function buildBingVisualSearchUrlForImage(publicImageUrl) {
  const url = String(publicImageUrl || "").trim();
  if (!url.startsWith("http://") && !url.startsWith("https://")) return null;
  return `https://www.bing.com/images/search?view=detailv2&iss=sbi&form=SBIVSP&sbisrc=UrlPaste&q=imgurl:${encodeURIComponent(url)}`;
}

/** Force-open HTTPS URL in Safari on iOS. */
export function buildSafariHttpsUrl(httpsUrl) {
  const url = String(httpsUrl || "").trim();
  if (!url.startsWith("https://")) return null;
  return `x-safari-${url}`;
}

function attachLensUrls(file, { base, token, proxyImageUrl, lensImageUrl }) {
  file.publicImageUrl = proxyImageUrl;
  file.googleLensUrl = `${base}/api/consignment-lens/${token}`;
  file.googleLensDirectUrl = buildGoogleLensUrlForImage(lensImageUrl);
  file.lensImageUrl = lensImageUrl;
}

/**
 * Sync host (local disk only). Prefer hostConsignmentPhotosForLensAsync in production.
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
        const mimetype = file.mimetype || mimeFromExt(ext);
        const binPath = path.join(PHOTO_DIR, `${token}.${ext}`);
        const metaPath = path.join(PHOTO_DIR, `${token}.json`);
        fs.writeFileSync(binPath, file.buffer);
        const proxyImageUrl = `${base}/api/consignment-photo/${token}`;
        fs.writeFileSync(
          metaPath,
          JSON.stringify({
            token,
            ext,
            mimetype,
            originalname: file.originalname || null,
            size: file.buffer.length,
            createdAt: new Date().toISOString(),
            expiresAt,
            lensImageUrl: proxyImageUrl,
          }),
          "utf8"
        );
        attachLensUrls(file, { base, token, proxyImageUrl, lensImageUrl: proxyImageUrl });
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

/**
 * Async host: local disk + optional S3 so Lens can fetch after Render disk wipes.
 */
export async function hostConsignmentPhotosForLensAsync(photoGroups) {
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
        const mimetype = file.mimetype || mimeFromExt(ext);
        const binPath = path.join(PHOTO_DIR, `${token}.${ext}`);
        const metaPath = path.join(PHOTO_DIR, `${token}.json`);
        fs.writeFileSync(binPath, file.buffer);

        const proxyImageUrl = `${base}/api/consignment-photo/${token}`;
        let lensImageUrl = proxyImageUrl;
        let s3Meta = null;

        if (isConsignmentLensS3Configured()) {
          try {
            s3Meta = await uploadLensPhotoToS3({ token, buffer: file.buffer, ext, mimetype });
            if (s3Meta?.signedUrl) lensImageUrl = s3Meta.signedUrl;
          } catch (s3Err) {
            console.warn("[consignment-lens] S3 host failed", s3Err?.message || s3Err);
          }
        }

        fs.writeFileSync(
          metaPath,
          JSON.stringify({
            token,
            ext,
            mimetype,
            originalname: file.originalname || null,
            size: file.buffer.length,
            createdAt: new Date().toISOString(),
            expiresAt,
            s3Bucket: s3Meta?.bucket || null,
            s3Key: s3Meta?.key || null,
            lensImageUrl,
          }),
          "utf8"
        );
        attachLensUrls(file, { base, token, proxyImageUrl, lensImageUrl });
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

export function readHostedConsignmentPhotoMeta(token) {
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

  return { safe, meta, metaPath };
}

export function readHostedConsignmentPhoto(token) {
  const found = readHostedConsignmentPhotoMeta(token);
  if (!found) return null;
  const { safe, meta } = found;

  const ext = String(meta.ext || "jpg").replace(/[^a-z0-9]/gi, "") || "jpg";
  const binPath = path.join(PHOTO_DIR, `${safe}.${ext}`);
  if (!fs.existsSync(binPath)) {
    // Meta-only: still useful for Lens redirect via stored lensImageUrl.
    if (meta.lensImageUrl || meta.s3Key) {
      return {
        buffer: null,
        mimetype: meta.mimetype || mimeFromExt(ext),
        originalname: meta.originalname || `photo.${ext}`,
        lensImageUrl: meta.lensImageUrl || null,
        meta,
      };
    }
    return null;
  }

  return {
    buffer: fs.readFileSync(binPath),
    mimetype: meta.mimetype || mimeFromExt(ext),
    originalname: meta.originalname || `photo.${ext}`,
    lensImageUrl: meta.lensImageUrl || null,
    meta,
  };
}

/** Local first, then S3 when configured. */
export async function readHostedConsignmentPhotoAsync(token) {
  const local = readHostedConsignmentPhoto(token);
  if (local?.buffer?.length) return local;

  const found = readHostedConsignmentPhotoMeta(token);
  if (!found?.meta) {
    // readHostedConsignmentPhoto may have returned meta-only without buffer
    if (local?.meta) {
      const fromS3 = await readLensPhotoFromS3(local.meta);
      if (fromS3?.buffer?.length) {
        return { ...fromS3, lensImageUrl: local.lensImageUrl || local.meta.lensImageUrl || null, meta: local.meta };
      }
      return local;
    }
    return null;
  }

  const fromS3 = await readLensPhotoFromS3(found.meta);
  if (!fromS3?.buffer?.length) {
    return local; // may be meta-only with lensImageUrl
  }

  try {
    const ext = String(found.meta.ext || "jpg").replace(/[^a-z0-9]/gi, "") || "jpg";
    fs.writeFileSync(path.join(PHOTO_DIR, `${found.safe}.${ext}`), fromS3.buffer);
  } catch {
    /* ignore */
  }

  return {
    ...fromS3,
    lensImageUrl: found.meta.lensImageUrl || null,
    meta: found.meta,
  };
}

function maybeCleanupExpiredPhotos() {
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
