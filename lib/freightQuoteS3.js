/**
 * Optional AWS S3 backup for freight-quote PDFs.
 * When configured, PDFs are uploaded and a time-limited signed download URL
 * is included in the internal email (and used if Resend attachment fails).
 *
 * Env:
 *   FREIGHT_PDF_S3_BUCKET or AWS_S3_BUCKET
 *   AWS_REGION (default us-west-2)
 *   AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY (or default AWS credential chain)
 *   FREIGHT_PDF_S3_PREFIX (default freight-quotes/)
 *   FREIGHT_PDF_S3_URL_TTL_SECONDS (default 604800 = 7 days)
 */

import { PutObjectCommand, S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";

let cachedClient = null;

function bucketName() {
  return String(process.env.FREIGHT_PDF_S3_BUCKET || process.env.AWS_S3_BUCKET || "").trim();
}

function region() {
  return String(process.env.AWS_REGION || process.env.AWS_DEFAULT_REGION || "us-west-2").trim();
}

function keyPrefix() {
  const raw = String(process.env.FREIGHT_PDF_S3_PREFIX || "freight-quotes/").trim();
  return raw.endsWith("/") ? raw : `${raw}/`;
}

function urlTtlSeconds() {
  const n = parseInt(process.env.FREIGHT_PDF_S3_URL_TTL_SECONDS || "604800", 10);
  return Number.isFinite(n) && n > 60 ? n : 604800;
}

export function isFreightPdfS3Configured() {
  return Boolean(bucketName());
}

function getClient() {
  if (cachedClient) return cachedClient;
  const opts = { region: region() };
  const accessKeyId = String(process.env.AWS_ACCESS_KEY_ID || "").trim();
  const secretAccessKey = String(process.env.AWS_SECRET_ACCESS_KEY || "").trim();
  if (accessKeyId && secretAccessKey) {
    opts.credentials = { accessKeyId, secretAccessKey };
  }
  cachedClient = new S3Client(opts);
  return cachedClient;
}

function safeKeyPart(value, fallback = "item") {
  return String(value || fallback)
    .replace(/[^a-zA-Z0-9._-]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 80) || fallback;
}

/**
 * Upload PDF bytes and return a signed HTTPS download URL.
 * @returns {Promise<{ ok: true, url: string, key: string, bucket: string, expiresIn: number } | { ok: false, error: string }>}
 */
export async function uploadFreightQuotePdfToS3({
  buffer,
  filename,
  requestId = "",
} = {}) {
  const bucket = bucketName();
  if (!bucket) {
    return { ok: false, error: "s3_not_configured" };
  }
  if (!Buffer.isBuffer(buffer) || !buffer.length) {
    return { ok: false, error: "empty_pdf" };
  }

  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const idPart = safeKeyPart(requestId || stamp, stamp);
  const filePart = safeKeyPart(filename || "estimate.pdf", "estimate.pdf");
  const key = `${keyPrefix()}${idPart}/${filePart}`;
  const expiresIn = urlTtlSeconds();

  try {
    const client = getClient();
    await client.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: key,
        Body: buffer,
        ContentType: "application/pdf",
        ContentDisposition: `attachment; filename="${filePart}"`,
        Metadata: {
          requestid: String(requestId || "").slice(0, 128),
        },
      })
    );

    const url = await getSignedUrl(
      client,
      new GetObjectCommand({ Bucket: bucket, Key: key }),
      { expiresIn }
    );

    return { ok: true, url, key, bucket, expiresIn };
  } catch (err) {
    console.error("[freight-quote] S3 PDF upload failed:", err?.message || err);
    return { ok: false, error: err?.message || "s3_upload_failed" };
  }
}
