import { Resend } from "resend";

function getResendClient() {
  if (!process.env.RESEND_API_KEY) {
    throw new Error("Missing RESEND_API_KEY");
  }
  return new Resend(process.env.RESEND_API_KEY);
}

/** @param {string | string[]} to */
export function parseRecipients(to) {
  if (Array.isArray(to)) return to.map((e) => String(e).trim()).filter(Boolean);
  if (typeof to === "string") {
    return to.split(",").map((e) => e.trim()).filter(Boolean);
  }
  return [];
}

export function isResendConfigured() {
  return Boolean(
    process.env.RESEND_API_KEY &&
      process.env.FROM_EMAIL &&
      process.env.INTERNAL_NOTIFY_EMAIL &&
      parseRecipients(process.env.INTERNAL_NOTIFY_EMAIL).length > 0
  );
}

/**
 * @param {{ to: string | string[], subject: string, html?: string, text?: string, replyTo?: string }} opts
 */
export async function sendEmail({ to, subject, html, text, replyTo }) {
  if (!process.env.FROM_EMAIL) {
    throw new Error("Missing FROM_EMAIL");
  }
  const recipients = parseRecipients(to);
  if (recipients.length === 0) {
    throw new Error("Missing email recipient");
  }

  const resend = getResendClient();
  const payload = {
    from: process.env.FROM_EMAIL,
    to: recipients.length === 1 ? recipients[0] : recipients,
    subject,
  };
  if (html) payload.html = html;
  if (text) payload.text = text;
  if (replyTo) payload.replyTo = replyTo;

  const result = await resend.emails.send(payload);
  if (result.error) {
    throw new Error(result.error.message || "Resend send failed");
  }
  return result;
}

/**
 * Internal ops alerts (duplicate placement, Shopify write failures, weight, Google guard, SKU images).
 * Env: INTERNAL_NOTIFY_EMAIL (comma-separated allowed).
 */
export async function sendInternalNotification({ subject, html, text, replyTo }) {
  const to = process.env.INTERNAL_NOTIFY_EMAIL;
  if (!to || parseRecipients(to).length === 0) {
    throw new Error("Missing INTERNAL_NOTIFY_EMAIL");
  }
  return sendEmail({ to, subject, html, text, replyTo });
}

/**
 * Send with base64 attachments (PDF, images). Inline: set contentId and use cid: in HTML.
 * @param {{ to: string | string[], subject: string, html?: string, text?: string, replyTo?: string, attachments?: Array<{ filename: string, content: Buffer | string, contentType?: string, contentId?: string }> }} opts
 */
export async function sendEmailWithAttachments({ to, subject, html, text, replyTo, attachments = [] }) {
  if (!process.env.FROM_EMAIL) {
    throw new Error("Missing FROM_EMAIL");
  }
  const recipients = parseRecipients(to);
  if (recipients.length === 0) {
    throw new Error("Missing email recipient");
  }

  const resend = getResendClient();
  const payload = {
    from: process.env.FROM_EMAIL,
    to: recipients.length === 1 ? recipients[0] : recipients,
    subject,
  };
  if (html) payload.html = html;
  if (text) payload.text = text;
  if (replyTo) payload.replyTo = replyTo;

  if (attachments.length > 0) {
    payload.attachments = attachments.map((att) => {
      const content = Buffer.isBuffer(att.content)
        ? att.content.toString("base64")
        : String(att.content ?? "");
      const row = {
        filename: att.filename,
        content,
      };
      if (att.contentType) row.contentType = att.contentType;
      if (att.contentId) row.contentId = att.contentId;
      return row;
    });
  }

  const result = await resend.emails.send(payload);
  if (result.error) {
    throw new Error(result.error.message || "Resend send failed");
  }
  return result;
}

/** Internal consignment / ops emails with PDF + image attachments. */
export async function sendInternalNotificationWithAttachments({ subject, html, text, replyTo, attachments }) {
  const to = process.env.INTERNAL_NOTIFY_EMAIL;
  if (!to || parseRecipients(to).length === 0) {
    throw new Error("Missing INTERNAL_NOTIFY_EMAIL");
  }
  return sendEmailWithAttachments({ to, subject, html, text, replyTo, attachments });
}
