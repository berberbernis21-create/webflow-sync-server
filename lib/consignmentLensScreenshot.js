/**
 * Capture a Google Lens visual-results screenshot for a publicly reachable image URL.
 * Soft-fails when Playwright/Chromium is unavailable (common on bare hosts).
 *
 * Env:
 * - CONSIGNMENT_LENS_SCREENSHOT_ENABLED — default true
 * - CONSIGNMENT_LENS_SCREENSHOT_TIMEOUT_MS — default 28000
 */
import { buildGoogleLensUrlForImage } from "./consignmentPhotoHost.js";

const ENABLED =
  String(process.env.CONSIGNMENT_LENS_SCREENSHOT_ENABLED || "true").toLowerCase() !== "false";
const TIMEOUT_MS = Math.max(
  8000,
  Math.min(
    60000,
    parseInt(process.env.CONSIGNMENT_LENS_SCREENSHOT_TIMEOUT_MS || "28000", 10) || 28000
  )
);

async function loadPlaywrightChromium() {
  try {
    const pw = await import("playwright");
    return pw.chromium;
  } catch (err) {
    console.warn("[consignment-lens-shot] playwright not installed:", err?.message || err);
    return null;
  }
}

/**
 * @param {string} publicImageUrl
 * @returns {Promise<Buffer|null>} PNG/JPEG buffer of Lens results viewport
 */
export async function captureGoogleLensResultsScreenshot(publicImageUrl) {
  if (!ENABLED) return null;
  const imageUrl = String(publicImageUrl || "").trim();
  if (!imageUrl.startsWith("http")) return null;

  const lensUrl = buildGoogleLensUrlForImage(imageUrl);
  if (!lensUrl) return null;

  const chromium = await loadPlaywrightChromium();
  if (!chromium) return null;

  let browser = null;
  try {
    browser = await chromium.launch({
      headless: true,
      args: [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",
        "--disable-blink-features=AutomationControlled",
        "--window-size=1280,900",
      ],
    });

    const context = await browser.newContext({
      viewport: { width: 1280, height: 900 },
      userAgent:
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
      locale: "en-US",
      timezoneId: "America/Phoenix",
    });
    const page = await context.newPage();
    page.setDefaultTimeout(TIMEOUT_MS);

    await page.goto(lensUrl, { waitUntil: "domcontentloaded", timeout: TIMEOUT_MS });

    // Wait for visual matches / results UI (selectors change; use several fallbacks).
    const selectors = [
      'div[data-id="visual-matches"]',
      "a[href*='http'] img",
      'div[role="main"] img',
      "c-wiz img",
    ];
    let found = false;
    for (const sel of selectors) {
      try {
        await page.waitForSelector(sel, { timeout: Math.min(12000, TIMEOUT_MS) });
        found = true;
        break;
      } catch {
        /* try next */
      }
    }
    // Give lazy images a moment even if selector was soft.
    await new Promise((r) => setTimeout(r, found ? 2200 : 3500));

    if (/\/sorry\//i.test(page.url())) {
      console.warn("[consignment-lens-shot] captcha/sorry page — skipping screenshot");
      return null;
    }

    const shot = await page.screenshot({
      type: "jpeg",
      quality: 72,
      fullPage: false,
    });
    return Buffer.from(shot);
  } catch (err) {
    console.warn("[consignment-lens-shot] capture failed:", err?.message || err);
    return null;
  } finally {
    try {
      await browser?.close();
    } catch {
      /* ignore */
    }
  }
}

/**
 * For each item, screenshot Lens results for the first photo that has a publicImageUrl.
 * Writes file.lensResultsScreenshot (Buffer) when successful.
 */
export async function captureLensScreenshotsForPhotoGroups(photoGroups, { maxItems = 8 } = {}) {
  if (!ENABLED) return { captured: 0, skipped: 0, failed: 0 };

  let captured = 0;
  let skipped = 0;
  let failed = 0;
  let processed = 0;

  for (const photos of (photoGroups || new Map()).values()) {
    if (processed >= maxItems) {
      skipped += 1;
      continue;
    }
    const first = (photos || []).find((f) => f?.publicImageUrl && f?.buffer?.length);
    if (!first?.publicImageUrl) {
      skipped += 1;
      continue;
    }
    processed += 1;
    try {
      const shot = await captureGoogleLensResultsScreenshot(first.publicImageUrl);
      if (shot?.length) {
        first.lensResultsScreenshot = shot;
        captured += 1;
      } else {
        failed += 1;
      }
    } catch (err) {
      failed += 1;
      console.warn("[consignment-lens-shot] item failed:", err?.message || err);
    }
  }

  return { captured, skipped, failed };
}
