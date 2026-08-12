/**
 * Capture a Google Lens visual-results screenshot for a publicly reachable image URL.
 * Soft-fails when Chromium is unavailable or Lens blocks the session (CAPTCHA).
 *
 * On Linux (Render), prefers @sparticuz/chromium — a self-contained binary that does
 * not need apt system deps. Locally, falls back to Playwright's bundled Chromium.
 *
 * Env:
 * - CONSIGNMENT_LENS_SCREENSHOT_ENABLED — default true
 * - CONSIGNMENT_LENS_SCREENSHOT_TIMEOUT_MS — default 28000
 * - CONSIGNMENT_LENS_USE_SPARTICUZ — "true" | "false" (default: auto on linux)
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

/** Last soft-fail reason for caller diagnostics (not a thrown error). */
let lastFailureReason = null;

export function getLastLensScreenshotFailureReason() {
  return lastFailureReason;
}

function shouldPreferSparticuz() {
  const flag = String(process.env.CONSIGNMENT_LENS_USE_SPARTICUZ || "")
    .trim()
    .toLowerCase();
  if (flag === "true" || flag === "1") return true;
  if (flag === "false" || flag === "0") return false;
  return process.platform === "linux";
}

async function launchWithSparticuz() {
  const chromium = (await import("@sparticuz/chromium")).default;
  const { chromium: pwChromium } = await import("playwright-core");
  const executablePath = await chromium.executablePath();
  if (!executablePath) {
    throw new Error("sparticuz executablePath empty");
  }
  return pwChromium.launch({
    executablePath,
    headless: true,
    args: [
      ...chromium.args,
      "--disable-blink-features=AutomationControlled",
      "--window-size=1280,900",
    ],
  });
}

async function launchWithPlaywright() {
  // Prefer hermetic browsers under node_modules when unset (survives Render build→runtime).
  if (!process.env.PLAYWRIGHT_BROWSERS_PATH) {
    process.env.PLAYWRIGHT_BROWSERS_PATH = "0";
  }
  const pw = await import("playwright");
  return pw.chromium.launch({
    headless: true,
    args: [
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--disable-dev-shm-usage",
      "--disable-blink-features=AutomationControlled",
      "--window-size=1280,900",
    ],
  });
}

async function launchBrowser() {
  const preferSparticuz = shouldPreferSparticuz();
  if (preferSparticuz) {
    try {
      const browser = await launchWithSparticuz();
      console.log("[consignment-lens-shot] launched via @sparticuz/chromium");
      return browser;
    } catch (err) {
      console.warn(
        "[consignment-lens-shot] sparticuz launch failed, trying Playwright:",
        err?.message || err
      );
    }
  }

  try {
    const browser = await launchWithPlaywright();
    console.log("[consignment-lens-shot] launched via Playwright Chromium");
    return browser;
  } catch (err) {
    lastFailureReason = `Chromium launch failed: ${err?.message || err}`;
    console.warn("[consignment-lens-shot]", lastFailureReason);
    return null;
  }
}

/**
 * @param {string} publicImageUrl
 * @returns {Promise<Buffer|null>} JPEG buffer of Lens results viewport
 */
export async function captureGoogleLensResultsScreenshot(publicImageUrl) {
  lastFailureReason = null;
  if (!ENABLED) {
    lastFailureReason = "disabled by CONSIGNMENT_LENS_SCREENSHOT_ENABLED";
    return null;
  }
  const imageUrl = String(publicImageUrl || "").trim();
  if (!imageUrl.startsWith("http")) {
    lastFailureReason = "public image URL missing";
    return null;
  }

  const lensUrl = buildGoogleLensUrlForImage(imageUrl);
  if (!lensUrl) {
    lastFailureReason = "could not build Google Lens URL";
    return null;
  }

  let browser = null;
  try {
    browser = await launchBrowser();
    if (!browser) return null;

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
      lastFailureReason = "Google Lens returned a CAPTCHA/sorry page";
      console.warn("[consignment-lens-shot]", lastFailureReason);
      return null;
    }

    const shot = await page.screenshot({
      type: "jpeg",
      quality: 72,
      fullPage: false,
    });
    return Buffer.from(shot);
  } catch (err) {
    lastFailureReason = `capture failed: ${err?.message || err}`;
    console.warn("[consignment-lens-shot]", lastFailureReason);
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
  if (!ENABLED) return { captured: 0, skipped: 0, failed: 0, reason: "disabled" };

  let captured = 0;
  let skipped = 0;
  let failed = 0;
  let processed = 0;
  let reason = null;

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
        reason = getLastLensScreenshotFailureReason() || reason || "unknown";
      }
    } catch (err) {
      failed += 1;
      reason = err?.message || String(err);
      console.warn("[consignment-lens-shot] item failed:", reason);
    }
  }

  return { captured, skipped, failed, reason };
}
