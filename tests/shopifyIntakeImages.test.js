import test from "node:test";
import assert from "node:assert/strict";
import {
  formatShopifyErrorBody,
  namesEqualFull,
  normalizeCallerImageUrls,
  normalizeFullItemName,
  parseIntakeItemIdFromText,
  prepareShopifyAttachment,
  shopifyProductHasDisplayImages,
  shopifyTitlePollIntervalMs,
  shopifyTitlePollMaxDelayMs,
  sniffImageKind,
} from "../lib/shopifyIntakeImages.js";

test("normalizeFullItemName collapses whitespace", () => {
  assert.equal(normalizeFullItemName("  Foo   Bar  "), "Foo Bar");
  assert.equal(namesEqualFull("Foo Bar", "foo  bar"), true);
  assert.equal(namesEqualFull("Chair IV", "Chair V"), false);
});

test("parseIntakeItemIdFromText finds item.html id", () => {
  const id = parseIntakeItemIdFromText(
    "https://lostfound-pricing-api.onrender.com/lost-and-found-intake/item.html?id=aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
  );
  assert.equal(id, "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
});

test("shopifyProductHasDisplayImages requires src", () => {
  assert.equal(shopifyProductHasDisplayImages({ images: [] }), false);
  assert.equal(shopifyProductHasDisplayImages({ images: [{ id: 1 }] }), false);
  assert.equal(
    shopifyProductHasDisplayImages({ images: [{ src: "https://cdn.example/a.jpg" }] }),
    true
  );
});

test("normalizeCallerImageUrls absolutizes and dedupes", () => {
  process.env.INTAKE_API_BASE = "https://lostfound-pricing-api.onrender.com";
  const urls = normalizeCallerImageUrls([
    "/api/lost-and-found-intake/media/view/abc/tok?v=full",
    "/api/lost-and-found-intake/media/view/abc/tok?v=full",
    "https://cdn.example/b.jpg",
    "",
  ]);
  assert.deepEqual(urls, [
    "https://lostfound-pricing-api.onrender.com/api/lost-and-found-intake/media/view/abc/tok?v=full",
    "https://cdn.example/b.jpg",
  ]);
});

test("formatShopifyErrorBody stringifies nested Shopify errors", () => {
  assert.equal(
    formatShopifyErrorBody({ image: ["Could not download image"] }),
    '{"image":["Could not download image"]}'
  );
  assert.equal(formatShopifyErrorBody("plain"), "plain");
  assert.equal(formatShopifyErrorBody(null), "");
});

test("sniffImageKind detects jpeg/png/webp", () => {
  assert.equal(sniffImageKind(Buffer.from([0xff, 0xd8, 0xff, 0xe0])), "jpeg");
  assert.equal(
    sniffImageKind(Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a])),
    "png"
  );
  const webp = Buffer.alloc(12);
  webp.write("RIFF", 0);
  webp.write("WEBP", 8);
  assert.equal(sniffImageKind(webp), "webp");
});

test("prepareShopifyAttachment converts webp to jpeg for Shopify REST", async () => {
  // 1x1 lossless WebP
  const webp = Buffer.from(
    "UklGRiQAAABXRUJQVlA4IBgAAAAwAQCdASoBAAEAAwA0JaQAA3AA/vuUAAA=",
    "base64"
  );
  const prepared = await prepareShopifyAttachment(webp);
  assert.equal(prepared.filename, "intake.jpg");
  assert.equal(sniffImageKind(prepared.buffer), "jpeg");
  assert.equal(prepared.converted, true);
});

test("shopify title poll defaults to ~5s steady cadence", () => {
  const prevInterval = process.env.SHOPIFY_TITLE_POLL_INTERVAL_MS;
  const prevAlias = process.env.SHOPIFY_TITLE_POLL_MS;
  const prevMax = process.env.SHOPIFY_TITLE_POLL_MAX_DELAY_MS;
  try {
    delete process.env.SHOPIFY_TITLE_POLL_INTERVAL_MS;
    delete process.env.SHOPIFY_TITLE_POLL_MS;
    delete process.env.SHOPIFY_TITLE_POLL_MAX_DELAY_MS;
    assert.equal(shopifyTitlePollIntervalMs(), 5000);
    assert.equal(shopifyTitlePollMaxDelayMs(5000), 5000);

    process.env.SHOPIFY_TITLE_POLL_MS = "5000";
    assert.equal(shopifyTitlePollIntervalMs(), 5000);

    process.env.SHOPIFY_TITLE_POLL_INTERVAL_MS = "4000";
    assert.equal(shopifyTitlePollIntervalMs(), 4000);
    process.env.SHOPIFY_TITLE_POLL_MAX_DELAY_MS = "12000";
    assert.equal(shopifyTitlePollMaxDelayMs(4000), 12000);
  } finally {
    if (prevInterval == null) delete process.env.SHOPIFY_TITLE_POLL_INTERVAL_MS;
    else process.env.SHOPIFY_TITLE_POLL_INTERVAL_MS = prevInterval;
    if (prevAlias == null) delete process.env.SHOPIFY_TITLE_POLL_MS;
    else process.env.SHOPIFY_TITLE_POLL_MS = prevAlias;
    if (prevMax == null) delete process.env.SHOPIFY_TITLE_POLL_MAX_DELAY_MS;
    else process.env.SHOPIFY_TITLE_POLL_MAX_DELAY_MS = prevMax;
  }
});
