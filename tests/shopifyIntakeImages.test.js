import test from "node:test";
import assert from "node:assert/strict";
import sharp from "sharp";
import {
  classifyShopifyBodyForIntake,
  composeShopifyBodyHtml,
  deriveListingSeoFields,
  formatShopifyDimsLine,
  hammingDistance64,
  imageAverageHash,
  isCustomerConditionListingNote,
  isDimsOnlyListingText,
  isInternalOpsListingNote,
  formatShopifyErrorBody,
  mergeShopifyListingTags,
  namesEqualFull,
  normalizeCallerImageUrls,
  normalizeFullItemName,
  parseIntakeItemIdFromText,
  prepareShopifyAttachment,
  resolveIntakeShopifyTaxonomy,
  shopifyProductHasDisplayImages,
  shopifyTitlePollIntervalMs,
  shopifyTitlePollMaxDelayMs,
  sniffImageKind,
  stripListingDimJunk,
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

test("imageAverageHash is stable and hammingDistance64 matches identical hashes", async () => {
  // Patterned images — solid colors collapse to the same aHash (all pixels == avg).
  const left = Buffer.alloc(32 * 32 * 3, 30);
  for (let y = 0; y < 32; y++) {
    for (let x = 0; x < 16; x++) {
      const i = (y * 32 + x) * 3;
      left[i] = 220;
      left[i + 1] = 220;
      left[i + 2] = 220;
    }
  }
  const right = Buffer.alloc(32 * 32 * 3, 30);
  for (let y = 0; y < 32; y++) {
    for (let x = 16; x < 32; x++) {
      const i = (y * 32 + x) * 3;
      right[i] = 220;
      right[i + 1] = 220;
      right[i + 2] = 220;
    }
  }
  const png = await sharp(left, { raw: { width: 32, height: 32, channels: 3 } })
    .png()
    .toBuffer();
  const other = await sharp(right, { raw: { width: 32, height: 32, channels: 3 } })
    .png()
    .toBuffer();
  const a = await imageAverageHash(png);
  const b = await imageAverageHash(png);
  assert.equal(hammingDistance64(a, b), 0);
  const c = await imageAverageHash(other);
  assert.ok(hammingDistance64(a, c) > 0);
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

test("stripListingDimJunk removes Measured + mid-copy measuring + old footers", () => {
  const messy =
    "Comfortable loveseat for a bedroom sitting nook. Measuring 40 H x 65 W x 29 D inches, it is an ideal apartment-size loveseat.\n\nDimensions: Width: 40\" × Depth: 65\" × Height: 120\".\nWeight: 99 lb.";
  const cleaned = stripListingDimJunk(messy);
  assert.match(cleaned, /Comfortable loveseat/i);
  assert.match(cleaned, /ideal apartment-size loveseat/i);
  assert.equal(/\bmeasur(?:ing|es)/i.test(cleaned), false);
  assert.equal(/Dimensions:/i.test(cleaned), false);
  assert.equal(/Weight:\s*\d+/i.test(cleaned), false);
  assert.equal(/40 H x 65 W/i.test(cleaned), false);
});

test("formatShopifyDimsLine puts Weight under Dimensions", () => {
  const line = formatShopifyDimsLine({ width: 16, depth: 13, height: 25, weightLbs: 13 });
  assert.equal(
    line,
    'Dimensions: Width: 16" × Depth: 13" × Height: 25".\nWeight: 13 lb.'
  );
  assert.equal(formatShopifyDimsLine({ width: 65.0, depth: 29.0, height: 40.0, weightLbs: 29.0 }),
    'Dimensions: Width: 65" × Depth: 29" × Height: 40".\nWeight: 29 lb.');
});


test("warehouse Shopify notes are internal ops; damage notes are customer condition", () => {
  const warehouse = "Box In Warehouse- tall box that says DITZ";
  assert.equal(isInternalOpsListingNote(warehouse), true);
  assert.equal(isCustomerConditionListingNote(warehouse), false);
  const helper =
    "This Arctic Explorer map print has a bold cartographic look and warm paper tone for a study wall.";
  const ops = classifyShopifyBodyForIntake(warehouse, helper);
  assert.equal(ops.kind, "internal_ops");
  assert.equal(ops.applyDescription, false);
  assert.equal(ops.applyNotes, false);
  assert.equal(ops.restoreListing, true);

  const damage = "Small scratch on lower left corner; otherwise solid.";
  assert.equal(isInternalOpsListingNote(damage), false);
  assert.equal(isCustomerConditionListingNote(damage), true);
  const cond = classifyShopifyBodyForIntake(damage, helper);
  assert.equal(cond.kind, "customer_condition");
  assert.equal(cond.applyDescription, false);
  assert.equal(cond.applyNotes, true);
  assert.equal(cond.restoreListing, true);
});

test("composeShopifyBodyHtml never returns dims-only or weight-only", () => {
  assert.equal(composeShopifyBodyHtml("", { width: 10, depth: 8, height: 3.5, weightLbs: 8 }), "");
  assert.equal(composeShopifyBodyHtml("   ", { weightLbs: 9 }), "");
  assert.equal(isDimsOnlyListingText(""), true);
  assert.equal(isDimsOnlyListingText('Dimensions: Width: 10" × Depth: 8" × Height: 3.5". Weight: 8 lb.'), true);
  const html = composeShopifyBodyHtml(
    "This vintage wood box has a small drawer and warm patina for a console or bookshelf.",
    { width: 10, depth: 8, height: 3.5, weightLbs: 8 }
  );
  assert.match(html, /vintage wood box/i);
  assert.match(html, /Dimensions:/i);
  assert.equal((html.match(/Dimensions:/gi) || []).length, 1);
});

test("deriveListingSeoFields builds SEO title/meta plus AEO/GEO", () => {
  const fields = deriveListingSeoFields({
    name: "Tuscany Fortunata Red Glazed Pottery Pitcher- 13X13X16",
    description:
      "This red glazed pottery pitcher from Tuscany Fortunata has a bold silhouette and classic handles. Ideal for a dining table or kitchen display.",
    brand: "Tuscany Fortunata",
    category: "Accessories",
    dims: { width: 13, depth: 13, height: 16, weightLbs: 12 },
  });
  assert.match(fields.seoTitle, /Tuscany Fortunata/i);
  assert.ok(fields.seoDescription.length <= 160);
  assert.match(fields.aeoDescription, /pitcher|pottery|glazed/i);
  assert.match(fields.geoDescription, /Tuscany Fortunata|Accessories|Dimensions/i);
});

test("resolveIntakeShopifyTaxonomy prefers Website Categories Bedroom over Traxia BED/Accessories", () => {
  const bed = resolveIntakeShopifyTaxonomy({
    merchType: "furniture",
    websiteCategory: "Bedroom",
    category: "BED",
    name: "RH Curved Wing Grey Leather King Low Profile Bed",
  });
  assert.equal(bed.department, "Furniture & Home");
  assert.equal(bed.category, "Bedroom");
  assert.equal(bed.websiteCode, "B");

  const bedCode = resolveIntakeShopifyTaxonomy({
    merchType: "furniture",
    websiteCategory: "B",
    category: "ACCESSORIES",
    name: "RH Graydon Shagreen Cal-King Bed",
  });
  assert.equal(bedCode.category, "Bedroom");

  const consoleTable = resolveIntakeShopifyTaxonomy({
    merchType: "furniture",
    websiteCategory: "Living Room",
    category: "CONSOLE",
    name: "Restoration Hardware Balustrade Salvaged Wood Ebonized Console Table",
  });
  assert.equal(consoleTable.category, "Living Room");

  const fromTraxiaBed = resolveIntakeShopifyTaxonomy({
    merchType: "furniture",
    websiteCategory: "",
    category: "BED",
    name: "King Bed",
  });
  assert.equal(fromTraxiaBed.category, "Bedroom");

  const fromName = resolveIntakeShopifyTaxonomy({
    merchType: "furniture",
    websiteCategory: "",
    category: "ACCESSORIES",
    name: "RH Graydon Shagreen Cal-King Bed W/ 2 Bedside Tables",
  });
  assert.equal(fromName.category, "Bedroom");
});

test("mergeShopifyListingTags writes FH+B for Bedroom website category", () => {
  const tags = mergeShopifyListingTags({
    existingTags: ["Category: ACCESSORIES", "Used", "ACTIVE", "X", "Accessories"],
    ecommerceTags: "FH, X, graydon-bed",
    merchType: "furniture",
    websiteCategory: "Bedroom",
    dims: { width: 80, depth: 91, height: 37.5 },
  });
  assert.ok(tags.includes("FH"));
  assert.ok(tags.includes("B"));
  assert.equal(tags.includes("X"), false);
  assert.equal(tags.some((t) => /^Accessories$/i.test(t)), false);
  assert.ok(tags.includes("Category: ACCESSORIES"));
  assert.ok(tags.includes("graydon-bed"));
});

test("composeShopifyBodyHtml strips prior footers then appends one clean block", () => {
  const html = composeShopifyBodyHtml(
    'A red glazed pottery pitcher. Measuring 16 H x 13 W x 13 D inches, bold silhouette.\n\nDimensions: Width: 16" × Depth: 13" × Height: 25". Weight: 40 lb.\n\nDimensions: Width: 16" × Depth: 13" × Height: 25".',
    { width: 16, depth: 13, height: 25, weightLbs: 13 }
  );
  assert.match(html, /red glazed pottery pitcher/i);
  assert.equal(/Measuring 16 H/i.test(html), false);
  assert.equal((html.match(/Dimensions:/gi) || []).length, 1);
  assert.match(html, /Dimensions: Width: 16&quot; × Depth: 13&quot; × Height: 25&quot;\./);
  assert.match(html, /Weight: 13 lb\./);
  assert.equal(/Dimensions:[\s\S]*Weight:/i.test(html.replace(/<br\s*\/?>/gi, "\n")), true);
  const tags = mergeShopifyListingTags({
    existingTags: ["Width: 40", "Height: 120", "Depth: 65", "Used", "ACTIVE", "X", "Accessories", "Furniture & Home"],
    ecommerceTags: ["stressless-loveseat", "paloma-leather"],
    merchType: "furniture",
    websiteCategory: "Living Room",
    dims: { width: 16, depth: 13, height: 25, weightLbs: 13 },
  });
  assert.ok(tags.includes("FH"));
  assert.ok(tags.includes("L"));
  assert.equal(tags.includes("X"), false);
  assert.equal(tags.some((t) => /^Accessories$/i.test(t)), false);
  assert.equal(tags.some((t) => /^Furniture & Home$/i.test(t)), false);
  assert.ok(tags.includes("Used"));
  assert.ok(tags.includes("ACTIVE"));
  assert.ok(tags.includes("stressless-loveseat"));
  assert.ok(tags.includes("Width: 16"));
  assert.ok(tags.includes("Depth: 13"));
  assert.ok(tags.includes("Height: 25"));
  assert.ok(tags.includes("Weight: 13"));
  assert.equal(tags.some((t) => /^Width:\s*40$/i.test(t)), false);
  assert.equal(tags.some((t) => /^Height:\s*120$/i.test(t)), false);
});

test("shopify title poll defaults to ~2.5s steady cadence (cap ~3s)", () => {
  const prevInterval = process.env.SHOPIFY_TITLE_POLL_INTERVAL_MS;
  const prevAlias = process.env.SHOPIFY_TITLE_POLL_MS;
  const prevMax = process.env.SHOPIFY_TITLE_POLL_MAX_DELAY_MS;
  try {
    delete process.env.SHOPIFY_TITLE_POLL_INTERVAL_MS;
    delete process.env.SHOPIFY_TITLE_POLL_MS;
    delete process.env.SHOPIFY_TITLE_POLL_MAX_DELAY_MS;
    assert.equal(shopifyTitlePollIntervalMs(), 2500);
    assert.equal(shopifyTitlePollMaxDelayMs(2500), 2500);

    process.env.SHOPIFY_TITLE_POLL_MS = "2500";
    assert.equal(shopifyTitlePollIntervalMs(), 2500);

    process.env.SHOPIFY_TITLE_POLL_INTERVAL_MS = "2000";
    assert.equal(shopifyTitlePollIntervalMs(), 2000);
    process.env.SHOPIFY_TITLE_POLL_MAX_DELAY_MS = "12000";
    assert.equal(shopifyTitlePollMaxDelayMs(2000), 12000);
  } finally {
    if (prevInterval == null) delete process.env.SHOPIFY_TITLE_POLL_INTERVAL_MS;
    else process.env.SHOPIFY_TITLE_POLL_INTERVAL_MS = prevInterval;
    if (prevAlias == null) delete process.env.SHOPIFY_TITLE_POLL_MS;
    else process.env.SHOPIFY_TITLE_POLL_MS = prevAlias;
    if (prevMax == null) delete process.env.SHOPIFY_TITLE_POLL_MAX_DELAY_MS;
    else process.env.SHOPIFY_TITLE_POLL_MAX_DELAY_MS = prevMax;
  }
});
