import test from "node:test";
import assert from "node:assert/strict";
import sharp from "sharp";
import {
  classifyShopifyBodyForIntake,
  extractShopifyListingFacts,
  appendIntakeNotesToDescription,
  listingMissesCustomerFlaw,
  naturalFlawSentence,
  selectIntakeItemForShopifyProduct,
  composeShopifyBodyHtml,
  deriveListingSeoFields,
  formatShopifyDimsLine,
  hammingDistance64,
  imageAverageHash,
  pairIntakePhotosToShopifyMedia,
  planShopifyMediaMoves,
  shopifyImageReorderNeedsWrite,
  imagePositionNeedsWrite,
  draftBlocksImageUpload,
  buildSyncSnapshot,
  shopifyWriteNeeded,
  planEcommerceOff,
  planIntakeWakeup,
  isRawFlawListingBody,
  listingBodyNeedsWrite,
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
  titlesMatchForRelink,
  shopifyStatusUnlinks,
  planShopifyProductGone,
  planShopifyRelink,
  planManualShopifyLink,
  gallerySyncOutcome,
  galleryImagesToDelete,
  intakePhotosShouldUpload,
  listingProductOmitsImages,
  scheduleGalleryRetry,
  setGalleryRetryHandler,
  syncWebhookFactsToIntake,
  refreshTitleDimensions,
  listingBodyHtmlForUpdate,
  convergeShopifyEvents,
  resolveQueuedListingJob,
  planSettledCleanliness,
  planSaveSync,
  shopifyProductFieldsFromPatch,
  planLiveFlawSync,
  readLiveTraxiaExtra,
  settleBuyerListingCopy,
  freshListingTags,
  proseHasInventedMaker,
  stripInventedMakers,
  stripMakerTags,
  skuRemoteImageUrls,
  skuFieldDataWithReadyImages,
  shopifyCdnImageIsReady,
  descriptionHasStackedProductProse,
  descriptionHasStackedWear,
  finalizeListingDescription,
  marketingListingBase,
  listingDeltaPayload,
  handleIntakeListingUpdate,
  resetListingUpdateCoalesceForTests,
  listingUpdateCoalesceSnapshotForTests,
  planImageOrderSync,
  decideSyncAuthority,
  intakePhotoIdsFromShopifyOrder,
  shopifyWritebackDecision,
  snapshotHash,
} from "../lib/shopifyIntakeImages.js";
import {
  planShopifyCallTimes,
  resetShopifyThrottleForTests,
  retryAfterShopify429,
} from "../lib/shopifyThrottle.js";

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

test("woven size sentence is not repeated under Dimensions", () => {
  const prose =
    'Original 1938 "Naked Birches" oil-on-canvas landscape painting in a natural wood frame, featuring a birch grove. Overall size is about 27.5 wide x 1.5 deep x 41.5 high.';
  const cleaned = stripListingDimJunk(prose);
  assert.match(cleaned, /Naked Birches/i);
  assert.equal(/27\.5 wide/i.test(cleaned), false);
  assert.equal(/1\.5 deep/i.test(cleaned), false);
  assert.equal(/41\.5 high/i.test(cleaned), false);
  const html = composeShopifyBodyHtml(prose, {
    width: 27.5,
    depth: 1.5,
    height: 41.5,
    weightLbs: 15,
  });
  assert.match(html, /birch grove/i);
  assert.equal(/27\.5 wide/i.test(html), false);
  assert.equal(/1\.5 deep/i.test(html), false);
  assert.equal(/41\.5 high/i.test(html), false);
  assert.equal((html.match(/Dimensions:/gi) || []).length, 1);
  assert.match(html, /Width: 27\.5&quot;/);
  assert.match(html, /Depth: 1\.5&quot;/);
  assert.match(html, /Height: 41\.5&quot;/);
  assert.match(html, /Weight: 15 lb/);
  const extendsHtml = composeShopifyBodyHtml(
    "This dining table extends to 77 inches. Overall size is about 36 wide x 18 deep x 30 high.",
    { width: 36, depth: 18, height: 30, weightLbs: 40 }
  );
  assert.match(extendsHtml, /extends to 77 inches/i);
  assert.equal(/36 wide/i.test(extendsHtml), false);
  assert.equal((extendsHtml.match(/Dimensions:/gi) || []).length, 1);
});

test("description without a size sentence still gets one Dimensions footer", () => {
  const html = composeShopifyBodyHtml(
    "This vintage wood box has a small drawer and warm patina for a console or bookshelf.",
    { width: 10, depth: 8, height: 3.5, weightLbs: 8 }
  );
  assert.match(html, /vintage wood box/i);
  assert.equal(/wide x/i.test(html), false);
  assert.equal((html.match(/Dimensions:/gi) || []).length, 1);
  assert.match(html, /Width: 10&quot;/);
  assert.match(html, /Weight: 8 lb/);
});

test("formatShopifyDimsLine keeps Weight on the Dimensions line", () => {
  const line = formatShopifyDimsLine({ width: 16, depth: 13, height: 25, weightLbs: 13 });
  assert.equal(
    line,
    'Dimensions: Width: 16" × Depth: 13" × Height: 25". Weight: 13 lb.'
  );
  assert.equal(formatShopifyDimsLine({ width: 65.0, depth: 29.0, height: 40.0, weightLbs: 29.0 }),
    'Dimensions: Width: 65" × Depth: 29" × Height: 40". Weight: 29 lb.');
  assert.equal(line.includes("\n"), false);
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

  const empty = classifyShopifyBodyForIntake("", helper);
  assert.equal(empty.kind, "empty");
  assert.equal(empty.restoreListing, true);

  const leaked =
    "Scratches. Angel on chariot painting wall art in a classic figurative style " +
    "with a warm-toned palette. This framed artwork is designed to hang on the wall " +
    "and works well as a focal piece in a living room, bedroom, or office. " +
    "AEO: This is a framed wall painting meant for vertical display.";
  const listed = classifyShopifyBodyForIntake(leaked, helper);
  assert.equal(listed.applyNotes, false);
  assert.equal(listed.applyDescription, false);
});

test("Shopify description payload keeps marketing copy and writes the flaw as a sentence", () => {
  const marketing =
    'Original 1938 "Naked Birches" oil-on-canvas landscape painting with birch trees and a wood frame.';
  const note = "Frame coming a little apart in top right";
  const sentence = "The frame is coming a little apart at the top right.";
  const shipping =
    "Pickup is available in Scottsdale, and we offer both local and nationwide delivery.";
  const prose = appendIntakeNotesToDescription(marketing, note);
  assert.match(prose, /oil-on-canvas landscape painting/);
  assert.match(prose, /The frame is coming a little apart at the top right/);
  assert.equal(prose.includes(note), false);
  assert.equal(prose.includes("AS IS"), false);
  assert.ok(prose.indexOf("oil-on-canvas") < prose.indexOf(sentence));
  assert.ok(prose.indexOf(sentence) < prose.indexOf(shipping));
  assert.equal(prose.split(shipping).length - 1, 1);
  assert.equal(appendIntakeNotesToDescription(prose, note), prose);
  const labeled = appendIntakeNotesToDescription(
    "Angel on chariot painting in a warm palette. AEO: This is a framed painting of an angel riding a chariot. Condition note: scratches are present.",
    ""
  );
  assert.equal(labeled.includes("AEO"), false);
  assert.equal(labeled.includes("Condition note"), false);
  assert.match(labeled, /This is a framed painting of an angel riding a chariot/);
  assert.match(labeled, /Scratches are present/);
  const html = composeShopifyBodyHtml(prose, {
    width: 27.5,
    depth: 1.5,
    height: 41.5,
    weightLbs: 15,
  });
  assert.match(html, /The frame is coming a little apart at the top right/);
  assert.match(html, /Dimensions:/);
  assert.ok(html.indexOf("The frame is coming a little apart") < html.indexOf("Dimensions:"));
  assert.ok(html.indexOf(shipping) < html.indexOf("Dimensions:"));
  const spill = appendIntakeNotesToDescription(marketing, "Measured Height=41.5 Width=27.5");
  assert.equal(spill.includes("Measured"), false);
  assert.equal(spill.includes("Width=27.5"), false);
  assert.ok(spill.includes(shipping));
  assert.equal(spill.includes(sentence), false);
  const warehouse = appendIntakeNotesToDescription(marketing, "In the warehouse");
  assert.equal(/warehouse/i.test(warehouse), false);
  assert.ok(warehouse.includes(shipping));
  const price = appendIntakeNotesToDescription(marketing, "Customer said the price was $500");
  assert.equal(/price/i.test(price), false);
  assert.equal(price.includes("$500"), false);
  assert.equal(price.includes("500"), false);
  const brand = appendIntakeNotesToDescription(marketing, "Brand is Everything Teak");
  assert.equal(/brand is/i.test(brand), false);
  assert.equal(/teak/i.test(brand), false);
  const scratch = "SMALL LITTLE SCRATCH IN TOP RIGHT";
  const scratchSentence = "There is a small scratch in the top right.";
  assert.equal(naturalFlawSentence(scratch), scratchSentence);
  const withScratch = appendIntakeNotesToDescription(marketing, "", scratch);
  assert.equal(withScratch.includes(scratchSentence), true);
  assert.equal(withScratch.includes(scratch), false);
  assert.equal(withScratch.includes("Condition note"), false);
  assert.equal(withScratch.includes("AS IS:"), false);
  assert.ok(withScratch.indexOf("oil-on-canvas") < withScratch.indexOf(scratchSentence));
  assert.equal(listingMissesCustomerFlaw(marketing, "", scratch), true);
  assert.equal(listingMissesCustomerFlaw(withScratch, "", scratch), false);
});

test("shopify identity uses product id and SKU before title", () => {
  const pid = "9824538460419";
  const sku = "R5FDVL";
  const locked = { id: "b5c575f8-4472-42f7-b1f0-86c07bf15d6c", shopifyProductId: pid, traxiaSku: sku, name: "old" };
  const other = { id: "other", shopifyProductId: "111", traxiaSku: "OTHER", name: "old" };
  const byId = selectIntakeItemForShopifyProduct({
    shopifyProductId: pid,
    sku,
    byProductId: locked,
    byExactTitle: other,
  });
  assert.equal(byId.via, "shopify_product_id");
  assert.equal(byId.item.id, locked.id);
  const bySku = selectIntakeItemForShopifyProduct({
    shopifyProductId: pid,
    sku,
    bySku: { id: locked.id, traxiaSku: sku, name: "old" },
    byExactTitle: other,
  });
  assert.equal(bySku.via, "traxia_sku");
  const first = selectIntakeItemForShopifyProduct({
    shopifyProductId: pid,
    sku,
    byExactTitle: { id: locked.id, name: "old" },
  });
  assert.equal(first.via, "exact_title");
  assert.equal(first.locked, false);
  const stolen = selectIntakeItemForShopifyProduct({
    shopifyProductId: pid,
    sku,
    byExactTitle: other,
  });
  assert.equal(stolen.item, null);
});

test("SMALLT crack note is one sentence and not an AS IS label", () => {
  const marketing =
    'Original 1938 "Naked Birches" oil-on-canvas landscape painting in a warm-toned wood frame for rustic interiors.';
  const body = appendIntakeNotesToDescription(marketing, "SMALLT CRACK IN TOP RIGHT");
  const sentence = "There is a small crack in the top right.";
  assert.match(body, /oil-on-canvas landscape painting/);
  assert.equal(body.includes("AS IS:"), false);
  assert.equal(body.includes("SMALLT"), false);
  assert.equal(/as shown in the photos/i.test(body), false);
  assert.ok(body.includes(sentence));
  assert.ok(body.indexOf("rustic interiors") < body.indexOf(sentence));
  assert.equal(appendIntakeNotesToDescription(body, "SMALLT CRACK IN TOP RIGHT").split(sentence).length - 1, 1);
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
  assert.equal(/13X13X16/i.test(fields.seoTitle), false);
  assert.ok(fields.seoDescription.length <= 160);
  assert.match(fields.aeoDescription, /pitcher|pottery|glazed/i);
  assert.match(fields.geoDescription, /Tuscany Fortunata/i);
  assert.match(fields.geoDescription, /Scottsdale/i);
  assert.equal(/\b(?:AEO|GEO|SEO)\s*:/i.test(fields.geoDescription), false);
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
  assert.equal(tags.includes("Used"), false);
  assert.equal(tags.includes("ACTIVE"), false);
  assert.ok(tags.includes("stressless-loveseat"));
  assert.ok(tags.includes("Width: 16"));
  assert.ok(tags.includes("Depth: 13"));
  assert.ok(tags.includes("Height: 25"));
  assert.ok(tags.includes("Weight: 13"));
  assert.equal(tags.some((t) => /^Width:\s*40$/i.test(t)), false);
  assert.equal(tags.some((t) => /^Height:\s*120$/i.test(t)), false);
});

test("extractShopifyListingFacts ignores zero defaults and reads real tags", () => {
  const facts = extractShopifyListingFacts({
    tags: "Used, ACTIVE, Location: The Lost & Found, ART - PAINTING, Width: 0, Height: 41.5, Depth: 1.5, Weight: 15",
    variants: [{ price: "125.00", weight: 0, grams: 0, weight_unit: "lb" }],
  });
  assert.equal(facts.width, null);
  assert.equal(facts.height, 41.5);
  assert.equal(facts.depth, 1.5);
  assert.equal(facts.weightLbs, 15);
  const fromGrams = extractShopifyListingFacts({
    tags: "Width: 27.5",
    variants: [{ grams: 6804, weight: 0, weight_unit: "lb" }],
  });
  assert.equal(fromGrams.width, 27.5);
  assert.ok(Math.abs(fromGrams.weightLbs - 15) < 0.05);
});

test("Traxia updated staff note stays out of the Shopify description", () => {
  const marketing =
    'Original 1938 "Naked Birches" oil-on-canvas landscape painting in a natural wood frame.';
  const prose = appendIntakeNotesToDescription(
    marketing,
    "[2026-11-21 22:50 UTC] Traxia updated price to $125\n[2026-11-21 22:50 UTC] Traxia updated dimensions to 27.5 x 1.5 x 41.5, weight 15 lb",
    "Scratches"
  );
  assert.match(prose, /Naked Birches/);
  assert.match(prose, /scratches/i);
  assert.equal(/Traxia updated/i.test(prose), false);
  assert.equal(/\$125/.test(prose), false);
  assert.equal(/27\.5 x 1\.5/.test(prose), false);
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

test("planShopifyMediaMoves reorders in one sequential move list", () => {
  const rotated = planShopifyMediaMoves(["a", "b", "c"], ["c", "a", "b"]);
  assert.deepEqual(rotated.order, ["c", "a", "b"]);
  assert.deepEqual(rotated.moves, [{ id: "c", newPosition: "0" }]);

  const reversed = planShopifyMediaMoves(["a", "b", "c"], ["c", "b", "a"]);
  assert.deepEqual(reversed.order, ["c", "b", "a"]);
  assert.equal(reversed.moves.length >= 1, true);

  const already = planShopifyMediaMoves(["a", "b"], ["a", "b"]);
  assert.deepEqual(already.moves, []);
  assert.deepEqual(already.order, ["a", "b"]);
});

test("pairIntakePhotosToShopifyMedia reorders existing images and skips distant extras", () => {
  const swapped = pairIntakePhotosToShopifyMedia([0n, 7n], [7n, 0n], {
    fullSet: false,
    maxHamming: 12,
  });
  assert.deepEqual(swapped, [1, 0]);

  const distant = pairIntakePhotosToShopifyMedia([0n], [(1n << 40n) - 1n], {
    fullSet: false,
    maxHamming: 12,
  });
  assert.deepEqual(distant, [null]);

  const sameCount = pairIntakePhotosToShopifyMedia([0n], [(1n << 40n) - 1n], {
    fullSet: true,
    maxHamming: 12,
  });
  assert.deepEqual(sameCount, [0]);
});

test("a cleared Traxia note rewrites the body and a live note keeps one sentence", () => {
  const marketing =
    'Original 1938 "Naked Birches" oil-on-canvas landscape painting in a warm-toned wood frame for rustic interiors.';
  const withNote = planLiveFlawSync({
    shopifyBody: marketing,
    liveNote: "SMALLT CRACK IN TOP RIGHT",
    liveKnown: true,
    storedCondition: "",
  });
  assert.match(withNote.prose, /There is a small crack in the top right\./);
  assert.equal(withNote.prose.includes("SMALLT"), false);
  assert.equal(withNote.prose.includes("AS IS:"), false);

  const stillThere = planLiveFlawSync({
    shopifyBody: withNote.prose,
    liveNote: "SMALLT CRACK IN TOP RIGHT",
    liveKnown: true,
    storedCondition: "SMALLT CRACK IN TOP RIGHT",
  });
  assert.equal(stillThere.flawMissing, false);
  assert.equal(stillThere.bodyHtml, "");

  const cleared = planLiveFlawSync({
    shopifyBody: withNote.prose,
    liveNote: "",
    liveKnown: true,
    storedCondition: "SMALLT CRACK IN TOP RIGHT",
  });
  assert.equal(cleared.flawMissing, true);
  assert.equal(cleared.condition, "");
  assert.match(cleared.bodyHtml, /rustic interiors/);
  assert.equal(/crack/i.test(cleared.bodyHtml), false);
  assert.equal(cleared.bodyHtml.includes("SMALLT"), false);
  assert.equal(readLiveTraxiaExtra({ body_html: withNote.prose, tags: "FH, A" }).known, false);
  assert.deepEqual(readLiveTraxiaExtra({ extraInformation: "" }), { known: true, text: "" });

  const item = {
    id: "b5c575f8-4472-42f7-b1f0-86c07bf15d6c",
    name: 'Original 1938 "Naked Birches" Oil on Canvas Landscape Painting - 27.5 x 41.5H',
    description: withNote.prose,
    notes: "",
    conditionIssues: "",
    retailPrice: 349,
    shopifyProductId: "9824538460419",
  };
  const saved = planSaveSync({ conditionIssues: { from: "SMALLT CRACK IN TOP RIGHT", to: "" } }, item);
  assert.equal(saved.sync, true);
  assert.equal(saved.payload.clearedNote, true);
  assert.equal(saved.payload.conditionIssues, "");
  assert.equal(/crack/i.test(saved.payload.description), false);
  assert.equal(saved.payload.price, undefined);
  assert.equal(saved.payload.retailPrice, undefined);
});

test("a blank note drops stacked condition sentences and a test note adds none", () => {
  const marketing =
    'Original 1938 "Naked Birches" oil-on-canvas landscape painting in a warm-toned wood frame for rustic interiors.';
  const stacked =
    `${marketing} There is a small crack in the top right. This piece is in excellent condition. ` +
    "There is a small scratch in the top right. Pickup is available in Scottsdale, Arizona.";
  const blank = appendIntakeNotesToDescription(stacked, "", "", {
    name: "Naked Birches",
    category: "ART - PAINTING",
  });
  assert.match(blank, /rustic interiors/);
  assert.equal(/\bcrack\b/i.test(blank), false);
  assert.equal(/\bscratch\b/i.test(blank), false);
  assert.equal(/excellent condition/i.test(blank), false);
  const junk = appendIntakeNotesToDescription(marketing, "TEST TEST TEST", "TEST TEST TEST", {
    name: "Naked Birches",
    category: "ART - PAINTING",
  });
  assert.equal(/\btest\b/i.test(junk), false);
  assert.match(junk, /rustic interiors/);
  const scratch = appendIntakeNotesToDescription(stacked, "", "small scratch in top right", {
    name: "Naked Birches",
    category: "ART - PAINTING",
  });
  assert.equal((scratch.match(/there is a small/gi) || []).length, 1);
  assert.match(scratch, /There is a small scratch in the top right\./);
  assert.equal(/\bcrack\b/i.test(scratch), false);
  assert.equal(/excellent condition/i.test(scratch), false);
});

test("empty shopify media uploads intake photos and a listing write never deletes images", () => {
  assert.equal(intakePhotosShouldUpload({ images: [] }, 8), true);
  assert.equal(intakePhotosShouldUpload({ id: 1 }, 8), true);
  assert.equal(
    intakePhotosShouldUpload({ images: [{ id: 1, src: "https://cdn.shopify.com/a.jpg" }] }, 8),
    false
  );
  assert.equal(intakePhotosShouldUpload({ images: [] }, 0), false);
  const unknown = { img: { id: 1 }, hash: null, used: false };
  assert.deepEqual(galleryImagesToDelete([unknown], { uploaded: 0, moved: 0 }), []);
  const orphan = { img: { id: 2 }, hash: "abc", used: false };
  assert.deepEqual(galleryImagesToDelete([orphan], { uploaded: 0, moved: 0 }), []);
  assert.deepEqual(
    galleryImagesToDelete(
      [orphan, { img: { id: 3 }, hash: "def", used: true }],
      { uploaded: 0, moved: 1 }
    ),
    [orphan]
  );
  const put = listingProductOmitsImages({ id: 9, body_html: "<p>Hi</p>", images: [] });
  assert.equal(Object.prototype.hasOwnProperty.call(put, "images"), false);
  assert.equal(put.body_html, "<p>Hi</p>");
  const fields = shopifyProductFieldsFromPatch({ bodyHtml: "<p>Hi</p>", images: [] });
  assert.equal(fields.images, undefined);
  assert.equal(fields.body_html, "<p>Hi</p>");
});

test("a Shopify image is included only after it answers, and a retry can use the new URL", () => {
  const stale = "https://cdn.shopify.com/s/files/1/intake_old.jpg";
  const fresh = "https://cdn.shopify.com/s/files/1/intake_new.jpg";
  const fieldData = {
    name: "Naked Birches",
    price: { value: 34900 },
    "main-image": { url: stale },
    "more-images": [{ url: fresh }],
  };
  assert.deepEqual(skuRemoteImageUrls(fieldData), [stale, fresh]);
  const waiting = skuFieldDataWithReadyImages(fieldData, []);
  assert.equal(waiting["main-image"], undefined);
  assert.equal(waiting["more-images"], undefined);
  assert.equal(waiting.price.value, 34900);
  const ready = skuFieldDataWithReadyImages(fieldData, [fresh]);
  assert.equal(ready["main-image"].url, fresh);
  assert.equal(ready["more-images"], undefined);
  const reread = skuFieldDataWithReadyImages(fieldData, [fresh], [fresh]);
  assert.equal(reread["main-image"].url, fresh);
  assert.equal(shopifyCdnImageIsReady(200), true);
  assert.equal(shopifyCdnImageIsReady(404), false);
});

test("cleared Extra Information removes the old flaw sentence", () => {
  const marketing =
    'Original 1938 "Naked Birches" oil-on-canvas landscape painting in a warm-toned wood frame for rustic interiors.';
  const previous = `${marketing} AS IS: SMALLT CRACK IN TOP RIGHT, as shown in the photos.`;
  const body = appendIntakeNotesToDescription(previous, "", "");
  assert.match(body, /oil-on-canvas landscape painting/);
  assert.match(body, /rustic interiors/);
  assert.equal(body.includes("AS IS:"), false);
  assert.equal(body.includes("SMALLT"), false);
  assert.equal(/crack/i.test(body), false);
  const woven = appendIntakeNotesToDescription(
    marketing,
    "",
    "SMALLT CRACK IN TOP RIGHT"
  );
  assert.match(woven, /There is a small crack in the top right\./);
  assert.equal(woven.includes("AS IS:"), false);
  assert.equal(woven.includes("SMALLT"), false);
});

test("raw flaw body is replaced by the listing paragraph plus one sentence", () => {
  const paragraph =
    'Original 1938 "Naked Birches" oil-on-canvas landscape painting in a warm-toned wood frame for rustic interiors.';
  const raw = "SMALLI CRACK IN TOP RIGHT";
  assert.equal(isRawFlawListingBody(raw), true);
  const written = appendIntakeNotesToDescription(paragraph, "", raw);
  assert.match(written, /oil-on-canvas landscape painting/);
  assert.match(written, /There is a small crack in the top right\./);
  assert.equal(written.includes("SMALLI"), false);
  assert.equal(written.includes("AS IS:"), false);
  assert.equal(listingBodyNeedsWrite(raw, written), true);
  assert.equal(listingBodyNeedsWrite(written, written), false);
});

test("second reorder of the same gallery does not write", () => {
  const first = planShopifyMediaMoves(["a", "b", "c"], ["c", "a", "b"]);
  assert.equal(shopifyImageReorderNeedsWrite(first.order, ["c", "a", "b"]), false);
  assert.equal(imagePositionNeedsWrite({ id: "a", position: 1 }, 1), false);
  assert.equal(imagePositionNeedsWrite({ id: "a", position: 3 }, 1), true);
});

test("e-commerce off keeps the lock and a later Intake wake-up uses that product", () => {
  const productId = "9824538460419";
  const sku = "R5EDVL";
  const intakeItemId = "b5c575f8-4472-42f7-b1f0-86c07bf15d6c";
  const off = planEcommerceOff({
    shopifyProductId: productId,
    sku,
    intakeItemId,
    body: 'Original 1938 "Naked Birches" oil-on-canvas landscape painting in a warm-toned wood frame.',
  });
  assert.equal(off.shopifyStatus, "draft");
  assert.equal(off.clearLock, false);
  assert.equal(off.keepPhotos, true);
  assert.equal(off.shopifyProductId, productId);
  assert.equal(off.sku, sku);
  assert.equal(/AS IS:/i.test(off.body), false);
  const wake = planIntakeWakeup({
    shopifyProductId: off.shopifyProductId,
    sku: off.sku,
    intakeItemId,
    shopifyStatus: "draft",
    alreadySynced: true,
  });
  assert.equal(wake.shopifyProductId, productId);
  assert.equal(wake.shopifyStatus, "active");
  assert.equal(wake.writeImages, true);
  assert.equal(wake.writeBody, true);
  assert.equal(wake.blockedByDraft, false);
  assert.equal(wake.blockedByAlreadySynced, false);
  assert.equal(draftBlocksImageUpload("draft"), false);
  const stored = buildSyncSnapshot({
    intakeItemId,
    shopifyProductId: productId,
    sku,
    body: off.body,
    title: "Naked Birches",
    tags: "FH, A",
    price: "349.00",
    imageOrder: ["1", "2"],
    publishStatus: "draft",
  });
  assert.equal(
    shopifyWriteNeeded({
      stored,
      desired: {
        intakeItemId,
        shopifyProductId: productId,
        sku,
        body: off.body,
        title: "Naked Birches",
        tags: "FH, A",
        price: "349.00",
        imageOrder: ["1", "2"],
        publishStatus: "draft",
      },
    }).write,
    false
  );
  assert.equal(
    shopifyWriteNeeded({
      stored,
      desired: {
        intakeItemId,
        shopifyProductId: productId,
        sku,
        body: off.body,
        title: "Naked Birches",
        tags: "FH, A",
        price: "349.00",
        publishStatus: "active",
      },
      explicit: true,
    }).reason,
    "intake_wakeup"
  );
});

test("excellent condition replaces a stale crack line", () => {
  const previous =
    'Original 1938 "Naked Birches" oil-on-canvas landscape painting in a warm-toned wood frame for rustic interiors. AS IS: SMALLT CRACK IN TOP RIGHT, as shown in the photos. Excellent Condition.';
  const body = appendIntakeNotesToDescription(previous, "", "excellent condition", {
    name: "Naked Birches",
    category: "ART - PAINTING",
  });
  assert.equal(body.includes("AS IS:"), false);
  assert.equal(/SMALLT|SMALT|smallt/i.test(body), false);
  assert.equal(/\bcrack\b/i.test(body), false);
  assert.equal((body.match(/excellent condition/gi) || []).length, 1);
  assert.match(body, /This piece is in excellent condition\./);
  assert.match(body, /rustic interiors/);
  assert.match(body, /oil-on-canvas/);
});

test("429 is retried and a partial reorder stays pending", async () => {
  resetShopifyThrottleForTests();
  const waits = [];
  let calls = 0;
  const done = await retryAfterShopify429(
    { status: 429, headers: { "retry-after": "1" } },
    async () => {
      calls += 1;
      return calls === 1 ? { status: 429, headers: {} } : { status: 200 };
    },
    { sleep: async (ms) => waits.push(ms) }
  );
  assert.equal(done.status, 200);
  assert.equal(calls, 2);
  assert.equal(waits[0], 1000);
  assert.deepEqual(planShopifyCallTimes(4), [0, 0, 500, 1000]);
  const outcome = gallerySyncOutcome({ desired: 6, moved: 2, uploaded: 0, failedMoves: 4 });
  assert.equal(outcome.status, "pending");
  assert.equal(outcome.reorderOk, false);
  assert.equal(outcome.moved, 2);
  let queued = null;
  setGalleryRetryHandler((job) => {
    queued = job;
  });
  const scheduled = scheduleGalleryRetry({ shopifyProductId: "9824312295683", intakeItemId: "item-1" });
  assert.equal(scheduled.queued, true);
  assert.equal(queued.shopifyProductId, "9824312295683");
  setGalleryRetryHandler(null);
});

test("webhook dimension tags post to Intake and rebuild the footer without a model", async () => {
  const item = {
    id: "b5c575f8-4472-42f7-b1f0-86c07bf15d6c",
    name: "Naked Birches Painting - 24X30X1H",
    description:
      'Original 1938 "Naked Birches" oil-on-canvas landscape painting. Measuring 24 H x 30 W x 1 D inches. Dimensions: Width: 24 in, Height: 1 in',
    dimWidth: 24,
    dimDepth: 1,
    dimHeight: 24,
    retailPrice: 349,
    // Prior agreed snapshot so Shopify dim/title tag edits are one-sided, not a crash.
    shopifySyncSnapshot: buildSyncSnapshot({
      title: "Naked Birches Painting - 24X30X1H",
      body: 'Original 1938 "Naked Birches" oil-on-canvas landscape painting.',
      price: 349,
      at: "2026-11-22T10:00:00Z",
    }),
  };
  const product = {
    id: 9824538460419,
    title: "Naked Birches Painting - 24X30X1H",
    tags: "Width: 30, Height: 36, Depth: 1.5, Weight: 8",
    variants: [{ sku: "R5EDVL", price: "349.00" }],
    status: "active",
    updated_at: "2026-11-22T18:00:00Z",
  };
  const result = await syncWebhookFactsToIntake(product, {
    resolveItem: async () => item,
    post: async (url, body) => ({ url, body }),
  });
  assert.match(result.url, /\/api\/lost-and-found-intake\/items\/b5c575f8-4472-42f7-b1f0-86c07bf15d6c\/shopify-image-sync$/);
  // Title W×D×H wins over Width:/Depth:/Height: tags (item 1 / armoire scramble fix).
  assert.equal(result.body.dimWidth, 24);
  assert.equal(result.body.dimDepth, 30);
  assert.equal(result.body.dimHeight, 1);
  assert.equal(result.body.weightLbs, 8);
  assert.equal(result.body.allowOpenAi, false);
  assert.equal(result.body.retailPrice, 349);
  // Title sticks exactly — dim tags never remangle the name token.
  assert.equal(result.body.name, "Naked Birches Painting - 24X30X1H");
  assert.match(result.rebuilt.body, /Width: 24/);
  assert.match(result.rebuilt.body, /Depth: 30/);
  assert.match(result.rebuilt.body, /Height: 1/);
  assert.equal(result.llmCalled, false);
});

test("a deleted product unlinks and an exact name relinks only an open row", () => {
  const item = {
    id: "keep",
    name: "Naked Birches - 30X1.5X36H",
    description: "Keep this listing.",
    traxiaSku: "R5EDVL",
    shopifyProductId: "9824538460419",
    photos: ["a", "b"],
  };
  const gone = planShopifyProductGone(item, { reason: "deleted" });
  assert.equal(gone.deleteItem, false);
  assert.equal(gone.clearDescription, false);
  assert.equal(gone.clearPhotos, false);
  assert.equal(gone.clearSku, false);
  assert.equal(gone.description, "Keep this listing.");
  assert.equal(gone.traxiaSku, "R5EDVL");
  assert.equal(gone.shopifyProductId, "9824538460419");
  assert.equal(gone.status, "unlinked");
  assert.equal(shopifyStatusUnlinks("draft"), false);
  assert.equal(shopifyStatusUnlinks("archived"), true);
  assert.equal(shopifyStatusUnlinks("unpublished"), true);
  assert.equal(shopifyStatusUnlinks("gone"), true);
  assert.equal(titlesMatchForRelink("Naked Birches - 30X1.5X36H - AS IS", "naked birches - 30x1.5x36h"), true);
  assert.equal(titlesMatchForRelink("Naked Birches", "Naked Birches - 30X1.5X36H"), false);
  const locked = { id: "locked", name: "Naked Birches - 30X1.5X36H", shopifyProductId: "111", traxiaSku: "KEEP" };
  const open = {
    id: "open",
    name: "Naked Birches - 30X1.5X36H - AS IS",
    shopifyProductId: "999",
    shopifyLinkState: "unlinked",
    traxiaSku: "R5EDVL",
  };
  const relink = planShopifyRelink({
    shopifyProductId: "555",
    title: "Naked Birches - 30X1.5X36H",
    sku: "R5EDVL",
    candidates: [locked, open],
  });
  assert.equal(relink.relink, true);
  assert.equal(relink.wake, true);
  assert.equal(relink.item.id, "open");
  assert.equal(relink.ignoredLocked, 1);
  const ambiguous = planShopifyRelink({
    shopifyProductId: "555",
    title: "Naked Birches - 30X1.5X36H",
    candidates: [open, { ...open, id: "other" }],
  });
  assert.equal(ambiguous.relink, false);
  assert.equal(ambiguous.action, "ambiguous");
  const one = planManualShopifyLink({
    title: "Naked Birches - 30X1.5X36H",
    products: [{ id: "9824538460419", title: "Naked Birches - 30X1.5X36H", status: "active", variants: [{ sku: "R5EDVL" }] }],
  });
  assert.equal(one.matchCount, 1);
  assert.equal(one.shopifyProductId, "9824538460419");
  assert.equal(one.traxiaSku, "R5EDVL");
  const none = planManualShopifyLink({ title: "Naked Birches - 30X1.5X36H", products: [{ id: "1", title: "Other" }] });
  assert.equal(none.matchCount, 0);
  assert.equal(none.message, "No exact match found for that name.");
  const many = planManualShopifyLink({
    title: "Naked Birches - 30X1.5X36H",
    products: [
      { id: "1", title: "Naked Birches - 30X1.5X36H", status: "active" },
      { id: "2", title: "Naked Birches - 30X1.5X36H - AS IS", status: "draft" },
    ],
  });
  assert.equal(many.matchCount, 2);
  assert.equal(many.linked, false);
  assert.equal(many.matches[0].id, "1");
  assert.equal(many.matches[1].id, "2");
});

const NAKED_BAD =
  'Original 1938 "Naked Birches" Oil on Canvas Landscape Painting - 27.5 x 41.5H BERNIS TEST (delete this part of name if you see it in store) - 27.5X1.5X41.5H';
const NAKED_CLEAN =
  'Original 1938 "Naked Birches" Oil on Canvas Landscape Painting - 27.5X1.5X41.5H';
const NAKED_CURRENT =
  'Original 1938 "Naked Birches" Oil on Canvas Landscape Painting - 27.5 x 41.5H';

const NAKED_TRUTH =
  'Original 1938 "Naked Birches" Oil on Canvas Landscape Painting - 27.5 x 41.5H Bernis Test';
const NAKED_KEPT =
  'Original 1938 "Naked Birches" Oil on Canvas Landscape Painting - 27.5 x 41.5H BERNIS TEST (delete this part of name if you see it in store)';

test("sync keeps the latest name and only updates suffix numbers", () => {
  assert.equal(refreshTitleDimensions(NAKED_BAD, { width: 27.5, depth: 1.5, height: 41.5 }), NAKED_KEPT);
  assert.equal(refreshTitleDimensions(NAKED_TRUTH, { width: 27.5, depth: 1.5, height: 41.5 }), NAKED_TRUTH);
  assert.equal(refreshTitleDimensions(NAKED_CLEAN, { width: 27.5, depth: 1.5, height: 41.5 }), NAKED_CLEAN);
  assert.equal(
    refreshTitleDimensions(NAKED_TRUTH, { width: 30, depth: 1.5, height: 42 }),
    'Original 1938 "Naked Birches" Oil on Canvas Landscape Painting - 30 x 42H Bernis Test'
  );
});

test("older webhook and echo do not replace the newest title", () => {
  assert.equal(
    convergeShopifyEvents([
      { updatedAt: "2026-11-22T17:00:00Z", title: NAKED_BAD, body: "old" },
      { updatedAt: "2026-11-22T18:00:00Z", title: NAKED_CURRENT, body: "new" },
      { updatedAt: "2026-11-22T17:30:00Z", title: NAKED_BAD, body: "old" },
    ]),
    NAKED_CURRENT
  );
  const echoed = convergeShopifyEvents([
    { updatedAt: "2026-11-22T18:00:00Z", title: NAKED_CURRENT, body: "same body" },
    { updatedAt: "2026-11-22T18:05:00Z", title: NAKED_CURRENT, body: "same body" },
  ]);
  assert.equal(echoed, NAKED_CURRENT);
});

test("queued listing job writes the current item instead of the captured payload", async () => {
  const item = {
    id: "b5c575f8-4472-42f7-b1f0-86c07bf15d6c",
    name: "NEW TITLE",
    description: "This is the current listing copy that is long enough to be a real description for the buyer.",
    notes: "",
    conditionIssues: "we are perfect",
    retailPrice: 349,
    shopifySyncSnapshot: JSON.stringify({ v: 1, at: "2026-11-22T16:00:00Z", title: "old-hash" }),
  };
  const written = resolveQueuedListingJob(
    { queuedAt: "2026-11-22T17:00:00Z", name: "OLD TITLE", description: "old captured body" },
    item
  );
  assert.equal(written.action, "write");
  assert.equal(written.payload.name, "NEW TITLE");
  assert.equal(written.payload.description.includes("old captured"), false);
  const { createHash } = await import("node:crypto");
  const titleHash = createHash("sha256").update("new title").digest("hex").slice(0, 16);
  item.shopifySyncSnapshot = JSON.stringify({ v: 1, at: "2026-11-22T18:00:00Z", title: titleHash });
  const skipped = resolveQueuedListingJob(
    { queuedAt: "2026-11-22T17:00:00Z", name: "OLD TITLE", description: "old captured body" },
    item
  );
  assert.equal(skipped.action, "noop");
  assert.equal(skipped.name, "NEW TITLE");
});

test("a dirty settled listing schedules one rebuild and a clean one does not", () => {
  const longBody =
    "Original 1938 oil-on-canvas landscape painting in a warm wood frame for rustic interiors. Pickup is available in Scottsdale.";
  assert.equal(planSettledCleanliness({ title: NAKED_BAD, body: longBody, note: "we are perfect" }).action, "rebuild");
  // body_as_is_paste must rebuild clean even on the corrective pass — never stop and leave garbage.
  const cleaned = planSettledCleanliness(
    { title: NAKED_BAD, body: "AS IS: SMALLT CRACK IN TOP RIGHT", note: "we are perfect" },
    { corrective: true }
  );
  assert.equal(cleaned.action, "rebuild");
  assert.equal(cleaned.forceCleanBody, true);
  const stopped = planSettledCleanliness(
    { title: NAKED_BAD, body: longBody, note: "we are perfect", tags: "used, location: A1" },
    { corrective: true }
  );
  assert.equal(stopped.action, "stop");
  assert.match(stopped.note, /cleanup/);
  assert.equal(
    planSettledCleanliness({ title: NAKED_CLEAN, body: longBody, note: "we are perfect", tags: "FH, A" }).action,
    "none"
  );
  assert.equal(
    planSettledCleanliness({ title: NAKED_TRUTH, body: longBody, note: "we are perfect", tags: "FH, A" }).action,
    "none"
  );
});

test("dual product paragraphs and AS IS paste collapse to one Intake paragraph", () => {
  const intake =
    "Curved wing grey leather king low profile platform bed with an upholstered wingback headboard. " +
    "Tailored grey leather upholstery wraps the headboard and frame for a clean, modern look " +
    "that suits contemporary and transitional bedrooms. " +
    "The distinctive wraparound wing silhouette adds depth and a cocooning feel behind the pillows " +
    "while keeping the profile streamlined. " +
    "A low platform design positions the mattress closer to the floor for an anchored, minimal " +
    "presence beside wood, linen, or metal nightstands.";
  const previousShopify =
    intake +
    " This RH curved wing grey leather king low profile bed features a sleek upholstered " +
    "headboard and a distinctive wraparound wing silhouette. " +
    "Designed to anchor your primary bedroom, this king-size platform bed showcases clean and " +
    "tailored grey leather upholstery that enhances its modern aesthetic. " +
    "Ideal for contemporary and transitional interiors, it pairs beautifully with wood, linen, " +
    "or metal nightstands. " +
    "Pickup is available in Scottsdale, and we offer both local and nationwide delivery. " +
    "Please see Delivery, Pickup & Freight Options below the description for additional details. " +
    "AS IS: this item is sold AS IS, with visible wear consistent with prior use, as shown in the photos.";
  const allowed =
    "Sold AS IS with visible wear consistent with prior use; " +
    "please review photos for the most accurate view of condition.";
  const item = {
    name: "RH Curved Wing Grey Leather King Low Profile Bed- AS IS",
    category: "BED",
    ecommerceTags: "FH, B, king-bed, grey-leather, wingback-headboard",
    merchType: "furniture",
    dims: { width: 82.5, depth: 91, height: 37.5, weightLbs: 91 },
  };

  assert.equal(descriptionHasStackedProductProse(previousShopify), true);
  assert.equal(descriptionHasStackedWear(previousShopify), true);

  // Restore / rebuild from Intake only — never concatenate the old dual Shopify body.
  const out = appendIntakeNotesToDescription(intake, "Sold AS IS with visible wear", "", item);
  assert.ok(out.startsWith("Curved wing grey leather king low profile platform bed"));
  assert.equal(out.includes("This RH curved wing"), false);
  assert.equal(out.includes("AS IS: this item is sold AS IS"), false);
  assert.equal(out.includes("AS IS:"), false);
  assert.equal(out.split(allowed).length - 1, 1);
  assert.equal(out.split("Pickup is available in Scottsdale").length - 1, 1);

  // Polluted Intake (mirrored dual body) also collapses on the next finalize.
  const cleaned = appendIntakeNotesToDescription(previousShopify, "Sold AS IS with visible wear", "", item);
  assert.ok(cleaned.startsWith("Curved wing grey leather king low profile platform bed"));
  assert.equal(cleaned.includes("This RH curved wing"), false);
  assert.equal(cleaned.includes("AS IS: this item is sold AS IS"), false);
  assert.equal(cleaned.split(allowed).length - 1, 1);

  // Idempotent: a second finalize does not stack another wear sentence or product paragraph.
  const again = appendIntakeNotesToDescription(cleaned, "Sold AS IS with visible wear", "", item);
  assert.equal(again.split(allowed).length - 1, 1);
  assert.equal(again.includes("This RH curved wing"), false);

  // Mirror must restore stacked Shopify bodies, not leave them or write them into Intake.
  const decision = classifyShopifyBodyForIntake(previousShopify, intake);
  assert.equal(decision.applyDescription, false);
  assert.equal(decision.restoreListing, true);

  // Empty Shopify body (user deleted description) restores from Intake finalize, not dual garbage.
  const empty = classifyShopifyBodyForIntake("", intake);
  assert.equal(empty.restoreListing, true);
  assert.equal(empty.applyDescription, false);

  const settled = planSettledCleanliness(
    { title: item.name, body: previousShopify, note: "Sold AS IS with visible wear" },
    { corrective: true }
  );
  assert.equal(settled.action, "rebuild");
  assert.equal(settled.forceCleanBody, true);
});

test("a name save rewrites body and tags; an unchanged save sends nothing", () => {
  const item = {
    id: "b5c575f8-4472-42f7-b1f0-86c07bf15d6c",
    name: NAKED_TRUTH,
    description: "This piece is in excellent condition. Pickup is available in Scottsdale.",
    notes: "",
    retailPrice: 349,
    dimWidth: 27.5,
    dimDepth: 1.5,
    dimHeight: 41.5,
    shopifyProductId: "9824538460419",
    ecommerceTags: "FH, A",
  };
  const named = planSaveSync({ name: { from: "old", to: NAKED_TRUTH } }, item);
  assert.equal(named.sync, true);
  assert.equal(named.payload.name, NAKED_TRUTH);
  assert.equal(named.payload.nameOnly, undefined);
  assert.equal(named.payload.replaceBody, true);
  assert.equal(named.payload.syncFields.includes("body"), true);
  assert.equal(named.payload.syncFields.includes("tags"), true);
  assert.equal(named.payload.dims, undefined);
  assert.equal(named.payload.price, undefined);
  const quiet = planSaveSync({}, item);
  assert.equal(quiet.sync, false);
  assert.equal(quiet.payload, null);
  assert.equal(planSaveSync({ _context: { source: "item-autosave" } }, item).sync, false);
  const written = resolveQueuedListingJob({ syncFields: ["name"], queuedAt: "2026-11-22T17:00:00Z" }, item);
  assert.equal(written.action, "write");
  assert.equal(written.payload.name, NAKED_TRUTH);
  assert.equal(written.payload.replaceBody, true);
  assert.equal(written.payload.dims, undefined);
});

test("hello and no scratch remove a baked scratch sentence", () => {
  const marketing =
    'Original 1938 "Naked Birches" oil-on-canvas landscape painting in a warm-toned wood frame for rustic interiors.';
  const polluted =
    `${marketing} There is a small scratch in the top right. Pickup is available in Scottsdale, Arizona.`;
  const hello = planLiveFlawSync({
    shopifyBody: polluted,
    liveNote: "hello",
    liveKnown: true,
    storedCondition: "small scratch in top right",
  });
  assert.equal(hello.flawMissing, true);
  assert.equal(/\bscratch\b/i.test(hello.prose), false);
  assert.equal(/\bhello\b/i.test(hello.prose), false);
  assert.match(hello.prose, /rustic interiors/);
  const negated = appendIntakeNotesToDescription(
    polluted,
    "no scratch",
    "small scratch in top right",
    { name: "Naked Birches", category: "ART - PAINTING" }
  );
  assert.equal(/\bscratch\b/i.test(negated), false);
  assert.equal(/there is no/i.test(negated), false);
  assert.equal(/as is/i.test(negated), false);
  assert.match(negated, /rustic interiors/);
  const real = appendIntakeNotesToDescription(polluted, "", "small scratch in top right", {
    name: "Naked Birches",
    category: "ART - PAINTING",
  });
  assert.equal((real.match(/there is a small/gi) || []).length, 1);
});

test("meta commentary and a test title do not name a maker", () => {
  const title = "Nightstand - TEST TEST Bernis";
  const generated =
    "Bernis nightstand wood with a clean, modern silhouette for bedroom styling. " +
    "This is a compact bedside table with a flat top for a lamp and a lower storage area to keep nighttime essentials close. " +
    "The design reads minimal and functional, with straight lines and a smooth finish that lets the wood tone do the work. " +
    "Use it beside the bed, as an end table in a living room, or as a small landing spot in an entryway. " +
    "Maker attribution is based on the Bernis name provided; no additional maker marks or material details are stated here.";
  const kept = settleBuyerListingCopy(generated, title);
  assert.equal(kept.flagged, false);
  assert.equal(kept.regenerated, false);
  assert.equal(/\bbernis\b/i.test(kept.description), false);
  assert.equal(/\btest\b/i.test(kept.description), false);
  assert.equal(/based on/i.test(kept.description), false);
  assert.equal(/stated/i.test(kept.description), false);
  assert.match(kept.description, /bedside/);
  let calls = 0;
  const fallen = settleBuyerListingCopy(
    "Maker attribution is based on the Bernis name provided; no additional maker marks or material details are stated here.",
    title,
    () => {
      calls += 1;
      return "Maker attribution is based on the Bernis name provided; no additional maker marks or material details are stated here.";
    }
  );
  assert.equal(calls, 1);
  assert.equal(fallen.flagged, true);
  assert.equal(fallen.regenerated, true);
  assert.equal(/\bbernis\b/i.test(fallen.description), false);
  assert.equal(/\btest\b/i.test(fallen.description), false);
  assert.match(fallen.description, /nightstand/i);
});

test("weight and dimensions update the footer and the matching tags together", () => {
  const title = "Vintage Limoges Linden Dinner Plates - Set Of 6 - 10X1X10";
  const prose =
    "Vintage Limoges linden dinner plates, a set of six for the table. Glazed white with a linden pattern.";
  const stale = `${prose}<br><br>Dimensions: Width: 10" × Depth: 1" × Height: 10".<br>Weight: 1 lb.`;
  const dims = { width: 10, depth: 1, height: 10, weightLbs: 10 };
  const html = listingBodyHtmlForUpdate(stale, prose, dims);
  assert.match(html, /Weight: 10 lb\./);
  assert.equal(html.includes("Weight: 1 lb"), false);
  const tags = mergeShopifyListingTags({
    existingTags: "Weight: 1, Width: 10, Depth: 1, Height: 10",
    ecommerceTags: "white",
    merchType: "furniture",
    websiteCategory: "D",
    dims,
  });
  assert.equal(tags.includes("Weight: 10"), true);
  assert.equal(tags.includes("Weight: 1"), false);
  assert.equal(tags.includes("Width: 10"), true);
  assert.equal(listingBodyHtmlForUpdate(html, prose, dims), "");
  assert.equal(listingBodyHtmlForUpdate(html, prose, { ...dims, weightLbs: 0 }), "");
  const wider = listingBodyHtmlForUpdate(html, prose, { ...dims, width: 12 });
  assert.match(wider, /Width: 12/);
  assert.match(wider, /Weight: 10 lb\./);
  const wideTags = mergeShopifyListingTags({
    existingTags: tags.join(", "),
    ecommerceTags: "white",
    merchType: "furniture",
    websiteCategory: "D",
    dims: { ...dims, width: 12 },
  });
  assert.equal(wideTags.includes("Width: 12"), true);
  assert.equal(wideTags.includes("Width: 10"), false);
  assert.equal(refreshTitleDimensions(title, { ...dims, weightLbs: 99 }), title);
  assert.equal(
    refreshTitleDimensions(title, { width: 12, depth: 1, height: 10 }),
    "Vintage Limoges Linden Dinner Plates - Set Of 6 - 12X1X10"
  );
  const item = {
    name: title,
    description: prose,
    notes: "",
    conditionIssues: "",
    dimWidth: 10,
    dimDepth: 1,
    dimHeight: 10,
    weightLbs: 10,
    retailPrice: 349,
  };
  assert.equal(planSaveSync({}, item).sync, false);
  const named = planSaveSync({ name: { from: "Old", to: title } }, item);
  assert.equal(named.payload.nameOnly, undefined);
  assert.equal(named.payload.name, title);
  assert.equal(named.payload.replaceBody, true);
  assert.equal(named.payload.price, undefined);
  const weight = planSaveSync({ weightLbs: { from: 1, to: 10 } }, item);
  assert.equal(weight.sync, true);
  assert.equal(weight.payload.dims.weightLbs, 10);
  assert.equal(weight.payload.allowOpenAi, false);
  assert.equal(weight.payload.price, undefined);
  // Dim-only save must not rewrite the title token.
  assert.equal(weight.payload.name, undefined);
});

test("an empty brand clears an invented maker and a set brand may keep it", () => {
  const junkTitle = "Nightstand - TEST TEST Bernis";
  const generated =
    "Bernis nightstand in wood with a clean, modern silhouette for bedroom styling. " +
    "This is a compact bedside table with a flat top for a lamp and a lower storage area to keep nighttime essentials close. " +
    "The design reads minimal and functional, with straight lines and a smooth finish. " +
    "Use it beside the bed or in an entryway.";
  const kept = settleBuyerListingCopy(generated, junkTitle, null, "");
  assert.equal(/\bbernis\b/i.test(kept.description), false);
  assert.equal(/\btest\b/i.test(kept.description), false);
  const freshTags = stripMakerTags(
    "FH, B, nightstand, bernis, bernis-nightstand-modern-wood-, test",
    junkTitle,
    "",
    "CABINET",
    generated
  );
  assert.equal(/\bbernis\b/i.test(freshTags), false);
  assert.equal(/\btest\b/i.test(freshTags), false);
  assert.match(freshTags, /nightstand/i);

  const stored =
    "Bernis nightstand in wood with a clean, modern silhouette for bedroom styling. " +
    "This is a compact bedside table with a flat top for a lamp and a lower storage area.";
  const current = "Modern Black 3-Drawer Nightstand";
  assert.equal(proseHasInventedMaker(stored, ""), true);
  assert.equal(stripInventedMakers(stored, current, ""), "");
  const rebuilt = freshListingTags({
    name: current,
    brand: "",
    websiteCategory: "B",
    merchType: "furniture",
  });
  assert.equal(/\bbernis\b/i.test(rebuilt), false);
  assert.equal(/bedside|neutral|\bwood\b/i.test(rebuilt), false);
  assert.match(rebuilt, /^FH, B/);
  assert.match(rebuilt, /nightstand/i);
  assert.match(rebuilt, /modern/i);
  assert.match(rebuilt, /black/i);
  const published = appendIntakeNotesToDescription(stored, "", "", { name: current, brand: "" });
  assert.match(published, /this nightstand is a straightforward piece for everyday use/i);
  assert.equal(/\bbernis\b/i.test(published), false);
  assert.equal(/silhouette|bedside|modern black/i.test(published), false);

  const branded = stripInventedMakers(stored, current, "Bernis");
  assert.match(branded, /\bbernis\b/i);
  const brandedTags = stripMakerTags("FH, B, nightstand, bernis", current, "Bernis", "CABINET", stored);
  assert.match(brandedTags, /\bbernis\b/i);
});

test("Furniture / Home and Office Den tags are FH and O only", () => {
  const tags = mergeShopifyListingTags({
    existingTags: "FH, X, O, Office Den",
    ecommerceTags: "FH, X, O, Office Den, desk",
    merchType: "furniture",
    websiteCategory: "Office Den",
  });
  assert.deepEqual(tags.slice(0, 2), ["FH", "O"]);
  assert.equal(tags.includes("X"), false);
  assert.equal(tags.some((tag) => /office den/i.test(tag)), false);
  assert.equal(tags.includes("desk"), true);
  const accessories = mergeShopifyListingTags({
    existingTags: "",
    ecommerceTags: "FH, X, Accessories",
    merchType: "furniture",
    websiteCategory: "Accessories",
  });
  assert.equal(accessories.includes("FH"), true);
  assert.equal(accessories.includes("X"), true);
  assert.equal(accessories.some((tag) => /^accessories$/i.test(tag)), false);
});

test("Luxury Goods tags are LG and the category code only", () => {
  const tags = mergeShopifyListingTags({
    existingTags: "LG, X, H, Handbags",
    ecommerceTags: "LG, X, H, Handbags, leather",
    merchType: "luxury",
    websiteCategory: "Handbags",
  });
  assert.deepEqual(tags.slice(0, 2), ["LG", "H"]);
  assert.equal(tags.includes("X"), false);
  assert.equal(tags.some((tag) => /handbag/i.test(tag)), false);
  assert.equal(tags.includes("leather"), true);
  const dining = mergeShopifyListingTags({
    existingTags: "FH, X, D, Dining Room",
    ecommerceTags: "FH, X, D, Dining Room, table",
    merchType: "furniture",
    websiteCategory: "Dining Room",
  });
  assert.deepEqual(dining.slice(0, 2), ["FH", "D"]);
  assert.equal(dining.includes("X"), false);
  assert.equal(dining.includes("table"), true);
});

test("from-scratch rewrite weaves Extra Info noticeable wear and drops dual paragraphs", () => {
  const p1 =
    "Grey leather king low profile platform bed with a curved wing upholstered headboard. " +
    "Tailored grey leather upholstery wraps the headboard and frame for a clean, modern look, " +
    "while the distinctive wraparound wing silhouette adds depth and a subtle wingback feel. " +
    "The low profile design keeps the mattress closer to the floor for a streamlined, contemporary " +
    "bedroom footprint. It pairs easily with wood, linen, or metal nightstands in both modern and " +
    "transitional interiors.";
  const p2 =
    "This RH curved wing grey leather king low profile bed features a sleek upholstered headboard " +
    "and a distinctive wraparound wing silhouette. Designed to anchor your primary bedroom, this " +
    "king-size platform bed showcases clean and tailored grey leather upholstery that enhances its " +
    "modern aesthetic. The low profile frame positions the mattress closer to the floor, creating a " +
    "streamlined look, while the curved wings add visual depth to the headboard. Ideal for contemporary " +
    "and transitional interiors, it pairs beautifully with wood, linen, or metal nightstands.";
  const dual = `${p1} ${p2}`;
  const woven =
    "Sold AS IS with noticeable wear consistent with prior use; " +
    "please review photos for the most accurate view of condition.";
  const stockVisible =
    "Sold AS IS with visible wear consistent with prior use; " +
    "please review photos for the most accurate view of condition.";
  const item = {
    name: "RH Curved Wing Grey Leather King Low Profile Bed- AS IS",
    merchType: "furniture",
    category: "BED",
    ecommerceTags: "FH, B",
    dimWidth: 82.5,
    dimLength: 91,
    dimHeight: 37.5,
    weightLbs: 91,
  };

  assert.equal(descriptionHasStackedProductProse(dual), true);
  assert.equal(/This RH curved wing/i.test(marketingListingBase(dual)), false);

  const out = finalizeListingDescription(dual, item, "noticeable wear", "");
  assert.match(out, /^Grey leather king low profile platform bed/);
  assert.equal(/This RH curved wing/i.test(out), false);
  assert.equal(/Designed to anchor your primary bedroom/i.test(out), false);
  assert.equal(/Ideal for contemporary/i.test(out), false);
  assert.match(out, /noticeable wear/i);
  assert.equal(out.includes(woven), true);
  assert.equal(out.includes(stockVisible), false);
  assert.equal((out.match(/Pickup is available in Scottsdale/g) || []).length, 1);
  assert.ok(out.indexOf(woven) < out.indexOf("Pickup is available in Scottsdale"));

  // Empty / dirty prior Shopify body is ignored — from-scratch only.
  const empty = appendIntakeNotesToDescription(p1, "noticeable wear", "", item);
  assert.equal(/This RH curved wing/i.test(empty), false);
  assert.equal(empty.includes(woven), true);

  // Coalesce rebuild uses latest Extra Info and replaceBody.
  const queued = resolveQueuedListingJob(
    { __rebuildFromCurrent: true, syncFields: ["body"], queuedAt: "2026-11-23T15:00:00Z" },
    {
      id: "d5efd059-f2e9-48cf-aa86-9c5634634ffb",
      name: item.name,
      description: dual,
      notes: "noticeable wear",
      conditionIssues: "",
      ecommerceTags: "FH, B",
      merchType: "furniture",
      dimWidth: 82.5,
      dim_length: 91,
      dimHeight: 37.5,
      weightLbs: 91,
      shopifyProductId: "9824309084419",
    }
  );
  assert.equal(queued.action, "write");
  assert.equal(queued.payload.replaceBody, true);
  assert.equal(queued.payload.rewriteBody, true);
  assert.equal(queued.payload.forceRefresh, true);
  assert.equal(queued.payload.notes, "noticeable wear");
  assert.equal(queued.payload.description.includes(woven), true);
  assert.equal(/This RH curved wing/i.test(queued.payload.description), false);

  const delta = listingDeltaPayload(
    {
      id: "d5efd059-f2e9-48cf-aa86-9c5634634ffb",
      name: item.name,
      description: p1,
      notes: "noticeable wear",
      conditionIssues: "",
      ecommerceTags: "FH, B",
      merchType: "furniture",
      dimWidth: 82.5,
      dimLength: 91,
      dimHeight: 37.5,
      weightLbs: 91,
    },
    ["body"]
  );
  assert.equal(delta.replaceBody, true);
  assert.equal(delta.rewriteBody, true);
  assert.equal(delta.description.includes(woven), true);
});

test("listing-update coalesce returns deferred and applies latest-wins once", async () => {
  resetListingUpdateCoalesceForTests();
  const itemId = "d5efd059-f2e9-48cf-aa86-9c5634634ffb";
  const calls = [];
  let release;
  const gate = new Promise((resolve) => {
    release = resolve;
  });
  const loadItem = async () => ({
    id: itemId,
    name: "RH Bed v3 final",
    shopifyProductId: "9824309084419",
    description:
      "Curved wing grey leather king low profile platform bed with an upholstered wingback headboard for a modern bedroom.",
    notes: "noticeable wear",
    conditionIssues: "",
    ecommerceTags: "FH, B",
    merchType: "furniture",
    dimWidth: 82.5,
    dimLength: 91,
    dimHeight: 37.5,
    weightLbs: 91,
    retailPrice: 1200,
  });

  const execute = async (payload) => {
    calls.push({
      name: payload.name,
      rebuild: payload.__rebuiltFromCurrent === true || payload.__rebuildFromCurrent === true,
    });
    if (calls.length === 1) await gate;
    return {
      ok: true,
      status: "updated",
      shopifyProductId: payload.shopifyProductId,
      intakeItemId: payload.intakeItemId,
      name: payload.name,
    };
  };

  const first = handleIntakeListingUpdate(
    {
      intakeItemId: itemId,
      shopifyProductId: "9824309084419",
      name: "RH Bed v1",
    },
    { execute, loadItem }
  );

  const coalesced = await handleIntakeListingUpdate(
    {
      intakeItemId: itemId,
      shopifyProductId: "9824309084419",
      name: "RH Bed v2 latest",
    },
    { execute, loadItem }
  );
  assert.equal(coalesced.deferred, true);
  assert.equal(coalesced.status, "coalesced");
  assert.equal(coalesced.ok, false);
  assert.equal(coalesced.reason, "in_flight_collapsed");
  assert.deepEqual(listingUpdateCoalesceSnapshotForTests().inflight, [`intake:${itemId}`]);
  assert.deepEqual(listingUpdateCoalesceSnapshotForTests().pending, [`intake:${itemId}`]);

  // Newer name while still in flight — pending must keep latest only.
  await handleIntakeListingUpdate(
    {
      intakeItemId: itemId,
      shopifyProductId: "9824309084419",
      name: "RH Bed v3 final",
    },
    { execute, loadItem }
  );

  release();
  const firstResult = await first;
  assert.equal(firstResult.status, "updated");
  assert.equal(calls[0].name, "RH Bed v1");

  // Pending settle (250ms) then one follow-up with latest Intake name (latest-wins).
  await new Promise((r) => setTimeout(r, 400));
  assert.equal(calls.length, 2);
  assert.equal(calls[1].name, "RH Bed v3 final");
  assert.equal(calls[1].rebuild, true);
  assert.deepEqual(listingUpdateCoalesceSnapshotForTests(), { inflight: [], pending: [] });
  resetListingUpdateCoalesceForTests();
});

test("listing-update coalesce is per-item so other items are not blocked", async () => {
  resetListingUpdateCoalesceForTests();
  let releaseBed;
  const bedGate = new Promise((resolve) => {
    releaseBed = resolve;
  });
  const seen = [];
  const execute = async (payload) => {
    seen.push(payload.intakeItemId);
    if (payload.intakeItemId === "d5efd059-f2e9-48cf-aa86-9c5634634ffb") {
      await bedGate;
    }
    return { ok: true, status: "updated", intakeItemId: payload.intakeItemId };
  };

  const bed = handleIntakeListingUpdate(
    {
      intakeItemId: "d5efd059-f2e9-48cf-aa86-9c5634634ffb",
      name: "RH Bed",
    },
    { execute }
  );
  const armoire = await handleIntakeListingUpdate(
    {
      intakeItemId: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
      name: "Greg Sheres armoire bernis karfen",
    },
    { execute }
  );
  assert.equal(armoire.status, "updated");
  assert.ok(seen.includes("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"));
  releaseBed();
  await bed;
  resetListingUpdateCoalesceForTests();
});

test("empty Shopify body rebuilds from Intake without title price or dims", () => {
  const item = {
    id: "b5c575f8-4472-42f7-b1f0-86c07bf15d6c",
    name: "RH Curved Wing Bed - 80X86X40",
    description:
      "This curved wing grey leather king low profile platform bed has a soft silhouette for a primary bedroom.",
    notes: "",
    conditionIssues: "noticeable wear",
    retailPrice: 2495,
    dimWidth: 80,
    dimDepth: 86,
    dimHeight: 40,
    shopifyProductId: "111",
  };
  const empty = classifyShopifyBodyForIntake("", item.description);
  assert.equal(empty.kind, "empty");
  assert.equal(empty.restoreListing, true);
  assert.equal(empty.applyDescription, false);
  const payload = listingDeltaPayload(item, ["body"]);
  assert.equal(payload.name, undefined);
  assert.equal(payload.price, undefined);
  assert.equal(payload.retailPrice, undefined);
  assert.equal(payload.dims, undefined);
  assert.equal(payload.replaceBody, true);
  assert.equal(payload.rewriteBody, true);
  assert.match(String(payload.description || ""), /wear/i);
  assert.equal(/AS\s*IS\s*:/i.test(String(payload.description || "")), false);
  const fields = shopifyProductFieldsFromPatch({
    bodyHtml: "<p>" + payload.description + "</p>",
    title: item.name,
    price: item.retailPrice,
    images: ["x"],
  });
  // Body-only update payload must not carry title/price/images when scoped correctly.
  // shopifyProductFieldsFromPatch includes title if passed � listing-update uses allowField instead.
  assert.equal(payload.syncFields.includes("body"), true);
  assert.equal(payload.syncFields.includes("name"), false);
  assert.equal(payload.syncFields.includes("price"), false);
  assert.equal(payload.syncFields.includes("dims"), false);
});

test("image order latest-wins both directions and echo stays noop", () => {
  const snap = snapshotHash("p1|p2|p3");
  const intake = snapshotHash("p1|p2|p3");
  const shopify = snapshotHash("p3|p1|p2");
  const adopt = planImageOrderSync({
    snapshotImagesHash: snap,
    intakeOrderHash: intake,
    shopifyImpliedIntakeHash: shopify,
  });
  assert.equal(adopt.action, "adopt_shopify");
  const push = planImageOrderSync({
    snapshotImagesHash: snap,
    intakeOrderHash: shopify,
    shopifyImpliedIntakeHash: intake,
  });
  assert.equal(push.action, "push_intake");
  const clash = planImageOrderSync({
    snapshotImagesHash: snap,
    intakeOrderHash: snapshotHash("p2|p1|p3"),
    shopifyImpliedIntakeHash: shopify,
  });
  assert.equal(clash.action, "push_intake");
  assert.equal(clash.reason, "clash_intake_wins");
  const echo = planImageOrderSync({
    snapshotImagesHash: shopify,
    intakeOrderHash: shopify,
    shopifyImpliedIntakeHash: shopify,
  });
  assert.equal(echo.action, "noop");
  const crash = planImageOrderSync({
    snapshotImagesHash: "",
    intakeOrderHash: intake,
    shopifyImpliedIntakeHash: shopify,
  });
  assert.equal(crash.action, "push_intake");
  assert.equal(crash.reason, "no_snapshot_intake_wins");
  // pairs[i]=media index; positions[media]=Shopify position → sort Intake by that.
  const ordered = intakePhotoIdsFromShopifyOrder(
    ["p1", "p2", "p3"],
    [2, 0, 1],
    [3, 1, 2]
  );
  assert.deepEqual(ordered, ["p3", "p1", "p2"]);
});

test("sync authority clash and crash prefer Intake", () => {
  const clash = decideSyncAuthority({
    snapshotHash: "a",
    intakeHash: "b",
    shopifyHash: "c",
  });
  assert.equal(clash.authority, "intake");
  assert.equal(clash.reason, "clash_intake_wins");
  const crash = decideSyncAuthority({
    snapshotHash: "",
    intakeHash: "b",
    shopifyHash: "c",
  });
  assert.equal(crash.authority, "intake");
  assert.equal(crash.reason, "no_snapshot_intake_wins");
  const shopifyOnly = decideSyncAuthority({
    snapshotHash: "a",
    intakeHash: "a",
    shopifyHash: "c",
  });
  assert.equal(shopifyOnly.authority, "shopify");
  const writebackClash = shopifyWritebackDecision({
    snapshot: { title: snapshotHash("Agreed"), at: "2026-11-23T10:00:00Z" },
    updatedAt: "2026-11-23T18:00:00Z",
    title: "Shopify Other",
    intakeTitle: "Intake Latest",
  });
  assert.equal(writebackClash.apply, false);
  assert.equal(writebackClash.reason, "clash_intake_wins");
  const writebackBootstrap = shopifyWritebackDecision({
    snapshot: {},
    updatedAt: "2026-11-23T18:00:00Z",
    title: "Shopify Other",
    intakeTitle: "Intake Latest",
  });
  assert.equal(writebackBootstrap.apply, true);
  assert.equal(writebackBootstrap.reason, "bootstrap");
});

test("name save expands to body and tags without remangling title", () => {
  const written = "Greg Sheres Post Modern Blonde Armoire- 42X23X75 bernis karfen";
  const item = {
    id: "armoire-1",
    name: written,
    description: "A blonde armoire with clean mid-century lines for a bedroom.",
    notes: "",
    conditionIssues: "",
    ecommerceTags: "FH, B",
    dimWidth: 42,
    dimDepth: 23,
    dimHeight: 75,
    retailPrice: 80,
    shopifyProductId: "999",
  };
  const named = planSaveSync({ name: { from: "old", to: written } }, item);
  assert.equal(named.sync, true);
  assert.equal(named.payload.name, written);
  assert.equal(named.payload.nameOnly, undefined);
  assert.equal(named.payload.replaceBody, true);
  assert.equal(named.payload.syncFields.includes("body"), true);
  assert.equal(named.payload.syncFields.includes("tags"), true);
  assert.equal(named.payload.dims, undefined);
  const dimsOnly = planSaveSync({ dimWidth: { from: 40, to: 42 } }, item);
  assert.equal(dimsOnly.payload.name, undefined);
  assert.equal(dimsOnly.payload.replaceBody, true);
  assert.equal(dimsOnly.payload.syncFields.includes("dims"), true);
});
