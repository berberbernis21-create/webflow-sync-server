import test from "node:test";
import assert from "node:assert/strict";
import {
  finalizeListingDescription,
  listingBuildItemFromRow,
  naturalSizeSentence,
} from "../lib/shopifyIntakeImages.js";

/**
 * Luxury Goods items state their size plainly. Furniture wording is unchanged.
 *
 * The same items and the same expected sentences are in Pricing Engine's
 * tests/test_luxury_size_sentence.py: both services must write the same sentence.
 */

const FURNITURE_WORDING =
  /\b(rooms?|walls?|hallway|footprint|oversized|seating|bedroom|storage|layout|nook|dining|desk)\b/i;

const FUR_JACKET = {
  name: "Vintage Canadian Red Fox Fur Jacket",
  merchType: "luxury",
  websiteCategory: "O",
  ecommerceTags: "LG, O, vintage-fur-jacket, canadian-fur, red-fox-fur",
  dimWidth: 22,
  dimHeight: 33,
};

const LUXURY_CASES = [
  // The item in the owner's screenshot.
  [FUR_JACKET, "It measures 22 inches wide and 33 inches tall."],
  // Every stored axis is stated; a shallow depth is not dropped.
  [
    {
      name: "Louis Vuitton Speedy 30 Handbag",
      merchType: "luxury",
      websiteCategory: "H",
      ecommerceTags: "LG, H",
      dimWidth: 12,
      dimDepth: 3,
      dimHeight: 9,
    },
    "It measures 12 inches wide, 3 inches deep, and 9 inches tall.",
  ],
  // Under LG the letter A is Accessories, not Art / Mirrors: no wall wording.
  [
    {
      name: "Hermes Leather Belt",
      merchType: "luxury",
      websiteCategory: "A",
      ecommerceTags: "LG, A",
      dimWidth: 40,
      dimHeight: 1.5,
    },
    "It measures 40 inches wide and 1.5 inches tall.",
  ],
  // A printed scarf is not a wall print.
  [
    {
      name: "Silk Floral Print Scarf",
      merchType: "luxury",
      websiteCategory: "R",
      ecommerceTags: "LG, R",
      dimWidth: 35,
      dimHeight: 35,
    },
    "It measures 35 inches wide and 35 inches tall.",
  ],
  // Jewelry, known from its code alone.
  [{ name: "Gold Chain", ecommerceTags: "LG, NK", dimWidth: 18 }, "It measures 18 inches wide."],
  // The vertical code alone is enough.
  [
    { name: "Leather Weekender", ecommerceTags: "LG, L", dimWidth: 20, dimDepth: 10, dimHeight: 12 },
    "It measures 20 inches wide, 10 inches deep, and 12 inches tall.",
  ],
];

const FURNITURE_CASES = [
  [
    { name: "Brass Floor Globe", merchType: "furniture", ecommerceTags: "FH, X", dimWidth: 22, dimHeight: 33 },
    "It measures 22 inches wide and 33 inches tall, tall enough to stand on the floor beside a console, fireplace or doorway.",
  ],
  [
    {
      name: "Greg Sheres Post Modern Blonde Armoire",
      merchType: "furniture",
      ecommerceTags: "FH, B",
      dimWidth: 42,
      dimDepth: 23,
      dimHeight: 75,
    },
    "It measures 42 inches wide, 23 inches deep, and 75 inches tall, so it works along a bedroom wall or hallway.",
  ],
  [
    {
      name: "Abstract Oil Painting",
      merchType: "furniture",
      websiteCategory: "A",
      ecommerceTags: "FH, A",
      dimWidth: 25,
      dimDepth: 1,
      dimHeight: 35,
    },
    "It measures 25 inches wide by 35 inches high.",
  ],
  [
    { name: "Jaipur Wool Area Rug - 8X10", merchType: "furniture", ecommerceTags: "FH, R" },
    "This is an 8 by 10 rug that anchors a seating group without feeling oversized.",
  ],
  [
    {
      name: "Walnut Dining Table",
      merchType: "furniture",
      ecommerceTags: "FH, D",
      dimWidth: 72,
      dimDepth: 40,
      dimHeight: 30,
    },
    "It measures 72 inches wide, 40 inches deep, and 30 inches tall, " +
      "so it works for dining or desk use without feeling oversized.",
  ],
];

test("Luxury Goods items state their size plainly", () => {
  for (const [item, expected] of LUXURY_CASES) {
    const sentence = naturalSizeSentence(item, "");
    assert.equal(sentence, expected, item.name);
    assert.equal(FURNITURE_WORDING.test(sentence), false, sentence);
    // The item shape the listing write and the mirror build gives the same sentence.
    assert.equal(naturalSizeSentence(listingBuildItemFromRow(item), ""), expected, item.name);
  }
});

test("furniture, wall art and rug size wording is unchanged", () => {
  for (const [item, expected] of FURNITURE_CASES) {
    assert.equal(naturalSizeSentence(item, ""), expected, item.name);
    assert.equal(naturalSizeSentence(listingBuildItemFromRow(item), ""), expected, item.name);
  }
});

test("the luxury sentence is the same whatever the description already says", () => {
  const existing = "It measures up to any wardrobe and works in everyday rooms without feeling oversized.";
  assert.equal(naturalSizeSentence(FUR_JACKET, existing), "It measures 22 inches wide and 33 inches tall.");
});

test("a luxury item with no size gets no size sentence", () => {
  const item = { name: "Vintage Fox Fur Jacket", merchType: "luxury", ecommerceTags: "LG, O" };
  assert.equal(naturalSizeSentence(item, ""), "");
});

test("snake_case row fields give the same sentence", () => {
  const row = {
    name: FUR_JACKET.name,
    merch_type: "luxury",
    website_category: "O",
    ecommerce_tags: FUR_JACKET.ecommerceTags,
    dim_width: 22,
    dim_height: 33,
  };
  assert.equal(naturalSizeSentence(row, ""), "It measures 22 inches wide and 33 inches tall.");
});

test("fur jacket description has the plain size once and no room phrase", () => {
  const base =
    "A vintage Canadian red fox fur jacket with a notched collar and a satin lining. " +
    "Two slash pockets sit at the hip and the fur is full and even throughout.";
  const out = finalizeListingDescription(base, FUR_JACKET, "", "");
  const product = out.replace(/\n*Dimensions:\s*[\s\S]*$/i, "").trim();
  assert.equal(product.split("It measures 22 inches wide and 33 inches tall.").length - 1, 1);
  assert.equal(out.includes("everyday rooms"), false);
  assert.equal(out.includes("without feeling oversized"), false);
  // One paragraph.
  assert.equal(product.includes("\n"), false);
  // A second pass writes the same text.
  assert.equal(finalizeListingDescription(out, FUR_JACKET, "", ""), out);
});
