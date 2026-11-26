import test from "node:test";
import assert from "node:assert/strict";

import {
  getEcommerceVerticalOverrideFromTags,
  getHardFhLgVerticalLockFromTags,
  getHardFhLgVerticalLockFromProduct,
  productHasLuxuryEcommerceTag,
  productHasFurnitureEcommerceTag,
} from "../ecommerceTags.js";

/*
 * LOCKED: LG/FH Shopify tags are the single source of truth for the product
 * vertical. Dimensions, titles, keywords, and any other heuristic must NEVER
 * override them. See `.cursor/rules/vertical-classification.mdc`.
 */

test("LG tag → luxury even with furniture-sized dimensions and a furniture-sounding title", () => {
  const product = {
    title: "Oversized Carved Wood Console Table Sideboard Cabinet", // screams furniture
    product_type: "Furniture",
    vendor: "Restoration Hardware",
    tags: ["LG", "Width: 72", "Depth: 20", "Height: 34", "Weight: 180", "Living Room"],
    body_html: "<p>Large solid wood console, 72 inches wide. Great for a living room or dining room.</p>",
  };
  const lock = getHardFhLgVerticalLockFromProduct(product);
  assert.equal(lock?.vertical, "luxury", "LG must win over furniture dimensions + title");
  assert.equal(lock?.tag, "LG");
  assert.equal(lock?.conflict, false);
  assert.equal(productHasLuxuryEcommerceTag(product), true);
  assert.equal(productHasFurnitureEcommerceTag(product), false);
});

test("FH tag → furniture even with a luxury-handbag-sounding title and tiny dimensions", () => {
  const product = {
    title: "Designer Leather Handbag Crossbody Clutch Purse Tote", // screams luxury
    product_type: "Handbags",
    vendor: "Louis Vuitton",
    tags: ["FH", "Width: 6", "Depth: 3", "Height: 5", "Accessories"],
    body_html: "<p>Small leather handbag with crossbody strap. Designer wallet and clutch.</p>",
  };
  const lock = getHardFhLgVerticalLockFromProduct(product);
  assert.equal(lock?.vertical, "furniture", "FH must win over luxury title + small dimensions");
  assert.equal(lock?.tag, "FH");
  assert.equal(lock?.conflict, false);
  assert.equal(productHasFurnitureEcommerceTag(product), true);
  assert.equal(productHasLuxuryEcommerceTag(product), false);
});

test("neither tag → no lock (null), so caller falls back to heuristics", () => {
  const product = {
    title: "Vintage Brass Table Lamp",
    product_type: "Lighting",
    vendor: "Unknown",
    tags: ["Used", "Lighting", "Width: 10"],
    body_html: "<p>A brass lamp.</p>",
  };
  assert.equal(getHardFhLgVerticalLockFromProduct(product), null);
  assert.equal(getEcommerceVerticalOverrideFromTags(product.tags), null);
});

test("both tags → luxury (LG wins) with conflict flag, independent of order", () => {
  const lgFirst = getHardFhLgVerticalLockFromProduct({ tags: ["LG", "H", "FH", "L"] });
  assert.equal(lgFirst?.vertical, "luxury");
  assert.equal(lgFirst?.conflict, true);

  const fhFirst = getHardFhLgVerticalLockFromProduct({ tags: ["FH", "L", "LG", "H"] });
  assert.equal(fhFirst?.vertical, "luxury");
  assert.equal(fhFirst?.conflict, true);
});

test("tag matching is case-insensitive and trims whitespace", () => {
  assert.equal(getEcommerceVerticalOverrideFromTags(["lg"])?.vertical, "luxury");
  assert.equal(getEcommerceVerticalOverrideFromTags([" LG "])?.vertical, "luxury");
  assert.equal(getEcommerceVerticalOverrideFromTags(["Lg"])?.vertical, "luxury");
  assert.equal(getEcommerceVerticalOverrideFromTags(["fh"])?.vertical, "furniture");
  assert.equal(getEcommerceVerticalOverrideFromTags([" Fh "])?.vertical, "furniture");
});

test("LG/FH are matched as whole tag codes, never as substrings of other words", () => {
  // Real-world words that merely contain the letters LG or FH must NOT match.
  // Note: "FH-B" / "LG-H" style hyphens are an intentional combined-code separator,
  // so a leading "FH-"/"LG-" DOES resolve a vertical. These are words that merely
  // contain the letters elsewhere and must stay null.
  const notVerticalTags = [
    "FLAG", "LUGGAGE", "LGBT", "LGE", "FHA", "FHX", "ELGIN",
    "LUXURY GOODS LG EDITION", "flag-pole",
  ];
  for (const tag of notVerticalTags) {
    assert.equal(
      getEcommerceVerticalOverrideFromTags([tag]),
      null,
      `"${tag}" must not be read as an LG/FH vertical tag`
    );
  }
});

test("combined / prefixed Traxia tag formats still resolve the vertical", () => {
  assert.equal(getEcommerceVerticalOverrideFromTags(["Ecommerce LG: NK"])?.vertical, "luxury");
  assert.equal(getEcommerceVerticalOverrideFromTags(["LG, H"])?.vertical, "luxury");
  assert.equal(getEcommerceVerticalOverrideFromTags(["FH/B"])?.vertical, "furniture");
  assert.equal(getEcommerceVerticalOverrideFromTags(["E-COMMERCE FH"])?.vertical, "furniture");
});

test("getHardFhLgVerticalLockFromTags mirrors the override and carries conflict", () => {
  assert.equal(getHardFhLgVerticalLockFromTags(["LG"])?.vertical, "luxury");
  assert.equal(getHardFhLgVerticalLockFromTags(["LG"])?.conflict, false);
  assert.equal(getHardFhLgVerticalLockFromTags(["FH", "LG"])?.conflict, true);
  assert.equal(getHardFhLgVerticalLockFromTags([]), null);
});
