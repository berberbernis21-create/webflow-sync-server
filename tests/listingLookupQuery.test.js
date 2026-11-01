import test from "node:test";
import assert from "node:assert/strict";
import {
  extractListingLookupQuery,
  listingSkuSkipsTitleFallback,
  normalizeListingSkuKey,
} from "../lib/listingLookupQuery.js";

test("product URL still resolves to a slug", () => {
  const q = extractListingLookupQuery(
    "https://www.lostandfoundresale.com/product/ethan-allen-sleigh-bed"
  );
  assert.equal(q.kind, "slug");
  assert.equal(q.value, "ethan-allen-sleigh-bed");
});

test("multi-word title stays a name lookup", () => {
  const q = extractListingLookupQuery("Ethan Allen American Impressions Full Sleigh Bed");
  assert.equal(q.kind, "name");
  assert.equal(q.value, "Ethan Allen American Impressions Full Sleigh Bed");
  assert.equal(listingSkuSkipsTitleFallback(q), false);
});

test("SKU code is an exact SKU lookup", () => {
  const q = extractListingLookupQuery("LF-1042");
  assert.equal(q.kind, "sku");
  assert.equal(q.value, "LF-1042");
  assert.equal(listingSkuSkipsTitleFallback(q), true);
  assert.equal(normalizeListingSkuKey(q.value), "lf-1042");
});

test("SKU label prefix is stripped", () => {
  assert.equal(extractListingLookupQuery("SKU: AB-12").value, "AB-12");
  assert.equal(extractListingLookupQuery("sku AB-12").kind, "sku");
  assert.equal(extractListingLookupQuery("SKU12345").value, "SKU12345");
});

test("dimension fragment stays a name lookup", () => {
  const q = extractListingLookupQuery("60x30x40H");
  assert.equal(q.kind, "name");
});

test("letter-only token is a SKU attempt that can still fall through to title", () => {
  const q = extractListingLookupQuery("Chandelier");
  assert.equal(q.kind, "sku");
  assert.equal(listingSkuSkipsTitleFallback(q), false);
});
