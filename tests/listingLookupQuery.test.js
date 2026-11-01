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

test("NY2A5X with spaces and mixed case is one Shopify SKU lookup", () => {
  const samples = ["NY2A5X", "NY2A5X ", " NY2A5X", "NY2 A5X", "NY 2A5X", "SKU: NY2A5X", "ny2a5x"];
  for (const raw of samples) {
    const q = extractListingLookupQuery(raw);
    assert.equal(q.kind, "sku", raw);
    assert.equal(q.value.replace(/\s+/g, "").toUpperCase(), "NY2A5X", raw);
    assert.equal(/\s/.test(q.value), false, raw);
    assert.equal(normalizeListingSkuKey(q.value), "ny2a5x", raw);
    assert.equal(listingSkuSkipsTitleFallback(q), true, raw);
  }
});
