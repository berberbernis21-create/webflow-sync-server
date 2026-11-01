import test from "node:test";
import assert from "node:assert/strict";
import {
  extractListingLookupQuery,
  listingSkuSkipsTitleFallback,
  normalizeListingSkuKey,
  pickListingSkuMatch,
  shopifySkuSearchQueries,
  storedListingSkuMatchRank,
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

test("catalog SKU is six or seven characters", () => {
  const six = extractListingLookupQuery("KVLUZK");
  assert.equal(six.kind, "sku");
  assert.equal(six.value, "KVLUZK");
  const seven = extractListingLookupQuery("KVLUZK2");
  assert.equal(seven.kind, "sku");
  assert.equal(seven.value, "KVLUZK2");
});

test("a space and more words stays a name lookup", () => {
  const samples = ["Lamp 12", "Sofa 36", "NY2 A5X", "NY 2A5X", "Red Chair"];
  for (const raw of samples) {
    const q = extractListingLookupQuery(raw);
    assert.equal(q.kind, "name", raw);
    assert.equal(q.value, raw, raw);
    assert.equal(listingSkuSkipsTitleFallback(q), false, raw);
  }
});

test("product name with dimensions stays a name lookup", () => {
  const samples = [
    "Ceramic Jack-O-Lantern 10X10X12",
    "Round Circles Mirror Art-20X1X36",
    "Art-20X1X36",
  ];
  for (const raw of samples) {
    const q = extractListingLookupQuery(raw);
    assert.equal(q.kind, "name", raw);
    assert.equal(q.value, raw, raw);
    assert.equal(listingSkuSkipsTitleFallback(q), false, raw);
  }
});

test("letter-only token is a SKU attempt that can still fall through to title", () => {
  const q = extractListingLookupQuery("Chandelier");
  assert.equal(q.kind, "sku");
  assert.equal(listingSkuSkipsTitleFallback(q), false);
});

test("plain SKU searches unquoted first, then quoted", () => {
  assert.deepEqual(shopifySkuSearchQueries("GND4FH"), ['sku:GND4FH', 'sku:"GND4FH"']);
  assert.deepEqual(shopifySkuSearchQueries(" ny2 a5x "), ['sku:ny2a5x', 'sku:"ny2a5x"']);
  assert.deepEqual(shopifySkuSearchQueries("LF-1042"), ['sku:LF-1042', 'sku:"LF-1042"']);
});

test("SKU search quotes codes that contain Shopify search operators", () => {
  assert.deepEqual(shopifySkuSearchQueries('AB"12'), ['sku:"AB\\"12"']);
  assert.deepEqual(shopifySkuSearchQueries("AB(12)"), ['sku:"AB(12)"']);
  assert.deepEqual(shopifySkuSearchQueries(""), []);
});

test("stored SKU matches the code or the code plus a numeric suffix", () => {
  assert.equal(storedListingSkuMatchRank("GND4FH", "gnd4fh"), 2);
  assert.equal(storedListingSkuMatchRank("GND4FH-2877", "GND4FH"), 1);
  assert.equal(storedListingSkuMatchRank("NY2A5X-99", "ny2 a5x"), 1);
  assert.equal(storedListingSkuMatchRank("GND4FH-2877", "GND4"), 0);
  assert.equal(storedListingSkuMatchRank("GND4FH-2877", "GND4FH-28"), 0);
  assert.equal(storedListingSkuMatchRank("Dracena Planter", "GND4FH"), 0);
});

test("ambiguous numeric suffixes are not treated as a hit", () => {
  const one = pickListingSkuMatch(
    [{ sku: "OTHER-1", title: "nope" }, { sku: "GND4FH-2877", title: "planter" }],
    "GND4FH"
  );
  assert.equal(one.sku, "GND4FH-2877");
  const exact = pickListingSkuMatch(
    [{ sku: "GND4FH-2877" }, { sku: "GND4FH" }],
    "GND4FH"
  );
  assert.equal(exact.sku, "GND4FH");
  const ambiguous = pickListingSkuMatch(
    [{ sku: "GND4FH-1" }, { sku: "GND4FH-2" }],
    "GND4FH"
  );
  assert.equal(ambiguous, null);
});

test("a single 6-character code is one Shopify SKU lookup", () => {
  const samples = ["NY2A5X", "NY2A5X ", " NY2A5X", "SKU: NY2A5X", "ny2a5x"];
  for (const raw of samples) {
    const q = extractListingLookupQuery(raw);
    assert.equal(q.kind, "sku", raw);
    assert.equal(q.value.replace(/\s+/g, "").toUpperCase(), "NY2A5X", raw);
    assert.equal(/\s/.test(q.value), false, raw);
    assert.equal(normalizeListingSkuKey(q.value), "ny2a5x", raw);
    assert.equal(listingSkuSkipsTitleFallback(q), true, raw);
  }
});
