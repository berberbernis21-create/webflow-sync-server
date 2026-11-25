import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import {
  QUALITY_MAKER_MISSING,
  QUALITY_OPENING_FRAGMENT,
  QUALITY_RESTATED,
  finalizeListingDescription,
  listingQualityIssues,
  naturalSizeSentence,
  settleBuyerListingCopy,
} from "../lib/shopifyIntakeImages.js";
import {
  inventedTitleSentence,
  isArtName,
  makerIsCertain,
  nameLead,
  openingIsFragment,
  openingParrotsName,
  requiredMaker,
  unpublishableSentence,
} from "../lib/listingQuality.js";

/**
 * Description quality rules: the code-built text and the deterministic check.
 *
 * tests/fixtures/listing_quality_cases.json is the same file as Pricing Engine's
 * tests/fixtures/listing_quality_cases.json, and tests/test_listing_quality.py
 * there asserts the same expectations. Both services must build the same text
 * for the same item, or they rewrite each other's work.
 */

const CASES = JSON.parse(
  fs.readFileSync(new URL("./fixtures/listing_quality_cases.json", import.meta.url), "utf8")
);
const BUILDS = Object.fromEntries(CASES.builds.map((c) => [c.id, c]));
const SETTLES = Object.fromEntries(CASES.settles.map((c) => [c.id, c]));

const build = (c) => finalizeListingDescription(c.stored, c.item, c.notes || "", "");

const settle = (c) => {
  const feedback = [];
  const settled = settleBuyerListingCopy(
    c.draft,
    c.item.name,
    c.second ? () => c.second : null,
    c.item.brand || "",
    c.previous || "",
    c.item,
    feedback
  );
  return { settled, feedback };
};

test("every build matches the shared expectation and is stable when rebuilt", () => {
  for (const c of CASES.builds) {
    const built = build(c);
    assert.equal(built, c.expect.built, c.id);
    assert.equal(finalizeListingDescription(built, c.item, c.notes || "", ""), built, c.id);
    assert.equal(nameLead(c.item.name), c.expect.lead, c.id);
    assert.deepEqual(listingQualityIssues(c.stored, c.item), c.expect.issuesBefore, c.id);
    assert.deepEqual(listingQualityIssues(built, c.item), c.expect.issuesAfter, c.id);
  }
});

test("every size sentence matches the shared expectation", () => {
  for (const c of CASES.sizes) {
    assert.equal(naturalSizeSentence(c.item, ""), c.expect, c.item.name);
  }
});

test("opening checks match the shared expectation", () => {
  for (const c of CASES.openings) {
    const maker = requiredMaker(c.name, c.brand || "");
    const art = isArtName(c.name, c.category || "");
    assert.equal(openingParrotsName(c.opening, c.name, maker, art), c.expect.parrots, c.opening);
    assert.equal(openingIsFragment(c.opening), c.expect.fragment, c.opening);
  }
});

test("the maker taken from the name matches the shared expectation", () => {
  for (const c of CASES.leads) {
    assert.equal(nameLead(c.name), c.expect.lead, c.name);
    assert.equal(makerIsCertain(c.name, ""), c.expect.certain, c.name);
  }
});

test("sentence classes match the shared expectation", () => {
  for (const c of CASES.sentences) {
    assert.equal(unpublishableSentence(c.sentence, c.name), c.expect.unpublishable, c.sentence);
    assert.equal(inventedTitleSentence(c.sentence, c.name), c.expect.inventedTitle, c.sentence);
  }
});

test("settle outcomes match the shared expectation", () => {
  for (const c of CASES.settles) {
    const { settled, feedback } = settle(c);
    const e = c.expect;
    assert.equal(settled.description, e.description, c.id);
    assert.deepEqual(settled.quality, e.quality, c.id);
    assert.equal(settled.weak, e.weak, c.id);
    assert.equal(settled.flagged, e.flagged, c.id);
    assert.equal(settled.keptPrevious, e.keptPrevious, c.id);
    assert.equal(settled.regenerated, e.regenerated, c.id);
    assert.deepEqual(feedback, e.feedback, c.id);
  }
});

test("small decor gets no room-fit clause and no filler", () => {
  for (const id of [
    "velvet-ornament",
    "silver-ornament",
    "mackenzie-harlequin",
    "black-white-gold-ornament",
    "candlestick-22",
  ]) {
    const lowered = build(BUILDS[id]).toLowerCase();
    assert.ok(!lowered.includes("everyday rooms"), id);
    assert.ok(!lowered.includes("without feeling oversized"), id);
    assert.ok(!lowered.includes("unspecified"), id);
    assert.ok(!lowered.includes("mixed materials"), id);
    assert.ok(!lowered.includes("made from velvet"), id);
  }
});

test("round ornaments read as across", () => {
  for (const id of ["velvet-ornament", "silver-ornament", "mackenzie-harlequin", "black-white-gold-ornament"]) {
    const built = build(BUILDS[id]);
    assert.ok(built.includes(" inches across and "), id);
    assert.ok(!built.includes(" inches deep"), id);
  }
});

test("the same category at different sizes gets different sentences", () => {
  const sentences = CASES.sizes
    .filter((c) => c.item.name.startsWith("Rustic Ornate Candlestick"))
    .map((c) => c.expect);
  assert.equal(sentences.length, 5);
  assert.equal(new Set(sentences.map((s) => s.slice(s.indexOf(",") + 1))).size, 5);
});

test("no size in the opening after the build", () => {
  for (const c of CASES.builds) {
    assert.ok(!listingQualityIssues(build(c), c.item).includes("opening_has_size"), c.id);
  }
});

test("pinned wording is unchanged", () => {
  assert.ok(build(BUILDS["terpning-giclee"]).includes("It measures 39.5 inches wide by 45.5 inches high."));
  assert.ok(build(BUILDS["liz-cummings-art"]).includes("It measures 31 inches wide by 70 inches high."));
  assert.ok(
    build(BUILDS["jaipur-rug"]).includes(
      "This is an 8 by 10 rug that anchors a seating group without feeling oversized."
    )
  );
  assert.ok(
    build(BUILDS["luxury-handbag"]).includes("It measures 12 inches wide, 7 inches deep, and 8.5 inches tall.")
  );
  assert.ok(build(BUILDS["greg-sheres-armoire"]).includes("so it works along a bedroom wall or hallway."));
});

test("artwork never reports missing facts or an invented title", () => {
  const c = BUILDS["liz-cummings-art"];
  const built = build(c);
  const lowered = built.toLowerCase();
  assert.ok(!lowered.includes("record"));
  assert.ok(!lowered.includes("does not state"));
  assert.ok(!lowered.includes("work title"));
  assert.ok(!lowered.includes("titled"));
  assert.ok(built.includes("Liz Cummings"));
  const issues = listingQualityIssues(built, c.item);
  assert.ok(issues.includes(QUALITY_OPENING_FRAGMENT));
  assert.ok(issues.includes(QUALITY_RESTATED));
  assert.deepEqual(listingQualityIssues(build(BUILDS["liz-cummings-art-good"]), c.item), []);
});

test("a real title in the name is kept", () => {
  const c = BUILDS["terpning-giclee"];
  const built = build(c);
  assert.ok(built.includes("White Water Passage"));
  assert.ok(built.includes("Howard Terpning"));
  assert.deepEqual(listingQualityIssues(built, c.item), []);
});

test("AS IS and real damage survive once", () => {
  const built = build(BUILDS["nightstand-as-is"]);
  assert.equal(built.split("AS IS").length - 1, 1);
  assert.ok(built.includes("scratches on the top"));
});

test("the Scottsdale close appears once in a single paragraph", () => {
  for (const c of CASES.builds) {
    if (c.item.merchType === "luxury") continue;
    const built = build(c);
    assert.equal(built.split("Pickup is available in Scottsdale").length - 1, 1, c.id);
    assert.ok(!built.includes("\n"), c.id);
  }
});

test("model-written freight lines are stripped from the draft", () => {
  const { settled } = settle(SETTLES["model-freight-and-delivery-lines-are-stripped"]);
  assert.ok(!settled.description.includes("Pickup is available"));
  assert.ok(!settled.description.includes("Freight"));
});

test("a certain maker is inserted and an uncertain maker is only flagged", () => {
  const inserted = settle(SETTLES["maker-inserted-and-unknown-dropped"]).settled;
  assert.ok(inserted.description.includes("Mackenzie Childs"));
  assert.ok(!inserted.description.includes("unspecified"));
  const artist = settle(SETTLES["artist-after-by-is-inserted-when-two-words"]).settled;
  assert.ok(artist.description.includes("Liz Cummings"));
  const uncertain = settle(SETTLES["uncertain-maker-is-flagged-not-inserted"]).settled;
  assert.ok(!uncertain.description.includes("Baccarat"));
  assert.deepEqual(uncertain.quality, [QUALITY_MAKER_MISSING]);
  assert.equal(nameLead("Side By Side Refrigerator"), "");
});

test("weaker copy is never saved over a better previous one", () => {
  const c = SETTLES["parrot-twice-keeps-better-previous"];
  const { settled, feedback } = settle(c);
  assert.equal(settled.description, c.previous);
  assert.equal(settled.keptPrevious, true);
  assert.ok(feedback.length > 0);
});

test("regeneration is asked for at most once", () => {
  const c = SETTLES["parrot-twice-keeps-better-previous"];
  let calls = 0;
  settleBuyerListingCopy(
    c.draft,
    c.item.name,
    () => {
      calls += 1;
      return c.second;
    },
    "",
    "",
    c.item,
    []
  );
  assert.equal(calls, 1);
});
