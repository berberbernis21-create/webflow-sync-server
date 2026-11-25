/**
 * Every recorded flaw is published: once, in the order written, however the
 * person punctuated the note. The cases are shared with Pricing Engine
 * (tests/fixtures/condition_sentence_cases.json is the same file in both
 * repos), so the two services always build the same text.
 */
import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import {
  buyerFacingClauses,
  finalizeListingDescription,
  flawBuyerSentence,
  isGeneratedFlawSentence,
} from "../lib/shopifyIntakeImages.js";

const FIXTURE = JSON.parse(
  fs.readFileSync(new URL("./fixtures/condition_sentence_cases.json", import.meta.url), "utf8")
);
const { base: BASE, item: ITEM, shipping: SHIPPING, cases: CASES } = FIXTURE;

function expected(row) {
  return [BASE, row.condition, SHIPPING].filter(Boolean).join(" ");
}

function build(row, description = BASE) {
  return finalizeListingDescription(description, ITEM, row.notes || "", row.conditionIssues || "");
}

/** Broken text a shopper would notice. */
function proseProblems(text) {
  const problems = [];
  const value = String(text || "");
  if (/ {2,}/.test(value)) problems.push("doubled space");
  if (/\s[,.;]/.test(value)) problems.push("orphaned punctuation");
  if (/,\s*[.,;]|\.\.|;\s*\./.test(value)) problems.push("orphaned comma");
  if (/["“”]/.test(value)) problems.push("quote-wrapped sentence");
  for (const sentence of value.split(/(?<=[.!?])\s+/)) {
    if (!sentence) continue;
    if (/^[a-z]/.test(sentence)) problems.push(`lower-case start: ${sentence}`);
    if (/\b(?:and|or|but|with|the|a|an|of|on|in|at|to)[.!?]?$/i.test(sentence)) {
      problems.push(`dangling joining word: ${sentence}`);
    }
    if (/^(?:and|or|but|with)\b/i.test(sentence)) problems.push(`starts with a joining word: ${sentence}`);
  }
  return problems;
}

test("every recorded flaw is published once, in order, in every punctuation style", () => {
  assert.ok(CASES.length >= 60);
  assert.equal(new Set(CASES.map((row) => row.name)).size, CASES.length);
  for (const row of CASES) {
    const out = build(row);
    assert.equal(out, expected(row), row.name);
    assert.deepEqual(proseProblems(out), [], row.name);
    assert.ok((out.match(/AS IS/g) || []).length <= 1, row.name);
  }
});

test("each flaw appears exactly once and none is dropped", () => {
  const flaws = [
    ["scratch on the top left", "scratch on top left"],
    ["chip on the back leg", "chip on the back leg"],
    ["stain on the seat", "stain on the seat"],
    ["dent on the left side", "dent on the left side"],
  ];
  const joiners = [". ", "\n", ", ", "; ", " and ", "\n- ", " - "];
  for (let count = 2; count <= 4; count += 1) {
    for (const joiner of joiners) {
      const note = (joiner === "\n- " ? "- " : "") + flaws.slice(0, count).map((row) => row[1]).join(joiner);
      const out = finalizeListingDescription(BASE, ITEM, note, "");
      let last = -1;
      for (const [published] of flaws.slice(0, count)) {
        const first = out.indexOf(published);
        assert.ok(first > last, `${JSON.stringify(note)} publishes "${published}" in order: ${out}`);
        assert.equal(out.indexOf(published, first + 1), -1, `${JSON.stringify(note)} repeats "${published}"`);
        last = first;
      }
      for (const [published] of flaws.slice(count)) {
        assert.ok(!out.includes(published), `${JSON.stringify(note)} invents "${published}"`);
      }
      assert.deepEqual(proseProblems(out), [], note);
    }
  }
});

test("our own text is recognised: building again from what we published changes nothing", () => {
  for (const row of CASES) {
    const published = expected(row);
    assert.equal(build(row, published), published, row.name);
  }
});

test("internal text between flaws is never published", () => {
  for (const row of CASES.filter((entry) => /consignor|warehouse|art folder|internal/i.test(entry.name))) {
    const out = build(row);
    assert.ok(!/consignor|warehouse|art folder|paperwork|box/i.test(out), `${row.name}: ${out}`);
  }
});

test("removing one flaw from the note removes just that one from the listing", () => {
  const three = finalizeListingDescription(
    BASE,
    ITEM,
    "scratch on top left. chip on the back leg. stain on the seat",
    ""
  );
  assert.ok(three.includes("a chip on the back leg"));
  // The old listing is the starting text, exactly as Shopify hands it back.
  const two = finalizeListingDescription(three, ITEM, "scratch on top left. stain on the seat", "");
  assert.equal(
    two,
    `${BASE} There is a scratch on the top left and a stain on the seat. ${SHIPPING}`
  );
  const one = finalizeListingDescription(two, ITEM, "stain on the seat", "");
  assert.equal(one, `${BASE} There is a stain on the seat. ${SHIPPING}`);
  const none = finalizeListingDescription(one, ITEM, "", "");
  assert.equal(none, `${BASE} ${SHIPPING}`);
});

test("a blended flaw follows whichever note still records it", () => {
  const intake = "chip on left leg";
  const traxia = "small chip on the left leg near the foot";
  const both = finalizeListingDescription(BASE, ITEM, traxia, intake);
  assert.equal(both, `${BASE} There is a small chip on the left leg near the foot. ${SHIPPING}`);
  // The Traxia note is deleted at the source: the Intake wording stays.
  assert.equal(
    finalizeListingDescription(both, ITEM, "", intake),
    `${BASE} There is a chip on the left leg. ${SHIPPING}`
  );
  // The person deletes the Intake line instead: Traxia's wording stays.
  assert.equal(finalizeListingDescription(both, ITEM, traxia, ""), both);
  // Both gone: the flaw leaves the listing.
  assert.equal(finalizeListingDescription(both, ITEM, "", ""), `${BASE} ${SHIPPING}`);
});

test("a flaw written without a place reads the same in both services", () => {
  assert.equal(flawBuyerSentence("small scratch"), "There is a small scratch.");
  assert.equal(flawBuyerSentence("scratches"), "There are scratches.");
  assert.equal(flawBuyerSentence("SMALLT SCRATCH ON TOP"), "There is a small scratch on the top.");
  assert.equal(flawBuyerSentence("as is"), "");
  assert.equal(flawBuyerSentence("top has a scratch"), "Top has a scratch.");
});

test("the joined flaw sentence is known as ours, and honest copy is not", () => {
  assert.ok(isGeneratedFlawSentence("There is a scratch on the top left and a chip on the back leg."));
  assert.ok(
    isGeneratedFlawSentence("There is a scratch on the top left, a chip on the back leg and a stain on the seat.")
  );
  assert.ok(isGeneratedFlawSentence("There is a small scratch."));
  assert.ok(!isGeneratedFlawSentence("There is a matching ottoman and a side table."));
  assert.ok(!isGeneratedFlawSentence("There are two drawers on the left side."));
});

test("a sentence that only starts with the note words is not cut down to its tail", () => {
  const body = "Rust on base adds to the industrial look of this lamp. It has a linen shade.";
  const out = finalizeListingDescription(body, { name: "Industrial Lamp" }, "rust on base", "");
  assert.deepEqual(proseProblems(out), []);
  assert.ok(!/^adds to/.test(out), out);
});

test("a feature that only shares words with a flaw does not hide the flaw", () => {
  const body = "A lounge chair with stain-resistant fabric on the seat and a walnut frame.";
  const out = finalizeListingDescription(body, { name: "Lounge Chair" }, "stain on the seat", "");
  assert.ok(out.includes("There is a stain on the seat."), out);
  assert.ok(out.includes("stain-resistant fabric"), out);
});

test("buyer-facing clauses ignore list markers", () => {
  assert.deepEqual(buyerFacingClauses("- scratch on top left\n* chip on the back leg\n2) stain on the seat", ""), [
    "scratch on top left",
    "chip on the back leg",
    "stain on the seat",
  ]);
});
