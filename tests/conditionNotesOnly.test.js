import test from "node:test";
import assert from "node:assert/strict";
import {
  buyerFacingClauses,
  finalizeListingDescription,
  marketingListingBase,
  notesAreInternalOnly,
  notesWarrantAsIs,
  planLiveFlawSync,
  planSettledCleanliness,
  shopifyBodyIsIntakeBuild,
  composeShopifyBodyHtml,
  listingBuildItemFromRow,
} from "../lib/shopifyIntakeImages.js";

const BASE = "A solid walnut side chair with a carved back and an upholstered seat.";
const ITEM = { name: "Walnut Side Chair" };

const CONDITION_NOTES = [
  "top has a scratch",
  "has one deep scratch across the front",
  "top has a blemish",
  "torn on the seat",
  "Frame nick",
  "rust on base",
  "lamp not working",
  "glass is loose in frame",
  "veneer peeling on left side",
  "small hole in back panel",
  "repaired leg",
  "water ring on top",
  "fading on left arm",
  "wobbly leg",
  "one drawer sticks",
  "needs new cord",
  "pet odor",
  "smells of smoke",
  "frame has some dings",
  "pilling on cushions",
  "small scratch in top right",
  "Small scratch on the back leg",
  "Frame coming a little apart in top right",
  "noticeable wear",
];

const INTERNAL_NOTES = [
  "Additional information in Art Folder",
  "Artwork in folder",
  "Item is in the warehouse",
  "Call Mike before selling.",
  "Hold for Sarah.",
];

test("every condition note publishes a sentence", () => {
  const empty = finalizeListingDescription(BASE, ITEM, "", "");
  for (const note of CONDITION_NOTES) {
    const out = finalizeListingDescription(BASE, ITEM, note, "");
    assert.notEqual(out, empty, note);
    assert.equal(notesWarrantAsIs(note, ""), true, note);
    const plan = planSettledCleanliness({
      title: `${ITEM.name} - AS IS`,
      body: out,
      note,
      tags: "FH",
      dims: {},
    });
    assert.equal(plan.action, "none", `${note}: ${JSON.stringify(plan)}`);
  }
});

test("internal notes publish nothing and match empty", () => {
  const empty = finalizeListingDescription(BASE, ITEM, "", "");
  for (const note of INTERNAL_NOTES) {
    const out = finalizeListingDescription(BASE, ITEM, note, "");
    assert.equal(out, empty, note);
    assert.equal(notesAreInternalOnly(note, ""), true, note);
    assert.deepEqual(buyerFacingClauses(note, ""), []);
  }
});

test("mixed note keeps only the condition clause", () => {
  const out = finalizeListingDescription(
    BASE,
    ITEM,
    "Small scratch on left arm. Paperwork in office.",
    ""
  );
  assert.match(out, /scratch/i);
  assert.equal(/paperwork|office/i.test(out), false);
  const mixed = finalizeListingDescription(
    BASE,
    ITEM,
    "small tear in canvas, paperwork in Art Folder",
    ""
  );
  assert.match(mixed, /tear/i);
  assert.equal(/paperwork|folder/i.test(mixed), false);
  assert.deepEqual(buyerFacingClauses("small tear in canvas, paperwork in Art Folder", ""), [
    "small tear in canvas",
  ]);
});

test("excellent condition and no scratch keep prior behavior", () => {
  const empty = finalizeListingDescription(BASE, ITEM, "", "");
  const excellent = finalizeListingDescription(BASE, ITEM, "excellent condition", "");
  assert.match(excellent, /excellent condition/i);
  assert.equal(notesWarrantAsIs("excellent condition", ""), false);
  assert.equal(finalizeListingDescription(BASE, ITEM, "no scratch", ""), empty);
  assert.equal(notesWarrantAsIs("no scratch", ""), false);
});

test("internal paraphrase is stripped from marketing base", () => {
  const dirty = `${BASE} Additional information. Pickup is available in Scottsdale.`;
  assert.equal(/Additional information/i.test(marketingListingBase(dirty)), false);
});

test("internal-only note body matches Intake build and stays clean", () => {
  const row = {
    name: "Walnut Side Chair",
    description: BASE,
    notes: "Additional information in Art Folder",
    conditionIssues: "",
    merchType: "furniture",
  };
  const prose = finalizeListingDescription(BASE, listingBuildItemFromRow(row), row.notes, "");
  const body = composeShopifyBodyHtml(prose, {}, listingBuildItemFromRow(row));
  assert.equal(shopifyBodyIsIntakeBuild(body, row), true);
  assert.equal(
    planSettledCleanliness({ title: row.name, body, note: row.notes, tags: "FH", dims: {} }).action,
    "none"
  );
});

test("condition note body matches Intake build after weave", () => {
  const row = {
    name: "Walnut Side Chair",
    description: BASE,
    notes: "top has a scratch",
    conditionIssues: "",
    merchType: "furniture",
  };
  const prose = finalizeListingDescription(BASE, listingBuildItemFromRow(row), row.notes, "");
  const body = composeShopifyBodyHtml(prose, {}, listingBuildItemFromRow(row));
  assert.match(prose, /scratch/i);
  assert.equal(shopifyBodyIsIntakeBuild(body, row), true);
});

test("Extra Information field is the current condition source in every direction", () => {
  const empty = finalizeListingDescription(BASE, ITEM, "", "");
  const scratch = "small scratch in top right";
  const warehouse = "Box in warehouse";
  const chip = "chip on the frame";
  const mixed = "Small scratch on left arm. Paperwork in office.";
  const internalOnly = "Paperwork in office.";
  const polluted = `${BASE} There is a small scratch in the top right. Pickup is available in Scottsdale.`;
  const item = { name: "Walnut Side Chair" };

  // 1) Flaw then emptied field.
  const cleared = planLiveFlawSync({
    shopifyBody: polluted,
    liveNote: "",
    liveKnown: true,
    storedCondition: scratch,
    item,
  });
  assert.equal(cleared.condition, "");
  assert.equal(cleared.flawMissing, true);
  assert.equal(finalizeListingDescription(BASE, ITEM, "", ""), empty);
  assert.equal(notesWarrantAsIs("", ""), false);

  // 2) Flaw then internal note: silent replace, rebuild without the flaw.
  const silent = planLiveFlawSync({
    shopifyBody: polluted,
    liveNote: warehouse,
    liveKnown: true,
    storedCondition: scratch,
    item,
  });
  assert.equal(silent.condition, warehouse);
  assert.equal(silent.flawMissing, true);
  const warehouseBody = finalizeListingDescription(BASE, ITEM, warehouse, "");
  assert.equal(warehouseBody, empty);
  assert.equal(/scratch/i.test(warehouseBody), false);
  assert.equal(notesWarrantAsIs(warehouse, ""), false);
  assert.equal(
    planSettledCleanliness({
      title: "Walnut Side Chair",
      body: warehouseBody,
      note: warehouse,
      tags: "FH",
      dims: {},
    }).action,
    "none"
  );
  const afterSilent = planLiveFlawSync({
    shopifyBody: warehouseBody,
    liveNote: warehouse,
    liveKnown: true,
    storedCondition: warehouse,
    item,
  });
  assert.equal(afterSilent.flawMissing, false);

  // 3) Flaw then a different flaw.
  const chipBody = finalizeListingDescription(BASE, ITEM, chip, "");
  assert.match(chipBody, /chip/i);
  assert.equal(/scratch/i.test(chipBody), false);
  assert.equal(notesWarrantAsIs(chip, ""), true);
  const replaced = planLiveFlawSync({
    shopifyBody: polluted,
    liveNote: chip,
    liveKnown: true,
    storedCondition: scratch,
    item,
  });
  assert.equal(replaced.condition, chip);
  assert.equal(replaced.flawMissing, true);

  // 4) Internal note then flaw.
  const scratchBody = finalizeListingDescription(BASE, ITEM, scratch, "");
  assert.match(scratchBody, /scratch/i);
  assert.equal(notesWarrantAsIs(scratch, ""), true);
  const fromInternal = planLiveFlawSync({
    shopifyBody: empty,
    liveNote: scratch,
    liveKnown: true,
    storedCondition: warehouse,
    item,
  });
  assert.equal(fromInternal.condition, scratch);
  assert.equal(fromInternal.flawMissing, true);

  // 5) Mixed note then the condition part deleted.
  const afterMixed = finalizeListingDescription(BASE, ITEM, internalOnly, "");
  assert.equal(afterMixed, empty);
  assert.equal(/scratch|paperwork/i.test(afterMixed), false);
  assert.equal(notesWarrantAsIs(internalOnly, ""), false);
  assert.deepEqual(buyerFacingClauses(mixed, ""), ["Small scratch on left arm"]);
  assert.deepEqual(buyerFacingClauses(internalOnly, ""), []);
});
