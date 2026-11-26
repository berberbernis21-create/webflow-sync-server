import test from "node:test";
import assert from "node:assert/strict";
import {
  finalizeListingDescription,
  healListingTagInputs,
  listingTagHealNeeded,
  listingTagSetsEqual,
  mergeProtectedListingTags,
  stripOrphanBrandFragments,
} from "../lib/shopifyIntakeImages.js";

const OPS_ONLY = "Used, SOFA, Location: The Lost & Found, Living Room, Furniture & Home, ACTIVE";
const FULL = "FH, L, sofa, Width: 96, Depth: 40, Height: 33, Weight: 250";

const bermanIntake = {
  id: "item-1",
  name: 'Berman Rosetti Bermani Sofa- 96"',
  merchType: "furniture",
  websiteCategory: "L",
  ecommerceTags: "",
  dimWidth: 96,
  dimDepth: 40,
  dimHeight: 33,
  weightLbs: 250,
};

test("planner: ops-only product needs tags even on a body-only write", () => {
  assert.equal(listingTagHealNeeded({ syncFields: ["body"] }, OPS_ONLY), true);
  assert.equal(listingTagHealNeeded({ syncFields: ["name"] }, OPS_ONLY), false);
  assert.equal(listingTagHealNeeded({ syncFields: ["body"] }, FULL), false);
});

test("heal rebuilds FH, room, and dims for a product that only has ops tags", async () => {
  const payload = { intakeItemId: "item-1", syncFields: ["body"], intakeItem: bermanIntake };
  const out = await healListingTagInputs(payload, { id: 9, title: bermanIntake.name, tags: OPS_ONLY }, {
    intakeItemId: "item-1",
    fullName: bermanIntake.name,
  });
  assert.equal(out.heal, true);
  assert.match(out.payload.ecommerceTags, /^FH, L/);
  assert.equal(out.payload.dims.width, 96);
  assert.equal(out.payload.dims.weightLbs, 250);
  // The caller's payload is not mutated.
  assert.equal(payload.ecommerceTags, undefined);
  const merged = mergeProtectedListingTags({
    existingTags: OPS_ONLY,
    ecommerceTags: out.payload.ecommerceTags,
    plan: { add: ["FH", "L"], remove: [] },
    dims: out.payload.dims,
    keepOpsTags: true,
  });
  for (const tag of ["Used", "SOFA", "Location: The Lost & Found", "Living Room", "Furniture & Home", "ACTIVE"]) {
    assert.ok(merged.includes(tag), `ops tag kept: ${tag}`);
  }
  for (const tag of ["FH", "L", "Width: 96", "Depth: 40", "Height: 33", "Weight: 250"]) {
    assert.ok(merged.includes(tag), `ecommerce tag added: ${tag}`);
  }
});

test("heal prefers Intake's stored ecommerce tags over a rebuild", async () => {
  const intake = { ...bermanIntake, ecommerceTags: "FH, L, sofa, upholstered" };
  const out = await healListingTagInputs(
    { intakeItemId: "item-1", syncFields: ["body"], intakeItem: intake },
    { id: 9, title: intake.name, tags: OPS_ONLY },
    { intakeItemId: "item-1", fullName: intake.name }
  );
  assert.equal(out.heal, true);
  assert.equal(out.payload.ecommerceTags, "FH, L, sofa, upholstered");
});

test("a product that already has FH and dim tags is a no-op", async () => {
  const out = await healListingTagInputs(
    { intakeItemId: "item-1", syncFields: ["body"], intakeItem: bermanIntake },
    { id: 9, title: bermanIntake.name, tags: FULL },
    { intakeItemId: "item-1", fullName: bermanIntake.name }
  );
  assert.equal(out.heal, false);
});

test("a luxury vertical is never given FH by the rebuild", async () => {
  const bag = { ...bermanIntake, name: "Gucci Tote", merchType: "luxury", websiteCategory: "T", ecommerceTags: "" };
  const out = await healListingTagInputs(
    { intakeItemId: "item-1", syncFields: ["body"], intakeItem: bag },
    { id: 9, title: bag.name, tags: "Used, ACTIVE" },
    { intakeItemId: "item-1", fullName: bag.name }
  );
  assert.match(out.payload.ecommerceTags, /^LG, T/);
  assert.doesNotMatch(out.payload.ecommerceTags, /\bFH\b/);
});

test("heal-only merge sends nothing when tags would not change", () => {
  assert.equal(listingTagSetsEqual("a, B, c", ["c", "b", "A"]), true);
  assert.equal(listingTagSetsEqual("a, b", "a, b, c"), false);
});

test("X on Furniture Accessories stays an ecommerce code", () => {
  const merged = mergeProtectedListingTags({
    existingTags: "Used",
    ecommerceTags: "FH, X, throw-pillow",
    plan: { add: ["FH", "X"], remove: [] },
    dims: { width: 18, depth: 6, height: 14, weightLbs: 2 },
    keepOpsTags: true,
  });
  assert.ok(merged.includes("X"));
  assert.ok(merged.includes("FH"));
});

const rudin = { name: "A. Rudin 772 Swivel Lounge Chair- Pair", brand: "A. Rudin", dimWidth: 33, dimDepth: 33, dimHeight: 35, weightLbs: 150 };
const rudinProse =
  "Clean-lined and modern, this pair of light gray upholstered swivel lounge chairs brings an easy, tailored look with square armrests and loose seat cushions. A. Rudin A. It measures 33 inches wide, 33 inches deep, and 35 inches tall, so it fits a living room or seating nook cleanly.";

test("orphan brand fragment A. Rudin A. never reaches the published prose", () => {
  const body = finalizeListingDescription(rudinProse, rudin, "", "");
  assert.doesNotMatch(body, /Rudin A\b/);
  assert.match(body, /It measures 33 inches wide/);
  assert.match(body, /Pickup is available in Scottsdale/);
});

test("a real brand sentence stays", () => {
  const prose = "A. Rudin made these chairs with a modern profile. They have square armrests.";
  assert.equal(stripOrphanBrandFragments(prose, rudin), prose);
  assert.equal(stripOrphanBrandFragments("Clean. A. Rudin. They swivel.", rudin), "Clean. They swivel.");
});

test("no brand means no fragment stripping", () => {
  const prose = "The sofa is clean. A. Rudin A. It fits.";
  assert.equal(stripOrphanBrandFragments(prose, { name: "Sofa", brand: "" }), prose);
});
