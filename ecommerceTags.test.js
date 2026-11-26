import test from "node:test";
import assert from "node:assert/strict";

import {
  ecommerceTagsAuthorizeVerticalChange,
  getEcommerceClassificationFromTags,
  getEcommerceVerticalOverrideFromTags,
  getHardFhLgVerticalLockFromTags,
} from "./ecommerceTags.js";

test("FH and category letter create a canonical furniture fingerprint", () => {
  const first = getEcommerceClassificationFromTags(["FH", "G"]);
  const reordered = getEcommerceClassificationFromTags(["G", "FH"]);

  assert.deepEqual(first, reordered);
  assert.equal(first.vertical, "furniture");
  assert.equal(first.categoryTag, "G");
  assert.equal(first.fingerprint, "V:FH|C:G");
});

test("LG jewelry and combined tag formats are parsed", () => {
  const separate = getEcommerceClassificationFromTags(["LG", "NK"]);
  const combined = getEcommerceClassificationFromTags(["Ecommerce LG: NK"]);

  assert.equal(separate.vertical, "luxury");
  assert.equal(separate.categoryTag, "NK");
  assert.equal(combined.fingerprint, separate.fingerprint);
});

test("conflicting ecommerce tags never silently choose by order", () => {
  const state = getEcommerceClassificationFromTags(["FH", "LG", "G", "X"]);

  assert.equal(state.vertical, null);
  assert.equal(state.categoryTag, null);
  assert.deepEqual(state.conflicts, ["multiple_vertical_tags", "multiple_category_tags"]);
  assert.equal(ecommerceTagsAuthorizeVerticalChange(state, "furniture"), false);
});

test("tagged inventory stays put when placement matches and repairs stale wrong placement", () => {
  const state = getEcommerceClassificationFromTags(["FH", "G"]);

  assert.equal(
    ecommerceTagsAuthorizeVerticalChange(state, "luxury"),
    true
  );
  assert.equal(
    ecommerceTagsAuthorizeVerticalChange(state, "furniture"),
    false
  );
});

test("retagging authorizes only the newly tagged vertical", () => {
  const newState = getEcommerceClassificationFromTags(["FH", "G"]);

  assert.equal(
    ecommerceTagsAuthorizeVerticalChange(newState, "luxury"),
    true
  );
});

test("untagged inventory delegates to best-guess classification", () => {
  const state = getEcommerceClassificationFromTags(["designer", "lighting"]);

  assert.equal(state.tagged, false);
  assert.equal(state.fingerprint, "");
  assert.equal(ecommerceTagsAuthorizeVerticalChange(state, "furniture"), null);
});

test("hard FH/LG lock: LG wins over FH by documented precedence, regardless of tag order", () => {
  // LG listed first, FH second.
  const lgFirst = getHardFhLgVerticalLockFromTags(["LG", "R", "FH", "X"]);
  assert.equal(lgFirst?.vertical, "luxury");
  assert.equal(lgFirst?.tag, "LG");
  assert.equal(lgFirst?.conflict, true);

  // FH listed first, LG second — LG STILL wins (precedence is order-independent).
  const fhFirst = getHardFhLgVerticalLockFromTags(["FH", "B", "LG"]);
  assert.equal(fhFirst?.vertical, "luxury");
  assert.equal(fhFirst?.tag, "LG");
  assert.equal(fhFirst?.conflict, true);

  assert.equal(getEcommerceVerticalOverrideFromTags(["FH", "B", "LG"])?.vertical, "luxury");
  assert.equal(getEcommerceVerticalOverrideFromTags(["FH", "B", "LG"])?.conflict, true);
});

test("single vertical tag carries conflict:false", () => {
  assert.equal(getEcommerceVerticalOverrideFromTags(["LG"])?.conflict, false);
  assert.equal(getEcommerceVerticalOverrideFromTags(["FH"])?.conflict, false);
  assert.equal(getHardFhLgVerticalLockFromTags(["LG"])?.conflict, false);
});

test("Burberry scarf style tags lock luxury even with Category ACCESSORIES noise", () => {
  const tags = [
    "LG",
    "R",
    "Category: ACCESSORIES",
    "Width: 57",
    "burberry-scarf",
    "Furniture & Home",
  ];
  const lock = getHardFhLgVerticalLockFromTags(tags);
  assert.equal(lock?.vertical, "luxury");
  assert.equal(lock?.tag, "LG");
  assert.equal(getEcommerceVerticalOverrideFromTags(tags)?.vertical, "luxury");
});
