import test from "node:test";
import assert from "node:assert/strict";

import {
  ecommerceTagsAuthorizeVerticalChange,
  getEcommerceClassificationFromTags,
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
