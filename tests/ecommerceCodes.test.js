import test from "node:test";
import assert from "node:assert/strict";
import axios from "axios";
import {
  ECOMMERCE_CATEGORY_LETTERS,
  ECOMMERCE_VERTICALS,
  ecommerceCategoryLetter,
  planListingCodeTags,
  resolveEcommerceCodes,
} from "../ecommerceTags.js";
import {
  handleIntakeListingUpdate,
  intakeMirrorFactHash,
  mergeProtectedListingTags,
  mirrorShopifyProductToIntake,
  resetIntakeMirrorRecentForTests,
  resetListingUpdateCoalesceForTests,
  resetMirrorWebhookCoalesceForTests,
  resetTraxiaExtraCacheForTests,
  resolveIntakeShopifyTaxonomy,
  tagMakerTokens,
} from "../lib/shopifyIntakeImages.js";
import { resetShopifyThrottleForTests } from "../lib/shopifyThrottle.js";

/**
 * E-commerce codes staff type in Traxia (FH / LG + a category letter) show up
 * as Shopify tags. They drive the Intake dropdowns (the mirror sends the
 * product's tags to Pricing Engine) and an Intake listing write leaves them in
 * place unless a person changed the dropdowns in the Intake app.
 * Tables: Furniture Tags.docx and Handbag Tags.docx.
 *
 * Shopify and Intake are an in-memory backend behind the axios adapter.
 */

process.env.SHOPIFY_STORE = "codes-test-store";
process.env.SHOPIFY_ACCESS_TOKEN = "test-token";
process.env.INTAKE_SESSION_COOKIE = "session=test";
process.env.INTAKE_API_BASE = "https://intake.codes.test";
delete process.env.INTAKE_SYNC_HOOK_SECRET;

const ITEM_ID = "bbbbbbbb-cccc-4ddd-8eee-ffffffffffff";
const PRODUCT_ID = "9717446902019";
const BODY =
  "A vintage red fox fur jacket with a notched collar, a satin lining and two slash pockets at the hip.";

function makeWorld({ tags = "FH, D, chair", stampReply = {} } = {}) {
  const state = {
    calls: [],
    stamps: [],
    stampReply,
    product: {
      id: Number(PRODUCT_ID),
      title: "Vintage Red Fox Fur Jacket",
      body_html: `<p>${BODY}</p>`,
      tags,
      status: "active",
      updated_at: new Date().toISOString(),
      variants: [{ id: 501, sku: "TRX-9", price: "450.00" }],
    },
    metafields: [],
  };
  const reply = (config, status, data) => ({
    data,
    status,
    statusText: String(status),
    headers: {},
    config,
    request: {},
  });
  state.adapter = async (config) => {
    const url = `${config.baseURL || ""}${config.url || ""}`;
    const method = String(config.method || "get").toUpperCase();
    const body = typeof config.data === "string" && config.data ? JSON.parse(config.data) : config.data;
    const shopify = /myshopify\.com/.test(url);
    const path = url
      .replace(/^https?:\/\/[^/]+/, "")
      .replace(/^\/admin\/api\/[^/]+/, "")
      .replace(/\?.*$/, "");
    state.calls.push({ method, path, shopify, body });
    if (shopify) {
      if (path === `/products/${PRODUCT_ID}.json` && method === "GET") {
        return reply(config, 200, { product: structuredClone(state.product) });
      }
      if (path === `/products/${PRODUCT_ID}.json` && method === "PUT") {
        Object.assign(state.product, body.product);
        return reply(config, 200, { product: structuredClone(state.product) });
      }
      if (/metafields/.test(path) && method === "GET") return reply(config, 200, { metafields: state.metafields });
      if (/metafields/.test(path)) return reply(config, 201, { metafield: { id: 1, ...(body?.metafield || {}) } });
      return reply(config, 404, {});
    }
    if (/\/items\/by-shopify-identity$/.test(path)) {
      return reply(config, 200, {
        byProductId: {
          id: ITEM_ID,
          name: state.product.title,
          shopifyProductId: PRODUCT_ID,
          traxiaSku: "TRX-9",
          notes: "",
          conditionIssues: "",
          description: BODY,
          dimWidth: 22,
          dimDepth: null,
          dimHeight: 33,
          weightLbs: null,
          retailPrice: 450,
          shopifyLinkState: "linked",
        },
        bySku: null,
      });
    }
    if (/\/shopify-image-sync$/.test(path) && method === "POST") {
      state.stamps.push(body);
      return reply(config, 200, { ok: true, ...state.stampReply });
    }
    return reply(config, 200, { ok: true });
  };
  state.productPuts = () =>
    state.calls.filter((call) => call.shopify && call.method === "PUT" && call.path === `/products/${PRODUCT_ID}.json`);
  state.tagList = () =>
    String(state.product.tags)
      .split(",")
      .map((tag) => tag.trim())
      .filter(Boolean);
  axios.defaults.adapter = state.adapter;
  resetShopifyThrottleForTests();
  resetListingUpdateCoalesceForTests();
  resetTraxiaExtraCacheForTests();
  resetMirrorWebhookCoalesceForTests();
  resetIntakeMirrorRecentForTests();
  return state;
}

/** What Pricing Engine posts when a name changes (listing_delta_payload): name, body and a tag refresh. */
function nameSavePayload(over = {}) {
  return {
    intakeItemId: ITEM_ID,
    shopifyProductId: PRODUCT_ID,
    allowOpenAi: false,
    syncFields: ["name", "body", "tags"],
    name: "Vintage Canadian Red Fox Fur Jacket",
    description: BODY,
    replaceBody: true,
    rewriteBody: true,
    notes: "",
    conditionIssues: "",
    clearedNote: true,
    ecommerceTags: "FH, X, vintage-fur-jacket, red-fox-fur",
    merchType: "furniture",
    websiteCategory: "X",
    category: "COAT",
    brand: "",
    intakeItem: { id: ITEM_ID, description: BODY, notes: "", conditionIssues: "", ecommerceTags: "FH, X" },
    ...over,
  };
}

/** A tags-only save from Intake (a dropdown or the tags field changed). */
function tagSavePayload(over = {}) {
  return {
    intakeItemId: ITEM_ID,
    shopifyProductId: PRODUCT_ID,
    allowOpenAi: false,
    syncFields: ["tags"],
    ecommerceTags: "LG, H, vintage-fur-jacket",
    merchType: "luxury",
    websiteCategory: "H",
    category: "COAT",
    brand: "",
    ...over,
  };
}

// --- the tables ------------------------------------------------------------

test("tables are the two documents; a letter is read inside its vertical", () => {
  assert.deepEqual(Object.keys(ECOMMERCE_VERTICALS), ["FH", "LG"]);
  assert.deepEqual(ECOMMERCE_CATEGORY_LETTERS.FH, {
    A: "Art / Mirrors", B: "Bedroom", D: "Dining Room", L: "Living Room", O: "Office Den",
    R: "Rugs", X: "Accessories", P: "Outdoor / Patio", G: "Lighting",
  });
  assert.deepEqual(ECOMMERCE_CATEGORY_LETTERS.LG, {
    A: "Accessories", B: "Backpacks", C: "Crossbody", H: "Handbags", L: "Luggage", O: "Other",
    S: "Small Bags", T: "Totes", W: "Wallets", R: "Scarves", E: "Belts",
    NK: "Necklaces", RG: "Rings", BR: "Bracelets", ER: "Earrings", OJ: "Other Jewelry",
  });
  const lux = resolveEcommerceCodes("LG, O, vintage-fur-jacket");
  assert.deepEqual([lux.vertical, lux.letter], ["LG", "O"]);
  assert.equal(ECOMMERCE_CATEGORY_LETTERS[lux.vertical][lux.letter], "Other");
  const home = resolveEcommerceCodes(["FH", "O", "desk"]);
  assert.equal(ECOMMERCE_CATEGORY_LETTERS[home.vertical][home.letter], "Office Den");
  assert.equal(resolveIntakeShopifyTaxonomy({ merchType: "luxury", websiteCategory: "O" }).category.trim(), "Other");
  assert.equal(resolveIntakeShopifyTaxonomy({ merchType: "furniture", websiteCategory: "O" }).category, "Office Den");
  // Legacy "J → Other Jewelry" and "Watches → Accessories" (Handbag Tags.docx).
  assert.equal(ecommerceCategoryLetter("LG", "J"), "OJ");
  assert.equal(ecommerceCategoryLetter("LG", "Watches"), "A");
  assert.equal(resolveIntakeShopifyTaxonomy({ merchType: "luxury", websiteCategory: "J" }).category, "Other Jewelry");
  // A code that is in neither document, or only in the other vertical, is a plain tag.
  const plain = resolveEcommerceCodes("LG, X, Z, QQ, fur");
  assert.deepEqual([plain.vertical, plain.letter, plain.letters], ["LG", "", []]);
});

test("jewelry codes are not read as invented maker names", () => {
  const drop = tagMakerTokens("LG, NK, RG, BR, ER, OJ, J", "Gold Chain Necklace", "", "JEWELRY", "");
  for (const code of ["nk", "rg", "br", "er", "oj", "j"]) assert.equal(drop.has(code), false, code);
});

// --- the write plan ----------------------------------------------------------

test("plan: codes on the product stay, a missing one is filled, a second letter is never added", () => {
  const keep = planListingCodeTags({ existingTags: "LG, O, fur", intakeVertical: "furniture", intakeLetter: "X" });
  assert.deepEqual([keep.mode, keep.vertical, keep.letter, keep.add, keep.remove], ["protect", "LG", "O", [], []]);

  const sameVertical = planListingCodeTags({ existingTags: "FH, A", intakeVertical: "furniture", intakeLetter: "X" });
  assert.deepEqual([sameVertical.letter, sameVertical.add, sameVertical.remove], ["A", [], []]);

  const noLetter = planListingCodeTags({ existingTags: "FH, oak", intakeVertical: "furniture", intakeLetter: "B" });
  assert.deepEqual([noLetter.letter, noLetter.add], ["B", ["B"]]);

  const nothing = planListingCodeTags({ existingTags: "oak", intakeVertical: "luxury", intakeLetter: "H" });
  assert.deepEqual([nothing.vertical, nothing.letter, nothing.add], ["LG", "H", ["LG", "H"]]);

  // Staff's vertical with no letter: Intake's letter belongs to another vertical and is not added.
  const otherVertical = planListingCodeTags({ existingTags: "LG", intakeVertical: "furniture", intakeLetter: "B" });
  assert.deepEqual([otherVertical.vertical, otherVertical.letter, otherVertical.add], ["LG", "", []]);
});

test("plan: a person's change in the Intake app replaces the codes", () => {
  const plan = planListingCodeTags({
    existingTags: "LG, O, fur",
    intakeVertical: "luxury",
    intakeLetter: "H",
    authority: "intake",
  });
  assert.deepEqual([plan.mode, plan.vertical, plan.letter], ["replace", "LG", "H"]);
  assert.deepEqual(plan.remove.sort(), ["LG", "O"]);
  assert.deepEqual(plan.add, ["LG", "H"]);
  // A new vertical takes the old vertical's letter with it.
  const cross = planListingCodeTags({
    existingTags: "FH, X, oak",
    intakeVertical: "luxury",
    intakeLetter: "T",
    authority: "intake",
  });
  assert.deepEqual(cross.remove.sort(), ["FH", "X"]);
  assert.deepEqual(cross.add, ["LG", "T"]);
});

test("plan: Intake's own last codes may be replaced, staff's may not", () => {
  const own = planListingCodeTags({ existingTags: "FH, X", intakeVertical: "furniture", intakeLetter: "B", base: "FH|X" });
  assert.deepEqual([own.mode, own.letter, own.remove.sort(), own.add], ["replace", "B", ["FH", "X"], ["FH", "B"]]);
  // The product no longer holds what Intake put there: staff changed it.
  const staff = planListingCodeTags({ existingTags: "FH, A", intakeVertical: "furniture", intakeLetter: "B", base: "FH|X" });
  assert.deepEqual([staff.mode, staff.letter, staff.add, staff.remove], ["protect", "A", [], []]);
});

test("plan: two letters and no way to tell leave every code tag untouched", () => {
  const plan = planListingCodeTags({ existingTags: "FH, A, X", intakeVertical: "furniture", intakeLetter: "A" });
  assert.deepEqual([plan.mode, plan.conflicts, plan.add, plan.remove], ["conflict", ["multiple_category_tags"], [], []]);
});

// --- tags a person typed -----------------------------------------------------

test("merge: tags already on the product are never dropped; only what Intake removed goes", () => {
  const plan = planListingCodeTags({ existingTags: "LG, O", intakeVertical: "luxury", intakeLetter: "O" });
  const merged = mergeProtectedListingTags({
    existingTags: "LG, O, heirloom, estate-find, ZZ, X, Width: 20, Category: COAT, Location: A1, Luxury Goods",
    ecommerceTags: "LG, O, vintage-fur-jacket, red-fox-fur",
    plan,
    dims: { width: 22, height: 33 },
    removedTags: ["estate-find", "O"],
  });
  // Typed tags, an unknown code and a letter of the other vertical all stay.
  for (const tag of ["LG", "O", "heirloom", "ZZ", "X", "Category: COAT"]) assert.ok(merged.includes(tag), tag);
  // Intake's descriptive tags are added.
  for (const tag of ["vintage-fur-jacket", "red-fox-fur"]) assert.ok(merged.includes(tag), tag);
  // A tag a person took off the Intake field is the only descriptive tag removed.
  assert.equal(merged.includes("estate-find"), false);
  // Machine tags: the size is rewritten, the location and our own department label go.
  assert.deepEqual(merged.filter((tag) => /^(Width|Height):/.test(tag)), ["Width: 22", "Height: 33"]);
  assert.equal(merged.includes("Location: A1"), false);
  assert.equal(merged.includes("Luxury Goods"), false);
  assert.equal(merged.filter((tag) => tag === "O").length, 1);
});

// --- Shopify → Intake (the mirror) -------------------------------------------

test("mirror sends the product's tags, so LG, O reaches the Intake dropdowns", async () => {
  const world = makeWorld({ tags: "LG, O, Category: COAT, Width: 22, Height: 33" });
  const result = await mirrorShopifyProductToIntake(structuredClone(world.product), { log: () => {} });
  assert.equal(result.ok, true);
  const stamp = world.stamps.at(-1);
  assert.equal(stamp.shopifyTags, "LG, O, Category: COAT, Width: 22, Height: 33");
  // Codes alone never ask for a description rewrite, and nothing is written to Shopify.
  assert.equal(stamp.resyncListing, false);
  assert.equal(world.productPuts().length, 0);
});

test("mirror: a tag-only change is a change (FH, O after FH, D), the same tags again are not", async () => {
  const world = makeWorld({ tags: "FH, D" });
  await mirrorShopifyProductToIntake(structuredClone(world.product), { log: () => {} });
  assert.equal(world.stamps.length, 1);
  // The same webhook again within the freshness window: nothing to mirror.
  const repeat = await mirrorShopifyProductToIntake(structuredClone(world.product), { log: () => {} });
  assert.equal(repeat.reason, "already_mirrored");
  assert.equal(world.stamps.length, 1);
  // Staff change the letter in Traxia. Title, price and description are the same.
  const before = intakeMirrorFactHash(world.product);
  world.product.tags = "FH, O";
  assert.notEqual(intakeMirrorFactHash(world.product), before);
  await mirrorShopifyProductToIntake(structuredClone(world.product), { log: () => {} });
  assert.equal(world.stamps.length, 2);
  assert.equal(world.stamps.at(-1).shopifyTags, "FH, O");
  assert.equal(world.productPuts().length, 0);
});

test("mirror: two letters with a known new one, the old letter is taken off with a tags-only write", async () => {
  // Pricing Engine holds the last synced codes and names the old letter.
  const world = makeWorld({ tags: "FH, A, X, painting", stampReply: { removeShopifyTags: ["A"] } });
  await mirrorShopifyProductToIntake(structuredClone(world.product), { log: () => {} });
  const puts = world.productPuts();
  assert.equal(puts.length, 1);
  assert.deepEqual(Object.keys(puts[0].body.product).sort(), ["id", "tags"]);
  assert.deepEqual(world.tagList(), ["FH", "X", "painting"]);
});

test("mirror: two letters with no way to tell, nothing is written", async () => {
  const world = makeWorld({ tags: "FH, A, X, painting", stampReply: { ecommerceCodeConflict: ["multiple_category_tags"] } });
  await mirrorShopifyProductToIntake(structuredClone(world.product), { log: () => {} });
  assert.equal(world.productPuts().length, 0);
  assert.deepEqual(world.tagList(), ["FH", "A", "X", "painting"]);
  // Only a code can be named for removal: a descriptive tag never is.
  const other = makeWorld({ tags: "FH, A, painting", stampReply: { removeShopifyTags: ["painting"] } });
  await mirrorShopifyProductToIntake(structuredClone(other.product), { log: () => {} });
  assert.equal(other.productPuts().length, 0);
});

// --- Intake → Shopify (the listing write) -----------------------------------

test("a later Intake name save keeps LG, O on the product and adds no second letter", async () => {
  const world = makeWorld({ tags: "LG, O, heirloom, Category: COAT" });
  const result = await handleIntakeListingUpdate(nameSavePayload());
  assert.equal(result.status, "updated");
  const tags = world.tagList();
  assert.ok(tags.includes("LG") && tags.includes("O"));
  assert.equal(tags.includes("FH"), false);
  assert.equal(tags.includes("X"), false);
  // The tag a person typed is still there, and Intake's descriptive tags were added.
  assert.ok(tags.includes("heirloom"));
  assert.ok(tags.includes("vintage-fur-jacket"));
  // The name is the one the person wrote. The taxonomy follows the codes on the product.
  assert.equal(world.product.title, "Vintage Canadian Red Fox Fur Jacket");
  assert.equal(result.ecommerceCodes.mode, "protect");
  assert.equal(result.ecommerceCodes.codes, "LG|O");
  assert.equal(result.taxonomy.department, "Luxury Goods");
  assert.equal(String(world.product.product_type).trim(), "Other");
  // The stamp that follows carries the product's tags, so Intake's dropdowns follow them.
  assert.match(world.stamps.at(-1).shopifyTags, /^LG, O, heirloom/);
});

test("a later Intake category change replaces the letter on the product", async () => {
  const world = makeWorld({ tags: "LG, O, heirloom" });
  const result = await handleIntakeListingUpdate(tagSavePayload({ taxonomyAuthority: "intake" }));
  assert.equal(result.status, "updated");
  const tags = world.tagList();
  assert.ok(tags.includes("LG") && tags.includes("H"));
  assert.equal(tags.includes("O"), false);
  assert.ok(tags.includes("heirloom"));
  assert.equal(result.ecommerceCodes.mode, "replace");
  assert.equal(world.product.product_type, "Handbags");
  // A tag-only change does not send a description.
  const put = world.productPuts()[0].body.product;
  assert.equal(Object.prototype.hasOwnProperty.call(put, "body_html"), false);
  assert.equal(Object.prototype.hasOwnProperty.call(put, "title"), false);
});

test("a later Intake vertical change replaces both codes", async () => {
  const world = makeWorld({ tags: "FH, X, oak" });
  await handleIntakeListingUpdate(tagSavePayload({ taxonomyAuthority: "intake" }));
  assert.deepEqual(world.tagList().filter((tag) => /^[A-Z]{1,2}$/.test(tag)).sort(), ["H", "LG"]);
});

test("a tag refresh without a person's change never replaces staff's letter", async () => {
  // Intake still says X. Staff typed A in Traxia after Intake's last write (base FH|X).
  const world = makeWorld({ tags: "FH, A, painting" });
  await handleIntakeListingUpdate(
    tagSavePayload({ ecommerceTags: "FH, X, oil-painting", merchType: "furniture", websiteCategory: "X", taxonomyBase: "FH|X" })
  );
  const tags = world.tagList();
  assert.ok(tags.includes("A"));
  assert.equal(tags.includes("X"), false);
});

test("two letters on the product and no person's change: code tags are left untouched", async () => {
  const world = makeWorld({ tags: "FH, A, X, painting" });
  const logs = [];
  const result = await handleIntakeListingUpdate(
    tagSavePayload({ ecommerceTags: "FH, A, oil-painting", merchType: "furniture", websiteCategory: "A" }),
    { log: (level, entry) => logs.push({ level, ...entry }) }
  );
  assert.equal(result.ecommerceCodes.mode, "conflict");
  const tags = world.tagList();
  assert.ok(tags.includes("FH") && tags.includes("A") && tags.includes("X"));
  assert.ok(logs.some((entry) => entry.event === "ecommerce_codes.conflict_kept"));
});

test("a tag a person took off the Intake field is removed; an absent tag is not", async () => {
  const world = makeWorld({ tags: "LG, O, heirloom, estate-find" });
  await handleIntakeListingUpdate(
    tagSavePayload({ ecommerceTags: "LG, O, fur", websiteCategory: "O", removedTags: ["estate-find"] })
  );
  const tags = world.tagList();
  assert.equal(tags.includes("estate-find"), false);
  assert.ok(tags.includes("heirloom"));
  assert.ok(tags.includes("fur"));
});
