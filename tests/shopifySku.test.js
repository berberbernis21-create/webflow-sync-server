// The SKU as Shopify shows it is sent to Intake on the stamp the mirror already
// makes: exact text, only on a sure link, never empty, and with no extra call.
// Shopify and Intake are in-memory stand-ins. Nothing leaves the process.
import test from "node:test";
import assert from "node:assert/strict";
import axios from "axios";
import {
  extractShopifySku,
  extractShopifyVariantSku,
  intakeMirrorFactHash,
  mirrorShopifyProductToIntake,
  mirrorShopifyProductToIntakeCoalesced,
  resetIntakeMirrorRecentForTests,
  resetMirrorWebhookCoalesceForTests,
  resetOwnListingWriteForTests,
  resetTraxiaExtraCacheForTests,
  selectIntakeItemForShopifyProduct,
  shopifySkuLinkStrength,
  shopifySkuStampFields,
  stampFromImageGateResult,
} from "../lib/shopifyIntakeImages.js";

process.env.SHOPIFY_STORE = "shopify-sku-store";
process.env.SHOPIFY_ACCESS_TOKEN = "test-token";
process.env.INTAKE_SESSION_COOKIE = "session=test";
process.env.INTAKE_API_BASE = "https://intake.shopify-sku.test";
delete process.env.INTAKE_SYNC_HOOK_SECRET;

const ITEM_ID = "907e51af-4050-4af9-8d1e-d781a32f4860";
const OTHER_ITEM_ID = "1b6f0c9e-7a55-4c1e-9d3b-0a4f6f2f9b11";
const PRODUCT_ID = "9827087286531";
const SKU = "94Q6KW";
const NAME = "Walnut Side Chair";
const BASE =
  "A walnut side chair with a carved back and an upholstered seat that suits a dining room or a study.";
const AT = "2026-11-24T18:00:00.000Z";

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function reply(config, status, data) {
  return { data, status, statusText: String(status), headers: {}, config, request: {} };
}

/**
 * link: how Intake knows the row.
 *   "product_id"  the row holds the Shopify product id (found by it)
 *   "traxia_sku"  the row holds no product id, only the same SKU (found by it)
 *   "title"       the row holds neither (found by its exact name only)
 */
function makeWorld({ link = "product_id", row = {}, product = {}, holdStampMs = 0 } = {}) {
  const state = {
    calls: [],
    stamps: [],
    logs: [],
    stampReply: {},
    row: {
      id: ITEM_ID,
      shopifyProductId: link === "product_id" ? PRODUCT_ID : null,
      traxiaSku: link === "title" ? null : SKU,
      name: NAME,
      description: BASE,
      notes: "",
      conditionIssues: "",
      ecommerceTags: "FH, D",
      dimWidth: 18,
      dimDepth: 18,
      dimHeight: 36,
      weightLbs: 12,
      retailPrice: 90,
      shopifyLinkState: link === "product_id" ? "linked" : null,
      ...row,
    },
    product: {
      id: Number(PRODUCT_ID),
      title: NAME,
      body_html: `<p>${BASE}</p>`,
      tags: "FH, D",
      status: "active",
      updated_at: AT,
      variants: [{ id: 1, sku: SKU, price: "90.00", grams: 5443 }],
      ...product,
    },
  };
  state.log = (level, data) => state.logs.push({ level, ...data });
  state.events = (name) => state.logs.filter((entry) => entry.event === name);

  const identityRow = () => ({ ...state.row });
  axios.defaults.adapter = async (config) => {
    const url = `${config.baseURL || ""}${config.url || ""}`;
    const method = String(config.method || "get").toUpperCase();
    const body =
      typeof config.data === "string" && config.data ? JSON.parse(config.data) : config.data;
    const path = url
      .replace(/^https?:\/\/[^/]+/, "")
      .replace(/^\/admin\/api\/[^/]+/, "")
      .replace(/\?.*$/, "");
    const shopify = /myshopify\.com/.test(url);
    state.calls.push({ method, path, body, shopify });
    if (shopify) return reply(config, 404, {});

    if (/\/items\/by-shopify-identity$/.test(path)) {
      const wantPid = String(config.params?.shopifyProductId || "");
      const wantSku = String(config.params?.traxiaSku || "").toLowerCase();
      const rowPid = String(state.row.shopifyProductId || "");
      const rowSku = String(state.row.traxiaSku || "").toLowerCase();
      return reply(config, 200, {
        byProductId: rowPid && rowPid === wantPid ? identityRow() : null,
        bySku: rowSku && rowSku === wantSku ? identityRow() : null,
      });
    }
    if (/\/items\/unlinked-by-name$/.test(path)) {
      return reply(config, 200, { matches: [], ambiguous: false, matchCount: 0 });
    }
    if (/\/items$/.test(path) && method === "GET") {
      // Name search: the row is found by its exact name.
      return reply(config, 200, { items: [identityRow()] });
    }
    if (path.endsWith(`/items/${ITEM_ID}`) && method === "GET") {
      return reply(config, 200, { item: identityRow() });
    }
    if (/\/shopify-image-sync$/.test(path) && method === "POST") {
      if (holdStampMs > 0) await sleep(holdStampMs);
      state.stamps.push(body);
      // What Pricing Engine does with the fields this test cares about.
      if (body.shopifyProductId) state.row.shopifyProductId = body.shopifyProductId;
      if (body.name) state.row.name = body.name;
      if (body.shopifySku && !state.stampReply.shopifySkuNotSaved) state.row.shopifySku = body.shopifySku;
      return reply(config, 200, { ok: true, shopifySku: state.row.shopifySku ?? null, ...state.stampReply });
    }
    return reply(config, 200, { ok: true });
  };
  resetTraxiaExtraCacheForTests();
  resetMirrorWebhookCoalesceForTests();
  resetIntakeMirrorRecentForTests();
  resetOwnListingWriteForTests();

  state.lastStamp = () => state.stamps[state.stamps.length - 1];
  state.shopifyCalls = () => state.calls.filter((call) => call.shopify);
  state.intakePosts = () => state.calls.filter((call) => !call.shopify && call.method === "POST");
  return state;
}

// ---------------------------------------------------------------------------
// Which SKU is read
// ---------------------------------------------------------------------------

test("one variant: its SKU is read exactly, only trimmed", () => {
  assert.equal(extractShopifySku({ variants: [{ sku: SKU }] }), SKU);
  assert.equal(extractShopifySku({ variants: [{ sku: "  aB-00 12/x  " }] }), "aB-00 12/x");
  assert.equal(extractShopifySku({ variants: [{ sku: "gnd4fh" }] }), "gnd4fh", "no case change");
  assert.equal(extractShopifySku({ variants: [{ sku: "007" }] }), "007");
  assert.equal(extractShopifySku({ variants: [{ sku: 94612 }] }), "94612");
  const long = "L".repeat(120);
  assert.equal(extractShopifySku({ variants: [{ sku: long }] }), long, "never cut short");
});

test("several variants: the first one that has a SKU is used", () => {
  assert.equal(
    extractShopifySku({ variants: [{ sku: "FIRST-1" }, { sku: "SECOND-2" }, { sku: "THIRD-3" }] }),
    "FIRST-1"
  );
  assert.equal(
    extractShopifySku({ variants: [{ sku: "" }, { sku: "   " }, { sku: "SECOND-2" }, { sku: "THIRD-3" }] }),
    "SECOND-2"
  );
});

test("no SKU on the product gives nothing to send, and a barcode is not a SKU", () => {
  for (const product of [
    null,
    {},
    { variants: [] },
    { variants: [{ sku: "" }] },
    { variants: [{ sku: null }, { sku: "   " }] },
    { variants: [{ barcode: "0123456789" }] },
  ]) {
    assert.equal(extractShopifySku(product), null);
    assert.deepEqual(shopifySkuStampFields(product), {});
  }
  // The older Traxia-style reader still falls back to the barcode. It is not changed.
  assert.equal(extractShopifyVariantSku({ variants: [{ barcode: "0123456789" }] }), "0123456789");
});

test("stamp fields carry the SKU and the date of the read, or nothing when Intake has it", () => {
  const product = { updated_at: AT, variants: [{ sku: ` ${SKU} ` }] };
  assert.deepEqual(shopifySkuStampFields(product), { shopifySku: SKU, shopifySkuAt: AT });
  assert.deepEqual(shopifySkuStampFields(product, { item: { shopifySku: "OLD-1" } }), {
    shopifySku: SKU,
    shopifySkuAt: AT,
  });
  assert.deepEqual(shopifySkuStampFields(product, { item: { shopifySku: null } }), {
    shopifySku: SKU,
    shopifySkuAt: AT,
  });
  assert.deepEqual(shopifySkuStampFields(product, { item: { shopifySku: SKU } }), {});
  assert.deepEqual(shopifySkuStampFields({ variants: [{ sku: SKU }] }), { shopifySku: SKU });
});

test("a SKU change is seen by the mirror's change check", () => {
  const product = { id: 1, title: NAME, variants: [{ sku: "A-1", price: "90.00" }] };
  const changed = { ...product, variants: [{ sku: "B-2", price: "90.00" }] };
  assert.notEqual(intakeMirrorFactHash(product), intakeMirrorFactHash(changed));
});

// ---------------------------------------------------------------------------
// Which links are sure enough
// ---------------------------------------------------------------------------

test("every way the mirror finds an Intake row is sorted into sure or not sure", () => {
  const linked = { id: ITEM_ID, shopifyProductId: PRODUCT_ID, traxiaSku: SKU, shopifyLinkState: "linked" };
  const skuOnly = { id: ITEM_ID, traxiaSku: SKU };
  const bare = { id: ITEM_ID, name: NAME };
  const unlinked = { id: ITEM_ID, shopifyProductId: PRODUCT_ID, shopifyLinkState: "unlinked" };
  const strength = (item, via) =>
    shopifySkuLinkStrength({ item, via, shopifyProductId: PRODUCT_ID, sku: SKU });

  // Sure: the row already holds this product id.
  let choice = selectIntakeItemForShopifyProduct({ shopifyProductId: PRODUCT_ID, sku: SKU, byProductId: linked });
  assert.equal(choice.via, "shopify_product_id");
  assert.deepEqual(strength(choice.item, choice.via), { strong: true, reason: "shopify_product_id" });

  // Sure: the row holds no product id and the same SKU.
  choice = selectIntakeItemForShopifyProduct({ shopifyProductId: PRODUCT_ID, sku: SKU, bySku: skuOnly });
  assert.equal(choice.via, "traxia_sku");
  assert.deepEqual(strength(choice.item, choice.via), { strong: true, reason: "traxia_sku" });

  // Not sure: first find by an Intake link in the product text.
  choice = selectIntakeItemForShopifyProduct({ shopifyProductId: PRODUCT_ID, sku: SKU, byExternalId: bare });
  assert.equal(choice.via, "external_link");
  assert.equal(strength(choice.item, choice.via).strong, false);

  // Not sure: first find by the exact title.
  choice = selectIntakeItemForShopifyProduct({ shopifyProductId: PRODUCT_ID, sku: SKU, byExactTitle: bare });
  assert.equal(choice.via, "exact_title");
  assert.equal(strength(choice.item, choice.via).strong, false);

  // Not sure: an unlinked row picked up again by its name.
  choice = selectIntakeItemForShopifyProduct({ shopifyProductId: PRODUCT_ID, sku: SKU, byExactTitle: unlinked });
  assert.equal(choice.via, "exact_name_relink");
  assert.deepEqual(strength(choice.item, choice.via), { strong: false, reason: "relink_by_name" });
  assert.equal(strength(bare, "exact_name_relink").strong, false);

  // Not sure: the row is tied to a different product, or there is no row.
  assert.deepEqual(strength({ id: ITEM_ID, shopifyProductId: "111", traxiaSku: SKU }, "exact_title"), {
    strong: false,
    reason: "other_product",
  });
  assert.equal(strength({ id: ITEM_ID, traxiaSku: "OTHER" }, "exact_title").strong, false);
  assert.equal(strength(null, "none").strong, false);
});

// ---------------------------------------------------------------------------
// The mirror
// ---------------------------------------------------------------------------

test("webhook mirror on a sure link sends the SKU on the one stamp it already makes", async () => {
  const world = makeWorld({ product: { variants: [{ id: 1, sku: ` ${SKU} `, price: "90.00" }] } });
  const result = await mirrorShopifyProductToIntake(structuredClone(world.product), { log: world.log });
  assert.equal(result.ok, true);
  assert.equal(world.stamps.length, 1);
  assert.equal(world.lastStamp().shopifySku, SKU);
  assert.equal(world.lastStamp().shopifySkuAt, AT);
  assert.equal(world.row.shopifySku, SKU);
  assert.equal(world.intakePosts().length, 1, "no extra call to Intake");
  assert.equal(world.shopifyCalls().length, 0, "no call to Shopify at all");
  assert.equal(world.events("shopify.intake_mirror.sku_not_saved").length, 0);
});

test("a sure link by the same Traxia SKU also sends it", async () => {
  const world = makeWorld({ link: "traxia_sku" });
  const result = await mirrorShopifyProductToIntake(structuredClone(world.product), { log: world.log });
  assert.equal(result.ok, true);
  assert.equal(world.lastStamp().shopifySku, SKU);
});

test("several variants: the first SKU is the one sent", async () => {
  const world = makeWorld({
    product: {
      variants: [
        { id: 1, sku: "", price: "90.00" },
        { id: 2, sku: "SECOND-2", price: "90.00" },
        { id: 3, sku: "THIRD-3", price: "90.00" },
      ],
    },
  });
  await mirrorShopifyProductToIntake(structuredClone(world.product), { log: world.log });
  assert.equal(world.lastStamp().shopifySku, "SECOND-2");
});

test("a product with no SKU sends no SKU field, so Intake keeps what it has", async () => {
  for (const variants of [[{ id: 1, sku: "", price: "90.00" }], [{ id: 1, price: "90.00" }], []]) {
    const world = makeWorld({ row: { shopifySku: SKU }, product: { variants } });
    const result = await mirrorShopifyProductToIntake(structuredClone(world.product), { log: world.log });
    assert.equal(result.ok, true);
    assert.equal(world.stamps.length, 1);
    assert.equal("shopifySku" in world.lastStamp(), false);
    assert.equal("shopifySkuAt" in world.lastStamp(), false);
    assert.equal(world.row.shopifySku, SKU);
  }
});

test("an unchanged SKU is not sent again", async () => {
  const world = makeWorld({ row: { shopifySku: SKU } });
  const result = await mirrorShopifyProductToIntake(structuredClone(world.product), { log: world.log });
  assert.equal(result.ok, true);
  assert.equal("shopifySku" in world.lastStamp(), false);
  assert.equal(world.intakePosts().length, 1);
  assert.equal(world.shopifyCalls().length, 0);
});

test("a SKU-only change adds the SKU to the stamp and nothing else: no rewrite, no Shopify write", async () => {
  const quiet = makeWorld({ row: { shopifySku: SKU } });
  await mirrorShopifyProductToIntake(structuredClone(quiet.product), { log: quiet.log });
  const before = quiet.lastStamp();
  const quietIntakeCalls = quiet.calls.filter((call) => !call.shopify).length;

  // Same product, same name, price, description and tags. Only the SKU reads differently.
  const world = makeWorld({
    row: { shopifySku: SKU },
    product: { variants: [{ id: 1, sku: "NEW-77x", price: "90.00", grams: 5443 }] },
  });
  const result = await mirrorShopifyProductToIntake(structuredClone(world.product), { log: world.log });
  assert.equal(result.ok, true);
  const stamp = world.lastStamp();
  assert.equal(stamp.shopifySku, "NEW-77x");
  assert.equal(stamp.shopifySkuAt, AT);
  // Nothing else on the stamp differs (the older traxiaSku fields follow the SKU as before).
  const skuKeys = new Set(["shopifySku", "shopifySkuAt", "traxiaSku", "sku"]);
  const rest = (body) => Object.fromEntries(Object.entries(body).filter(([key]) => !skuKeys.has(key)));
  assert.deepEqual(rest(stamp), rest(before));
  assert.equal(stamp.resyncListing, false, "no description rebuild is asked for");
  assert.equal(stamp.name, NAME, "the name is the one Intake already has");
  assert.equal("bodyHtml" in stamp ? stamp.bodyHtml == null : true, true);
  assert.equal(world.shopifyCalls().length, 0, "nothing is written to (or read from) Shopify");
  assert.equal(world.calls.filter((call) => !call.shopify).length, quietIntakeCalls, "no extra Intake call");
  assert.equal(world.row.name, NAME);
});

test("a weak guess by exact title does not send the SKU, and the rest of the stamp is as before", async () => {
  const world = makeWorld({ link: "title", row: { shopifySku: "KEEP-1" } });
  const result = await mirrorShopifyProductToIntake(structuredClone(world.product), { log: world.log });
  assert.equal(result.ok, true);
  assert.equal(world.stamps.length, 1);
  const stamp = world.lastStamp();
  assert.equal("shopifySku" in stamp, false);
  assert.equal(world.row.shopifySku, "KEEP-1", "an existing value is not overwritten");
  // Matching itself is unchanged: the row still gets the product id, name and Traxia SKU.
  assert.equal(stamp.shopifyProductId, PRODUCT_ID);
  assert.equal(stamp.traxiaSku, SKU);
  assert.equal(stamp.name, NAME);
  const skipped = world.events("shopify.intake_mirror.sku_not_saved");
  assert.equal(skipped.length, 1);
  assert.equal(skipped[0].matchVia, "exact_title");
  assert.equal(skipped[0].intakeItemId, ITEM_ID);
  assert.equal(skipped[0].shopifyProductId, PRODUCT_ID);

  // Next update: the row now holds the product id, so the link is sure and the SKU goes.
  resetIntakeMirrorRecentForTests();
  world.row.shopifySku = null;
  world.product.updated_at = "2026-11-24T18:05:00.000Z";
  world.product.title = `${NAME} - AS IS`;
  await mirrorShopifyProductToIntake(structuredClone(world.product), { log: world.log });
  assert.equal(world.lastStamp().shopifySku, SKU);
});

test("a SKU that is already on another Intake item is refused by Intake and logged", async () => {
  const world = makeWorld();
  world.stampReply = {
    shopifySkuNotSaved: "conflict",
    shopifySkuConflict: { itemId: ITEM_ID, otherItemId: OTHER_ITEM_ID },
  };
  const result = await mirrorShopifyProductToIntake(structuredClone(world.product), { log: world.log });
  assert.equal(result.ok, true, "the rest of the mirror is not affected");
  assert.equal(world.row.shopifySku ?? null, null);
  const conflicts = world.events("shopify.intake_mirror.sku_conflict");
  assert.equal(conflicts.length, 1);
  assert.equal(conflicts[0].level, "warn");
  assert.equal(conflicts[0].intakeItemId, ITEM_ID);
  assert.equal(conflicts[0].otherIntakeItemId, OTHER_ITEM_ID);
  assert.equal(conflicts[0].shopifyProductId, PRODUCT_ID);
  assert.equal(conflicts[0].shopifySku, SKU);
  assert.equal(/token|secret|cookie/i.test(JSON.stringify(conflicts[0])), false);
});

test("the coalesced follow-up read sends the SKU from that one fresh read", async () => {
  const world = makeWorld({ holdStampMs: 60 });
  const first = structuredClone(world.product);
  const fresh = structuredClone(world.product);
  fresh.variants = [{ id: 1, sku: "NEW-77x", price: "90.00", grams: 5443 }];
  fresh.updated_at = "2026-11-24T18:05:00.000Z";
  let freshReads = 0;
  const loader = async () => {
    freshReads += 1;
    return structuredClone(fresh);
  };
  const run = mirrorShopifyProductToIntakeCoalesced(first, {
    log: world.log,
    loadShopifyProduct: loader,
    settleMs: 10,
    probeNotes: false,
  });
  await sleep(15);
  const mid = await mirrorShopifyProductToIntakeCoalesced(structuredClone(fresh), {
    log: world.log,
    loadShopifyProduct: loader,
    settleMs: 10,
    probeNotes: false,
  });
  assert.equal(mid.coalesced, true);
  const done = await run;
  assert.equal(done.followUpRounds, 1);
  assert.equal(freshReads, 1, "one fresh Shopify read, as before");
  assert.equal(world.stamps[0].shopifySku, SKU);
  assert.equal(world.lastStamp().shopifySku, "NEW-77x");
  assert.equal(world.lastStamp().shopifySkuAt, "2026-11-24T18:05:00.000Z");
  assert.equal(world.row.shopifySku, "NEW-77x");
  assert.equal(world.shopifyCalls().length, 0);
});

test("the image step's stamp (also used by sync-all) carries the SKU of the product it already read", async () => {
  const world = makeWorld();
  await stampFromImageGateResult(
    {
      intakeItemId: ITEM_ID,
      shopifyProductId: PRODUCT_ID,
      status: "uploaded",
      uploaded: 2,
      product: structuredClone(world.product),
    },
    { log: world.log }
  );
  assert.equal(world.stamps.length, 1);
  assert.equal(world.lastStamp().status, "synced");
  assert.equal(world.lastStamp().shopifySku, SKU);
  assert.equal(world.lastStamp().shopifySkuAt, AT);
  assert.equal(world.shopifyCalls().length, 0);

  // No product in hand: nothing about the SKU is sent.
  await stampFromImageGateResult(
    { intakeItemId: ITEM_ID, shopifyProductId: PRODUCT_ID, status: "skipped_no_photos", blocked: true },
    { log: world.log }
  );
  assert.equal(world.stamps.length, 2);
  assert.equal("shopifySku" in world.lastStamp(), false);
});
