import test from "node:test";
import assert from "node:assert/strict";
import axios from "axios";
import {
  composeShopifyBodyHtml,
  finalizeListingDescription,
  handleIntakeListingUpdate,
  intakeRowBuildProse,
  intakeRowBuildsNothing,
  intakeRowCarriesDims,
  listingBuildItemFromRow,
  listingUpdateCoalesceSnapshotForTests,
  mirrorShopifyProductToIntake,
  planSettledCleanliness,
  resetListingGenerationMemory,
  resetListingUpdateCoalesceForTests,
  shopifyBodyFingerprint,
  shopifyBodyIsIntakeBuild,
} from "../lib/shopifyIntakeImages.js";
import { resetShopifyThrottleForTests } from "../lib/shopifyThrottle.js";

/**
 * The loop this file pins down:
 *   listing write → Shopify webhook → mirror asks Intake for a rewrite of the
 *   description the server just wrote → another listing write → ...
 *
 * A webhook for the server's own write (first write, coalesced follow-up,
 * corrective rebuild) must not ask for a rewrite. A real change made in
 * Shopify or Traxia (name, size, emptied description, cleared note, relink)
 * must still ask for one.
 *
 * Shopify and Intake are an in-memory backend behind the axios adapter. The
 * identity lookup answers in two shapes: "old" is what the live Pricing Engine
 * returns (no size, tag or brand fields), "new" also carries them.
 */

const ITEM_ID = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";
const NEUTRAL =
  "A walnut side chair with a carved back and an upholstered seat that suits a dining room or a study.";
const RH_NAME = "RH Curved Wing Grey Leather King Low Profile Bed- AS IS";
const RH_PARAGRAPH =
  "This RH curved wing grey leather king low profile bed has a tall winged headboard wrapped in grey leather and a low platform frame for a king mattress.";

process.env.SHOPIFY_STORE = "echo-test-store";
process.env.SHOPIFY_ACCESS_TOKEN = "test-token";
process.env.INTAKE_SESSION_COOKIE = "session=test";
process.env.INTAKE_API_BASE = "https://intake.echo.test";
delete process.env.INTAKE_SYNC_HOOK_SECRET;

let nextProductId = 9100;
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const grams = (lbs) => Math.round(lbs * 453.59237);

function makeWorld({ row = {}, identity = "new", weightLbs = 0, tags = "FH, D", rowReadFails = false } = {}) {
  const pid = String((nextProductId += 1));
  const state = { pid, identity, rowReadFails, linked: true, unlinked: [], calls: [], stamps: [] };
  state.row = {
    id: ITEM_ID,
    shopifyProductId: pid,
    traxiaSku: "TRX-1",
    name: "Walnut Side Chair",
    description: NEUTRAL,
    notes: "",
    conditionIssues: "",
    ecommerceTags: "FH, D",
    brand: "",
    merchType: "furniture",
    category: "CHAIR",
    dimWidth: null,
    dimDepth: null,
    dimLength: null,
    dimHeight: null,
    weightLbs: null,
    retailPrice: 90,
    shopifyLinkState: "linked",
    history: [],
    ...row,
  };
  state.product = {
    id: Number(pid),
    title: state.row.name,
    body_html: "",
    tags,
    status: "active",
    updated_at: new Date().toISOString(),
    variants: [{ id: 501, sku: "TRX-1", price: "90.00", grams: weightLbs ? grams(weightLbs) : 0 }],
  };
  const identityRow = () => {
    const r = state.row;
    const base = {
      id: r.id,
      name: r.name,
      shopifyProductId: r.shopifyProductId,
      traxiaSku: r.traxiaSku,
      notes: r.notes,
      conditionIssues: r.conditionIssues,
      description: r.description,
      shopifyLinkState: r.shopifyLinkState,
      lastShopifyProductId: null,
    };
    if (state.identity === "old") return base;
    return {
      ...base,
      publicNotes: r.notes,
      ecommerceTags: r.ecommerceTags,
      brand: r.brand,
      merchType: r.merchType,
      category: r.category,
      dimWidth: r.dimWidth,
      dimDepth: r.dimDepth,
      dimLength: r.dimLength,
      dimHeight: r.dimHeight,
      weightLbs: r.weightLbs,
    };
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
    const shopify = /myshopify\.com\/admin\//.test(url);
    const path = url.replace(/^https?:\/\/[^/]+/, "").replace(/^\/admin\/api\/[^/]+/, "").replace(/\?.*$/, "");
    state.calls.push({ method, path, shopify, body });
    if (shopify) {
      if (path === `/products/${pid}.json` && method === "GET") {
        return reply(config, 200, { product: structuredClone(state.product) });
      }
      if (path === `/products/${pid}.json` && method === "PUT") {
        const { variants, id: _id, ...rest } = body.product;
        Object.assign(state.product, rest);
        if (Array.isArray(variants) && variants[0]?.price != null) {
          state.product.variants[0].price = String(variants[0].price);
        }
        state.product.updated_at = new Date(Date.now() + state.calls.length).toISOString();
        return reply(config, 200, { product: structuredClone(state.product) });
      }
      if (/metafields/.test(path) && method === "GET") return reply(config, 200, { metafields: [] });
      if (/metafields/.test(path)) {
        return reply(config, 201, { metafield: { id: 7000 + state.calls.length, ...(body?.metafield || {}) } });
      }
      return reply(config, 404, {});
    }
    if (/\/items\/by-shopify-identity$/.test(path)) {
      return reply(config, 200, { byProductId: state.linked ? identityRow() : null, bySku: null });
    }
    if (/\/items\/unlinked-by-name$/.test(path)) return reply(config, 200, { matches: state.unlinked });
    if (path.endsWith(`/items/${ITEM_ID}`) && method === "GET") {
      if (state.rowReadFails) return reply(config, 500, { error: "down" });
      return reply(config, 200, { item: { ...state.row } });
    }
    if (/\/shopify-image-sync$/.test(path) && method === "POST") {
      state.stamps.push(body);
      return reply(config, 200, { ok: true });
    }
    if (/\/items$/.test(path) && method === "GET") return reply(config, 200, { items: [] });
    return reply(config, 200, { ok: true });
  };
  state.productPuts = () =>
    state.calls.filter((c) => c.shopify && c.method === "PUT" && c.path === `/products/${pid}.json`).length;
  state.rowReads = () =>
    state.calls.filter((c) => !c.shopify && c.method === "GET" && c.path.endsWith(`/items/${ITEM_ID}`)).length;
  state.mirrorStamps = () => state.stamps.filter((s) => s.shopifyUpdatedAt);
  return state;
}

function use(world) {
  axios.defaults.adapter = world.adapter;
  resetShopifyThrottleForTests();
  resetListingUpdateCoalesceForTests();
  return world;
}

/** What Pricing Engine posts for a full listing push. `inline` is the newer Pricing Engine. */
function listingPayload(world, { inline = world.identity === "new" } = {}) {
  const r = world.row;
  const payload = {
    intakeItemId: r.id,
    shopifyProductId: world.pid,
    allowOpenAi: false,
    name: r.name,
    description: intakeRowBuildProse(r),
    replaceBody: true,
    rewriteBody: true,
    notes: r.notes,
    conditionIssues: r.conditionIssues,
    ecommerceTags: r.ecommerceTags,
    merchType: r.merchType,
    websiteCategory: "D",
    category: r.category,
    brand: r.brand,
    dimWidth: r.dimWidth,
    dimDepth: r.dimDepth,
    dimHeight: r.dimHeight,
    weightLbs: r.weightLbs,
  };
  if (inline) {
    payload.intakeItem = {
      id: r.id,
      description: r.description,
      notes: r.notes,
      conditionIssues: r.conditionIssues,
      ecommerceTags: r.ecommerceTags,
      dimWidth: r.dimWidth,
      dimDepth: r.dimDepth,
      dimLength: r.dimLength,
      dimHeight: r.dimHeight,
      weightLbs: r.weightLbs,
    };
  }
  return payload;
}

/** Wait until no listing-update is running or queued (follow-ups and corrective rebuilds included). */
async function drain() {
  let quiet = 0;
  for (let i = 0; i < 1200; i += 1) {
    const snap = listingUpdateCoalesceSnapshotForTests();
    quiet = snap.inflight.length || snap.pending.length ? 0 : quiet + 1;
    // The follow-up starts 250 ms after a run ends, so stay quiet for longer than that.
    if (quiet >= 10) return;
    await sleep(50);
  }
  throw new Error("listing-update chain did not settle");
}

/** One products/update webhook for the product as Shopify holds it now. */
async function webhook(world) {
  const before = world.mirrorStamps().length;
  const result = await mirrorShopifyProductToIntake(structuredClone(world.product), { log: () => {} });
  const stamps = world.mirrorStamps().slice(before);
  return { result, stamp: stamps[stamps.length - 1] || null };
}

/** Shopify already holds the description a listing write produces for this row. */
function putInSync(world) {
  const r = world.row;
  world.product.body_html = composeShopifyBodyHtml(intakeRowBuildProse(r), {
    width: r.dimWidth,
    depth: r.dimDepth ?? r.dimLength,
    height: r.dimHeight,
    weightLbs: r.weightLbs,
  });
  world.product.title = r.name;
  world.product.variants[0].grams = r.weightLbs ? grams(r.weightLbs) : 0;
  return world;
}

const SIZED = { dimWidth: 18, dimDepth: 18, dimLength: 18, dimHeight: 36, weightLbs: 12 };
const WORN = { notes: "noticeable wear", conditionIssues: "noticeable wear" };

function assertNoRewriteAsked(stamp, label = "") {
  assert.ok(stamp, `${label}: the webhook was mirrored`);
  assert.equal(stamp.resyncListing, false, `${label}: no rewrite is asked for`);
  assert.equal("bodyHtml" in stamp, false, `${label}: the description is not sent back to Intake`);
  assert.equal(stamp.bodyIsIntakeBuild, true, `${label}: the description is the Intake build`);
}

// ---------------------------------------------------------------------------
// Own writes: none of them may come back as a rewrite request.
// ---------------------------------------------------------------------------

for (const identity of ["old", "new"]) {
  test(`own listing write comes back as a webhook and asks for nothing (${identity} Pricing Engine lookup)`, async () => {
    const world = use(makeWorld({ identity, weightLbs: 12, row: { ...SIZED, ...WORN } }));
    const written = await handleIntakeListingUpdate(listingPayload(world), { log: () => {} });
    assert.equal(written.status, "updated");
    await drain();
    const puts = world.productPuts();
    const reads = world.rowReads();

    const { stamp } = await webhook(world);
    assertNoRewriteAsked(stamp, identity);
    assert.equal(
      stamp.bodyFingerprint,
      written.shopifyBodyFingerprint,
      "the webhook and the write reply describe the same description"
    );
    assert.equal(world.productPuts(), puts, "no further Shopify write");
    // The old lookup has no size fields, so the row is read once (lite). The new one needs no read.
    assert.equal(world.rowReads() - reads, identity === "old" ? 1 : 0);
  });
}

test("coalesced follow-up write comes back as a webhook and asks for nothing", async () => {
  const world = use(makeWorld({ identity: "old", weightLbs: 12, row: { ...SIZED, ...WORN } }));
  const followUps = [];
  const onFollowUpWrite = (result) => followUps.push(result);
  const first = handleIntakeListingUpdate(listingPayload(world), { log: () => {}, onFollowUpWrite });
  const second = await handleIntakeListingUpdate(listingPayload(world), { log: () => {}, onFollowUpWrite });
  assert.equal(second.deferred, true, "the second post is answered 202 deferred");
  assert.equal(second.reason, "in_flight_collapsed");
  await first;
  await drain();
  assert.equal(world.productPuts(), 2, "the first write and one follow-up");
  // The route suppresses the webhook for a write it is told about. The first
  // write is its own reply; the follow-up is reported here, once.
  assert.equal(followUps.length, 1, "the follow-up write is reported to the caller");
  assert.equal(followUps[0].shopifyProductId, world.pid);
  assert.equal(followUps[0].status, "updated");

  const { stamp } = await webhook(world);
  assertNoRewriteAsked(stamp, "follow-up");
  await drain();
  assert.equal(world.productPuts(), 2, "the follow-up's webhook starts nothing");
});

test("corrective rebuild comes back as a webhook and asks for nothing (RH bed)", async () => {
  const world = use(
    makeWorld({ identity: "old", row: { name: RH_NAME, description: RH_PARAGRAPH, category: "BED", ...WORN } })
  );
  const followUps = [];
  const written = await handleIntakeListingUpdate(listingPayload(world), {
    log: () => {},
    onFollowUpWrite: (result) => followUps.push(result),
  });
  assert.equal(written.status, "updated");
  await drain();
  const puts = world.productPuts();
  // Whether this paragraph is rebuilt depends on the cleanliness rules of the
  // tree under test. Either way the count is capped and each rebuild is reported.
  assert.ok(puts >= 1 && puts <= 3, `the write plus at most two corrective rebuilds (got ${puts})`);
  assert.equal(followUps.length, puts - 1, "each corrective rebuild is reported to the caller");
  assert.ok(followUps.every((result) => result.shopifyProductId === world.pid));

  const { stamp } = await webhook(world);
  assertNoRewriteAsked(stamp, "corrective");
  await drain();
  assert.equal(world.productPuts(), puts, "the corrective write's webhook starts nothing");
});

test("a thin body that a rebuild cannot help is written once, and its webhook asks for nothing", async () => {
  // No description and no note: the body stays thin. A rebuild reads the same
  // Intake row and cannot lengthen it, so no corrective pass is spent on it
  // and no cleanup error is stamped. Before, this ran to the cap (3 writes).
  const world = use(makeWorld({ identity: "new", row: { description: "" } }));
  const followUps = [];
  await handleIntakeListingUpdate(listingPayload(world), {
    log: () => {},
    onFollowUpWrite: (result) => followUps.push(result),
  });
  await drain();
  const puts = world.productPuts();
  assert.equal(puts, 1, "one write and no corrective rebuild for thin copy");
  assert.equal(followUps.length, 0, "nothing follows a thin body");
  assert.equal(
    world.stamps.filter((s) => /needs a cleanup/i.test(String(s.detail || ""))).length,
    0,
    "thin copy is not stamped as a sync error"
  );
  const { stamp } = await webhook(world);
  assert.equal(stamp.resyncListing, false);
  assert.equal("bodyHtml" in stamp, false);
  await drain();
  assert.equal(world.productPuts(), puts, "nothing follows the corrective writes' webhook");
});

/** Put a stand-in model in front of the world. `answer` is an HTTP status or a description. */
function withModel(world, answer) {
  const asks = [];
  const inner = world.adapter;
  axios.defaults.adapter = async (config) => {
    const url = `${config.baseURL || ""}${config.url || ""}`;
    if (!/api\.openai\.com/.test(url)) return inner(config);
    asks.push(typeof config.data === "string" ? JSON.parse(config.data) : config.data);
    const base = { statusText: "", headers: {}, config, request: {} };
    if (typeof answer === "number") return { ...base, status: answer, data: { error: "down" } };
    return {
      ...base,
      status: 200,
      data: { choices: [{ message: { content: JSON.stringify({ description: answer, tags: "FH, D" }) } }] },
    };
  };
  return asks;
}

const THIN_ROW = { description: "A walnut side chair with a carved back and an upholstered seat for a dining room." };

test("a thin item with the model unavailable: one model call, then it stops", async () => {
  const previousKey = process.env.OPENAI_API_KEY;
  process.env.OPENAI_API_KEY = "test-key";
  resetListingGenerationMemory();
  try {
    const world = use(makeWorld({ identity: "new", row: THIN_ROW }));
    const asks = withModel(world, 503);
    const post = () =>
      handleIntakeListingUpdate(
        { ...listingPayload(world), allowOpenAi: true, forceRefresh: true },
        { log: () => {} }
      );
    await post();
    await drain();
    assert.equal(asks.length, 1, "the model is asked once");
    const puts = world.productPuts();
    assert.equal(puts, 1, "the description on file is written once");
    assert.match(world.product.body_html, /walnut side chair with a carved back/i, "the previous description is kept");
    assert.doesNotMatch(world.product.body_html, /straightforward piece/i);

    // The same item is posted again and again (saves, retries, webhooks): no further model call.
    for (let i = 0; i < 5; i += 1) {
      await post();
      await drain();
    }
    const { stamp } = await webhook(world);
    assertNoRewriteAsked(stamp, "thin, model down");
    await drain();
    assert.equal(asks.length, 1, "still one model call after five more posts and a webhook");
    assert.ok(world.productPuts() <= puts + 5, `no write beyond one per post (got ${world.productPuts()})`);
    assert.equal(
      world.stamps.filter((s) => /needs a cleanup/i.test(String(s.detail || ""))).length,
      0,
      "thin copy is never stamped as a cleanup error"
    );

    // A real change to the item allows one more.
    world.row.name = "Walnut Carved Side Chair";
    await post();
    await drain();
    assert.equal(asks.length, 2, "a new name allows one more generation");
    await post();
    await drain();
    assert.equal(asks.length, 2);
  } finally {
    resetListingGenerationMemory();
    if (previousKey == null) delete process.env.OPENAI_API_KEY;
    else process.env.OPENAI_API_KEY = previousKey;
  }
});

test("a blank item with the model unavailable gets its recorded facts once, no filler", async () => {
  const previousKey = process.env.OPENAI_API_KEY;
  process.env.OPENAI_API_KEY = "test-key";
  resetListingGenerationMemory();
  try {
    const world = use(makeWorld({ identity: "new", row: { description: "", brand: "Baker" } }));
    const asks = withModel(world, 503);
    const post = () =>
      handleIntakeListingUpdate(
        { ...listingPayload(world), allowOpenAi: true, forceRefresh: true },
        { log: () => {} }
      );
    await post();
    await drain();
    assert.equal(asks.length, 1);
    assert.equal(world.productPuts(), 1);
    assert.match(world.product.body_html, /Walnut Side Chair by Baker\./);
    assert.doesNotMatch(world.product.body_html, /straightforward piece|everyday use/i);
    for (let i = 0; i < 3; i += 1) {
      await post();
      await drain();
    }
    assert.equal(asks.length, 1, "the model is not asked again for the same facts");
    assert.ok(world.productPuts() <= 4);
  } finally {
    resetListingGenerationMemory();
    if (previousKey == null) delete process.env.OPENAI_API_KEY;
    else process.env.OPENAI_API_KEY = previousKey;
  }
});

test("a model that answers thin again is not asked in a loop", async () => {
  const previousKey = process.env.OPENAI_API_KEY;
  process.env.OPENAI_API_KEY = "test-key";
  resetListingGenerationMemory();
  try {
    const world = use(makeWorld({ identity: "new", row: THIN_ROW }));
    const asks = withModel(world, "A walnut chair.");
    const post = () =>
      handleIntakeListingUpdate(
        { ...listingPayload(world), allowOpenAi: true, forceRefresh: true },
        { log: () => {} }
      );
    for (let i = 0; i < 4; i += 1) {
      await post();
      await drain();
    }
    // One generation is one call, plus at most one second try inside it when the first answer is rejected.
    assert.ok(asks.length >= 1 && asks.length <= 2, `one generation only (got ${asks.length} calls)`);
    assert.match(world.product.body_html, /carved back and an upholstered seat/i, "the longer text on file stays");
    assert.ok(world.productPuts() <= 4);
  } finally {
    resetListingGenerationMemory();
    if (previousKey == null) delete process.env.OPENAI_API_KEY;
    else process.env.OPENAI_API_KEY = previousKey;
  }
});

test("the whole chain ends: post, 202 deferred, single retry, write, webhooks", async () => {
  const world = use(makeWorld({ identity: "new", weightLbs: 12, row: { ...SIZED, ...WORN } }));
  const log = () => {};
  const first = handleIntakeListingUpdate(listingPayload(world), { log });
  const deferred = await handleIntakeListingUpdate(listingPayload(world), { log });
  assert.equal(deferred.deferred, true);
  await first;
  await drain();
  // Pricing Engine retries a deferred post exactly once.
  const retry = await handleIntakeListingUpdate(listingPayload(world), { log });
  assert.equal(retry.status, "updated");
  await drain();
  assert.equal(world.productPuts(), 3, "first write, follow-up, retry");

  // Shopify reports the last state. Nothing is asked of Intake, so no listing-update follows.
  const { stamp } = await webhook(world);
  assertNoRewriteAsked(stamp, "chain");
  assert.equal(stamp.bodyFingerprint, retry.shopifyBodyFingerprint);
  await drain();
  assert.equal(world.productPuts(), 3, "the chain ended at three writes");
  assert.equal(world.mirrorStamps().filter((s) => s.resyncListing).length, 0);
});

test("after a restart the stored item plus one new webhook still ends", async () => {
  const world = use(makeWorld({ identity: "old", weightLbs: 12, row: { ...SIZED, ...WORN } }));
  await handleIntakeListingUpdate(listingPayload(world), { log: () => {} });
  await drain();
  const puts = world.productPuts();
  // A restart loses every in-memory map. The item and the Shopify product are what is left.
  resetListingUpdateCoalesceForTests();
  resetShopifyThrottleForTests();
  world.product.updated_at = new Date(Date.now() + 60_000).toISOString();

  const { stamp } = await webhook(world);
  assertNoRewriteAsked(stamp, "restart");
  await drain();
  assert.equal(world.productPuts(), puts);
  const snap = listingUpdateCoalesceSnapshotForTests();
  assert.deepEqual([snap.inflight.length, snap.pending.length], [0, 0]);
});

test("an item with no Intake description and an empty Shopify description does not bounce", async () => {
  const world = use(makeWorld({ identity: "new", row: { description: "" } }));
  // A listing write from a row with no description and no note leaves the
  // Shopify description empty, however many times it runs.
  await handleIntakeListingUpdate(listingPayload(world), { log: () => {} });
  await drain();
  assert.equal(String(world.product.body_html || "").replace(/<[^>]+>/g, " ").trim(), "");
  // So the empty description already is the build, and the webhook asks for nothing.
  const { stamp } = await webhook(world);
  assert.equal(stamp.resyncListing, false);
  assert.equal(stamp.bodyFingerprint, "");

  // With a note there is a flaw sentence to write. It is asked for once, and
  // the webhook for that write asks for nothing more.
  const noted = use(makeWorld({ identity: "new", row: { description: "", ...WORN } }));
  assert.equal((await webhook(noted)).stamp.resyncListing, true);
  await handleIntakeListingUpdate(listingPayload(noted), { log: () => {} });
  await drain();
  assert.match(String(noted.product.body_html), /noticeable wear/i);
  noted.product.updated_at = new Date(Date.now() + 60_000).toISOString();
  const after = await webhook(noted);
  assert.equal(after.stamp.resyncListing, false, "the flaw sentence is in Shopify, nothing more is asked");
  // An older lookup that does not say what the description is: unknown, so ask.
  assert.equal(intakeRowBuildsNothing({ id: "x", name: "Chair" }), false);
  assert.equal(intakeRowBuildsNothing({ id: "x", description: null, notes: "" }), true);
});

// ---------------------------------------------------------------------------
// Real one-sided changes: each still asks for the rewrite.
// ---------------------------------------------------------------------------

test("a description emptied in Shopify still asks for a rebuild", async () => {
  const world = use(putInSync(makeWorld({ identity: "old", weightLbs: 12, row: { ...SIZED, ...WORN } })));
  world.product.body_html = "";
  const { stamp } = await webhook(world);
  assert.equal(stamp.resyncListing, true);
  assert.equal(stamp.bodyFingerprint, "", "an empty description is reported as empty");
  assert.equal(stamp.bodyIsIntakeBuild, false);
});

for (const identity of ["old", "new"]) {
  test(`a weight changed in Shopify is adopted and asks for a rebuild (${identity} Pricing Engine lookup)`, async () => {
    const world = use(putInSync(makeWorld({ identity, weightLbs: 12, row: { ...SIZED } })));
    world.product.variants[0].grams = grams(20);
    const { stamp } = await webhook(world);
    assert.equal(stamp.resyncListing, true, "the size really differs, so the description is rebuilt");
    assert.equal(stamp.weightLbs, 20, "the new weight goes to Intake as written");
    // Intake has its own description, so the Shopify one is never copied over it.
    assert.equal("bodyHtml" in stamp, false, "the old Shopify description is not handed to Intake");
  });
}

test("a width changed through a Shopify tag asks for a rebuild", async () => {
  const world = use(putInSync(makeWorld({ identity: "new", weightLbs: 12, row: { ...SIZED } })));
  world.product.tags = "FH, D, Width: 30";
  const { stamp } = await webhook(world);
  assert.equal(stamp.resyncListing, true);
  assert.equal(stamp.dimWidth, 30);
});

test("a name changed in Shopify or Traxia is passed on exactly and asks for a rebuild", async () => {
  const world = use(putInSync(makeWorld({ identity: "new", weightLbs: 12, row: { ...SIZED } })));
  const typed = "bernis karfen Walnut Side Chair - Oiled 42X23X75 - AS IS";
  world.product.title = typed;
  const { stamp } = await webhook(world);
  assert.equal(stamp.resyncListing, true);
  assert.equal(stamp.name, typed, "the title is not altered");
});

test("a note cleared in Traxia is passed on and asks for a rebuild", async () => {
  const world = use(
    putInSync(makeWorld({ identity: "new", row: { notes: "", conditionIssues: "Small scratch on the left arm." } }))
  );
  world.product.extraInformation = "";
  const { stamp } = await webhook(world);
  assert.equal(stamp.conditionIssues, "", "the cleared note reaches Intake");
  assert.equal(stamp.resyncListing, true, "the description still says the old flaw, so it is rebuilt");
});

test("a note changed on the Intake row asks for a rebuild when Shopify still shows the old one", async () => {
  const world = use(putInSync(makeWorld({ identity: "new", row: { ...WORN } })));
  world.row.notes = "Small scratch on the left arm.";
  world.row.conditionIssues = "Small scratch on the left arm.";
  const { stamp } = await webhook(world);
  assert.equal(stamp.resyncListing, true);
  assert.equal(stamp.bodyIsIntakeBuild, false);
});

test("a relink asks for the wake-up write", async () => {
  const world = use(putInSync(makeWorld({ identity: "new", row: { ...SIZED }, weightLbs: 12 })));
  world.linked = false;
  world.unlinked = [{ ...world.row, shopifyProductId: null, shopifyLinkState: "unlinked" }];
  const { stamp } = await webhook(world);
  assert.equal(stamp.resyncListing, true);
  assert.equal(stamp.shopifyLinkState, "linked");
  assert.equal(stamp.status, "pending");
});

const EDITED_IN_SHOPIFY =
  "<p>A hand-finished walnut chair made for a writing desk, with a new linen seat and tightened joints throughout.</p>";

test("a description edited in Shopify is reported as different from the Intake build", async () => {
  const world = use(putInSync(makeWorld({ identity: "new", row: { ...SIZED }, weightLbs: 12 })));
  const before = shopifyBodyFingerprint(world.product.body_html);
  world.product.body_html = EDITED_IN_SHOPIFY;
  const { stamp } = await webhook(world);
  assert.equal(stamp.bodyIsIntakeBuild, false);
  assert.notEqual(stamp.bodyFingerprint, before);
  assert.equal(stamp.bodyFingerprint, shopifyBodyFingerprint(EDITED_IN_SHOPIFY));
});

test("a description written in Shopify still reaches an Intake row that has none", async () => {
  const world = use(makeWorld({ identity: "new", row: { description: "" } }));
  world.product.body_html = EDITED_IN_SHOPIFY;
  const { stamp } = await webhook(world);
  assert.equal(stamp.bodyHtml, EDITED_IN_SHOPIFY, "the description goes to Intake unchanged");
  assert.equal(stamp.bodyIsIntakeBuild, false);
});

// ---------------------------------------------------------------------------
// Missing size fields are unknown, not changed.
// ---------------------------------------------------------------------------

test("size fields that were never sent are not a size change", async () => {
  // Old lookup (no size fields) and the row cannot be read: the size is unknown.
  const world = use(
    putInSync(makeWorld({ identity: "old", rowReadFails: true, weightLbs: 12, row: { ...SIZED } }))
  );
  const { stamp } = await webhook(world);
  assert.equal(stamp.resyncListing, false, "an unknown size is not treated as changed");
  assert.equal(stamp.weightLbs, 12, "the Shopify size still goes to Intake");
});

test("intakeRowCarriesDims tells a missing field from an empty one", () => {
  assert.equal(intakeRowCarriesDims({ id: "x", name: "Chair" }), false);
  assert.equal(intakeRowCarriesDims({ id: "x", weightLbs: null }), true);
  assert.equal(intakeRowCarriesDims({ id: "x", dim_width: 18 }), true);
  assert.equal(intakeRowCarriesDims(null), false);
});

// ---------------------------------------------------------------------------
// The build check and the fingerprint.
// ---------------------------------------------------------------------------

test("the build check calls the real builder: flaw sentence and RH paragraph are the build", () => {
  for (const row of [
    { name: "Walnut Side Chair", description: NEUTRAL, ...WORN },
    { name: RH_NAME, description: RH_PARAGRAPH, ...WORN },
    { name: RH_NAME, description: RH_PARAGRAPH, notes: "", conditionIssues: "" },
  ]) {
    const body = composeShopifyBodyHtml(intakeRowBuildProse(row), {});
    assert.equal(shopifyBodyIsIntakeBuild(body, row), true, row.name);
    assert.equal(shopifyBodyIsIntakeBuild("", row), false, "an empty description is never the build");
    assert.equal(
      shopifyBodyIsIntakeBuild(body, { ...row, description: `${row.description} It also has brass feet.` }),
      false,
      "a changed Intake description is not the build"
    );
  }
});

test("opening-size fixtures: write body matches mirror Intake build and stays clean", () => {
  const fixtures = [
    {
      name: "Jaipur Wool Area Rug - 8X10",
      brand: "Jaipur",
      websiteCategory: "R",
      description:
        "Jaipur wool area rug 8x10 in a versatile, room-ready size for living rooms. Woven in wool for a naturally resilient feel underfoot.",
      dimWidth: 8,
      dimDepth: 0.5,
      dimHeight: 10,
      weightLbs: 59,
      notes: "",
      conditionIssues: "",
      merchType: "furniture",
    },
    {
      name: "Greg Sheres Post Modern Blonde Armoire- 42X23X75 bernis karfen",
      brand: "Greg Sheres",
      description:
        "Greg Sheres Post Modern Blonde Armoire- 42X23X75 bernis karfen in a room-ready size for bedrooms. Blonde wood and clean post-modern lines make a strong storage statement.",
      dimWidth: 42,
      dimDepth: 23,
      dimHeight: 75,
      weightLbs: 120,
      notes: "",
      conditionIssues: "",
      merchType: "furniture",
    },
    {
      name: "Vintage Oak Console",
      description:
        "Vintage oak console with a warm aged finish. It sits well against a wall and softens an entry or living room.",
      dimWidth: null,
      dimDepth: null,
      dimHeight: null,
      weightLbs: 55,
      notes: "",
      conditionIssues: "",
      merchType: "furniture",
    },
    {
      name: "RH Curved Wing Grey Leather King Low Profile Bed- AS IS",
      brand: "Restoration Hardware",
      description:
        "Restoration Hardware grey leather king bed 80X90 in a versatile size for primary bedrooms. Low profile lines and soft grey leather suit a modern suite.",
      dimWidth: 80,
      dimDepth: 90,
      dimHeight: 12,
      weightLbs: 180,
      notes: "noticeable wear",
      conditionIssues: "",
      merchType: "furniture",
    },
  ];
  for (const row of fixtures) {
    const item = listingBuildItemFromRow(row);
    const prose = finalizeListingDescription(row.description, item, row.notes, row.conditionIssues);
    const dims = {
      width: row.dimWidth,
      depth: row.dimDepth,
      height: row.dimHeight,
      weightLbs: row.weightLbs,
      websiteCategory: row.websiteCategory || "",
    };
    const body = composeShopifyBodyHtml(prose, dims, item);
    assert.equal(shopifyBodyIsIntakeBuild(body, row), true, `mirror match: ${row.name}`);
    assert.equal(/\bmeasures,\s|\bmeasures\s+so\b/i.test(body), false, `broken measures: ${row.name}`);
    const plan = planSettledCleanliness({
      title: row.name,
      body,
      note: row.notes || "",
      tags: "FH",
      dims: {
        width: row.dimWidth,
        depth: row.dimDepth,
        height: row.dimHeight,
      },
    });
    assert.equal(plan.action, "none", `${row.name}: ${JSON.stringify(plan)}`);
    // Mostly-fallback openings must still clear the 48-char minimum once size/shipping attach.
    const plain = String(body).replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
    assert.ok(plain.length >= 48, `thin body: ${row.name} (${plain.length})`);
  }
});

test("the description fingerprint ignores how Shopify hands the HTML back", () => {
  const sent =
    "<p>Vintage oak console &amp; mirror. It's sold AS IS.<br><br>Dimensions: 18\" W x 18\" D x 36\" H</p>";
  const returned = [
    "<p>Vintage oak console &amp; mirror. It's sold AS IS.<br /><br />Dimensions: 18\" W x 18\" D x 36\" H</p>",
    "<p>Vintage oak console &#38; mirror. It&#39;s sold AS IS.<br><br>Dimensions: 18&quot; W x 18&quot; D x 36&quot; H</p>",
    '<p data-mce-fragment="1">Vintage oak console &amp; mirror. It\'s sold AS IS.<br>\n<br>Dimensions: 18" W x 18" D x 36" H</p>\n',
    "<p>Vintage oak console &amp; mirror.&nbsp;It's sold AS IS.</p><p>Dimensions: 18\" W x 18\" D x 36\" H</p>",
    "<meta charset=\"utf-8\"><p>Vintage  oak console &amp; mirror. It's sold AS IS.<br><br>Dimensions: 18\" W x 18\" D x 36\" H</p>",
  ];
  const want = shopifyBodyFingerprint(sent);
  assert.ok(want);
  for (const html of returned) assert.equal(shopifyBodyFingerprint(html), want, html);
  assert.notEqual(shopifyBodyFingerprint(sent.replace("oak", "walnut")), want, "a changed word is a change");
  assert.notEqual(shopifyBodyFingerprint(sent.replace("36", "38")), want, "a changed size is a change");
  assert.equal(shopifyBodyFingerprint(""), "");
  assert.equal(shopifyBodyFingerprint("<p> </p><br>"), "");
});
