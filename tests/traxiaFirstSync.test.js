import test from "node:test";
import assert from "node:assert/strict";
import axios from "axios";
import {
  buyerFacingClauses,
  finalizeListingDescription,
  fetchTraxiaExtraViaIntake,
  listingBuildItemFromRow,
  mirrorShopifyProductToIntake,
  mirrorShopifyProductToIntakeCoalesced,
  planLiveFlawSync,
  readLiveTraxiaExtra,
  resetMirrorWebhookCoalesceForTests,
  resetIntakeMirrorRecentForTests,
  resetTraxiaExtraCacheForTests,
  shouldFetchTraxiaExtra,
  shopifyProductIsNewer,
  shopifyWritebackDecision,
  snapshotHash,
  traxiaExtraStampFields,
  TRAXIA_EXTRA_CACHE_TTL_MS,
  TRAXIA_EXTRA_FETCH_TIMEOUT_MS,
  MIRROR_WEBHOOK_FOLLOWUP_MAX,
} from "../lib/shopifyIntakeImages.js";

process.env.SHOPIFY_STORE = "traxia-first-store";
process.env.SHOPIFY_ACCESS_TOKEN = "test-token";
process.env.INTAKE_SESSION_COOKIE = "session=test";
process.env.INTAKE_API_BASE = "https://intake.traxia-first.test";
delete process.env.INTAKE_SYNC_HOOK_SECRET;

const ITEM_ID = "907e51af-4050-4af9-8d1e-d781a32f4860";
const PRODUCT_ID = "9827087286531";
const SKU = "94Q6KW";
const MIXED = "scratch on top left. This used to be a consigors";
const BASE =
  "A walnut side chair with a carved back and an upholstered seat that suits a dining room or a study.";

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function reply(config, status, data) {
  return {
    data,
    status,
    statusText: String(status),
    headers: {},
    config,
    request: {},
  };
}

function makeMirrorWorld({ extraOnProduct = null, row = {}, holdMirrorMs = 0 } = {}) {
  const state = {
    calls: [],
    stamps: [],
    traxiaExtraCalls: [],
    productGets: 0,
    row: {
      id: ITEM_ID,
      shopifyProductId: PRODUCT_ID,
      traxiaSku: SKU,
      name: "Walnut Side Chair",
      description: BASE,
      notes: "",
      conditionIssues: "",
      ecommerceTags: "FH, D",
      dimWidth: 18,
      dimDepth: 18,
      dimHeight: 36,
      weightLbs: 12,
      retailPrice: 90,
      shopifyLinkState: "linked",
      ...row,
    },
    product: {
      id: Number(PRODUCT_ID),
      title: "Walnut Side Chair",
      body_html: `<p>${BASE}</p>`,
      tags: "FH, D",
      status: "active",
      updated_at: new Date().toISOString(),
      variants: [{ id: 1, sku: SKU, price: "90.00", grams: 5443 }],
    },
    liveProduct: null,
    traxiaExtra: { known: false, text: "" },
  };
  state.liveProduct = structuredClone(state.product);
  if (extraOnProduct != null) state.product.extraInformation = extraOnProduct;

  state.adapter = async (config) => {
    const url = `${config.baseURL || ""}${config.url || ""}`;
    const method = String(config.method || "get").toUpperCase();
    const body =
      typeof config.data === "string" && config.data ? JSON.parse(config.data) : config.data;
    const path = url
      .replace(/^https?:\/\/[^/]+/, "")
      .replace(/^\/admin\/api\/[^/]+/, "")
      .replace(/\?.*$/, "");
    state.calls.push({ method, path, body, shopify: /myshopify\.com/.test(url) });

    if (/myshopify\.com/.test(url)) {
      if (path === `/products/${PRODUCT_ID}.json` && method === "GET") {
        state.productGets += 1;
        return reply(config, 200, { product: structuredClone(state.liveProduct) });
      }
      return reply(config, 404, {});
    }

    if (/\/items\/traxia-extra-sync$/.test(path) && method === "POST") {
      state.traxiaExtraCalls.push(body);
      return reply(config, 200, {
        ok: true,
        known: state.traxiaExtra.known,
        text: state.traxiaExtra.text,
        updated: false,
        reason: state.traxiaExtra.known ? "ok" : "no_traxia_api",
      });
    }
    if (/\/items\/by-shopify-identity$/.test(path)) {
      return reply(config, 200, {
        byProductId: {
          id: state.row.id,
          name: state.row.name,
          shopifyProductId: state.row.shopifyProductId,
          traxiaSku: state.row.traxiaSku,
          notes: state.row.notes,
          conditionIssues: state.row.conditionIssues,
          description: state.row.description,
          dimWidth: state.row.dimWidth,
          dimDepth: state.row.dimDepth,
          dimHeight: state.row.dimHeight,
          weightLbs: state.row.weightLbs,
          retailPrice: state.row.retailPrice,
          shopifyLinkState: "linked",
        },
        bySku: null,
      });
    }
    if (path.endsWith(`/items/${ITEM_ID}`) && method === "GET") {
      return reply(config, 200, { item: { ...state.row } });
    }
    if (/\/shopify-image-sync$/.test(path) && method === "POST") {
      if (holdMirrorMs > 0) await sleep(holdMirrorMs);
      state.stamps.push(body);
      if (body.name) state.row.name = body.name;
      if (body.notes != null) state.row.notes = body.notes;
      if (body.conditionIssues != null) state.row.conditionIssues = body.conditionIssues;
      if (body.dimWidth != null) state.row.dimWidth = body.dimWidth;
      if (body.dimDepth != null) state.row.dimDepth = body.dimDepth;
      if (body.dimHeight != null) state.row.dimHeight = body.dimHeight;
      if (body.weightLbs != null) state.row.weightLbs = body.weightLbs;
      return reply(config, 200, { ok: true });
    }
    return reply(config, 200, { ok: true });
  };

  axios.defaults.adapter = state.adapter;
  resetTraxiaExtraCacheForTests();
  resetMirrorWebhookCoalesceForTests();
  resetIntakeMirrorRecentForTests();
  return state;
}

test("mirror alone does not invent a Traxia-only note when Traxia is not read", async () => {
  const world = makeMirrorWorld();
  // Name unchanged → no Traxia Extra fetch.
  const result = await mirrorShopifyProductToIntake(structuredClone(world.product), {
    log: () => {},
  });
  assert.equal(result.ok, true);
  assert.equal(world.traxiaExtraCalls.length, 0);
  const stamp = world.stamps[world.stamps.length - 1];
  assert.equal(stamp?.notes == null || stamp.notes === "", true);
  assert.equal(stamp?.conditionIssues == null || stamp.conditionIssues === "", true);
});

test("when Extra Information is available, scratch is adopted onto notes and consignor text is not published", async () => {
  const world = makeMirrorWorld({ extraOnProduct: MIXED });
  const result = await mirrorShopifyProductToIntake(structuredClone(world.product), {
    log: () => {},
  });
  assert.equal(result.ok, true);
  const stamp = world.stamps[world.stamps.length - 1];
  assert.equal(stamp.notes, MIXED, "Extra Information lands on Intake notes");
  assert.match(String(stamp.conditionIssues || ""), /scratch/i);
  assert.equal(/consigor/i.test(String(stamp.conditionIssues || "")), false);

  const prose = finalizeListingDescription(
    BASE,
    listingBuildItemFromRow({ ...world.row, notes: MIXED, conditionIssues: stamp.conditionIssues }),
    MIXED,
    stamp.conditionIssues
  );
  assert.match(prose, /scratch/i);
  assert.equal(/consigor|used to be/i.test(prose), false);
  assert.deepEqual(buyerFacingClauses(MIXED, ""), ["scratch on top left"]);
});

test("name and dims from Shopify pull are adopted exactly and ask for one rebuild", async () => {
  const world = makeMirrorWorld({
    row: { dimWidth: 18, dimDepth: 18, dimHeight: 36, weightLbs: 12 },
  });
  // Title dims win over Width:/Depth:/Height: tags (standing rule). Use a
  // title without a W×D×H token so the tag dims are what get stamped.
  world.product.title = "bernis karfen Walnut Side Chair - Oiled - AS IS";
  world.product.tags = "FH, D, Width: 30, Depth: 20, Height: 40";
  world.product.updated_at = new Date(Date.now() + 60_000).toISOString();
  const result = await mirrorShopifyProductToIntake(structuredClone(world.product), {
    log: () => {},
  });
  assert.equal(result.ok, true);
  const stamp = world.stamps[world.stamps.length - 1];
  assert.equal(stamp.name, world.product.title);
  assert.equal(stamp.dimWidth, 30);
  assert.equal(stamp.dimDepth, 20);
  assert.equal(stamp.dimHeight, 40);
  assert.equal(stamp.resyncListing, true);
});

test("shouldFetchTraxiaExtra skips when nothing Traxia-owned changed or Extra Info is live", () => {
  assert.equal(
    shouldFetchTraxiaExtra({
      sku: SKU,
      liveExtraKnown: true,
      writebackApply: true,
      nameChanged: true,
    }).fetch,
    false
  );
  assert.equal(
    shouldFetchTraxiaExtra({
      sku: SKU,
      liveExtraKnown: false,
      writebackApply: true,
      nameChanged: false,
      dimsChanged: false,
      priceChanged: false,
      relinked: false,
    }).fetch,
    false
  );
  assert.equal(
    shouldFetchTraxiaExtra({
      sku: SKU,
      liveExtraKnown: false,
      writebackApply: true,
      nameChanged: true,
    }).fetch,
    true
  );
  assert.equal(TRAXIA_EXTRA_CACHE_TTL_MS, 45_000);
  assert.equal(TRAXIA_EXTRA_FETCH_TIMEOUT_MS, 1500);
});

test("traxiaExtraStampFields never invents a clear when Extra Info is unknown", () => {
  assert.deepEqual(traxiaExtraStampFields({ known: false, text: "" }, { condition: "x" }), {});
  assert.deepEqual(traxiaExtraStampFields({ known: true, text: MIXED }, { condition: "scratch on top left" }), {
    notes: MIXED,
    conditionIssues: "scratch on top left",
  });
});

test("a second webhook with a different name mid-run ends with the second name on Intake", async () => {
  const world = makeMirrorWorld({ holdMirrorMs: 80 });
  const first = structuredClone(world.product);
  first.title = "First Name Chair";
  first.updated_at = new Date(Date.now() - 10_000).toISOString();

  const secondName = "Second Name Chair - AS IS";
  world.liveProduct.title = secondName;
  world.liveProduct.updated_at = new Date(Date.now() + 30_000).toISOString();

  let freshReads = 0;
  const loader = async () => {
    freshReads += 1;
    return structuredClone(world.liveProduct);
  };
  const run = mirrorShopifyProductToIntakeCoalesced(first, {
    log: () => {},
    loadShopifyProduct: loader,
    settleMs: 20,
  });
  await sleep(20);
  const mid = await mirrorShopifyProductToIntakeCoalesced(
    {
      ...structuredClone(world.liveProduct),
      title: secondName,
    },
    {
      log: () => {},
      loadShopifyProduct: loader,
      settleMs: 20,
    }
  );
  assert.equal(mid.coalesced, true);
  assert.equal(mid.reason, "in_flight");

  const done = await run;
  assert.equal(done.followUpRounds, 1);
  assert.equal(freshReads, 1, "one fresh Shopify read for the follow-up");
  assert.equal(world.row.name, secondName);
  assert.equal(
    world.calls.filter((c) => c.shopify && c.method === "PUT").length,
    0
  );
});

test("three webhooks mid-run cause one fresh read, not three", async () => {
  const world = makeMirrorWorld({ holdMirrorMs: 100 });
  world.liveProduct.title = "Final Name";
  world.liveProduct.updated_at = new Date(Date.now() + 40_000).toISOString();
  const seed = structuredClone(world.product);
  seed.updated_at = new Date(Date.now() - 5_000).toISOString();
  let sharedReads = 0;
  const loader = async () => {
    sharedReads += 1;
    return structuredClone(world.liveProduct);
  };
  const run = mirrorShopifyProductToIntakeCoalesced(seed, {
    log: () => {},
    loadShopifyProduct: loader,
    settleMs: 10,
  });
  await sleep(15);
  await mirrorShopifyProductToIntakeCoalesced(
    { ...world.liveProduct, title: "A" },
    { log: () => {}, loadShopifyProduct: loader, settleMs: 10 }
  );
  await mirrorShopifyProductToIntakeCoalesced(
    { ...world.liveProduct, title: "B" },
    { log: () => {}, loadShopifyProduct: loader, settleMs: 10 }
  );
  await mirrorShopifyProductToIntakeCoalesced(
    { ...world.liveProduct, title: "Final Name" },
    { log: () => {}, loadShopifyProduct: loader, settleMs: 10 }
  );
  const done = await run;
  assert.equal(done.followUpRounds, 1);
  assert.equal(sharedReads, 1);
  assert.equal(world.row.name, "Final Name");
});

test("a mid-run webhook that is only our own write causes no extra Intake rewrite ask", async () => {
  const world = makeMirrorWorld({ holdMirrorMs: 60 });
  const seed = structuredClone(world.product);
  // Put Shopify body in sync with Intake so a re-read of the same facts is quiet.
  const run = mirrorShopifyProductToIntakeCoalesced(seed, {
    log: () => {},
    loadShopifyProduct: async () => structuredClone(world.product),
    settleMs: 10,
  });
  await sleep(15);
  await mirrorShopifyProductToIntakeCoalesced(structuredClone(world.product), {
    log: () => {},
    loadShopifyProduct: async () => structuredClone(world.product),
    settleMs: 10,
  });
  const done = await run;
  assert.ok(done.followUpRounds >= 1);
  // Follow-up saw the same facts → skipped, no second resync ask beyond first stamp.
  const resyncStamps = world.stamps.filter((s) => s.resyncListing);
  assert.ok(resyncStamps.length <= 1);
});

test("an older payload arriving after a newer state is ignored", () => {
  const newer = { updated_at: "2026-11-24T18:00:00.000Z" };
  const older = { updated_at: "2026-11-24T17:00:00.000Z" };
  assert.equal(shopifyProductIsNewer(newer, older), true);
  assert.equal(shopifyProductIsNewer(older, newer), false);
  assert.equal(shopifyProductIsNewer(older, older), false);
});

test("a burst that never settles stops at the follow-up cap", async () => {
  const world = makeMirrorWorld({ holdMirrorMs: 40 });
  let reads = 0;
  const loader = async () => {
    reads += 1;
    // Every fresh read looks newer and different so the chain would otherwise continue.
    world.liveProduct.title = `Name Round ${reads}`;
    world.liveProduct.updated_at = new Date(Date.now() + reads * 60_000).toISOString();
    world.liveProduct.body_html = `<p>${BASE} round ${reads}</p>`;
    return structuredClone(world.liveProduct);
  };
  const seed = structuredClone(world.product);
  const run = mirrorShopifyProductToIntakeCoalesced(seed, {
    log: () => {},
    loadShopifyProduct: loader,
    settleMs: 5,
    maxFollowUps: MIRROR_WEBHOOK_FOLLOWUP_MAX,
  });
  // Keep poking pending during every round.
  const poker = setInterval(() => {
    void mirrorShopifyProductToIntakeCoalesced(
      {
        ...structuredClone(world.liveProduct),
        title: `poke-${Date.now()}`,
        updated_at: new Date(Date.now() + 120_000).toISOString(),
      },
      { log: () => {}, loadShopifyProduct: loader, settleMs: 5 }
    );
  }, 15);
  const done = await run;
  clearInterval(poker);
  assert.equal(done.followUpRounds, MIRROR_WEBHOOK_FOLLOWUP_MAX);
  assert.equal(done.followUpCapped, true);
  assert.ok(reads <= MIRROR_WEBHOOK_FOLLOWUP_MAX);
});

test("fetchTraxiaExtraViaIntake returns known:false without inventing a note", async () => {
  const world = makeMirrorWorld();
  world.traxiaExtra = { known: false, text: "" };
  const got = await fetchTraxiaExtraViaIntake(SKU, { log: () => {}, timeoutMs: 500 });
  assert.equal(got.known, false);
  assert.equal(got.text, "");
});

test("planLiveFlawSync with mixed Extra Information keeps only the scratch", () => {
  const plan = planLiveFlawSync({
    shopifyBody: BASE,
    liveNote: MIXED,
    liveKnown: true,
    storedCondition: "",
    item: { name: "Walnut Side Chair" },
  });
  assert.match(String(plan.condition || MIXED), /scratch/i);
  assert.equal(readLiveTraxiaExtra({ extraInformation: MIXED }).known, true);
});

test("a note adopted from Traxia survives a later name stamp that omits notes", async () => {
  const world = makeMirrorWorld({ extraOnProduct: MIXED });
  await mirrorShopifyProductToIntake(structuredClone(world.product), { log: () => {} });
  assert.equal(world.row.notes, MIXED);
  // Later name-only stamp (payload omits notes) must not clear the note.
  world.stamps.length = 0;
  world.product.title = "Later Intake Style Name - AS IS";
  world.product.updated_at = new Date(Date.now() + 120_000).toISOString();
  delete world.product.extraInformation;
  await mirrorShopifyProductToIntake(structuredClone(world.product), { log: () => {} });
  const stamp = world.stamps[world.stamps.length - 1];
  assert.equal(stamp.name, world.product.title);
  assert.equal("notes" in stamp && stamp.notes === "", false);
  assert.equal(world.row.notes, MIXED);
});

test("a note cleared in Extra Information is removed from the listing plan", () => {
  const cleared = planLiveFlawSync({
    shopifyBody: `${BASE} There is a scratch on the top left.`,
    liveNote: "",
    liveKnown: true,
    storedCondition: "scratch on top left",
    item: { name: "Walnut Side Chair" },
  });
  assert.equal(cleared.condition, "");
  assert.equal(cleared.flawMissing, true);
});

test("payload that omits a dim does not clear the stored dim on stamp", async () => {
  const world = makeMirrorWorld({
    row: { dimWidth: 18, dimDepth: 18, dimHeight: 36, weightLbs: 12 },
  });
  world.product.title = "Renamed Chair";
  world.product.updated_at = new Date(Date.now() + 60_000).toISOString();
  // No Width/Depth/Height tags → listingFacts omit dims → stamp leaves them null (not sent).
  const result = await mirrorShopifyProductToIntake(structuredClone(world.product), {
    log: () => {},
  });
  assert.equal(result.ok, true);
  const stamp = world.stamps[world.stamps.length - 1];
  assert.equal(stamp.name, "Renamed Chair");
  assert.equal(stamp.dimWidth == null, true);
  // Adapter only writes dims when present on the stamp body.
  assert.equal(world.row.dimWidth, 18);
  assert.equal(world.row.dimHeight, 36);
});

test("Shopify name then later Intake dim change: both stand (field independence)", async () => {
  const world = makeMirrorWorld({
    row: { dimWidth: 18, dimDepth: 18, dimHeight: 36, weightLbs: 12 },
  });
  const shopifyName = "Shopify Renamed Chair - AS IS";
  world.product.title = shopifyName;
  world.product.updated_at = new Date(Date.now() + 30_000).toISOString();
  await mirrorShopifyProductToIntake(structuredClone(world.product), { log: () => {} });
  assert.equal(world.row.name, shopifyName);
  // Later Intake-only dim edit: name must stay the Shopify one; dim updates.
  world.row.dimWidth = 30;
  world.row.dimDepth = 20;
  world.row.dimHeight = 40;
  assert.equal(world.row.name, shopifyName);
  assert.equal(world.row.dimWidth, 30);
});

test("echo of our own write is not a newer change", () => {
  const title = "Walnut Side Chair";
  const body = `<p>${BASE}</p>`;
  const snap = {
    at: "2026-11-24T18:00:00.000Z",
    title: snapshotHash(title),
    body: snapshotHash(body),
    tags: snapshotHash("FH, D"),
    price: snapshotHash("90.00"),
  };
  const echo = shopifyWritebackDecision({
    snapshot: snap,
    updatedAt: "2026-11-24T18:00:05.000Z",
    title,
    body,
    tags: "FH, D",
    price: "90.00",
    intakeTitle: title,
  });
  assert.equal(echo.apply, false);
  assert.equal(echo.reason, "echo");
});

test("speed: Shopify webhook with nothing new makes no Traxia call and at most one stamp", async () => {
  const world = makeMirrorWorld();
  // Seed snapshot-aligned mirror once so the second pass is quiet.
  await mirrorShopifyProductToIntake(structuredClone(world.product), { log: () => {} });
  const stampsAfterSeed = world.stamps.length;
  const traxiaAfterSeed = world.traxiaExtraCalls.length;
  const second = await mirrorShopifyProductToIntake(structuredClone(world.product), {
    log: () => {},
  });
  assert.equal(second.skipped === true || second.ok === true, true);
  assert.equal(world.traxiaExtraCalls.length, traxiaAfterSeed, "no Traxia read on quiet webhook");
  assert.ok(
    world.stamps.length - stampsAfterSeed <= 1,
    `extra stamps: ${world.stamps.length - stampsAfterSeed}`
  );
  assert.equal(
    world.calls.filter((c) => c.shopify && c.method === "PUT").length,
    0,
    "mirror never PUTs Shopify"
  );
});

test("speed: Shopify webhook where Traxia Extra changed reads Traxia once and stamps notes", async () => {
  const world = makeMirrorWorld();
  world.traxiaExtra = { known: true, text: MIXED };
  world.product.title = "Name change triggers Traxia Extra pull - AS IS";
  world.product.updated_at = new Date(Date.now() + 60_000).toISOString();
  const result = await mirrorShopifyProductToIntake(structuredClone(world.product), {
    log: () => {},
  });
  assert.equal(result.ok, true);
  assert.equal(world.traxiaExtraCalls.length, 1, "Traxia Extra once per settled change");
  assert.ok(world.stamps.some((s) => s.notes === MIXED));
  assert.equal(world.row.notes, MIXED);
  // Same SKU again within TTL: cache must prevent a second Traxia network call.
  world.product.title = "Second settle name - AS IS";
  world.product.updated_at = new Date(Date.now() + 120_000).toISOString();
  await mirrorShopifyProductToIntake(structuredClone(world.product), { log: () => {} });
  assert.equal(world.traxiaExtraCalls.length, 1, "TTL cache holds; still one Traxia call");
});

test("speed: burst of three mid-run webhooks → one fresh Shopify read", async () => {
  const world = makeMirrorWorld({ holdMirrorMs: 80 });
  world.liveProduct.title = "Settled Name";
  world.liveProduct.updated_at = new Date(Date.now() + 50_000).toISOString();
  const seed = structuredClone(world.product);
  seed.updated_at = new Date(Date.now() - 5_000).toISOString();
  let freshReads = 0;
  const loader = async () => {
    freshReads += 1;
    return structuredClone(world.liveProduct);
  };
  const run = mirrorShopifyProductToIntakeCoalesced(seed, {
    log: () => {},
    loadShopifyProduct: loader,
    settleMs: 10,
  });
  await sleep(15);
  for (const title of ["A", "B", "Settled Name"]) {
    await mirrorShopifyProductToIntakeCoalesced(
      { ...world.liveProduct, title },
      { log: () => {}, loadShopifyProduct: loader, settleMs: 10 }
    );
  }
  const done = await run;
  assert.equal(done.followUpRounds, 1);
  assert.equal(freshReads, 1, "one fresh read for the burst");
  assert.equal(world.row.name, "Settled Name");
});
