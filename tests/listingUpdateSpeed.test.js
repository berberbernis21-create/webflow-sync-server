import test from "node:test";
import assert from "node:assert/strict";
import axios from "axios";
import {
  handleIntakeListingUpdate,
  inlineIntakeItem,
  planMetafieldWrite,
  resetListingUpdateCoalesceForTests,
} from "../lib/shopifyIntakeImages.js";
import {
  acquireShopifySlot,
  resetShopifyThrottleForTests,
  runWithShopifyPriority,
  shopifyPriorityActive,
} from "../lib/shopifyThrottle.js";

/**
 * A name change must reach Shopify without waiting on work it does not need:
 * no full Intake item download, one metafield read, writes only for values
 * that changed, and ahead of bulk sync calls on the shared rate limit.
 *
 * Shopify and Intake are replaced by an in-memory backend through the axios
 * adapter. The real rate limiter stays in place.
 */

const ITEM_ID = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";
const PRODUCT_ID = "9001";
const OLD_BODY =
  "Vintage oak rocker from the previous listing with a cane seat and turned spindles in the back.";
const INTAKE_BASE =
  "A walnut side chair with a carved back and a worn leather seat for a dining room or study.";
const EXTRA_INFO = "Small scratch on the left arm.";
const TYPED_NAME = "bernis karfen Walnut Side Chair 42X23X75 - AS IS";

process.env.SHOPIFY_STORE = "speed-test-store";
process.env.SHOPIFY_ACCESS_TOKEN = "test-token";
process.env.INTAKE_SESSION_COOKIE = "session=test";
process.env.INTAKE_API_BASE = "https://intake.speed.test";
delete process.env.INTAKE_SYNC_HOOK_SECRET;

function makeBackend({ metafields = [], indexStatus = 200 } = {}) {
  const state = {
    calls: [],
    product: {
      id: Number(PRODUCT_ID),
      title: "Old Chair Name",
      body_html: `<p>${OLD_BODY}</p>`,
      tags: "FH, D, chair",
      status: "active",
      variants: [{ id: 501, sku: "TRX-1", price: "90.00" }],
    },
    metafields: metafields.map((row, index) => ({ id: 7000 + index, ...row })),
    intakeItem: {
      id: ITEM_ID,
      shopifyProductId: PRODUCT_ID,
      name: TYPED_NAME,
      description: INTAKE_BASE,
      notes: EXTRA_INFO,
      conditionIssues: EXTRA_INFO,
      ecommerceTags: "FH, D, chair",
      dimWidth: 18,
      dimDepth: 18,
      dimHeight: 36,
      weightLbs: 12,
      history: [],
    },
    nextMetafieldId: 8000,
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
    const path = url.replace(/^https?:\/\/[^/]+/, "").replace(/^\/admin\/api\/[^/]+/, "");
    state.calls.push({ at: Date.now(), method, path, shopify, params: config.params || null, body });
    if (shopify) {
      if (path === `/products/${PRODUCT_ID}.json` && method === "GET") {
        return reply(config, 200, { product: state.product });
      }
      if (path === `/products/${PRODUCT_ID}.json` && method === "PUT") {
        Object.assign(state.product, body.product);
        return reply(config, 200, { product: state.product });
      }
      if (path === `/products/${PRODUCT_ID}/metafields.json` && method === "GET") {
        const { namespace, key } = config.params || {};
        if (!namespace && !key && indexStatus !== 200) return reply(config, indexStatus, {});
        const rows = state.metafields.filter(
          (row) => (!namespace || row.namespace === namespace) && (!key || row.key === key)
        );
        return reply(config, 200, { metafields: rows });
      }
      if (path === `/products/${PRODUCT_ID}/metafields.json` && method === "POST") {
        const row = { id: (state.nextMetafieldId += 1), ...body.metafield };
        state.metafields.push(row);
        return reply(config, 201, { metafield: row });
      }
      const put = path.match(/^\/metafields\/(\d+)\.json$/);
      if (put && method === "PUT") {
        const row = state.metafields.find((entry) => String(entry.id) === put[1]);
        if (!row) return reply(config, 404, {});
        row.value = body.metafield.value;
        return reply(config, 200, { metafield: row });
      }
      return reply(config, 404, {});
    }
    if (path === `/api/lost-and-found-intake/items/${ITEM_ID}` && method === "GET") {
      return reply(config, 200, { item: state.intakeItem });
    }
    // Intake stamp and anything else on the Intake side.
    return reply(config, 200, { ok: true });
  };
  state.shopifyCalls = () => state.calls.filter((call) => call.shopify);
  state.itemGets = () =>
    state.calls.filter((call) => !call.shopify && call.method === "GET" && /\/items\//.test(call.path));
  state.metafieldWrites = () =>
    state.shopifyCalls().filter(
      (call) => /metafields/.test(call.path) && (call.method === "PUT" || call.method === "POST")
    );
  state.metafieldReads = () =>
    state.shopifyCalls().filter((call) => /metafields/.test(call.path) && call.method === "GET");
  state.productPuts = () =>
    state.shopifyCalls().filter(
      (call) => call.path === `/products/${PRODUCT_ID}.json` && call.method === "PUT"
    );
  return state;
}

/** What Pricing Engine posts for a name change (listing_delta_payload + intakeItem). */
function nameChangePayload({ inline = true } = {}) {
  const payload = {
    intakeItemId: ITEM_ID,
    shopifyProductId: PRODUCT_ID,
    allowOpenAi: false,
    syncFields: ["name", "body", "tags"],
    name: TYPED_NAME,
    description: `${INTAKE_BASE} ${EXTRA_INFO}`,
    replaceBody: true,
    rewriteBody: true,
    notes: EXTRA_INFO,
    conditionIssues: EXTRA_INFO,
    ecommerceTags: "FH, D, chair",
    merchType: "furniture",
    websiteCategory: "D",
    category: "CHAIR",
    brand: "",
  };
  if (inline) {
    payload.intakeItem = {
      id: ITEM_ID,
      description: INTAKE_BASE,
      notes: EXTRA_INFO,
      conditionIssues: EXTRA_INFO,
      ecommerceTags: "FH, D, chair",
      dimWidth: 18,
      dimDepth: 18,
      dimLength: 18,
      dimHeight: 36,
      weightLbs: 12,
    };
  }
  return payload;
}

function arm(options) {
  const backend = makeBackend(options);
  axios.defaults.adapter = backend.adapter;
  resetShopifyThrottleForTests();
  resetListingUpdateCoalesceForTests();
  return backend;
}

function plainText(html) {
  return String(html || "")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function countOf(text, needle) {
  return String(text).split(needle).length - 1;
}

test("name change writes title, body, and tags in one PUT without reading the Intake item", async () => {
  const backend = arm();
  const started = Date.now();
  const result = await handleIntakeListingUpdate(nameChangePayload());
  assert.equal(result.ok, true);
  assert.equal(result.status, "updated");

  // No Intake item download at all: the payload carried the fields.
  assert.equal(backend.itemGets().length, 0);

  const puts = backend.productPuts();
  assert.equal(puts.length, 1);
  const sent = puts[0].body.product;
  // The title is the user's text, unchanged.
  assert.equal(sent.title, TYPED_NAME);
  // Body rebuilt from Intake: base copy once, Extra Information woven in, old Shopify body gone.
  const body = plainText(sent.body_html);
  assert.equal(body.includes("walnut side chair"), true);
  assert.equal(countOf(body, "walnut side chair"), 1);
  assert.match(body, /scratch/i);
  assert.equal(body.includes("Vintage oak rocker"), false);
  assert.equal(countOf(body, "Dimensions:"), 1);
  // Tags are refreshed in the same PUT.
  assert.equal(typeof sent.tags, "string");
  assert.match(sent.tags, /FH/);

  // The product PUT is the second Shopify call, right after the product GET.
  const shopify = backend.shopifyCalls();
  assert.equal(shopify[0].method, "GET");
  assert.equal(shopify[0].path, `/products/${PRODUCT_ID}.json`);
  assert.equal(shopify[1].method, "PUT");
  assert.equal(shopify[1].path, `/products/${PRODUCT_ID}.json`);
  assert.ok(shopify[1].at - started < 400, `PUT issued after ${shopify[1].at - started}ms`);

  // Metafields: one read of the whole set, then one create per value (none existed).
  assert.equal(backend.metafieldReads().length, 1);
  assert.equal(backend.metafieldReads()[0].params.limit, 250);
  const writes = backend.metafieldWrites();
  assert.ok(writes.length >= 4 && writes.length <= 6, `metafield writes: ${writes.length}`);
  assert.equal(writes.every((call) => call.method === "POST"), true);
  // Before: a lookup GET in front of every write (2 calls per metafield).
  assert.equal(shopify.length, 3 + writes.length);
});

test("a save whose metafields already hold the same values writes none of them", async () => {
  const first = arm();
  await handleIntakeListingUpdate(nameChangePayload());
  const stored = first.metafields.map(({ namespace, key, value, type }) => ({
    namespace,
    key,
    value,
    type,
  }));
  assert.ok(stored.length >= 4);

  const backend = arm({ metafields: stored });
  const result = await handleIntakeListingUpdate(nameChangePayload());
  assert.equal(result.status, "updated");
  assert.equal(backend.productPuts().length, 1);
  assert.equal(backend.productPuts()[0].body.product.title, TYPED_NAME);
  assert.equal(backend.metafieldReads().length, 1);
  assert.equal(backend.metafieldWrites().length, 0);
  // Product GET, product PUT, one metafield read. Was 2 + 2 per metafield.
  assert.equal(backend.shopifyCalls().length, 3);
});

test("only the metafield whose value changed is written, by update with no lookup", async () => {
  const first = arm();
  await handleIntakeListingUpdate(nameChangePayload());
  const stored = first.metafields.map(({ namespace, key, value, type }) => ({
    namespace,
    key,
    value,
    type,
  }));
  const target = stored.find((row) => row.key === "aeo_description");
  assert.ok(target, "aeo_description was written by the first save");
  target.value = "stale answer text from an older name";

  const backend = arm({ metafields: stored });
  await handleIntakeListingUpdate(nameChangePayload());
  const writes = backend.metafieldWrites();
  assert.equal(writes.length, 1);
  assert.equal(writes[0].method, "PUT");
  assert.equal(backend.metafieldReads().length, 1);
  assert.equal(backend.shopifyCalls().length, 4);
  const now = backend.metafields.find((row) => row.key === "aeo_description");
  assert.notEqual(now.value, "stale answer text from an older name");
});

test("a failed metafield read falls back to per-key lookups and still writes everything", async () => {
  const backend = arm({ indexStatus: 500 });
  const result = await handleIntakeListingUpdate(nameChangePayload());
  assert.equal(result.status, "updated");
  const writes = backend.metafieldWrites();
  assert.ok(writes.length >= 4);
  // One failed read of the set, then one lookup per metafield as before.
  assert.equal(backend.metafieldReads().length, 1 + writes.length);
  assert.equal(backend.productPuts().length, 1);
});

test("without inline fields the listing path reads the item once, lite", async () => {
  const backend = arm();
  const result = await handleIntakeListingUpdate(nameChangePayload({ inline: false }));
  assert.equal(result.status, "updated");
  const gets = backend.itemGets();
  assert.equal(gets.length, 1);
  assert.equal(String(gets[0].params?.lite), "1");
  const body = plainText(backend.productPuts()[0].body.product.body_html);
  assert.equal(countOf(body, "walnut side chair"), 1);
  assert.match(body, /scratch/i);
});

test("inline Intake fields for another item are ignored", () => {
  const payload = { intakeItem: { id: "someone-else", description: "x" } };
  assert.equal(inlineIntakeItem(payload, ITEM_ID), null);
  assert.equal(inlineIntakeItem({ intakeItem: "nope" }, ITEM_ID), null);
  assert.equal(inlineIntakeItem({}, ITEM_ID), null);
  const own = { intakeItem: { id: ITEM_ID, description: "x" } };
  assert.equal(inlineIntakeItem(own, ITEM_ID), own.intakeItem);
});

test("a coalesced follow-up rebuilds from the current Intake row, read once and lite", async () => {
  const backend = arm();
  // Hold the first write open so the second save lands while it is in flight.
  let release;
  const gate = new Promise((resolve) => {
    release = resolve;
  });
  const inner = backend.adapter;
  axios.defaults.adapter = async (config) => {
    const res = await inner(config);
    if (String(config.method).toLowerCase() === "put" && /\/products\/\d+\.json$/.test(config.url)) {
      await gate;
    }
    return res;
  };
  const firstRun = handleIntakeListingUpdate(nameChangePayload());
  await new Promise((resolve) => setTimeout(resolve, 50));
  // The user typed again. Intake now holds the newer name and a newer description.
  const newer = "bernis karfen Walnut Side Chair - AS IS - Sophia";
  backend.intakeItem.name = newer;
  backend.intakeItem.description =
    "A walnut armchair with a carved back and a newly noted leather seat for a reading corner.";
  const stale = nameChangePayload();
  stale.name = newer;
  const second = await handleIntakeListingUpdate(stale);
  assert.equal(second.deferred, true);
  assert.equal(second.status, "coalesced");
  release();
  await firstRun;
  // The follow-up starts once the first run has finished its metafield writes
  // (rate limited), then the 250ms pending settle, then its own GET and PUT.
  const deadline = Date.now() + 15000;
  while (backend.productPuts().length < 2 && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  // Let the follow-up finish so nothing is left running after the test.
  await new Promise((resolve) => setTimeout(resolve, 1500));

  const puts = backend.productPuts();
  assert.equal(puts.length, 2);
  const last = puts[1].body.product;
  assert.equal(last.title, newer);
  const body = plainText(last.body_html);
  // The follow-up used the row it just loaded, not the stale inline copy.
  assert.equal(body.includes("walnut armchair"), true);
  assert.equal(body.includes("walnut side chair"), false);
  assert.match(body, /scratch/i);
  // One lite read for the follow-up. The write reused it.
  const gets = backend.itemGets();
  assert.equal(gets.length, 1);
  assert.equal(String(gets[0].params?.lite), "1");
});

test("planMetafieldWrite: skip same value, update changed, create missing, lookup without an index", () => {
  const index = new Map([
    ["custom.department", { id: 1, value: "Furniture & Home", type: "single_line_text_field" }],
  ]);
  const spec = {
    namespace: "custom",
    key: "department",
    value: "Furniture & Home",
    type: "single_line_text_field",
  };
  assert.deepEqual(planMetafieldWrite(index, spec), { action: "skip", id: 1 });
  assert.deepEqual(planMetafieldWrite(index, { ...spec, value: "Luxury Goods" }), {
    action: "update",
    id: 1,
  });
  assert.deepEqual(planMetafieldWrite(index, { ...spec, type: "multi_line_text_field" }), {
    action: "update",
    id: 1,
  });
  assert.deepEqual(planMetafieldWrite(index, { ...spec, key: "vertical" }), { action: "create" });
  assert.deepEqual(planMetafieldWrite(null, spec), { action: "lookup" });
});

test("Intake edits go ahead of waiting bulk calls, and bulk calls are not starved", async () => {
  resetShopifyThrottleForTests({ tokens: 0 });
  const order = [];
  const all = [];
  // Two bulk calls are already waiting when the edit arrives.
  all.push(acquireShopifySlot().then(() => order.push("bulk-1")));
  all.push(acquireShopifySlot().then(() => order.push("bulk-2")));
  runWithShopifyPriority(() => {
    assert.equal(shopifyPriorityActive(), true);
    for (let i = 1; i <= 4; i += 1) {
      all.push(acquireShopifySlot().then(() => order.push(`edit-${i}`)));
    }
  });
  assert.equal(shopifyPriorityActive(), false);
  await Promise.all(all);
  // Three edits first, then a bulk call gets its turn, then the rest. Nothing is dropped.
  assert.deepEqual(order, ["edit-1", "edit-2", "edit-3", "bulk-1", "edit-4", "bulk-2"]);
});

test("priority follows the request through axios; later bulk requests still complete", async () => {
  const backend = arm();
  resetShopifyThrottleForTests({ tokens: 0 });
  const base = "https://speed-test-store.myshopify.com/admin/api/2024-01";
  const opts = { validateStatus: () => true };
  const bulkA = axios.get(`${base}/products/111.json`, opts);
  const bulkB = axios.get(`${base}/products/222.json`, opts);
  const edit = runWithShopifyPriority(() => axios.get(`${base}/products/${PRODUCT_ID}.json`, opts));
  const [a, b, e] = await Promise.all([bulkA, bulkB, edit]);
  assert.equal(e.status, 200);
  assert.equal(a.status, 404);
  assert.equal(b.status, 404);
  const paths = backend.shopifyCalls().map((call) => call.path);
  assert.deepEqual(paths, [`/products/${PRODUCT_ID}.json`, "/products/111.json", "/products/222.json"]);
});
