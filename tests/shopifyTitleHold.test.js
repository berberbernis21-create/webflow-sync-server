import test from "node:test";
import assert from "node:assert/strict";
import axios from "axios";
import {
  buildListingProsePrompt,
  ensureRestorationHardware,
  finalizeListingDescription,
  handleIntakeListingUpdate,
  resetListingUpdateCoalesceForTests,
  scalePresenceHint,
  shopifyTitleIsNewerThanIntake,
  snapshotHash,
  titleHasRhBrand,
} from "../lib/shopifyIntakeImages.js";
import { resetShopifyThrottleForTests } from "../lib/shopifyThrottle.js";

/**
 * RH Salvaged Wood Oversized Mirror (TDHXQX): Traxia says "... - 90X56H", Intake
 * still carried an older typed name. A full listing write must not post that stale
 * name over the fresher Shopify/Traxia title, and the description names
 * Restoration Hardware.
 */

const ITEM_ID = "bbbbbbbb-bbbb-4ccc-8ddd-eeeeeeeeeeee";
const PRODUCT_ID = "9301";
const TRAXIA_TITLE = "RH Salvaged Wood Oversized Mirror - 90X56H";
const STALE_INTAKE_NAME = "RH Salvaged Wood Oversized Mirror0 9056";
const BASE =
  "Salvaged wood oversized wall mirror in a rustic style. The oversized scale works well as a full-length mirror and suits larger wall spans where you want a substantial frame presence.";

process.env.SHOPIFY_STORE = "title-hold-store";
process.env.SHOPIFY_ACCESS_TOKEN = "test-token";
process.env.INTAKE_SESSION_COOKIE = "session=test";
process.env.INTAKE_API_BASE = "https://intake.hold.test";
delete process.env.INTAKE_SYNC_HOOK_SECRET;

function arm({ liveTitle = TRAXIA_TITLE, liveTags = "" } = {}) {
  const state = {
    calls: [],
    product: {
      id: Number(PRODUCT_ID),
      title: liveTitle,
      body_html: "",
      tags: liveTags,
      status: "draft",
      variants: [{ id: 601, sku: "TDHXQX", price: "0.00" }],
    },
  };
  const reply = (config, status, data) => ({ data, status, statusText: String(status), headers: {}, config, request: {} });
  axios.defaults.adapter = async (config) => {
    const url = `${config.baseURL || ""}${config.url || ""}`;
    const method = String(config.method || "get").toUpperCase();
    const body = typeof config.data === "string" && config.data ? JSON.parse(config.data) : config.data;
    const path = url.replace(/^https?:\/\/[^/]+/, "").replace(/^\/admin\/api\/[^/]+/, "");
    const shopify = /myshopify\.com\/admin\//.test(url);
    state.calls.push({ method, path, shopify, body });
    if (shopify) {
      if (path === `/products/${PRODUCT_ID}.json` && method === "GET") return reply(config, 200, { product: state.product });
      if (path === `/products/${PRODUCT_ID}.json` && method === "PUT") {
        Object.assign(state.product, body.product);
        return reply(config, 200, { product: state.product });
      }
      if (/metafields/.test(path) && method === "GET") return reply(config, 200, { metafields: [] });
      if (/metafields/.test(path)) return reply(config, 201, { metafield: { id: 1, ...(body?.metafield || {}) } });
      return reply(config, 404, {});
    }
    return reply(config, 200, { ok: true });
  };
  state.puts = () =>
    state.calls.filter((c) => c.shopify && c.method === "PUT" && c.path === `/products/${PRODUCT_ID}.json`);
  resetShopifyThrottleForTests();
  resetListingUpdateCoalesceForTests();
  return state;
}

function fullPayload(extra = {}) {
  return {
    intakeItemId: ITEM_ID,
    shopifyProductId: PRODUCT_ID,
    allowOpenAi: false,
    name: STALE_INTAKE_NAME,
    description: BASE,
    replaceBody: true,
    rewriteBody: true,
    notes: "",
    conditionIssues: "",
    ecommerceTags: "",
    merchType: "furniture",
    websiteCategory: "A",
    category: "MIRROR",
    brand: "",
    intakeItem: {
      id: ITEM_ID,
      name: STALE_INTAKE_NAME,
      description: BASE,
      ecommerceTags: "",
      merchType: "furniture",
      websiteCategory: "A",
      category: "MIRROR",
      dimWidth: 90,
      dimDepth: 2,
      dimHeight: 56,
      weightLbs: 300,
    },
    ...extra,
  };
}

test("shopifyTitleIsNewerThanIntake: truth table", () => {
  const live = TRAXIA_TITLE;
  const base = { liveTitle: live, intakeName: STALE_INTAKE_NAME };
  // No marker from Pricing Engine: old behavior, never held.
  assert.equal(shopifyTitleIsNewerThanIntake({ ...base }), false);
  // Never synced before and Shopify differs: Shopify wins.
  assert.equal(shopifyTitleIsNewerThanIntake({ ...base, snapshotTitle: "none" }), true);
  // Shopify moved since the last sync: Shopify wins.
  assert.equal(shopifyTitleIsNewerThanIntake({ ...base, snapshotTitle: snapshotHash("Some older title") }), true);
  // Shopify has not moved since the last sync: this is an Intake edit, write it.
  assert.equal(shopifyTitleIsNewerThanIntake({ ...base, snapshotTitle: snapshotHash(live) }), false);
  // Same title in both places: nothing to hold.
  assert.equal(shopifyTitleIsNewerThanIntake({ ...base, intakeName: live, snapshotTitle: "none" }), false);
  // An explicit name edit or a rename always writes.
  assert.equal(shopifyTitleIsNewerThanIntake({ ...base, snapshotTitle: "none", syncFields: ["name"] }), false);
  assert.equal(shopifyTitleIsNewerThanIntake({ ...base, snapshotTitle: "none", priorName: "Old" }), false);
});

test("a full write with a stale Intake name leaves the Traxia title alone", async () => {
  const backend = arm();
  const result = await handleIntakeListingUpdate(fullPayload({ snapshotTitle: "none" }));
  assert.equal(result.ok, true);
  assert.equal(result.titleHeld, true);
  const puts = backend.puts();
  assert.ok(puts.length >= 1);
  for (const put of puts) assert.equal(put.body.product.title, undefined);
  assert.equal(backend.product.title, TRAXIA_TITLE);
});

test("the same write still lands the body and heals the empty tags", async () => {
  const backend = arm({ liveTags: "" });
  await handleIntakeListingUpdate(fullPayload({ snapshotTitle: "none" }));
  const sent = Object.assign({}, ...backend.puts().map((p) => p.body.product));
  assert.match(String(sent.body_html || ""), /Restoration Hardware/);
  assert.doesNotMatch(String(sent.body_html || "").replace(/Restoration Hardware/g, ""), /\bRH\b/);
  assert.match(String(sent.tags || ""), /FH/);
});

test("an Intake name edit still reaches Shopify", async () => {
  const backend = arm();
  await handleIntakeListingUpdate(
    fullPayload({ snapshotTitle: "none", syncFields: ["name", "body"], name: TRAXIA_TITLE + " NEW" })
  );
  assert.equal(backend.product.title, TRAXIA_TITLE + " NEW");
});

test("without the marker the server behaves as before", async () => {
  const backend = arm();
  await handleIntakeListingUpdate(fullPayload());
  assert.equal(backend.product.title, STALE_INTAKE_NAME);
});

test("RH is expanded only as a whole word in the name", () => {
  assert.equal(titleHasRhBrand("RH Salvaged Wood Oversized Mirror - 90X56H"), true);
  for (const name of ["RH-7 Chair", "Marsh Table", "TDHRH Mirror", "Rhine Lamp", ""]) {
    assert.equal(titleHasRhBrand(name), false, name);
  }
  const rh = { name: TRAXIA_TITLE };
  const out = finalizeListingDescription(BASE, rh, "", "");
  assert.match(out, /^Restoration Hardware salvaged wood oversized wall mirror/);
  assert.equal(out.split("Restoration Hardware").length - 1, 1);
  // Idempotent and never invented for other names.
  assert.equal(ensureRestorationHardware(out, rh), out);
  assert.equal(ensureRestorationHardware("Salvaged mirror.", { name: "Marsh Mirror" }), "Salvaged mirror.");
  assert.equal(ensureRestorationHardware("Mirror with an RH finish.", rh), "Mirror with a Restoration Hardware finish.");
});

test("oversized mirror gets a scale presence hint for stronger copy", () => {
  const dims = { width: 90, depth: 2, height: 56 };
  const hint = scalePresenceHint({ name: TRAXIA_TITLE, category: "MIRROR", dims });
  assert.match(hint, /SCALE PRESENCE/i);
  assert.match(hint, /oversized/i);
  assert.equal(scalePresenceHint({ name: TRAXIA_TITLE, category: "MIRROR", dims: { width: 24, height: 36 } }), "");
  const { user } = buildListingProsePrompt({
    name: TRAXIA_TITLE,
    brand: "Restoration Hardware",
    category: "MIRROR",
    merchType: "furniture",
    dims,
    dimHint: 'Dimensions: Width: 90" × Depth: 2" × Height: 56". Weight: 300 lb.',
  });
  assert.match(user, /SCALE PRESENCE/);
  assert.match(user, /Restoration Hardware/);
});
