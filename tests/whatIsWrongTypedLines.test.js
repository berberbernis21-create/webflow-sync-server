/**
 * "What is wrong" keeps what a person typed there when a note arrives from
 * the Shopify description, and similar notes from Intake and Traxia are said
 * once in the listing while the stored texts stay apart.
 *
 * Nothing here calls a live service: Intake and Shopify are an in-memory
 * stand-in (axios adapter). The stamp route stores "What is wrong" the way
 * Pricing Engine does (condition_after_shopify_note, the twin of
 * conditionAfterShopifyNote).
 */
import test from "node:test";
import assert from "node:assert/strict";
import axios from "axios";
import {
  conditionAfterShopifyNote,
  finalizeListingDescription,
  listingBuildItemFromRow,
  mirrorShopifyProductToIntake,
  planLiveFlawSync,
  rememberedShopifyBodyNote,
  resetIntakeMirrorRecentForTests,
  resetMirrorWebhookCoalesceForTests,
  resetOwnListingWriteForTests,
  resetRemovedBodyNotesForTests,
  resetTraxiaExtraCacheForTests,
} from "../lib/shopifyIntakeImages.js";

process.env.SHOPIFY_STORE = "typed-lines-store";
process.env.SHOPIFY_ACCESS_TOKEN = "test-token";
process.env.INTAKE_SESSION_COOKIE = "session=test";
process.env.INTAKE_API_BASE = "https://intake.typed-lines.test";
delete process.env.INTAKE_SYNC_HOOK_SECRET;

const ITEM_ID = "7c2e3b1f-8d20-4b68-8a66-1e4d2d2f8b21";
const PRODUCT_ID = "9827087287111";
const SKU = "95R7LX";
const TYPED = "leg is loose";
const SCRATCH = "scratch on top left";
const CHIP = "chip on the back leg";
const INTAKE_CHIP = "chip on left leg";
const TRAXIA_CHIP = "small chip on the left leg near the foot";
const BASE =
  "A walnut side chair with a carved back and an upholstered seat that suits a dining room or a study.";

const T0 = Date.parse("2026-11-24T18:00:00.000Z");
const at = (seconds) => new Date(T0 + seconds * 1000).toISOString();

function reply(config, status, data) {
  return { data, status, statusText: String(status), headers: {}, config, request: {} };
}

function makeWorld(row = {}) {
  const state = {
    stamps: [],
    shopifyWrites: 0,
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
      shopifySyncSnapshot: null,
      ...row,
    },
  };
  state.product = (bodyHtml, seconds) => ({
    id: Number(PRODUCT_ID),
    title: "Walnut Side Chair",
    tags: "FH, D",
    status: "active",
    updated_at: at(seconds),
    variants: [{ id: 1, sku: SKU, price: "90.00", grams: 5443 }],
    body_html: bodyHtml,
  });
  state.listing = () =>
    finalizeListingDescription(
      state.row.description,
      listingBuildItemFromRow(state.row),
      state.row.notes,
      state.row.conditionIssues
    );
  state.paragraph = () => state.listing().split(/\s*Dimensions:/)[0].trim();
  state.lines = () => String(state.row.conditionIssues || "").split("\n").filter(Boolean);
  state.memory = () => rememberedShopifyBodyNote(state.row);

  axios.defaults.adapter = async (config) => {
    const url = `${config.baseURL || ""}${config.url || ""}`;
    const method = String(config.method || "get").toUpperCase();
    const body =
      typeof config.data === "string" && config.data ? JSON.parse(config.data) : config.data;
    const path = url
      .replace(/^https?:\/\/[^/]+/, "")
      .replace(/^\/admin\/api\/[^/]+/, "")
      .replace(/\?.*$/, "");
    if (/myshopify\.com/.test(url)) {
      if (method !== "GET") state.shopifyWrites += 1;
      return reply(config, 404, {});
    }
    if (/\/items\/traxia-extra-sync$/.test(path)) {
      return reply(config, 200, { ok: true, known: false, text: "", reason: "no_traxia_api" });
    }
    if (/\/items\/by-shopify-identity$/.test(path)) {
      return reply(config, 200, {
        byProductId: {
          ...state.row,
          shopifySyncSnapshot: state.row.shopifySyncSnapshot || null,
          shopifyBodyNote: rememberedShopifyBodyNote(state.row),
        },
        bySku: null,
      });
    }
    if (path.endsWith(`/items/${ITEM_ID}`) && method === "GET") {
      return reply(config, 200, { item: { ...state.row } });
    }
    if (/\/shopify-image-sync$/.test(path) && method === "POST") {
      state.stamps.push(body);
      const before = state.row.notes;
      const remembered = rememberedShopifyBodyNote(state.row);
      if (body.notes != null) state.row.notes = body.notes;
      if (body.conditionIssues != null && body.notes != null) {
        // What Pricing Engine stores for a note stamp.
        state.row.conditionIssues = conditionAfterShopifyNote({
          storedCondition: state.row.conditionIssues,
          previousNote: before,
          rememberedNote: remembered,
          newNote: state.row.notes,
          incoming: body.conditionIssues,
        });
      }
      let snap = {};
      try {
        snap = state.row.shopifySyncSnapshot ? JSON.parse(state.row.shopifySyncSnapshot) : {};
      } catch (_err) {
        snap = {};
      }
      if (body.shopifyBodyNote != null) {
        state.row.shopifyBodyNote = body.shopifyBodyNote;
        snap.shopifyBodyNote = body.shopifyBodyNote;
      }
      if (body.shopifyUpdatedAt) snap.at = body.shopifyUpdatedAt;
      state.row.shopifySyncSnapshot = JSON.stringify(snap);
      return reply(config, 200, { ok: true });
    }
    return reply(config, 200, { ok: true });
  };

  resetTraxiaExtraCacheForTests();
  resetMirrorWebhookCoalesceForTests();
  resetIntakeMirrorRecentForTests();
  resetOwnListingWriteForTests();
  resetRemovedBodyNotesForTests();
  return state;
}

async function webhook(world, product) {
  resetIntakeMirrorRecentForTests();
  world.stamps.length = 0;
  return mirrorShopifyProductToIntake(structuredClone(product), { log: () => {}, probeNotes: true });
}

const rebuildAsks = (world) => world.stamps.filter((stamp) => stamp.resyncListing === true).length;

test("the rule: typed lines stay in place, the note's copy is replaced, nothing is written twice", () => {
  const after = (storedCondition, previousNote, rememberedNote, newNote, incoming) =>
    conditionAfterShopifyNote({ storedCondition, previousNote, rememberedNote, newNote, incoming });
  // Replaced.
  assert.equal(after(`${TYPED}\n${SCRATCH}\nseat is torn`, SCRATCH, SCRATCH, CHIP, CHIP), `${TYPED}\nseat is torn\n${CHIP}`);
  // Shortened.
  assert.equal(
    after(`${SCRATCH}\n${TYPED}\n${CHIP}`, `${SCRATCH}. ${CHIP}`, `${SCRATCH}. ${CHIP}`, CHIP, CHIP),
    `${TYPED}\n${CHIP}`
  );
  // Emptied.
  assert.equal(after(`${SCRATCH}\n${TYPED}`, SCRATCH, SCRATCH, "", ""), TYPED);
  // Nothing typed: just the note's lines.
  assert.equal(after("", "", "", SCRATCH, SCRATCH), SCRATCH);
  // Seen again: unchanged.
  assert.equal(after(`${TYPED}\n${SCRATCH}`, SCRATCH, SCRATCH, SCRATCH, SCRATCH), `${TYPED}\n${SCRATCH}`);
  // A leftover copy sent back on a removal is not put back.
  assert.equal(after(`${TYPED}\nlamp not working`, "lamp not working", "lamp not working", "", "lamp not working"), TYPED);
});

test("the plan builds the listing from the field as it will read, and sends only the note's part", () => {
  const body = `${BASE} Leg is loose. Pickup is available in Scottsdale, and we offer both local and nationwide delivery.`;
  const plan = planLiveFlawSync({
    shopifyBody: body,
    liveNote: SCRATCH,
    storedCondition: TYPED,
    item: { name: "Walnut Side Chair" },
    previousNote: "",
    rememberedNote: "",
  });
  assert.equal(plan.condition, SCRATCH, "the stamp carries the note's part only");
  assert.equal(plan.conditionAfter, `${TYPED}\n${SCRATCH}`);
  assert.match(plan.prose, /There is a scratch on the top left\./);
  assert.match(plan.prose, /Leg is loose\./);

  const cleared = planLiveFlawSync({
    shopifyBody: plan.prose,
    liveNote: "",
    storedCondition: `${TYPED}\n${SCRATCH}`,
    item: { name: "Walnut Side Chair" },
    previousNote: SCRATCH,
    rememberedNote: SCRATCH,
  });
  assert.equal(cleared.condition, "");
  assert.equal(cleared.conditionAfter, TYPED);
  assert.equal(/scratch/i.test(cleared.prose), false);
  assert.match(cleared.prose, /Leg is loose\./);
  assert.equal(cleared.flawMissing, true, "the removal is a real change to write");

  // Without the Intake note before the change, the plan is the older one.
  const older = planLiveFlawSync({ shopifyBody: "", liveNote: SCRATCH, storedCondition: TYPED });
  assert.equal(older.condition, SCRATCH);
  assert.equal(older.conditionAfter, SCRATCH);
});

test("a typed line survives an arriving note, a shortened one and an emptied description", async () => {
  const world = makeWorld({ conditionIssues: TYPED });
  await webhook(world, world.product(`${SCRATCH}. ${CHIP}`, 10));
  assert.deepEqual(world.lines(), [TYPED, SCRATCH, CHIP]);
  assert.equal(world.stamps[0].conditionIssues, `${SCRATCH}\n${CHIP}`, "only the note's part is sent");
  assert.match(world.paragraph(), /There is a scratch on the top left and a chip on the back leg\./);
  assert.match(world.paragraph(), /Leg is loose\./);

  await webhook(world, world.product(CHIP, 60));
  assert.deepEqual(world.lines(), [TYPED, CHIP]);
  assert.equal(rebuildAsks(world), 1);
  assert.equal(/scratch/i.test(world.paragraph()), false);

  await webhook(world, world.product("", 120));
  assert.equal(world.row.notes, "");
  assert.deepEqual(world.lines(), [TYPED]);
  assert.equal(rebuildAsks(world), 1, "one rebuild for the removal");
  assert.equal(/chip/i.test(world.paragraph()), false);
  assert.match(world.paragraph(), /Leg is loose\./);
  assert.equal(world.shopifyWrites, 0, "the mirror never writes Shopify");
});

test("the same flaw in Intake and in Traxia is said once and follows what is left", async () => {
  const world = makeWorld({ conditionIssues: INTAKE_CHIP });
  await webhook(world, world.product(TRAXIA_CHIP, 10));
  // Both originals are stored, with the memory of which part came from Shopify.
  assert.equal(world.row.notes, TRAXIA_CHIP);
  assert.deepEqual(world.lines(), [INTAKE_CHIP, TRAXIA_CHIP]);
  assert.equal(world.memory(), TRAXIA_CHIP);
  assert.equal(rebuildAsks(world), 1);
  assert.match(world.paragraph(), /There is a small chip on the left leg near the foot\./);
  assert.equal((world.paragraph().match(/chip/gi) || []).length, 1);

  // Traxia deletes its note: the flaw stays, in the Intake wording.
  await webhook(world, world.product("", 60));
  assert.equal(world.row.notes, "");
  assert.equal(world.memory(), "");
  assert.deepEqual(world.lines(), [INTAKE_CHIP]);
  assert.equal(rebuildAsks(world), 1, "one rebuild");
  assert.match(world.paragraph(), /There is a chip on the left leg\./);
  assert.equal(/near the foot/.test(world.paragraph()), false);
});

test("the Intake line was deleted and Traxia's remains; then both are gone", async () => {
  const world = makeWorld({ conditionIssues: INTAKE_CHIP });
  await webhook(world, world.product(TRAXIA_CHIP, 10));
  // The person deletes their line in Intake.
  world.row.conditionIssues = TRAXIA_CHIP;
  assert.match(world.paragraph(), /There is a small chip on the left leg near the foot\./);

  await webhook(world, world.product("", 60));
  assert.equal(world.row.notes, "");
  assert.deepEqual(world.lines(), []);
  assert.equal(world.stamps.length, 1, "one stamp");
  assert.equal(rebuildAsks(world), 1, "one rebuild");
  assert.equal(/chip/i.test(world.paragraph()), false);
});

test("a similar note typed in the Intake note field blends, and the two stay apart in the fields", async () => {
  const world = makeWorld({ notes: INTAKE_CHIP, conditionIssues: INTAKE_CHIP });
  await webhook(world, world.product(TRAXIA_CHIP, 10));
  assert.equal(world.row.notes, `${INTAKE_CHIP}. ${TRAXIA_CHIP}`);
  assert.deepEqual(world.lines(), [INTAKE_CHIP, TRAXIA_CHIP]);
  assert.equal(world.memory(), TRAXIA_CHIP);
  assert.equal((world.paragraph().match(/chip/gi) || []).length, 1);
  assert.match(world.paragraph(), /There is a small chip on the left leg near the foot\./);

  await webhook(world, world.product("", 60));
  assert.equal(world.row.notes, INTAKE_CHIP);
  assert.deepEqual(world.lines(), [INTAKE_CHIP]);
  assert.match(world.paragraph(), /There is a chip on the left leg\./);
});

test("different and conflicting notes from the two sources are both published", async () => {
  const world = makeWorld({ conditionIssues: "chip on left leg\nstain on the seat" });
  await webhook(world, world.product("chip on the right leg. scratch on top", 10));
  const listing = world.paragraph();
  for (const flaw of ["a chip on the right leg", "a scratch on the top", "a chip on the left leg", "a stain on the seat"]) {
    assert.equal(listing.split(flaw).length - 1, 1, `${flaw}: ${listing}`);
  }
});

test("three sources of overlap publish each distinct flaw once", async () => {
  const world = makeWorld({
    notes: "chip on left leg. Box in warehouse",
    conditionIssues: "stain on seat\nscratch on top",
  });
  await webhook(world, world.product(`${TRAXIA_CHIP}. stain on the seat`, 10));
  const listing = world.paragraph();
  for (const kind of ["chip", "stain", "scratch"]) {
    assert.equal((listing.match(new RegExp(kind, "gi")) || []).length, 1, `${kind}: ${listing}`);
  }
  assert.match(listing, /small chip on the left leg near the foot/);
  assert.equal(/warehouse/i.test(listing), false);
  assert.match(world.row.notes, /chip on left leg/);
  assert.match(world.row.notes, /small chip on the left leg near the foot/);
  assert.ok(world.lines().includes("stain on seat") && world.lines().includes("scratch on top"));
});

test("an AS IS label goes with its whole sentence", () => {
  const item = { name: "Walnut Side Table" };
  const lead = "A walnut side table with one drawer and tapered legs.";
  const tail = "It suits a bedroom or a hallway.";
  const shipping = "Pickup is available in Scottsdale, and we offer both local and nationwide delivery.";
  for (const label of ["This table is sold AS IS: chip on leg.", "AS IS: chip on leg, as shown in the photos."]) {
    assert.equal(
      finalizeListingDescription(`${lead} ${label} ${tail}`, item, "", ""),
      `${lead} ${tail} ${shipping}`
    );
  }
});
