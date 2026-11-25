/**
 * The Intake note reflects the last state of Traxia Extra Information.
 *
 * Extra Information shows up as the Shopify description. When it is emptied
 * or shortened at the source, the text that came from it leaves the Intake
 * note and the listing. Text typed in Intake stays. Nothing here calls a live
 * service: Intake and Shopify are an in-memory stand-in (axios adapter).
 */
import test from "node:test";
import assert from "node:assert/strict";
import axios from "axios";
import {
  bodyNoteIsStaleAfterRemoval,
  composeShopifyBodyHtml,
  finalizeListingDescription,
  listingBuildItemFromRow,
  mergePendingTraxiaBodyNote,
  mergeShopifyBodyNoteIntoIntake,
  mirrorShopifyProductToIntake,
  mirrorShopifyProductToIntakeCoalesced,
  readTraxiaNoteFromShopifyBody,
  rememberedShopifyBodyNote,
  resetIntakeMirrorRecentForTests,
  resetMirrorWebhookCoalesceForTests,
  resetOwnListingWriteForTests,
  resetRemovedBodyNotesForTests,
  resetTraxiaExtraCacheForTests,
  shopifyBodyIsIntakeBuild,
  shopifyNoteTextRemoved,
  traxiaBodyNoteFromPayload,
} from "../lib/shopifyIntakeImages.js";

process.env.SHOPIFY_STORE = "note-last-state-store";
process.env.SHOPIFY_ACCESS_TOKEN = "test-token";
process.env.INTAKE_SESSION_COOKIE = "session=test";
process.env.INTAKE_API_BASE = "https://intake.note-last-state.test";
delete process.env.INTAKE_SYNC_HOOK_SECRET;

const ITEM_ID = "5b1d2a0e-7c1f-4a57-9a55-0d3c1c1f7a10";
const PRODUCT_ID = "9827087286999";
const SKU = "94Q6KW";
const SCRATCH = "scratch on top left";
const CONSIGNOR = "This used to be a consignors";
const MIXED = `${SCRATCH}. ${CONSIGNOR}`;
const CHIP = "chip on the back leg";
const TWO_FLAWS = `${SCRATCH}. ${CHIP}`;
const INTAKE_CHIP = "small chip on the left leg";
const BASE =
  "A walnut side chair with a carved back and an upholstered seat that suits a dining room or a study.";

const T0 = Date.parse("2026-11-24T18:00:00.000Z");
const at = (seconds) => new Date(T0 + seconds * 1000).toISOString();
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function reply(config, status, data) {
  return { data, status, statusText: String(status), headers: {}, config, request: {} };
}

/** One Intake row and one Shopify product. The stamp route stores what a stamp carries. */
function makeWorld({ row = {}, holdMirrorMs = 0 } = {}) {
  const state = {
    stamps: [],
    events: [],
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
  if (state.row.shopifyBodyNote && !state.row.shopifySyncSnapshot) {
    state.row.shopifySyncSnapshot = JSON.stringify({ shopifyBodyNote: state.row.shopifyBodyNote });
  }
  state.log = (level, data) => state.events.push({ level, ...data });
  state.product = (bodyHtml, seconds, extra = {}) => {
    const product = {
      id: Number(PRODUCT_ID),
      title: "Walnut Side Chair",
      tags: "FH, D",
      status: "active",
      updated_at: at(seconds),
      variants: [{ id: 1, sku: SKU, price: "90.00", grams: 5443 }],
      ...extra,
    };
    if (bodyHtml !== undefined) product.body_html = bodyHtml;
    return product;
  };
  /** The description our own rebuild writes from the row as it is now. */
  state.ownBuild = () => {
    const item = listingBuildItemFromRow(state.row);
    const prose = finalizeListingDescription(
      state.row.description,
      item,
      state.row.notes,
      state.row.conditionIssues
    );
    return composeShopifyBodyHtml(prose, {}, item);
  };
  state.listing = () =>
    finalizeListingDescription(
      state.row.description,
      listingBuildItemFromRow(state.row),
      state.row.notes,
      state.row.conditionIssues
    );
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
      if (holdMirrorMs > 0) await sleep(holdMirrorMs);
      state.stamps.push(body);
      if (body.notes != null) state.row.notes = body.notes;
      if (body.conditionIssues != null) state.row.conditionIssues = body.conditionIssues;
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
  world.events.length = 0;
  return mirrorShopifyProductToIntake(structuredClone(product), {
    log: world.log,
    probeNotes: true,
  });
}

const rebuildAsks = (world) => world.stamps.filter((stamp) => stamp.resyncListing === true).length;
const noteStamps = (world) =>
  world.stamps.filter((stamp) => Object.prototype.hasOwnProperty.call(stamp, "notes"));
const event = (world, name) => world.events.find((row) => row.event === name);

// ---------------------------------------------------------------------------
// 1. Add then delete.
// ---------------------------------------------------------------------------

test("1. Extra Information added then deleted leaves nothing behind", async () => {
  const world = makeWorld();
  await webhook(world, world.product(MIXED, 10));
  assert.equal(world.row.notes, MIXED, "stored on the Intake note");
  assert.equal(world.memory(), MIXED, "remembered as came-from-Shopify");
  assert.match(world.row.conditionIssues, /scratch/i);
  assert.match(world.listing(), /scratch/i);

  await webhook(world, world.product("", 60));
  assert.equal(world.row.notes, "", "buyer-facing and internal parts are both gone");
  assert.equal(world.memory(), "", "remembered part cleared");
  assert.equal(world.row.conditionIssues, "", "condition no longer carries the scratch");
  assert.equal(rebuildAsks(world), 1, "one rebuild");
  assert.equal(world.stamps.length, 1, "one stamp: store and rebuild together");
  const stamp = world.stamps[0];
  assert.equal(stamp.notes, "");
  assert.equal(stamp.shopifyBodyNote, "");
  assert.equal(/scratch|consignor/i.test(world.listing()), false, "listing drops the sentence");
  assert.equal(world.shopifyWrites, 0, "the mirror never writes Shopify");
});

// ---------------------------------------------------------------------------
// 2. Shortened, not emptied.
// ---------------------------------------------------------------------------

test("2. shortened to the internal part: scratch leaves note and listing, consignor text stays", async () => {
  const world = makeWorld();
  await webhook(world, world.product(MIXED, 10));
  await webhook(world, world.product(CONSIGNOR, 60));
  assert.equal(world.row.notes, CONSIGNOR, "internal text stays in the note");
  assert.equal(world.memory(), CONSIGNOR);
  assert.equal(/scratch/i.test(world.row.conditionIssues), false);
  assert.equal(rebuildAsks(world), 1);
  assert.equal(/scratch|consignor|used to be/i.test(world.listing()), false);
});

test("2. one of two damage sentences removed: that one goes, the other stays", async () => {
  const world = makeWorld();
  await webhook(world, world.product(TWO_FLAWS, 10));
  assert.equal(world.row.notes, TWO_FLAWS);
  assert.match(world.row.conditionIssues, /scratch/i);
  assert.match(world.row.conditionIssues, /chip/i);
  await webhook(world, world.product(CHIP, 60));
  assert.equal(world.row.notes, CHIP);
  assert.equal(world.memory(), CHIP);
  assert.equal(/scratch/i.test(world.row.conditionIssues), false);
  assert.match(world.row.conditionIssues, /chip/i);
  assert.equal(rebuildAsks(world), 1);
  assert.equal(/scratch/i.test(world.listing()), false);
  assert.match(world.listing(), /chip/i);
});

test("2. two flaws written as one sentence, one removed: the listing keeps only the other", async () => {
  const world = makeWorld();
  await webhook(world, world.product(`${SCRATCH}, ${CHIP}`, 10));
  assert.match(world.listing(), /scratch/i);
  assert.match(world.listing(), /chip/i);
  await webhook(world, world.product(CHIP, 60));
  assert.equal(world.row.notes, CHIP);
  assert.equal(world.row.conditionIssues, CHIP);
  assert.equal(rebuildAsks(world), 1);
  assert.equal(/scratch/i.test(world.listing()), false);
  assert.match(world.listing(), /chip/i);
});

test("2. shortened next to text typed in Intake: only the removed sentence goes", async () => {
  const world = makeWorld({ row: { notes: INTAKE_CHIP, conditionIssues: INTAKE_CHIP } });
  await webhook(world, world.product(MIXED, 10));
  assert.match(world.row.notes, /small chip on the left leg/);
  await webhook(world, world.product(CONSIGNOR, 60));
  assert.equal(world.row.notes, `${INTAKE_CHIP}. ${CONSIGNOR}`);
  assert.equal(/scratch/i.test(world.row.conditionIssues), false);
  assert.match(world.row.conditionIssues, /small chip on the left leg/);
  assert.equal(/scratch/i.test(world.listing()), false);
  assert.match(world.listing(), /chip/i);
});

test("2. a person changed the came-from-Shopify text in Intake: what is left of it still goes", () => {
  // The consignor sentence was deleted in Intake; the remembered text is no
  // longer in the note as one piece. The scratch sentence still leaves.
  assert.deepEqual(
    mergeShopifyBodyNoteIntoIntake({
      storedNotes: `${INTAKE_CHIP}. ${SCRATCH}.`,
      rememberedShopifyNote: MIXED,
      incomingShopifyNote: "",
    }),
    { notes: `${INTAKE_CHIP}.`, shopifyBodyNote: "", changed: true }
  );
});

// ---------------------------------------------------------------------------
// 3. The note started in Intake and came back in the description.
// ---------------------------------------------------------------------------

test("3. arriving text already in the Intake note is remembered, not duplicated, and later removed", async () => {
  const world = makeWorld({ row: { notes: MIXED, conditionIssues: SCRATCH } });
  assert.equal(world.memory(), "");
  await webhook(world, world.product(MIXED, 10));
  assert.equal(world.row.notes, MIXED, "no duplicate");
  assert.equal(world.memory(), MIXED, "now remembered as came-from-Shopify");

  await webhook(world, world.product("", 60));
  assert.equal(world.row.notes, "", "a later removal at the source removes it from Intake");
  assert.equal(world.memory(), "");
  assert.equal(/scratch/i.test(world.listing()), false);
});

test("3. arriving text is part of the Intake note: remembered; the rest of the note stays", async () => {
  const world = makeWorld({
    row: { notes: `${INTAKE_CHIP}. ${SCRATCH}`, conditionIssues: `${INTAKE_CHIP}\n${SCRATCH}` },
  });
  await webhook(world, world.product(SCRATCH, 10));
  assert.equal(world.row.notes, `${INTAKE_CHIP}. ${SCRATCH}`, "no duplicate");
  assert.equal(world.memory(), SCRATCH);
  await webhook(world, world.product("", 60));
  assert.equal(world.row.notes, INTAKE_CHIP, "text typed in Intake stays");
  assert.match(world.listing(), /chip/i);
  assert.equal(/scratch/i.test(world.listing()), false);
});

test("3. part of the arriving text is already in the Intake note: only the new part is added", async () => {
  const world = makeWorld({ row: { notes: SCRATCH, conditionIssues: SCRATCH } });
  await webhook(world, world.product(MIXED, 10));
  assert.equal(world.row.notes, MIXED, "scratch is not written twice");
  assert.equal(world.memory(), MIXED);
  await webhook(world, world.product("", 60));
  assert.equal(world.row.notes, "");
});

test("3. a word inside a different Intake sentence is not the arriving note", () => {
  // "scratch" arriving must not be taken as already said by "no scratch on the legs".
  const merged = mergeShopifyBodyNoteIntoIntake({
    storedNotes: "no scratch on the legs",
    rememberedShopifyNote: "",
    incomingShopifyNote: "scratch",
  });
  assert.equal(merged.notes, "no scratch on the legs. scratch");
  // And removing it later leaves the Intake sentence whole.
  assert.equal(
    mergeShopifyBodyNoteIntoIntake({
      storedNotes: merged.notes,
      rememberedShopifyNote: "scratch",
      incomingShopifyNote: "",
    }).notes,
    "no scratch on the legs"
  );
});

// ---------------------------------------------------------------------------
// 4. Notes with no memory.
// ---------------------------------------------------------------------------

test("4. a note typed only in Intake survives an empty description (no memory, no wipe)", async () => {
  const world = makeWorld({ row: { notes: INTAKE_CHIP, conditionIssues: INTAKE_CHIP } });
  await webhook(world, world.product("", 10));
  assert.equal(world.row.notes, INTAKE_CHIP);
  assert.equal(world.row.conditionIssues, INTAKE_CHIP);
  assert.equal(noteStamps(world).length, 0, "no stamp touches the note");
  assert.equal(event(world, "shopify.intake_mirror.traxia_note_cleared"), undefined);
  assert.match(world.listing(), /chip/i, "damage is still told to buyers");
});

test("4. a note stored before the memory existed is left on an empty, and gains a memory when it arrives again", async () => {
  // Stored from Shopify by the earlier version: no shopifyBodyNote. An empty
  // description cannot tell it from a note typed in Intake, so it is left.
  const world = makeWorld({ row: { notes: MIXED, conditionIssues: SCRATCH } });
  await webhook(world, world.product("", 10));
  assert.equal(world.row.notes, MIXED, "left behind: not known to have come from Shopify");
  // The source still holds it and sends it again: now it is remembered...
  await webhook(world, world.product(MIXED, 60));
  assert.equal(world.row.notes, MIXED);
  assert.equal(world.memory(), MIXED);
  // ...so a removal at the source is followed.
  await webhook(world, world.product("", 120));
  assert.equal(world.row.notes, "");
});

// ---------------------------------------------------------------------------
// 5. The empty is not lost to timing.
// ---------------------------------------------------------------------------

async function burst(world, first, later, fresh, { gapMs = 20 } = {}) {
  resetIntakeMirrorRecentForTests();
  world.stamps.length = 0;
  world.events.length = 0;
  let freshReads = 0;
  const options = {
    log: world.log,
    loadShopifyProduct: async () => {
      freshReads += 1;
      return structuredClone(typeof fresh === "function" ? fresh() : fresh);
    },
    settleMs: 15,
    probeNotes: true,
  };
  const run = mirrorShopifyProductToIntakeCoalesced(structuredClone(first), options);
  await sleep(gapMs);
  for (const payload of later) {
    const mid = await mirrorShopifyProductToIntakeCoalesced(structuredClone(payload), options);
    assert.equal(mid.coalesced, true, "arrived while the first was in flight");
  }
  const done = await run;
  return { done, freshReads };
}

test("5. empty description then our own rebuild in one burst: the empty is still acted on", async () => {
  const world = makeWorld({
    holdMirrorMs: 80,
    row: { notes: MIXED, conditionIssues: SCRATCH, shopifyBodyNote: MIXED },
  });
  const own = world.ownBuild();
  assert.match(own, /scratch/i);
  const { freshReads } = await burst(
    world,
    world.product(own, 10),
    [world.product("", 20), world.product(own, 30)],
    world.product(own, 30)
  );
  assert.equal(freshReads, 1, "one fresh read for the burst");
  assert.equal(world.row.notes, "", "removal was not lost behind our own description");
  assert.equal(world.memory(), "");
  assert.equal(world.row.conditionIssues, "");
  assert.equal(rebuildAsks(world), 1, "one rebuild drops the sentence");
  assert.equal(event(world, "shopify.intake_mirror.traxia_note_cleared").source, "stashed_webhook");
});

test("5. note applied first, then emptied in the same burst, then our rebuild: ends removed", async () => {
  const world = makeWorld({ holdMirrorMs: 80 });
  const { done } = await burst(
    world,
    world.product(MIXED, 10),
    [world.product("", 20), world.product(`<p>${BASE}</p>`, 30)],
    world.product(`<p>${BASE}</p>`, 30)
  );
  assert.ok(done.followUpRounds >= 1);
  assert.equal(world.row.notes, "", "the later state (emptied) wins");
  assert.equal(world.memory(), "");
});

test("5. empty followed by a real note in the same burst: the note wins", async () => {
  const world = makeWorld({
    holdMirrorMs: 80,
    row: { notes: MIXED, conditionIssues: SCRATCH, shopifyBodyNote: MIXED },
  });
  const own = world.ownBuild();
  await burst(
    world,
    world.product(own, 10),
    [world.product("", 20), world.product(TWO_FLAWS, 30)],
    world.product(TWO_FLAWS, 30)
  );
  assert.equal(world.row.notes, TWO_FLAWS);
  assert.equal(world.memory(), TWO_FLAWS);
});

test("5. a payload with no body field is not an empty, and our own build is never a removal", async () => {
  const world = makeWorld({
    holdMirrorMs: 80,
    row: { notes: MIXED, conditionIssues: SCRATCH, shopifyBodyNote: MIXED },
  });
  const own = world.ownBuild();
  assert.equal(shopifyBodyIsIntakeBuild(own, world.row), true);
  assert.equal(readTraxiaNoteFromShopifyBody(own, world.row).reason, "own_build");
  assert.equal(traxiaBodyNoteFromPayload(world.product(undefined, 20)).reason, "body_omitted");
  assert.equal(traxiaBodyNoteFromPayload(world.product(undefined, 20)).known, false);
  await burst(
    world,
    world.product(own, 10),
    [world.product(undefined, 20), world.product(own, 30)],
    world.product(own, 30)
  );
  assert.equal(world.row.notes, MIXED, "nothing was removed");
  assert.equal(world.memory(), MIXED);
  assert.equal(noteStamps(world).length, 0);

  // Direct webhook without a body field: same.
  await webhook(world, world.product(undefined, 60));
  assert.equal(world.row.notes, MIXED);
  assert.equal(noteStamps(world).length, 0);
});

test("5. stash rule: the later Shopify state wins; with no order a note wins over an empty", () => {
  const note = { known: true, text: MIXED, reason: "traxia_note_body", at: at(10) };
  const empty = { known: true, text: "", reason: "empty_clear", at: at(20) };
  assert.equal(mergePendingTraxiaBodyNote(note, empty).text, "", "note then empty: empty");
  assert.equal(
    mergePendingTraxiaBodyNote({ ...empty, at: at(10) }, { ...note, at: at(20) }).text,
    MIXED,
    "empty then note: note"
  );
  assert.equal(
    mergePendingTraxiaBodyNote(empty, note).text,
    "",
    "an older note arriving after a newer empty does not undo it"
  );
  assert.equal(
    mergePendingTraxiaBodyNote({ ...note, at: at(10) }, { ...empty, at: at(10) }).text,
    MIXED,
    "same Shopify second: the note wins"
  );
  const own = { known: false, text: "", reason: "own_build" };
  const omitted = { known: false, text: "", reason: "body_omitted" };
  assert.equal(mergePendingTraxiaBodyNote(empty, own), empty);
  assert.equal(mergePendingTraxiaBodyNote(empty, omitted), empty);
  assert.equal(mergePendingTraxiaBodyNote(null, own), null);
});

// ---------------------------------------------------------------------------
// 6. Does not come back by itself.
// ---------------------------------------------------------------------------

test("6. after a removal: our rebuild, a later webhook and a late old payload do not put it back; the source can add it again", async () => {
  const world = makeWorld();
  await webhook(world, world.product(MIXED, 10));
  await webhook(world, world.product("", 60));
  assert.equal(world.row.notes, "");

  // Our own rebuild lands on Shopify and comes back as a webhook.
  const own = world.ownBuild();
  assert.equal(/scratch/i.test(own), false);
  await webhook(world, world.product(own, 70));
  assert.equal(world.row.notes, "");
  assert.equal(noteStamps(world).length, 0);
  assert.equal(rebuildAsks(world), 0, "no second rebuild, no loop");

  // A later webhook for something else (tags) with the same description.
  await webhook(world, world.product(own, 90, { tags: "FH, D, walnut" }));
  assert.equal(world.row.notes, "");
  assert.equal(world.memory(), "");
  assert.equal(noteStamps(world).length, 0);

  // The old payload from before the removal arrives late.
  const late = await webhook(world, world.product(MIXED, 10));
  assert.equal(world.row.notes, "", "a stale payload does not put the removed text back");
  assert.equal(world.memory(), "");
  assert.equal(noteStamps(world).length, 0);
  assert.equal(late.skipped === true || world.stamps.length === 0, true);

  // The same text typed again at the source (a newer payload) is a new note.
  await webhook(world, world.product(MIXED, 200));
  assert.equal(world.row.notes, MIXED, "stored again");
  assert.equal(world.memory(), MIXED);
});

test("6. a late old payload inside a burst does not put the removed text back", async () => {
  const world = makeWorld({ holdMirrorMs: 80 });
  await webhook(world, world.product(MIXED, 10));
  await webhook(world, world.product("", 60));
  assert.equal(world.row.notes, "");
  assert.equal(bodyNoteIsStaleAfterRemoval(PRODUCT_ID, MIXED, at(10)), true);
  assert.equal(bodyNoteIsStaleAfterRemoval(PRODUCT_ID, MIXED, at(200)), false, "newer: a new note");
  assert.equal(bodyNoteIsStaleAfterRemoval(PRODUCT_ID, CHIP, at(10)), false, "a different note");

  const own = world.ownBuild();
  await burst(
    world,
    world.product(own, 70),
    [world.product(MIXED, 10)],
    world.product(own, 70)
  );
  assert.equal(world.row.notes, "", "old payload stashed mid-burst is ignored");
  assert.equal(world.memory(), "");
  assert.ok(event(world, "shopify.intake_mirror.traxia_note_stale_ignored"));
});

test("6. after a shortening, a late payload with the longer note does not bring the scratch back", async () => {
  const world = makeWorld({ holdMirrorMs: 60 });
  await webhook(world, world.product(MIXED, 10));
  await webhook(world, world.product(CONSIGNOR, 60));
  const own = world.ownBuild();
  await burst(world, world.product(own, 70), [world.product(MIXED, 10)], world.product(own, 70));
  assert.equal(world.row.notes, CONSIGNOR);
  assert.equal(/scratch/i.test(world.listing()), false);
});

// ---------------------------------------------------------------------------
// 7. Removed in Intake.
// ---------------------------------------------------------------------------

test("7. text a person deleted in Intake stays deleted when the same description is seen again", async () => {
  // Stored and remembered, then a person emptied the Intake note field.
  const world = makeWorld({ row: { notes: "", conditionIssues: "", shopifyBodyNote: MIXED } });
  await webhook(world, world.product(MIXED, 60));
  assert.equal(world.row.notes, "", "not a new change: not stored again");
  assert.equal(noteStamps(world).length, 0);
  assert.equal(world.memory(), MIXED, "still known as seen, so it stays deleted next time too");
  assert.equal(rebuildAsks(world), 1, "the raw note on Shopify is replaced by the Intake listing");
  assert.equal(/scratch/i.test(world.listing()), false);

  // When the source then changes, the new text is taken.
  await webhook(world, world.product(CHIP, 120));
  assert.equal(world.row.notes, CHIP);
});

// ---------------------------------------------------------------------------
// 8. Log line.
// ---------------------------------------------------------------------------

test("8. a removal logs traxia_note_cleared with ids and how many characters left, never the text", async () => {
  const world = makeWorld();
  await webhook(world, world.product(MIXED, 10));
  await webhook(world, world.product("", 60));
  const cleared = event(world, "shopify.intake_mirror.traxia_note_cleared");
  assert.ok(cleared, "traxia_note_cleared logged");
  assert.equal(cleared.shopifyProductId, PRODUCT_ID);
  assert.equal(cleared.intakeItemId, ITEM_ID);
  assert.equal(cleared.action, "cleared");
  assert.equal(cleared.textLen, 0);
  assert.equal(cleared.removedLen, SCRATCH.length + CONSIGNOR.length);
  assert.equal(/scratch|consignor/i.test(JSON.stringify(world.events)), false, "no note text in logs");
  assert.ok(event(world, "shopify.intake_mirror.ok"));
});

test("8. a shortened note logs traxia_note_shortened with the same fields", async () => {
  const world = makeWorld();
  await webhook(world, world.product(MIXED, 10));
  assert.equal(event(world, "shopify.intake_mirror.traxia_note_body").action, "stored");
  await webhook(world, world.product(CONSIGNOR, 60));
  const shortened = event(world, "shopify.intake_mirror.traxia_note_shortened");
  assert.ok(shortened, "traxia_note_shortened logged");
  assert.equal(shortened.shopifyProductId, PRODUCT_ID);
  assert.equal(shortened.intakeItemId, ITEM_ID);
  assert.equal(shortened.action, "shortened");
  assert.equal(shortened.removedLen, SCRATCH.length);
  assert.equal(shortened.textLen, CONSIGNOR.length);
  assert.equal(/scratch|consignor/i.test(JSON.stringify(world.events)), false, "no note text in logs");

  // A different note in place of the old one is "replaced", with what left.
  await webhook(world, world.product(CHIP, 120));
  const replaced = event(world, "shopify.intake_mirror.traxia_note_body");
  assert.equal(replaced.action, "replaced");
  assert.equal(replaced.removedLen, CONSIGNOR.length);
  assert.deepEqual(
    shopifyNoteTextRemoved({
      notesBefore: MIXED,
      notesAfter: "",
      rememberedShopifyNote: MIXED,
      incomingShopifyNote: "",
    }),
    { removedLen: SCRATCH.length + CONSIGNOR.length, subset: false }
  );
});
