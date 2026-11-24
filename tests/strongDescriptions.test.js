import test from "node:test";
import assert from "node:assert/strict";
import {
  answerSentence,
  assessCleanProduct,
  bodyTooShortToMirror,
  buildListingProsePrompt,
  claimListingGeneration,
  composeShopifyBodyHtml,
  deriveListingSeoFields,
  descriptionHasStackedProductProse,
  descriptionIsBlankOrFiller,
  factualListingCopy,
  finalizeListingDescription,
  generateShopifyListingProse,
  listingCopyIsPublishable,
  listingGenerationFactsKey,
  listingProseIsThin,
  marketingListingBase,
  planSettledCleanliness,
  productSentences,
  resetListingGenerationMemory,
  settleBuyerListingCopy,
  truncateTitleKeepingType,
} from "../lib/shopifyIntakeImages.js";

/**
 * Strong descriptions: the sync server half. The same cases, with the same
 * texts and the same expected results, are in Pricing Engine
 * tests/test_strong_descriptions.py.
 */

const NAME = "Howard Terpning - White Water Passage Signed Artist's Proof Giclee Canvas - 39.5 x 45.5H";
const S1 =
  "Howard Terpning White Water Passage signed artists proof giclee on canvas wall art in a large vertical format.";
const S2 =
  "This signed artist's proof giclee on canvas by Howard Terpning is a collectible Western art print with rich, painterly color.";
const S3 = "The canvas surface gives the image a textured look compared with paper prints.";
const S4 = "It suits a living room, office, or hallway as a statement piece.";
const FULL = [S1, S2, S3, S4].join(" ");
const PICKUP =
  "Pickup is available in Scottsdale, and we offer both local and nationwide delivery. Please see Delivery, Pickup & Freight Options below the description for additional details.";
const ART = {
  name: NAME,
  brand: "",
  category: "ART - PRINT",
  merchType: "furniture",
  dimWidth: 39.5,
  dimDepth: 39.5,
  dimHeight: 45.5,
  weightLbs: 15,
};

const RH_ITEM = { name: "RH Curved Wing Grey Leather King Low Profile Bed- AS IS", brand: "RH", category: "BED" };
const RH_FIRST =
  "Grey leather king low profile platform bed with a curved wing upholstered headboard. Tailored grey leather upholstery wraps the headboard and frame for a clean, modern look.";
const RH_SECOND =
  "This RH curved wing grey leather king low profile bed features a sleek upholstered headboard and a distinctive wraparound wing silhouette. Designed to anchor your primary bedroom, this king-size platform bed showcases clean and tailored grey leather upholstery.";

// ---------------------------------------------------------------------------
// 1. Stop the cut, keep the protection.
// ---------------------------------------------------------------------------

test("the four-sentence art description survives whole", () => {
  assert.equal(marketingListingBase(FULL, ART), FULL);
  assert.equal(marketingListingBase(FULL), FULL, "same answer with no item to give");
  assert.equal(descriptionHasStackedProductProse(FULL, ART), false);
  const built = finalizeListingDescription(FULL, ART, "", "");
  for (const sentence of [S1, S2, S3, S4]) {
    assert.equal(built.split(sentence).length - 1, 1, `kept once: ${sentence}`);
  }
  assert.ok(built.startsWith(S1), "the opening is still the opening");
});

test("one sentence about the same subject is not a second write-up", () => {
  const two = `${S1} ${S2}`;
  assert.equal(marketingListingBase(two, ART), two);
  assert.equal(descriptionHasStackedProductProse(two, ART), false);
});

test("the same paragraph twice is reduced to one", () => {
  assert.equal(descriptionHasStackedProductProse(`${FULL} ${FULL}`, ART), true);
  assert.equal(marketingListingBase(`${FULL} ${FULL}`, ART), FULL);
  assert.equal(marketingListingBase(`${FULL}\n\n${FULL}`, ART), FULL);
  const built = finalizeListingDescription(`${FULL} ${FULL}`, ART, "", "");
  assert.equal(built.split(S2).length - 1, 1);
  assert.equal(built.split(S4).length - 1, 1);
});

test("two different write-ups of the same item back to back are reduced to one", () => {
  const doubled = `${RH_FIRST} ${RH_SECOND}`;
  assert.equal(descriptionHasStackedProductProse(doubled, RH_ITEM), true);
  assert.equal(marketingListingBase(doubled, RH_ITEM), RH_FIRST);

  const zinc = { name: "Zinc Top Desk", brand: "", category: "DESK" };
  const first =
    "Zinc top desk with two drawers and an iron base. The riveted zinc top has a soft grey patina.";
  const second =
    "This industrial writing desk pairs a riveted zinc top with a sturdy iron base. Two drawers keep supplies close, and the grey patina suits a loft office.";
  assert.equal(marketingListingBase(`${first} ${second}`, zinc), first);
});

// ---------------------------------------------------------------------------
// 3. One minimum.
// ---------------------------------------------------------------------------

test("one definition of too thin, counting product sentences only", () => {
  assert.equal(listingProseIsThin(S1), true, "one sentence of 110 characters is thin");
  assert.equal(listingCopyIsPublishable(S1), false);
  assert.equal(listingProseIsThin(`${S1} ${PICKUP}`), true, "the pickup close is not product copy");
  assert.equal(
    listingProseIsThin(`${S1} It measures 39.5 inches wide and 45.5 inches tall. ${PICKUP}`),
    true,
    "the size sentence is not product copy"
  );
  assert.equal(listingProseIsThin(`${S1} ${S2}`), false);
  assert.equal(listingCopyIsPublishable(`${S1} ${S2}`), true);
  assert.equal(listingProseIsThin(""), true);

  const html = composeShopifyBodyHtml(
    finalizeListingDescription(S1, ART, "", ""),
    { width: 39.5, depth: 39.5, height: 45.5, weightLbs: 15 },
    ART
  );
  assert.equal(listingProseIsThin(html), true, "the footer and the close do not make a body full");
  assert.equal(productSentences(html).length, 1);

  const thin = assessCleanProduct({ title: NAME, body: `${S1} ${PICKUP}`, tags: "FH, A" });
  assert.ok(thin.violations.includes("body_thin"));
  const full = assessCleanProduct({ title: NAME, body: `${FULL} ${PICKUP}`, tags: "FH, A" });
  assert.equal(full.violations.includes("body_thin"), false);
});

test("the Shopify mirror still takes a one-sentence description typed in Shopify", () => {
  // The mirror asks only whether there is a description at all (48 characters),
  // not whether it is a finished one.
  assert.equal(bodyTooShortToMirror(S1), false);
  assert.equal(bodyTooShortToMirror("Nice chair."), true);
});

// ---------------------------------------------------------------------------
// 4. Thin copy is never dirty forever.
// ---------------------------------------------------------------------------

test("thin copy asks for no rebuild and is never a cleanup error", () => {
  const thin = { title: NAME, body: `${S1} ${PICKUP}`, tags: "FH, A" };
  for (const options of [{}, { corrective: true }, { correctivePass: 1 }, { correctivePass: 2 }]) {
    const plan = planSettledCleanliness(thin, options);
    assert.equal(plan.action, "none", JSON.stringify(options));
    assert.deepEqual(plan.violations, []);
    assert.equal(plan.thin, true);
    assert.equal(plan.note, undefined);
  }
});

test("a generation is claimed once per item per change", () => {
  resetListingGenerationMemory();
  const facts = { name: NAME, brand: "", category: "ART - PRINT", notes: "", dims: { width: 39.5 }, description: S1 };
  const key = listingGenerationFactsKey(facts);
  assert.equal(claimListingGeneration("item-1", key), true);
  for (let i = 0; i < 20; i += 1) assert.equal(claimListingGeneration("item-1", key), false);
  // Storing a longer description is not a new change.
  assert.equal(claimListingGeneration("item-1", listingGenerationFactsKey({ ...facts, description: FULL })), false);
  // A new name, a new note, or a deleted description is.
  assert.equal(
    claimListingGeneration("item-1", listingGenerationFactsKey({ ...facts, name: `${NAME} II` })),
    true
  );
  assert.equal(claimListingGeneration("item-1", listingGenerationFactsKey({ ...facts, description: "" })), true);
  // Another item is not blocked.
  assert.equal(claimListingGeneration("item-2", key), true);
  assert.equal(claimListingGeneration("", key), false);
  resetListingGenerationMemory();
});

// ---------------------------------------------------------------------------
// 5. No filler fallback.
// ---------------------------------------------------------------------------

test("an item with no description gets only its recorded facts", () => {
  assert.equal(
    factualListingCopy(ART),
    "Howard Terpning, White Water Passage Signed Artist's Proof Giclee Canvas. It is listed under Art - Print."
  );
  assert.equal(
    factualListingCopy({ name: "Zinc Top Desk - 60X30X30H - AS IS", brand: "Restoration Hardware", category: "DESK" }),
    "Zinc Top Desk by Restoration Hardware."
  );
  // A description the build throws out whole (test words): the recorded facts stand in.
  const built = finalizeListingDescription("This Bernis test chair is a hello world piece.", ART, "", "");
  assert.doesNotMatch(built, /straightforward piece|everyday use/i);
  assert.match(built, /^Howard Terpning, White Water Passage/);
  // The fixed filler sentence older builds stored is not a description any more.
  const filler = "This piece is a straightforward piece for everyday use.";
  assert.equal(marketingListingBase(filler, ART), "");
  assert.equal(descriptionIsBlankOrFiller(filler), true);
  assert.equal(descriptionIsBlankOrFiller(`${filler} ${PICKUP}`), true);
  assert.equal(descriptionIsBlankOrFiller(""), true);
  assert.equal(descriptionIsBlankOrFiller(S1), false);
  assert.equal(listingProseIsThin(`${filler} ${S1}`), true, "the filler sentence is not counted");
  assert.equal(marketingListingBase(`${filler} ${FULL}`, ART), FULL);
  assert.doesNotMatch(finalizeListingDescription(filler, ART, "", ""), /straightforward piece|everyday use/i);
});

test("rejected model text keeps the previous description, or the recorded facts", () => {
  const rejected = "Based on the name provided, no additional details are stated.";
  const kept = settleBuyerListingCopy(rejected, NAME, null, "", FULL, ART);
  assert.equal(kept.description, FULL);
  const blank = settleBuyerListingCopy(rejected, NAME, null, "", "", ART);
  assert.equal(blank.description, factualListingCopy(ART));
  for (const out of [kept.description, blank.description]) {
    assert.doesNotMatch(out, /straightforward piece|everyday use/i);
  }
});

// ---------------------------------------------------------------------------
// 6. Prompts and the search fields.
// ---------------------------------------------------------------------------

test("the art prompt has the art instructions and the artist, not the furniture rules", () => {
  const prompt = buildListingProsePrompt({
    name: NAME,
    notes: "",
    brand: "",
    category: "ART - PRINT",
    merchType: "furniture",
    tags: "FH, A",
    dims: { width: 39.5, depth: 39.5, height: 45.5 },
  });
  assert.equal(prompt.mode, "art");
  const text = `${prompt.system}\n${prompt.user}`;
  assert.match(text, /COPY MODE ART/);
  assert.match(text, /ARTIST \(from the title, because the Brand field is empty\): Howard Terpning\./);
  assert.match(text, /Name this artist in the description/);
  assert.match(text, /the medium/);
  assert.match(text, /edition type/);
  assert.match(text, /whether it is signed/);
  assert.match(text, /RECORDED SIZE: 39\.5 inches wide by 45\.5 inches high/);
  assert.match(text, /Never give a depth for wall art/);
  assert.match(text, /Sentence 1 says what the item is in the words a shopper would search/);
  assert.match(text, /stands alone as the answer to what is this/);
  assert.match(text, /Length follows the facts/);
  assert.match(text, /Do not describe what this piece shows/);
  assert.match(text, /Never invent condition, damage, wear, repairs, provenance/);
  // Rules already agreed stay.
  assert.match(text, /one paragraph/);
  assert.match(text, /Never wrap a sentence, tagline, or highlight in quotation marks/);
  assert.match(text, /AS IS appears only there/);
  assert.match(text, /Scottsdale pickup and delivery close is added once at the end/);
  // The furniture instructions and the ban on naming a person are gone.
  assert.doesNotMatch(text, /COPY MODE FURNITURE/);
  assert.doesNotMatch(text, /Do not name a maker/);
  assert.doesNotMatch(text, /personal names in the title/);
  assert.doesNotMatch(text, /what it fits or where it works/);
  assert.doesNotMatch(text, /\b\d\s*(?:to|-|–)\s*\d\s+sentences\b/i);

  const branded = buildListingProsePrompt({ name: NAME, brand: "Howard Terpning", category: "ART - PRINT" });
  assert.match(branded.user, /ARTIST \(the Brand field\): Howard Terpning/);
});

test("a furniture prompt keeps its maker rule and asks for no sentence count", () => {
  const prompt = buildListingProsePrompt({
    name: "Walnut Three Drawer Nightstand - 24X18X26H",
    brand: "",
    category: "NIGHT STAND",
    merchType: "furniture",
  });
  assert.equal(prompt.mode, "furniture");
  const text = `${prompt.system}\n${prompt.user}`;
  assert.match(text, /COPY MODE FURNITURE/);
  assert.match(text, /BRAND: \(empty\)\. Do not name a maker\./);
  assert.doesNotMatch(text, /COPY MODE ART/);
  assert.match(text, /Length follows the facts/);
  assert.doesNotMatch(text, /\b\d\s*(?:to|-|–)\s*\d\s+sentences\b/i);
  // A mirror filed under art is not written as art.
  const mirror = buildListingProsePrompt({ name: "Gold Framed Wall Mirror", category: "ART / MIRRORS" });
  assert.equal(mirror.mode, "furniture");
});

test("the answer text is the sentence that names the item, not the longest one", () => {
  const filler =
    "Art changes the way a room feels, and a large canvas like this one brings warmth, depth, and a strong focal point to a living room, office, or hallway.";
  assert.ok(filler.length > S2.length && filler.length > S1.length);
  assert.equal(answerSentence([filler, S3, S2], { name: NAME, brand: "" }), S2);
  const fields = deriveListingSeoFields({
    name: NAME,
    description: [S1, filler, S3].join(" "),
    brand: "",
    category: "ART - PRINT",
  });
  assert.equal(fields.aeoDescription, S1);
});

test("the search title is cut at a word and keeps the item type", () => {
  const fields = deriveListingSeoFields({ name: NAME, description: FULL, brand: "", category: "ART - PRINT" });
  assert.equal(fields.seoTitle, "Howard Terpning - White Water Passage Signed Artist's Proof Canvas");
  assert.ok(fields.seoTitle.length <= 70);
  assert.equal(/39\.5/.test(fields.seoTitle), false, "the size token is not in the search title");
  assert.equal(truncateTitleKeepingType("Walnut Side Chair", 70), "Walnut Side Chair");
  const long = truncateTitleKeepingType(
    "Pair of Vintage Mid Century Walnut Nightstands with Marble Tops and Polished Brass Pulls",
    70
  );
  assert.equal(long, "Pair of Vintage Mid Century Walnut Nightstands with Marble Tops");
});

// ---------------------------------------------------------------------------
// The model call itself, with a stand-in for the model.
// ---------------------------------------------------------------------------

test("a model that is down or answers thin leaves the previous description in place", async () => {
  const previousKey = process.env.OPENAI_API_KEY;
  process.env.OPENAI_API_KEY = "test-key";
  try {
    let calls = 0;
    const down = await generateShopifyListingProse(
      { ...ART, description: S1 },
      {
        post: async () => {
          calls += 1;
          return { status: 503, data: {} };
        },
      }
    );
    assert.equal(down.ok, false);
    assert.equal(calls, 1, "a failed call is not repeated");

    calls = 0;
    const thin = await generateShopifyListingProse(
      { ...ART, description: FULL },
      {
        post: async () => {
          calls += 1;
          return {
            status: 200,
            data: { choices: [{ message: { content: JSON.stringify({ description: "A signed giclee.", tags: "FH, A" }) } }] },
          };
        },
      }
    );
    assert.ok(calls <= 2, `one call and at most one second try inside a generation (got ${calls})`);
    if (thin.ok) assert.equal(thin.description, FULL, "a thin answer never replaces a full description");
    assert.doesNotMatch(String(thin.description || ""), /straightforward piece/i);

    let prompt = "";
    const good = await generateShopifyListingProse(
      { ...ART, description: "" },
      {
        post: async (_url, data) => {
          prompt = data.messages.map((message) => message.content).join("\n");
          return {
            status: 200,
            data: { choices: [{ message: { content: JSON.stringify({ description: FULL, tags: "FH, A" }) } }] },
          };
        },
      }
    );
    assert.equal(good.ok, true);
    assert.equal(good.description, FULL, "a full answer about an artist is kept whole");
    assert.match(prompt, /COPY MODE ART/);
    assert.match(prompt, /Howard Terpning\. Name this artist/);
  } finally {
    if (previousKey == null) delete process.env.OPENAI_API_KEY;
    else process.env.OPENAI_API_KEY = previousKey;
  }
});
