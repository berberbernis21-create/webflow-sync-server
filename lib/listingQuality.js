/**
 * Deterministic quality rules for listing descriptions.
 *
 * Pure text functions: no model, no network, no database, and no import from
 * the rest of the sync server. Pricing Engine carries the same rules, word for
 * word, in Back_End/lost_found_intake/listing_quality.py. Both services run
 * them on the same text, so a change here must be made there too
 * (tests/fixtures/listing_quality_cases.json is the same file in both repos
 * and pins the two to the same answers).
 *
 * What is here:
 *   - the maker named at the start of an item name (nameLead, requiredMaker)
 *   - whether a description names that maker (makerPresent)
 *   - whether an opening sentence only says the name again (openingParrotsName)
 *   - whether an opening is a keyword fragment and not a sentence (openingIsFragment)
 *   - sentences that admit a missing fact or say nothing (bannedSentence),
 *     a material that repeats itself (trimSelfMaterial), and a later sentence
 *     that only restates the name and the sentences before it (restatedIndexes)
 *   - the size sentence for home goods, chosen by what the item is and how big
 *     it is (homeSizeCandidates, pickSizeSentence)
 */

const words = (text) => new Set(String(text).split(/\s+/).filter(Boolean));

// ---------------------------------------------------------------------------
// Words
// ---------------------------------------------------------------------------

const STOP_WORDS = words(`
  a an the and or of for with in on to this that these those is are was were be been
  it its as at by from into onto over under than then very just more most such also
  while which who has have had not but you your our their any each both all one two
  w h x l d s t
`);

// Words that describe a thing. They are never a maker, so the run of words at
// the start of an item name ends at the first one of these.
const DESCRIPTIVE_WORDS = words(`
  black white gold golden silver gray grey red blue green brown tan beige ivory cream navy pink purple
  orange yellow teal turquoise aqua burgundy maroon bronze copper brass pewter chrome nickel platinum
  rose blush taupe charcoal ebony ebonized blonde blond natural neutral multicolor multi colorful clear
  amber cobalt emerald jade ruby sapphire coral olive sage mint mustard rust plum lavender indigo khaki
  wood wooden oak walnut mahogany teak pine maple cherry birch elm ash bamboo rattan wicker cane rush
  burl veneer lacquer lacquered glass crystal ceramic porcelain pottery stoneware earthenware terracotta
  clay metal iron steel aluminum tin zinc wrought cast leather suede velvet linen cotton wool silk
  fabric upholstered upholstery marble granite stone travertine onyx alabaster concrete resin acrylic
  lucite plastic paper canvas capiz shell bone horn pearl beaded bead beads feather feathers fur faux
  mirrored mirror gilt gilded enamel enameled mosaic tile woven wire rope jute sisal seagrass mohair
  chenille tweed boucle
  large small medium big huge mini miniature oversize oversized tall short long wide narrow low high
  petite grand giant little tiny compact slim round square oval rectangular oblong circular curved
  triple double single pair set lot group collection trio duo
  vintage antique modern modernist contemporary traditional transitional rustic ornate classic classical
  retro mcm mid century midcentury post postmodern deco nouveau victorian colonial georgian regency
  baroque rococo gothic industrial farmhouse coastal bohemian boho tribal ethnic folk primitive
  southwest southwestern western native american french italian english spanish danish swedish
  scandinavian chinese japanese asian african indian mexican moroccan persian turkish tuscan
  mediterranean european chinoiserie
  striped stripe stripes horizontal vertical harlequin polka dot dots checked checkered check plaid
  floral filigree filagree swirl swirled swirls geometric abstract paisley damask toile ikat pattern
  patterned solid
  hand handmade handcrafted blown carved painted glazed signed numbered limited edition original
  framed unframed custom salvaged reclaimed distressed weathered aged polished brushed hammered
  embossed etched engraved inlaid inlay tufted quilted embroidered slumped coiled turned pierced
  footed lidded
  ornament ornaments candlestick candlesticks candle holder holders urn urns vase vases pitcher
  pitchers pot pots planter planters basket baskets bowl bowls tray trays box boxes jar jars bottle
  bottles statue statues sculpture sculptures figurine figurines figure bust table tables chair chairs
  sofa sofas loveseat sectional bench benches stool stools ottoman desk desks console consoles cabinet
  cabinets armoire wardrobe dresser dressers nightstand nightstands bed beds headboard bookcase shelf
  shelves sideboard buffet credenza hutch chest trunk lamp lamps chandelier sconce pendant lantern
  rug rugs runner carpet painting paintings print prints giclee lithograph serigraph etching poster
  photograph art artwork clock clocks frame frames pillow pillows throw blanket board cutting plate
  plates platter dish dishes cup cups mug mugs glasses decanter teapot kettle refrigerator cooler bar
  cart stand rack hook hooks wall floor accent decor decorative piece item christmas holiday easter
  halloween thanksgiving seasonal ball drop style design santa tree bird bell pig pear apple horse dog
  cat elephant lion owl fish rooster angel snowman reindeer pumpkin flower flowers leaf leaves star
  heart moon sun wine bottle zone dual area room living dining bedroom office outdoor patio garden
  kitchen bath entry reading life proof artist
  where when what how why there here new old used
`);

// Words that may sit in front of the maker without ending the search for one.
const LEAD_SKIP_WORDS = words(`
  vintage antique large small medium big huge mini pair set lot signed original rare custom new old
  mcm the a an of two three four five six
`);

const TEST_NAME_RE = /\b(?:test|testing|tests)\b/i;
const BLANK_BRANDS = new Set([
  "", "unbranded", "nobrand", "no-brand", "non-branded", "nonbranded", "none", "n/a", "na", "unknown",
]);

function normalize(text) {
  let s = String(text ?? "").toLowerCase().replace(/\u2019/g, "'");
  s = s.replace(/\bb\s*[&/]\s*w\b/g, "black white");
  s = s.replace(/\bw\s*\/\s*/g, "with ");
  s = s.replace(/\bpost[\s-]+modern/g, "postmodern");
  s = s.replace(/\bmid[\s-]+century/g, "midcentury");
  s = s.replace(/\bmcm\b/g, "midcentury modern");
  s = s.replace(/\bfilagree\b/g, "filigree");
  s = s.replace(/\bblond\b/g, "blonde");
  s = s.replace(/\bgrey\b/g, "gray");
  s = s.replace(/\bas[\s-]*is\b/g, " ");
  return s;
}

function stem(word) {
  if (word.length > 4 && word.endsWith("ies")) return `${word.slice(0, -3)}y`;
  if (
    word.length > 4 &&
    (word.endsWith("ches") || word.endsWith("shes") || word.endsWith("xes") || word.endsWith("sses"))
  ) {
    return word.slice(0, -2);
  }
  if (
    word.length > 3 &&
    word.endsWith("s") &&
    !word.endsWith("ss") &&
    !word.endsWith("us") &&
    !word.endsWith("is")
  ) {
    return word.slice(0, -1);
  }
  return word;
}

/** Lowercase words of two or more letters. A possessive keeps its owner (artist's -> artist). */
function letterWords(text) {
  const out = [];
  for (const raw of normalize(text).match(/[a-z]+(?:'[a-z]+)?/g) || []) {
    const word = raw.split("'")[0];
    if (word.length >= 2) out.push(word);
  }
  return out;
}

/** Stems of the words that carry meaning, in order. Numbers and filler words are left out. */
export function contentStems(text) {
  return letterWords(text)
    .filter((word) => !STOP_WORDS.has(word))
    .map(stem);
}

function allStems(text) {
  return letterWords(text).map(stem);
}

export function splitSentences(text) {
  return String(text ?? "")
    .trim()
    .split(/(?<=[.!?])\s+/)
    .map((part) => part.trim())
    .filter(Boolean);
}

// ---------------------------------------------------------------------------
// The maker in the item name
// ---------------------------------------------------------------------------

/**
 * The brand, maker, designer or artist words an item name starts with, as written.
 *
 * "Mackenzie Childs B&W Harlequin Ornament" -> "Mackenzie Childs".
 * "Greg Sheres Post Modern Blonde Armoire- 42X23X75" -> "Greg Sheres".
 * "Large Black Velvet Ornament" -> "" (the name starts with descriptive words).
 * "Original Abstract Art By Liz Cummings- 31X2.5X70H" -> "Liz Cummings" (the
 * name says who it is by).
 * "Side By Side Refrigerator" -> "" (words around By are not a maker).
 * Empty for a name that carries test words, and for a name that is nothing
 * but those leading words (a painting title is not a maker).
 */
export function nameLead(name) {
  return leadInfo(name).lead;
}

/** { lead, certain }. certain is false for a single word found only after "By". */
function leadInfo(name) {
  const none = { lead: "", certain: false };
  const text = String(name ?? "").trim();
  if (!text || TEST_NAME_RE.test(text)) return none;
  const tokens = text.split(/\s+/).filter(Boolean);
  const tidy = (list) => list.join(" ").replace(/^[ -]+|[ -]+$/g, "");
  const { lead, used } = makerRun(tokens, 0, true);
  // "Side By Side ...": words in front of By describe the item, they are not its maker.
  const beforeBy = used < tokens.length && tokens[used].toLowerCase() === "by";
  if (lead.length && used < tokens.length && !beforeBy) return { lead: tidy(lead), certain: true };
  for (let index = 1; index < tokens.length - 1; index += 1) {
    if (tokens[index].toLowerCase() !== "by") continue;
    // "Side By Side", "Step By Step": the same word on both sides is not a maker.
    if (tokens[index - 1].toLowerCase() === tokens[index + 1].toLowerCase()) continue;
    const named = makerRun(tokens, index + 1, false).lead;
    if (named.length && named[0].replace(/[^A-Za-z]/g, "").length >= 3) {
      // Two or three capitalized words after By read as a person or a company.
      return { lead: tidy(named), certain: named.length >= 2 };
    }
  }
  return none;
}

/** Up to three capitalized, non-descriptive words from tokens[start...]. Returns { lead, used }. */
function makerRun(tokens, start, skipLeadWords) {
  const lead = [];
  let used = start;
  for (const token of tokens.slice(start)) {
    const bare = token.replace(/^[,;:()[\]"]+|[,;:()[\]"]+$/g, "");
    const parts = bare
      .split(/[^A-Za-z]+/)
      .filter(Boolean)
      .map((part) => part.toLowerCase());
    if (skipLeadWords && !lead.length && parts.length && parts.every((part) => LEAD_SKIP_WORDS.has(part))) {
      used += 1;
      continue;
    }
    const shaped = /^[A-Z][A-Za-z'.-]*$/.test(bare);
    if (!shaped || !parts.length || bare.toLowerCase() === "by") break;
    if (parts.some((part) => DESCRIPTIVE_WORDS.has(part))) break;
    lead.push(bare.replace(/[,;:]+$/, ""));
    used += 1;
    if (lead.length >= 3 || /[,;:]$/.test(token.trimEnd())) break;
  }
  return { lead, used };
}

/** The maker a description must name: the Brand field, else the name's leading maker words. */
export function requiredMaker(name, brand = "") {
  const field = String(brand ?? "").replace(/\s+/g, " ").trim();
  if (!BLANK_BRANDS.has(field.toLowerCase())) return field;
  return nameLead(name);
}

/**
 * True when the maker may be written into a description by code: the Brand
 * field is filled, or the item name clearly gives one. A single word found
 * only after "By" is not certain: it is flagged when missing, never inserted.
 */
export function makerIsCertain(name, brand = "") {
  const field = String(brand ?? "").replace(/\s+/g, " ").trim();
  if (!BLANK_BRANDS.has(field.toLowerCase())) return true;
  return leadInfo(name).certain;
}

/** Lowercase words of the name's leading maker (three letters or more). Never invented makers. */
export function nameMakerTokens(name) {
  return (nameLead(name).toLowerCase().match(/[a-z]+/g) || []).filter((word) => word.length >= 3);
}

/** True when every word of the maker is in the text (Mackenzie-Childs counts for Mackenzie Childs). */
export function makerPresent(text, maker) {
  const wanted = allStems(maker);
  if (!wanted.length) return true;
  const have = new Set(allStems(text));
  return wanted.every((word) => have.has(word));
}

/** A work of art by its category or by an art medium in its name. Mirrors are not art. */
export function isArtName(name, category = "") {
  const title = String(name ?? "");
  if (/\bmirrors?\b/i.test(title)) return false;
  if (/^\s*art\b/i.test(String(category ?? ""))) return true;
  return /\b(?:giclee|lithograph|serigraph|etching|engraving|woodcut|linocut|screen\s*print|silkscreen|artist'?s?\s+proof|oil\s+on\s+\w+|acrylic\s+on\s+\w+|watercolou?r|gouache|oil\s+painting|paintings?|art\s+print|limited\s+edition)\b/i.test(
    title
  );
}

// ---------------------------------------------------------------------------
// The opening sentence
// ---------------------------------------------------------------------------

function unique(list) {
  const seen = new Set();
  const out = [];
  for (const word of list) {
    if (!seen.has(word)) {
      seen.add(word);
      out.push(word);
    }
  }
  return out;
}

/**
 * True when the first sentence is the item name said again.
 *
 * Two ways to be the name again:
 *   1. Four out of five of the opening's own content words come from the name.
 *   2. The opening starts with a word from the name (or the maker) and nearly
 *      every content word of the name (four out of five, at least two) is in
 *      its first few content words. Not applied to a work of art, whose
 *      opening has to state the artist, the title and the medium.
 * A name with fewer than two descriptive words cannot be judged.
 */
export function openingParrotsName(opening, name, maker = "", art = false) {
  const makerStems = new Set(allStems(maker));
  const nameWords = unique(contentStems(name).filter((word) => !makerStems.has(word)));
  const openAll = contentStems(opening);
  const openWords = openAll.filter((word) => !makerStems.has(word));
  if (nameWords.length < 2 || !openWords.length) return false;
  const nameSet = new Set(nameWords);
  const fromName = openWords.filter((word) => nameSet.has(word)).length;
  if (fromName * 5 >= openWords.length * 4) return true;
  if (art) return false;
  const first = openAll[0];
  if (!nameSet.has(first) && !makerStems.has(first)) return false;
  const window = new Set(openWords.slice(0, nameWords.length + 2));
  const hits = nameWords.filter((word) => window.has(word)).length;
  const need = Math.max(2, Math.floor((4 * nameWords.length + 4) / 5));
  return hits >= need;
}

const VERB_WORDS = words(`
  is are was were has have had can will would does did
  brings bring adds add makes make gives give offers offer combines combine creates create lends lend
  keeps keep suits suit fits works hangs hang sits sit rests rises rise catches catch reflects reflect
  carries carry holds hold comes come turns takes take shows wears feels looks reads glows shines
  draws draw anchors blends delivers deliver captures capture provides provide showcases showcase
  depicts depict evokes evoke recalls recall echoes softens soften brightens brighten warms
  elevates elevate complements complement wraps wrap opens closes
  serves serve stretches stretch runs spans reaches reach measures weighs
  surrounds surround features feature pairs stands lets invites invite celebrates
  celebrate remains remain stays stay becomes become seems seem appears appear belongs belong includes
  include contains contain tells tell speaks speak leans lean meets meet
  defines define decorates decorate
  extends extend swivels reclines lifts hides reveals reveal
  sleeps fills fill completes introduces introduce
  glints gleams sparkles shimmers dangles swings spins hovers floats perches
  follows follow flanks flank tapers taper flares flare
`);
const VERB_SHAPE_RE = /\b[a-z]{2,}s\s+(?:a|an|the|this|that|these|its|it|any|every|each|your|you|both)\b/;
const CONTRACTION_RE = /\b(?:it|that|there|here|what)'s\b|\b(?:they|you|we)'re\b/;
// A past-tense verb with its object: "created White Water Passage", "painted the scene".
// Read on the sentence as written, because the object may be a capitalized title.
// "hand-painted vase" and "designed for the holidays" are not this shape.
const PAST_VERB_SHAPE_RE =
  /\b(?:created|painted|designed|produced|built|carved|founded|published|introduced|made)\s+(?:(?:a|an|the|this|these|his|her|its|their)\b|[A-Z][a-z])/;

/** True when the first sentence has no verb: a keyword line, not a sentence. */
export function openingIsFragment(opening) {
  const text = normalize(opening);
  const found = text.match(/[a-z]+/g) || [];
  if (!found.length) return true;
  if (found.some((word) => VERB_WORDS.has(word))) return false;
  if (VERB_SHAPE_RE.test(text) || CONTRACTION_RE.test(text)) return false;
  if (PAST_VERB_SHAPE_RE.test(String(opening ?? ""))) return false;
  return true;
}

function articleFor(word) {
  return /^[aeiou]/.test(String(word ?? "").toLowerCase()) ? "an" : "a";
}

/**
 * Put the maker in front of the item, the way the item name has it.
 *
 * "..., this oversized ornament brings ..." -> "..., this Mackenzie Childs oversized ornament brings ...".
 * "A tall armoire ..." -> "A Greg Sheres tall armoire ...".
 * "This is a tall armoire ..." -> "This is a Greg Sheres tall armoire ...".
 * Returns "" when the sentence has no this / these / a / an / the in front of
 * the item to hang the maker on.
 */
export function insertMakerIntoOpening(opening, maker) {
  const text = String(opening ?? "").trim();
  const who = String(maker ?? "").replace(/\s+/g, " ").trim();
  if (!text || !who) return "";
  for (const match of text.matchAll(/\b(?:this|these)\s+([A-Za-z][A-Za-z'-]*)/gi)) {
    // "This is ..." uses this as the subject, not in front of the item.
    if (VERB_WORDS.has(match[1].toLowerCase())) continue;
    const at = match.index + match[0].length - match[1].length;
    return `${text.slice(0, at)}${who} ${text.slice(at)}`;
  }
  let match = text.match(/^((?:this|it)\s+is\s+)(an?)\s+/i);
  if (match) return `${match[1]}${articleFor(who)} ${who} ${text.slice(match[0].length)}`;
  match = text.match(/^the\s+/i);
  if (match) return `${text.slice(0, match[0].length)}${who} ${text.slice(match[0].length)}`;
  match = text.match(/^(an?)\s+/i);
  if (match) {
    let article = articleFor(who);
    if (/^[A-Z]/.test(match[1])) article = article[0].toUpperCase() + article.slice(1);
    return `${article} ${who} ${text.slice(match[0].length)}`;
  }
  return "";
}

// ---------------------------------------------------------------------------
// Sentences that should not be published
// ---------------------------------------------------------------------------

const UNKNOWN_FACT_RE =
  /\b(?:unspecified|unidentified|unknown|undetermined|unbranded)\b|\bnot\s+(?:specified|known|stated|identified|listed|provided|recorded|indicated|disclosed)\b|\bmixed[\s-]+materials?\b|\b(?:material|materials|maker|brand|origin|artist|designer|age|manufacturer)\s+(?:is|are|was|remains?)\s+(?:unclear|uncertain|unverified)\b|\bcannot\s+be\s+(?:determined|confirmed|verified)\b|\b(?:does|do|did)(?:\s+not|n't)\s+(?:state|specify|list|give|provide|record|indicate|mention|disclose|identify)\b|\bnot\s+(?:mentioned|given|documented|noted)\b|\bno\s+(?:[a-z'-]+\s+){0,6}(?:is|are|was|were)\s+(?:stated|recorded|specified|provided|listed|indicated|given|mentioned|noted|documented|known|available)\b/i;
// A sentence about the paperwork and not the piece: "the record", "the listing",
// "our records", "the data". A record cabinet or a record player is furniture.
const RECORD_MENTION_RE =
  /\b(?:the|our|this|its)\s+(?:record|records|listing|data)\b(?!\s+(?:cabinet|player|collection|storage|stand|rack|holder|console|album|albums|shelf|crate|box|bin|changer|sleeve|label|store|shop)\b)/i;
// A sentence that states a title: "titled White Water Passage", "The work title is Abstract Art."
const TITLE_CLAIM_RE =
  /\b(?:[Tt]itled|[Ee]ntitled)\s+["']?(?=[A-Z])|\b[Tt]itle(?:\s+of\s+(?:the|this)\s+[a-z]+)?(?:\s+(?:is|reads)\b|\s*:)\s*|\b(?:work|piece|painting|print|artwork|canvas|composition|sculpture)\s+is\s+(?:called|named)\s+/;
// Type, style and medium words. They are never the title of a work.
const GENERIC_TITLE_STEMS = new Set(
  [
    ...words(`
  abstract abstraction art artwork artist painting painted paint print printed landscape seascape
  cityscape still life mixed media original work piece composition study portrait figure figurative
  floral botanical scene untitled canvas oil acrylic watercolor watercolour gouache pastel ink charcoal
  pencil giclee lithograph serigraph etching engraving sculpture collage panel wall hanging signed
  numbered limited edition proof framed unframed modern modernist contemporary impressionist
  expressionist minimalist decorative fine large small
`),
  ].map(stem)
);
const EMPTY_LINE_RE =
  /\ba\s+well[\s-]placed\b|\bcan\s+make\s+a(?:ny)?\s+room\s+feel\b|\binstantly\s+more\s+finished\b|\bchanges\s+the\s+way\s+a\s+room\s+feels\b|\badds\s+character\s+to\s+any\s+(?:space|room|home)\b|\bperfect\s+for\s+any\s+(?:room|space|home|decor)\b|\bconversation[\s-]starter\b|\bties\s+(?:the|any|a)\s+room\s+together\b|\bworks\s+in\s+everyday\s+rooms\b|\bsure\s+to\s+impress\b/i;
const SELF_MATERIAL_RE =
  /,?\s+(?:that\s+is\s+|which\s+is\s+)?(?:made|crafted|constructed)\s+(?:out\s+of|of|from|with|in)\s+(?:an?\s+|the\s+)?([a-z]+)\b/i;

/**
 * True when a sentence gives the work a title the item name does not carry.
 *
 * "The work title is Abstract Art." for "Original Abstract Art By Liz
 * Cummings": the words are a type and a style, not a title. A stated title is
 * real only when one of its words is in the item name and is not the maker
 * and not a type, style or medium word ("titled White Water Passage" for
 * "Howard Terpning White Water Passage Signed Artist's Proof Giclee").
 * Calling a work untitled is a title claim too, unless the name says so.
 */
export function inventedTitleSentence(sentence, name = "") {
  const text = String(sentence ?? "");
  const title = String(name ?? "");
  if (/\buntitled\b/i.test(text) && !/\buntitled\b/i.test(title)) return true;
  const match = TITLE_CLAIM_RE.exec(text);
  if (!match) return false;
  const span = text.slice(match.index + match[0].length).split(/[,.;:!?()]|\s+[Bb]y\s+/)[0];
  const found = contentStems(span).slice(0, 8);
  if (!found.length) return false;
  const makerStems = new Set(allStems(nameLead(title)));
  const real = new Set(
    contentStems(title).filter((word) => !makerStems.has(word) && !GENERIC_TITLE_STEMS.has(word))
  );
  return !found.some((word) => real.has(word));
}

/**
 * A sentence that must never be published, whatever is left without it.
 *
 * It reports a missing fact (unspecified, unknown, not stated, mixed
 * materials), talks about the record, the listing or the data instead of the
 * piece, or states a title the item does not have.
 */
export function unpublishableSentence(sentence, name = "") {
  const text = String(sentence ?? "");
  return UNKNOWN_FACT_RE.test(text) || RECORD_MENTION_RE.test(text) || inventedTitleSentence(text, name);
}

/** A line that could describe any item ("A well-placed armoire can make a room feel more finished"). */
export function emptyLineSentence(sentence) {
  return EMPTY_LINE_RE.test(String(sentence ?? ""));
}

/** A sentence that must never be published, or a line that could describe any item. */
export function bannedSentence(sentence, name = "") {
  return unpublishableSentence(sentence, name) || emptyLineSentence(sentence);
}

/** "a velvet ornament made from velvet" -> "a velvet ornament". Other sentences are returned as they are. */
export function trimSelfMaterial(sentence) {
  const text = String(sentence ?? "");
  const match = text.match(SELF_MATERIAL_RE);
  if (!match) return text;
  const material = stem(match[1].toLowerCase());
  const before = new Set(allStems(text.slice(0, match.index)));
  if (!before.has(material)) return text;
  const rest = text.slice(match.index + match[0].length);
  // More is said after the material ("... made from velvet with a gold cap"): leave it alone.
  if (rest.replace(/^[ .!?]+|[ .!?]+$/g, "")) return text;
  const trimmed = text.slice(0, match.index).replace(/[ ,;]+$/, "");
  return trimmed + (rest.trim() || ".");
}

/**
 * Indexes of later sentences that only repeat the name and the sentences before them.
 *
 * A sentence is a restatement when four out of five of its content words are
 * already in the item name or an earlier sentence (three out of five for a
 * sentence that starts This is / It is, the shape of a restated definition).
 * A sentence with a number in it is a size or a count and is never judged,
 * and never counts as an earlier sentence.
 */
export function restatedIndexes(sentences, name = "") {
  const seen = new Set(contentStems(name));
  const found = [];
  (sentences || []).forEach((sentence, index) => {
    if (/\d/.test(sentence)) return;
    const list = contentStems(sentence);
    if (index > 0 && list.length >= 2) {
      const known = list.filter((word) => seen.has(word)).length;
      const definition = /^\s*(?:this|it)\s+is\s+/i.test(sentence);
      if (known * 5 >= list.length * (definition ? 3 : 4)) {
        found.push(index);
        return;
      }
    }
    for (const word of list) seen.add(word);
  });
  return found;
}

/**
 * Drop sentences that admit a missing fact, say nothing, or only restate.
 *
 * isProtected(sentence) keeps a sentence untouched (condition and AS IS
 * wording). A sentence that must never be published (unpublishableSentence: a
 * missing fact reported, the record or the listing talked about, a title the
 * item does not have) is always dropped, whole. An empty line or a
 * restatement is dropped only when the text left over is not too thin by
 * isThin (a text that was already too thin may still lose one, but never its
 * last sentence). The first sentence is never dropped as a restatement.
 * Running this twice gives the same text.
 */
export function stripLowValueSentences(prose, name = "", isThin = null, isProtected = null) {
  const text = String(prose ?? "").replace(/\s{2,}/g, " ").trim();
  if (!text) return "";
  const thin = typeof isThin === "function" ? isThin : () => false;
  const guarded = typeof isProtected === "function" ? isProtected : () => false;
  const parts = [];
  for (const part of splitSentences(text)) {
    if (guarded(part)) {
      parts.push(part);
      continue;
    }
    if (unpublishableSentence(part, name)) continue;
    parts.push(/\d/.test(part) ? part : trimSelfMaterial(part));
  }
  if (!parts.length) return "";
  const wasThin = thin(parts.join(" "));
  const tryDrop = (index) => {
    const rest = [...parts.slice(0, index), ...parts.slice(index + 1)];
    if (!rest.length) return false;
    if (thin(rest.join(" ")) && !wasThin) return false;
    parts.splice(index, 1);
    return true;
  };
  let index = 0;
  while (index < parts.length) {
    const part = parts[index];
    if (!guarded(part) && !/\d/.test(part) && emptyLineSentence(part) && tryDrop(index)) continue;
    index += 1;
  }
  for (;;) {
    let dropped = false;
    for (const at of restatedIndexes(parts, name)) {
      if (guarded(parts[at])) continue;
      if (tryDrop(at)) {
        dropped = true;
        break;
      }
    }
    if (!dropped) break;
  }
  return parts.join(" ").replace(/\s{2,}/g, " ").trim();
}

// ---------------------------------------------------------------------------
// The size sentence for home goods
// ---------------------------------------------------------------------------

const ROUND_NAME_RE =
  /\b(?:ornaments?|baubles?|balls?|spheres?|orbs?|globes?|round|circular|bowls?|vases?|pots?|planters?|jars?|baskets?|plates?|platters?)\b/;
const ORNAMENT_RE = /\b(?:ornaments?|baubles?)\b/;
const CANDLESTICK_RE = /\b(?:candle\s?sticks?|candle\s?holders?|candelabras?|pillar\s+holders?)\b/;
const LAMP_RE = /\blamps?\b/;
const LIGHT_FIXTURE_RE = /\b(?:sconces?|chandeliers?|pendants?)\b/;
const STORAGE_RE = /\b(?:armoires?|wardrobes?|dressers?|cabinets?|bookcases?|sideboards?|buffets?)\b/;
const SEATING_RE = /\b(?:sofas?|loveseats?|sectionals?|armchairs?|recliners?|settees?|chaises?)\b/;
const PERCH_RE = /\b(?:stools?|bench|benches|ottomans?|poufs?)\b/;
const TABLE_RE = /\b(?:consoles?|tables?|desks?)\b/;
const BED_RE = /\b(?:beds?|nightstands?)\b/;
const SOFT_GOODS_RE =
  /\b(?:pillows?|cushions?|throws?|blankets?|quilts?|duvets?|bedding|curtains?|drapes?|towels?|runners?|tablecloths?|napkins?)\b/;
const PLAIN_SIZE_RE =
  /\b(?:books?|tvs?|televisions?|speakers?|refrigerators?|fridges?|coolers?|freezers?|washers?|dryers?|ovens?|ranges?|microwaves?|dishwashers?|grills?|appliances?|fans?|heaters?|bikes?|bicycles?|treadmills?|doors?|windows?|shutters?|fountains?|umbrellas?|boards?)\b/;
const FLOOR_DECOR_RE =
  /\b(?:floor|vases?|urns?|statues?|sculptures?|planters?|pots?|jars?|lanterns?|baskets?|globes?|figures?|totems?)\b/;

function same(a, b) {
  return a != null && b != null && Math.abs(a - b) < 1e-6;
}

/**
 * Size sentences that are true for this kind and size of home item.
 *
 * Returns { candidates, rotate }. The caller writes the first candidate that
 * does not repeat a phrase already in the description. rotate is true for
 * small decor, where the starting candidate is picked by the item name so
 * that near-identical items listed side by side do not all read the same.
 *
 * measure is the labeled measure the caller already built ("3 inches wide,
 * 3 inches deep, and 3.5 inches tall"). width, depth, height and weight are
 * numbers or null. Rugs, wall art and Luxury Goods never reach this function:
 * their wording is the caller's and is unchanged.
 */
export function homeSizeCandidates({
  name = "",
  kind = "",
  width = null,
  depth = null,
  height = null,
  widthText = "",
  heightText = "",
  measure = "",
  weight = null,
} = {}) {
  const blob = String(name ?? "").toLowerCase();
  const sort = String(kind ?? "");
  const sides = [width, depth, height].filter((side) => side != null);
  const longest = sides.length ? Math.max(...sides) : 0;
  const flat = [width, depth].filter((side) => side != null);
  const footprint = flat.length ? Math.max(...flat) : 0;
  const tallest = height != null && same(height, longest);
  const roundItem = ROUND_NAME_RE.test(blob) && same(width, depth);
  let across = measure;
  if (roundItem) {
    across = `about ${widthText} inches across`;
    if (height != null) across = `${across} and ${heightText} inches tall`;
  }
  const fixed = (candidates) => ({ candidates, rotate: false });
  const varied = (candidates) => ({ candidates, rotate: true });
  const plain = [`It measures ${measure}.`];

  // Soft goods and light fixtures: the size and nothing more.
  if (SOFT_GOODS_RE.test(blob) || LIGHT_FIXTURE_RE.test(blob)) return fixed(plain);
  if (LAMP_RE.test(blob)) {
    if (/\bfloor\b/.test(blob) && height != null && height >= 48) {
      return fixed([
        `It measures ${measure}, a floor lamp height that stands beside a sofa, chair or bed.`,
        `At ${measure}, it reaches reading height beside a chair or sofa.`,
      ]);
    }
    if (height != null && height <= 36 && !/\bfloor\b/.test(blob)) {
      return fixed([
        `It measures ${measure}, a tabletop scale for a nightstand, end table or desk.`,
        `At ${measure}, it sits comfortably on a nightstand, console or desk.`,
      ]);
    }
    return fixed(plain);
  }

  // Hanging ornaments
  if (ORNAMENT_RE.test(blob)) {
    if (longest && longest <= 4.5) {
      return varied([
        `It measures ${across}, a size that hangs easily on a tree branch, wreath or garland.`,
        `At ${across}, it is a standard ornament size for a tree, wreath or garland.`,
        `It measures ${across}, small enough to hang on a tree, tuck into a wreath or tie onto a gift.`,
      ]);
    }
    if (longest && longest <= 8) {
      return varied([
        `It measures ${across}, an oversized ornament scale that stands out on a tree, wreath or mantel.`,
        `At ${across}, it is larger than a standard ornament and reads clearly on a full-size tree.`,
        `It measures ${across}, a generous size for a large tree, a wreath or a tabletop holiday display.`,
      ]);
    }
    return fixed([`It measures ${across}.`]);
  }

  // Candlesticks
  if (CANDLESTICK_RE.test(blob) && height != null) {
    let stands = `${heightText} inches tall`;
    if (width != null && (depth == null || same(width, depth))) {
      stands = `${stands} and ${widthText} inches wide`;
    } else if (width != null) {
      stands = measure;
    }
    if (height < 18) {
      return varied([
        `It stands ${stands}, a tabletop height for a mantel, shelf or dining table.`,
        `At ${stands}, it suits a mantel, sideboard or dining table centerpiece.`,
      ]);
    }
    if (height < 28) {
      return varied([
        `It stands ${stands}, tall enough to anchor a console, buffet or fireplace hearth.`,
        `At ${stands}, it has the height to stand out on a console, hearth or large dining table.`,
      ]);
    }
    if (height < 36) {
      return varied([
        `It stands ${stands}, a floor-standing height for a hearth, entry or stair landing.`,
        `At ${stands}, it is tall enough to stand on the floor beside a fireplace or doorway.`,
      ]);
    }
    if (height < 44) {
      return varied([
        `It stands ${stands}, about waist high as a floor candlestick beside a fireplace or in an entry.`,
        `At ${stands}, it works as a floor piece that rises to about waist height.`,
      ]);
    }
    return varied([
      `It stands ${stands}, a tall floor candlestick with real presence beside a fireplace, doorway or staircase.`,
      `At ${stands}, it is a floor-standing piece for an entry, hearth or stair landing.`,
    ]);
  }

  // Storage furniture
  if (STORAGE_RE.test(blob)) {
    let first;
    if (/\b(?:sideboards?|buffets?)\b/.test(blob)) {
      first = `It measures ${measure}, so it works along a dining room wall or in an entry.`;
    } else if (/\bbookcases?\b/.test(blob)) {
      first = `It measures ${measure}, so it works along an office, living room or hallway wall.`;
    } else {
      first = `It measures ${measure}, so it works along a bedroom wall or hallway.`;
    }
    const third =
      height == null || height >= 60
        ? `With a footprint of ${measure}, it reads as tall storage rather than a bulky box.`
        : `With a footprint of ${measure}, it stores plenty while staying below eye level.`;
    return fixed([
      first,
      `It measures ${measure}, giving it a real storage footprint without crowding the room.`,
      third,
    ]);
  }

  // Seating
  if (sort === "sofa" || sort === "chair" || SEATING_RE.test(blob)) {
    let first;
    if (width != null && width >= 84) {
      first = `It measures ${measure}, so it suits a full-size living room or family room seating area.`;
    } else if (width != null && width >= 60) {
      first = `It measures ${measure}, so it fits a living room, den or office seating area.`;
    } else {
      first = `It measures ${measure}, so it fits a living room or seating nook cleanly.`;
    }
    return fixed([first, `At ${measure}, it has a comfortable everyday seating scale.`]);
  }
  if (PERCH_RE.test(blob)) {
    const first =
      footprint >= 36
        ? `It measures ${measure}, so it fits an entry, a hallway or the foot of a bed.`
        : `It measures ${measure}, a compact scale that tucks in wherever extra seating is needed.`;
    return fixed([first, `At ${measure}, it has a comfortable everyday seating scale.`]);
  }

  // Tables and desks
  if (sort === "table" || sort === "desk" || TABLE_RE.test(blob)) {
    const second = `At ${measure}, the top has a practical everyday footprint.`;
    if (sort === "desk" || /\b(?:desks?|dining|kitchen|conference)\b/.test(blob)) {
      return fixed([
        `It measures ${measure}, so it works for dining or desk use without feeling oversized.`,
        second,
      ]);
    }
    if (/\b(?:consoles?|sofa\s+tables?|entry|hall)\b/.test(blob)) {
      return fixed([`It measures ${measure}, so it fits an entry, a hallway or the wall behind a sofa.`, second]);
    }
    if (/\b(?:coffee|cocktail)\b/.test(blob)) {
      return fixed([
        `It measures ${measure}, a low profile that sits in front of a sofa or between chairs.`,
        second,
      ]);
    }
    if (/\b(?:pedestals?|plant\s+stands?)\b/.test(blob)) {
      const candidates = [
        `It measures ${measure}, a slim footprint for a corner, an entry or the end of a hallway.`,
      ];
      if (height != null && height >= 28) {
        candidates.push(`At ${measure}, it raises a plant, lamp or sculpture to display height.`);
      }
      return fixed(candidates);
    }
    if (
      /\b(?:side|end|accent|occasional|nesting|bedside|drink|lamp)\b/.test(blob) ||
      (footprint && footprint < 30)
    ) {
      return fixed([`It measures ${measure}, so it tucks beside a sofa, chair or bed.`, second]);
    }
    return fixed([
      `It measures ${measure}, so it works for dining or desk use without feeling oversized.`,
      second,
    ]);
  }

  // Beds and nightstands
  if (sort === "bed" || sort === "nightstand" || BED_RE.test(blob)) {
    if (
      (sort === "nightstand" || /\bnightstands?\b/.test(blob)) &&
      height != null &&
      height >= 18 &&
      height <= 34
    ) {
      return fixed([
        `It measures ${measure}, so it sits at bedside height next to a primary or guest bed.`,
        `At ${measure}, it keeps a low bedroom footprint.`,
      ]);
    }
    return fixed([
      `It measures ${measure}, so it suits a primary or guest bedroom layout.`,
      `At ${measure}, it keeps a low bedroom footprint.`,
    ]);
  }

  // Everything else: decor and accessories by scale. Large or heavy items,
  // soft goods and appliances state their size and nothing more.
  if (!longest || PLAIN_SIZE_RE.test(blob) || (weight != null && weight > 50)) return fixed(plain);
  if (longest <= 8) {
    return varied([
      `It measures ${across}, a compact size for a shelf, desk or bookcase.`,
      `At ${across}, it tucks easily onto a shelf, side table or windowsill.`,
      `It measures ${across}, small enough to group with other pieces on a shelf or tabletop.`,
    ]);
  }
  if (longest <= 14) {
    return varied([
      `It measures ${across}, a comfortable size for a shelf, console or tabletop.`,
      `At ${across}, it sits well on a side table, bookshelf or entry table.`,
      `It measures ${across}, sized to display on a console, sideboard or coffee table.`,
    ]);
  }
  if (longest <= 24) {
    if (tallest) {
      return varied([
        `It measures ${across}, tall enough to stand out on a console, sideboard or hearth.`,
        `At ${across}, it has the height for a console, entry table or hearth display.`,
        `It measures ${across}, a size that holds its own on a sideboard, entry table or hearth.`,
      ]);
    }
    return varied([
      `It measures ${across}, substantial enough for a console, sideboard or dining table centerpiece.`,
      `At ${across}, it has enough presence for a console, hearth or large coffee table.`,
      `It measures ${across}, a size that holds its own on a sideboard, entry table or hearth.`,
    ]);
  }
  if (longest <= 40 && tallest && FLOOR_DECOR_RE.test(blob)) {
    return varied([
      `It measures ${across}, tall enough to stand on the floor beside a console, fireplace or doorway.`,
      `At ${across}, it works as a floor piece in an entry, a corner or beside a hearth.`,
    ]);
  }
  return fixed([`It measures ${across}.`]);
}

function nameNumber(name) {
  let total = 0;
  for (const char of String(name ?? "").toLowerCase()) {
    if (/^[a-z0-9]$/.test(char)) total += char.charCodeAt(0);
  }
  return total;
}

/**
 * The first candidate that does not repeat the description (repeats(sentence) is false).
 *
 * With rotate, the search starts at a candidate chosen by the item name. When
 * every candidate repeats, furniture keeps its last one (as before) and small
 * decor keeps the one its name chose.
 */
export function pickSizeSentence(candidates, rotate, name, repeats) {
  if (!candidates || !candidates.length) return "";
  let ordered = [...candidates];
  if (rotate) {
    const start = nameNumber(name) % ordered.length;
    ordered = [...ordered.slice(start), ...ordered.slice(0, start)];
  }
  for (const sentence of ordered) {
    if (!repeats(sentence)) return sentence;
  }
  return rotate ? ordered[0] : ordered[ordered.length - 1];
}
