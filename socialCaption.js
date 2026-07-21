/**
 * POST /api/social-caption — Static IG/FB captions for Meta Business Suite extension.
 * Solo posts: listing text. Set posts (items[]): listing text + optional photos (URLs/base64) for vision.
 * Tuned to match Lost + Found's existing social voice: emojis, location tags, CTAs.
 */

const FRAME_INSTRUCTIONS = {
  pov_driven: `FRAME MODE: POV DRIVEN
- First-person resale operator voice
- Clear stance on why people misunderstand value
- Confident, insightful, never arrogant`,
  educational: `FRAME MODE: EDUCATIONAL
- Teach one clear, actionable idea
- Smart but uninformed reader
- Practical takeaway at the end`,
  resale_philosophy: `FRAME MODE: RESALE PHILOSOPHY
- Beliefs and principles behind resale
- Reflective, grounded, memorable
- No hard sell`,
  product_focused: `FRAME MODE: PRODUCT FOCUSED
- Show the product working without overselling
- Practical, outcome-oriented
- Materials, construction, design intent`,
  quiet_luxury: `FRAME MODE: QUIET LUXURY
- Restraint, precision, short sentences
- Signal quality without explaining everything`,
  algorithm_busting: `FRAME MODE: ALGORITHM BUSTING
- Contrarian or curiosity-driven hook
- Punchy, direct, then prove the point`,
  conversation_starter: `FRAME MODE: CONVERSATION STARTER
- Thoughtful open-ended question
- Curious, not bait`,
};

const INTENT_INSTRUCTIONS = {
  teaching_taste: `OBJECTIVE: TEACHING TASTE
- Shape how the audience evaluates quality and value
- Teach recognition of value beyond brand or retail price
- Quiet guidance, not a lecture`,
  building_desire: `OBJECTIVE: BUILDING DESIRE
- Make the item feel wanted
- Scarcity, condition, demand, long-term appeal — no urgency pressure`,
  selling_softly: `OBJECTIVE: SELLING SOFTLY
- Nudge without pushing; sell implied, not stated
- Lead toward trusting the price/process — no "buy now"`,
  driving_traffic: `OBJECTIVE: DRIVING STORE TRAFFIC
- Create a reason to click or visit
- Tease details best seen in-store or on the site`,
  encouraging_comments: `OBJECTIVE: ENCOURAGING COMMENTS
- Spark thoughtful engagement
- End with an open-ended question (not yes/no)`,
  reinforcing_brand: `OBJECTIVE: REINFORCING BRAND IDENTITY
- Sound unmistakably Lost + Found: data over hype, clarity over guesswork
- Do not explain the brand aloud — let voice signal it`,
  historical_fact: `OBJECTIVE: HISTORICAL POST (Did you know?)
- Start with a real historical fact about the item/brand/era
- Then connect that history to modern resale value and demand`,
  consign_with_us: `OBJECTIVE: CONSIGN WITH US
- About the consignment process — NOT selling this specific SKU
- No hard-selling this item's price; invite people to consign
- Conversational, welcoming, process-focused`,
};

const LOCATION_EXAMPLES_FURNITURE = `LOCATION LINE EXAMPLES (pick ONE and vary each caption — do NOT reuse the same one back-to-back, NEVER default to "Sitting pretty in Scottsdale"):
📍 Located in Scottsdale, AZ near Scottsdale Quarter
📍 Scottsdale pickup · Lost + Found Resale Interiors
📍 Scottsdale, Arizona · shop the floor or shop this post
📍 On our Scottsdale floor right now
📍 In-store in Scottsdale · online for everyone else
📍 Scottsdale showroom find · we ship everywhere
📍 Come touch it in Scottsdale, or grab it right from this post
📍 Local? Come see it. Not local? We ship everywhere.
NEVER: "inside @lostandfoundresale" | NEVER: art scene | NEVER: "Ships from Scottsdale" as the location line | NEVER: "we ship most pieces"`;

/**
 * True shipping/fulfillment facts from lostandfoundresale.com product pages.
 * The model may pull ONE natural line from these — never the whole list.
 */
const FULFILLMENT_FACTS = `FULFILLMENT FACTS (true — pick AT MOST ONE angle per caption, reword it naturally, or skip entirely):
- We ship everywhere (parcel + freight) — NEVER say "most pieces" or "most items"
- Parcel-size items: standard shipping shows automatically at checkout
- Big / bulky / oversized pieces: freight preparation + LTL freight or third-party carriers — still ship everywhere
- Local delivery: $95/hr flat rate — same rate no matter the size or number of items (two movers + large box truck via trusted third-party providers)
- Self pickup in Scottsdale always welcome
- We help coordinate freight quotes (FreightCenter etc.) for out-of-town buyers
- All items sold as-is · all sales final
Example one-liners (vary, never copy verbatim every post):
"We ship everywhere."
"Small enough to ship — checkout does the math."
"Yes, we can freight this — we ship everywhere."
"Local delivery $95/hr — same rate no matter size or how many pieces."
"Pickup in Scottsdale or we ship it everywhere."
NEVER: "we ship most pieces" | NEVER: paste policy paragraphs, storage fees, 72-hour windows, liftgate details, or "see Delivery, Pickup & Freight Options"`;

/**
 * Style variety pool — one is picked at random per request so posts don't all sound the same.
 * Extra important when frame/intent are "let the engine decide".
 */
const CAPTION_STYLES = [
  {
    id: "classic_house",
    text: `STYLE FOR THIS POST: CLASSIC HOUSE
- The standard Lost + Found layout done clean: punchy tagline, tight features, hungry sell body
- Polished but warm`,
  },
  {
    id: "natural_conversational",
    text: `STYLE FOR THIS POST: NATURAL / CONVERSATIONAL
- Write like a real person talking to a friend who loves interiors
- Looser sentences, contractions, one aside in parentheses allowed
- Fewer emojis (keep 💰 📐 📍), tagline can read like a text message
- Still hits every mandatory layout section`,
  },
  {
    id: "minimal_organized",
    text: `STYLE FOR THIS POST: MINIMAL / ORGANIZED
- Short. Clean. Almost catalog-like.
- Tagline is 3-6 words. Features are 3 crisp lines. Body is 2-3 tight sentences max.
- Zero filler words`,
  },
  {
    id: "funky_playful",
    text: `STYLE FOR THIS POST: FUNKY / PLAYFUL
- Fun hook, personality, a little cheeky (never cringe, never all-caps spam)
- Unexpected but fitting emojis, playful phrasing like "this one's a mood"
- Body still sells hard with real facts`,
  },
  {
    id: "cool_editorial",
    text: `STYLE FOR THIS POST: COOL / EDITORIAL
- Reads like a design magazine caption: confident, visual, a little cinematic
- Lead with the room scene this piece creates
- Restrained emojis, elevated vocabulary without being pretentious`,
  },
  {
    id: "storyteller",
    text: `STYLE FOR THIS POST: STORYTELLER
- Open with a tiny scene or moment (morning light, dinner party, reading nook)
- Then land the facts: price, dims, features
- Warm, sensory, human`,
  },
  {
    id: "bold_hype",
    text: `STYLE FOR THIS POST: BOLD / HYPE (tasteful)
- Big energy hook, ALL CAPS allowed for 2-4 words max
- Fast rhythm, short punches, confident close
- Hype the piece, never fake urgency`,
  },
  {
    id: "expert_curator",
    text: `STYLE FOR THIS POST: EXPERT CURATOR
- Speak as the eye that found this piece: why it made the floor
- Point out one detail most people would miss
- Quietly authoritative, teaches taste`,
  },
];

function pickCaptionStyle() {
  return CAPTION_STYLES[Math.floor(Math.random() * CAPTION_STYLES.length)];
}

function inferSocialItemType(text) {
  const t = String(text || "").toLowerCase();
  if (
    /\b(earrings?|ear clips?|necklace|pendant|brooch|bracelet|bangle|ring|locket|cufflinks?|anklet|choker|cabochon|sterling silver|925 silver|14k|18k|gemstone|aventurine|carnelian|turquoise)\b/.test(
      t
    )
  ) {
    return "jewelry";
  }
  if (
    /\b(handbag|purse|tote|crossbody|satchel|clutch|shoulder bag|wallet|hobo|bucket bag|top handle|minaudiere|pochette|backpack|briefcase|duffle|duffel|classic flap)\b/.test(
      t
    )
  ) {
    return "handbag";
  }
  return "other";
}

function buildItemAccuracyRules(itemType, sourceText) {
  const sourceHasSeason = /\b(spring|summer|autumn|fall|winter)\b/i.test(sourceText);
  return `ITEM ACCURACY OVERRIDES (higher priority than the selected style):
- Detected product type: ${itemType}
- The hook is the highest-priority sales line. Make it a KILLER 4-10 word hook: specific, confident, memorable, and rooted in one distinguishing item fact (material, stone, color, form, maker, scale, or design). Pair that fact with desire or impact. No generic "golden hour", "nature's elegance", "small scale, big impact", changing-seasons, morning-routine, or random lifestyle opening.
- ${
    sourceHasSeason
      ? "Seasonal language is allowed only because the listing itself contains a season."
      : "The listing contains no season. NEVER mention spring, summer, autumn, fall, winter, or imply a current season."
  }
- ${
    itemType === "jewelry"
      ? "This is JEWELRY. Use a jewelry-appropriate emoji such as 💎, ✨, 📿, or the product itself. NEVER use 👜, 👛, 💼, handbag, purse, tote, bag, or handbag-specific language."
      : "Use only an emoji and product language matching the detected item."
  }
- ${
    itemType === "jewelry" || itemType === "handbag"
      ? "NEVER include a dimensions line or measurements. Sell what it is: brand/maker, material, stones, color, silhouette, construction, and distinctive design."
      : "Include dimensions only when they help a buyer understand this item."
  }
- If a style instruction conflicts with these accuracy rules, these rules win.`;
}

function buildFallbackHook(itemType, sourceText) {
  const t = String(sourceText || "").toLowerCase();
  if (itemType === "jewelry") {
    if (/\b(orchid|dogwood|flower|floral|leaf)\b/.test(t)) {
      return "🌸 Golden botanicals with statement-making power.";
    }
    if (/\bsigned\b/.test(t)) {
      return "💎 Signed detail with collector-level presence.";
    }
    if (/\b(14k|18k|gold)\b/.test(t)) {
      return "✨ Real gold, impossible-to-ignore detail.";
    }
    if (/\b(turquoise|aventurine|carnelian|aquamarine|amber|opal|gemstone)\b/.test(t)) {
      return "💎 Natural color that refuses to whisper.";
    }
    if (/\b(earrings?|ear clips?)\b/.test(t)) {
      return "✨ Face-framing detail with instant impact.";
    }
    return "💎 Jewelry with undeniable point of view.";
  }
  if (/\b(art|artwork|painting|print|canvas)\b/.test(t)) {
    return "🖼️ The wall just found its focal point.";
  }
  return "✨ The detail that transforms the whole room.";
}

function enforceItemAccurateOpening(caption, itemType, sourceText) {
  let lines = String(caption || "").split(/\r?\n/);
  if (itemType === "jewelry" || itemType === "handbag") {
    lines = lines.filter(
      (line) =>
        !/^\s*📐/.test(line) &&
        !/^\s*(?:dimensions?|measurements?|size)\s*:/i.test(line) &&
        !/\b\d+(?:\.\d+)?\s*(?:"|in\b|inches?)\s*[x×]\s*\d/i.test(line)
    );
  }
  const originalFirstIndex = lines.findIndex((line) => line.trim());
  const originalFirst = originalFirstIndex >= 0 ? lines[originalFirstIndex] : "";
  if (itemType === "jewelry") {
    lines = lines.map((line) => line.replace(/[👜👛💼]/gu, "💎"));
  }

  const sourceHasSeason = /\b(spring|summer|autumn|fall|winter)\b/i.test(sourceText);
  const firstIndex = lines.findIndex((line) => line.trim());
  if (firstIndex < 0) return lines.join("\n");

  const first = lines[firstIndex];
  const wrongType =
    itemType === "jewelry" &&
    /\b(handbag|purse|tote|satchel|crossbody|bag)\b|[👜👛💼]/i.test(originalFirst || first);
  const inventedSeason =
    !sourceHasSeason && /\b(spring|summer|autumn|fall|winter)\b/i.test(first);
  const genericJewelryHook =
    itemType === "jewelry" &&
    /\b(golden hour|nature'?s elegance|all that'?s needed|changing seasons?|small scale)\b/i.test(first);

  if (wrongType || inventedSeason || genericJewelryHook) {
    lines[firstIndex] = buildFallbackHook(itemType, sourceText);
  }
  return lines.join("\n");
}

const LOCATION_EXAMPLES_LUXURY = `LOCATION LINE EXAMPLES (ALWAYS include @lostandfoundresale — vary phrasing):
📍 @lostandfoundresale · Scottsdale, AZ
📍 Available @lostandfoundresale in Scottsdale
📍 Scottsdale luxury resale · @lostandfoundresale
NEVER: Ships from Scottsdale | NEVER use lostandfoundresale.com as the shop URL (handbags site only)`;

const BASE_PROMPTS = {
  resale_interiors: `You are Lost + Found Resale Interiors' best static-post writer. You crush Facebook + Instagram captions the way our brand already sounds: sharp hooks, sparse but punchy emojis, real listing facts, clean layout, zero fluff SEO spam.

BRAND VOICE:
- Teach taste. Resale is intentional. Materials, scale, form, space.
- Strong hooks with CREATIVE emojis (not a wall of 🔥🔥🔥)
- Vary capitalization across posts (Title Case / Sentence case / ALL CAPS hooks)
- Educational, not lecturing. Confident, never try-hard.

ITEM EMOJIS (pick the one that matches the piece - NEVER 👜):
lamp💡 · chair🪑 · sofa/sectional🛋️ · table/desk🍽️ · dresser/cabinet🗄️ · art/painting🖼️ · rug🧶 · mirror🪞 · vase/pot🏺 · clock⏰ · lighting ✨ · stool/bench🪑 · bookshelf📚 · outdoor🪴

MANDATORY CAPTION LAYOUT (blank line between EVERY section - NO hashtags anywhere in caption):
1) TAGLINE / HOOK - short punchy sell line with 1-3 emojis (example vibe: "💡 ✨ Illuminate Your Space with Sophistication") - make THIS item feel desirable
2) PRODUCT NAME LINE - full listing title + matching item emoji (pendant/candle lighting → 🕯️ or 💡, NEVER 👜 for furniture)
3) PRICE - 💰 $XX.XX exact from listing (include $) - place RIGHT AFTER the title, ABOVE dimensions
4) DIMENSIONS - only if in listing: 📐 W" x D" x H" (or W" x H" if depth missing). Never write "inches"
5) FEATURES - either:
   - one flowing line: ✨ feature, ✨ feature, ✨ feature
   - OR 2-5 short lines each starting with ✨
   Pull from description. Sell materials, finish, style, AS IS honesty.
6) BODY - 1 hungry sales paragraph that markets the hell out of it: atmosphere, room impact, why someone should want it NOW - still honest, no fake urgency spam
7) Perfect for: - 2-4 bullets using "-" (not •)
8) LOCATION 📍 - one strong Scottsdale line. Vary wording. Examples:
${LOCATION_EXAMPLES_FURNITURE}
9) CTA:
   PRIMARY (vary): 👉 Shop the feed · Tap to shop · Shop right here · Shop this post
   Then: For store details, consigning with us, our policies & more: lostandfoundresale.com

HASHTAGS:
- Put the 5 hashtags ONLY in the JSON "hashtags" array
- NEVER put #hashtags in the caption string (not top, not bottom, not middle)

QUALITY BAR - MAKE IT KILLER:
- A stranger scrolling should STOP and want this piece
- Specific > generic: materials, finish, scale, room impact from THIS listing only
- Tagline must be unique to this SKU - if it could sell a random vase, rewrite it
- Body sells atmosphere + why it belongs in someone's home NOW, without fake countdown urgency
- AS IS stays honest and framed as character / history when true
- End with a clear easy next step (shop / come see it / we ship everywhere)
- Exactly 5 sharp, specific hashtags in the array only (not #HomeDecor alone if you can be more specific)

HARD BANS:
- Hashtags inside "caption"
- Markdown (** __ * [links](url))
- Em dashes / en dashes / long dashes (— – ―) - ALWAYS use a normal hyphen (-) or a comma instead. Zero exceptions.
- Price below features or below dimensions (price must be above dimensions)
- "Ships from Scottsdale"
- "inside @lostandfoundresale"
- "Sitting pretty in Scottsdale" as a default / go-to line
- "we ship most pieces" / "shipping available on most" - say we ship everywhere
- Generic hooks that could fit any product
- "Please see … below the description" / Delivery-Pickup-Freight Options pointers
- Long shipping policy paragraphs
- Output JSON only`,

  luxury_handbags: `You are Lost + Found Luxury Handbags & Accessories' best static-post writer. Crush Facebook + Instagram captions: calm confidence, authenticity without shouting, brand as signal not ad. Sell hard without sounding desperate.

BRAND VOICE:
- Luxury as circulation. Intentional. Clean.
- Creative but restrained emojis
- Philosophy and education over hard sell

ITEM EMOJIS:
handbag👜 · scarf🧣 · shoes👠 · wallet💳 · watch⌚ · jewelry📿 · men's bag💼 · belt🪢 · sunglasses🕶️

MANDATORY CAPTION LAYOUT (blank line between EVERY section - NO hashtags in caption):
1) TAGLINE / HOOK - short desire-forward line with emojis
2) PRODUCT NAME LINE - title + item emoji
3) PRICE 💰 $XX.XX - exact listing price (never "Price Upon Request" if a price exists)
4) FEATURES - ✔ or 🤍/💜 lines (or one ✨ comma line) from description
5) BODY - sell the lifestyle / why this piece, without repeating the full title
6) LOCATION 📍 - ALWAYS @lostandfoundresale (${LOCATION_EXAMPLES_LUXURY})
7) CTA - 👉 Shop the feed (or similar) then lostandfoundhandbags.com (NEVER lostandfoundresale.com for shop link)

HASHTAGS: exactly 5 in JSON "hashtags" array ONLY - never inside caption.

QUALITY BAR:
- Premium, human, persuasive
- No invented authenticity claims
- JSON only

HARD BANS:
- Hashtags inside "caption"
- Dimensions unless listing has them or user asked
- Markdown; em/en/long dashes (— – ―) - use hyphen (-) or comma only
- "Ships from Scottsdale"
- Generic influencer fluff
- "Please see … below the description" / Delivery-Pickup-Freight Options pointers
- Long shipping policy paragraphs`,
};

function getFrameBlock(frame) {
  const key = String(frame || "").trim();
  if (key && FRAME_INSTRUCTIONS[key]) return FRAME_INSTRUCTIONS[key];
  return `FRAME MODE: Let the engine decide (Data-Led)
- Confident, evidence-backed product storytelling from listing facts
- COMMIT fully to the STYLE FOR THIS POST block below — that's your personality this post
- Vary hooks, location lines, CTAs, and rhythm from post to post while keeping the mandatory layout sections`;
}

function getIntentBlock(intent) {
  const key = String(intent || "").trim();
  if (key && INTENT_INSTRUCTIONS[key]) return INTENT_INSTRUCTIONS[key];
  return `OBJECTIVE: Let the engine decide (Data-Led)
- Demonstrate authority without hard persuasion
- Let the listing dictate educate vs desire-build vs soft-sell
- Confident, final, evidence-backed`;
}

function getBasePrompt(division) {
  return BASE_PROMPTS[division] || BASE_PROMPTS.resale_interiors;
}

function formatHashtags(hashtags) {
  if (Array.isArray(hashtags)) {
    return hashtags
      .map((h) => String(h || "").trim())
      .filter(Boolean)
      .map((h) => (h.startsWith("#") ? h : `#${h}`))
      .join(" ");
  }
  return String(hashtags || "").trim();
}

/** Never return empty — fill to 5 tags from defaults when the model forgets. */
function ensureHashtags(hashtags, listing = {}, division = "resale_interiors") {
  const collected = [];
  const take = (tag) => {
    let t = String(tag || "").trim();
    if (!t) return;
    if (!t.startsWith("#")) t = `#${t}`;
    t = t.replace(/[^#A-Za-z0-9_]/g, "");
    if (!/^#[A-Za-z0-9_]+$/.test(t)) return;
    if (!collected.some((x) => x.toLowerCase() === t.toLowerCase())) collected.push(t);
  };

  String(formatHashtags(hashtags) || "")
    .replace(/,/g, " ")
    .split(/\s+/)
    .forEach((t) => {
      if (t.includes("#")) (t.match(/#[A-Za-z0-9_]+/g) || []).forEach(take);
      else if (/^[A-Za-z0-9_]+$/.test(t)) take(t);
    });

  const fallbacks =
    division === "luxury_handbags"
      ? ["#LuxuryResale", "#DesignerFinds", "#ScottsdaleLuxury", "#Consignment", "#LostAndFound"]
      : ["#ScottsdaleInteriors", "#ResaleFinds", "#LostAndFound", "#ShopLocalAZ", "#VintageModern"];

  String(listing.title || listing.vertical || "")
    .replace(/[^A-Za-z0-9\s]/g, " ")
    .split(/\s+/)
    .filter((w) => w.length >= 4)
    .slice(0, 3)
    .forEach((w) => take(`#${w}`));

  fallbacks.forEach(take);
  return collected.slice(0, 5).join(" ");
}

/** Strip any #tags the model wrongly put inside the caption body (top, bottom, or mid). */
function stripHashtagsFromCaption(caption) {
  let t = String(caption || "");
  // Remove every #tag token — hashtags belong only in the JSON array / appended once at end.
  t = t.replace(/#[A-Za-z0-9_]+/g, " ");
  // Drop lines that are now empty / whitespace-only after tag removal
  t = t
    .split(/\r?\n/)
    .map((line) => line.replace(/[ \t]{2,}/g, " ").trimEnd())
    .filter((line, i, arr) => line.trim() || (i > 0 && i < arr.length - 1 && arr[i - 1].trim()))
    .join("\n");
  return t.replace(/[ \t]+\n/g, "\n").replace(/\n{3,}/g, "\n\n").trim();
}

/** Never ship em/en/long dashes in captions - replace with hyphen. */
function stripEmDashes(text) {
  return String(text || "")
    .replace(/[\u2014\u2013\u2015\u2212\uFE58\uFE63\uFF0D]/g, "-") // em, en, horizontal, minus, small/fullwidth
    .replace(/\s+-\s+/g, " - ");
}

/**
 * Ensure 💰 price line sits above 📐 dimensions when both exist.
 * Model sometimes still puts price under features.
 */
function ensurePriceAboveDimensions(caption) {
  const parts = String(caption || "").split(/\n/);
  let priceIdx = -1;
  let dimIdx = -1;
  for (let i = 0; i < parts.length; i++) {
    const line = parts[i].trim();
    if (priceIdx < 0 && /^💰/.test(line)) priceIdx = i;
    if (dimIdx < 0 && /^📐/.test(line)) dimIdx = i;
  }
  if (priceIdx < 0 || dimIdx < 0 || priceIdx < dimIdx) return String(caption || "");

  const priceLine = parts[priceIdx];
  // Remove price line (and a blank line immediately after it if present)
  parts.splice(priceIdx, 1);
  if (parts[priceIdx] !== undefined && parts[priceIdx].trim() === "") parts.splice(priceIdx, 1);
  // Recompute dim index after splice
  dimIdx = parts.findIndex((l) => /^\s*📐/.test(l));
  if (dimIdx < 0) return parts.join("\n");
  // Insert price (with blank line) immediately before dimensions
  parts.splice(dimIdx, 0, priceLine, "");
  return parts.join("\n").replace(/\n{3,}/g, "\n\n").trim();
}

/**
 * Strip website shipping boilerplate ("please see Delivery… below the description").
 * Do NOT inject a stock nationwide-shipping line — captions should vary and
 * center the product story, not logistics.
 */
function sanitizeListingTextForCaption(text) {
  let t = String(text || "");
  if (!t.trim()) return "";

  // Drop "please see … below / Delivery, Pickup & Freight Options …" style pointers
  t = t.replace(
    /\s*Please see[^.!?\n]*(?:Delivery|Pickup|Freight|shipping|below)[^.!?\n]*[.!]?\s*/gi,
    " "
  );
  t = t.replace(
    /\s*[^.!?\n]*\bbelow the description\b[^.!?\n]*[.!]?\s*/gi,
    " "
  );
  t = t.replace(
    /\s*[^.!?\n]*Delivery,\s*Pickup\s*(?:&|and)\s*Freight Options[^.!?\n]*[.!]?\s*/gi,
    " "
  );
  // Remove stock logistics paragraphs from site copy — model may lightly nod if natural
  t = t.replace(
    /\s*Pickup is available in Scottsdale[^.!?\n]*[.!]?\s*/gi,
    " "
  );
  t = t.replace(
    /\s*we offer both local and nationwide delivery[^.!?\n]*[.!]?\s*/gi,
    " "
  );
  t = t.replace(
    /\s*[^.!?\n]*\bnationwide (?:shipping|delivery)\b[^.!?\n]*[.!]?\s*/gi,
    " "
  );
  t = t.replace(/\s{2,}/g, " ").trim();
  return t;
}

const SHIPPING_CAPTION_RULES = `SHIPPING / PICKUP (OPTIONAL — DO NOT REPEAT THE SAME LINE EVERY POST):
- The HEART of every caption is what the ITEM is: materials, look, scale, style, room, story from the listing description
- Shipping/pickup is secondary. Some posts should skip logistics entirely
- When you DO mention it, use the FULFILLMENT FACTS below — one short natural phrase max, different each post
- Match the fact to the item: parcel-size → auto shipping at checkout; big/bulky → freight / we ship everywhere; local → pickup or local delivery $95/hr (same rate no matter size or item count)
${FULFILLMENT_FACTS}`;

const PRODUCT_FOCUS_RULES = `PRODUCT-FIRST RULE (MOST IMPORTANT):
- Build the killer caption from the MAIN product explanation in the listing — what it is, how it looks/feels, why it belongs in a room
- Lead with uniqueness: maker/style cues, texture, color, form, era, AS IS honesty when present
- Features ✨ and body paragraphs must come from that core description, not from shipping or store policy text
- Price and dimensions support the story; they are not the story
- Every caption should feel written for THIS SKU only`;

const SET_CAPTION_RULES = `SET / MULTI-ITEM POST (CRITICAL):
You are writing ONE caption for a curated GROUP of products in a single Meta post (carousel / multi-photo).

COUNT LANGUAGE (HARD RULE):
- SET SIZE is given in PRODUCT CONTEXT — match it exactly in the hook
- 2 items → pair / duo / these two (NEVER trio, quartet, "set of 4")
- 3 items → trio / these three (NEVER pair, duo, quartet)
- 4 items → quartet / set of four / these four (NEVER trio)
- 5+ items → collection / set of N / these N pieces (NEVER trio unless N is 3)
- Wrong count words make the caption look broken — double-check before you write the tagline

VOICE:
- Speak to the collection as a whole first — how the pieces work together, the vibe, the room story
- Then list each piece cleanly so shoppers can find every tagged item
- Mixed categories (glasses + bowls + accessories, lamps + tables, etc.) must FLOW — find the shared taste thread (material, era, color, use, hostess moment, tablescape) instead of a random dump
- Prefer language like "a set that…" / "these pieces…" / "pairing…" — never sound like 3 separate posts glued together

MANDATORY LAYOUT (blank line between sections — NO hashtags in caption):
1) TAGLINE / HOOK — one scroll-stopping line about the SET vibe (count-accurate)
2) SHORT INTRO — 1-2 sentences on why these belong together
3) ITEM ROSTER — one compact line per item, in order. Format:
   ✨ Item full title - 💰 $price - 📐 dims (omit 📐 if none; omit 💰 if no price)
   Keep each roster line to ONE line
4) SHARED BODY — 1 short paragraph selling the group without repeating every title
5) Perfect for: — 2-4 bullets with "-"
6) LOCATION 📍 — one Scottsdale line (vary wording)
7) CTA / CONVERSATION — ONLY at the very end (after Location). Keep it clean. Never mid-caption.

MULTI-PHOTO CTA (INSTAGRAM BUG — HARD BAN):
- Instagram does NOT reliably show a "shop this post" banner on multi-image carousels
- NEVER say: "Shop this post" / "shop this post" / "Tap to shop this post" / "Shop the post"
- Instead rotate soft CTAs like:
  - 👉 Shop this store right here
  - 👉 Visit us online at lostandfoundresale.com (or lostandfoundhandbags.com for luxury)
  - 👉 Visit us in store in Scottsdale
  - 👉 Come see these pieces on the floor
- Then one line: For store details, consigning with us, our policies & more: [correct site URL]
- Furniture/resale → lostandfoundresale.com | Luxury handbags → lostandfoundhandbags.com

RULES:
- Include EVERY item's real name and price from PRODUCT CONTEXT — never invent or skip
- Do NOT overwhelm: no long per-item feature essays; roster stays tight
- Do NOT write a full solo-product layout for each piece
- If 4+ items, keep intro + body shorter so the roster stays scannable
- Keep conversation + CTA at the END only — clean closer, not scattered asks
- Same hard bans (no markdown, no em dashes, no hashtags in caption)
- Exactly 5 hashtags in the JSON array only`;

/**
 * Multi-photo IG bug: never ship "Shop this post". Soften to store / online / in-store.
 * Also fix obvious wrong group-count words in the opening lines.
 */
function enforceSetCaptionHygiene(caption, setSize, division = "resale_interiors") {
  let t = String(caption || "");
  const n = Math.max(0, Number(setSize) || 0);
  const site =
    division === "luxury_handbags" ? "lostandfoundhandbags.com" : "lostandfoundresale.com";
  const brandOnline =
    division === "luxury_handbags" ? "Lost & Found Handbags" : "Lost & Found Resale";

  // Count nouns in the first few non-empty lines only (hook / intro).
  const lines = t.split(/\n/);
  let touched = 0;
  for (let i = 0; i < lines.length && touched < 3; i += 1) {
    if (!String(lines[i] || "").trim()) continue;
    let line = lines[i];
    if (n !== 3) {
      line = line.replace(/\btrios?\b/gi, n === 2 ? "pair" : n === 4 ? "quartet" : "collection");
    }
    if (n !== 2) {
      line = line.replace(/\bduos?\b/gi, n === 3 ? "trio" : n === 4 ? "quartet" : "collection");
    }
    if (n !== 4) {
      line = line.replace(/\bquartets?\b/gi, n === 2 ? "pair" : n === 3 ? "trio" : "collection");
    }
    if (n === 4) {
      line = line.replace(/\bsets? of three\b/gi, "set of four");
      line = line.replace(/\bthese three\b/gi, "these four");
    }
    if (n === 2) {
      line = line.replace(/\bsets? of (?:three|four|3|4)\b/gi, "pair");
      line = line.replace(/\bthese (?:three|four)\b/gi, "these two");
    }
    if (line !== lines[i]) touched += 1;
    lines[i] = line;
  }
  t = lines.join("\n");

  // Kill "shop this post" language for multi-image captions.
  t = t
    .replace(/👉\s*Tap to shop this post\.?/gi, "👉 Shop this store right here")
    .replace(/👉\s*Shop this post\.?/gi, "👉 Shop this store right here")
    .replace(/👉\s*Shop the post\.?/gi, "👉 Shop this store right here")
    .replace(/\bTap to shop this post\b/gi, `Visit us online at ${brandOnline}`)
    .replace(/\bShop this post\b/gi, "Shop this store right here")
    .replace(/\bshop this post\b/gi, "shop this store right here")
    .replace(/\bShop the post\b/gi, "Visit us in store")
    .replace(/\bshop the post\b/gi, "visit us in store");

  // If a soft CTA remains but the site URL vanished, append a clean closer line once.
  if (
    /shop this store right here|visit us online|visit us in store/i.test(t) &&
    !new RegExp(site.replace(/\./g, "\\."), "i").test(t)
  ) {
    t = `${t.replace(/\s+$/g, "")}\n\nFor store details, consigning with us, our policies & more: ${site}`;
  }

  return t.replace(/\n{3,}/g, "\n\n").trim();
}

export function registerSocialCaptionRoute(app, { log } = {}) {
  const webflowLog =
    typeof log === "function"
      ? log
      : (level, payload) => console[level === "error" ? "error" : "log"]("[social-caption]", payload);

  app.post("/api/social-caption", async (req, res) => {
    const key = String(process.env.OPENAI_API_KEY || "").trim();
    if (!key) {
      return res.status(503).json({
        error: "OPENAI_API_KEY is not set on this server",
        code: "openai_missing",
      });
    }

    const body = req.body && typeof req.body === "object" ? req.body : {};
    const division =
      body.division === "luxury_handbags" ? "luxury_handbags" : "resale_interiors";
    const frame = body.frame != null ? String(body.frame || "").trim() : "";
    const intent = body.intent != null ? String(body.intent || "").trim() : "";
    const rawItems = Array.isArray(body.items) ? body.items : [];
    const setItems = rawItems
      .map((it, idx) => {
        const listingObj = it?.listing && typeof it.listing === "object" ? it.listing : {};
        const featured =
          it?.featuredImage && typeof it.featuredImage === "object" ? it.featuredImage : {};
        const name = String(it?.itemName || listingObj.title || "").trim();
        if (!name) return null;
        const imageUrl = String(
          it?.imageUrl || featured.url || listingObj.imageUrl || ""
        ).trim();
        const imageBase64 = String(it?.imageBase64 || featured.base64 || "").trim();
        const mime = String(it?.mime || featured.mime || "image/jpeg").trim() || "image/jpeg";
        return {
          index: idx + 1,
          itemName: name,
          imageUrl,
          imageBase64,
          mime,
          listing: {
            title: String(listingObj.title || name).trim(),
            price: String(listingObj.price || "").trim(),
            dimensions: String(listingObj.dimensions || "").trim(),
            description: sanitizeListingTextForCaption(listingObj.description || ""),
            productUrl: String(listingObj.productUrl || "").trim(),
            vertical: String(listingObj.vertical || "").trim(),
          },
        };
      })
      .filter(Boolean)
      .slice(0, 10);
    const isSetPost = setItems.length >= 2;

    const itemName = String(body.itemName || body.title || setItems[0]?.itemName || "").trim();
    const input = String(body.input || "").trim().slice(0, 2500);
    const listing = body.listing && typeof body.listing === "object" ? body.listing : {};
    const model = String(process.env.OPENAI_SOCIAL_MODEL || "gpt-4o").trim();

    if (!isSetPost && !itemName && !input && !listing.title && !listing.description) {
      return res.status(400).json({
        error: "Provide itemName and listing context (title, price, dimensions, description).",
      });
    }

    const cleanDescription = sanitizeListingTextForCaption(listing.description || "");
    const cleanInput = sanitizeListingTextForCaption(input);
    const itemSourceText = isSetPost
      ? setItems
          .map((it) =>
            [it.listing.title, it.itemName, it.listing.vertical, it.listing.description]
              .filter(Boolean)
              .join(" ")
          )
          .join("\n")
      : [listing.title, itemName, listing.vertical, cleanDescription, cleanInput]
          .filter(Boolean)
          .join(" ");
    const detectedItemType = inferSocialItemType(itemSourceText);

    const listingBits = isSetPost
      ? [
          `SET SIZE: ${setItems.length} items`,
          ...setItems.map((it) =>
            [
              `--- ITEM ${it.index} ---`,
              `Item name: ${it.listing.title || it.itemName}`,
              it.listing.price ? `Price: ${it.listing.price}` : "",
              it.listing.dimensions ? `Dimensions: ${it.listing.dimensions}` : "",
              it.listing.productUrl ? `Product URL: ${it.listing.productUrl}` : "",
              it.listing.vertical ? `Vertical: ${it.listing.vertical}` : "",
              it.listing.description
                ? `Listing description:\n${it.listing.description.slice(0, 1200)}`
                : "",
            ]
              .filter(Boolean)
              .join("\n")
          ),
          cleanInput ? `Staff notes / extra context:\n${cleanInput.slice(0, 1500)}` : "",
        ]
          .filter(Boolean)
          .join("\n\n")
      : [
          `Item name: ${listing.title || itemName || ""}`,
          listing.price ? `Price: ${listing.price}` : "",
          listing.dimensions ? `Dimensions: ${listing.dimensions}` : "",
          listing.productUrl ? `Product URL: ${listing.productUrl}` : "",
          listing.vertical ? `Vertical: ${listing.vertical}` : "",
          cleanDescription
            ? `Listing description (PRIMARY SOURCE for the caption — what the item is, look, feel, use):\n${cleanDescription.slice(
                0,
                2800
              )}`
            : "",
          cleanInput ? `Staff notes / extra context:\n${cleanInput.slice(0, 2500)}` : "",
        ]
          .filter(Boolean)
          .join("\n");

    const style = pickCaptionStyle();

    const resolveSetImageUrl = (it) => {
      const url = String(it?.imageUrl || "").trim();
      if (/^https?:\/\//i.test(url)) return url;
      const b64 = String(it?.imageBase64 || "").trim();
      if (!b64) return "";
      const raw = b64.includes(",") ? b64.split(",").pop() : b64;
      if (raw.length > 700000) return "";
      const mime = String(it?.mime || "image/jpeg").trim() || "image/jpeg";
      return `data:${mime};base64,${raw}`;
    };

    const setImageParts = [];
    if (isSetPost) {
      for (const it of setItems) {
        const imgUrl = resolveSetImageUrl(it);
        if (!imgUrl) continue;
        setImageParts.push({
          type: "text",
          text: `PHOTO FOR ITEM ${it.index}: ${it.listing.title || it.itemName}`,
        });
        setImageParts.push({
          type: "image_url",
          image_url: { url: imgUrl, detail: "low" },
        });
      }
    }

    const textPrompt = isSetPost
      ? `${getBasePrompt(division)}

${getFrameBlock(frame)}

${getIntentBlock(intent)}

${style.text}

${SET_CAPTION_RULES}

${SHIPPING_CAPTION_RULES}

VARIETY RULES:
- Commit to STYLE FOR THIS POST
- Never reuse the same tagline/location/CTA defaults
- Say we ship everywhere (never "most pieces")
- Hashtags ONLY in JSON hashtags array

MEDIA TYPE: static multi-item Meta post. Photos are attached BELOW in ITEM order (photo 1 = ITEM 1, etc.).
Use the images to make the set feel cohesive (shared material, color, era, scale, vibe) — still never invent facts not in PRODUCT CONTEXT.

TASK:
1) Read every item in PRODUCT CONTEXT and look at every photo below.
2) SET SIZE is ${setItems.length} — use count-accurate language (never call ${setItems.length} items a trio unless size is 3).
3) Write ONE cohesive set caption that flows across mixed categories.
4) Roster must include every item name + price (+ dims when present).
5) Keep it scannable — desire first, details tight, never overwhelming.
6) CTA / conversation ONLY at the end. Because this is a MULTI-PHOTO post, NEVER say "Shop this post" (Instagram shop-banner bug). Use "Shop this store right here" / "Visit us online" / "Visit us in store" instead.
7) Exactly 5 hashtags in hashtags array; ZERO hashtags in caption.
8) Never use em/en dashes — only hyphens or commas.

Return JSON:
{
  "caption": "full caption with real newline characters — NO hashtags",
  "hashtags": ["#one", "#two", "#three", "#four", "#five"],
  "item_type": "set",
  "reasoning": "one short sentence"
}

PRODUCT CONTEXT:
${listingBits}`
      : `${getBasePrompt(division)}

${getFrameBlock(frame)}

${getIntentBlock(intent)}

${style.text}

${PRODUCT_FOCUS_RULES}

${SHIPPING_CAPTION_RULES}

${buildItemAccuracyRules(detectedItemType, itemSourceText)}

VARIETY RULES (CRITICAL — posts must NOT sound alike):
- Commit fully to the STYLE FOR THIS POST personality above
- Never reuse the same tagline formula, location line, or CTA as a "default"
- "Sitting pretty in Scottsdale" is BANNED as a go-to — rotate through all location options
- NEVER say "we ship most pieces" — say we ship everywhere
- Local delivery angle when used: $95/hr, same rate no matter size or how many items
- Rotate CTAs: Shop the feed / Tap to shop / Shop this post / Shop right here / DM us / Come see it / We ship everywhere
- Vary emoji density and capitalization by style
- Make the caption feel custom-written for THIS photo and THIS SKU only

MEDIA TYPE: static_post (photo is already on the Meta post — you do NOT receive an image).
Write a KILLER, scroll-stopping, one-of-a-kind caption from the product story. Desire first. Facts sharp. Logistics optional and short. Do not reuse the same shipping sentence every post.

TASK:
1) Treat PRODUCT CONTEXT as ground truth. Never invent.
2) Center the caption on what the item IS — market the hell out of it, make desire obvious.
3) Match the house layout: tagline → title → 💰 price → dims → ✨ features → sell body → Perfect for: → 📍 → CTA.
4) Obey FRAME + OBJECTIVE + STYLE FOR THIS POST.
5) Shipping only if natural - one short line max; we ship everywhere; local delivery $95/hr flat; never "see below".
6) Return exactly 5 hashtags in the hashtags array ONLY - caption must contain ZERO hashtags.
7) NEVER use em dashes or en dashes anywhere - only regular hyphens (-) or commas.

Return JSON:
{
  "caption": "full caption with real newline characters between sections — NO hashtags in this field",
  "hashtags": ["#one", "#two", "#three", "#four", "#five"],
  "item_type": "short item type",
  "reasoning": "one short sentence"
}

PRODUCT CONTEXT:
${listingBits}`;

    const userContent =
      isSetPost && setImageParts.length
        ? [{ type: "text", text: textPrompt }, ...setImageParts]
        : textPrompt;

    try {
      const resp = await fetch("https://api.openai.com/v1/chat/completions", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${key}`,
        },
        body: JSON.stringify({
          model,
          temperature: isSetPost ? 0.95 : 1.05,
          max_tokens: isSetPost ? 2800 : 2200,
          response_format: { type: "json_object" },
          messages: [
            {
              role: "system",
              content: isSetPost
                ? `You are Lost & Found Resale's elite social caption writer for multi-item set posts. You receive listing facts AND photos in the same order as the items. Write one cohesive caption for the whole group, then a tight roster of every item with name and price. Match SET SIZE exactly in the hook (never say trio for 4 items). Mixed categories must flow. Never overwhelm. CTA/conversation only at the end. NEVER say "Shop this post" on multi-photo captions (Instagram shop-banner bug) — use visit us online / visit us in store / shop this store right here. Hashtags ONLY in the JSON hashtags array. NEVER use em/en dashes. No markdown. Return valid JSON only.`
                : "You are Lost & Found Resale's elite social caption writer - the one who makes people stop scrolling and want the piece. You have RANGE: natural, minimal, funky, editorial, storyteller, bold, or curator. Always keep the mandatory layout (tagline, title, PRICE right after title then dims, features, hungry sell body, Perfect for, location, CTA). Commit hard to the assigned style so no two posts sound alike. Say we ship everywhere (never 'most pieces'). Local delivery is $95/hr same rate no matter size or item count. Hashtags ONLY in the JSON hashtags array. NEVER use em/en dashes - only hyphens or commas. Sell hard, stay honest, make it amazing. No markdown. Return valid JSON only.",
            },
            { role: "user", content: userContent },
          ],
        }),
      });

      const raw = await resp.text();
      if (!resp.ok) {
        webflowLog("error", {
          event: "api.social_caption.openai_http",
          status: resp.status,
          body: raw.slice(0, 400),
        });
        return res.status(502).json({
          error: `OpenAI request failed (${resp.status})`,
          detail: raw.slice(0, 200),
        });
      }

      let json;
      try {
        json = JSON.parse(raw);
      } catch {
        return res.status(502).json({ error: "OpenAI returned invalid JSON wrapper" });
      }

      const content = json?.choices?.[0]?.message?.content;
      if (!content) {
        return res.status(502).json({ error: "OpenAI returned empty content" });
      }

      let parsed;
      try {
        parsed = JSON.parse(content);
      } catch {
        return res.status(502).json({ error: "OpenAI content was not valid JSON" });
      }

      const captionRaw = ensurePriceAboveDimensions(
        stripEmDashes(stripHashtagsFromCaption(String(parsed.caption || "").replace(/\s+$/g, "")))
      );
      const caption = isSetPost
        ? enforceSetCaptionHygiene(captionRaw, setItems.length, division)
        : enforceItemAccurateOpening(captionRaw, detectedItemType, itemSourceText);
      const hashtags = ensureHashtags(
        parsed.hashtags,
        {
          title: isSetPost
            ? setItems.map((it) => it.listing.title).join(" ")
            : listing?.title || itemName || "",
          vertical: listing?.vertical || "",
        },
        division
      );
      if (!caption) {
        return res.status(502).json({ error: "Empty caption from model" });
      }

      webflowLog("info", {
        event: "api.social_caption.ok",
        model,
        division,
        frame: frame || "data_led",
        style: style.id,
        textOnly: !(isSetPost && setImageParts.length),
        imageCount: isSetPost ? setImageParts.filter((p) => p.type === "image_url").length : 0,
        setSize: isSetPost ? setItems.length : 1,
        captionLen: caption.length,
      });

      const fullText = hashtags
        ? `${caption.replace(/\n{2,}$/g, "").trimEnd()}\n\n${hashtags}`
        : caption;
      return res.json({
        success: true,
        model,
        caption,
        hashtags,
        fullText,
        setSize: isSetPost ? setItems.length : 1,
        imageCount: isSetPost ? setImageParts.filter((p) => p.type === "image_url").length : 0,
        content: { ...parsed, caption, hashtags: parsed.hashtags, fullText },
      });
    } catch (err) {
      webflowLog("error", {
        event: "api.social_caption.error",
        message: err?.message || String(err),
      });
      return res.status(500).json({ error: err?.message || "social-caption failed" });
    }
  });
}
