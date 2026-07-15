/**
 * POST /api/social-caption — Static IG/FB captions for Meta Business Suite extension.
 * Text-only from listing facts (name, price, dimensions, description). No image to OpenAI.
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

const LOCATION_EXAMPLES_FURNITURE = `LOCATION LINE EXAMPLES (pick ONE and vary each caption — do not copy verbatim every time):
📍 Located in Scottsdale, AZ near Scottsdale Quarter
📍 Scottsdale pickup · Lost + Found Resale Interiors
📍 Sitting pretty in Scottsdale — come see it in person
📍 Scottsdale, Arizona · shop the floor or shop this post
NEVER: "inside @lostandfoundresale" | NEVER: art scene | NEVER: Ships from Scottsdale`;

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

QUALITY BAR - MARKET IT:
- Caption should make a stranger want this piece
- Lead with desire, close with easy next step
- AS IS stays honest and framed as character when true
- No invented brands, sizes, or prices
- Exactly 5 specific hashtags in the array only

HARD BANS:
- Hashtags inside "caption"
- Markdown (** __ * [links](url))
- Em dashes / en dashes / long dashes (— – ―) - ALWAYS use a normal hyphen (-) or a comma instead. Zero exceptions.
- Price below features or below dimensions (price must be above dimensions)
- "Ships from Scottsdale"
- "inside @lostandfoundresale"
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
- Vary structure each time while keeping the mandatory layout sections`;
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
- Shipping/pickup is secondary. Many posts should skip logistics entirely
- When you DO mention it, keep it natural and DIFFERENT each time (one short phrase max), e.g. vary across posts:
  "Ships nationwide" / "We can ship this" / "Available to ship" / "Scottsdale pickup or ship it out"
- NEVER copy-paste "Nationwide shipping is available." on every caption
- NEVER write "Please see Delivery, Pickup & Freight Options below the description…"
- NEVER tell people to scroll/look below the description
- NEVER dump freight policies, brokers, liftgate fees, or 72-hour windows`;

const PRODUCT_FOCUS_RULES = `PRODUCT-FIRST RULE (MOST IMPORTANT):
- Build the killer caption from the MAIN product explanation in the listing — what it is, how it looks/feels, why it belongs in a room
- Lead with uniqueness: maker/style cues, texture, color, form, era, AS IS honesty when present
- Features ✨ and body paragraphs must come from that core description, not from shipping or store policy text
- Price and dimensions support the story; they are not the story
- Every caption should feel written for THIS SKU only`;

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
    const itemName = String(body.itemName || body.title || "").trim();
    const input = String(body.input || "").trim().slice(0, 2500);
    const listing = body.listing && typeof body.listing === "object" ? body.listing : {};
    const model = String(process.env.OPENAI_SOCIAL_MODEL || "gpt-4o").trim();

    if (!itemName && !input && !listing.title && !listing.description) {
      return res.status(400).json({
        error: "Provide itemName and listing context (title, price, dimensions, description).",
      });
    }

    const cleanDescription = sanitizeListingTextForCaption(listing.description || "");
    const cleanInput = sanitizeListingTextForCaption(input);

    const listingBits = [
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

    const textPrompt = `${getBasePrompt(division)}

${getFrameBlock(frame)}

${getIntentBlock(intent)}

${PRODUCT_FOCUS_RULES}

${SHIPPING_CAPTION_RULES}

MEDIA TYPE: static_post (photo is already on the Meta post — you do NOT receive an image).
Build a KILLER, one-of-a-kind caption from the product story in the listing. Do not reuse the same shipping sentence every post.

TASK:
1) Treat PRODUCT CONTEXT as ground truth. Never invent.
2) Center the caption on what the item IS — market it hard, make desire obvious.
3) Match the house layout: tagline → title → 💰 price → dims → ✨ features → sell body → Perfect for: → 📍 → CTA.
4) Obey FRAME + OBJECTIVE.
5) Shipping only if natural - vary or omit; never "see below".
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

    try {
      const resp = await fetch("https://api.openai.com/v1/chat/completions", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${key}`,
        },
        body: JSON.stringify({
          model,
          temperature: 0.92,
          max_tokens: 2200,
          response_format: { type: "json_object" },
          messages: [
            {
              role: "system",
              content:
                "You are Lost & Found's elite social caption writer and salesperson. House style: punchy tagline, title, PRICE right after title then dims, sparkle features, desire-driven body, Perfect for, location, CTA. Hashtags belong ONLY in the JSON hashtags array. NEVER use em/en dashes - only hyphens or commas. Sell hard while staying honest. No markdown. Return valid JSON only.",
            },
            { role: "user", content: textPrompt },
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

      const caption = ensurePriceAboveDimensions(
        stripEmDashes(stripHashtagsFromCaption(String(parsed.caption || "").replace(/\s+$/g, "")))
      );
      const hashtags = formatHashtags(parsed.hashtags);
      if (!caption) {
        return res.status(502).json({ error: "Empty caption from model" });
      }

      webflowLog("info", {
        event: "api.social_caption.ok",
        model,
        division,
        frame: frame || "data_led",
        textOnly: true,
        captionLen: caption.length,
      });

      const fullText = hashtags ? `${caption}\n\n${hashtags}` : caption;
      return res.json({
        success: true,
        model,
        caption,
        hashtags,
        // Hashtags once, at the end only
        fullText,
        // Never leak the raw model caption (often had leading hashtags).
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
