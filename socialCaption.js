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

ITEM EMOJIS (pick the one that matches the piece — NEVER 👜):
lamp💡 · chair🪑 · sofa/sectional🛋️ · table/desk🍽️ · dresser/cabinet🗄️ · art/painting🖼️ · rug🧶 · mirror🪞 · vase/pot🏺 · clock⏰ · lighting ✨ · stool/bench🪑 · bookshelf📚 · outdoor🪴

MANDATORY CAPTION LAYOUT (blank line between EVERY section):
1) HOOK — 1–2 lines, emoji-forward, scroll-stopping, specific to THIS piece (not generic "vintage vibes")
2) PRODUCT NAME LINE — full listing title + item-specific emoji
3) DIMENSIONS — only if present in listing: 📐 W" x D" x H" (never write the word "inches")
4) FEATURES — 2–5 lines starting with ✨ pulled from description (materials, finish, maker, AS IS notes, hardware)
5) PRICE — 💰 exact price from listing. If description/title implies markdown/sale, call it out cleanly
6) BODY — 1–2 short paragraphs. Sell the room it belongs in. Do NOT paste the full product name again
7) Perfect for: — 2–4 bullets with • (room types, style moments, who will love it)
8) LOCATION 📍 — one strong Scottsdale line (${LOCATION_EXAMPLES_FURNITURE})
9) TWO-PART CTA:
   PRIMARY (vary): Shop right here · Tap to shop · Shop on IG · Shop on Facebook · Shop this post
   SECONDARY: store details / consigning / policies → lostandfoundresale.com

QUALITY BAR (this is what "amazing" means):
- Sounds like a human who knows furniture, not a catalog bot
- Every emoji earns its place
- AS IS / condition language from the listing stays honest and plain
- No invented brands, sizes, woods, or prices
- Hashtags: exactly 5, specific (mix item + style + Scottsdale/resale), not #love #instagood

HARD BANS:
- Markdown (** __ * [links](url))
- Em dashes / en dashes (use hyphen or comma)
- "Ships from Scottsdale"
- "inside @lostandfoundresale"
- Generic hooks that could fit any product
- "Please see … below the description" / Delivery-Pickup-Freight Options pointers
- Long shipping policy paragraphs
- Output JSON only`,

  luxury_handbags: `You are Lost + Found Luxury Handbags & Accessories' best static-post writer. Crush Facebook + Instagram captions: calm confidence, authenticity without shouting, brand as signal not ad.

BRAND VOICE:
- Luxury as circulation. Intentional. Clean.
- Creative but restrained emojis
- Philosophy and education over hard sell

ITEM EMOJIS:
handbag👜 · scarf🧣 · shoes👠 · wallet💳 · watch⌚ · jewelry📿 · men's bag💼 · belt🪢 · sunglasses🕶️

MANDATORY CAPTION LAYOUT (blank line between EVERY section):
1) HOOK — emoji-forward, specific to brand/silhouette/era from listing
2) PRODUCT NAME LINE — title + item emoji
3) PRICE 💰 — exact listing price (never "Price Upon Request" if a price exists)
4) FEATURES — ✔ or 🤍/💜 lines from description (auth notes only if listing says so)
5) BODY — descriptive + lifestyle paragraphs; natural name variations, not verbatim repeats
6) LOCATION 📍 — ALWAYS @lostandfoundresale (${LOCATION_EXAMPLES_LUXURY})
7) CTA — lostandfoundhandbags.com (NEVER lostandfoundresale.com for the shop link)

QUALITY BAR:
- Feels premium and human
- No invented authenticity claims
- Exactly 5 sharp hashtags
- JSON only

HARD BANS:
- Dimensions unless listing has them or user asked
- Markdown; em/en dashes
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
2) Center the caption on what the item IS (from the main description).
3) Turn description details into ✨/✔ feature lines and a vivid body that could only fit this SKU.
4) Obey FRAME + OBJECTIVE.
5) Shipping only if it fits naturally — and phrase it differently than recent posts; often omit it.
6) Return exactly 5 hashtags.

Return JSON:
{
  "caption": "full caption with real newline characters between sections",
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
                "You are Lost & Found's elite social caption writer. House style: creative emojis, clean layout, Scottsdale location tags, two-part CTAs, no markdown. Lead with the product story from the listing description. Shipping is optional and must vary or be omitted — never the same nationwide line every post. Return valid JSON only.",
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

      const caption = String(parsed.caption || "").replace(/\s+$/g, "");
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

      return res.json({
        success: true,
        model,
        caption,
        hashtags,
        fullText: hashtags ? `${caption}\n\n${hashtags}` : caption,
        content: parsed,
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
