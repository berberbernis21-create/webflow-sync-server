/**
 * LLM-based category classifier: picks the exact category within a vertical.
 * - Luxury: Handbags, Totes, Crossbody, Backpacks, Small Bags, Wallets, Luggage, Scarves, Belts, Jewelry, Accessories, Other
 *   Rules: jewelry → Jewelry; shoes → Other; miscellaneous luxury items (straps, pouches, charms, etc.) → Accessories; Other only for footwear or truly uncategorizable.
 * - Furniture: LivingRoom, DiningRoom, OfficeDen, Rugs, ArtMirrors, Bedroom, Accessories, OutdoorPatio, Lighting
 *   Rules: art/paintings/photographs/framed → ArtMirrors; umbrellas/patio → OutdoorPatio.
 * Uses same OPENAI_API_KEY as vertical classifier. Falls back to null on failure so caller can use keyword logic.
 *
 * Subcategory LLM runs only when evidence confidence is below LLM_CATEGORY_CONFIDENCE_THRESHOLD (default 0.8); see server.js detectCategoryFurnitureEvidence / detectLuxuryCategoryEvidence.
 */

import axios from "axios";

const OPENAI_API_URL = "https://api.openai.com/v1/chat/completions";
const DEFAULT_MODEL = "gpt-4o-mini";

function stripHtml(html) {
  if (!html || typeof html !== "string") return "";
  return html.replace(/<[^>]*>/g, " ").replace(/\s+/g, " ").trim();
}

function getProductText(product) {
  const title = (product.title || "").trim();
  const productType = (product.product_type || "").trim();
  const vendor = (product.vendor || "").trim();
  const tags = Array.isArray(product.tags)
    ? product.tags
    : typeof product.tags === "string"
      ? product.tags.split(",").map((s) => s.trim()).filter(Boolean)
      : [];
  const tagsStr = tags.join(", ");
  const description = stripHtml(product.body_html || "").trim();
  return { title, productType, vendor, tagsStr, description };
}

const LUXURY_CATEGORIES = [
  "Handbags", "Totes", "Crossbody", "Backpacks", "Small Bags",
  "Wallets", "Luggage", "Scarves", "Belts", "Jewelry", "Accessories", "Other"
];

const FURNITURE_CATEGORIES = [
  "LivingRoom", "DiningRoom", "OfficeDen", "Rugs", "ArtMirrors",
  "Bedroom", "Accessories", "OutdoorPatio", "Lighting"
];

const LUXURY_SYSTEM = `You are a retail category classifier for luxury consignment.
Use ONLY the product title and description to choose exactly ONE category. Do not use product type or tags. Return only valid JSON.

Allowed categories: ${LUXURY_CATEGORIES.join(", ")}.

RULES (mandatory):
- Jewelry, earrings, bracelets, necklaces, rings, pendants, brooches, clip-on earrings → always "Jewelry".
- Wedding/stacking/eternity bands and other ring-style **bands** that include sizing (e.g. “size 8”), hammered metal, or gold/silver-tone wording → "Jewelry" (not Accessories). Opera-length / multi-strand statement necklaces, including titles that say “multi-media” as a style descriptor → "Jewelry" (not books/media).
- Barrettes, hair accessories, keychains, key rings, bag charms, purse hooks, straps, gloves and similar wearable add-ons → always "Accessories".
- Shoes, sneakers, boots, heels, sandals, loafers, mules, flats, pumps, footwear → always "Other".
- Handbags, shoulder bags, satchels, day bags → "Handbags".
- Totes, carryalls, book totes → "Totes".
- Crossbody, camera bag, WOC, chain bag, sling bag → "Crossbody".
- Backpacks, daypacks, rucksacks → "Backpacks".
- Small bags, clutches, pochettes, wristlets, minaudiere, pouches, cosmetic pouches/cases, makeup bags, vanity cases, toiletry/beauty pouches → "Small Bags".
- Wallets, cardholders, key pouches, passport holders → "Wallets".
- Luggage, briefcases, weekender, duffle, keepall → "Luggage".
- Scarves, shawls, wraps, stoles → "Scarves".
- Belts, chain belts, waist belts, belt accessories (including charm bags worn on belt) → always "Belts".
- PREFER "Accessories" for miscellaneous luxury add-ons that are not bags or pouches: dust bags, straps, bag charms, keychains, key rings, barrettes, sunglass cases, phone cases, decorative accessories, gloves, purse hooks, etc.
- Treat agendas, agenda covers, document holders, notebooks, notepads, folios, business card cases and similar stationery/office pieces as "Other" (not Accessories).
- Use "Other" ONLY for: (1) footwear, or (2) these stationery/office items, or (3) truly uncategorizable/odd items that do not fit anywhere. When in doubt between "Small Bags" and "Accessories" for a pouch-like item, choose "Small Bags"; when in doubt between "Accessories" and "Other", choose "Accessories" except for the stationery/office items above.

Output format only: {"category": "<one of allowed>", "confidence": 0-1, "reasoning": "brief"}`;

const FURNITURE_SYSTEM = `You are a retail category classifier for furniture and home.
Given a product (title, type, tags, description), choose exactly ONE category. Return only valid JSON.

Allowed categories: ${FURNITURE_CATEGORIES.join(", ")}.
(These map to: Living Room, Dining Room, Office Den, Rugs, Art/Mirrors, Bedroom, Accessories, Outdoor/Patio, Lighting.)

RULES (mandatory):
- Paintings, art prints, framed art, photographs, framed photos, canvas art, sculpture, statues, figurines, mirrors, wall art, lithographs → "ArtMirrors".
- Umbrellas, umbrella stand, patio umbrella, outdoor umbrella, patio furniture, outdoor seating, garden, deck, adirondack, hammock, fire pit → "OutdoorPatio".
- Patio / porch / deck dining — including **patio dining table**, outdoor dining table, mosaic or tile **patio** tables — is **"OutdoorPatio"**, not "DiningRoom". Use "DiningRoom" only for **indoor** dining when there is no patio/outdoor/deck/porch context.
- Sofas, chairs, tables, coffee table, console, ottoman, sectional, loveseat → "LivingRoom".
- **Bench / benches** (rustic wood bench, entryway bench, mudroom bench, storage bench, settle bench, bed bench, hall bench) are **seating furniture** → **"LivingRoom"**, never "Accessories". **Grommets** or metal rings on wood describe **furniture detailing / hardware**, not handbags. **Dining bench** → **"DiningRoom"**. **Patio / outdoor / garden / porch bench** (clearly outdoor seating) → **"OutdoorPatio"**.
- Wall units, shelving units, etageres, modular/media walls, large room dividers with shelves — case goods for living spaces → "LivingRoom" (never "Accessories" just because they hold decor).
- Trays alone (butler tray, butlers tray, serving tray, decorative tray, ottoman tray — removable tray or tray-with-stand sold as a tray) → "Accessories". Tray **table** (explicit tray+table furniture, TV tray table, folding tray table meant as a small table) → "LivingRoom".
- Dining table, dining chairs, buffet, sideboard, bar cart, hutch → "DiningRoom".
- Desk, office chair, filing cabinet, bookshelf (office) → "OfficeDen".
- Game tables (shuffleboard, pool table, ping pong, foosball), exercise/workout/fitness equipment (dumbbells, treadmill, home gym), play equipment, game room furniture → "OfficeDen".
- Rug, runner, area rug → "Rugs".
- Bed, headboard, nightstand, dresser, armoire, vanity → "Bedroom".
- Headboard/bedroom furniture remains "Bedroom" even if description says phrases like "works of art" or "artful design".
- **Mattress**, **box spring**, split box, box foundation, pillow-top / euro top, adjustable base/bed → **"Bedroom"** (never "Accessories" because description mentions "pillow" or "box").
- **Chandelier**, sconce, pendant light, **floor lamp**, **table lamp**, **desk lamp**, **bedside lamp**, torchiere → "Lighting". Lamps are never "Accessories".
- Freestanding **candlesticks**, **candleholder** / **candle holder**, taper holders, and tabletop candle pieces (pewter, brass, glass hurricanes, clay taper holders, etc.) → "Accessories", not "Lighting". If **candlestick**, **candleholder**, or **candle holder** is in the title → **Accessories** — never LivingRoom because of “dining table” / “centerpiece” in the description.
- Vases, **tray** / **trays** (decorative, serving, pedestal tray — not **tray table** furniture), **bowl** / **bowls** (decorative, serving, metal bowl, fruit bowl — not **bowl chair** furniture), **pedestal bowl**, compotes, wood or ceramic decorative bowls, **scroll box** / document box, lidded vanity boxes, pillows, clocks, picture frames (empty), trinkets, **home decor** tabletop pieces → "Accessories" (small decor only — not wall storage furniture). Pedestal != pedestal **dining table**. Copy about dining or living **rooms** does **not** recategorize a bowl to DiningRoom or LivingRoom. If **vase**, **bowl** (not bowl chair), **pedestal bowl**, or **tray** (non-tray-table) is in the title → **Accessories**, never LivingRoom.
- **Books** (including encyclopedias, hardcovers, **coffee table books**, art/illustrated books, tomes) → "Accessories", not LivingRoom — "coffee table" in marketing copy refers to display style, not furniture.
- **Decanters**, wine decanters, crystal or glass **carafe** / **carafes** for serving → "Accessories" (tabletop serveware), not DiningRoom case goods; luxury branding in copy does not change this. If **decanter** or **carafe** is in the title → **Accessories**, never LivingRoom.
- Beaded or deerskin **lance**, ceremonial/decorative lance, indigenous-style beaded staff (sold as decor or collectible, not athletic equipment) → "Accessories", not LivingRoom or ArtMirrors sculpture.
- If unclear, prefer the most specific match; default "Accessories" only when nothing else fits.

Output format only: {"category": "<one of allowed>", "confidence": 0-1, "reasoning": "brief"}`;

function buildUserPrompt(product) {
  const { title, description } = getProductText(product);
  return `Classify this product into exactly one category. Use ONLY the title and description below. Return only valid JSON: {"category": "<allowed value>", "confidence": 0-1, "reasoning": "brief"}

Title: ${title || "(none)"}
Description: ${description ? description.slice(0, 2000) : "(none)"}`;
}

function parseResponse(rawContent, allowedCategories) {
  const trimmed = (rawContent || "").trim();
  const jsonStr = trimmed.replace(/^```(?:json)?\s*/i, "").replace(/\s*```\s*$/i, "").trim();
  let parsed;
  try {
    parsed = JSON.parse(jsonStr);
  } catch {
    return null;
  }
  if (!parsed || typeof parsed !== "object") return null;
  const cat = parsed.category;
  if (!cat || typeof cat !== "string") return null;
  const normalized = allowedCategories.find((c) => c.toLowerCase() === cat.trim().toLowerCase()) || cat.trim();
  if (!allowedCategories.includes(normalized)) return null;
  const confidence = typeof parsed.confidence === "number" ? parsed.confidence : parseFloat(parsed.confidence);
  const safeConfidence = Number.isFinite(confidence) ? Math.max(0, Math.min(1, confidence)) : 0;
  return {
    category: normalized,
    confidence: safeConfidence,
    reasoning: typeof parsed.reasoning === "string" ? parsed.reasoning : "",
  };
}

/**
 * Classify product into a single category for the given vertical using OpenAI.
 * @param {object} product - Shopify product { title, product_type, vendor, tags, body_html }
 * @param {"luxury"|"furniture"} vertical
 * @param {object} [logPayload] - Optional object for logging raw/parsed
 * @param {Function} [logFn] - Optional (level, data) logger
 * @returns {Promise<{ category: string, confidence: number, reasoning: string } | null>} Null if disabled, no key, or parse failure (caller should use keyword fallback).
 */
export async function classifyCategoryWithLLM(product, vertical, logPayload = {}, logFn = null) {
  const openaiKey = process.env.OPENAI_API_KEY;
  const model = (process.env.OPENAI_CATEGORY_MODEL || process.env.OPENAI_VERTICAL_MODEL || DEFAULT_MODEL).trim() || DEFAULT_MODEL;
  const useLLMCategory = process.env.LLM_CATEGORY_ENABLED !== "false" && process.env.LLM_CATEGORY_ENABLED !== "0";

  if (!useLLMCategory || !openaiKey || typeof openaiKey !== "string" || !openaiKey.trim()) {
    if (logFn && !openaiKey) logFn("info", { event: "llm_category.skipped", reason: "OPENAI_API_KEY missing or LLM_CATEGORY_ENABLED=false" });
    logPayload.skipped = true;
    return null;
  }

  const systemPrompt = vertical === "luxury" ? LUXURY_SYSTEM : FURNITURE_SYSTEM;
  const allowedCategories = vertical === "luxury" ? LUXURY_CATEGORIES : FURNITURE_CATEGORIES;
  const userPrompt = buildUserPrompt(product);

  try {
    const res = await axios.post(
      OPENAI_API_URL,
      {
        model,
        messages: [
          { role: "system", content: systemPrompt },
          { role: "user", content: userPrompt },
        ],
        temperature: 0,
        max_tokens: 150,
      },
      {
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${openaiKey}`,
        },
        timeout: 15000,
      }
    );

    const rawContent = res.data?.choices?.[0]?.message?.content;
    logPayload.raw = rawContent;

    const parsed = parseResponse(rawContent, allowedCategories);
    logPayload.parsed = parsed;

    if (parsed && parsed.confidence >= 0.5) {
      if (logFn) logFn("info", { event: "llm_category.result", vertical, category: parsed.category, confidence: parsed.confidence });
      return parsed;
    }
    if (logFn && parsed) logFn("info", { event: "llm_category.low_confidence", vertical, category: parsed?.category, confidence: parsed?.confidence });
    return parsed;
  } catch (err) {
    if (logFn) logFn("warn", { event: "llm_category.error", vertical, message: err.message || String(err) });
    logPayload.error = err.message || String(err);
    return null;
  }
}
