import { isArizonaDestination, palletizeItems } from "./freightPalletize.js";

const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

function truthy(value) {
  if (value === true || value === 1) return true;
  const s = String(value ?? "")
    .trim()
    .toLowerCase();
  return s === "yes" || s === "true" || s === "1" || s === "on";
}

function falsy(value) {
  if (value === false || value === 0) return true;
  const s = String(value ?? "")
    .trim()
    .toLowerCase();
  return s === "no" || s === "false" || s === "0" || s === "off";
}

function yesNo(value, fallback = null) {
  if (truthy(value)) return true;
  if (falsy(value)) return false;
  return fallback;
}

function str(value, max = 500) {
  return String(value ?? "")
    .trim()
    .slice(0, max);
}

function numOrNull(value) {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? n : null;
}

function parseItems(raw) {
  if (Array.isArray(raw)) return raw;
  if (typeof raw === "string" && raw.trim()) {
    try {
      const parsed = JSON.parse(raw);
      return Array.isArray(parsed) ? parsed : [];
    } catch {
      return [];
    }
  }
  return [];
}

/**
 * Normalize Webflow / JSON body into a freight quote request.
 */
export function validateFreightQuoteRequest(body = {}) {
  const customerName = str(body.customerName || body.name, 120);
  const customerEmail = str(body.customerEmail || body.email, 200).toLowerCase();
  const customerPhone = str(body.customerPhone || body.phone, 40);

  const street = str(body.street || body.customerStreetAddress || body.address, 200);
  const city = str(body.city || body.customerCity, 100);
  const state = str(body.state || body.customerState, 40);
  const zip = str(body.zip || body.customerZip || body.postalCode, 20);
  const deliveryNotes = str(body.deliveryNotes || body.notes || body.message, 2000);

  const modeRaw = str(body.mode || body.requestType || "please_quote", 40).toLowerCase();
  const mode =
    modeRaw === "estimate" || modeRaw === "get_estimate" || modeRaw === "self_estimate"
      ? "estimate"
      : "please_quote";

  const deliveryTypeRaw = str(body.deliveryType || body.locationType || "residential", 40).toLowerCase();
  const deliveryType = deliveryTypeRaw.startsWith("comm") ? "commercial" : "residential";

  if (!customerName) {
    return { ok: false, error: "Please enter your name." };
  }
  if (!customerEmail || !EMAIL_RE.test(customerEmail)) {
    return { ok: false, error: "Please enter a valid email address." };
  }
  if (!street || !city || !state || !zip) {
    return { ok: false, error: "Please enter the full delivery address (street, city, state, ZIP)." };
  }

  const rawItems = parseItems(body.items);
  // Also accept flat item_1_title style fields from Webflow forms.
  if (!rawItems.length) {
    for (let i = 1; i <= 20; i += 1) {
      const title = str(body[`item_${i}_title`] || body[`item${i}Title`], 300);
      if (!title) continue;
      rawItems.push({
        title,
        widthIn: body[`item_${i}_width`] ?? body[`item${i}Width`],
        depthIn: body[`item_${i}_depth`] ?? body[`item${i}Depth`],
        heightIn: body[`item_${i}_height`] ?? body[`item${i}Height`],
        weightLb: body[`item_${i}_weight`] ?? body[`item${i}Weight`],
        quantity: body[`item_${i}_quantity`] ?? body[`item${i}Quantity`] ?? 1,
        price: body[`item_${i}_price`] ?? body[`item${i}Price`],
        productUrl: body[`item_${i}_url`] ?? body[`item${i}Url`],
        source: body[`item_${i}_source`] ?? "manual",
        nonStackable: body[`item_${i}_non_stackable`],
        category: body[`item_${i}_category`],
      });
    }
  }

  if (!rawItems.length) {
    return {
      ok: false,
      error: "Add at least one item (lookup by exact title or enter dimensions manually).",
    };
  }

  const items = rawItems
    .map((it, index) => {
      const title = str(it.title || it.itemName || it.name, 300);
      if (!title) return null;
      return {
        title,
        widthIn: numOrNull(it.widthIn ?? it.width),
        depthIn: numOrNull(it.depthIn ?? it.depth ?? it.lengthIn ?? it.length),
        heightIn: numOrNull(it.heightIn ?? it.height),
        stackedHeightIn: numOrNull(it.stackedHeightIn),
        weightLb: numOrNull(it.weightLb ?? it.weight),
        quantity: numOrNull(it.quantity) || 1,
        price: str(it.price, 40),
        productUrl: str(it.productUrl || it.url, 500),
        source: str(it.source, 40) || (it.lookedUp ? "listing_lookup" : "manual"),
        lookedUp: Boolean(it.lookedUp),
        nonStackable: it.nonStackable,
        category: str(it.category, 80),
        notes: str(it.notes, 500),
        freightClass: numOrNull(it.freightClass),
        index: index + 1,
      };
    })
    .filter(Boolean);

  if (!items.length) {
    return { ok: false, error: "Add at least one item with a title." };
  }

  const hasDock = yesNo(body.hasDock ?? body.dock, null);
  const hasForklift = yesNo(body.hasForklift ?? body.forklift, null);
  const hasFreightElevator = yesNo(body.hasFreightElevator ?? body.freightElevator, null);
  const hasStairs = yesNo(body.hasStairs ?? body.stairs, null);
  const stairFlights = numOrNull(body.stairFlights ?? body.flightsOfStairs) || 0;
  const needsMoreThanTwoPeople = yesNo(
    body.needsMoreThanTwoPeople ?? body.moreThanTwoPeople ?? body.extraLabor,
    null
  );
  const hasTightTurns = yesNo(body.hasTightTurns ?? body.tightTurns, null);
  const insideDeliveryRequested = yesNo(body.insideDeliveryRequested ?? body.insideDelivery, false);
  const unpackingRequested = yesNo(body.unpackingRequested ?? body.unpacking, false);

  // SOP defaults: liftgate PU + delivery unless dock/forklift confirmed.
  let needsLiftgatePickup = yesNo(body.needsLiftgatePickup ?? body.liftgatePickup, null);
  let needsLiftgateDelivery = yesNo(body.needsLiftgateDelivery ?? body.liftgateDelivery, null);
  if (needsLiftgatePickup == null) needsLiftgatePickup = hasDock === true ? false : true;
  if (needsLiftgateDelivery == null) {
    needsLiftgateDelivery = hasDock === true || hasForklift === true ? false : true;
  }

  const residentialDelivery = deliveryType === "residential";

  const access = {
    deliveryType,
    residentialDelivery,
    hasDock,
    hasForklift,
    needsLiftgatePickup,
    needsLiftgateDelivery,
    hasFreightElevator,
    hasStairs,
    stairFlights: hasStairs ? stairFlights : 0,
    needsMoreThanTwoPeople,
    hasTightTurns,
    insideDeliveryRequested,
    unpackingRequested,
  };

  const unansweredAccess = [];
  if (hasDock == null) unansweredAccess.push("dock at destination");
  if (hasForklift == null) unansweredAccess.push("forklift at destination");
  if (hasFreightElevator == null) unansweredAccess.push("freight elevator");
  if (hasStairs == null) unansweredAccess.push("stairs");
  if (hasStairs === true && !stairFlights) unansweredAccess.push("how many flights of stairs");
  if (needsMoreThanTwoPeople == null) unansweredAccess.push("needs more than 2 people");
  if (hasTightTurns == null) unansweredAccess.push("tight turns / narrow halls");

  if (unansweredAccess.length) {
    return {
      ok: false,
      error: `Please answer access questions: ${unansweredAccess.join(", ")}.`,
    };
  }

  const palletized = palletizeItems(items);
  const isLocalAz = isArizonaDestination({ state, zip });

  return {
    ok: true,
    submission: {
      customerName,
      customerEmail,
      customerPhone,
      street,
      city,
      state,
      zip,
      deliveryNotes,
      mode,
      access,
      isLocalAz,
      items,
      palletized,
    },
  };
}
