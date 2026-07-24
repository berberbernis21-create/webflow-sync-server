import {
  MAX_FREIGHT_ITEMS,
  MAX_STAIR_FLIGHTS,
  SHOWROOM_ORIGIN,
  isArizonaStateOrZip,
  palletizeItems,
} from "./freightPalletize.js";

const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/i;

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

function bool(value, fallback = false) {
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

function normalizeAccess(body = {}) {
  const a = body.access && typeof body.access === "object" ? body.access : {};
  const destinationType = str(body.destination_type || body.deliveryType || "", 40).toLowerCase();
  const deliveryPath = str(body.delivery_path || body.deliveryPath || "", 40).toLowerCase();
  const isCompanyTruck = deliveryPath === "local_az" || deliveryPath === "pickup_az";
  const residential =
    a.residential != null
      ? bool(a.residential)
      : destinationType.startsWith("comm")
        ? false
        : true;

  const dock = bool(a.dock ?? a.hasDock, false);
  const forklift = bool(a.forklift ?? a.hasForklift, false);

  const whiteGlove = bool(a.white_glove ?? a.white_glove_delivery ?? a.whiteGlove, false);
  const roomOfChoice = bool(a.room_of_choice ?? a.roomOfChoice, false);
  const warehousePickup = bool(a.warehouse_pickup ?? a.warehousePickup, false);
  const storeLoading = bool(a.store_loading ?? a.storeLoading, false);
  const noLiftgate = bool(a.no_liftgate ?? a.noLiftgate, false);

  // Liftgate is FreightCenter/LTL language — not a customer choice on local truck jobs.
  // Nationwide: customer selects delivery liftgate; pickup liftgate is staff-confirmed (estimate default yes).
  let liftgatePickup = false;
  let liftgateDelivery = false;
  if (!isCompanyTruck) {
    liftgatePickup = bool(a.liftgate_pickup ?? a.liftgatePickup, true);
    if (storeLoading) liftgatePickup = bool(a.liftgate_pickup, false);
    if (a.liftgate_delivery != null || a.liftgateDelivery != null) {
      liftgateDelivery = bool(a.liftgate_delivery ?? a.liftgateDelivery, true);
    } else if (noLiftgate) {
      liftgateDelivery = false;
    } else {
      liftgateDelivery = true;
    }
    if (dock || forklift || warehousePickup) liftgateDelivery = false;
  }

  const stairs = bool(a.stairs ?? a.hasStairs, false);
  let stairFlights = Number(a.stair_flights ?? a.stairFlights ?? 0);
  if (!Number.isFinite(stairFlights) || stairFlights < 0) stairFlights = 0;
  stairFlights = Math.min(MAX_STAIR_FLIGHTS, Math.floor(stairFlights));
  const stairNotes = stairs ? str(a.stair_notes ?? a.stairNotes, 500) : "";
  if (!stairs) stairFlights = 0;

  return {
    residential,
    commercial: bool(a.commercial, !residential),
    dock,
    forklift,
    freight_elevator: bool(a.freight_elevator ?? a.hasFreightElevator, false),
    freight_elevator_notes: str(a.freight_elevator_notes, 500),
    stairs,
    stair_flights: stairFlights,
    stair_notes: stairNotes,
    needs_more_than_two_people: bool(
      a.needs_more_than_two_people ?? a.needsMoreThanTwoPeople,
      false
    ),
    tight_turns_or_narrow_halls: bool(
      a.tight_turns_or_narrow_halls ?? a.hasTightTurns,
      false
    ),
    gated_access: bool(a.gated_access, false),
    gate_code_or_instructions: str(a.gate_code_or_instructions, 500),
    inside_delivery: bool(
      a.inside_delivery ?? a.insideDeliveryRequested,
      whiteGlove
    ),
    room_placement: bool(a.room_placement, roomOfChoice || whiteGlove),
    unpacking_or_debris_removal: bool(
      a.unpacking_or_debris_removal ?? a.unpackingRequested,
      false
    ),
    disassembly_or_assembly: bool(a.disassembly_or_assembly, false),
    long_carry: bool(a.long_carry, false),
    parking_or_time_restrictions: bool(a.parking_or_time_restrictions, false),
    fragile_or_special_handling: bool(a.fragile_or_special_handling, false),
    white_glove: whiteGlove,
    room_of_choice: roomOfChoice,
    warehouse_pickup: warehousePickup,
    store_loading: storeLoading,
    no_liftgate: !liftgateDelivery,
    liftgate_pickup_staff_to_confirm: !isCompanyTruck,
    notes: str(a.notes ?? body.deliveryNotes, 2000),
    liftgate_pickup: liftgatePickup,
    liftgate_delivery: liftgateDelivery,
  };
}

/**
 * Accept Webflow freight calculator payload (snake_case) + legacy aliases.
 */
export function validateFreightQuoteRequest(body = {}, { requireItemsComplete = false } = {}) {
  // Honeypot | browsers often autofill "website". Never fake-success;
  // if the rest of the form looks real, ignore autofill and continue.
  const honeypot = str(body.company_website || body.website || body.url_honeypot, 200);
  const customerNameEarly = str(body.customer_name || body.customerName || body.name, 120);
  const customerEmailEarly = str(
    body.customer_email || body.customerEmail || body.email,
    200
  ).toLowerCase();
  const looksLikeRealCustomer =
    Boolean(customerNameEarly) && EMAIL_RE.test(customerEmailEarly) && parseItems(body.items).length > 0;
  if (honeypot && !looksLikeRealCustomer) {
    return { ok: false, error: "Submission rejected.", status: 400, honeypot: true };
  }
  if (honeypot && looksLikeRealCustomer) {
    console.warn("[freight-quote] ignoring autofilled honeypot for real-looking submit");
  }

  const customerName = customerNameEarly;
  const customerEmail = customerEmailEarly;
  const customerPhone = str(body.customer_phone || body.customerPhone || body.phone, 40);

  const addr =
    body.delivery_address && typeof body.delivery_address === "object" ? body.delivery_address : {};
  const street = str(addr.street || body.street || body.customerStreetAddress, 200);
  const unit = str(addr.unit || body.unit, 40);
  const city = str(addr.city || body.city || body.customerCity, 100);
  const state = str(addr.state || body.state || body.customerState, 40);
  const zip = str(addr.zip || body.zip || body.customerZip || body.postalCode, 20);
  const fullFromParts = [street, unit, city, state, zip].filter(Boolean).join(", ");
  const full = str(addr.full || body.full_address || fullFromParts, 400);

  const modeRaw = str(body.request_mode || body.mode || body.requestType || "please_quote", 40).toLowerCase();
  const requestMode =
    modeRaw === "estimate" || modeRaw === "get_estimate" || modeRaw === "self_estimate"
      ? "estimate"
      : "please_quote";

  let deliveryPath = str(body.delivery_path || body.deliveryPath || "", 40).toLowerCase();
  if (deliveryPath !== "local_az" && deliveryPath !== "pickup_az" && deliveryPath !== "nationwide") {
    deliveryPath = isArizonaStateOrZip({ state, zip }) ? "local_az" : "nationwide";
  }

  const destinationTypeRaw = str(
    body.destination_type || body.deliveryType || (body.access?.residential === false ? "commercial" : "residential"),
    40
  ).toLowerCase();
  const destinationType = destinationTypeRaw.startsWith("comm") ? "commercial" : "residential";

  if (!customerName) {
    return { ok: false, error: "Please enter your name.", status: 400 };
  }
  if (!customerEmail || !EMAIL_RE.test(customerEmail)) {
    return { ok: false, error: "Please enter a valid email address.", status: 400 };
  }
  if (!street || !city || !state || !zip) {
    return {
      ok: false,
      error: "Please enter the full delivery address (street, city, state, ZIP).",
      status: 400,
    };
  }

  if ((deliveryPath === "local_az" || deliveryPath === "pickup_az") && !isArizonaStateOrZip({ state, zip })) {
    return {
      ok: false,
      error:
        deliveryPath === "pickup_az"
          ? "Consignor pickup must be within Arizona. Out-of-state items need a different arrangement."
          : "This destination is outside Arizona. Please switch to nationwide freight for an out-of-state quote.",
      status: 400,
    };
  }

  let rawItems = parseItems(body.items);
  if (!rawItems.length) {
    for (let i = 1; i <= MAX_FREIGHT_ITEMS; i += 1) {
      const title = str(body[`item_${i}_title`], 300);
      if (!title) continue;
      rawItems.push({
        title,
        width: body[`item_${i}_width`],
        depth: body[`item_${i}_depth`],
        height: body[`item_${i}_height`],
        weight: body[`item_${i}_weight`],
        quantity: body[`item_${i}_quantity`] ?? 1,
        price: body[`item_${i}_price`],
        product_url: body[`item_${i}_url`],
        source: body[`item_${i}_source`] ?? "manual",
        freight_class: body[`item_${i}_freight_class`],
        non_stackable: body[`item_${i}_non_stackable`],
      });
    }
  }

  if (!rawItems.length) {
    return {
      ok: false,
      error: "Add at least one item (lookup by exact title or enter dimensions manually).",
      status: 400,
    };
  }
  if (rawItems.length > MAX_FREIGHT_ITEMS) {
    return {
      ok: false,
      error: `You can include at most ${MAX_FREIGHT_ITEMS} items per request.`,
      status: 400,
    };
  }

  const items = rawItems
    .map((it) => {
      const title = str(it.title || it.itemName || it.name, 300);
      if (!title) return null;
      return {
        source: str(it.source, 40) || "manual",
        title,
        width: numOrNull(it.width ?? it.widthIn),
        depth: numOrNull(it.depth ?? it.depthIn ?? it.length),
        height: numOrNull(it.height ?? it.heightIn),
        weight: numOrNull(it.weight ?? it.weightLb),
        quantity: numOrNull(it.quantity) || 1,
        price: it.price,
        product_url: str(it.product_url || it.productUrl, 500),
        image_url: str(it.image_url || it.imageUrl || it.image, 800),
        freight_class:
          it.freight_class === null || it.freight_class === ""
            ? null
            : it.freight_class !== undefined
              ? numOrNull(it.freight_class)
              : it.freightClass === null || it.freightClass === ""
                ? null
                : numOrNull(it.freightClass),
        non_stackable: it.non_stackable ?? it.nonStackable,
        // Keep Webflow summary pallet so emails match the on-page freight line.
        pallet: it.pallet && typeof it.pallet === "object" ? it.pallet : undefined,
      };
    })
    .filter(Boolean);

  if (!items.length) {
    return { ok: false, error: "Add at least one item with a title.", status: 400 };
  }

  const access = normalizeAccess({
    ...body,
    destination_type: destinationType,
    delivery_path: body.delivery_path || body.deliveryPath || "",
  });
  // Match Webflow: stairs need flights and/or a stair note.
  if (access.stairs && access.stair_flights < 1 && !access.stair_notes) {
    return {
      ok: false,
      error: "Please enter how many flights of stairs (1-20) or add a stair note.",
      status: 400,
    };
  }

  // Align with Webflow Part 2: trust payload dims + item.pallet; same SOP if pallet missing.
  const palletized = palletizeItems(items, {
    allowTitleDimFallback: false,
    preferClientPallet: true,
    useWebflowCalculator: true,
  });
  if (requireItemsComplete) {
    const incomplete = palletized.filter((r) => !r.ok);
    if (incomplete.length) {
      return {
        ok: false,
        error: `Item "${incomplete[0].title}" is missing ${incomplete[0].missing.join(", ")}.`,
        status: 400,
      };
    }
  }

  const originAddress = str(body.origin_address, 300) || SHOWROOM_ORIGIN;
  const pageUrl = str(body.page_url || body.pageUrl, 500);
  const clientSubmittedAt = str(body.submitted_at || body.submittedAt, 80);
  const idempotencyKey = str(body.idempotency_key || body.idempotencyKey, 120);

  return {
    ok: true,
    submission: {
      request_mode: requestMode,
      delivery_path: deliveryPath,
      customer_name: customerName,
      customer_email: customerEmail,
      customer_phone: customerPhone,
      destination_type: destinationType,
      delivery_address: { street, unit, city, state, zip, full },
      street,
      unit,
      city,
      state,
      zip,
      origin_address: originAddress,
      access,
      items: palletized,
      page_url: pageUrl,
      client_submitted_at: clientSubmittedAt,
      idempotency_key: idempotencyKey,
      multi_item_note:
        palletized.length > 1
          ? "Multiple items returned as separate freight entries. Staff may consolidate after review | do not assume one shared pallet."
          : "",
    },
  };
}
