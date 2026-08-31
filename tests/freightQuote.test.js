import test from "node:test";
import assert from "node:assert/strict";
import {
  palletizeItem,
  palletizeItems,
  calculateLocalRouteEstimate,
  applyLocalTruckDriveMinutes,
  inferFreightClass,
  shouldMarkNonStackable,
  parseSetCountFromTitle,
  describeStackConfirmNeed,
} from "../lib/freightPalletize.js";
import { validateFreightQuoteRequest } from "../lib/freightQuoteValidation.js";
import { buildManualReviewReasons } from "../lib/freightLocalEstimate.js";
import { isAllowedConsignmentOrigin } from "../lib/consignmentCors.js";
import {
  estimateNationwideRange,
  NATIONWIDE_FLOOR_USD,
} from "../lib/freightNationwideRate.js";

test("local pricing: 15 min = $95", () => {
  assert.equal(calculateLocalRouteEstimate(15).estimated_price, 95);
});

test("drive time adds 3-minute local truck pad", () => {
  assert.equal(applyLocalTruckDriveMinutes(15), 18);
  assert.equal(applyLocalTruckDriveMinutes(10), 13);
  assert.equal(applyLocalTruckDriveMinutes(8.2), 12);
  assert.equal(applyLocalTruckDriveMinutes(12), 15);
});

test("local pricing: 17 min = $100", () => {
  assert.equal(calculateLocalRouteEstimate(17).estimated_price, 100);
});

test("local pricing: 19 min = $105", () => {
  assert.equal(calculateLocalRouteEstimate(19).estimated_price, 105);
});

test("local pricing: 21 min = $110", () => {
  assert.equal(calculateLocalRouteEstimate(21).estimated_price, 110);
});

test("local pricing: 25 min = $115", () => {
  assert.equal(calculateLocalRouteEstimate(25).estimated_price, 115);
});

test("local pricing: 1-2 items add $0", () => {
  assert.equal(calculateLocalRouteEstimate(15, { itemCount: 1 }).multi_item_adder, 0);
  assert.equal(calculateLocalRouteEstimate(15, { itemCount: 2 }).estimated_price, 95);
});

test("local pricing: 3+ items without dims use $10 per item after first 2", () => {
  const est = calculateLocalRouteEstimate(15, { itemCount: 3 });
  assert.equal(est.multi_item_adder, 10);
  assert.equal(est.estimated_price, 105);
  assert.equal(calculateLocalRouteEstimate(15, { itemCount: 4 }).estimated_price, 115);
  assert.equal(calculateLocalRouteEstimate(15, { itemCount: 5 }).estimated_price, 125);
});

test("local pricing: small footprint items add $5 per item after first 2", () => {
  const small = { width: 20, depth: 20, height: 30, weight: 40, quantity: 1 };
  assert.equal(
    calculateLocalRouteEstimate(15, { items: [small, small, small] }).multi_item_adder,
    5
  );
  assert.equal(
    calculateLocalRouteEstimate(15, { items: [small, small, small] }).estimated_price,
    100
  );
  assert.equal(
    calculateLocalRouteEstimate(15, { items: [small, small, small, small] }).multi_item_adder,
    10
  );
});

test("local pricing: large footprint items add $10 per item after first 2", () => {
  const large = { width: 30, depth: 30, height: 30, weight: 80, quantity: 1 };
  assert.equal(
    calculateLocalRouteEstimate(15, { items: [large, large, large] }).multi_item_adder,
    10
  );
  assert.equal(
    calculateLocalRouteEstimate(15, {
      items: [
        { width: 20, depth: 20, height: 30, weight: 40, quantity: 1 },
        { width: 20, depth: 20, height: 30, weight: 40, quantity: 1 },
        { width: 20, depth: 20, height: 30, weight: 40, quantity: 1 },
        large,
      ],
    }).multi_item_adder,
    15
  );
});

test("local pricing: items over 60 in any dimension add $10 each", () => {
  const sofa = { width: 84, depth: 36, height: 34, weight: 120, quantity: 1 };
  const tableAt60 = { width: 60, depth: 40, height: 30, weight: 80, quantity: 1 };
  const armoire = { width: 40, depth: 24, height: 72, weight: 200, quantity: 1 };

  const soloSofa = calculateLocalRouteEstimate(15, { items: [sofa] });
  assert.equal(soloSofa.large_dim_adder, 10);
  assert.equal(soloSofa.large_dim_count, 1);
  assert.equal(soloSofa.estimated_price, 105);

  const at60 = calculateLocalRouteEstimate(15, { items: [tableAt60] });
  assert.equal(at60.large_dim_adder, 0);
  assert.equal(at60.estimated_price, 95);

  const tall = calculateLocalRouteEstimate(15, { items: [armoire] });
  assert.equal(tall.large_dim_adder, 10);
  assert.equal(tall.estimated_price, 105);

  const twoLarge = calculateLocalRouteEstimate(15, { items: [sofa, armoire] });
  assert.equal(twoLarge.large_dim_adder, 20);
  assert.equal(twoLarge.estimated_price, 115);
});

test("local pricing: items over 300 lb add $10 each", () => {
  const at300 = { width: 40, depth: 40, height: 40, weight: 300, quantity: 1 };
  const at301 = { width: 40, depth: 40, height: 40, weight: 301, quantity: 1 };
  const heavySafe = { width: 30, depth: 30, height: 40, weight: 450, quantity: 1 };

  assert.equal(calculateLocalRouteEstimate(15, { items: [at300] }).heavy_weight_adder, 0);
  assert.equal(calculateLocalRouteEstimate(15, { items: [at301] }).heavy_weight_adder, 10);
  assert.equal(calculateLocalRouteEstimate(15, { items: [at301] }).estimated_price, 105);

  const heavySafeEst = calculateLocalRouteEstimate(15, { items: [heavySafe] });
  assert.equal(heavySafeEst.heavy_weight_adder, 10);
  assert.equal(heavySafeEst.oversize_confirm, true);
  assert.equal(heavySafeEst.estimated_price, 140);
});

test("Webflow payload: trust client pallet + entered dims (no title invent)", () => {
  const rows = palletizeItems(
    [
      {
        title: "Dessin Fournir Round Dining Table- 60X30H (No Leaf)",
        width: 60,
        depth: 30,
        height: 30,
        weight: 149,
        quantity: 1,
        freight_class: null,
        non_stackable: false,
      },
    ],
    { allowTitleDimFallback: false, useWebflowCalculator: true }
  );
  assert.equal(rows[0].width, 60);
  assert.equal(rows[0].depth, 30);
  assert.equal(rows[0].height, 30);
  assert.equal(rows[0].weight, 149);
  assert.equal(rows[0].pallet.width, 60);
  assert.equal(rows[0].pallet.depth, 40);
  assert.equal(rows[0].pallet.height, 35);
  assert.equal(rows[0].pallet.weight, 179);
});

test("SOP small cabinet → 48x40x35 @ 85 lb (class null until confirmed)", () => {
  const r = palletizeItem({
    title: "Small cabinet",
    width: 24,
    depth: 19,
    height: 30,
    weight: 55,
  });
  assert.equal(r.ok, true);
  assert.deepEqual(
    { w: r.pallet.width, d: r.pallet.depth, h: r.pallet.height, wt: r.pallet.weight, c: r.pallet.freight_class },
    { w: 48, d: 40, h: 35, wt: 85, c: null }
  );
  assert.equal(r.pallet.suggested_freight_class, 150);
});

test("dining table title suggests class 175", () => {
  assert.equal(inferFreightClass({ title: "Dining table oak" }), 175);
});

test("standard cabinet title suggests class 150", () => {
  assert.equal(inferFreightClass({ title: "Small cabinet" }), 150);
});

test("oversized width rounds width only", () => {
  const r = palletizeItem({ title: "Wood desk", width: 55.6, depth: 29, height: 31, weight: 85 });
  assert.equal(r.pallet.width, 60);
  assert.equal(r.pallet.depth, 40);
  assert.equal(r.pallet.height, 36);
  assert.equal(r.pallet.weight, 115);
});

test("oversized depth rounds depth only to next 5", () => {
  const r = palletizeItem({ title: "Deep piece", width: 40, depth: 45.2, height: 30, weight: 50 });
  assert.equal(r.pallet.width, 48);
  assert.equal(r.pallet.depth, 50);
});

test("null freight class stays null (Not sure)", () => {
  const r = palletizeItem({
    title: "Small cabinet",
    width: 24,
    depth: 19,
    height: 30,
    weight: 55,
    freight_class: null,
  });
  assert.equal(r.pallet.freight_class, null);
  assert.equal(r.pallet.suggested_freight_class, 150);
});

test("dining table with explicit class 175", () => {
  const r = palletizeItem({
    title: "Dining table",
    width: 72,
    depth: 42,
    height: 30,
    weight: 120,
    freight_class: 175,
  });
  assert.equal(r.pallet.freight_class, 175);
  assert.equal(r.pallet.width, 75);
  assert.equal(r.pallet.depth, 45);
});

test("set of 4 swivel chairs: qty 1, total weight, stacked (not 4× tall)", () => {
  const r = palletizeItem({
    title: "Mitchell Gold + Bob Williams Poppy Swivel Dining Chairs- Set of 4- 26X23X30H",
    width: 26,
    depth: 23,
    height: 30,
    weight: 172,
    quantity: 1,
    freight_class: null,
  });
  assert.equal(r.ok, true);
  assert.equal(r.quantity, 1);
  assert.equal(r.set_count, 4);
  assert.equal(r.product.dims_are, "per_piece");
  assert.equal(r.product.weight_is, "total_for_set");
  // Swivel chairs stack with padding (not 4× piece height)
  assert.equal(r.packing.packing_mode, "nested_stack");
  assert.equal(r.packing.stacked_height_in, 54); // 30 + 3*8
  assert.equal(r.pallet.height, 59); // +5" pallet board
  assert.equal(r.pallet.weight, 202); // 172 + 30, NOT 172*4
  assert.equal(r.pallet.width, 48);
  assert.equal(r.pallet.depth, 40);
});

test("set of 6 nestable chairs uses nested stack height", () => {
  const r = palletizeItem({
    title: "Dining Chairs Set of 6 - 20X20X36H",
    width: 20,
    depth: 20,
    height: 36,
    weight: 108,
    quantity: 1,
  });
  assert.equal(r.set_count, 6);
  assert.equal(r.packing.packing_mode, "nested_stack");
  assert.ok(r.packing.stacked_height_in > 36);
  assert.equal(r.pallet.weight, 138); // 108+30
});

test("parseSetCountFromTitle", () => {
  assert.equal(parseSetCountFromTitle("Chairs- Set of 4- 26X23X30H"), 4);
  assert.equal(parseSetCountFromTitle("Pair of lamps"), 2);
  assert.equal(parseSetCountFromTitle("Single sofa"), 1);
});

test("stack confirm: set of 4 with qty 1 is not customer qty 2+", () => {
  const setListing = describeStackConfirmNeed({
    title: "Baker Furniture McGuire Passage Swivel Counter Stool- Set of 4",
    quantity: 1,
    set_count: 4,
    pallet: { stack_confirm_required: true, set_count: 4 },
  });
  assert.equal(setListing.kind, "set_listing");
  assert.equal(setListing.quantity, 1);
  assert.equal(setListing.set_count, 4);
  assert.equal(setListing.claim, null);

  const customerQty = describeStackConfirmNeed({
    title: "Recliner",
    quantity: 2,
    flip_stack_claimed: "yes",
  });
  assert.equal(customerQty.kind, "customer_qty");
  assert.equal(customerQty.claim, "yes");
});

test("out-of-state as local_az rejected", () => {
  const v = validateFreightQuoteRequest({
    request_mode: "estimate",
    delivery_path: "local_az",
    customer_name: "Test",
    customer_email: "a@b.com",
    street: "1 Main",
    city: "Austin",
    state: "TX",
    zip: "78701",
    access: {
      residential: true,
      dock: false,
      forklift: false,
      freight_elevator: false,
      stairs: false,
      needs_more_than_two_people: false,
      tight_turns_or_narrow_halls: false,
    },
    items: [{ title: "Desk", width: 48, depth: 24, height: 30, weight: 80 }],
  });
  assert.equal(v.ok, false);
  assert.match(v.error, /nationwide freight/i);
});

test("nationwide residential payload accepted", () => {
  const v = validateFreightQuoteRequest({
    request_mode: "please_quote",
    delivery_path: "nationwide",
    customer_name: "Test",
    customer_email: "a@b.com",
    customer_phone: "4805551212",
    destination_type: "residential",
    delivery_address: {
      street: "1 Main",
      city: "Denver",
      state: "CO",
      zip: "80202",
      full: "1 Main, Denver, CO 80202",
    },
    access: {
      residential: true,
      commercial: false,
      dock: false,
      forklift: false,
      freight_elevator: false,
      stairs: true,
      stair_flights: 1,
      needs_more_than_two_people: false,
      tight_turns_or_narrow_halls: true,
      liftgate_pickup: true,
      liftgate_delivery: true,
    },
    items: [
      {
        source: "manual",
        title: "Dining table",
        width: 72,
        depth: 42,
        height: 30,
        weight: 120,
        quantity: 1,
        freight_class: 175,
      },
    ],
  });
  assert.equal(v.ok, true);
  assert.equal(v.submission.delivery_path, "nationwide");
  assert.equal(v.submission.access.liftgate_pickup, true);
  assert.equal(v.submission.items[0].pallet.freight_class, 175);
  assert.equal(v.submission.items[0].pallet.width, 75);
  assert.equal(v.submission.items[0].pallet.depth, 45);
});

test("commercial dock clears default liftgate delivery unless forced", () => {
  const v = validateFreightQuoteRequest({
    request_mode: "please_quote",
    delivery_path: "nationwide",
    customer_name: "Biz",
    customer_email: "ops@example.com",
    street: "100 Industrial",
    city: "Dallas",
    state: "TX",
    zip: "75201",
    destination_type: "commercial",
    access: {
      residential: false,
      commercial: true,
      dock: true,
      forklift: true,
      freight_elevator: false,
      stairs: false,
      needs_more_than_two_people: false,
      tight_turns_or_narrow_halls: false,
    },
    items: [{ title: "Cabinet", width: 30, depth: 20, height: 40, weight: 70 }],
  });
  assert.equal(v.ok, true);
  assert.equal(v.submission.access.liftgate_pickup, true);
  assert.equal(v.submission.access.liftgate_delivery, false);
});

test("stairs flights review reason", () => {
  const reasons = buildManualReviewReasons({
    stairs: true,
    stair_flights: 2,
    needs_more_than_two_people: true,
    extra_people: 1,
    tight_turns_or_narrow_halls: true,
  });
  assert.ok(reasons.some((r) => /2 flights/i.test(r)));
  assert.ok(reasons.some((r) => /1 extra person/i.test(r)));
});

test("qty 2 recliner flip-stack claim uses one pallet + small height add", () => {
  const stacked = palletizeItem({
    title: "La-Z-Boy Talladega Wall Recliner - 38X32X46H",
    width: 38,
    depth: 32,
    height: 46,
    weight: 95,
    quantity: 2,
    flip_stack_claimed: "yes",
  });
  assert.equal(stacked.ok, true);
  assert.equal(stacked.pallet.weight, 220);
  assert.equal(stacked.pallet.width, 48);
  assert.equal(stacked.pallet.depth, 40);
  assert.equal(stacked.pallet.height, 54); // 46 + 3 + 5 pallet
  assert.equal(stacked.pallet.stack_confirm_required, true);
  assert.match(stacked.pallet.packing_notes.join(" "), /TEAM MUST CONFIRM/i);

  const layered = palletizeItem({
    title: "La-Z-Boy Talladega Wall Recliner - 38X32X46H",
    width: 38,
    depth: 32,
    height: 46,
    weight: 95,
    quantity: 2,
    flip_stack_claimed: "no",
  });
  assert.equal(layered.ok, true);
  assert.equal(layered.pallet.weight, 220);
  assert.ok(layered.pallet.height >= 46 * 2);
});

test("qty 2 requires flip_stack_claimed on validate", () => {
  const missing = validateFreightQuoteRequest({
    customer_name: "Test User",
    customer_email: "test@example.com",
    customer_phone: "4805551212",
    street: "7167 E Rancho Vista Dr",
    city: "Scottsdale",
    state: "AZ",
    zip: "85251",
    delivery_path: "nationwide",
    request_mode: "estimate",
    access: { stairs: false, needs_more_than_two_people: false },
    items: [
      {
        title: "La-Z-Boy Talladega Wall Recliner",
        width: 38,
        depth: 32,
        height: 46,
        weight: 95,
        quantity: 2,
      },
    ],
  });
  assert.equal(missing.ok, false);
  assert.match(String(missing.error || ""), /flipped\/stacked|flip\/stack/i);

  const localOk = validateFreightQuoteRequest({
    customer_name: "Test User",
    customer_email: "test@example.com",
    customer_phone: "4805551212",
    street: "7167 E Rancho Vista Dr",
    city: "Scottsdale",
    state: "AZ",
    zip: "85251",
    delivery_path: "local_az",
    request_mode: "estimate",
    access: { stairs: false, needs_more_than_two_people: false, assembly_required: "no" },
    items: [
      {
        title: "La-Z-Boy Talladega Wall Recliner",
        width: 38,
        depth: 32,
        height: 46,
        weight: 95,
        quantity: 2,
      },
    ],
  });
  assert.equal(localOk.ok, true);
  assert.equal(localOk.submission.items[0].flip_stack_claimed, "");
});

test("local multi-item adder uses total quantity across lines", () => {
  const small = { width: 20, depth: 20, height: 30, weight: 40, quantity: 1 };
  assert.equal(calculateLocalRouteEstimate(15, { itemCount: 2 }).estimated_price, 95);
  assert.equal(
    calculateLocalRouteEstimate(15, { items: [small, small, small] }).estimated_price,
    100
  );
});

test("local pricing: 1 extra person uses $130/hr", () => {
  const est = calculateLocalRouteEstimate(15, {
    access: { needs_more_than_two_people: true, extra_people: 1 },
  });
  assert.equal(est.hourly_rate, 130);
  assert.equal(est.estimated_price, 130);
});

test("local pricing: 2 extra people doubles estimate", () => {
  const est = calculateLocalRouteEstimate(15, {
    access: { needs_more_than_two_people: true, extra_people: 2 },
  });
  assert.equal(est.truck_multiplier, 2);
  assert.equal(est.estimated_price, 190);
});

test("local pricing: assembly beyond 15 min adds rounded fee", () => {
  const none = calculateLocalRouteEstimate(15, {
    items: [{ title: "Desk", assembly_required: "no" }],
  });
  assert.equal(none.assembly_fee, 0);
  assert.equal(none.estimated_price, 95);

  const within = calculateLocalRouteEstimate(15, {
    items: [{ title: "Desk", assembly_required: "yes", assembly_over_15: "no" }],
  });
  assert.equal(within.assembly_fee, 0);
  assert.equal(within.assembly_required, true);
  assert.equal(within.estimated_price, 95);

  // 20 extra min beyond included 15 @ $95/hr = 20 × (95/60) ≈ $31.67 → $35
  const billed = calculateLocalRouteEstimate(15, {
    items: [
      {
        title: "Armoire",
        assembly_required: "yes",
        assembly_over_15: "yes",
        assembly_extra_minutes: 20,
      },
    ],
  });
  assert.equal(billed.assembly_extra_minutes, 20);
  assert.equal(billed.assembly_fee, 35);
  assert.equal(billed.estimated_price, 130);
  assert.equal(billed.assembly_confirm_over_60, false);

  const idk = calculateLocalRouteEstimate(15, {
    items: [{ title: "Desk", assembly_required: "idk" }],
  });
  assert.equal(idk.assembly_fee, 0);
  assert.equal(idk.assembly_required, false);
  assert.equal(idk.estimated_price, 95);

  const overIdk = calculateLocalRouteEstimate(15, {
    items: [
      {
        title: "Desk",
        assembly_required: "yes",
        assembly_over_15: "idk",
      },
    ],
  });
  assert.equal(overIdk.assembly_fee, 0);
  assert.equal(overIdk.assembly_required, true);

  // Also works from access-level answers (form Labor section).
  const fromAccess = calculateLocalRouteEstimate(15, {
    access: {
      assembly_required: "yes",
      assembly_over_15: "yes",
      assembly_extra_minutes: 20,
    },
  });
  assert.equal(fromAccess.assembly_fee, 35);
  assert.equal(fromAccess.estimated_price, 130);

  const over60 = calculateLocalRouteEstimate(15, {
    items: [
      {
        title: "Wall unit",
        assembly_required: "yes",
        assembly_over_15: "yes",
        assembly_extra_minutes: 75,
      },
    ],
  });
  assert.equal(over60.assembly_confirm_over_60, true);
  assert.ok(over60.assembly_fee > 0);
});

test("local/pickup assembly question is optional; follow-ups required when Yes", () => {
  const unanswered = validateFreightQuoteRequest({
    delivery_path: "local_az",
    customer_name: "Test User",
    customer_email: "test@example.com",
    customer_phone: "4805551212",
    street: "1 Main St",
    city: "Scottsdale",
    state: "AZ",
    zip: "85260",
    destination_type: "Residential",
    items: [
      {
        title: "Table",
        width: 40,
        depth: 20,
        height: 30,
        weight: 50,
        quantity: 1,
      },
    ],
  });
  assert.equal(unanswered.ok, true);

  const nameWeightOnly = validateFreightQuoteRequest({
    delivery_path: "local_az",
    customer_name: "Test User",
    customer_email: "test@example.com",
    customer_phone: "4805551212",
    street: "1 Main St",
    city: "Scottsdale",
    state: "AZ",
    zip: "85260",
    destination_type: "Residential",
    items: [{ title: "Sofa", weight: 120, quantity: 1 }],
  });
  assert.equal(nameWeightOnly.ok, true);

  const missingWeight = validateFreightQuoteRequest({
    delivery_path: "local_az",
    customer_name: "Test User",
    customer_email: "test@example.com",
    customer_phone: "4805551212",
    street: "1 Main St",
    city: "Scottsdale",
    state: "AZ",
    zip: "85260",
    destination_type: "Residential",
    items: [{ title: "Sofa", quantity: 1 }],
  });
  assert.equal(missingWeight.ok, false);
  assert.match(String(missingWeight.error || ""), /weight/i);

  const missingFollowUp = validateFreightQuoteRequest({
    delivery_path: "local_az",
    customer_name: "Test User",
    customer_email: "test@example.com",
    customer_phone: "4805551212",
    street: "1 Main St",
    city: "Scottsdale",
    state: "AZ",
    zip: "85260",
    destination_type: "Residential",
    access: { assembly_required: "yes" },
    items: [{ title: "Table", weight: 50, quantity: 1 }],
  });
  assert.equal(missingFollowUp.ok, false);
  assert.match(String(missingFollowUp.error || ""), /15 minutes/i);

  const ok = validateFreightQuoteRequest({
    delivery_path: "local_az",
    customer_name: "Test User",
    customer_email: "test@example.com",
    customer_phone: "4805551212",
    street: "1 Main St",
    city: "Scottsdale",
    state: "AZ",
    zip: "85260",
    destination_type: "Residential",
    access: {
      assembly_required: "yes",
      assembly_over_15: "yes",
      assembly_extra_minutes: 20,
    },
    items: [
      {
        title: "Table",
        width: 40,
        depth: 20,
        height: 30,
        weight: 50,
        quantity: 1,
      },
    ],
  });
  assert.equal(ok.ok, true);
  assert.equal(ok.submission.access.assembly_required, "yes");
  assert.equal(ok.submission.access.assembly_extra_minutes, 20);
  assert.equal(ok.submission.access.disassembly_or_assembly, true);

  const idkOk = validateFreightQuoteRequest({
    delivery_path: "local_az",
    customer_name: "Test User",
    customer_email: "test@example.com",
    customer_phone: "4805551212",
    street: "1 Main St",
    city: "Scottsdale",
    state: "AZ",
    zip: "85260",
    destination_type: "Residential",
    access: { assembly_required: "idk" },
    items: [
      {
        title: "Table",
        width: 40,
        depth: 20,
        height: 30,
        weight: 50,
        quantity: 1,
      },
    ],
  });
  assert.equal(idkOk.ok, true);
  assert.equal(idkOk.submission.access.assembly_required, "idk");
  assert.equal(idkOk.submission.access.assembly_extra_minutes, 0);
  assert.equal(idkOk.submission.access.disassembly_or_assembly, false);
});

test("local pricing: stairs first flight free then $7", () => {
  const one = calculateLocalRouteEstimate(15, {
    access: { stairs: true, stair_flights: 1 },
  });
  assert.equal(one.stair_fee, 0);
  assert.equal(one.estimated_price, 95);

  const two = calculateLocalRouteEstimate(15, {
    access: { stairs: true, stair_flights: 2 },
  });
  assert.equal(two.stair_fee, 7);
  assert.equal(two.estimated_price, 102);

  const three = calculateLocalRouteEstimate(15, {
    access: { stairs: true, stair_flights: 3 },
  });
  assert.equal(three.stair_fee, 14);
  assert.equal(three.estimated_price, 109);
});

test("local pricing: round trip over 100 miles still bills at full $95/hr", () => {
  const est = calculateLocalRouteEstimate(15, { distanceMiles: 55 });
  assert.equal(est.long_haul, true);
  assert.equal(est.hourly_rate, 95);
  assert.equal(est.estimated_price, 95);
});

test("local pricing: oversize needs 299+ lb and over 72 H, or 450+ lb", () => {
  const both = calculateLocalRouteEstimate(15, {
    items: [{ title: "Huge armoire", width: 40, height: 80, weight: 300 }],
  });
  assert.equal(both.oversize_confirm, true);
  assert.equal(both.hourly_rate, 130);
  assert.equal(both.large_dim_adder, 10);
  assert.equal(both.estimated_price, 140);

  const tallOnly = calculateLocalRouteEstimate(15, {
    items: [{ title: "Tall cabinet", width: 56, height: 92, weight: 200 }],
  });
  assert.equal(tallOnly.oversize_confirm, false);
  assert.equal(tallOnly.hourly_rate, 95);
  assert.equal(tallOnly.large_dim_adder, 10);
  assert.equal(tallOnly.estimated_price, 105);

  const heavyOnly = calculateLocalRouteEstimate(15, {
    items: [{ title: "Heavy chest", width: 40, height: 40, weight: 300 }],
  });
  assert.equal(heavyOnly.oversize_confirm, false);
  assert.equal(heavyOnly.hourly_rate, 95);

  const wideCouch = calculateLocalRouteEstimate(15, {
    items: [{ title: "Sofa", width: 90, height: 34, weight: 180 }],
  });
  assert.equal(wideCouch.oversize_confirm, false);
  assert.equal(wideCouch.hourly_rate, 95);
  assert.equal(wideCouch.large_dim_adder, 10);
  assert.equal(wideCouch.estimated_price, 105);

  const at450 = calculateLocalRouteEstimate(15, {
    items: [{ title: "Safe", width: 30, height: 40, weight: 450 }],
  });
  assert.equal(at450.oversize_confirm, true);
  assert.equal(at450.hourly_rate, 130);
  assert.equal(at450.heavy_weight_adder, 10);
  assert.equal(at450.estimated_price, 140);

  const veryHeavy = calculateLocalRouteEstimate(15, {
    items: [{ title: "Safe", width: 30, height: 40, weight: 551 }],
  });
  assert.equal(veryHeavy.oversize_confirm, true);
  assert.equal(veryHeavy.hourly_rate, 130);
  assert.equal(veryHeavy.heavy_weight_adder, 10);
  assert.equal(veryHeavy.estimated_price, 140);
});

test("local validation requires extra_people when more than two selected", () => {
  const v = validateFreightQuoteRequest({
    customer_name: "Test User",
    customer_email: "test@example.com",
    customer_phone: "4805551212",
    street: "7167 E Rancho Vista Dr",
    city: "Scottsdale",
    state: "AZ",
    zip: "85251",
    delivery_path: "local_az",
    request_mode: "estimate",
    access: {
      stairs: false,
      needs_more_than_two_people: true,
      more_than_two_people_reason: "heavy",
      assembly_required: "no",
    },
    items: [{ title: "Cabinet", width: 30, depth: 20, height: 40, weight: 70 }],
  });
  assert.equal(v.ok, false);
  assert.match(String(v.error || ""), /1 extra person|2 extra people/i);
});

test("honeypot alone rejected; autofill ignored for real form", () => {
  const spamOnly = validateFreightQuoteRequest({
    company_website: "http://spam.test",
  });
  assert.equal(spamOnly.ok, false);
  assert.equal(spamOnly.honeypot, true);

  const autofill = validateFreightQuoteRequest({
    company_website: "http://autofill.example",
    customer_name: "Bernis Berber",
    customer_email: "bernis.berber@icloud.com",
    street: "7167 E Rancho Vista Dr",
    city: "Scottsdale",
    state: "AZ",
    zip: "85251",
    delivery_path: "local_az",
    request_mode: "estimate",
    access: { assembly_required: "no" },
    items: [
      {
        title: "Table",
        width: 60,
        depth: 30,
        height: 30,
        weight: 149,
        pallet: { width: 60, depth: 40, height: 35, weight: 179 },
      },
    ],
  });
  assert.equal(autofill.ok, true);
  assert.equal(autofill.submission.items[0].height, 30);
  assert.equal(autofill.submission.items[0].pallet.height, 35);
});

test("invalid email rejected", () => {
  const v = validateFreightQuoteRequest({
    customer_name: "T",
    customer_email: "not-an-email",
    street: "1",
    city: "Phoenix",
    state: "AZ",
    zip: "85001",
    items: [{ title: "X", width: 10, depth: 10, height: 10, weight: 10 }],
  });
  assert.equal(v.ok, false);
});

test("multiple items stay separate pallet entries", () => {
  const v = validateFreightQuoteRequest({
    customer_name: "T",
    customer_email: "t@x.com",
    street: "1",
    city: "Phoenix",
    state: "AZ",
    zip: "85001",
    delivery_path: "local_az",
    access: {
      residential: true,
      dock: false,
      forklift: false,
      freight_elevator: false,
      stairs: false,
      needs_more_than_two_people: false,
      tight_turns_or_narrow_halls: false,
      assembly_required: "no",
    },
    items: [
      { title: "A", width: 24, depth: 20, height: 30, weight: 40 },
      { title: "B", width: 24, depth: 20, height: 30, weight: 40 },
    ],
  });
  assert.equal(v.ok, true);
  assert.equal(v.submission.items.length, 2);
  assert.ok(v.submission.multi_item_note);
});

test("CORS allows production + webflow.io + localhost", () => {
  assert.equal(isAllowedConsignmentOrigin("https://www.lostandfoundresale.com"), true);
  assert.equal(isAllowedConsignmentOrigin("https://lostandfoundresale.com"), true);
  assert.equal(isAllowedConsignmentOrigin("https://lf-freight.webflow.io"), true);
  assert.equal(isAllowedConsignmentOrigin("http://localhost:3000"), true);
  assert.equal(isAllowedConsignmentOrigin("https://evil.example"), false);
});

test("nationwide range never below $350", () => {
  const short = estimateNationwideRange({
    miles: 50,
    items: [{ weight: 40, pallet: { weight: 70 } }],
    access: { residential: true, liftgate_delivery: true },
  });
  assert.equal(short.status, "estimated_range");
  assert.ok(short.range_low >= NATIONWIDE_FLOOR_USD);
  assert.ok(short.range_high >= short.range_low);
});

test("nationwide white-glove access pushes high end up", () => {
  const base = estimateNationwideRange({
    miles: 1800,
    items: [{ weight: 172, pallet: { weight: 202 } }],
    access: { residential: true, liftgate_delivery: true },
  });
  const glove = estimateNationwideRange({
    miles: 1800,
    items: [{ weight: 172, pallet: { weight: 202 } }],
    access: {
      residential: true,
      liftgate_delivery: true,
      inside_delivery: true,
      room_placement: true,
      unpacking_or_debris_removal: true,
    },
  });
  assert.ok(glove.range_high > base.range_high);
  assert.equal(glove.white_glove_likely, true);
  assert.ok(glove.range_low >= NATIONWIDE_FLOOR_USD);
});

test("consignor pickup_az accepted for Arizona address", () => {
  const v = validateFreightQuoteRequest({
    customer_name: "Consignor",
    customer_email: "c@x.com",
    street: "123 Main St",
    city: "Phoenix",
    state: "AZ",
    zip: "85001",
    delivery_path: "pickup_az",
    access: {
      residential: true,
      dock: false,
      forklift: false,
      freight_elevator: false,
      stairs: false,
      needs_more_than_two_people: false,
      tight_turns_or_narrow_halls: false,
      assembly_required: "no",
    },
    items: [{ title: "Console", width: 48, depth: 18, height: 30, weight: 80 }],
  });
  assert.equal(v.ok, true);
  assert.equal(v.submission.delivery_path, "pickup_az");
});

test("local delivery and pickup share the same surcharge rules", () => {
  const sofa = { width: 84, depth: 36, height: 34, weight: 320, quantity: 1 };
  const est = calculateLocalRouteEstimate(15, { items: [sofa] });
  assert.equal(est.multi_item_adder, 0);
  assert.equal(est.large_dim_adder, 10);
  assert.equal(est.heavy_weight_adder, 10);
  assert.equal(est.estimated_price, 115);
});

test("consignor pickup_az rejected outside Arizona", () => {
  const v = validateFreightQuoteRequest({
    customer_name: "Consignor",
    customer_email: "c@x.com",
    street: "1 Main",
    city: "Denver",
    state: "CO",
    zip: "80202",
    delivery_path: "pickup_az",
    access: {
      residential: true,
      dock: false,
      forklift: false,
      freight_elevator: false,
      stairs: false,
      needs_more_than_two_people: false,
      tight_turns_or_narrow_halls: false,
    },
    items: [{ title: "Table", width: 40, depth: 20, height: 30, weight: 50 }],
  });
  assert.equal(v.ok, false);
  assert.match(v.error, /Arizona/i);
});
