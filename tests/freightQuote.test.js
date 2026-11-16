import test from "node:test";
import assert from "node:assert/strict";
import zlib from "node:zlib";
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
import {
  buildFreightQuoteEmails,
  enrichSubmissionItemMedia,
} from "../lib/freightQuoteEmail.js";
import { absoluteHttpsImageUrl, SHOP_ALL_URL } from "../lib/listingTitleDisplay.js";
import { generateLocalSignPdf } from "../lib/freightLocalSignPdf.js";
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

function localDeliveryBody(extra = {}) {
  return {
    customer_name: "Bernis Berber",
    customer_email: "bernis.berber@icloud.com",
    customer_phone: "4807727004",
    street: "7167 E Rancho Vista Dr",
    unit: "#1005",
    city: "Scottsdale",
    state: "AZ",
    zip: "85251",
    delivery_path: "local_az",
    request_mode: "estimate",
    destination_type: "residential",
    access: {
      stairs: false,
      needs_more_than_two_people: false,
      assembly_required: "no",
      preferred_days_windows: "Tuesday or Wednesday, 10am-2pm or 1pm-5pm",
      ...(extra.access || {}),
    },
    items: [{ title: "Chair", width: 20, depth: 20, height: 30, weight: 40, quantity: 1 }],
    ...extra,
    access: {
      stairs: false,
      needs_more_than_two_people: false,
      assembly_required: "no",
      preferred_days_windows: "Tuesday or Wednesday, 10am-2pm or 1pm-5pm",
      ...(extra.access || {}),
    },
  };
}

test("delivery partner accepts only Cruz Pro Line", () => {
  const selected = validateFreightQuoteRequest(
    localDeliveryBody({ access: { delivery_partner: "cruz_pro_line" } })
  );
  assert.equal(selected.ok, true);
  assert.equal(selected.submission.access.delivery_partner, "cruz_pro_line");

  const spaced = validateFreightQuoteRequest(
    localDeliveryBody({ access: { delivery_partner: "Cruz Pro Line" } })
  );
  assert.equal(spaced.submission.access.delivery_partner, "cruz_pro_line");

  const other = validateFreightQuoteRequest(
    localDeliveryBody({ access: { delivery_partner: "Some Other Movers" } })
  );
  assert.equal(other.ok, true);
  assert.equal(other.submission.access.delivery_partner, "");

  const blank = validateFreightQuoteRequest(localDeliveryBody());
  assert.equal(blank.submission.access.delivery_partner, "");

  const nationwide = validateFreightQuoteRequest({
    ...localDeliveryBody({ access: { delivery_partner: "cruz_pro_line" } }),
    delivery_path: "nationwide",
    state: "CA",
    city: "Los Angeles",
    zip: "90001",
    access: { delivery_partner: "cruz_pro_line", liftgate_delivery: true },
    items: [{ title: "Chair", width: 20, depth: 20, height: 30, weight: 40, quantity: 1 }],
  });
  assert.equal(nationwide.ok, true);
  assert.equal(nationwide.submission.access.delivery_partner, "");
});

test("internal call out shows Cruz Pro Line only when selected", () => {
  const ctx = {
    requestId: "test-1",
    route: { drive_minutes: 23, distance_miles: 8.9 },
    localEstimate: { estimated_price: 110, drive_minutes: 23, distance_miles: 8.9 },
  };
  const off = validateFreightQuoteRequest(localDeliveryBody());
  const offMail = buildFreightQuoteEmails(off.submission, ctx);
  assert.match(offMail.internal.html, /Call out items/);
  assert.match(offMail.internal.html, /Preferred days \/ windows/);
  assert.doesNotMatch(offMail.internal.html, /480-376-9485/);
  assert.doesNotMatch(offMail.internal.text, /480-376-9485/);

  const on = validateFreightQuoteRequest(
    localDeliveryBody({ access: { delivery_partner: "cruz_pro_line" } })
  );
  const onMail = buildFreightQuoteEmails(on.submission, ctx);
  const callOut = onMail.internal.html.split("Call out items")[1] || "";
  assert.match(callOut, /Cruz Pro Line/);
  assert.match(callOut, /480-376-9485/);
  assert.match(onMail.internal.text, /Delivery partner: Cruz Pro Line · 480-376-9485/);
  const prefAt = callOut.indexOf("Preferred days / windows");
  const partnerAt = callOut.indexOf("Delivery partner");
  assert.ok(prefAt >= 0 && partnerAt > prefAt);
});

function pdfOps(buf) {
  const raw = buf.toString("latin1");
  const parts = [];
  const re = /\/Filter \/FlateDecode\s*>>\s*stream\r?\n([\s\S]*?)\nendstream/g;
  let match;
  while ((match = re.exec(raw))) {
    parts.push(zlib.inflateSync(Buffer.from(match[1], "latin1")).toString("latin1"));
  }
  const content = parts.join("\n");
  const ops = [];
  const opRe = /1 0 0 1 ([\d.]+) ([\d.]+) Tm\n[\s\S]*?\[([\s\S]*?)\]\s*TJ/g;
  while ((match = opRe.exec(content))) {
    let text = "";
    const hexRe = /<([0-9A-Fa-f]+)>/g;
    let hex;
    while ((hex = hexRe.exec(match[3]))) {
      for (let i = 0; i < hex[1].length; i += 2) {
        text += String.fromCharCode(parseInt(hex[1].slice(i, i + 2), 16));
      }
    }
    ops.push({ x: Number(match[1]), y: Number(match[2]), text });
  }
  return ops;
}

function pdfFind(ops, needle) {
  return ops.find((op) => op.text.includes(needle)) || null;
}

test("sign PDF fills delivery company and keeps preferred days in their own field", async () => {
  const ctx = {
    requestId: "test-sign",
    route: { drive_minutes: 23, distance_miles: 8.9 },
    localEstimate: { estimated_price: 110, drive_minutes: 23, distance_miles: 8.9 },
  };
  const longDays =
    "Tuesday or Wednesday, 10am-2pm or 1pm-5pm. Thursday morning 9am-1pm also works if the first two days are full.";
  const blank = validateFreightQuoteRequest(
    localDeliveryBody({ access: { preferred_days_windows: longDays } })
  );
  const blankOps = pdfOps(await generateLocalSignPdf(blank.submission, ctx));
  assert.equal(blankOps.some((op) => op.text.includes("480-376-9485")), false);
  assert.ok(pdfFind(blankOps, "DELIVERY COMPANY"));
  assert.ok(pdfFind(blankOps, "is for delivery only"));
  assert.ok(pdfFind(blankOps, "Tuesday or Wednesday"));
  assert.ok(pdfFind(blankOps, "Thursday morning"));

  const selected = validateFreightQuoteRequest(
    localDeliveryBody({
      access: { delivery_partner: "cruz_pro_line", preferred_days_windows: longDays },
    })
  );
  const ops = pdfOps(await generateLocalSignPdf(selected.submission, ctx));
  const company = pdfFind(ops, "Cruz Pro Line");
  const phone = pdfFind(ops, "480-376-9485");
  const label = pdfFind(ops, "DELIVERY COMPANY");
  const rateLabel = pdfFind(ops, "CONFIRMED RATE");
  const note = pdfFind(ops, "is for delivery only");
  const banner = pdfFind(ops, "Local Arizona delivery");
  const days = pdfFind(ops, "Thursday morning");
  const items = pdfFind(ops, "ITEMS");
  assert.ok(company && phone && label && rateLabel && note && banner && days && items);
  assert.match(banner.text, /Estimate \$110/);
  assert.match(banner.text, /\(\$95\/hr\)/);
  assert.ok(label.y > company.y, "DELIVERY COMPANY label sits above the company name");
  assert.ok(company.y > phone.y, "phone sits under the company name");
  assert.ok(phone.y - note.y > 6, "rate note sits below the phone on the left");
  assert.ok(
    company.x < 140 && phone.x < 140 && label.x < 140 && note.x < 140,
    "company, phone, and the $95/hr note stay in the left column"
  );
  assert.ok(rateLabel.x > 250, "CONFIRMED RATE stays in the right column");
  assert.ok(company.x < rateLabel.x - 80, "company name is not in the rate column");
  const confirmedRate = ops.find((op) => op.text.trim() === "$95/hr");
  assert.ok(confirmedRate, "CONFIRMED RATE value is $95/hr");
  assert.ok(confirmedRate.x > 250, "$95/hr stays in the right column");
  assert.ok(confirmedRate.y - rateLabel.y > 8, "$95/hr sits above CONFIRMED RATE");
  assert.ok(confirmedRate.y > note.y, "$95/hr stays above the left delivery-only note");
  const customerPhone = pdfFind(ops, "(480) 772-7004");
  assert.ok(customerPhone, "customer phone stays in the PHONE field");
  assert.notEqual(customerPhone.text.trim(), "$95/hr", "$95/hr is not printed in the PHONE field");
  assert.ok(confirmedRate.y > customerPhone.y, "$95/hr stays above the PHONE field");
  assert.ok(days.y - items.y > 16, "preferred days sit above the ITEMS header");
  for (const hit of ops.filter((op) => op.text.includes("($95/hr)"))) {
    assert.ok(hit.x < 200, "($95/hr) stays on the left");
    assert.ok(Math.abs(hit.y - company.y) > 8, "($95/hr) does not sit on the company name");
    assert.ok(Math.abs(hit.y - label.y) > 8, "($95/hr) stays off the DELIVERY COMPANY line");
  }

  const short = validateFreightQuoteRequest(
    localDeliveryBody({
      access: { delivery_partner: "cruz_pro_line", preferred_days_windows: "s" },
    })
  );
  const shortOps = pdfOps(await generateLocalSignPdf(short.submission, ctx));
  const shortDays = pdfFind(shortOps, "CUSTOMER PREFERRED DAYS");
  const shortValue = shortOps.find((op) => op.text === "s");
  const shortItems = pdfFind(shortOps, "ITEMS");
  assert.ok(shortDays && shortValue && shortItems);
  assert.ok(shortDays.y > shortValue.y && shortValue.y - shortItems.y > 16);
});

test("sign PDF wraps long disassembly access text above preferred days", async () => {
  const ctx = {
    requestId: "test-sign-assembly-wrap",
    route: { drive_minutes: 26, distance_miles: 12 },
    localEstimate: {
      estimated_price: 165,
      drive_minutes: 26,
      distance_miles: 12,
      assembly_extra_minutes: 13,
    },
  };
  const v = validateFreightQuoteRequest(
    localDeliveryBody({
      access: {
        delivery_partner: "cruz_pro_line",
        assembly_required: "yes",
        assembly_over_15: "yes",
        assembly_extra_minutes: 13,
        preferred_days_windows: "Monday 3-6",
        stairs: true,
        stair_flights: 1,
      },
    })
  );
  assert.equal(v.ok, true);
  const buf = await generateLocalSignPdf(v.submission, ctx);
  assert.ok(buf && buf.length > 500);
  const pageCount = (buf.toString("latin1").match(/\/Type\s*\/Page\b/g) || []).length;
  // Prefer one page; allow page 2 rather than crowding contact / preferred / signatures.
  assert.ok(pageCount >= 1 && pageCount <= 2, "sign PDF uses at most two pages");

  const ops = pdfOps(buf);
  const disassemblyLabel = pdfFind(ops, "Disassembly & reassembly");
  const additional = pdfFind(ops, "Additional Disassembly");
  const totalLine = pdfFind(ops, "Total Disassembly");
  const preferred = pdfFind(ops, "CUSTOMER PREFERRED DAYS");
  const preferredVal = pdfFind(ops, "Monday 3-6");
  const items = pdfFind(ops, "ITEMS");
  assert.ok(disassemblyLabel, "disassembly label present");
  assert.ok(additional, "additional disassembly timing present");
  assert.ok(preferred && preferredVal && items);

  // PDF y grows upward: access timing must sit above preferred days / items.
  assert.ok(
    additional.y > preferred.y + 8,
    "long disassembly value stays above CUSTOMER PREFERRED DAYS (no stamp-over)"
  );
  if (totalLine) {
    assert.ok(
      totalLine.y > preferred.y + 4,
      "total disassembly line also stays above preferred days"
    );
  }
  assert.ok(preferred.y > preferredVal.y, "preferred label above its value");
  assert.ok(preferredVal.y - items.y > 12, "preferred value stays above ITEMS");

  // Wrapped access value must not share the preferred-days baseline.
  for (const hit of ops.filter(
    (op) =>
      op.text.includes("Additional Disassembly") ||
      op.text.includes("Total Disassembly & Reassembly Time")
  )) {
    assert.ok(
      Math.abs(hit.y - preferred.y) > 6,
      `assembly text must not overlap preferred days (y=${hit.y} vs ${preferred.y})`
    );
  }
});

test("sign PDF + email keep multi preferred windows and non-overlapping contact labels", async () => {
  const multiPreferred =
    "Mon 3-6\nTue 10-12\nWed Idk IDk IDK IDKL\nThu morning backup if needed";
  const ctx = {
    requestId: "test-multi-preferred-contact",
    route: { drive_minutes: 20, distance_miles: 12.3 },
    localEstimate: {
      estimated_price: 145,
      drive_minutes: 20,
      distance_miles: 12.3,
      assembly_extra_minutes: 15,
    },
  };
  const v = validateFreightQuoteRequest(
    localDeliveryBody({
      customer_name: "Joanne Royer",
      customer_email: "bernis.berber@icloud.com",
      customer_phone: "4802098133",
      street: "7607 E Bonita Drive",
      unit: "",
      city: "Scottsdale",
      state: "AZ",
      zip: "85250",
      access: {
        delivery_partner: "cruz_pro_line",
        assembly_required: "yes",
        assembly_over_15: "yes",
        assembly_extra_minutes: 15,
        preferred_days_windows: multiPreferred,
        stairs: true,
        stair_flights: 1,
        freight_elevator: true,
        fragile_or_special_handling: true,
      },
    })
  );
  assert.equal(v.ok, true);
  assert.equal(v.submission.access.preferred_days_windows, multiPreferred);

  const buf = await generateLocalSignPdf(v.submission, ctx);
  assert.ok(buf && buf.length > 500);
  const ops = pdfOps(buf);
  const nameVal = pdfFind(ops, "Joanne Royer");
  const nameLabel = pdfFind(ops, "NAME");
  const emailVal = pdfFind(ops, "bernis.berber@icloud.com");
  const emailLabel = pdfFind(ops, "EMAIL");
  const phoneVal = pdfFind(ops, "(480) 209-8133");
  const phoneLabel = pdfFind(ops, "PHONE");
  const addrVal = pdfFind(ops, "7607 E Bonita Drive");
  const addrLabel = pdfFind(ops, "DELIVERY ADDRESS");
  assert.ok(nameVal && nameLabel && emailVal && emailLabel && phoneVal && phoneLabel);
  assert.ok(addrVal && addrLabel);
  // PDF y grows upward: value above its label, and each label above the next value.
  assert.ok(nameVal.y > nameLabel.y + 2, "NAME value above NAME label");
  assert.ok(nameLabel.y > emailVal.y + 2, "NAME label above EMAIL value (no stamp-over)");
  assert.ok(emailVal.y > emailLabel.y + 2, "EMAIL value above EMAIL label");
  assert.ok(emailLabel.y > addrVal.y + 2, "EMAIL label above ADDRESS value (no stamp-over)");
  assert.ok(addrVal.y > addrLabel.y + 2, "ADDRESS value above ADDRESS label");
  assert.ok(phoneVal.y > phoneLabel.y + 2, "PHONE value above PHONE label");

  for (const needle of ["Mon 3-6", "Tue 10-12", "Wed Idk IDk IDK IDKL", "Thu morning backup"]) {
    assert.ok(pdfFind(ops, needle), `sign PDF keeps preferred window: ${needle}`);
  }

  const mail = buildFreightQuoteEmails(v.submission, ctx);
  assert.match(mail.customer.html, /Mon 3-6/);
  assert.match(mail.customer.html, /Tue 10-12/);
  assert.match(mail.customer.html, /Wed Idk IDk IDK IDKL/);
  assert.match(mail.customer.html, /Thu morning backup if needed/);
  assert.match(mail.customer.html, /Preferred days \/ windows/);
  assert.match(mail.internal.html, /Mon 3-6/);
  assert.match(mail.internal.html, /Wed Idk IDk IDK IDKL/);
  assert.match(mail.internal.text, /Mon 3-6/);
  assert.match(mail.internal.text, /Thu morning backup if needed/);
});

test("store employee: only applies to local delivery", () => {
  const pickup = validateFreightQuoteRequest(
    localDeliveryBody({ delivery_path: "pickup_az", store_employee: true, staff_code: "" })
  );
  assert.equal(pickup.ok, true);
  assert.equal(pickup.submission.store_sale, false);
});

test("store employee: wrong code is rejected", () => {
  const v = validateFreightQuoteRequest(
    localDeliveryBody({ store_employee: true, staff_code: "1111" })
  );
  assert.equal(v.ok, false);
  assert.equal(v.status, 403);
});

test("store employee: in-store sale lists items with no Buy Now or SOLD", () => {
  const ctx = {
    requestId: "test-store",
    route: { drive_minutes: 23, distance_miles: 8.9 },
    localEstimate: { estimated_price: 110, drive_minutes: 23, distance_miles: 8.9 },
  };
  const item = {
    title: "Walnut Cabinet (No Longer Available)",
    width: 39,
    depth: 19,
    height: 42,
    weight: 150,
    quantity: 1,
    product_url: "https://www.lostandfoundresale.com/product/walnut-cabinet-1234",
    sold: true,
  };
  const staff = validateFreightQuoteRequest(
    localDeliveryBody({ store_employee: true, staff_code: "2012", items: [item] })
  );
  assert.equal(staff.ok, true);
  assert.equal(staff.submission.store_sale, true);
  assert.equal(staff.submission.items[0].sold, false);
  const mail = buildFreightQuoteEmails(staff.submission, ctx);
  assert.doesNotMatch(mail.customer.html, /Buy Now|\(SOLD\)/);
  assert.doesNotMatch(mail.customer.text, /Buy Now|\(SOLD\)/);
  assert.match(mail.customer.html, /Your item\(s\)/);
  assert.match(mail.internal.subject, /^In-Store Sale \|/);
  assert.match(mail.internal.html, /In-store sale/);

  const customer = validateFreightQuoteRequest(
    localDeliveryBody({ store_employee: false, items: [{ ...item, sold: false, title: "Walnut Cabinet" }] })
  );
  assert.equal(customer.submission.store_sale, false);
  const customerMail = buildFreightQuoteEmails(customer.submission, ctx);
  assert.match(customerMail.customer.html, /Buy your item\(s\)/);
  assert.match(customerMail.customer.html, /Buy Now/);
});

test("absoluteHttpsImageUrl normalizes listing image candidates", () => {
  assert.equal(
    absoluteHttpsImageUrl("//cdn.shopify.com/s/files/1/x/product.jpg"),
    "https://cdn.shopify.com/s/files/1/x/product.jpg"
  );
  assert.equal(
    absoluteHttpsImageUrl({ url: "http://cdn.shopify.com/a.jpg" }),
    "https://cdn.shopify.com/a.jpg"
  );
  assert.equal(absoluteHttpsImageUrl("[object Object]", "/relative.jpg"), "");
  assert.equal(
    absoluteHttpsImageUrl("[object Object]", {
      src: "https://cdn.prod.website-files.com/abc/photo.jpg",
    }),
    "https://cdn.prod.website-files.com/abc/photo.jpg"
  );
  assert.equal(
    absoluteHttpsImageUrl(
      "https://cdn.shopify.com/s/files/1/x/a.jpg",
      "https://cdn.prod.website-files.com/abc/photo.jpg"
    ),
    "https://cdn.prod.website-files.com/abc/photo.jpg"
  );
});

test("validation keeps absolute https image_url and drops junk", () => {
  const ok = validateFreightQuoteRequest(
    localDeliveryBody({
      items: [
        {
          title: "Chair",
          width: 20,
          depth: 20,
          height: 30,
          weight: 40,
          quantity: 1,
          image_url: "//cdn.shopify.com/s/files/1/x/chair.jpg?v=1",
        },
      ],
    })
  );
  assert.equal(ok.ok, true);
  assert.equal(ok.submission.items[0].image_url, "https://cdn.shopify.com/s/files/1/x/chair.jpg?v=1");

  const junk = validateFreightQuoteRequest(
    localDeliveryBody({
      items: [
        {
          title: "Chair",
          width: 20,
          depth: 20,
          height: 30,
          weight: 40,
          quantity: 1,
          image_url: "[object Object]",
        },
      ],
    })
  );
  assert.equal(junk.submission.items[0].image_url, "");
});

test("store sale internal email: off-site listing uses Shop All + staff note + https image", () => {
  const ctx = {
    requestId: "test-store-offsite",
    route: { drive_minutes: 23, distance_miles: 8.9 },
    localEstimate: { estimated_price: 110, drive_minutes: 23, distance_miles: 8.9 },
  };
  const img = "https://cdn.shopify.com/s/files/1/0123/4567/products/walnut.jpg?v=9";
  const staff = validateFreightQuoteRequest(
    localDeliveryBody({
      store_employee: true,
      staff_code: "2012",
      items: [
        {
          title: "Walnut Cabinet",
          width: 39,
          depth: 19,
          height: 42,
          weight: 150,
          quantity: 1,
          product_url: "https://www.lostandfoundresale.com/product/walnut-cabinet-1234",
          image_url: img,
          listing_off_site: true,
        },
      ],
    })
  );
  assert.equal(staff.ok, true);
  // listing_off_site is enrichment-only; set it on the submission items for render.
  staff.submission.items[0].listing_off_site = true;
  const mail = buildFreightQuoteEmails(staff.submission, ctx);
  assert.match(mail.internal.html, new RegExp(`src="${img.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}"`));
  assert.match(mail.internal.html, new RegExp(`href="${SHOP_ALL_URL.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}"`));
  assert.match(mail.internal.html, />Shop All</);
  assert.doesNotMatch(mail.internal.html, />View listing</);
  assert.match(
    mail.internal.html,
    /This item may disappear from that listing link; you will be redirected to Shop All/
  );
  assert.match(mail.internal.text, new RegExp(SHOP_ALL_URL.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")));
  assert.match(
    mail.internal.text,
    /This item may disappear from that listing link; you will be redirected to Shop All/
  );
  // Customer store-sale email: real Shop All <a href> on the item (+ image), plus redirect note.
  const shopAllEsc = SHOP_ALL_URL.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  assert.match(
    mail.customer.html,
    /This item may disappear from that listing link; you will be redirected to Shop All/
  );
  assert.match(
    mail.customer.text,
    /This item may disappear from that listing link; you will be redirected to Shop All/
  );
  assert.match(mail.customer.html, new RegExp(`href="${shopAllEsc}"[^>]*>Shop All</`));
  assert.match(mail.customer.html, new RegExp(`<a href="${shopAllEsc}"[^>]*>\\s*<img `));
  assert.match(mail.customer.text, new RegExp(`Shop All: ${shopAllEsc}`));
  assert.doesNotMatch(mail.customer.html, /Buy Now|View listing|\(SOLD\)/);
  // Customer email still gets the real https thumbnail (no Buy Now for store sale).
  assert.match(mail.customer.html, new RegExp(`src="${img.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}"`));
});

test("sign PDF: IMPORTANT stays below items; missing dims show Unknown + weight", async () => {
  const ctx = {
    requestId: "test-sign-overlap-unknown",
    route: { drive_minutes: 28, distance_miles: 14.2 },
    localEstimate: {
      estimated_price: 147,
      drive_minutes: 28,
      distance_miles: 14.2,
      assembly_extra_minutes: 15,
    },
  };
  const preferred =
    "Bernis Berber\nskfddf Tuesday or Wednesday morning preferred, also open Thursday after 1pm if needed\ndfdfdd weekend backup window Saturday 10-2";
  const v = validateFreightQuoteRequest(
    localDeliveryBody({
      customer_name: "Joanne Royer",
      customer_email: "bernis.berber@icloud.com",
      street: "7607 E Bonita Drive",
      unit: "",
      city: "Scottsdale",
      state: "AZ",
      zip: "85250",
      access: {
        delivery_partner: "cruz_pro_line",
        assembly_required: "yes",
        assembly_over_15: "yes",
        assembly_extra_minutes: 15,
        preferred_days_windows: preferred,
        stairs: true,
        stair_flights: 2,
        freight_elevator: true,
      },
      items: [
        {
          title: "Kincaid Emory Side Chair - Set of 4 - 26X20.5X39H",
          width: 26,
          depth: 20.5,
          height: 39,
          weight: 150,
          quantity: 1,
        },
        {
          title: "Unpholstered Slipcovered Bench - 60X26X27H",
          width: 60,
          depth: 25.5,
          height: 27,
          weight: 130,
          quantity: 1,
        },
        {
          title: "Canadel Collection Boat Shape Dining Table",
          width: null,
          depth: null,
          height: null,
          weight: 100,
          quantity: 1,
        },
      ],
    })
  );
  assert.equal(v.ok, true);
  assert.equal(v.submission.items[2].width, null);
  assert.equal(v.submission.items[2].weight, 100);

  const buf = await generateLocalSignPdf(v.submission, ctx);
  assert.ok(buf && buf.length > 500);
  const pageCount = (buf.toString("latin1").match(/\/Type\s*\/Page\b/g) || []).length;
  // Prefer one page via compression; allow page 2 rather than overlapping signatures.
  assert.ok(pageCount >= 1 && pageCount <= 2, "sign PDF uses at most two pages");

  const ops = pdfOps(buf);
  // Em dash may round-trip as WinAnsi 0x97 — match on stable words only.
  const important = pdfFind(ops, "PLEASE READ BEFORE SIGNING") || pdfFind(ops, "IMPORTANT");
  const itemsHeader = pdfFind(ops, "ITEMS");
  const unknown = pdfFind(ops, "Unknown");
  const weight100 = ops.find((op) => /100\s*lb/.test(op.text));
  const kincaid = ops.find((op) => /Kincaid/i.test(op.text));
  const canadel = ops.find((op) => /Canadel/i.test(op.text));
  const slipcovered = ops.find((op) => /Slipcovered/i.test(op.text));
  const agree = pdfFind(ops, "Customer agrees to pay all fees");
  const printName = pdfFind(ops, "PRINT NAME");
  const joannes = ops.filter((op) => op.text.includes("Joanne Royer"));
  const sigJoanne =
    (printName && joannes.find((j) => Math.abs(j.x - printName.x) < 3)) || joannes[joannes.length - 1];
  const emp = pdfFind(ops, "L+F EMPLOYEE");
  assert.ok(important, "IMPORTANT header present");
  assert.ok(itemsHeader, "ITEMS header present");
  assert.ok(unknown, "missing dims print as Unknown");
  assert.ok(weight100, "entered weight 100 lb still shown");
  assert.ok(kincaid && canadel && slipcovered, "item titles present");
  assert.ok(agree && printName && sigJoanne && emp, "signature block labels present");
  assert.doesNotMatch(unknown.text, /48["″]?[x×]40/i);
  assert.doesNotMatch(
    ops.filter((op) => /Canadel|Unknown|100\s*lb/.test(op.text)).map((o) => o.text).join(" "),
    /48["″]?[x×]40/i
  );

  // PDF y grows upward. Same-page: items must sit above IMPORTANT. Page-2 legal block is OK.
  const itemHits = ops.filter(
    (op) =>
      /Kincaid|Slipcovered|Canadel|Unknown/i.test(op.text) &&
      !/IMPORTANT|PLEASE READ/i.test(op.text)
  );
  assert.ok(itemHits.length >= 3, "expected all three item lines");
  if (pageCount === 1) {
    for (const hit of itemHits) {
      assert.ok(
        hit.y > important.y + 8,
        `item text must sit above IMPORTANT (y=${hit.y} vs important=${important.y}: ${hit.text.slice(0, 60)})`
      );
    }
    const lowestItemY = Math.min(...itemHits.map((h) => h.y));
    assert.ok(lowestItemY - important.y >= 12, "clear gap between last item and IMPORTANT");
    assert.ok(itemsHeader.y > important.y + 20, "ITEMS header above IMPORTANT");
  } else {
    // Legal block on page 2: IMPORTANT near top of that page, items stay on page 1 (higher raw y band).
    assert.ok(important.y > 600, "IMPORTANT on page 2 starts near top");
    assert.ok(
      Math.min(...itemHits.map((h) => h.y)) < 400,
      "item lines remain on page 1 (not stamped into legal block)"
    );
  }

  // Signature stack: agreement above fields; prefilled name above PRINT NAME label; emp below.
  assert.ok(important.y - agree.y >= 20, "IMPORTANT clear of agreement line");
  assert.ok(agree.y - printName.y >= 12, "agreement clear of PRINT NAME");
  assert.ok(sigJoanne.y - printName.y >= 12, "prefilled name sits above PRINT NAME (no stamp-over)");
  assert.ok(printName.y - emp.y >= 14, "customer sig row clear of L+F EMPLOYEE row");
});

test("enrichSubmissionItemMedia prefers listing https image over junk client image", async () => {
  const listingImg = "https://cdn.shopify.com/s/files/1/ok/live.jpg";
  const prevFetch = globalThis.fetch;
  globalThis.fetch = async () => ({
    ok: true,
    json: async () => ({
      listing: {
        title: "Walnut Cabinet",
        product_url: "https://www.lostandfoundresale.com/product/walnut-cabinet-1234",
        image_url: listingImg,
        sold: true,
      },
      images: [listingImg],
    }),
  });
  try {
    const enriched = await enrichSubmissionItemMedia({
      delivery_path: "local_az",
      store_sale: true,
      items: [
        {
          index: 1,
          source: "listing",
          title: "Walnut Cabinet",
          product_url: "https://www.lostandfoundresale.com/product/walnut-cabinet-1234",
          image_url: "[object Object]",
          sold: false,
        },
      ],
    });
    assert.equal(enriched.items[0].image_url, listingImg);
    assert.equal(enriched.items[0].sold, false);
    assert.equal(enriched.items[0].listing_off_site, true);
  } finally {
    globalThis.fetch = prevFetch;
  }
});

test("enrichSubmissionItemMedia marks listing_off_site on 404 for store_sale", async () => {
  const prevFetch = globalThis.fetch;
  globalThis.fetch = async () => ({ ok: false, status: 404, json: async () => ({}) });
  try {
    const enriched = await enrichSubmissionItemMedia({
      delivery_path: "local_az",
      store_sale: true,
      items: [
        {
          index: 1,
          source: "listing",
          title: "Gone Piece",
          product_url: "https://www.lostandfoundresale.com/product/gone-piece-1",
          image_url: "https://cdn.shopify.com/s/files/1/ok/cached.jpg",
        },
      ],
    });
    assert.equal(enriched.items[0].listing_off_site, true);
    assert.equal(enriched.items[0].image_url, "https://cdn.shopify.com/s/files/1/ok/cached.jpg");
  } finally {
    globalThis.fetch = prevFetch;
  }
});
