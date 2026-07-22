import test from "node:test";
import assert from "node:assert/strict";
import {
  palletizeItem,
  palletizeItems,
  calculateLocalRouteEstimate,
  inferFreightClass,
  shouldMarkNonStackable,
  parseSetCountFromTitle,
} from "../lib/freightPalletize.js";
import { validateFreightQuoteRequest } from "../lib/freightQuoteValidation.js";
import { buildManualReviewReasons } from "../lib/freightLocalEstimate.js";
import { isAllowedConsignmentOrigin } from "../lib/consignmentCors.js";
import {
  estimateNationwideRange,
  NATIONWIDE_FLOOR_USD,
} from "../lib/freightNationwideRate.js";

test("local pricing: 17 min = $95", () => {
  assert.equal(calculateLocalRouteEstimate(17).estimated_price, 95);
});

test("local pricing: 19 min = $100 (matches Webflow)", () => {
  assert.equal(calculateLocalRouteEstimate(19).estimated_price, 100);
});

test("local pricing: 21 min = $105", () => {
  assert.equal(calculateLocalRouteEstimate(21).estimated_price, 105);
});

test("local pricing: 25 min = $110", () => {
  assert.equal(calculateLocalRouteEstimate(25).estimated_price, 110);
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

test("set of 4 swivel chairs: qty 1, total weight, layered pallet", () => {
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
  // Swivel → layered (not nested): 2 per layer on 48x40, 2 layers → 60" + 5" pallet
  assert.equal(r.packing.packing_mode, "layered");
  assert.equal(r.packing.pieces_per_layer, 2);
  assert.equal(r.packing.layers, 2);
  assert.equal(r.pallet.height, 65);
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
    tight_turns_or_narrow_halls: true,
  });
  assert.ok(reasons.some((r) => /2 flights/i.test(r)));
  assert.ok(reasons.some((r) => /More than two movers/i.test(r)));
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
