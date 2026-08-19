/**
 * Staff cheat sheet: local AZ delivery + consignor pickup pricing.
 * Run: node scripts/generate-local-pricing-cheatsheet.mjs
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import PDFDocument from "pdfkit";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const OUT_DIR = path.join(ROOT, "docs");
const OUT_FILE = path.join(OUT_DIR, "local-delivery-pickup-pricing-cheatsheet.pdf");

const NAVY = "#07127c";
const GOLD = "#c9a227";
const MUTED = "#555555";
const INK = "#111111";
const BORDER = "#d4cfc3";
const MARGIN = 44;
const PAGE_W = 612;
const CONTENT_W = PAGE_W - MARGIN * 2;
const FOOTER = 28;

function pageBottom(doc) {
  return doc.page.height - MARGIN - FOOTER;
}

function ensureSpace(doc, h) {
  if (doc.y + h <= pageBottom(doc)) return;
  doc.addPage();
  drawPageHeader(doc, false);
}

function drawPageHeader(doc, first = false) {
  if (first) {
    doc
      .rect(0, 0, PAGE_W, 72)
      .fill(NAVY);
    doc
      .fillColor("#ffffff")
      .font("Helvetica-Bold")
      .fontSize(16)
      .text("LOST & FOUND RESALE INTERIORS", MARGIN, 22, { width: CONTENT_W });
    doc
      .font("Helvetica")
      .fontSize(10)
      .fillColor("#c9d0ff")
      .text("Local Delivery & Consignor Pickup — Staff Pricing Cheat Sheet", MARGIN, 44, {
        width: CONTENT_W,
      });
    doc.y = 88;
  } else {
    doc
      .font("Helvetica-Bold")
      .fontSize(9)
      .fillColor(NAVY)
      .text("Local Delivery & Pickup Pricing", MARGIN, MARGIN - 8, { width: CONTENT_W });
    doc
      .moveTo(MARGIN, MARGIN + 10)
      .lineTo(PAGE_W - MARGIN, MARGIN + 10)
      .strokeColor(BORDER)
      .lineWidth(0.5)
      .stroke();
    doc.y = MARGIN + 18;
  }
  doc.fillColor(INK);
}

function sectionTitle(doc, title) {
  ensureSpace(doc, 28);
  doc.moveDown(0.35);
  doc.font("Helvetica-Bold").fontSize(12).fillColor(NAVY).text(title, MARGIN, doc.y, {
    width: CONTENT_W,
  });
  doc
    .moveTo(MARGIN, doc.y + 4)
    .lineTo(MARGIN + 120, doc.y + 4)
    .strokeColor(GOLD)
    .lineWidth(2)
    .stroke();
  doc.moveDown(0.55);
  doc.fillColor(INK);
}

function body(doc, text, opts = {}) {
  ensureSpace(doc, 16);
  doc
    .font(opts.bold ? "Helvetica-Bold" : "Helvetica")
    .fontSize(opts.size || 9.5)
    .fillColor(opts.color || INK)
    .text(text, MARGIN, doc.y, { width: CONTENT_W, lineGap: 2, ...opts });
  doc.moveDown(0.15);
}

function bullet(doc, text) {
  ensureSpace(doc, 14);
  doc
    .font("Helvetica")
    .fontSize(9.5)
    .fillColor(INK)
    .text(`•  ${text}`, MARGIN + 6, doc.y, { width: CONTENT_W - 12, lineGap: 1.5 });
  doc.moveDown(0.08);
}

function drawTable(doc, headers, rows, colWidths) {
  const rowH = 16;
  const headerH = 18;
  const totalW = colWidths.reduce((a, b) => a + b, 0);
  ensureSpace(doc, headerH + rows.length * rowH + 8);

  let x = MARGIN;
  let y = doc.y;
  doc.rect(x, y, totalW, headerH).fill(NAVY);
  doc.font("Helvetica-Bold").fontSize(8.5).fillColor("#ffffff");
  headers.forEach((h, i) => {
    doc.text(h, x + 4, y + 4, { width: colWidths[i] - 8 });
    x += colWidths[i];
  });
  y += headerH;
  rows.forEach((row, ri) => {
    x = MARGIN;
    const bg = ri % 2 === 0 ? "#f7f6f2" : "#ffffff";
    doc.rect(x, y, totalW, rowH).fill(bg);
    doc.font("Helvetica").fontSize(8.5).fillColor(INK);
    row.forEach((cell, ci) => {
      doc.text(String(cell), x + 4, y + 4, { width: colWidths[ci] - 8 });
      x += colWidths[ci];
    });
    y += rowH;
  });
  doc.y = y + 8;
}

function exampleBox(doc, title, lines) {
  ensureSpace(doc, 24 + lines.length * 13);
  const startY = doc.y;
  doc
    .font("Helvetica-Bold")
    .fontSize(9.5)
    .fillColor(NAVY)
    .text(title, MARGIN + 8, startY + 8, { width: CONTENT_W - 16 });
  let y = startY + 22;
  doc.font("Helvetica").fontSize(9).fillColor(INK);
  lines.forEach((line) => {
    doc.text(line, MARGIN + 8, y, { width: CONTENT_W - 16 });
    y += 13;
  });
  const h = y - startY + 6;
  doc
    .roundedRect(MARGIN, startY, CONTENT_W, h, 4)
    .lineWidth(0.75)
    .strokeColor(BORDER)
    .stroke();
  doc.y = startY + h + 6;
}

function drawFooter(doc, pageNum) {
  doc
    .font("Helvetica")
    .fontSize(7.5)
    .fillColor(MUTED)
    .text(
      "Internal use only · Lost & Found Resale · Local AZ delivery (local_az) & consignor pickup (pickup_az) · Nationwide freight uses different rules",
      MARGIN,
      doc.page.height - MARGIN + 6,
      { width: CONTENT_W, align: "center" }
    );
  doc.text(String(pageNum), PAGE_W - MARGIN - 20, doc.page.height - MARGIN + 6, {
    width: 20,
    align: "right",
  });
}

async function buildPdf() {
  fs.mkdirSync(OUT_DIR, { recursive: true });
  const doc = new PDFDocument({ size: "LETTER", margin: MARGIN, bufferPages: true });
  const stream = fs.createWriteStream(OUT_FILE);
  doc.pipe(stream);

  drawPageHeader(doc, true);

  body(
    doc,
    "Same pricing rules for Local Arizona Delivery and Consignor Pickup. Customers see truck-adjusted drive time (mapped route + 3 min local truck travel), miles, and preliminary estimate. Raw Google/car minutes are never shown."
  );

  sectionTitle(doc, "Core formula");
  body(doc, "FINAL = (route + multi-item + large + heavy) × trucks + stairs + assembly", {
    bold: true,
  });

  sectionTitle(doc, "Rate & base route");
  drawTable(
    doc,
    ["Item", "Rule"],
    [
      ["Base price", "$95 covers ~15 min one-way truck time (round-trip billing)"],
      ["Standard rate label", "billed at 95/hr"],
      ["3-person crew rate", "billed at 130/hr — see oversize crew rules below"],
      ["Two trucks", "billed at 95/hr · two trucks (2× route-side adders)"],
      ["Truck time shown", "Google mapped time + 3 min (local truck travel)"],
      ["Distance shown", "Mapped miles (unchanged)"],
    ],
    [140, CONTENT_W - 140]
  );

  sectionTitle(doc, "Truck time → price (one-way truck minutes)");
  drawTable(
    doc,
    ["Google (car)", "Truck time shown", "Typical route price"],
    [
      ["12 min", "15 min", "$95"],
      ["14 min", "17 min", "$100"],
      ["16 min", "19 min", "$105"],
      ["22 min", "25 min", "$115"],
    ],
    [100, 120, CONTENT_W - 220]
  );
  body(
    doc,
    'Customer fine print: "Distance is based on the mapped route. Drive time includes local truck travel time and is not live traffic."',
    { size: 8.5, color: MUTED }
  );

  sectionTitle(doc, "Piece surcharges (local delivery + pickup only)");
  drawTable(
    doc,
    ["Surcharge", "When", "Amount"],
    [
      ["Multi-item", "3rd piece and up (first 2 included in base)", "+$5 if footprint ≤ 24×24 in · +$10 if bigger"],
      ["Large piece", "Any piece — width, depth, OR height > 60 in", "+$10 per piece"],
      ["Heavy piece", "Any piece — weight > 300 lb", "+$10 per piece"],
      ["Stairs", "After 1st flight", "+$7 per additional flight"],
    ],
    [90, 250, CONTENT_W - 340]
  );
  bullet(doc, "Large and heavy apply to every qualifying piece, including the 1st and 2nd.");
  bullet(doc, "Multi-item only starts on the 3rd piece.");
  bullet(doc, "A piece can stack surcharges (e.g. large sofa + heavy safe = +$20 on that piece).");

  sectionTitle(doc, "Disassembly & reassembly (local delivery + pickup)");
  body(
    doc,
    "Asked on the local/pickup form (job-level). First 15 minutes of disassembly/reassembly with wrapping/prep is included in the base route — no extra charge. Only minutes entered BEYOND that 15 are billable."
  );
  drawTable(
    doc,
    ["Customer answers", "Price", "What customer sees"],
    [
      ["No", "$0", "Nothing extra"],
      ["Not sure", "$0", "Treated as no for pricing"],
      ["Yes → within 15 min", "$0", "Needed — within 15 min with wrapping/prep (no extra charge)"],
      ["Yes → over 15 min → No", "$0", "Required, but no extra time billed"],
      ["Yes → over 15 min → Not sure", "$0", "Required, no extra time billed"],
      ["Yes → over 15 min → Yes + X extra min", "Fee on X only", "Additional X min · Total (15+X) min"],
      ["Extra minutes > 60", "Fee applies + staff flag", "Red note: confirm time with customer"],
    ],
    [155, 55, CONTENT_W - 210]
  );
  body(doc, "Assembly fee formula: extra minutes only (beyond included 15) × ($95 ÷ 60 per minute), rounded up to nearest $5. Does NOT add to route/drive minutes.", {
    bold: true,
    size: 9,
  });
  bullet(doc, "20 extra minutes → $35 assembly fee (15 min route $95 → total $130)");
  bullet(doc, "75 extra minutes → $120 assembly fee + manual review to confirm with customer");
  bullet(doc, "Assembly fee is added AFTER the truck multiplier (not doubled on two-truck jobs).");
  body(
    doc,
    "Customer email shows minutes only for assembly — not a separate dollar line for the assembly fee (fee is in the total).",
    { size: 8.5, color: MUTED }
  );

  sectionTitle(doc, "Stairs");
  drawTable(
    doc,
    ["Flights of stairs", "Stair fee"],
    [
      ["1 flight", "$0 (included)"],
      ["2 flights", "+$7"],
      ["3 flights", "+$14"],
      ["4 flights", "+$21"],
    ],
    [140, CONTENT_W - 140]
  );
  bullet(doc, "Stairs fee is added after truck multiplier (not doubled on two-truck jobs).");

  sectionTitle(doc, "Extra crew & two trucks");
  drawTable(
    doc,
    ["Customer selects", "Route rate", "Multiplier"],
    [
      ["Standard (2 movers)", "billed at 95/hr", "×1 truck"],
      ["1 extra person (3 total)", "billed at 130/hr", "×1 truck"],
      ["2 extra people (4 total)", "billed at 95/hr · two trucks", "×2 trucks on route + multi + large + heavy"],
    ],
    [155, 155, CONTENT_W - 310]
  );
  bullet(doc, "Customer must explain why extra crew is needed (form validation).");
  bullet(doc, "Oversize items (299+ lb & over 72 in H, or 450+ lb) auto-use $130/hr even if customer did not select extra crew.");

  sectionTitle(doc, "Out-of-town / long haul (still local truck)");
  bullet(doc, "Round trip over 100 miles → route priced at 80% of $95/hr (~$76/hr effective).");
  bullet(doc, "Label may say “Best-Guess Estimate (Out of town — callout)” — confirm before scheduling.");
  bullet(doc, "If 2 extra people on a long-haul job: two trucks at the reduced 80% rate.");

  sectionTitle(doc, "What doubles on two-truck jobs vs what does not");
  drawTable(
    doc,
    ["Component", "×2 trucks?", "Notes"],
    [
      ["Route price", "Yes", "Based on drive time at applicable hourly rate"],
      ["Multi-item / large / heavy adders", "Yes", "Added before multiplier"],
      ["Stair fee", "No", "Added once after multiplier"],
      ["Assembly fee", "No", "Added once after multiplier"],
    ],
    [130, 70, CONTENT_W - 200]
  );

  sectionTitle(doc, "Manual review flags (may not auto-add $)");
  body(doc, "These flag staff for follow-up. Estimate still generates but team should confirm:", {
    size: 9,
  });
  bullet(doc, "Stairs, extra crew, oversize 3-person crew, assembly over 60 extra minutes");
  bullet(doc, "Freight elevator · tight turns / narrow halls · long carry");
  bullet(doc, "Fragile / special handling · parking or time restrictions · gated access");
  bullet(doc, "One-way drive over ~75 minutes · out-of-town 100+ mile round trip");

  sectionTitle(doc, "3-person crew ($130/hr) — separate from +$10 surcharges");
  bullet(doc, "299+ lb AND over 72 in height, OR");
  bullet(doc, "450+ lb (any height).");
  body(doc, "Wide sofas alone do not trigger 3-person crew unless weight/height rules apply.", {
    size: 9,
    color: MUTED,
  });

  sectionTitle(doc, "Examples — delivery & pickup (same math)");

  exampleBox(doc, "Example 1 — Simple nearby job", [
    "1 chair · 20×20×36 · 25 lb · ~15 min truck time",
    "Route $95 · Multi $0 · Large $0 · Heavy $0 → Total $95",
    "Shows: Preliminary Estimate $95 · billed at 95/hr",
  ]);

  exampleBox(doc, "Example 2 — Three small items", [
    "3 side tables · each 20×20×24 · ~15 min truck",
    "Route $95 · Multi +$5 (3rd piece, small) → Total $100",
  ]);

  exampleBox(doc, "Example 3 — One sofa (large dimension)", [
    "1 sofa · 84×36×34 · 180 lb · ~15 min truck",
    "Route $95 · Large +$10 (width > 60 in) → Total $105",
  ]);

  exampleBox(doc, "Example 4 — Heavy dresser", [
    "1 dresser · 40×20×34 · 320 lb · ~15 min truck",
    "Route $95 · Heavy +$10 → Total $105",
  ]);

  exampleBox(doc, "Example 5 — Armoire (large + heavy + 3-person crew)", [
    "1 armoire · 40×24×80 · 450 lb · ~15 min truck",
    "Route $130 (3-person) · Large +$10 · Heavy +$10 → Total $150",
    "Shows: billed at 130/hr",
  ]);

  exampleBox(doc, "Example 6 — Farther address + multi-item", [
    "3 small chairs · Google ~19 min car → truck 22 min",
    "Route ~$110 · Multi +$5 (3rd chair) → Total $115",
    "Customer sees ~22 min each way (not 19).",
  ]);

  exampleBox(doc, "Example 7 — Stairs", [
    "1 chair · ~15 min · 3 flights of stairs",
    "Route $95 · Stairs +$14 (2 extra flights × $7) → Total $109",
  ]);

  exampleBox(doc, "Example 8 — Busy delivery (4 pieces)", [
    "22 min truck · 2 flights of stairs",
    "2 small decor (16×16) — in base",
    "1 table 66×40 — multi +$10, large +$10",
    "1 cabinet 36×24×74 · 310 lb — multi +$10, large +$10, heavy +$10",
    "Route $110 · Multi +$20 · Large +$20 · Heavy +$10 · Stairs +$7 → Total $167",
  ]);

  exampleBox(doc, "Example 9 — Two-truck job", [
    "2 large sofas · ~15 min · 2 extra people / two trucks",
    "(Route $95 + large +$10 + large +$10) × 2 trucks → Total $230",
    "Shows: billed at 95/hr · two trucks",
  ]);

  exampleBox(doc, "Example 10 — Disassembly within 15 min (no charge)", [
    "1 bed frame · ~15 min · Assembly: Yes, within 15 min",
    "Route $95 · Assembly $0 → Total $95",
    "Customer sees: disassembly needed — within 15 min, no extra charge.",
  ]);

  exampleBox(doc, "Example 11 — Disassembly over 15 min (+20 extra minutes)", [
    "1 armoire · ~15 min · Assembly: Yes, over 15 min, 20 extra minutes entered",
    "Route $95 · Assembly +$35 (20 min @ $95/hr, not added to drive time) → Total $130",
    "Customer sees: Additional 20 Minutes · Total 35 Minutes (15 included + 20).",
  ]);

  exampleBox(doc, "Example 12 — Everything stacked", [
    "2 chairs + 1 table (66×40) · 22 min truck · 2 flights · Assembly +30 extra min · 1 extra person",
    "Route ~$110 · Multi +$10 · Large +$10 · Stairs +$7",
    "Assembly +$50 (30 min @ $95/hr — separate from route time)",
    "Note: if 3-person crew triggered, route becomes $130 base instead of $110.",
  ]);

  sectionTitle(doc, "What customers receive");
  bullet(doc, "Thank you, [Name]");
  bullet(doc, "Preliminary Estimate: $XXX");
  bullet(doc, "billed at 95/hr (or 130/hr / two-truck variant)");
  bullet(doc, "Estimated drive time (truck-adjusted) · round trip · miles each way");
  bullet(doc, "Local truck travel time disclaimer");
  bullet(doc, "Optional: multi-item, large-item, heavy-item handling lines");
  bullet(doc, "Optional: disassembly/reassembly minutes (or within-15-min note)");
  bullet(doc, "Optional: confirm-with-customer banner if assembly > 60 extra min");
  body(doc, "Pickup vs delivery: same numbers — wording says consignor pickup or local delivery.", {
    size: 9,
    color: MUTED,
  });

  sectionTitle(doc, "Not included on local/pickup");
  bullet(doc, "Nationwide freight (different pallet/LTL pricing — assembly affects range, not this sheet)");
  bullet(doc, "Raw Google/car drive minutes (internal only)");
  bullet(doc, "Per-item assembly questions on nationwide form (local/pickup uses job-level assembly question)");

  const pages = doc.bufferedPageRange();
  for (let i = 0; i < pages.count; i++) {
    doc.switchToPage(i);
    drawFooter(doc, i + 1);
  }

  doc.end();
  await new Promise((resolve, reject) => {
    stream.on("finish", resolve);
    stream.on("error", reject);
  });
  console.log(`Wrote ${OUT_FILE} (${fs.statSync(OUT_FILE).size} bytes)`);
}

buildPdf().catch((err) => {
  console.error(err);
  process.exit(1);
});
