# Webflow embeds

## Delivery & Freight Calculator

Source of truth: the customer's Part 1 + Part 2 (look, copy, payload, summary).

### Part 1
Paste `PART1-embed-html-css.html` into the Embed element (tiny loader — CSS/HTML hosted on Render).
Editable full markup: `PART1-embed-html-css.source.html` — after edits, run `node scripts/minify-freight-part1.mjs`.

### Part 2
Paste `PART2-before-body-js.html` into Page Settings → Custom Code → Before `</body>` (minified for Webflow 50k).
Editable source: `PART2-before-body-js.source.html` — after edits, run `node scripts/minify-freight-part2.mjs`.
Part 2 loads the calculator HTML from Render into the Part 1 mount.

Do not redesign. Backend must accept this payload and return drive minutes / route so Part 2 `renderSummary` can show the estimate.

## Consignment Item Submission

See [`consign-submit/README.md`](./consign-submit/README.md).

| File | Webflow location |
|------|------------------|
| `consign-submit/INSIDE-HEAD-styles.html` | Inside `<head>` |
| `consign-submit/PART1-embed-html-css.html` | Page Embed |
| `consign-submit/PART2-before-body-js.html` | Before `</body>` |
