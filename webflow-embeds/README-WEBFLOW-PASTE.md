# Delivery & Freight Calculator | Webflow paste

Source of truth: the customer's Part 1 + Part 2 (look, copy, payload, summary).

## Part 1
Paste `PART1-embed-html-css.html` into the Embed element.

## Part 2
Paste `PART2-before-body-js.html` into Page Settings → Custom Code → Before `</body>`.

Do not redesign. Backend must accept this payload and return drive minutes / route so Part 2 `renderSummary` can show the estimate.
