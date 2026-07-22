# Delivery & Freight Calculator — Webflow paste guide

## Part 1 — Embed (HTML + CSS only)

1. Open the Delivery & Freight Calculator page in Webflow.
2. Select the Embed element.
3. Paste the full contents of `PART1-embed-html-css.html`.
4. Do **not** put JavaScript in this embed (keeps the Designer preview visible).

## Part 2 — Before `</body>` (JavaScript only)

1. Page Settings → Custom Code → **Before `</body>`**.
2. Paste the full contents of `PART2-before-body-js.html`.
3. Publish staging and production.

## Behavior (aligned with Render)

- Webflow collects raw fields only.
- `POST /api/freight-quote/preview` → calculate + display (no emails).
- `POST /api/freight-quote` → recalculate + 1 customer email + 1 internal email.
- Price, pallet dims, freight class confirmation, and nationwide rate status come from Render `display` / `items[].pallet` only.
- Staging and production both use: `https://webflow-sync-server.onrender.com`
