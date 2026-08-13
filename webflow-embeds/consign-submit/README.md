# Consignment Item Submission | Webflow paste

Furniture consign page embeds for `lostandfoundresale.com/submit-items`.
Lives next to the freight calculator embeds in `webflow-embeds/`.

Backend: `POST https://webflow-sync-server.onrender.com/api/consignment-submission`

## Paste locations

| File | Webflow location |
|------|------------------|
| `INSIDE-HEAD-styles.html` | Page Settings → Custom Code → Inside `<head>` tag |
| `PART1-embed-html-css.html` | Embed element on the page (minified HTML + CSS for paste limit) |
| `PART2-before-body-js.html` | Page Settings → Custom Code → Before `</body>` tag (minified, under Webflow 50k limit) |

Editable sources:
- PART1: `PART1-embed-html-css.source.html` → run `node scripts/minify-consign-part1.mjs`
- PART2: `PART2-before-body-js.source.html` → run `node scripts/minify-consign-part2.mjs`

## Rules enforced (front + back)

- Brand / Maker required (enter brand or `Unknown`) — helps prioritize review
- At least 1 photo per item (required); fewer than 3 shows the usual red helpful note only (does not block)
- Up to 10 items / 30 photos per submission
- Soft red tip after photo upload: upload each item individually — save this item, then add the next (faster review / better approval chances)
