# Local Delivery & Pickup Pricing Deploy

**Commit:** `3d4a27a` · **Pushed:** main · **Render:** redeploy `webflow-sync-server`

## Webflow — paste these

| Where | File in this folder |
|-------|---------------------|
| **Before `</body>`** | `WEBFLOW-PART2-before-body.html` |
| **Embed element** (only if form UI outdated) | Copy from repo: `webflow-embeds/PART1-embed-html-css.html` |

Part 2 is a thin loader (~713 chars). Calculator JS loads from Render after deploy — **no re-paste needed if you already use this loader.**

---

## Pricing rules (delivery + pickup)

### Formula
```
FINAL = (route + multi-item + large + heavy) × trucks + stairs + assembly
```

### Base
- **$95** ≈ 15 min one-way **truck** time (round-trip billing)
- Customer label: **billed at 95/hr** (or 130/hr / two-truck variants)
- **Truck time** = Google mapped time **+ 3 min** (never show raw Google time)
- Fine print: drive time includes **local truck travel time**

### Piece surcharges
| Rule | Amount |
|------|--------|
| Multi-item (3rd+ piece) | +$5 if footprint ≤ 24×24 in · +$10 if bigger |
| Large (any side > 60 in) | +$10 per piece (all pieces, incl. 1–2) |
| Heavy (> 300 lb) | +$10 per piece (all pieces) |
| Stairs | 1st flight free · +$7 per extra flight |

### 3-person crew ($130/hr)
- 299+ lb **and** over 72 in tall, **or** 450+ lb

### Disassembly & reassembly
- First **15 min included** — no charge
- Customer enters **extra minutes beyond 15** only
- Fee = extra min × (**$95 ÷ 60**), round up to **$5**
- **Does NOT** add to route/drive minutes
- Over **60 extra min** → confirm with customer flag

### Examples
| Scenario | Total |
|----------|-------|
| 1 chair, 15 min truck | $95 |
| 3 small items | $100 |
| 84" sofa | $105 |
| 320 lb dresser | $105 |
| +20 min assembly extra | $130 ($95 + $35) |
| 3 flights stairs | +$14 on top of route |

---

## Files changed (server)
- `lib/freightPalletize.js` — pricing engine
- `lib/freightQuoteEmail.js` — customer email copy
- `lib/freightLocalEstimate.js` — API estimate block
- `lib/freightQuoteValidation.js` — staff notes
- `public/embeds/freight-part2.js` — calculator JS (Render)
- `tests/freightQuote.test.js` — 48 tests

## Staff PDF
`../local-delivery-pickup-pricing-cheatsheet.pdf`

## Full code diff
`changes.patch` (git diff `d730695..3d4a27a`)
