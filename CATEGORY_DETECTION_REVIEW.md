# Luxury Category Detection — Full Review

## Overview

Luxury products get a **category** (Handbags, Totes, Backpacks, etc.) from two sources, in order:

1. **product_type** → `getLuxuryCategoryFromType()` — exact type mapping
2. **title + description** → `detectLuxuryCategoryFromTitle()` — keyword matching (when type returns "Other ")

Title/description uses **word-boundary matching** and iterates categories in **insertion order**. The **first match wins**.

---

## Flow (Luxury Goods only)

**Title/description first, product_type fallback.** We run through the full category keyword list from title+description before trusting product_type.

```
1. isShoeProduct(title, description) → if match, "Other "
2. detectLuxuryCategoryFromTitle(title, description) → iterate all CATEGORY_KEYWORDS
   └─ If match → use it (title semantics win over product_type)
3. getLuxuryCategoryFromType(productType) → fallback only when title/description yield no match
```

detectLuxuryCategoryFromTitle(title, description):
  1. Combine title + description (HTML stripped), lowercase
  2. SHOE_KEYWORDS check → if match, return null → "Other "
  3. Loop CATEGORY_KEYWORDS in object order, first match wins
  4. No match → null → "Other "
```

---

## Current Category Order (CATEGORY_KEYWORDS)

| # | Category      | Some keywords                                           |
|---|---------------|---------------------------------------------------------|
| 1 | Handbags      | handbag, shoulder bag, satchel, day bag, bucket bag...  |
| 2 | Totes         | tote, cabas, book tote, carryall...                     |
| 3 | Crossbody     | crossbody, camera bag, woc, chain bag...                |
| 4 | **Backpacks** | backpack, drawstring backpack, daypack, rucksack...     |
| 5 | Small Bags    | clutch, pochette, pouch, wristlet, minaudiere...        |
| 6 | Wallets       | wallet, cardholder, passport holder, coin purse...      |
| 7 | Luggage       | luggage, briefcase, weekender, duffle, keepall...       |
| 8 | Scarves       | scarf, shawl, wrap, stole...                            |
| 9 | Belts         | belt, waist belt, chain belt...                         |
|10 | Accessories   | belt bag, strap, cosmetic pouch, dust bag...            |

---

## Sources of Misclassification

### 1. Order sensitivity (fixed: Backpacks before Small Bags)

- **Problem:** Descriptions often mention accessories (e.g. "comes with a clutch"). "clutch" is in Small Bags. If Small Bags was checked before Backpacks, a backpack product could match Small Bags.
- **Fix applied:** Backpacks moved before Small Bags so "backpack" wins when both appear.

### 2. Accessory mentions in description

- **Problem:** Title + description are concatenated with equal weight. So:
  - "Chanel Drawstring Backpack" + "comes with a chic clutch" → both "backpack" and "clutch" in text.
  - Order fix helps when the main product keyword appears; if the main keyword is vague, accessory words can still steal the match.
- **Possible improvement:** Weight title higher than description (e.g. require title match for ambiguous cases, or score title matches more heavily).

### 3. Overlapping keywords across categories

| Term         | In Category   | Risk                                                                 |
|--------------|---------------|----------------------------------------------------------------------|
| "pochette"   | Small Bags, Crossbody ("multi pochette") | Order-dependent; Crossbody checked first                            |
| "sling bag"  | Crossbody     | Some sling bags are backpacks; Crossbody comes before Backpacks      |
| "strap"      | Accessories   | Many bags mention "strap" in description → could match Accessories   |
| "chain bag"  | Crossbody     | Overlaps with handbag styles                                         |

### 4. product_type vs title/description

- **product_type** uses **exact** key match: "backpack" → Backpacks, but "drawstring backpack" → no match → falls to title detection.
- If product_type is wrong or generic (e.g. "Handbag" for a backpack), we rely entirely on title/description. Order and keywords matter.

### 5. Shoes (handled)

- SHOE_KEYWORDS checked first; any shoe term → "Other ". Prevents shoes from matching Handbags, Accessories, etc.

---

## Recommendations

### Already done

- [x] Backpacks before Small Bags (order)
- [x] Shoes → Other (SHOE_KEYWORDS early return)
- [x] Title-first logic — match on title before description; accessory mentions in description no longer override main product

### Optional improvements

1. **Title-priority scoring (advanced)** — superseded by title-first two-pass approach  
   Score matches by location: title match = 2, description match = 1. Pick highest score. Would require refactoring `detectLuxuryCategoryFromTitle` from "first match" to "best score".

2. **More specific before generic**  
   Consider checking more specific categories (Backpacks, Crossbody, Luggage) before broad ones (Handbags, Accessories). Current order is already partially optimized.

3. **"Bag" in Handbags**  
   "bag" is very generic. If it causes wrong matches, consider:
   - Removing "bag" from Handbags and relying on "handbag", "shoulder bag", etc., or
   - Making "bag" require additional context (e.g. not matching when "backpack", "tote", "luggage" also present).

4. **"strap" in Accessories**  
   Many bag descriptions say "leather strap", "detachable strap". If bags are wrongly classified as Accessories, consider removing "strap" or making it conditional.

5. **Document product_type best practices**  
   Encourage consistent Shopify product_type values (e.g. "Backpack", "Handbag", "Clutch") so type-based mapping works and title fallback is needed less often.

---

## Quick reference: Category order

```
Handbags → Totes → Crossbody → Backpacks → Small Bags → Wallets → Luggage → Scarves → Belts → Accessories
```

First match wins. Shoes go to Other before any category check.
