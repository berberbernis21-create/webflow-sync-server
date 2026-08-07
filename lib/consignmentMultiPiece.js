/**
 * Detect when one consignment "item" is actually multiple distinct pieces.
 * Only call out when confidence is medium/high — avoid false alarms on matching sets.
 */

const WORD_COUNTS = {
  two: 2,
  three: 3,
  four: 4,
  five: 5,
  six: 6,
  seven: 7,
  eight: 8,
  nine: 9,
  ten: 10,
};

const PIECE_NOUNS =
  /\b(pieces?|items?|artworks?|paintings?|prints?|sculptures?|canvases|chairs?|sofas?|sectionals?|tables?|lamps?|rugs?|mirrors?|dressers?|nightstands?)\b/i;

const MATCHING_SET_HINT =
  /\b(set|pair|matching|ensemble|collection of matching|dining set|patio set|bedroom set|sectional)\b/i;

const DISTINCT_ART_HINT =
  /\b(art|artwork|paintings?|prints?|sculptures?|canvases|mixed media|gallery)\b/i;

const UNRELATED_HINT =
  /\b(different|various|assorted|unrelated|separate|misc|miscellaneous|and a |plus a )\b/i;

function parseCountToken(token) {
  const t = String(token || "")
    .trim()
    .toLowerCase();
  if (WORD_COUNTS[t]) return WORD_COUNTS[t];
  const n = parseInt(t, 10);
  return Number.isFinite(n) && n >= 2 && n <= 20 ? n : null;
}

/**
 * Title/notes heuristics for multi-piece submissions.
 * @returns {{ distinctItemCount: number, confidence: 'high'|'medium'|'low'|null, reason: string }|null}
 */
export function detectMultiPieceFromText(item) {
  const name = String(item?.itemName || "").trim();
  const notes = String(item?.notes || "").trim();
  const blob = `${name} ${notes}`.trim();
  if (!blob) return null;

  // "2 High end art pieces…" / "three chairs and a table"
  const leading = blob.match(
    /^\s*(\d+|two|three|four|five|six|seven|eight|nine|ten)\s+(.{0,80}?)(?:\b(pieces?|items?|artworks?|paintings?|prints?|sculptures?|chairs?|sofas?|tables?)\b)/i
  );
  if (leading) {
    const count = parseCountToken(leading[1]);
    if (count && count >= 2) {
      const rest = `${leading[2] || ""} ${leading[3] || ""}`;
      if (MATCHING_SET_HINT.test(blob) && !DISTINCT_ART_HINT.test(blob) && !UNRELATED_HINT.test(blob)) {
        return {
          distinctItemCount: count,
          confidence: "medium",
          reason: `Title suggests ${count} pieces that may be a matching set — confirm from photos whether they are one SKU or distinct items.`,
        };
      }
      if (DISTINCT_ART_HINT.test(blob) || UNRELATED_HINT.test(blob) || /art pieces/i.test(blob)) {
        return {
          distinctItemCount: count,
          confidence: "high",
          reason: `Title explicitly describes ${count} distinct pieces (not a single SKU). Price/analyze each separately.`,
        };
      }
      return {
        distinctItemCount: count,
        confidence: "medium",
        reason: `Title suggests ${count} pieces in one item slot — confirm from photos whether they are matching or distinct.`,
      };
    }
  }

  // "sofa and chairs", "painting and sculpture"
  if (
    /\b(and|&)\b/i.test(name) &&
    PIECE_NOUNS.test(name) &&
    (UNRELATED_HINT.test(blob) || DISTINCT_ART_HINT.test(name) || /\b(sofa|sectional|table|chair|painting|print).{0,40}\b(and|&)\b.{0,40}\b(sofa|chair|table|painting|print|lamp|rug)\b/i.test(name))
  ) {
    return {
      distinctItemCount: 2,
      confidence: "medium",
      reason: "Title lists multiple furniture/art types together — confirm distinct items from photos.",
    };
  }

  return null;
}

/**
 * Merge title heuristics + model photoBundle. Only flag when confidence is medium/high.
 */
export function resolvePhotoBundle(item, llmBundle = {}, photoCount = 0) {
  const textHit = detectMultiPieceFromText(item);
  const llmMixed = Boolean(llmBundle?.mixedItemsDetected);
  const llmCount = Math.max(1, Math.round(Number(llmBundle?.distinctItemCount) || 1));
  const llmConfidence = ["high", "medium", "low"].includes(
    String(llmBundle?.mixedItemsConfidence || "").toLowerCase()
  )
    ? String(llmBundle.mixedItemsConfidence).toLowerCase()
    : llmMixed
      ? "medium"
      : null;

  const pieces = Array.isArray(llmBundle?.pieces)
    ? llmBundle.pieces
        .map((p, idx) => ({
          label: String(p?.label || `Piece ${idx + 1}`).trim().slice(0, 80),
          description: String(p?.description || "").trim().slice(0, 400),
          pricingNote: String(p?.pricingNote || "").trim().slice(0, 240),
        }))
        .filter((p) => p.description || p.label)
        .slice(0, 8)
    : [];

  let mixed = false;
  let confidence = null;
  let distinctItemCount = 1;
  let reason = "";

  if (textHit?.confidence === "high") {
    mixed = true;
    confidence = "high";
    distinctItemCount = Math.max(textHit.distinctItemCount, llmCount > 1 ? llmCount : 1);
    reason = textHit.reason;
  } else if (llmMixed && (llmConfidence === "high" || llmConfidence === "medium") && llmCount > 1) {
    mixed = true;
    confidence = llmConfidence;
    distinctItemCount = llmCount;
    reason =
      String(llmBundle?.photoObservations || "").trim() ||
      "Vision analysis indicates multiple distinct pieces in this item's photos.";
  } else if (textHit?.confidence === "medium" && (llmMixed || photoCount >= 2)) {
    // Title hints + photos/model support — still call out, but medium confidence.
    mixed = true;
    confidence = "medium";
    distinctItemCount = Math.max(textHit.distinctItemCount, llmCount > 1 ? llmCount : textHit.distinctItemCount);
    reason = textHit.reason;
  } else if (textHit?.confidence === "medium" && !llmMixed) {
    // Title-only medium without model confirmation: note for reviewer, do not hard-flag as confirmed mixed.
    mixed = false;
    confidence = "low";
    distinctItemCount = textHit.distinctItemCount;
    reason = `${textHit.reason} (not confirmed from photos yet).`;
  }

  const observations = [
    String(llmBundle?.photoObservations || "").trim(),
    reason && mixed ? reason : "",
    !mixed && reason ? reason : "",
  ]
    .filter(Boolean)
    .join(" ")
    .slice(0, 1600);

  return {
    mixedItemsDetected: mixed,
    mixedItemsConfidence: confidence,
    distinctItemCount: mixed ? distinctItemCount : Math.max(1, distinctItemCount),
    photoObservations: observations,
    pieces: mixed ? pieces : pieces.slice(0, 0),
    titleHint: textHit,
  };
}

/** True when we should show the multi-piece callout to the team. */
export function shouldShowMultiPieceCallout(photoBundle) {
  if (!photoBundle?.mixedItemsDetected) return false;
  const c = String(photoBundle.mixedItemsConfidence || "").toLowerCase();
  return c === "high" || c === "medium";
}

/**
 * Expand into separate analysis items only when we are confident they are distinct
 * and the submission supports it (customer wording + enough photos when medium).
 */
export function shouldExpandIntoSeparateItems(item, photoCount = 0) {
  if (item?._splitChild) return false;
  const bundle = resolvePhotoBundle(item, item?.photoBundle || {}, photoCount);
  if (!bundle.mixedItemsDetected || bundle.distinctItemCount < 2) return false;
  if (bundle.mixedItemsConfidence === "high") return true;
  // Medium: require at least one photo per piece so the split is grounded in the upload.
  if (
    bundle.mixedItemsConfidence === "medium" &&
    photoCount >= bundle.distinctItemCount
  ) {
    return true;
  }
  return false;
}

function stripLeadingCount(name) {
  return String(name || "")
    .replace(
      /^\s*(\d+|two|three|four|five|six|seven|eight|nine|ten)\s+/i,
      ""
    )
    .trim();
}

function singularizePiecePhrase(text) {
  return String(text || "")
    .replace(/\bpieces\b/gi, "piece")
    .replace(/\bitems\b/gi, "item")
    .replace(/\bartworks\b/gi, "artwork")
    .replace(/\bpaintings\b/gi, "painting")
    .replace(/\bprints\b/gi, "print")
    .replace(/\bsculptures\b/gi, "sculpture")
    .replace(/\bchairs\b/gi, "chair")
    .replace(/\bsofas\b/gi, "sofa")
    .replace(/\btables\b/gi, "table")
    .trim();
}

export function buildSplitPieceName(parentItem, pieceIndex, pieceCount, pieceMeta = null) {
  if (pieceMeta?.label) {
    return `${String(pieceMeta.label).trim()} (${pieceIndex} of ${pieceCount})`;
  }
  const stripped = singularizePiecePhrase(stripLeadingCount(parentItem?.itemName));
  const m = stripped.match(
    /^(.*?\b(?:piece|item|artwork|painting|print|sculpture|chair|sofa|table))\b(.*)$/i
  );
  if (m) {
    const head = m[1].trim();
    const tail = m[2].trim().replace(/^[\s,–—-]+/, "");
    return tail
      ? `${head} ${pieceIndex} of ${pieceCount} — ${tail}`
      : `${head} ${pieceIndex} of ${pieceCount}`;
  }
  const base = stripped || "Piece";
  return `${base} ${pieceIndex} of ${pieceCount}`;
}

/** Chunk photos across pieces (sequential groups — customers usually shoot one piece, then the next). */
export function assignPhotosToPieces(photos, pieceCount) {
  const n = Math.max(1, Math.floor(Number(pieceCount) || 1));
  const list = Array.isArray(photos) ? photos : [];
  if (n === 1) return [list];
  if (!list.length) return Array.from({ length: n }, () => []);

  if (list.length < n) {
    // Not enough photos to isolate — share all so each piece still gets vision context.
    return Array.from({ length: n }, () => [...list]);
  }

  const groups = Array.from({ length: n }, () => []);
  const base = Math.floor(list.length / n);
  let remainder = list.length % n;
  let idx = 0;
  for (let i = 0; i < n; i += 1) {
    const take = base + (remainder > 0 ? 1 : 0);
    if (remainder > 0) remainder -= 1;
    groups[i] = list.slice(idx, idx + take);
    idx += take;
  }
  return groups;
}

/**
 * Split confident multi-piece form lines into separate items for internal pricing/email/PDF.
 * Customer-facing confirmation should keep the original `items` / photos.
 *
 * @returns {{ items: object[], photoGroups: Map<number, object[]>, splitCount: number, notes: string[] }}
 */
export function expandConfidentMultiPieceItems(items, photoGroups, { maxItems = 10 } = {}) {
  const cap = Math.max(1, Math.floor(Number(maxItems) || 10));
  const sourceGroups = photoGroups instanceof Map ? photoGroups : new Map();
  const outItems = [];
  const outPhotos = new Map();
  const notes = [];
  let splitCount = 0;
  let nextNumber = 1;

  for (let i = 0; i < (items || []).length; i += 1) {
    const item = items[i];
    const parentNum = Number(item?.itemNumber) > 0 ? Number(item.itemNumber) : i + 1;
    const photos = sourceGroups.get(parentNum) || [];
    const remainingSlots = cap - outItems.length;

    if (remainingSlots <= 0) break;

    if (!shouldExpandIntoSeparateItems(item, photos.length)) {
      const copy = { ...item, itemNumber: nextNumber };
      outItems.push(copy);
      outPhotos.set(nextNumber, photos);
      nextNumber += 1;
      continue;
    }

    const bundle = resolvePhotoBundle(item, item?.photoBundle || {}, photos.length);
    const pieceCount = Math.min(bundle.distinctItemCount, remainingSlots);
    if (pieceCount < 2) {
      const copy = { ...item, itemNumber: nextNumber };
      outItems.push(copy);
      outPhotos.set(nextNumber, photos);
      nextNumber += 1;
      continue;
    }

    const photoChunks = assignPhotosToPieces(photos, pieceCount);
    const sharedPhotos = photos.length < pieceCount;
    notes.push(
      `Submission Item #${parentNum} ("${String(item?.itemName || "").slice(0, 80)}") was split into ${pieceCount} separate analysis items (confidence ${String(bundle.mixedItemsConfidence || "").toUpperCase()}).`
    );
    splitCount += 1;

    for (let p = 0; p < pieceCount; p += 1) {
      const pieceMeta = bundle.pieces?.[p] || null;
      const pieceName = buildSplitPieceName(item, p + 1, pieceCount, pieceMeta);
      const warnParts = [
        String(item?.warnings || "").trim(),
        `Split from submission Item #${parentNum} (${p + 1} of ${pieceCount}; confidence ${String(bundle.mixedItemsConfidence || "").toUpperCase()}).`,
        bundle.photoObservations || bundle.titleHint?.reason || "",
        sharedPhotos
          ? "Photos were shared across split pieces (fewer photos than pieces) — confirm assignment manually."
          : "Photos partitioned for this piece from the original upload.",
      ].filter(Boolean);

      outItems.push({
        ...item,
        itemNumber: nextNumber,
        itemName: pieceName,
        _splitChild: true,
        splitFromItemNumber: parentNum,
        splitPieceIndex: p + 1,
        splitPieceCount: pieceCount,
        splitConfidence: bundle.mixedItemsConfidence,
        splitReason: bundle.photoObservations || bundle.titleHint?.reason || "",
        notes: [String(item?.notes || "").trim(), pieceMeta?.description || ""]
          .filter(Boolean)
          .join(" | "),
        warnings: warnParts.join(" "),
        // Prevent re-detecting "2 art pieces" on the child name.
        photoBundle: {
          mixedItemsDetected: false,
          mixedItemsConfidence: null,
          distinctItemCount: 1,
          photoObservations: pieceMeta?.description || "",
          pieces: [],
        },
      });
      outPhotos.set(nextNumber, photoChunks[p] || []);
      nextNumber += 1;
    }
  }

  return { items: outItems, photoGroups: outPhotos, splitCount, notes };
}
