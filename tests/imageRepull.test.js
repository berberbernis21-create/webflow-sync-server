import test from "node:test";
import assert from "node:assert/strict";
import axios from "axios";
import sharp from "sharp";
import {
  ensureShopifyProductHasIntakeImages,
  handleIntakeImagesReorder,
  imageAverageHash,
  imageHashCacheStatsForTests,
  intakePhotoHashKey,
  mergeQueuedSyncOptions,
  ownWriteSyncSkipsGalleryCheck,
  pairIntakePhotosToShopifyMedia,
  resetImageHashCacheForTests,
  shopifyImageHashKey,
  snapshotHash,
} from "../lib/shopifyIntakeImages.js";
import { resetShopifyThrottleForTests } from "../lib/shopifyThrottle.js";

/**
 * The cost this file pins down: every product sync downloaded each Intake
 * photo (S3, another AWS region) and each Shopify image just to fingerprint
 * it. A photo's fingerprint is now remembered by its version, so an unchanged
 * photo is never downloaded again, and the sync right after our own listing
 * or gallery write skips the photo compare.
 *
 * The accuracy side: a photo with new bytes (new pv) is downloaded and
 * fingerprinted again, a reorder still reaches Shopify, Shopify-side order is
 * still adopted, and an empty Intake gallery never wipes Shopify.
 *
 * Intake, Shopify and the image CDN are an in-memory backend behind the
 * axios adapter. Every image GET is counted.
 */

process.env.SHOPIFY_STORE = "repull-test-store";
process.env.SHOPIFY_ACCESS_TOKEN = "test-token";
process.env.INTAKE_SESSION_COOKIE = "session=test";
process.env.INTAKE_API_BASE = "https://intake.repull.test";
delete process.env.INTAKE_SYNC_HOOK_SECRET;

const ITEM_ID = "bbbbbbbb-cccc-4ddd-8eee-ffffffffffff";
const PHOTO_IDS = [
  "11111111-1111-4111-8111-111111111111",
  "22222222-2222-4222-8222-222222222222",
  "33333333-3333-4333-8333-333333333333",
];

/** 64x64 PNG made of 8x8 black/white cells from a seeded bit pattern. */
async function patternImage(seed) {
  let x = seed * 2654435761;
  const cells = [];
  for (let i = 0; i < 64; i += 1) {
    x = (x * 1103515245 + 12345) >>> 0;
    cells.push((x >>> 16) & 1);
  }
  const raw = Buffer.alloc(64 * 64);
  for (let y = 0; y < 64; y += 1) {
    for (let col = 0; col < 64; col += 1) {
      raw[y * 64 + col] = cells[Math.floor(y / 8) * 8 + Math.floor(col / 8)] ? 255 : 0;
    }
  }
  return sharp(raw, { raw: { width: 64, height: 64, channels: 1 } }).png().toBuffer();
}

let nextProductId = 7700;
let nextImageId = 880000;

async function makeWorld() {
  const pid = String((nextProductId += 1));
  const bytes = await Promise.all([patternImage(1), patternImage(2), patternImage(3)]);
  const state = { pid, calls: [], stamps: [], imageGets: [], bytesByUrl: new Map() };
  state.photos = PHOTO_IDS.map((id, i) => ({
    id,
    sortOrder: i,
    version: 1764000000 + i,
    updatedAt: `2026-11-20T10:0${i}:00.12345${i}+00:00`,
    sizeBytes: bytes[i].length,
    bytes: bytes[i],
  }));
  state.images = state.photos.map((photo, i) => {
    const id = (nextImageId += 1);
    const src = `https://cdn.shopify.com/s/files/1/0001/files/p${id}.png?v=${1764000100 + i}`;
    state.bytesByUrl.set(src, photo.bytes);
    return { id, product_id: Number(pid), position: i + 1, src };
  });
  state.snapshotIds = PHOTO_IDS.slice();
  state.product = {
    id: Number(pid),
    title: "Walnut Side Chair",
    body_html: "<p>A walnut side chair.</p>",
    tags: "FH, D",
    status: "active",
    updated_at: "2026-11-20T10:10:00Z",
    variants: [{ id: 501, sku: "TRX-1", price: "90.00" }],
  };

  const photoUrl = (photo) =>
    `/api/lost-and-found-intake/media/view/${photo.id}/tok${photo.id.slice(0, 4)}?v=full&pv=${photo.version}`;
  state.item = () => ({
    id: ITEM_ID,
    name: state.product.title,
    shopifyProductId: pid,
    shopifySyncSnapshot: JSON.stringify({ images: snapshotHash(state.snapshotIds.join("|")) }),
    photos: state.photos.map((photo) => ({
      id: photo.id,
      itemId: ITEM_ID,
      url: photoUrl(photo),
      thumbUrl: photoUrl(photo).replace("v=full", "v=thumb"),
      sortOrder: photo.sortOrder,
      sizeBytes: photo.sizeBytes,
      updatedAt: photo.updatedAt,
      version: photo.version,
    })),
  });
  state.restProduct = () => ({
    ...structuredClone(state.product),
    images: [...state.images].sort((a, b) => a.position - b.position).map((img) => ({ ...img })),
  });
  /** Shopify semantics: putting an image at a position shifts the others. */
  state.place = (image, position) => {
    const order = [...state.images].filter((img) => img !== image).sort((a, b) => a.position - b.position);
    if (image) order.splice(Math.max(0, Math.min(order.length, Number(position) - 1)), 0, image);
    order.forEach((img, i) => {
      img.position = i + 1;
    });
  };
  /** Rotate one photo in Intake: new bytes, new key, new pv. */
  state.rotate = async (index) => {
    const photo = state.photos[index];
    photo.bytes = await sharp(photo.bytes).rotate(90).png().toBuffer();
    photo.sizeBytes = photo.bytes.length;
    photo.version += 60;
    photo.updatedAt = "2026-11-21T09:00:00.654321+00:00";
  };

  const reply = (config, status, data, headers = {}) => ({
    data,
    status,
    statusText: String(status),
    headers,
    config,
    request: {},
  });
  state.adapter = async (config) => {
    const url = `${config.baseURL || ""}${config.url || ""}`;
    const method = String(config.method || "get").toUpperCase();
    const body = typeof config.data === "string" && config.data ? JSON.parse(config.data) : config.data;
    const shopify = /myshopify\.com\/admin\//.test(url);
    const path = url.replace(/^https?:\/\/[^/]+/, "").replace(/^\/admin\/api\/[^/]+/, "").replace(/\?.*$/, "");
    state.calls.push({ method, path, shopify, body, url });

    if (/cdn\.shopify\.com/.test(url)) {
      state.imageGets.push({ source: "shopify", url });
      const buf = state.bytesByUrl.get(url);
      return buf ? reply(config, 200, buf, { "content-type": "image/png" }) : reply(config, 404, Buffer.alloc(0));
    }
    if (/\/media\/view\//.test(url)) {
      state.imageGets.push({ source: "intake", url });
      const photo = state.photos.find((p) => url.includes(p.id) && url.includes(`pv=${p.version}`));
      return photo
        ? reply(config, 200, photo.bytes, { "content-type": "image/png" })
        : reply(config, 404, Buffer.alloc(0));
    }
    if (shopify) {
      if (path === "/graphql.json") {
        const query = String(body?.query || "");
        if (/productReorderMedia/.test(query)) {
          const order = [...state.images].sort((a, b) => a.position - b.position);
          for (const move of body.variables.moves) {
            const id = Number(String(move.id).match(/(\d+)$/)[1]);
            const from = order.findIndex((img) => img.id === id);
            const [img] = order.splice(from, 1);
            order.splice(Number(move.newPosition), 0, img);
          }
          order.forEach((img, i) => {
            img.position = i + 1;
          });
          return reply(config, 200, {
            data: { productReorderMedia: { job: { id: "job-1" }, mediaUserErrors: [] } },
          });
        }
        if (/ProductMedia\(/.test(query)) {
          const nodes = [...state.images]
            .sort((a, b) => a.position - b.position)
            .map((img) => ({ id: `gid://shopify/MediaImage/${img.id}`, image: { url: img.src } }));
          return reply(config, 200, { data: { product: { media: { nodes } } } });
        }
        return reply(config, 200, { data: {} });
      }
      if (path === `/products/${pid}.json` && method === "GET") {
        return reply(config, 200, { product: state.restProduct() });
      }
      if (path === `/products/${pid}.json` && method === "PUT") {
        Object.assign(state.product, body.product);
        return reply(config, 200, { product: state.restProduct() });
      }
      if (path === `/products/${pid}/metafields.json`) return reply(config, 200, { metafields: [] });
      if (path === `/products/${pid}/images.json` && method === "POST") {
        const id = (nextImageId += 1);
        const src = `https://cdn.shopify.com/s/files/1/0001/files/p${id}.png?v=1764009999`;
        if (body.image.attachment) state.bytesByUrl.set(src, Buffer.from(body.image.attachment, "base64"));
        const image = { id, product_id: Number(pid), position: 0, src };
        state.images.push(image);
        state.place(image, body.image.position || state.images.length);
        return reply(config, 200, { image: { ...image } });
      }
      const imageMatch = path.match(new RegExp(`^/products/${pid}/images/(\\d+)\\.json$`));
      if (imageMatch) {
        const image = state.images.find((img) => img.id === Number(imageMatch[1]));
        if (!image) return reply(config, 404, {});
        if (method === "DELETE") {
          state.images = state.images.filter((img) => img !== image);
          state.place(null, 0);
          return reply(config, 200, {});
        }
        if (method === "PUT") {
          state.place(image, body.image.position);
          return reply(config, 200, { image: { ...image } });
        }
      }
      return reply(config, 404, {});
    }
    if (path.endsWith(`/items/${ITEM_ID}`) && method === "GET") {
      return reply(config, 200, { item: state.item() });
    }
    if (/\/shopify-image-sync$/.test(path) && method === "POST") {
      state.stamps.push(body);
      if (Array.isArray(body?.photoIds) && body.photoIds.length) state.snapshotIds = body.photoIds.slice();
      return reply(config, 200, { ok: true });
    }
    if (/\/items$/.test(path) && method === "GET") return reply(config, 200, { items: [] });
    return reply(config, 200, { ok: true });
  };

  state.intakeGets = () => state.imageGets.filter((g) => g.source === "intake").length;
  state.shopifyGets = () => state.imageGets.filter((g) => g.source === "shopify").length;
  state.shopifyOrder = () =>
    [...state.images].sort((a, b) => a.position - b.position).map((img) => state.bytesByUrl.get(img.src));
  state.bodyWrites = () =>
    state.calls.filter(
      (c) => c.shopify && c.method === "PUT" && c.path === `/products/${pid}.json` && "body_html" in (c.body?.product || {})
    ).length;
  state.imageDeletes = () => state.calls.filter((c) => c.shopify && c.method === "DELETE").length;
  return state;
}

function use(world) {
  axios.defaults.adapter = world.adapter;
  resetShopifyThrottleForTests();
  resetImageHashCacheForTests();
  return world;
}

/** One product sync's image gate, as syncSingleProductCore runs it. */
function gate(world, options = {}) {
  return ensureShopifyProductHasIntakeImages(world.restProduct(), {
    log: () => {},
    intakeItemId: ITEM_ID,
    ...options,
  });
}

// ---------------------------------------------------------------------------
// Fix 1: an unchanged photo is fingerprinted once, then never downloaded again.
// ---------------------------------------------------------------------------

test("a product sync with unchanged photos downloads zero image bytes and gives the same result", async () => {
  const world = use(await makeWorld());
  const first = await gate(world);
  assert.equal(first.imageOrderAction, "noop", "in sync on the first pass");
  assert.equal(world.intakeGets(), 3, "first pass reads each Intake photo once");
  assert.equal(world.shopifyGets(), 3, "first pass reads each Shopify image once");

  const before = world.imageGets.length;
  const statsBefore = imageHashCacheStatsForTests();
  for (let i = 0; i < 4; i += 1) {
    const again = await gate(world);
    assert.equal(again.imageOrderAction, first.imageOrderAction);
    assert.equal(again.status, first.status);
    assert.equal(again.blocked, first.blocked);
  }
  assert.equal(world.imageGets.length - before, 0, "four more syncs download no image at all");
  const stats = imageHashCacheStatsForTests();
  assert.equal(stats.misses, statsBefore.misses, "no fingerprint is computed again");
  assert.equal(stats.hits - statsBefore.hits, 4 * 6, "every fingerprint came from memory");
});

test("a rotated photo (new pv) is downloaded and fingerprinted again", async () => {
  const world = use(await makeWorld());
  await gate(world);
  const intakeBefore = world.intakeGets();
  const shopifyBefore = world.shopifyGets();
  const oldKey = intakePhotoHashKey(world.item().photos[1].url, world.item());
  const oldHash = await imageAverageHash(world.photos[1].bytes);

  await world.rotate(1);
  const newKey = intakePhotoHashKey(world.item().photos[1].url, world.item());
  assert.notEqual(newKey, oldKey, "new bytes give a new key");
  assert.notEqual(await imageAverageHash(world.photos[1].bytes), oldHash, "the rotated file fingerprints differently");

  const missesBefore = imageHashCacheStatsForTests().misses;
  await gate(world);
  assert.equal(world.intakeGets() - intakeBefore, 1, "only the rotated photo is downloaded");
  assert.equal(imageHashCacheStatsForTests().misses - missesBefore, 1, "and only it is fingerprinted again");
  assert.equal(world.shopifyGets() - shopifyBefore, 0, "unchanged Shopify files are not downloaded");
  const rotatedUrl = world.imageGets.filter((g) => g.source === "intake").at(-1).url;
  assert.match(rotatedUrl, new RegExp(`${PHOTO_IDS[1]}.*pv=${world.photos[1].version}`), "the new version was read");
});

test("a rotated photo is uploaded by a full gallery reconcile and the old file is replaced", async () => {
  const world = use(await makeWorld());
  await gate(world);
  await world.rotate(1);
  const rotatedBytes = world.photos[1].bytes;
  const result = await handleIntakeImagesReorder(
    { intakeItemId: ITEM_ID, shopifyProductId: world.pid },
    { log: () => {} }
  );
  assert.equal(result.ok, true);
  assert.equal(result.uploaded, 1, "the rotated photo is uploaded");
  assert.equal(result.deleted, 1, "the old orientation leaves Shopify");
  const order = world.shopifyOrder();
  assert.equal(order.length, 3);
  assert.ok(order[1].equals(rotatedBytes), "the rotated photo sits in its Intake position");
  assert.ok(order[0].equals(world.photos[0].bytes));
  assert.ok(order[2].equals(world.photos[2].bytes));
});

test("a photo URL without a provable version is downloaded as before", () => {
  const item = {
    photos: [{ id: PHOTO_IDS[0], updatedAt: "2026-11-20T10:00:00.1+00:00", version: 5, sizeBytes: 9 }],
  };
  const base = `https://intake.repull.test/api/lost-and-found-intake/media/view/${PHOTO_IDS[0]}/tok`;
  assert.ok(intakePhotoHashKey(`${base}?v=full&pv=5`, item));
  assert.equal(intakePhotoHashKey(`${base}?v=full`, item), null, "no pv");
  assert.equal(intakePhotoHashKey(`${base}?v=full&pv=0`, item), null, "pv 0");
  assert.equal(intakePhotoHashKey(`${base}?v=full&pv=6`, item), null, "pv differs from the record");
  assert.equal(intakePhotoHashKey(`${base}?v=full&pv=5`, null), null, "no photo record");
  assert.equal(
    intakePhotoHashKey(`${base}?v=full&pv=5`, { photos: [{ id: PHOTO_IDS[0], version: 5 }] }),
    null,
    "no updatedAt"
  );
  assert.notEqual(
    intakePhotoHashKey(`${base}?v=full&pv=5`, item),
    intakePhotoHashKey(`${base}?v=thumb&pv=5`, item),
    "full and thumb are fingerprinted separately"
  );
  assert.ok(shopifyImageHashKey("https://cdn.shopify.com/s/files/1/2/files/a.jpg?v=17"));
  assert.equal(shopifyImageHashKey("https://cdn.shopify.com/s/files/1/2/files/a.jpg"), null, "no ?v=");
  assert.equal(shopifyImageHashKey("https://example.com/a.jpg?v=1"), null, "not Shopify");
});

// ---------------------------------------------------------------------------
// Reorder and adopt: order comes from Intake sort order / Shopify positions.
// ---------------------------------------------------------------------------

test("an Intake reorder reaches Shopify with no download and no description write", async () => {
  const world = use(await makeWorld());
  await gate(world);
  const before = world.imageGets.length;

  // Move the last photo to the front in Intake (sort order only; pv unchanged).
  world.photos[2].sortOrder = -1;
  const result = await gate(world);
  assert.equal(result.imageOrderAction, "push_intake");
  assert.equal(world.imageGets.length - before, 0, "a reorder downloads nothing");
  const order = world.shopifyOrder();
  assert.ok(order[0].equals(world.photos[2].bytes), "Shopify shows the moved photo first");
  assert.ok(order[1].equals(world.photos[0].bytes));
  assert.ok(order[2].equals(world.photos[1].bytes));
  assert.equal(world.bodyWrites(), 0, "the description is not rewritten");
  assert.equal(world.imageDeletes(), 0);

  // Echo: the next sync sees Shopify and Intake agree and does nothing.
  const echo = await gate(world);
  assert.equal(echo.imageOrderAction, "noop", "our own reorder does not come back as a change");
  assert.equal(world.imageGets.length - before, 0);
});

test("a Shopify-side reorder is still adopted into Intake", async () => {
  const world = use(await makeWorld());
  await gate(world);
  const before = world.imageGets.length;
  world.images[0].position = 2;
  world.images[1].position = 1;
  const result = await gate(world);
  assert.equal(result.imageOrderAction, "adopt_shopify");
  assert.deepEqual(world.snapshotIds, [PHOTO_IDS[1], PHOTO_IDS[0], PHOTO_IDS[2]]);
  assert.equal(world.imageGets.length - before, 0, "adopting downloads nothing");
});

test("an empty Intake gallery never wipes Shopify images", async () => {
  const world = use(await makeWorld());
  world.photos = [];
  const result = await handleIntakeImagesReorder(
    { intakeItemId: ITEM_ID, shopifyProductId: world.pid },
    { log: () => {} }
  );
  assert.equal(result.status, "skipped_empty_gallery");
  assert.equal(world.imageDeletes(), 0);
  assert.equal(world.images.length, 3);
});

test("the matching threshold is still 12", async () => {
  const base = await imageAverageHash(await patternImage(9));
  const flip = (hash, n) => {
    let out = BigInt(hash);
    for (let i = 0; i < n; i += 1) out ^= 1n << BigInt(i);
    return out;
  };
  assert.deepEqual(pairIntakePhotosToShopifyMedia([base], [flip(base, 12)]), [0], "12 bits apart pairs");
  assert.deepEqual(pairIntakePhotosToShopifyMedia([base], [flip(base, 13)]), [null], "13 bits apart does not");
});

// ---------------------------------------------------------------------------
// Fix 2: the sync after our own write skips the photo compare.
// ---------------------------------------------------------------------------

test("the sync after a listing write skips the photo compare; a normal sync still runs it", async () => {
  const world = use(await makeWorld());
  const skipped = await gate(world, { skipGalleryCheck: true });
  assert.equal(skipped.imageOrderAction, "skipped_own_write");
  assert.equal(skipped.blocked, false, "Webflow/Google are not held back");
  assert.equal(world.imageGets.length, 0, "no photo is downloaded");
  assert.equal(
    world.calls.filter((c) => !c.shopify && c.path.endsWith(`/items/${ITEM_ID}`)).length,
    0,
    "the Intake item is not even read"
  );
  assert.ok(
    world.calls.some((c) => c.path === `/products/${world.pid}/metafields.json`),
    "Shopify metafields (dimensions) are still loaded for the downstream sync"
  );
  assert.equal(world.stamps.length, 0, "nothing is stamped as checked");

  const full = await gate(world);
  assert.equal(full.imageOrderAction, "noop");
  assert.equal(world.intakeGets(), 3, "a normal sync still compares the photos");
});

test("skipping the compare never skips uploading photos to a product with none", async () => {
  const world = use(await makeWorld());
  world.images = [];
  const result = await gate(world, { skipGalleryCheck: true });
  assert.equal(result.status, "uploaded");
  assert.equal(result.uploaded, 3);
  assert.equal(world.images.length, 3);
});

test("which follow-up syncs may skip the photo compare", () => {
  const pid = "123";
  assert.equal(ownWriteSyncSkipsGalleryCheck("listing_update", { ok: true, shopifyProductId: pid }), true);
  assert.equal(
    ownWriteSyncSkipsGalleryCheck("gallery_reorder", { ok: true, shopifyProductId: pid, status: "reordered" }),
    true,
    "a finished gallery reconcile"
  );
  assert.equal(
    ownWriteSyncSkipsGalleryCheck("gallery_reorder", {
      ok: false,
      shopifyProductId: pid,
      status: "reorder_pending",
      retry: true,
    }),
    false,
    "an unfinished gallery write keeps the check"
  );
  assert.equal(ownWriteSyncSkipsGalleryCheck("webhook", { ok: true, shopifyProductId: pid }), false);
  assert.equal(ownWriteSyncSkipsGalleryCheck("listing_update", { ok: true }), false, "no product");

  // A webhook (real change) collapsed with our follow-up keeps the full check.
  const own = { skipIntakeImageGate: true };
  const webhook = {};
  assert.deepEqual(mergeQueuedSyncOptions(undefined, own), { skipIntakeImageGate: true });
  assert.deepEqual(mergeQueuedSyncOptions(own, webhook), { skipIntakeImageGate: false });
  assert.deepEqual(mergeQueuedSyncOptions({ skipIntakeImageGate: false }, own), { skipIntakeImageGate: false });
  assert.deepEqual(mergeQueuedSyncOptions(own, own), { skipIntakeImageGate: true });
});
