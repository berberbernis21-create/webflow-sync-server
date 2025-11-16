import express from "express";
import axios from "axios";
import cors from "cors";
import dotenv from "dotenv";
import { CATEGORY_KEYWORDS } from "./categoryKeywords.js";

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json());

/* ======================================================
   CATEGORY DETECTOR (Simple + Reliable)
   (for "normal" categories like Handbags, Wallets, etc.)
====================================================== */
function detectCategory(title) {
  if (!title) return "Other";

  const normalized = title.toLowerCase();

  for (const [category, keywords] of Object.entries(CATEGORY_KEYWORDS)) {
    for (const keyword of keywords) {
      if (normalized.includes(keyword.toLowerCase())) {
        return category;
      }
    }
  }

  return "Other";
}

/* ======================================================
   SHOPIFY: FETCH ALL PRODUCTS (since_id pagination)
====================================================== */

async function fetchAllShopifyProducts() {
  const store = process.env.SHOPIFY_STORE;
  const token = process.env.SHOPIFY_ACCESS_TOKEN;

  if (!store || !token) {
    throw new Error("Missing SHOPIFY_STORE or SHOPIFY_ACCESS_TOKEN");
  }

  let allProducts = [];
  let lastId = 0;

  while (true) {
    const baseUrl = `https://${store}.myshopify.com/admin/api/2024-01/products.json`;
    const url =
      lastId === 0
        ? `${baseUrl}?limit=250`
        : `${baseUrl}?limit=250&since_id=${lastId}`;

    console.log("🛒 Fetching Shopify products:", url);

    const response = await axios.get(url, {
      headers: {
        "X-Shopify-Access-Token": token,
        "Content-Type": "application/json",
      },
    });

    const products = response.data.products || [];
    console.log(`   → Received ${products.length} products`);

    if (!products.length) break;

    allProducts = allProducts.concat(products);
    lastId = products[products.length - 1].id;

    if (products.length < 250) break; // no more pages
  }

  console.log(`📦 Total Shopify products fetched: ${allProducts.length}`);
  return allProducts;
}

/* ======================================================
   WEBFLOW: FIND EXISTING ITEM BY shopify-product-id
====================================================== */

async function findExistingWebflowItem(shopifyProductId) {
  let page = 1;

  while (true) {
    const url = `https://api.webflow.com/v2/collections/${process.env.WEBFLOW_COLLECTION_ID}/items?page=${page}&limit=100`;

    const response = await axios.get(url, {
      headers: {
        Authorization: `Bearer ${process.env.WEBFLOW_TOKEN}`,
        accept: "application/json",
      },
    });

    const items = response.data.items || [];

    for (const item of items) {
      if (item.fieldData?.["shopify-product-id"] === String(shopifyProductId)) {
        return item;
      }
    }

    if (!response.data.pagination?.nextPage) break;

    page = response.data.pagination.nextPage;
  }

  return null;
}

/* ======================================================
   CORE: SYNC A SINGLE SHOPIFY PRODUCT → WEBFLOW
====================================================== */

async function syncSingleProduct(product) {
  const shopifyProductId = product.id;
  const name = product.title;
  const brand = product.vendor;
  const description = product.body_html;
  const price = product.variants?.[0]?.price || null;
  const slug = product.handle;
  const shopifyUrl = `https://${process.env.SHOPIFY_STORE}.myshopify.com/products/${slug}`;

  // Images
  const allImages = (product.images || []).map((img) => img.src);
  const featuredImage = product.image?.src || allImages[0] || null;
  const gallery = allImages.filter((url) => url !== featuredImage);

  // Inventory (for SOLD logic)
  const firstVariant = product.variants?.[0];
  const inventoryQty = typeof firstVariant?.inventory_quantity === "number"
    ? firstVariant.inventory_quantity
    : null;

  const normalizedTitle = (name || "").toLowerCase();

  const isSoldByTitle =
    normalizedTitle.includes("sold") ||
    normalizedTitle.includes("reserved");

  const isSoldByInventory =
    inventoryQty !== null && inventoryQty <= 0;

  const isRecentlySold = isSoldByTitle || isSoldByInventory;

  // Base category from keywords
  let category = detectCategory(name);

  // Override category + visibility if Recently Sold
  let showOnWebflow = true;
  if (isRecentlySold) {
    category = "Recently Sold";
    showOnWebflow = false;
  }

  console.log(
    `🔁 Syncing product ${shopifyProductId} | "${name}" | qty=${inventoryQty} | RecentlySold=${isRecentlySold}`
  );

  const fieldDataBase = {
    name,
    brand,
    price,
    description,
    "shopify-product-id": String(shopifyProductId),
    "shopify-url": shopifyUrl,
    category,

    // Images
    "featured-image": featuredImage ? { url: featuredImage } : null,
    "image-1": gallery[0] ? { url: gallery[0] } : null,
    "image-2": gallery[1] ? { url: gallery[1] } : null,
    "image-3": gallery[2] ? { url: gallery[2] } : null,
    "image-4": gallery[3] ? { url: gallery[3] } : null,
    "image-5": gallery[4] ? { url: gallery[4] } : null,

    "show-on-webflow": showOnWebflow,
    "featured-item-on-homepage": false,
  };

  const existing = await findExistingWebflowItem(shopifyProductId);

  if (existing) {
    console.log("✏️ Updating Webflow item:", existing.id);

    const fieldData = {
      ...fieldDataBase,
      slug: existing.fieldData.slug, // keep existing slug
    };

    await axios.patch(
      `https://api.webflow.com/v2/collections/${process.env.WEBFLOW_COLLECTION_ID}/items/${existing.id}`,
      { fieldData },
      {
        headers: {
          Authorization: `Bearer ${process.env.WEBFLOW_TOKEN}`,
          "Content-Type": "application/json",
        },
      }
    );

    return { operation: "update", id: existing.id };
  } else {
    console.log("🆕 Creating new Webflow item…");

    const fieldData = {
      ...fieldDataBase,
      slug, // use Shopify slug on creation
    };

    const createResp = await axios.post(
      `https://api.webflow.com/v2/collections/${process.env.WEBFLOW_COLLECTION_ID}/items`,
      { fieldData },
      {
        headers: {
          Authorization: `Bearer ${process.env.WEBFLOW_TOKEN}`,
          "Content-Type": "application/json",
        },
      }
    );

    return { operation: "create", id: createResp.data.id };
  }
}

/* ======================================================
   ROUTES
====================================================== */

app.get("/", (req, res) => {
  res.send("Lost & Found – Full Shopify → Webflow Sync (No Make.com)");
});

/**
 * 🔄 FULL SYNC ENDPOINT
 * Call this from a cron / Render job / button in an internal tool
 */
app.post("/sync-all", async (req, res) => {
  try {
    console.log("🔄 FULL SYNC STARTED…");

    const products = await fetchAllShopifyProducts();

    let created = 0;
    let updated = 0;

    for (const product of products) {
      try {
        const result = await syncSingleProduct(product);
        if (result.operation === "create") created++;
        if (result.operation === "update") updated++;
      } catch (innerErr) {
        console.error("⚠️ Error syncing product", product.id, innerErr.toString());
        if (innerErr.response) console.error("🔻", innerErr.response.data);
      }
    }

    console.log("✅ FULL SYNC COMPLETE:", {
      total: products.length,
      created,
      updated,
    });

    res.json({
      status: "ok",
      message: "Full sync complete",
      total: products.length,
      created,
      updated,
    });
  } catch (err) {
    console.error("🔥 /sync-all ERROR:", err);
    if (err.response) console.error("🔻", err.response.data);
    res.status(500).json({ error: err.toString() });
  }
});

/* ======================================================
   SERVER
====================================================== */

const PORT = process.env.PORT || 4000;
app.listen(PORT, () => {
  console.log(`🔥 L&F Sync Server running on port ${PORT}`);
});
