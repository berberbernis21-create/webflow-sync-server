import express from "express";
import axios from "axios";
import cors from "cors";
import dotenv from "dotenv";

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json());

// ======================================================
// SHOPIFY REST – PRODUCT + IMAGES
// ======================================================

async function fetchShopifyProduct(productId) {
  const url = `https://${process.env.SHOPIFY_STORE}.myshopify.com/admin/api/2024-01/products/${productId}.json`;

  const response = await axios.get(url, {
    headers: {
      "X-Shopify-Access-Token": process.env.SHOPIFY_ACCESS_TOKEN,
      "Content-Type": "application/json",
    },
  });

  return response.data.product;
}

// ======================================================
// 🔍 FIND EXISTING WEBFLOW ITEM (API v2 cannot filter by field)
// Manually paginate and match the shopify-product-id
// ======================================================

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
        console.log("🔎 Found existing Webflow item:", item.id);
        return item;
      }
    }

    if (!response.data.pagination?.nextPage) break;
    page = response.data.pagination.nextPage;
  }

  return null;
}

// ======================================================
// ROUTES
// ======================================================

app.get("/", (req, res) => {
  res.send("L&F Webflow Sync Server (6 Images + No Duplicates) Running");
});

app.post("/webflow-sync", async (req, res) => {
  try {
    const { shopifyProductId } = req.body;

    if (!shopifyProductId) {
      return res.status(400).json({ error: "Missing shopifyProductId" });
    }

    console.log("📦 Syncing Shopify Product:", shopifyProductId);

    // 1️⃣ Fetch Shopify data
    const product = await fetchShopifyProduct(shopifyProductId);

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

    console.log("🖼️ Images Found:", {
      featuredImage,
      gallery,
    });

    // 2️⃣ Build Webflow payload
    const fieldData = {
      name,
      slug,
      brand,
      price,
      description,
      "shopify-product-id": String(shopifyProductId),
      "shopify-url": shopifyUrl,

      // Featured image
      "featured-image": featuredImage ? { url: featuredImage } : null,

      // Up to 6 gallery images
      "image-1": gallery[0] ? { url: gallery[0] } : null,
      "image-2": gallery[1] ? { url: gallery[1] } : null,
      "image-3": gallery[2] ? { url: gallery[2] } : null,
      "image-4": gallery[3] ? { url: gallery[3] } : null,
      "image-5": gallery[4] ? { url: gallery[4] } : null,
      "image-6": gallery[5] ? { url: gallery[5] } : null,

      "show-on-webflow": true,
      "featured-item-on-homepage": false,
    };

    // 3️⃣ Create or Update
    const existing = await findExistingWebflowItem(shopifyProductId);

    let itemId;
    let operation;

    if (existing) {
      operation = "update";
      console.log("✏️ Updating Webflow item:", existing.id);

      const updateResp = await axios.patch(
        `https://api.webflow.com/v2/collections/${process.env.WEBFLOW_COLLECTION_ID}/items/${existing.id}`,
        { fieldData },
        {
          headers: {
            Authorization: `Bearer ${process.env.WEBFLOW_TOKEN}`,
            "Content-Type": "application/json",
          },
        }
      );

      itemId = updateResp.data.id;
    } else {
      operation = "create";
      console.log("🆕 Creating new Webflow item…");

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

      itemId = createResp.data.id;
    }

    console.log(`✅ Webflow item ${operation}d:`, itemId);

    // 4️⃣ Return response
    res.json({
      status: "ok",
      operation,
      itemId,
      featured: featuredImage,
      galleryUsed: gallery.slice(0, 6),
    });
  } catch (err) {
    console.error("🔥 SERVER ERROR:", err);
    if (err.response) console.error("🔻 Response:", err.response.data);
    res.status(500).json({ error: err.toString() });
  }
});

// ======================================================
// SERVER
// ======================================================

const PORT = process.env.PORT || 4000;
app.listen(PORT, () => {
  console.log(`🔥 Webflow Sync Server running on port ${PORT}`);
});
