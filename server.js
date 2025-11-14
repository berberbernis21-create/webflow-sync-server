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
      "Content-Type": "application/json"
    }
  });

  return response.data.product;
}

// ======================================================
// ROUTES
// ======================================================

app.get("/", (req, res) => {
  res.send("L&F Webflow Sync Server (Individual Image Fields) Running");
});

app.post("/webflow-sync", async (req, res) => {
  try {
    const { shopifyProductId } = req.body;

    if (!shopifyProductId) {
      return res.status(400).json({ error: "Missing shopifyProductId" });
    }

    console.log("📦 Syncing Shopify Product:", shopifyProductId);

    // 1️⃣ Get product + all images
    const product = await fetchShopifyProduct(shopifyProductId);

    const name = product.title;
    const brand = product.vendor;
    const description = product.body_html;
    const price = product.variants?.[0]?.price || null;
    const slug = product.handle;
    const shopifyUrl = `https://${process.env.SHOPIFY_STORE}.myshopify.com/products/${slug}`;

    // All Shopify image URLs
    const allImages = (product.images || []).map((img) => img.src);

    const featuredImage = product.image?.src || allImages[0] || null;
    const gallery = allImages.filter((url) => url !== featuredImage);

    console.log("🖼️ Images Found:", {
      featuredImage,
      gallery
    });

    // 2️⃣ Prepare EXACT Webflow field slugs
    const fieldData = {
      name,
      slug,
      brand,
      price,
      description,
      "shopify-product-id": shopifyProductId,
      "shopify-url": shopifyUrl,
      "featured-image": featuredImage ? { url: featuredImage } : null,

      // Individual image fields (max 3)
      "image-1": gallery[0] ? { url: gallery[0] } : null,
      "image-2": gallery[1] ? { url: gallery[1] } : null,
      "image-3": gallery[2] ? { url: gallery[2] } : null,

      // Optional defaults
      "show-on-webflow": true,
      "featured-item-on-homepage": false
    };

    // 3️⃣ Create Webflow item
    const created = await axios.post(
      `https://api.webflow.com/v2/collections/${process.env.WEBFLOW_COLLECTION_ID}/items`,
      { fieldData },
      {
        headers: {
          Authorization: `Bearer ${process.env.WEBFLOW_TOKEN}`,
          "Content-Type": "application/json"
        }
      }
    );

    console.log("✅ Webflow Item Created:", created.data.id);

    res.json({
      status: "ok",
      itemId: created.data.id,
      featured: featuredImage,
      galleryUsed: gallery.slice(0, 3)
    });

  } catch (err) {
    console.error("🔥 SERVER ERROR:", err);
    if (err.response) console.error("🔻 Response:", err.response.data);
    res.status(500).json({ error: err.toString() });
  }
});

// ======================================================

const PORT = process.env.PORT || 4000;
app.listen(PORT, () => {
  console.log(`🔥 Webflow Sync Server running on port ${PORT}`);
});
