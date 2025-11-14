import express from "express";
import axios from "axios";
import cors from "cors";
import dotenv from "dotenv";
import https from "https";

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json());

// ======================================================
// HELPERS
// ======================================================

// Download ANY image URL → into a Buffer
async function downloadImage(url) {
  const response = await axios.get(url, {
    responseType: "arraybuffer"
  });
  return Buffer.from(response.data);
}

// Upload Buffer → Webflow S3
async function uploadToWebflow(imageBuffer, filename) {
  const target = await axios.post(
    "https://api.webflow.com/v2/assets/upload",
    {
      fileName: filename,
      mimeType: "image/jpeg"
    },
    {
      headers: {
        Authorization: `Bearer ${process.env.WEBFLOW_TOKEN}`,
        "Content-Type": "application/json"
      }
    }
  );

  const { uploadUrl, assetUrl } = target.data;

  await axios.put(uploadUrl, imageBuffer, {
    headers: {
      "Content-Type": "image/jpeg"
    },
    maxBodyLength: Infinity,
    maxContentLength: Infinity,
    httpsAgent: new https.Agent({ rejectUnauthorized: false })
  });

  return assetUrl;
}

// Patch MULTI-IMAGE field after item is created
async function patchWebflowImages(itemId, urls) {
  return axios.patch(
    `https://api.webflow.com/v2/collections/${process.env.WEBFLOW_COLLECTION_ID}/items/${itemId}`,
    {
      fieldData: {
        "image-s": urls.map((u) => ({ url: u }))
      }
    },
    {
      headers: {
        Authorization: `Bearer ${process.env.WEBFLOW_TOKEN}`,
        "Content-Type": "application/json"
      }
    }
  );
}

// Fetch Shopify Product
async function fetchShopifyProduct(id) {
  const url = `https://${process.env.SHOPIFY_STORE}.myshopify.com/admin/api/2024-01/products/${id}.json`;

  const response = await axios.get(url, {
    headers: {
      "X-Shopify-Access-Token": process.env.SHOPIFY_ACCESS_TOKEN
    }
  });

  return response.data.product;
}

// ======================================================
// ROUTES
// ======================================================

app.get("/", (req, res) => {
  res.send("L&F Webflow Sync Server Running");
});

// MAIN ENDPOINT
app.post("/webflow-sync", async (req, res) => {
  try {
    const { shopifyProductId } = req.body;

    if (!shopifyProductId) {
      return res.status(400).json({ error: "Missing shopifyProductId" });
    }

    console.log("📦 Syncing Shopify Product:", shopifyProductId);

    // 1️⃣ Fetch from Shopify
    const product = await fetchShopifyProduct(shopifyProductId);

    const name = product.title;
    const brand = product.vendor;
    const description = product.body_html;
    const price = product.variants[0]?.price || null;
    const handle = product.handle;
    const shopifyUrl = `https://${process.env.SHOPIFY_STORE}.myshopify.com/products/${handle}`;

    // Featured image
    const featuredImage = product.image?.src || null;

    // All images (including featured)
    const allImages = product.images.map((img) => img.src);

    // 2️⃣ Create Webflow Item
    const payload = {
      fieldData: {
        name,
        price,
        brand,
        description,
        "shopify-product-id": shopifyProductId,
        "shopify-url": shopifyUrl,
        "featured-image": featuredImage ? { url: featuredImage } : null
      }
    };

    const created = await axios.post(
      `https://api.webflow.com/v2/collections/${process.env.WEBFLOW_COLLECTION_ID}/items`,
      payload,
      {
        headers: {
          Authorization: `Bearer ${process.env.WEBFLOW_TOKEN}`,
          "Content-Type": "application/json",
          Accept: "application/json"
        }
      }
    );

    const itemId = created.data.id;
    console.log("✅ Webflow item created:", itemId);

    // 3️⃣ Upload ALL non-featured images to Webflow S3
    const galleryImages = allImages.filter((img) => img !== featuredImage);
    const webflowUrls = [];

    for (const imageUrl of galleryImages) {
      try {
        console.log("⬇️ Downloading:", imageUrl);
        const buffer = await downloadImage(imageUrl);

        const filename = imageUrl.split("/").pop() || "image.jpg";

        console.log("⬆️ Uploading to Webflow:", filename);
        const webflowUrl = await uploadToWebflow(buffer, filename);

        webflowUrls.push(webflowUrl);
      } catch (err) {
        console.error("❌ Image upload failed:", err.message);
      }
    }

    // 4️⃣ Patch multi-image field
    if (webflowUrls.length > 0) {
      await patchWebflowImages(itemId, webflowUrls);
      console.log("🖼️ Multi-image field updated:", webflowUrls.length);
    }

    res.json({
      status: "ok",
      itemId,
      totalImagesUploaded: webflowUrls.length
    });

  } catch (err) {
    console.error("🔥 SERVER ERROR:", err.response?.data || err.message);
    res.status(500).json({
      error: err.response?.data || err.message
    });
  }
});

// ======================================================

const PORT = process.env.PORT || 4000;
app.listen(PORT, () => {
  console.log(`🔥 Webflow Sync Server running on port ${PORT}`);
});
