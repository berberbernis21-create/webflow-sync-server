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

// Download image BY ID using Shopify Admin API (never CDN)
// This returns guaranteed bytes.
async function downloadShopifyImage(productId, imageId) {
  const url = `https://${process.env.SHOPIFY_STORE}.myshopify.com/admin/api/2024-01/products/${productId}/images/${imageId}.json`;

  const response = await axios.get(url, {
    headers: { "X-Shopify-Access-Token": process.env.SHOPIFY_ACCESS_TOKEN }
  });

  // Shopify returns base64
  const base64 = response.data.image.attachment;
  if (!base64) throw new Error("Shopify returned no attachment data");

  return Buffer.from(base64, "base64");
}

// RAW S3 upload for Webflow
function uploadRawToWebflow(uploadUrl, buffer, mimeType) {
  return new Promise((resolve, reject) => {
    const url = new URL(uploadUrl);
    const options = {
      method: "PUT",
      hostname: url.hostname,
      path: url.pathname + url.search,
      headers: {
        "Content-Type": mimeType,
        "Content-Length": buffer.length
      }
    };

    const req = https.request(options, (res) => {
      if (res.statusCode >= 200 && res.statusCode < 300) {
        resolve(true);
      } else {
        reject(`Upload failed with code ${res.statusCode}`);
      }
    });

    req.on("error", reject);
    req.write(buffer);
    req.end();
  });
}

// Upload to Webflow S3
async function uploadToWebflow(imageBuffer) {
  const filename = `image-${Math.random().toString(36).substring(2, 12)}.jpg`;

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

  await uploadRawToWebflow(uploadUrl, imageBuffer, "image/jpeg");

  return assetUrl;
}

// Patch multi-image gallery
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

// Fetch product from Shopify
async function fetchShopifyProduct(id) {
  const url = `https://${process.env.SHOPIFY_STORE}.myshopify.com/admin/api/2024-01/products/${id}.json`;

  const response = await axios.get(url, {
    headers: { "X-Shopify-Access-Token": process.env.SHOPIFY_ACCESS_TOKEN }
  });

  return response.data.product;
}

// ======================================================
// ROUTES
// ======================================================

app.get("/", (req, res) => {
  res.send("L&F Webflow Sync Server Running");
});

// MAIN SYNC ENDPOINT
app.post("/webflow-sync", async (req, res) => {
  try {
    const { shopifyProductId } = req.body;

    if (!shopifyProductId)
      return res.status(400).json({ error: "Missing shopifyProductId" });

    console.log("📦 Syncing Shopify Product:", shopifyProductId);

    // 1️⃣ Fetch product
    const product = await fetchShopifyProduct(shopifyProductId);

    const name = product.title;
    const brand = product.vendor;
    const description = product.body_html;
    const price = product.variants?.[0]?.price || null;
    const handle = product.handle;
    const shopifyUrl = `https://${process.env.SHOPIFY_STORE}.myshopify.com/products/${handle}`;

    const featuredImageId = product.image?.id || null;
    const allImageIds = product.images.map((img) => img.id);

    // 2️⃣ Create Webflow item
    const created = await axios.post(
      `https://api.webflow.com/v2/collections/${process.env.WEBFLOW_COLLECTION_ID}/items`,
      {
        fieldData: {
          name,
          price,
          brand,
          description,
          "shopify-product-id": shopifyProductId,
          "shopify-url": shopifyUrl,
          "featured-image": featuredImageId
            ? { url: product.image.src }
            : null
        }
      },
      {
        headers: {
          Authorization: `Bearer ${process.env.WEBFLOW_TOKEN}`,
          "Content-Type": "application/json"
        }
      }
    );

    const itemId = created.data.id;
    console.log("✅ Webflow item created:", itemId);

    // 3️⃣ Upload gallery images via ADMIN API
    const galleryIds = allImageIds.filter((id) => id !== featuredImageId);

    const webflowUrls = [];

    for (const imgId of galleryIds) {
      try {
        console.log("⬇️ Fetching raw image via Admin API:", imgId);

        const buffer = await downloadShopifyImage(shopifyProductId, imgId);

        console.log("⬆️ Uploading to Webflow...");
        const url = await uploadToWebflow(buffer);

        webflowUrls.push(url);
      } catch (err) {
        console.error("❌ Failed gallery image:", err);
      }
    }

    // 4️⃣ Save to Webflow
    if (webflowUrls.length > 0)
      await patchWebflowImages(itemId, webflowUrls);

    res.json({
      status: "ok",
      itemId,
      uploaded: webflowUrls.length
    });

  } catch (err) {
    console.error("🔥 SERVER ERROR:", err);
    res.status(500).json({ error: err });
  }
});

// ======================================================

const PORT = process.env.PORT || 4000;
app.listen(PORT, () =>
  console.log(`🔥 Webflow Sync Server running on port ${PORT}`)
);
