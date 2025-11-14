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

// Download ANY image URL → Buffer (FIXED: force JPEG, avoid WEBP)
async function downloadImage(url) {
  const response = await axios.get(url, {
    responseType: "arraybuffer",
    headers: {
      "Accept": "image/jpeg",
      "User-Agent": "Mozilla/5.0"
    }
  });
  return Buffer.from(response.data);
}

// RAW S3 upload (required by Webflow)
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

// Upload Buffer → Webflow S3 (FIXED: random filenames)
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

// Patch MULTI-IMAGE field
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

    // 1️⃣ Fetch product from Shopify
    const product = await fetchShopifyProduct(shopifyProductId);

    const name = product.title;
    const brand = product.vendor;
    const description = product.body_html;
    const price = product.variants[0]?.price || null;
    const handle = product.handle;
    const shopifyUrl = `https://${process.env.SHOPIFY_STORE}.myshopify.com/products/${handle}`;

    const featuredImage = product.image?.src || null;
    const allImages = product.images.map((img) => img.src);

    // 2️⃣ Create item in Webflow
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

    // 3️⃣ Upload gallery images
    const galleryImages = allImages.filter((img) => img !== featuredImage);
    const webflowUrls = [];

    for (const imageUrl of galleryImages) {
      try {
        console.log("⬇️ Downloading:", imageUrl);
        const buffer = await downloadImage(imageUrl);

        console.log("⬆️ Uploading image to Webflow...");
        const webflowUrl = await uploadToWebflow(buffer);

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
