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

// Extract file ID from Shopify /files/... filenames
function extractFileIdFromCDN(url) {
  try {
    const filename = url.split("/").pop().split("?")[0];
    const match = filename.match(/^(\d+)/);
    return match ? match[1] : null;
  } catch {
    return null;
  }
}

// Download via Shopify FILES API
async function fetchFilePublicUrl(fileId) {
  const url = `https://${process.env.SHOPIFY_STORE}.myshopify.com/admin/api/2024-01/files/${fileId}.json`;

  const response = await axios.get(url, {
    headers: { "X-Shopify-Access-Token": process.env.SHOPIFY_ACCESS_TOKEN }
  });

  return response.data.file.public_url;
}

// Download an image from ANY URL
async function downloadImage(url) {
  const response = await axios.get(url, {
    responseType: "arraybuffer",
    headers: {
      "User-Agent": "Mozilla/5.0",
      Accept: "image/*"
    }
  });
  return Buffer.from(response.data);
}

// RAW upload for Webflow S3
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

// Upload raw bytes → Webflow
async function uploadToWebflow(buffer) {
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

  await uploadRawToWebflow(target.data.uploadUrl, buffer, "image/jpeg");
  return target.data.assetUrl;
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

// Fetch product
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

    const featuredImage = product.image?.src || null;
    const allImages = product.images.map((img) => img.src);

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
          "featured-image": featuredImage ? { url: featuredImage } : null
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

    // 3️⃣ Upload gallery images WITH FILE API FIX
    const gallery = allImages.filter((src) => src !== featuredImage);
    const uploadedUrls = [];

    for (const imageUrl of gallery) {
      try {
        console.log("🔍 Checking if image is a Shopify File:", imageUrl);

        let downloadUrl = imageUrl;

        if (imageUrl.includes("/files/")) {
          const fileId = extractFileIdFromCDN(imageUrl);
          console.log("📂 Extracted file ID:", fileId);

          if (fileId) {
            console.log("🔗 Fetching public_url from Files API...");
            downloadUrl = await fetchFilePublicUrl(fileId);

            console.log("📥 Using file public_url:", downloadUrl);
          }
        }

        console.log("⬇️ Downloading:", downloadUrl);
        const buffer = await downloadImage(downloadUrl);

        console.log("⬆️ Uploading to Webflow…");
        const webflowUrl = await uploadToWebflow(buffer);

        uploadedUrls.push(webflowUrl);

      } catch (err) {
        console.error("❌ Failed gallery image:", err.message || err);
      }
    }

    // 4️⃣ Save gallery images to Webflow
    if (uploadedUrls.length > 0) {
      await patchWebflowImages(itemId, uploadedUrls);
      console.log("🖼️ Gallery patched:", uploadedUrls.length);
    }

    res.json({
      status: "ok",
      itemId,
      uploaded: uploadedUrls.length
    });

  } catch (err) {
    console.error("🔥 SERVER ERROR:", err);
    res.status(500).json({ error: err.toString() });
  }
});

// ======================================================

const PORT = process.env.PORT || 4000;
app.listen(PORT, () =>
  console.log(`🔥 Webflow Sync Server running on port ${PORT}`)
);
