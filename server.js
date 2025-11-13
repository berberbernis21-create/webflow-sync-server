import express from "express";
import axios from "axios";
import cors from "cors";
import dotenv from "dotenv";
import FormData from "form-data";
import https from "https";

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json());

// -------------------------------
// HELPERS
// -------------------------------

// Download Shopify image into a Buffer (using built-in fetch)
async function downloadImage(url) {
  const res = await fetch(url);
  return Buffer.from(await res.arrayBuffer());
}

// Upload image buffer to Webflow (S3)
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

// Patch item with all multi-images
async function patchWebflowImages(itemId, urls) {
  return axios.patch(
    `https://api.webflow.com/v2/collections/${process.env.WEBFLOW_COLLECTION_ID}/items/${itemId}`,
    {
      fieldData: {
        "image-s": urls.map(url => ({ url }))
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

// -------------------------------
// ROUTES
// -------------------------------

app.get("/test", (req, res) => {
  res.json({ status: "ok", message: "Test endpoint works" });
});

app.get("/", (req, res) => {
  res.send("L&F Webflow Sync Active");
});

app.post("/webflow-sync", async (req, res) => {
  try {
    const {
      name,
      price,
      brand,
      description,
      shopifyProductId,
      shopifyUrl,
      featuredImage,
      images
    } = req.body;

    console.log("📦 Incoming:", name);

    if (!name || !shopifyProductId) {
      return res.status(400).json({
        status: "error",
        message: "Missing required fields (name or shopifyProductId)"
      });
    }

    // 1️⃣ CREATE WEBFLOW ITEM (featured only)
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

    const newItemId = created.data.id;
    console.log("✅ Created Webflow item:", newItemId);

    // 2️⃣ PROCESS AND UPLOAD OTHER IMAGES
    const otherImages = images?.filter(url => url !== featuredImage) || [];

    let webflowUrls = [];
    for (let url of otherImages) {
      try {
        console.log("⬇️ Download:", url);
        const buffer = await downloadImage(url);

        const filename = url.split("/").pop() || "image.jpg";

        console.log("⬆️ Uploading to Webflow…", filename);
        const uploaded = await uploadToWebflow(buffer, filename);

        console.log("🌐 Webflow URL:", uploaded);
        webflowUrls.push(uploaded);
      } catch (err) {
        console.error("❌ Image upload failed:", err.message);
      }
    }

    // 3️⃣ PATCH MULTI–IMAGE FIELD
    if (webflowUrls.length > 0) {
      await patchWebflowImages(newItemId, webflowUrls);
      console.log("🖼️ Multi-image updated:", webflowUrls.length, "images");
    }

    return res.json({
      status: "ok",
      itemId: newItemId,
      addedImages: webflowUrls.length
    });

  } catch (err) {
    console.error("🔥 SERVER ERROR:", err.response?.data || err.message);

    return res.status(500).json({
      status: "error",
      message: err.response?.data || err.message
    });
  }
});

// -------------------------------

const PORT = process.env.PORT || 4000;
app.listen(PORT, () => {
  console.log(`Webflow Sync Server live on port ${PORT}`);
});
