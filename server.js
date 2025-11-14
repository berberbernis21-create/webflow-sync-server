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
// SHOPIFY GRAPHQL
// ======================================================

const SHOPIFY_GRAPHQL_URL =
  `https://${process.env.SHOPIFY_STORE}.myshopify.com/admin/api/2024-01/graphql.json`;

async function fetchProductViaGraphQL(productId) {
  const productGid = `gid://shopify/Product/${productId}`;

  const query = `
    query GetProduct($id: ID!) {
      product(id: $id) {
        id
        title
        vendor
        handle
        bodyHtml
        variants(first: 1) {
          edges {
            node { price }
          }
        }
      }
    }
  `;

  const response = await axios.post(
    SHOPIFY_GRAPHQL_URL,
    { query, variables: { id: productGid } },
    {
      headers: {
        "X-Shopify-Access-Token": process.env.SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json"
      }
    }
  );

  if (response.data.errors) {
    console.error("GraphQL Errors:", response.data.errors);
    throw new Error("Shopify GraphQL query failed");
  }

  return response.data.data.product;
}

// ======================================================
// FETCH MATCHING FILES FROM SHOPIFY FILES (REAL SOURCE)
// ======================================================

async function fetchMatchingFiles(pattern) {
  const query = `
    query FilesQuery($search: String!) {
      files(first: 50, query: $search) {
        edges {
          node {
            ... on MediaImage {
              id
              image { url }
            }
          }
        }
      }
    }
  `;

  const response = await axios.post(
    SHOPIFY_GRAPHQL_URL,
    {
      query,
      variables: {
        search: pattern   // example: "623572" matches Traxia filenames
      }
    },
    {
      headers: {
        "X-Shopify-Access-Token": process.env.SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json"
      }
    }
  );

  const edges = response.data?.data?.files?.edges || [];
  return edges
    .map(e => e.node?.image?.url)
    .filter(Boolean);
}

// ======================================================
// IMAGE DOWNLOAD
// ======================================================

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

// ======================================================
// WEBFLOW UPLOAD
// ======================================================

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

// ======================================================
// ROUTES
// ======================================================

app.get("/", (req, res) => {
  res.send("L&F Webflow Sync Server Running (Files API Version)");
});

app.post("/webflow-sync", async (req, res) => {
  try {
    const { shopifyProductId } = req.body;
    if (!shopifyProductId)
      return res.status(400).json({ error: "Missing shopifyProductId" });

    console.log("📦 Syncing Shopify Product:", shopifyProductId);

    // 1️⃣ GET PRODUCT BASICS
    const product = await fetchProductViaGraphQL(shopifyProductId);

    const name = product.title;
    const brand = product.vendor;
    const description = product.bodyHtml;
    const price = product.variants.edges[0]?.node?.price || null;
    const shopifyUrl = `https://${process.env.SHOPIFY_STORE}.myshopify.com/products/${product.handle}`;

    // 2️⃣ EXTRACT SEARCH PATTERN (last 6 digits)
    const pattern = shopifyProductId.slice(-6);

    console.log("🔍 Searching Shopify Files for:", pattern);

    const fileImages = await fetchMatchingFiles(pattern);

    if (!fileImages.length) {
      console.log("⚠️ No images found in Shopify Files.");
    }

    const featuredImage = fileImages[0] || null;
    const galleryImages = fileImages.slice(1);

    // 3️⃣ CREATE WEBFLOW ITEM
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

    // 4️⃣ UPLOAD GALLERY IMAGES
    const uploadedUrls = [];

    for (const url of galleryImages) {
      try {
        console.log("⬇️ Downloading:", url);
        const buffer = await downloadImage(url);

        console.log("⬆️ Uploading to Webflow...");
        const webflowUrl = await uploadToWebflow(buffer);

        uploadedUrls.push(webflowUrl);
      } catch (err) {
        console.error("❌ Failed image:", err.message);
      }
    }

    // 5️⃣ PATCH GALLERY
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
