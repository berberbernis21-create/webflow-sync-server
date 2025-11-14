import express from "express";
import axios from "axios";
import cors from "cors";
import dotenv from "dotenv";
import https from "https";
import fileType from "file-type";   // ← IMPORTANT: detects PNG/JPG/WEBP automatically

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
          edges { node { price } }
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
    throw new Error(JSON.stringify(response.data.errors));
  }

  return response.data.data.product;
}

// ======================================================
// SHOPIFY FILES – TRUE IMAGE SOURCE
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
      variables: { search: pattern }
    },
    {
      headers: {
        "X-Shopify-Access-Token": process.env.SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json"
      }
    }
  );

  return (response.data?.data?.files?.edges || [])
    .map(e => e.node?.image?.url)
    .filter(Boolean);
}

// ======================================================
// IMAGE DOWNLOAD + MIME DETECTION
// ======================================================

async function downloadImage(url) {
  const response = await axios.get(url, {
    responseType: "arraybuffer",
    headers: { "User-Agent": "Mozilla/5.0" }
  });

  const buffer = Buffer.from(response.data);

  const type = await fileType.fromBuffer(buffer);
  if (!type) throw new Error("MIME detection failed");

  return { buffer, mime: type.mime };   // ← REAL MIME (image/webp, image/jpeg, etc.)
}

// ======================================================
// WEBFLOW UPLOAD (CORRECT MIME)
// ======================================================

function uploadRawToWebflow(uploadUrl, buffer, mimeType) {
  return new Promise((resolve, reject) => {
    const url = new URL(uploadUrl);
    const opts = {
      method: "PUT",
      hostname: url.hostname,
      path: url.pathname + url.search,
      headers: {
        "Content-Type": mimeType,
        "Content-Length": buffer.length
      }
    };

    const req = https.request(opts, (res) => {
      if (res.statusCode >= 200 && res.statusCode < 300) resolve();
      else reject(`Upload failed with ${res.statusCode}`);
    });

    req.on("error", reject);
    req.write(buffer);
    req.end();
  });
}

async function uploadToWebflow(buffer, mime) {
  const ext = mime.split("/")[1]; // jpg / png / webp
  const filename = `img-${Date.now()}.${ext}`;

  const target = await axios.post(
    "https://api.webflow.com/v2/assets/upload",
    {
      fileName: filename,
      mimeType: mime
    },
    {
      headers: {
        Authorization: `Bearer ${process.env.WEBFLOW_TOKEN}`,
        "Content-Type": "application/json"
      }
    }
  );

  await uploadRawToWebflow(target.data.uploadUrl, buffer, mime);
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

app.get("/", (req, res) => res.send("L&F Sync Server Running"));

app.post("/webflow-sync", async (req, res) => {
  try {
    const { shopifyProductId } = req.body;

    const product = await fetchProductViaGraphQL(shopifyProductId);

    const name = product.title;
    const brand = product.vendor;
    const description = product.bodyHtml;
    const price = product.variants.edges[0]?.node?.price;
    const shopifyUrl = `https://${process.env.SHOPIFY_STORE}.myshopify.com/products/${product.handle}`;

    // 🔍 Find images based on last digits of ID
    const pattern = shopifyProductId.slice(-6);
    const fileUrls = await fetchMatchingFiles(pattern);

    const featuredImage = fileUrls[0] || null;
    const gallery = fileUrls.slice(1);

    // Create Webflow item
    const created = await axios.post(
      `https://api.webflow.com/v2/collections/${process.env.WEBFLOW_COLLECTION_ID}/items`,
      {
        fieldData: {
          name,
          brand,
          price,
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

    // Upload gallery images
    const uploaded = [];

    for (const url of gallery) {
      const { buffer, mime } = await downloadImage(url);
      const uploadedUrl = await uploadToWebflow(buffer, mime);
      uploaded.push(uploadedUrl);
    }

    if (uploaded.length > 0) {
      await patchWebflowImages(itemId, uploaded);
    }

    res.json({ status: "ok", itemId, uploadedCount: uploaded.length });

  } catch (err) {
    res.status(500).json({ error: err.toString() });
  }
});

// ======================================================

const PORT = process.env.PORT || 4000;
app.listen(PORT, () => console.log(`Running on ${PORT}`));
