import express from "express";
import axios from "axios";
import cors from "cors";
import dotenv from "dotenv";

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json());

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

    if (!name || !shopifyProductId) {
      return res.status(400).json({
        status: "error",
        message: "Missing required fields (name or shopifyProductId)"
      });
    }

    const payload = {
      fieldData: {
        name,
        price,
        brand,
        description,
        "shopify-product-id": shopifyProductId,
        "shopify-url": shopifyUrl,
        "featured-image": featuredImage ? { url: featuredImage } : null,
        images: images || []
      }
    };

    const response = await axios.post(
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

    return res.json({
      status: "ok",
      webflowItemId: response.data.id,
      webflowResponse: response.data
    });

  } catch (err) {

    // 🔥🔥 ADD THIS LOGGING HERE 🔥🔥
    console.error("🔥 SERVER ERROR:", err.response?.data || err.message || err);

    return res.status(500).json({
      status: "error",
      message: err.response?.data || err.message
    });
  }
});

const PORT = process.env.PORT || 4000;
app.listen(PORT, () => {
  console.log(`Webflow Sync Server live on port ${PORT}`);
});
