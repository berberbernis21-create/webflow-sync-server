import test from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import express from "express";
import {
  buildGoogleLensUrlForImage,
  hostConsignmentPhotosForLens,
  getPublicBaseUrl,
} from "../lib/consignmentPhotoHost.js";
import consignmentRouter from "../routes/consignmentSubmission.js";
import { buildConsignmentEmail } from "../lib/consignmentEmail.js";

test("buildGoogleLensUrlForImage uses uploadbyurl with encoded image URL", () => {
  const imageUrl = "https://webflow-sync-server.onrender.com/api/consignment-photo/abc123";
  const lens = buildGoogleLensUrlForImage(imageUrl);
  assert.equal(
    lens,
    `https://lens.google.com/uploadbyurl?url=${encodeURIComponent(imageUrl)}`
  );
  assert.equal(buildGoogleLensUrlForImage("not-a-url"), null);
});

test("hostConsignmentPhotosForLens sets direct Lens URL and short launch URL", () => {
  const file = {
    buffer: Buffer.from([0xff, 0xd8, 0xff, 0xd9]),
    mimetype: "image/jpeg",
    originalname: "shelf.jpg",
  };
  const groups = new Map([[1, [file]]]);
  const result = hostConsignmentPhotosForLens(groups);
  assert.equal(result.hosted, 1);
  assert.equal(result.failed, 0);
  assert.match(file.publicImageUrl, /\/api\/consignment-photo\/[a-f0-9]{48}$/);
  assert.match(file.googleLensUrl, /\/api\/consignment-lens\/[a-f0-9]{48}$/);
  assert.equal(
    file.googleLensDirectUrl,
    buildGoogleLensUrlForImage(file.publicImageUrl)
  );
  assert.ok(file.googleLensDirectUrl.startsWith("https://lens.google.com/uploadbyurl?url="));
});

test("GET /api/consignment-lens/:token returns 302 to lens.google.com", async () => {
  const file = {
    buffer: Buffer.from([0xff, 0xd8, 0xff, 0xd9]),
    mimetype: "image/jpeg",
    originalname: "proof.jpg",
  };
  hostConsignmentPhotosForLens(new Map([[1, [file]]]));
  const token = file.googleLensUrl.split("/").pop();
  assert.match(token, /^[a-f0-9]{48}$/);

  const app = express();
  app.use("/api", consignmentRouter);
  const server = http.createServer(app);
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address();

  try {
    const res = await fetch(`http://127.0.0.1:${port}/api/consignment-lens/${token}`, {
      redirect: "manual",
      headers: { "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0" },
    });
    assert.equal(res.status, 302);
    const location = res.headers.get("location");
    assert.ok(location, "Location header required");
    assert.ok(
      location.startsWith("https://lens.google.com/uploadbyurl?url="),
      `expected Lens Location, got: ${location}`
    );
    const expectedImage = `${getPublicBaseUrl()}/api/consignment-photo/${token}`;
    assert.ok(location.includes(encodeURIComponent(expectedImage)));
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});

test("GET /api/consignment-lens/:token on Outlook iPhone serves the Bing bridge page, NOT a direct Lens redirect (uploadbyurl renders empty in that webview)", async () => {
  const file = {
    buffer: Buffer.from([0xff, 0xd8, 0xff, 0xd9]),
    mimetype: "image/jpeg",
    originalname: "mattress.jpg",
  };
  hostConsignmentPhotosForLens(new Map([[1, [file]]]));
  const token = file.googleLensUrl.split("/").pop();

  const app = express();
  app.use("/api", consignmentRouter);
  const server = http.createServer(app);
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address();

  try {
    const res = await fetch(`http://127.0.0.1:${port}/api/consignment-lens/${token}`, {
      redirect: "manual",
      headers: {
        // Outlook iOS in-app browser user agent.
        "user-agent":
          "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 Outlook-iOS/2.0",
      },
    });
    // Must NOT 302 straight to Lens — that reproduces the empty google.com search.
    assert.equal(res.status, 200);
    assert.match(res.headers.get("content-type") || "", /text\/html/);
    const body = await res.text();
    assert.match(body, /bing\.com\/images\/search/, "bridge must lead with Bing visual search");
    assert.match(body, /Open Google Lens in Safari/);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});

test("GET /api/consignment-lens/:token?lens=1 forces the direct Lens redirect even on a mobile UA", async () => {
  const file = {
    buffer: Buffer.from([0xff, 0xd8, 0xff, 0xd9]),
    mimetype: "image/jpeg",
    originalname: "nightstand.jpg",
  };
  hostConsignmentPhotosForLens(new Map([[1, [file]]]));
  const token = file.googleLensUrl.split("/").pop();

  const app = express();
  app.use("/api", consignmentRouter);
  const server = http.createServer(app);
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address();

  try {
    const res = await fetch(`http://127.0.0.1:${port}/api/consignment-lens/${token}?lens=1`, {
      redirect: "manual",
      headers: {
        "user-agent":
          "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) Mobile/15E148 Outlook-iOS/2.0",
      },
    });
    assert.equal(res.status, 302);
    const expectedImage = `${getPublicBaseUrl()}/api/consignment-photo/${token}`;
    assert.equal(
      res.headers.get("location"),
      `https://lens.google.com/uploadbyurl?url=${encodeURIComponent(expectedImage)}`
    );
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});

test("GET /api/consignment-lens/:token?bridge=1 serves the Bing/Safari fallback page", async () => {
  const file = {
    buffer: Buffer.from([0xff, 0xd8, 0xff, 0xd9]),
    mimetype: "image/jpeg",
    originalname: "chair.jpg",
  };
  hostConsignmentPhotosForLens(new Map([[1, [file]]]));
  const token = file.googleLensUrl.split("/").pop();

  const app = express();
  app.use("/api", consignmentRouter);
  const server = http.createServer(app);
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address();

  try {
    const res = await fetch(`http://127.0.0.1:${port}/api/consignment-lens/${token}?bridge=1`);
    assert.equal(res.status, 200);
    assert.match(res.headers.get("content-type") || "", /text\/html/);
    const body = await res.text();
    assert.match(body, /bing\.com\/images\/search/);
    assert.match(body, /Open Google Lens in Safari/);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});

test("GET /api/consignment-photo/:token serves 200 image bytes (publicly fetchable by Lens)", async () => {
  const jpeg = Buffer.from([0xff, 0xd8, 0xff, 0xe0, 0x00, 0x10, 0x4a, 0x46, 0xff, 0xd9]);
  const file = { buffer: jpeg, mimetype: "image/jpeg", originalname: "sofa.jpg" };
  hostConsignmentPhotosForLens(new Map([[1, [file]]]));
  const token = file.publicImageUrl.split("/").pop();

  const app = express();
  app.use("/api", consignmentRouter);
  const server = http.createServer(app);
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address();

  try {
    const res = await fetch(`http://127.0.0.1:${port}/api/consignment-photo/${token}`);
    assert.equal(res.status, 200);
    assert.match(res.headers.get("content-type") || "", /^image\//);
    // Cross-origin headers so Google's servers can fetch the image.
    assert.equal(res.headers.get("access-control-allow-origin"), "*");
    const bytes = Buffer.from(await res.arrayBuffer());
    assert.equal(bytes.length, jpeg.length);
    assert.equal(bytes[0], 0xff);
    assert.equal(bytes[1], 0xd8);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});

test("consignment email 'Google Lens' button links the short /api/consignment-lens token URL (no long query param for SafeLinks to strip), not raw lens.google.com", () => {
  const file = {
    buffer: Buffer.from([0xff, 0xd8, 0xff, 0xd9]),
    mimetype: "image/jpeg",
    originalname: "lamp.jpg",
  };
  hostConsignmentPhotosForLens(new Map([[1, [file]]]));
  assert.match(file.googleLensUrl, /\/api\/consignment-lens\/[a-f0-9]{48}$/);

  const email = buildConsignmentEmail({
    body: { customerName: "Test Consignor", customerEmail: "t@example.com" },
    items: [{ itemName: "Table Lamp", category: "Lighting", condition: "Good" }],
    photoGroups: new Map([[1, [file]]]),
    submittedAt: "Nov 24, 2026",
  });

  // The button must point at our short launch URL so Outlook's in-app browser
  // gets the Bing/Safari bridge page instead of a raw Lens link that errors.
  assert.ok(
    email.html.includes(`href="${file.googleLensUrl}"`),
    "email should link the short /api/consignment-lens launch URL"
  );
  assert.ok(
    !email.html.includes("https://lens.google.com/uploadbyurl"),
    "email button must not link raw lens.google.com (breaks Outlook mobile)"
  );
});
