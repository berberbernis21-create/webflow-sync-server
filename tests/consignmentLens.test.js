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
