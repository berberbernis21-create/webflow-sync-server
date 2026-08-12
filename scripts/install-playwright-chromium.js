/**
 * Install Playwright Chromium into a hermetic path (node_modules) so Render
 * build and runtime share the same binaries. Soft-fails — production Linux
 * prefers @sparticuz/chromium for Lens screenshots.
 */
import { spawnSync } from "child_process";

if (!process.env.PLAYWRIGHT_BROWSERS_PATH) {
  process.env.PLAYWRIGHT_BROWSERS_PATH = "0";
}

const result = spawnSync("npx", ["playwright", "install", "chromium"], {
  stdio: "inherit",
  env: process.env,
  shell: true,
});

if (result.status !== 0) {
  console.warn(
    "[postinstall] playwright chromium install failed (non-fatal); Lens shots will use @sparticuz/chromium on Linux"
  );
  process.exit(0);
}
