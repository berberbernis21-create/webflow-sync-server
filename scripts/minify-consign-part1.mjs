/**
 * Minify PART1 (HTML + CSS) for Webflow embed paste (~50k limit).
 * Edit: PART1-embed-html-css.source.html
 * Paste: PART1-embed-html-css.html
 *
 * Usage: node scripts/minify-consign-part1.mjs
 */
import fs from "fs";
import { execFileSync } from "child_process";
import path from "path";

const dir = "webflow-embeds/consign-submit";
const pastePath = path.join(dir, "PART1-embed-html-css.html");
const sourcePath = path.join(dir, "PART1-embed-html-css.source.html");
const rawPath = path.join(dir, "_part1-raw.html");
const minPath = path.join(dir, "_part1-min.html");

// If paste still looks like editable source (has multi-line CSS comments / spacing),
// bootstrap .source.html from it once.
if (!fs.existsSync(sourcePath)) {
  fs.writeFileSync(sourcePath, fs.readFileSync(pastePath, "utf8"));
} else {
  const paste = fs.readFileSync(pastePath, "utf8");
  // Prefer keeping source as truth; if paste was edited in Designer dump and still has
  // the readable banner comment + unminified style block, sync source from paste.
  if (
    paste.includes("PART 1 OF 2: VISIBLE WEBFLOW EMBED") &&
    paste.includes("\n  .lf-submit-page {")
  ) {
    fs.writeFileSync(sourcePath, paste);
  }
}

const source = fs.readFileSync(sourcePath, "utf8");
fs.writeFileSync(rawPath, source);

execFileSync(
  "npx",
  [
    "--yes",
    "html-minifier-terser",
    rawPath,
    "--collapse-whitespace",
    "--remove-comments",
    "--minify-css",
    "true",
    "--minify-js",
    "false",
    "--output",
    minPath,
  ],
  { stdio: "inherit", shell: true }
);

const minHtml = fs.readFileSync(minPath, "utf8").trim();
const out = `<!--
LOST & FOUND CONSIGNMENT SUBMISSION
PART 1: Webflow Embed (minified for paste limit)
Readable source: PART1-embed-html-css.source.html
Also paste INSIDE-HEAD-styles.html in Inside <head>, and PART2 before </body>.
-->
${minHtml}
`;

fs.writeFileSync(pastePath, out);
fs.unlinkSync(rawPath);
fs.unlinkSync(minPath);

console.log(
  JSON.stringify(
    {
      sourceChars: source.length,
      pasteChars: out.length,
      under50k: out.length < 50000,
      headroom: 50000 - out.length,
    },
    null,
    2
  )
);
