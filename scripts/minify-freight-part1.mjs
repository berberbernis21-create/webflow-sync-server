import fs from "fs";
import { execFileSync } from "child_process";
import path from "path";

const dir = "webflow-embeds";
const pastePath = path.join(dir, "PART1-embed-html-css.html");
const sourcePath = path.join(dir, "PART1-embed-html-css.source.html");
const rawCssPath = path.join(dir, "_part1-raw.css");
const minCssPath = path.join(dir, "_part1-min.css");

const paste = fs.readFileSync(pastePath, "utf8");
// Promote paste → source when paste is still the readable (non-minified) version.
if (
  !paste.includes("minified for Webflow 50k") &&
  (paste.includes("STRONG SELECTED STATES") || paste.includes("\n.lf-choice label {"))
) {
  fs.writeFileSync(sourcePath, paste);
}

const source = fs.readFileSync(sourcePath, "utf8");
const styleStart = source.indexOf("<style>");
const styleEnd = source.indexOf("</style>");
if (styleStart < 0 || styleEnd < styleStart) {
  console.error("Could not find <style> block in Part 1 source");
  process.exit(1);
}

const css = source.slice(styleStart + "<style>".length, styleEnd);
const htmlAfter = source.slice(styleEnd + "</style>".length);

fs.writeFileSync(rawCssPath, css);
execFileSync(
  "npx",
  ["--yes", "clean-css-cli", "-o", minCssPath, rawCssPath],
  { stdio: "inherit", shell: true }
);
const minCss = fs.readFileSync(minCssPath, "utf8").trim();

let html = htmlAfter
  .replace(/<!--[\s\S]*?-->/g, "")
  .replace(/\r\n/g, "\n")
  // Collapse whitespace between tags; keep single spaces in text nodes.
  .replace(/>\s+</g, "><")
  .replace(/\n{2,}/g, "\n")
  .trim();

const out = `<!-- LF freight calc Part 1 (minified for Webflow 50k). Source: PART1-embed-html-css.source.html -->
<style>${minCss}</style>
${html}
`;

fs.writeFileSync(pastePath, out);
fs.unlinkSync(rawCssPath);
fs.unlinkSync(minCssPath);

console.log(
  JSON.stringify(
    {
      sourceChars: source.length,
      pasteChars: out.length,
      cssIn: css.length,
      cssOut: minCss.length,
      under50k: out.length < 50000,
      headroom: 50000 - out.length,
    },
    null,
    2
  )
);
