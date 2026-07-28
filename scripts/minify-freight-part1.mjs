import fs from "fs";
import { execFileSync } from "child_process";
import path from "path";

const dir = "webflow-embeds";
const publicEmbeds = path.join("public", "embeds");
const pastePath = path.join(dir, "PART1-embed-html-css.html");
const sourcePath = path.join(dir, "PART1-embed-html-css.source.html");
const rawCssPath = path.join(dir, "_part1-raw.css");
const minCssPath = path.join(dir, "_part1-min.css");
const hostedCssPath = path.join(publicEmbeds, "freight-part1.css");
const hostedHtmlPath = path.join(publicEmbeds, "freight-part1.html");
const API_BASE = "https://webflow-sync-server.onrender.com";

fs.mkdirSync(publicEmbeds, { recursive: true });

const paste = fs.readFileSync(pastePath, "utf8");
// Only promote paste → source when paste is still a full readable Part 1 (never the tiny loader).
if (
  !paste.includes("loads CSS/HTML from Render") &&
  !paste.includes("minified for Webflow 50k") &&
  paste.includes("STRONG SELECTED STATES") &&
  paste.includes('id="lfForm"')
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
  .replace(/>\s+</g, "><")
  .replace(/\n{2,}/g, "\n")
  .trim();

// Hosted assets (CSS always from Render; HTML also hosted as fallback for thin mounts).
fs.writeFileSync(hostedCssPath, minCss + "\n");
fs.writeFileSync(hostedHtmlPath, html + "\n");

// Webflow Embed paste: external CSS + inline HTML so the form shows in the designer
// (scripts still only run in Preview / published). Stays under Webflow's 50k limit.
const out = `<!-- LF freight calc Part 1: CSS from Render + HTML inline for Webflow designer. Edit PART1-embed-html-css.source.html then run node scripts/minify-freight-part1.mjs -->
<link rel="stylesheet" href="${API_BASE}/embeds/freight-part1.css">
<div id="lfCalcHost">${html}</div>
`;

fs.writeFileSync(pastePath, out);
fs.unlinkSync(rawCssPath);
fs.unlinkSync(minCssPath);

console.log(
  JSON.stringify(
    {
      sourceChars: source.length,
      pasteChars: out.length,
      hostedCssChars: minCss.length,
      hostedHtmlChars: html.length,
      under50k: out.length < 50000,
      under10k: out.length < 10000,
      headroom50k: 50000 - out.length,
    },
    null,
    2
  )
);
