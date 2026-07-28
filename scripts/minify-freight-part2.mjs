import fs from "fs";
import { execFileSync } from "child_process";
import path from "path";

const dir = "webflow-embeds";
const pastePath = path.join(dir, "PART2-before-body-js.html");
const sourcePath = path.join(dir, "PART2-before-body-js.source.html");
const thinPath = path.join(dir, "lf-freight-calc-part2-thin-client.html");
const rawJsPath = path.join(dir, "_part2-raw.js");
const minJsPath = path.join(dir, "_part2-min.js");

const paste = fs.readFileSync(pastePath, "utf8");
// Only promote paste → source when paste is still the readable (non-minified) version.
if (
  !paste.includes("minified for Webflow 50k") &&
  (paste.includes("function localEstimate") || paste.includes("function buildPayload") || paste.includes("function palletize"))
) {
  fs.writeFileSync(sourcePath, paste);
}

const source = fs.readFileSync(sourcePath, "utf8");
const startTag = "<script>";
const endTag = "</script>";
const lastScriptStart = source.lastIndexOf(startTag);
const lastScriptEnd = source.lastIndexOf(endTag);
if (lastScriptStart < 0 || lastScriptEnd < lastScriptStart) {
  console.error("Could not find inline script in source");
  process.exit(1);
}

const js = source.slice(lastScriptStart + startTag.length, lastScriptEnd);
fs.writeFileSync(rawJsPath, js);

execFileSync(
  "npx",
  [
    "--yes",
    "terser",
    rawJsPath,
    "--compress",
    "passes=2,drop_console=true",
    "--mangle",
    "--comments",
    "false",
    "--output",
    minJsPath,
  ],
  { stdio: "inherit", shell: true }
);

const minJs = fs.readFileSync(minJsPath, "utf8").trim();
const out = `<!-- LF freight calc Part 2 (minified for Webflow 50k). Source: PART2-before-body-js.source.html -->
<script>
${minJs}
</script>
`;

fs.writeFileSync(pastePath, out);
fs.writeFileSync(thinPath, out);
fs.unlinkSync(rawJsPath);
fs.unlinkSync(minJsPath);

console.log(
  JSON.stringify(
    {
      sourceChars: source.length,
      pasteChars: out.length,
      jsIn: js.length,
      jsOut: minJs.length,
      under50k: out.length < 50000,
      headroom: 50000 - out.length,
    },
    null,
    2
  )
);
