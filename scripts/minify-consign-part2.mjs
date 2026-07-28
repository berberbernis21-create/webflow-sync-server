import fs from "fs";
import { execFileSync } from "child_process";
import path from "path";

const dir = "webflow-embeds/consign-submit";
const pastePath = path.join(dir, "PART2-before-body-js.html");
const sourcePath = path.join(dir, "PART2-before-body-js.source.html");
const rawJsPath = path.join(dir, "_part2-raw.js");
const minJsPath = path.join(dir, "_part2-min.js");

const paste = fs.readFileSync(pastePath, "utf8");
if (paste.includes("function createLfModal")) {
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
const out = `<!--
LOST & FOUND CONSIGNMENT SUBMISSION
PART 2: Before </body> (minified for Webflow 50k limit)
Readable source: PART2-before-body-js.source.html
-->
<script src="https://cdn.jsdelivr.net/npm/heic2any@0.0.4/dist/heic2any.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/compressorjs@1.3.0/dist/compressor.min.js"></script>
<script>
${minJs}
</script>
`;

fs.writeFileSync(pastePath, out);
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
