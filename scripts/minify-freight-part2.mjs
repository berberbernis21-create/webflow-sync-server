import fs from "fs";
import { execFileSync } from "child_process";
import path from "path";
import crypto from "crypto";

const dir = "webflow-embeds";
const publicEmbeds = path.join("public", "embeds");
const pastePath = path.join(dir, "PART2-before-body-js.html");
const sourcePath = path.join(dir, "PART2-before-body-js.source.html");
const thinPath = path.join(dir, "lf-freight-calc-part2-thin-client.html");
const hostedJsPath = path.join(publicEmbeds, "freight-part2.js");
const rawJsPath = path.join(dir, "_part2-raw.js");
const minJsPath = path.join(dir, "_part2-min.js");
const API_BASE = "https://webflow-sync-server.onrender.com";

fs.mkdirSync(publicEmbeds, { recursive: true });

const paste = fs.readFileSync(pastePath, "utf8");
// Only promote paste → source when paste is still the readable (non-minified) version.
if (
  !paste.includes("minified for Webflow 50k") &&
  !paste.includes("loads JS from Render") &&
  (paste.includes("function localEstimate") ||
    paste.includes("function buildPayload") ||
    paste.includes("function palletize"))
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
fs.writeFileSync(hostedJsPath, minJs + "\n");

const hash = crypto.createHash("sha1").update(minJs).digest("hex").slice(0, 10);
const loader = `<!-- LF freight calc Part 2 (loads JS from Render to stay under Webflow 50k). Source: PART2-before-body-js.source.html -->
<script>
(function(){
  var s=document.createElement("script");
  s.src="${API_BASE}/embeds/freight-part2.js?v=${hash}";
  s.defer=true;
  s.onerror=function(){
    var host=document.getElementById("lfCalcHost")||document.getElementById("lfCalc");
    if(host)host.innerHTML='<p style="margin:24px auto;max-width:980px;padding:18px;font:14px/1.5 Arial,Helvetica,sans-serif;color:#9c2f2f;text-align:center">Could not load the delivery calculator script. Please refresh the page.</p>';
  };
  document.head.appendChild(s);
})();
</script>
`;

fs.writeFileSync(pastePath, loader);
fs.writeFileSync(thinPath, loader);
fs.unlinkSync(rawJsPath);
fs.unlinkSync(minJsPath);

console.log(
  JSON.stringify(
    {
      sourceChars: source.length,
      pasteChars: loader.length,
      hostedJsChars: minJs.length,
      hash,
      under50k: loader.length < 50000,
      headroom: 50000 - loader.length,
    },
    null,
    2
  )
);
