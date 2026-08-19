import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { execSync } from "child_process";

const root = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const srcPath = path.join(root, "webflow-embeds/PART2-before-body-js.source.html");
const outPath = path.join(root, "public/embeds/freight-part2.js");
const tmpPath = path.join(root, "webflow-embeds/.part2-build.js");

const src = fs.readFileSync(srcPath, "utf8");
const match = src.match(/<script>\s*([\s\S]*?)\s*<\/script>/i);
if (!match) throw new Error("script block not found in PART2 source");

const js = match[1].trim();
fs.writeFileSync(tmpPath, js);
execSync(`npx --yes terser "${tmpPath}" -c -m -o "${outPath}"`, {
  stdio: "inherit",
  cwd: root,
});
fs.unlinkSync(tmpPath);
console.log(`built ${outPath} (${fs.statSync(outPath).size} bytes)`);
