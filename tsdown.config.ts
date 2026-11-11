// @ts-expect-error
import { readFileSync } from "node:fs";
import { defineConfig } from "tsdown";

const pkg = JSON.parse(readFileSync("package.json", "utf8"));

export default defineConfig({
  entry: Object.keys(pkg.exports).map((name) => `src/${name.slice(2)}`),
  outDir: "lib",
});
