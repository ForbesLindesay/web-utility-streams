// Runs a single benchmark measurement and prints the result as JSON. The
// runner starts a fresh process for every measurement, so libraries can't
// affect each other's memory usage or JIT optimisations.
//
// Usage: node benchmarks/worker.mjs <suite> <case index> <size in bytes> <size name> [verify]
//
// With "verify", encoders keep their output and parse it back, so the runner
// can check it. This is slower, so verify runs are not timed.

import { suites } from "./cases.mjs";
import { dataFile, recordCountForBytes } from "./data.mjs";

const [suiteName, caseIndex, sizeBytes, sizeName, mode] = process.argv.slice(2);
const suite = suites[suiteName];
const benchmark = suite.cases[Number(caseIndex)];
const size = Number(sizeBytes);

const start = performance.now();
const result = await benchmark.run({
  file: dataFile(suite.format, sizeName),
  count: recordCountForBytes(size),
  size,
  verify: mode === "verify",
});
const ms = performance.now() - start;

process.stdout.write(
  JSON.stringify({
    ms,
    // maxRSS is in kilobytes
    maxRssBytes: process.resourceUsage().maxRSS * 1024,
    ...result,
  }),
);
