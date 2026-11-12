// Runs a single benchmark measurement and prints the result as JSON. The
// runner starts a fresh process for every measurement, so libraries can't
// affect each other's memory usage or JIT optimisations.
//
// Usage: node benchmarks/worker.ts <suite> <case index> <size in bytes> <size name> [verify]
//
// With "verify", encoders keep their output and parse it back, so the runner
// can check it. This is slower, so verify runs are not timed.

import { dataFile, recordCountForBytes } from "./data.ts";
import type { CaseResult, Suite, WorkerResult } from "./shared.ts";

const [suiteName, caseIndex, sizeBytes, sizeName = "", mode] =
  process.argv.slice(2);
// Only the suite being measured is imported, so other suites' libraries
// aren't loaded
const suite: Suite = (await import(`./suites/${suiteName}.ts`)).default;
const benchmark = suite.cases[Number(caseIndex)];
if (!benchmark) throw new Error(`Unknown case: ${suiteName} ${caseIndex}`);
const size = Number(sizeBytes);

const start = performance.now();
const result = await benchmark.run({
  // Suites with generated input don't read a file
  file: suite.format ? dataFile(suite.format, sizeName) : "",
  count: recordCountForBytes(size),
  size,
  verify: mode === "verify",
});
const ms = performance.now() - start;
// Cases can return a `finish` function, to check their output after the
// timer has stopped
const { finish, ...rest } = result;
const checked: CaseResult = finish ? await finish() : {};

const output: WorkerResult = {
  ms,
  // maxRSS is in kilobytes
  maxRssBytes: process.resourceUsage().maxRSS * 1024,
  ...rest,
  ...checked,
};
process.stdout.write(JSON.stringify(output));
