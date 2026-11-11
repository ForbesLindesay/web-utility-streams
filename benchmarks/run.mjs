// Runs the benchmarks and prints the results as Markdown tables.
//
// Usage: node benchmarks/run.mjs [options]
//
//   --sizes        Input sizes (default: 10MB,256MB,2GB)
//   --suites       decode-json, decode-ndjson, encode-json, encode-ndjson,
//                  split-lines, decode-sse, encode-sse
//                  (default: all)
//   --libraries    Only run cases whose library name includes one of these
//   --runs         Runs per measurement, the median is reported
//                  (default: 5 for sizes up to 100MB, otherwise 1)
//   --out          The Markdown file to write the results to, as they are
//                  produced (default: benchmarks/RESULTS.md)
//
// The library must be built first (`npm run build`), as the benchmarks import
// it from `lib/` via the package's exports.

import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { parseArgs } from "node:util";

import { suites } from "./cases.mjs";

// Encoder output is only collected and checked for sizes up to this, as it
// has to fit in a single string
const MAX_VERIFIED_OUTPUT_BYTES = 256e6;
import {
  dataFile,
  expectedIdSum,
  recordCountForBytes,
  writeDataFile,
} from "./data.mjs";

const { values: options } = parseArgs({
  options: {
    sizes: { type: "string", default: "10MB,256MB,2GB" },
    suites: { type: "string", default: Object.keys(suites).join(",") },
    libraries: { type: "string" },
    runs: { type: "string" },
    out: {
      type: "string",
      default: path.join(import.meta.dirname, "RESULTS.md"),
    },
  },
});

const list = (value) =>
  value
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);

function parseSize(text) {
  const match = /^(\d+(?:\.\d+)?)(KB|MB|GB)$/i.exec(text);
  if (!match) throw new Error(`Invalid size: ${text} (expected e.g. 10MB)`);
  const units = { KB: 1e3, MB: 1e6, GB: 1e9 };
  return Math.round(Number(match[1]) * units[match[2].toUpperCase()]);
}

const sizes = list(options.sizes).map((name) => ({
  name,
  bytes: parseSize(name),
}));
const suiteNames = list(options.suites);
for (const name of suiteNames) {
  if (!suites[name]) throw new Error(`Unknown suite: ${name}`);
}
const libraryFilter = options.libraries ? list(options.libraries) : undefined;

if (!fs.existsSync(path.join(import.meta.dirname, "..", "lib"))) {
  console.error("Build the library first: npm run build");
  process.exit(1);
}

const formatBytes = (bytes) =>
  bytes >= 1e9
    ? `${(bytes / 1e9).toFixed(2)} GB`
    : `${(bytes / 1e6).toFixed(bytes >= 1e8 ? 0 : 1)} MB`;
const formatTime = (ms) =>
  ms >= 60_000
    ? `${(ms / 60_000).toFixed(1)} min`
    : ms >= 1000
      ? `${(ms / 1000).toFixed(2)} s`
      : `${ms.toFixed(0)} ms`;
const median = (values) => {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.floor(sorted.length / 2)];
};

/**
 * Run one measurement in a fresh Node.js process
 */
function runWorker(args) {
  return new Promise((resolve) => {
    const child = spawn(
      process.execPath,
      [path.join(import.meta.dirname, "worker.mjs"), ...args],
      { stdio: ["ignore", "pipe", "pipe"] },
    );
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (chunk) => (stdout += chunk));
    child.stderr.on("data", (chunk) => (stderr += chunk));
    child.on("close", (code) => {
      if (code === 0) {
        resolve(JSON.parse(stdout));
      } else {
        const message =
          stderr
            .split("\n")
            .find((line) => /Error|heap out of memory/.test(line)) ??
          `exited with code ${code}`;
        resolve({ error: message.trim() });
      }
    });
  });
}

/**
 * Check a result is correct, returning an error message if not
 */
function check(result, suite, size) {
  if (result.error) return result.error;
  const count = recordCountForBytes(size.bytes);
  if (result.count === undefined) {
    // Baselines, and encoders that aren't verifying their output
    return suite.encoder && !result.chars && result.ignored === undefined
      ? "no output"
      : undefined;
  }
  if (result.count !== count || result.idSum !== expectedIdSum(count)) {
    return `incorrect result: ${result.count} records with id sum ${result.idSum}, expected ${count} records with id sum ${expectedIdSum(count)}`;
  }
}

async function ensureDataFile(format, size) {
  const file = dataFile(format, size.name);
  if (fs.existsSync(file)) return;
  process.stderr.write(`Generating ${file}... `);
  const start = performance.now();
  await writeDataFile(file, format, recordCountForBytes(size.bytes));
  process.stderr.write(
    `done (${formatBytes(fs.statSync(file).size)} in ${formatTime(performance.now() - start)})\n`,
  );
}

// Write the results to the output file as they are produced, so a long run
// can be followed (and partial results aren't lost if it is stopped).
fs.writeFileSync(options.out, "");
const print = (line = "") => {
  console.log(line);
  fs.appendFileSync(options.out, line + "\n");
};

print("# Benchmark results");
print();
print(
  `- Node.js ${process.version} on ${os.type()} ${os.arch()}, ${os.cpus()[0]?.model ?? "unknown CPU"}, ${Math.round(os.totalmem() / 2 ** 30)} GB RAM`,
);
print(`- Date: ${new Date().toISOString().slice(0, 10)}`);
print(
  "- Times are the median of the runs, measured in a fresh process for each run. Throughput is input bytes (for decoders) or output characters (for encoders) per second. Peak memory is the peak resident set size of the process. Decoders that use web streams read the input with `FileHandle.readableWebStream()` (16KiB chunks), and decoders that use Node.js streams read it with `fs.createReadStream()` (64KiB chunks).",
);

for (const suiteName of suiteNames) {
  const suite = suites[suiteName];
  print();
  print(`## ${suiteName}`);
  print();
  print(suite.description + ".");

  for (const size of sizes) {
    const runs = Number(options.runs ?? (size.bytes <= 100e6 ? 5 : 1));
    if (!suite.encoder) await ensureDataFile(suite.format, size);
    const fileBytes = suite.encoder
      ? undefined
      : fs.statSync(dataFile(suite.format, size.name)).size;

    const rows = [];
    for (const [index, benchmark] of suite.cases.entries()) {
      if (
        libraryFilter &&
        benchmark.library !== "baseline" &&
        !libraryFilter.some((name) => benchmark.library.includes(name))
      ) {
        continue;
      }
      const row = { benchmark, maxRssBytes: 0 };
      rows.push(row);
      if (benchmark.maxSize !== undefined && size.bytes > benchmark.maxSize) {
        row.cell = "n/a (too large for memory)";
        continue;
      }
      const label = `${suiteName} ${size.name}: ${benchmark.library} ${benchmark.name}`;
      const times = [];
      let error;
      let outputBytes;
      if (
        suite.encoder &&
        benchmark.library !== "baseline" &&
        size.bytes <= MAX_VERIFIED_OUTPUT_BYTES
      ) {
        // Check the output in a separate, untimed run, so collecting the
        // output doesn't affect the timed runs' speed or memory usage
        process.stderr.write(`${label} (verify)... `);
        const result = await runWorker([
          suiteName,
          String(index),
          String(size.bytes),
          size.name,
          "verify",
        ]);
        error =
          check(result, suite, size) ??
          (result.count === undefined ? "output was not checked" : undefined);
        process.stderr.write(error ? `FAILED: ${error}\n` : "OK\n");
        if (error) {
          row.cell = `failed: ${error}`;
          continue;
        }
      }
      for (let run = 0; run < runs; run++) {
        process.stderr.write(`${label} (run ${run + 1}/${runs})... `);
        const result = await runWorker([
          suiteName,
          String(index),
          String(size.bytes),
          size.name,
        ]);
        error = check(result, suite, size);
        if (error) {
          process.stderr.write(`FAILED: ${error}\n`);
          break;
        }
        process.stderr.write(`${formatTime(result.ms)}\n`);
        times.push(result.ms);
        outputBytes = result.chars;
        row.maxRssBytes = Math.max(row.maxRssBytes, result.maxRssBytes);
      }
      if (error) {
        row.cell = `failed: ${error}`;
        continue;
      }
      row.time = median(times);
      const bytes = suite.encoder ? outputBytes : fileBytes;
      row.cell = bytes
        ? `${formatTime(row.time)} (${((bytes / 1e6 / row.time) * 1000).toFixed(0)} MB/s)`
        : formatTime(row.time);
    }

    // Fastest first, with baselines at the end
    const isBaseline = (row) => row.benchmark.library === "baseline";
    rows.sort(
      (a, b) =>
        Number(isBaseline(a)) - Number(isBaseline(b)) ||
        (a.time ?? Infinity) - (b.time ?? Infinity),
    );

    print();
    print(
      suite.encoder
        ? `### ${size.name} (${recordCountForBytes(size.bytes).toLocaleString("en")} records)`
        : `### ${size.name} (${formatBytes(fileBytes)}, ${recordCountForBytes(size.bytes).toLocaleString("en")} records)`,
    );
    print();
    print("| Library | Approach | Time | Peak memory |");
    print("| --- | --- | --- | --- |");
    for (const row of rows) {
      const bold = (text) => (row.benchmark.ours ? `**${text}**` : text);
      print(
        `| ${bold(row.benchmark.library)} | ${bold(row.benchmark.name)} | ${bold(row.cell)} | ${row.maxRssBytes ? formatBytes(row.maxRssBytes) : ""} |`,
      );
    }
  }
}

console.error(`Results written to ${options.out}`);
