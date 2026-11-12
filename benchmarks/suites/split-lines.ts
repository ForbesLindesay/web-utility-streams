import fs from "node:fs";
import readline from "node:readline";
import SplitLinesStream from "web-utility-streams/SplitLinesStream";
import split2 from "split2";
import {
  countIterable,
  countWeb,
  MAX_IN_MEMORY_BYTES,
  nodeFile,
  OURS,
  readFileBaseline,
  type Suite,
  type WithId,
} from "../shared.ts";

/**
 * Each line of the NDJSON file starts with `{"id":N,`, so we can get the id
 * cheaply, to check every line was split correctly
 */
const lineRecord = (line: string): WithId => ({
  id: Number(line.slice(6, line.indexOf(","))),
});

function* splitLines(text: string): Generator<string> {
  for (const line of text.split("\n")) if (line) yield line;
}

export default {
  format: "ndjson",
  description:
    "Split a file with one JSON record per line into lines, without parsing them",
  cases: [
    {
      library: OURS,
      name: "SplitLinesStream",
      ours: true,
      run: async ({ file }) =>
        countWeb(
          file,
          (stream) =>
            stream
              .pipeThrough(new TextDecoderStream())
              .pipeThrough(new SplitLinesStream()),
          lineRecord,
        ),
    },
    {
      library: "split2",
      name: "split2()",
      run: async ({ file }) =>
        countIterable((await nodeFile(file)).pipe(split2()), lineRecord),
    },
    {
      library: "node:readline",
      name: "readline.createInterface",
      run: async ({ file }) =>
        countIterable(
          readline.createInterface({
            input: await nodeFile(file),
            crlfDelay: Infinity,
          }),
          lineRecord,
        ),
    },
    {
      library: "baseline",
      name: "split of the whole file (not streaming)",
      maxSize: MAX_IN_MEMORY_BYTES,
      run: async ({ file }) =>
        countIterable(
          splitLines(await fs.promises.readFile(file, "utf8")),
          lineRecord,
        ),
    },
    readFileBaseline,
  ],
} satisfies Suite;
