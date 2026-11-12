import fs from "node:fs";
import readline from "node:readline";
import NewlineDelimitedJsonDecoderStream from "web-utility-streams/NewlineDelimitedJsonDecoderStream";
import { JSONParser as StreamParserJson } from "@streamparser/json-whatwg";
import ndjson from "ndjson";
import split2 from "split2";
import {
  asRecord,
  countIterable,
  countWeb,
  MAX_IN_MEMORY_BYTES,
  nodeFile,
  OURS,
  parseNdjson,
  type Suite,
  webFile,
} from "../shared.ts";

export default {
  format: "ndjson",
  description: "Decode a file with one JSON record per line",
  cases: [
    {
      library: OURS,
      name: "NewlineDelimitedJsonDecoderStream",
      ours: true,
      run: async ({ file }) =>
        countWeb(file, (stream) =>
          stream
            .pipeThrough(new TextDecoderStream())
            .pipeThrough(new NewlineDelimitedJsonDecoderStream()),
        ),
    },
    {
      library: "split2",
      name: "split2(JSON.parse)",
      run: async ({ file }) =>
        countIterable((await nodeFile(file)).pipe(split2(JSON.parse))),
    },
    {
      library: "ndjson",
      name: "ndjson.parse()",
      run: async ({ file }) =>
        countIterable((await nodeFile(file)).pipe(ndjson.parse())),
    },
    // {
    //   library: "stream-json",
    //   name: "jsonl/parser (Node streams)",
    //   run: async ({ file }) =>
    //     countIterable(
    //       (await nodeFile(file)).pipe(streamJsonJsonlParser.asStream()),
    //       (item) => asRecord(item.value),
    //     ),
    // },
    {
      library: "@streamparser/json",
      name: 'JSONParser with separator "\\n" (web streams)',
      run: async ({ file }) =>
        countWeb(
          file,
          (stream) =>
            stream.pipeThrough(
              new StreamParserJson({ paths: ["$"], separator: "\n" }),
            ),
          (item) => asRecord(item.value),
        ),
    },
    {
      library: "node:readline",
      name: "readline + JSON.parse",
      run: async ({ file }) =>
        countIterable(
          (async function* () {
            const lines = readline.createInterface({
              input: await nodeFile(file),
              crlfDelay: Infinity,
            });
            for await (const line of lines) {
              if (line) yield JSON.parse(line);
            }
          })(),
        ),
    },
    {
      library: "baseline",
      name: "split + JSON.parse of the whole file (not streaming)",
      maxSize: MAX_IN_MEMORY_BYTES,
      run: async ({ file }) =>
        countIterable(parseNdjson(await fs.promises.readFile(file, "utf8"))),
    },
    {
      library: "baseline",
      name: "Read the file through TextDecoderStream only",
      baseline: true,
      run: async ({ file }) => {
        const { stream, close } = await webFile(file);
        try {
          let chars = 0;
          for await (const chunk of stream.pipeThrough(
            new TextDecoderStream(),
          )) {
            chars += chunk.length;
          }
          return { chars };
        } finally {
          await close();
        }
      },
    },
  ],
} satisfies Suite;
