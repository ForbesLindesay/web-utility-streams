import fs from "node:fs";
import JsonDecoderStream from "web-utility-streams/JsonDecoderStream";
import { JSONParser as StreamParserJson } from "@streamparser/json-whatwg";
import bfj from "bfj";
import JSONStream from "JSONStream";
import webChain from "stream-chain/web";
import { parser as streamJsonParser } from "stream-json";
import { pick as streamJsonPick } from "stream-json/filters/pick.js";
import { streamArray as streamJsonStreamArray } from "stream-json/streamers/stream-array.js";
import {
  asRecord,
  asTransform,
  type Counted,
  countIterable,
  countWeb,
  MAX_IN_MEMORY_BYTES,
  nodeFile,
  OURS,
  type Suite,
  webFile,
  type WithId,
} from "../shared.ts";

/**
 * For older streams that don't support async iteration
 */
function countEvents(
  stream: NodeJS.ReadableStream | NodeJS.ReadWriteStream,
  sources: NodeJS.ReadableStream[] = [],
  getRecord: (value: unknown) => WithId = (value) => value as WithId,
): Promise<Counted> {
  return new Promise((resolve, reject) => {
    let count = 0;
    let idSum = 0;
    stream.on("data", (value: unknown) => {
      count++;
      idSum += getRecord(value).id;
    });
    stream.on("end", () => resolve({ count, idSum }));
    stream.on("error", reject);
    for (const source of sources) source.on("error", reject);
  });
}

export default {
  format: "json",
  description:
    'Decode the elements of the "results" array in `{"total":N,"page":1,"results":[...],"next":null}`',
  cases: [
    {
      library: OURS,
      name: "JsonDecoderStream",
      ours: true,
      run: async ({ file }) =>
        countWeb(file, (stream) =>
          stream
            .pipeThrough(new TextDecoderStream())
            .pipeThrough(new JsonDecoderStream({ path: ["results"] })),
        ),
    },
    // {
    //   library: "stream-json",
    //   name: "chain([parser, pick, streamArray]) (Node streams)",
    //   run: async ({ file }) =>
    //     countIterable(
    //       (await nodeFile(file)).pipe(
    //         chain([
    //           streamJsonParser(),
    //           streamJsonPick({ filter: "results" }),
    //           streamJsonStreamArray(),
    //         ]),
    //       ),
    //       (item) => asRecord(item.value),
    //     ),
    // },
    {
      library: "stream-json",
      name: "chain([parser, pick, streamArray]) (web streams)",
      run: async ({ file }) =>
        countWeb(
          file,
          (stream) =>
            stream.pipeThrough(
              asTransform(
                webChain([
                  streamJsonParser(),
                  streamJsonPick({ filter: "results" }),
                  streamJsonStreamArray(),
                ]),
              ),
            ),
          (item) => asRecord(item.value),
        ),
    },
    {
      library: "@streamparser/json",
      name: "JSONParser (web streams)",
      run: async ({ file }) =>
        countWeb(
          file,
          (stream) =>
            stream.pipeThrough(
              new StreamParserJson({
                paths: ["$.results.*"],
                keepStack: false,
              }),
            ),
          (item) => asRecord(item.value),
        ),
    },
    {
      library: "JSONStream",
      name: 'JSONStream.parse("results.*")',
      run: async ({ file }) => {
        const source = await nodeFile(file);
        return countEvents(source.pipe(JSONStream.parse("results.*")), [
          source,
        ]);
      },
    },
    {
      library: "bfj",
      name: "bfj.match",
      maxSize: MAX_IN_MEMORY_BYTES, // It may look like a streaming API, but it doesn't really seem ot stream
      run: async ({ file }) =>
        countIterable(
          bfj.match(await nodeFile(file), (_key, _value, depth) => depth === 2),
        ),
    },
    {
      library: "baseline",
      name: "JSON.parse of the whole file (not streaming)",
      maxSize: MAX_IN_MEMORY_BYTES,
      run: async ({ file }) =>
        countIterable(
          JSON.parse(await fs.promises.readFile(file, "utf8")).results,
        ),
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
