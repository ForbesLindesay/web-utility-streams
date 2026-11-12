import { Readable } from "node:stream";
import JsonEncoderStream from "web-utility-streams/JsonEncoderStream";
import JSONStream from "JSONStream";
import chain from "stream-chain";
import { disassembler as streamJsonDisassembler } from "stream-json/disassembler.js";
import { stringer as streamJsonStringer } from "stream-json/stringer.js";
import { records } from "../data.ts";
import {
  MAX_IN_MEMORY_BYTES,
  OURS,
  sinkEvents,
  sinkWeb,
  type Suite,
  textSink,
  webRecords,
  type WithId,
} from "../shared.ts";

const parseJsonArray = (text: string): WithId[] => JSON.parse(text);

const parseJsonResults = (text: string): WithId[] => JSON.parse(text).results;

export default {
  format: "json",
  encoder: true,
  description:
    "Encode generated records as the elements of a JSON array, discarding the output",
  cases: [
    {
      library: OURS,
      name: "JsonEncoderStream",
      ours: true,
      run: ({ count, verify }) =>
        sinkWeb(
          webRecords(count).pipeThrough(new JsonEncoderStream()),
          verify,
          parseJsonArray,
        ),
    },
    {
      library: OURS,
      name: 'JsonEncoderStream with path ["results"] and extraData',
      ours: true,
      run: ({ count, verify }) =>
        sinkWeb(
          webRecords(count).pipeThrough(
            new JsonEncoderStream({
              path: ["results"],
              extraData: { total: count, page: 1 },
            }),
          ),
          verify,
          parseJsonResults,
        ),
    },
    {
      library: "JSONStream",
      name: "JSONStream.stringify()",
      run: ({ count, verify }) =>
        sinkEvents(
          Readable.from(records(count)).pipe(JSONStream.stringify()),
          verify,
          parseJsonArray,
        ),
    },
    {
      library: "stream-json",
      name: "chain([disassembler, stringer]) (Node streams)",
      run: ({ count, verify }) =>
        sinkEvents(
          Readable.from(records(count)).pipe(
            chain([
              streamJsonDisassembler(),
              streamJsonStringer({ makeArray: true }),
            ]),
          ),
          verify,
          parseJsonArray,
        ),
    },
    {
      library: "baseline",
      name: "JSON.stringify of the whole array (not streaming)",
      maxSize: MAX_IN_MEMORY_BYTES,
      run: ({ count, verify }) => {
        const sink = textSink(verify, parseJsonArray);
        sink.write(JSON.stringify([...records(count)]));
        return sink.result();
      },
    },
    {
      library: "baseline",
      name: "Generate the records only",
      baseline: true,
      run: async ({ count }) => {
        let chars = 0;
        for await (const record of webRecords(count)) chars += record.id;
        return { chars: 0, ignored: chars };
      },
    },
  ],
} satisfies Suite;
