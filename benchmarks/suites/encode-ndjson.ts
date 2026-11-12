import { Readable } from "node:stream";
import NewlineDelimitedJsonEncoderStream from "web-utility-streams/NewlineDelimitedJsonEncoderStream";
import ndjson from "ndjson";
import { stringer as streamJsonJsonlStringer } from "stream-json/jsonl/stringer.js";
import { records } from "../data.ts";
import {
  OURS,
  parseNdjson,
  sinkEvents,
  sinkWeb,
  type Suite,
  textSink,
  webRecords,
} from "../shared.ts";

export default {
  format: "ndjson",
  encoder: true,
  description:
    "Encode generated records as newline delimited JSON, discarding the output",
  cases: [
    {
      library: OURS,
      name: "NewlineDelimitedJsonEncoderStream",
      ours: true,
      run: ({ count, verify }) =>
        sinkWeb(
          webRecords(count).pipeThrough(
            new NewlineDelimitedJsonEncoderStream(),
          ),
          verify,
          parseNdjson,
        ),
    },
    {
      library: "ndjson",
      name: "ndjson.stringify()",
      run: ({ count, verify }) =>
        sinkEvents(
          Readable.from(records(count)).pipe(ndjson.stringify()),
          verify,
          parseNdjson,
        ),
    },
    {
      library: "stream-json",
      name: "jsonl/stringer (Node streams)",
      run: ({ count, verify }) =>
        sinkEvents(
          Readable.from(records(count)).pipe(streamJsonJsonlStringer()),
          verify,
          parseNdjson,
        ),
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
    {
      library: "baseline",
      name: "JSON.stringify + .join('\\n')",
      baseline: true,
      run: async ({ count, verify }) => {
        const sink = textSink(verify, parseNdjson);
        for await (const record of webRecords(count)) {
          sink.write(JSON.stringify(record) + "\n");
        }
        return sink.result();
      },
    },
  ],
} satisfies Suite;
