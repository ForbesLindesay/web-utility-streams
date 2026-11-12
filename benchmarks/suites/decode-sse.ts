import EventSourceDecoderStream from "web-utility-streams/EventSourceDecoderStream";
import { EventSourceParserStream } from "eventsource-parser/stream";
// @microsoft/fetch-event-source doesn't export its parser, and its ESM build
// can't be imported by Node.js, so we import the CommonJS build directly
import fetchEventSourceParse from "@microsoft/fetch-event-source/lib/cjs/parse.js";
import {
  countWeb,
  OURS,
  readFileBaseline,
  type Suite,
  webFile,
  type WithId,
} from "../shared.ts";

/**
 * An event's id, which is the id of the record in its data
 */
const eventRecord = (event: { id?: string | null }): WithId => ({
  id: Number(event.id),
});

export default {
  format: "sse",
  description:
    "Decode a `text/event-stream` file with one event per record, each with an id, a type, and the record's JSON as its data (split over multiple lines for every 10th event)",
  cases: [
    {
      library: OURS,
      name: "EventSourceDecoderStream",
      ours: true,
      run: async ({ file }) =>
        countWeb(
          file,
          (stream) =>
            stream
              .pipeThrough(new TextDecoderStream())
              .pipeThrough(new EventSourceDecoderStream()),
          eventRecord,
        ),
    },
    {
      library: "eventsource-parser",
      name: "EventSourceParserStream",
      run: async ({ file }) =>
        countWeb(
          file,
          (stream) =>
            stream
              .pipeThrough(new TextDecoderStream())
              .pipeThrough(new EventSourceParserStream()),
          eventRecord,
        ),
    },
    {
      library: "@microsoft/fetch-event-source",
      name: "getLines + getMessages (callbacks, decodes bytes)",
      run: async ({ file }) => {
        const { stream, close } = await webFile(file);
        let count = 0;
        let idSum = 0;
        const onChunk = fetchEventSourceParse.getLines(
          fetchEventSourceParse.getMessages(
            () => {},
            () => {},
            (message) => {
              count++;
              idSum += Number(message.id);
            },
          ),
        );
        try {
          for await (const chunk of stream) onChunk(chunk);
        } finally {
          await close();
        }
        return { count, idSum };
      },
    },
    readFileBaseline,
  ],
} satisfies Suite;
