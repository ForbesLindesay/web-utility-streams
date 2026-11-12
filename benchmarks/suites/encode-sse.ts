import EventSourceEncoderStream from "web-utility-streams/EventSourceEncoderStream";
import { encode as eventSourceEncode } from "eventsource-encoder";
import { events, type DataEvent } from "../data.ts";
import { OURS, sinkWeb, type Suite, type WithId } from "../shared.ts";

/**
 * A web ReadableStream of `count` generated server-sent events
 */
function webEvents(count: number): ReadableStream<DataEvent> {
  return ReadableStream.from(events(count));
}

/**
 * Parse `text/event-stream` output, checking each event's data is the JSON
 * of the record with the event's id. This is deliberately simple and
 * independent of the libraries being measured, so it only supports the "\n"
 * line endings they all output.
 */
function* parseSse(text: string): Generator<WithId> {
  for (const block of text.split("\n\n")) {
    if (!block) continue;
    let id;
    const data = [];
    for (const line of block.split("\n")) {
      if (line.startsWith("id: ")) id = Number(line.slice(4));
      else if (line.startsWith("data: ")) data.push(line.slice(6));
    }
    const record = JSON.parse(data.join("\n"));
    if (record.id !== id) {
      throw new Error(`Event ${id} has the data for record ${record.id}`);
    }
    yield record;
  }
}

export default {
  format: "sse",
  encoder: true,
  description:
    "Encode generated events, each with an id, a type, and a record's JSON as its data (multi-line for every 10th event), discarding the output",
  cases: [
    {
      library: OURS,
      name: "EventSourceEncoderStream",
      ours: true,
      run: ({ count, verify }) =>
        sinkWeb(
          webEvents(count).pipeThrough(new EventSourceEncoderStream()),
          verify,
          parseSse,
        ),
    },
    {
      library: "eventsource-encoder",
      name: "encode() in a TransformStream",
      run: ({ count, verify }) =>
        sinkWeb(
          webEvents(count).pipeThrough(
            new TransformStream({
              transform(event, controller) {
                controller.enqueue(
                  eventSourceEncode({
                    event: event.type,
                    id: event.id,
                    data: event.data,
                  }),
                );
              },
            }),
          ),
          verify,
          parseSse,
        ),
    },
    {
      library: "baseline",
      name: "Generate the events only",
      baseline: true,
      run: async ({ count }) => {
        let chars = 0;
        for await (const event of webEvents(count)) chars += event.data.length;
        return { chars: 0, ignored: chars };
      },
    },
  ],
} satisfies Suite;
