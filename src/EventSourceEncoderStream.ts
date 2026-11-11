// https://html.spec.whatwg.org/multipage/server-sent-events.html#parsing-an-event-stream

import type { EventSourceEvent } from "./EventSourceDecoderStream.js";
import { DEFAULT_LINE_ENDING, type LineEnding } from "./LineEndings.js";

const DEFAULT_EVENT_TYPE = "message";
const LINE_BREAK = /\r\n|\r|\n/;

/**
 * The input to `EventSourceEncoderStream`. Only `data` is required. `type`
 * defaults to `"message"`. Other fields are not sent if not provided.
 */
export interface EventSourceEncoderInput {
  type?: EventSourceEvent["type"] | null;
  data: EventSourceEvent["data"];
  id?: EventSourceEvent["id"];
  retryMilliseconds?: EventSourceEvent["retryMilliseconds"];
}

export interface EventSourceEncoderStreamOptions {
  /**
   * The line ending used to terminate each line of the output. Defaults to
   * `"\n"`. Line breaks within `data` are also converted to this line ending.
   */
  lineEnding?: LineEnding;
}

/**
 * Encode events in the `text/event-stream` format. Each event is output as
 * a single string chunk, including the trailing blank line that marks the end
 * of the event.
 *
 * The output is suitable for piping through a `TextEncoderStream` and sending
 * as the body of an HTTP response with `Content-Type: text/event-stream`. It
 * can be decoded using `EventSourceDecoderStream`.
 *
 * Events that cannot be represented in the `text/event-stream` format (e.g. an
 * `id` or `type` containing a line break) will error the stream.
 */
export default class EventSourceEncoderStream extends TransformStream<
  EventSourceEncoderInput,
  string
> {
  constructor({
    lineEnding = DEFAULT_LINE_ENDING,
  }: EventSourceEncoderStreamOptions = {}) {
    super({
      transform(event, controller) {
        controller.enqueue(encodeEvent(event, lineEnding));
      },
    });
  }
}

function encodeEvent(
  { type, data, id, retryMilliseconds }: EventSourceEncoderInput,
  lineEnding: LineEnding,
) {
  let result = "";
  if (type != null && type !== "" && type !== DEFAULT_EVENT_TYPE) {
    assertSingleLine("type", type);
    result += `event: ${type}${lineEnding}`;
  }
  if (id != null) {
    assertSingleLine("id", id);
    if (id.includes("\u0000")) {
      throw new TypeError(
        `EventSourceEncoderStream: "id" cannot contain a null character`,
      );
    }
    result += `id: ${id}${lineEnding}`;
  }
  if (retryMilliseconds != null) {
    if (!Number.isSafeInteger(retryMilliseconds) || retryMilliseconds < 0) {
      throw new TypeError(
        `EventSourceEncoderStream: "retryMilliseconds" must be a non-negative integer, got ${retryMilliseconds}`,
      );
    }
    result += `retry: ${retryMilliseconds}${lineEnding}`;
  }
  assertString("data", data);
  for (const line of data.split(LINE_BREAK)) {
    // Always include the space after the colon, so that if the line itself
    // starts with a space, that space is preserved when decoding.
    result += `data: ${line}${lineEnding}`;
  }
  return result + lineEnding;
}

function assertString(name: string, value: string) {
  if (typeof value !== "string") {
    throw new TypeError(
      `EventSourceEncoderStream: Expected "${name}" to be a string but got ${typeof value}`,
    );
  }
}
function assertSingleLine(name: string, value: string) {
  assertString(name, value);
  if (LINE_BREAK.test(value)) {
    throw new TypeError(
      `EventSourceEncoderStream: "${name}" cannot contain line breaks`,
    );
  }
}
