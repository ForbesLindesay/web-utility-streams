// https://html.spec.whatwg.org/multipage/server-sent-events.html#parsing-an-event-stream
// https://html.spec.whatwg.org/multipage/server-sent-events.html#event-stream-interpretation

import SplitLinesStream from "./SplitLinesStream.js";
import TransformPipelineStream from "./TransformPipelineStream.js";

const DEFAULT_EVENT_TYPE = "message";
export interface EventSourceEvent {
  type: string;
  data: string;
  id: string | null;
  /**
   * The reconnection time in milliseconds
   */
  retryMilliseconds: number | null;
}

export interface EventSourceDecoderStreamOptions {
  /**
   * Called whenever a valid `id` field is received, even if no event is
   * dispatched afterwards (e.g. the stream ends or the event has no data).
   */
  onId?: (id: string) => void;
  /**
   * Called whenever a valid `retry` field is received, even if no event is
   * dispatched afterwards (e.g. the stream ends or the event has no data).
   */
  onRetry?: (retryMilliseconds: number) => void;
}

/**
 * Decode a stream of text in the `text/event-stream` format into
 * `EventSourceEvent` objects. The text can be split into chunks at any point.
 *
 * The `id` and `retryMilliseconds` properties of each event reflect any `id`
 * and `retry` fields received since the previous event was emitted, or `null`
 * if there were none. To be notified of every `id` and `retry` field as soon
 * as it is received, use the `onId` and `onRetry` options.
 *
 * If `onId` or `onRetry` throws, the stream is errored.
 */
export default class EventSourceDecoderStream extends TransformPipelineStream<
  string,
  EventSourceEvent
> {
  private _latestId: string | null = null;
  private _retryMilliseconds: number | null = null;

  public get latestId(): string | null {
    return this._latestId;
  }
  public get retryMilliseconds(): number | null {
    return this._retryMilliseconds;
  }

  constructor({ onId, onRetry }: EventSourceDecoderStreamOptions = {}) {
    let id: string | null = null;
    let retryMilliseconds: number | null = null;
    let eventType: string | null = null;
    let eventDataBuffer: string[] = [];

    super((input) =>
      input.pipeThrough(new SplitLinesStream()).pipeThrough(
        new TransformStream({
          transform: (line, controller) => {
            if (line.startsWith(":")) {
              // comment
            } else if (line === "") {
              // end of event
              if (eventDataBuffer.length) {
                controller.enqueue({
                  type: eventType || DEFAULT_EVENT_TYPE,
                  data: eventDataBuffer.join("\n"),
                  id,
                  retryMilliseconds,
                });
                id = null;
                retryMilliseconds = null;
              }
              eventType = null;
              eventDataBuffer = [];
            } else {
              // If the line does not contain a ":" character, the entire line
              // is the field name. If it does contain a ":" character, the
              // field name is the string up to the first such character, while
              // the field value is the remaining characters (discarding an optional
              // leading space character).
              let fieldName = line;
              let value = "";
              const idx = line.indexOf(":");
              if (idx !== -1) {
                fieldName = line.slice(0, idx);
                value = line.slice(line[idx + 1] === " " ? idx + 2 : idx + 1);
              }
              switch (fieldName) {
                case "event":
                  eventType = value;
                  break;
                case "data":
                  eventDataBuffer.push(value);
                  break;
                case "id":
                  if (!value.includes("\u0000")) {
                    id = value;
                    this._latestId = value;
                    onId?.(value);
                  }
                  break;
                case "retry":
                  if (/^\d+$/.test(value)) {
                    retryMilliseconds = parseInt(value, 10);
                    this._retryMilliseconds = retryMilliseconds;
                    onRetry?.(retryMilliseconds);
                  }
                  break;
                // ignore other field names
              }
            }
          },
        }),
      ),
    );
  }
}
