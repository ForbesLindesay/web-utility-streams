// https://html.spec.whatwg.org/multipage/server-sent-events.html#parsing-an-event-stream
// https://html.spec.whatwg.org/multipage/server-sent-events.html#event-stream-interpretation

import LineSplitter from "./internal/LineSplitter.js";

const DEFAULT_EVENT_TYPE = "message";
const SPACE = 32;
const COLON = 58;
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
export default class EventSourceDecoderStream extends TransformStream<
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
    // The data lines of the current event, joined with "\n", or null if
    // there haven't been any
    let eventData: string | null = null;
    let splitter!: LineSplitter;

    super({
      start: (controller) => {
        splitter = new LineSplitter((text, start, end) => {
          if (start === end) {
            // A blank line marks the end of the event
            if (eventData !== null) {
              controller.enqueue({
                type: eventType || DEFAULT_EVENT_TYPE,
                data: eventData,
                id,
                retryMilliseconds,
              });
              id = null;
              retryMilliseconds = null;
            }
            eventType = null;
            eventData = null;
            return;
          }
          // If the line does not contain a ":" character, the entire line
          // is the field name. If it does contain a ":" character, the
          // field name is the string up to the first such character, while
          // the field value is the remaining characters (discarding an
          // optional leading space character). A line starting with ":" is
          // a comment, which has an empty field name, so it is ignored.
          //
          // The line is `text.slice(start, end)`, but we check the field
          // name in place, so we only create a string for the value.
          if (
            text.charCodeAt(start + 4) === COLON &&
            text.startsWith("data", start) &&
            start + 4 < end
          ) {
            // Most lines are data lines, so we handle them first
            const valueStart =
              text.charCodeAt(start + 5) === SPACE && start + 5 < end
                ? start + 6
                : start + 5;
            const value = text.slice(valueStart, end);
            eventData = eventData === null ? value : `${eventData}\n${value}`;
            return;
          }
          let nameEnd = text.indexOf(":", start);
          if (nameEnd === -1 || nameEnd > end) nameEnd = end;
          const field = fieldName(text, start, nameEnd);
          if (field === null) return;
          let valueStart = nameEnd + 1;
          if (valueStart < end && text.charCodeAt(valueStart) === SPACE) {
            valueStart++;
          }
          const value = valueStart < end ? text.slice(valueStart, end) : "";
          switch (field) {
            case "data":
              eventData = eventData === null ? value : `${eventData}\n${value}`;
              break;
            case "event":
              eventType = value;
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
          }
        });
      },
      transform: (chunk) => {
        splitter.write(chunk);
      },
      flush: () => {
        // An event that isn't followed by a blank line is not dispatched, so
        // we only need to process the final line for any id/retry fields
        splitter.end();
      },
    });
  }
}

/**
 * Get the name of the field in `text.slice(start, end)`, if it's one we
 * handle. Other field names (and comments) are ignored.
 */
function fieldName(
  text: string,
  start: number,
  end: number,
): "data" | "event" | "id" | "retry" | null {
  switch (end - start) {
    case 4:
      return text.startsWith("data", start) ? "data" : null;
    case 5:
      return text.startsWith("event", start)
        ? "event"
        : text.startsWith("retry", start)
          ? "retry"
          : null;
    case 2:
      return text.startsWith("id", start) ? "id" : null;
    default:
      return null;
  }
}
