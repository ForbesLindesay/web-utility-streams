import LineSplitter from "./internal/LineSplitter.js";
import SplitLinesStream from "./SplitLinesStream.js";
import TransformPipelineStream from "./TransformPipelineStream.js";

export interface NewlineDelimitedJsonDecoderStreamOptions {
  /**
   * Called for each line that is not valid JSON. You can skip the line (by
   * doing nothing), enqueue a replacement value, or error the stream. If
   * `onError` throws, the stream is errored.
   *
   * Defaults to erroring the stream with the JSON parse error.
   */
  onError?: (
    invalidLine: string,
    error: Error,
    controller: TransformStreamDefaultController<unknown>,
  ) => void;
}

let lastError: Error | undefined;
function tryParse(chunk: string) {
  try {
    return JSON.parse(chunk);
  } catch (ex: any) {
    lastError = ex;
    return undefined;
  }
}
function handleError(
  onError: NewlineDelimitedJsonDecoderStreamOptions["onError"],
  line: string,
  controller: TransformStreamDefaultController<unknown>,
) {
  const err = lastError;
  lastError = undefined;
  // Ignore blank (or whitespace only) lines. We only check for
  // these after JSON.parse fails, as they're rare.
  if (line.trim()) {
    if (onError) onError(line, err!, controller);
    else controller.error(err!);
  }
}
/**
 * Decode a stream of text in the newline delimited JSON format. The text can
 * be split into chunks at any point, any line ending is supported, and blank
 * lines are ignored.
 */
export default class NewlineDelimitedJsonDecoderStream extends TransformStream<
  string,
  unknown
> {
  constructor({ onError }: NewlineDelimitedJsonDecoderStreamOptions = {}) {
    let splitter!: LineSplitter;
    super({
      start(controller) {
        splitter = new LineSplitter((text, start, end) => {
          const line = text.slice(start, end);
          const value = tryParse(line);
          if (lastError) handleError(onError, line, controller);
          else controller.enqueue(value);
        });
      },
      transform(chunk) {
        splitter.write(chunk);
      },
      flush() {
        splitter.end();
      },
    });
  }
}
