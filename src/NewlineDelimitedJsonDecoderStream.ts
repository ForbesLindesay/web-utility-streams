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

/**
 * Decode a stream of text in the newline delimited JSON format. The text can
 * be split into chunks at any point, any line ending is supported, and blank
 * lines are ignored.
 */
export default class NewlineDelimitedJsonDecoderStream extends TransformPipelineStream<
  string,
  unknown
> {
  constructor({
    onError = (_, error, controller) => controller.error(error),
  }: NewlineDelimitedJsonDecoderStreamOptions = {}) {
    super((input) =>
      input.pipeThrough(new SplitLinesStream()).pipeThrough(
        new TransformStream<string, unknown>({
          transform(chunk, controller) {
            // Ignore blank (or whitespace only) lines
            if (!chunk.trim()) return;
            const parsed = jsonParse(chunk);
            if (parsed.ok) {
              controller.enqueue(parsed.value);
            } else {
              onError(chunk, parsed.err, controller);
            }
          },
        }),
      ),
    );
  }
}

function jsonParse(
  text: string,
): { ok: true; value: unknown } | { ok: false; err: Error } {
  try {
    return { ok: true, value: JSON.parse(text) };
  } catch (err: any) {
    return { ok: false, err };
  }
}
