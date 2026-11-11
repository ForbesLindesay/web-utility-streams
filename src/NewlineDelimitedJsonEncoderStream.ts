import { DEFAULT_LINE_ENDING, type LineEnding } from "./LineEndings.js";

export interface NewlineDelimitedJsonEncoderStreamOptions {
  /**
   * The line ending written after each JSON value. Defaults to `"\n"`.
   */
  lineEnding?: LineEnding;
}

/**
 * Encode each chunk as JSON, followed by a line ending. Chunks that cannot be
 * represented as JSON (e.g. `undefined`, functions, symbols, `BigInt` values or
 * circular structures) will error the stream.
 */
export default class NewlineDelimitedJsonEncoderStream extends TransformStream<
  unknown,
  string
> {
  constructor({
    lineEnding = DEFAULT_LINE_ENDING,
  }: NewlineDelimitedJsonEncoderStreamOptions = {}) {
    super({
      transform(chunk, controller) {
        const json: string | undefined = JSON.stringify(chunk);
        if (json === undefined) {
          throw new TypeError(
            `Cannot encode a value of type ${typeof chunk} as newline delimited JSON`,
          );
        }
        controller.enqueue(json + lineEnding);
      },
    });
  }
}
