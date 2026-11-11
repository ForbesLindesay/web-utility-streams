import JsonTokenizer, { type JsonToken } from "./internal/JsonTokenizer.js";

export type { JsonToken };

/**
 * Split a stream of JSON text into tokens. The text can be split into chunks
 * at any point.
 *
 * Each token is checked to be valid (e.g. strings have valid escape sequences
 * and numbers are correctly formatted), but the overall structure of the
 * document is not checked, so e.g. `[1, }` produces tokens without an error.
 * Whitespace is skipped. If the text contains an invalid token, the stream is
 * errored with a `SyntaxError`.
 *
 * A number or `true`/`false`/`null` is only emitted once the character after
 * it has been received (or the input ends), as it could continue in the next
 * chunk.
 */
export default class JsonTokenDecoderStream extends TransformStream<
  string,
  JsonToken
> {
  constructor() {
    let tokenizer!: JsonTokenizer;
    super({
      start(controller) {
        tokenizer = new JsonTokenizer("JsonTokenDecoderStream", (token) =>
          controller.enqueue(token),
        );
      },
      transform(chunk) {
        tokenizer.write(chunk);
      },
      flush() {
        tokenizer.end();
      },
    });
  }
}
