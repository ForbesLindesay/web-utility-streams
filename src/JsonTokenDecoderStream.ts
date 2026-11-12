import JsonTokenizer from "./internal/JsonTokenizer.js";

export interface JsonTokenDecoderStreamOptions {
  readonly maxValueLength?: number;
}

/**
 * A token in a JSON document. `position` is the index of the token's first
 * character in the input text.
 *
 * A string is a `KEY` token if it is directly inside an object, immediately
 * after the `{` or a `,`. All other strings are `LITERAL` tokens.
 */
export type JsonToken =
  | {
      readonly type: "PUNCTUATION";
      readonly value: "{" | "}" | "[" | "]" | ":" | ",";
      readonly position: number;
    }
  | {
      readonly type: "KEY";
      readonly value: string;
      readonly position: number;
    }
  | {
      readonly type: "LITERAL";
      readonly value: string | number | boolean | null;
      readonly position: number;
    };

/**
 * Split a stream of JSON text into tokens. The text can be split into chunks
 * at any point.
 *
 * The text must be a single JSON value, such as an object or array. Each
 * token is checked to be valid (e.g. strings have valid escape sequences and
 * numbers are correctly formatted), and to be in a valid place in the
 * document (e.g. brackets match, and keys are followed by a `:`). Whitespace
 * is skipped. If there is a problem, the stream is errored with a
 * `SyntaxError` as soon as it is found, after emitting the tokens before it.
 * If the input ends before the value is complete, the stream is errored with
 * a `SyntaxError` when it closes.
 *
 * A number or `true`/`false`/`null` is only emitted once the character after
 * it has been received (or the input ends), as it could continue in the next
 * chunk.
 */
export default class JsonTokenDecoderStream extends TransformStream<
  string,
  JsonToken
> {
  constructor({
    maxValueLength = 100 * 2 ** 20,
  }: JsonTokenDecoderStreamOptions = {}) {
    let tokenizer!: JsonTokenizer;
    super({
      start(controller) {
        const onPunctuation = (code: number, position: number) => {
          controller.enqueue({
            type: "PUNCTUATION",
            value: String.fromCharCode(code) as
              "{" | "}" | "[" | "]" | ":" | ",",
            position,
          });
          return false;
        };
        tokenizer = new JsonTokenizer(
          "JsonTokenDecoderStream",
          maxValueLength,
          {
            onLiteral(value, position) {
              controller.enqueue({
                type: "LITERAL",
                // We never return `true` from onOpen so we never capture
                value: value as string | number | boolean | null,
                position,
              });
            },
            onKey(value, position) {
              controller.enqueue({ type: "KEY", value, position });
            },
            onOpen: onPunctuation,
            onClose: onPunctuation,
            onPunctuation,
          },
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
