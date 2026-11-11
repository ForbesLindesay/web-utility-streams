import { createJsonTokenizer, type JsonToken } from "./JsonTokenizer.js";

export interface JsonDecoderStreamOptions {
  /**
   * The keys to get from the root object to the array or object to stream.
   * Defaults to streaming the root of the document.
   */
  readonly path?: readonly string[];
  /**
   * If supplied, this callback is called with all the data that's not at
   * the supplied `path`, once the whole document has been parsed. This is the
   * document with the value at `path` replaced by `undefined` (the key is
   * still present), so it can be passed back to `JsonEncoderStream` as
   * `extraData`. If `path` is empty, it is called with `undefined`.
   *
   * It is called as soon as the end of the document is reached, which may be
   * before all the emitted elements have been read from the stream. If the
   * document is invalid before then, it is not called. If it throws, the
   * stream is errored.
   *
   * Note that when this is supplied, all the data outside of `path` is kept
   * in memory until the end of the document, rather than being discarded.
   */
  readonly onExtraData?: (extraData: unknown) => void;
}

interface Container {
  readonly kind: "object" | "array";
  /**
   * How we handle a JSON object/array:
   *
   * - "path": an object on the way to the target array/object.
   * - "target": the array/object whose elements we emit. For an object, we
   *   emit each property as a `[key, value]` tuple.
   * - "build": an object/array inside one of the elements we are emitting, so
   *   we need to construct its value.
   * - "skip": anything else. We check it is valid JSON, but discard it.
   */
  readonly mode: "path" | "target" | "build" | "skip";
  /**
   * What we expect next inside this container
   */
  expects:
    | "key" // after "," in an object or "{"
    | ":" // after a key
    | "value" // after ":" in an object, or "," in an array or "["
    | ","; // after a value
  canEnd: boolean; // true after a value or immediately after open char
  /**
   * The most recent key, for objects
   */
  key: string | undefined;
  /**
   * The value being constructed, if `mode` is "build"
   */
  readonly value: Record<string, unknown> | unknown[] | undefined;
}

/**
 * Decode a JSON array or object from a stream of text, emitting each element
 * as soon as it has been parsed, without waiting for the rest of the document.
 * For an array, each element is emitted. For an object, each property is
 * emitted as a `[key, value]` tuple. The text can be split into chunks at any
 * point.
 *
 * By default, the root of the document is streamed. If the array/object is
 * nested inside other objects, pass the keys to get from the root to it as
 * `path`.
 *
 * The entire document is validated, but only the elements being emitted are
 * kept in memory, so other (potentially large) properties are discarded as
 * they are parsed. To receive those other properties instead, pass an
 * `onExtraData` callback. If the document is not valid JSON, the stream is errored as
 * soon as the problem is found. If there is no array/object at `path`, the
 * stream is errored once the whole document has been parsed. Because elements
 * are emitted as soon as they are parsed, some elements may be emitted before
 * an error is found later in the document.
 *
 * If the key at any point in `path` appears more than once in an object, the
 * elements from every match are emitted. Similarly, if a streamed object has
 * the same key more than once, a tuple is emitted for each of them (unlike
 * `JSON.parse`, which keeps only the last value).
 */
export default class JsonDecoderStream extends TransformStream<
  string,
  unknown
> {
  constructor({ path = [], onExtraData }: JsonDecoderStreamOptions = {}) {
    const tokenizer = createJsonTokenizer("JsonDecoderStream");
    const stack: Container[] = [];
    let finished = false;
    let foundTarget = false;
    let extraData: unknown;

    const unexpected = (token: JsonToken): never => {
      const description =
        typeof token.value === "string" && token.type !== "PUNCTUATION"
          ? "string"
          : typeof token.value === "number"
            ? `number ${token.value}`
            : `token ${token.value}`;
      throw new SyntaxError(
        `JsonDecoderStream: Unexpected ${description} at position ${token.position}`,
      );
    };

    /**
     * Check that a value can start here, and decide how to handle it.
     */
    const childMode = (token: JsonToken): Container["mode"] => {
      const parent = stack.at(-1);
      if (!parent) {
        if (!path.length) {
          foundTarget = true;
          return "target";
        }
        return "path";
      }
      if (parent.expects !== "value") return unexpected(token);

      switch (parent.mode) {
        case "path":
          if (parent.key !== path[stack.length - 1]) return "skip";

          if (stack.length === path.length) {
            foundTarget = true;
            return "target";
          }

          return "path";
        case "target":
        case "build":
          return "build";
        case "skip":
          return "skip";
      }
    };

    /**
     * A value (scalar, object or array) has been completely parsed.
     * `value` is only meaningful if the parent is being built or emitted.
     */
    const endValue = (
      value: unknown,
      controller: TransformStreamDefaultController<unknown>,
    ) => {
      const parent = stack.at(-1);
      if (!parent) {
        extraData = value;
        finished = true;
        return;
      }
      if (parent.mode === "target") {
        controller.enqueue(
          parent.kind === "object" ? [parent.key, value] : value,
        );
      } else if (parent.mode === "build" || onExtraData) {
        if (Array.isArray(parent.value)) {
          parent.value.push(value);
        } else {
          // Use defineProperty rather than assignment so that a key of
          // "__proto__" creates a property (like JSON.parse does) rather
          // than changing the object's prototype.
          Object.defineProperty(parent.value, parent.key!, {
            value,
            writable: true,
            enumerable: true,
            configurable: true,
          });
        }
      }
      parent.expects = ",";
      parent.canEnd = true;
    };

    const handleToken = (
      token: JsonToken,
      controller: TransformStreamDefaultController<unknown>,
    ) => {
      if (finished) return unexpected(token);

      const top = stack.at(-1);
      if (token.type === "KEY") {
        if (top?.expects !== "key") return unexpected(token);
        top.key = token.value;
        top.expects = ":";
        top.canEnd = false;
        return;
      }
      if (token.type === "LITERAL") {
        // A string, number, boolean or null value
        if (top && top.expects !== "value") return unexpected(token);
        endValue(token.value, controller);
        return;
      }
      switch (token.value) {
        case "{":
        case "[": {
          const kind = token.value === "{" ? "object" : "array";
          const mode = childMode(token);
          stack.push({
            kind,
            mode,
            expects: kind === "object" ? "key" : "value",
            canEnd: true,
            key: undefined,
            value:
              mode === "build" || (onExtraData && mode !== "target")
                ? kind === "object"
                  ? {}
                  : []
                : undefined,
          });
          return;
        }
        case "}":
        case "]": {
          const kind = token.value === "}" ? "object" : "array";
          if (top?.kind !== kind || !top.canEnd) return unexpected(token);
          stack.pop();
          endValue(top.value, controller);
          return;
        }
        case ":":
          if (top?.expects !== ":") return unexpected(token);
          top.expects = "value";
          return;
        case ",":
          if (top?.expects !== ",") return unexpected(token);
          top.expects = top.kind === "object" ? "key" : "value";
          top.canEnd = false;
          return;
      }
    };

    super({
      transform(chunk, controller) {
        tokenizer.write(chunk, (token) => handleToken(token, controller));
      },
      flush(controller) {
        tokenizer.end((token) => handleToken(token, controller));
        if (!finished) {
          throw new SyntaxError(
            "JsonDecoderStream: Unexpected end of JSON input",
          );
        }
        if (!foundTarget) {
          throw new TypeError(
            `JsonDecoderStream: Expected an array or object at ${path.length === 0 ? "the root" : `path ${JSON.stringify(path)}`}`,
          );
        }
        if (onExtraData) onExtraData(extraData);
      },
    });
  }
}
