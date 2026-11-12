import JsonTokenizer, {
  OPEN_ARRAY,
  OPEN_OBJECT,
} from "./internal/JsonTokenizer.js";

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

  readonly maxValueLength?: number;
}

interface Container {
  readonly kind: typeof OPEN_ARRAY | typeof OPEN_OBJECT;
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
 * `onExtraData` callback. If the document is not valid JSON, the stream is
 * errored when the problem is found. For speed, each element (and each
 * object/array that is being discarded) is parsed in one go when its end is
 * reached, so a problem inside one is found when its end is reached, or after
 * 1MB of it, for larger values. If there is no array/object at `path`, the
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
  constructor({
    path = [],
    maxValueLength = 100 * 10 ** 20,
    onExtraData,
  }: JsonDecoderStreamOptions = {}) {
    // The tokenizer checks the structure of the document, so we only need to
    // track where we are in it, and build the values we need
    const stack: Container[] = [];
    let foundTarget = false;
    let extraData: unknown;
    let controller!: TransformStreamDefaultController<unknown>;

    /**
     * Decide how to handle an object/array that is starting
     */
    const childMode = (): Container["mode"] => {
      const parent = stack.at(-1);
      if (!parent) {
        if (!path.length) {
          foundTarget = true;
          return "target";
        }
        return "path";
      }

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
    const endValue = (value: unknown) => {
      const parent = stack.at(-1);
      if (!parent) {
        extraData = value;
        return;
      }
      if (parent.mode === "target") {
        controller.enqueue(
          parent.kind === OPEN_OBJECT ? [parent.key, value] : value,
        );
      } else if (parent.mode === "build" || onExtraData) {
        if (parent.kind === OPEN_ARRAY) {
          (parent.value as unknown[]).push(value);
        } else if (parent.key === "__proto__") {
          // Use defineProperty rather than assignment so that a key of
          // "__proto__" creates a property (like JSON.parse does) rather
          // than changing the object's prototype. This is much slower than
          // assignment, so we only use it when we need to.
          Object.defineProperty(parent.value, parent.key, {
            value,
            writable: true,
            enumerable: true,
            configurable: true,
          });
        } else {
          (parent.value as Record<string, unknown>)[parent.key!] = value;
        }
      }
    };

    const tokenizer = new JsonTokenizer("JsonDecoderStream", maxValueLength, {
      onKey: (key) => {
        // Keys are always directly inside an object
        stack[stack.length - 1]!.key = key;
      },
      onLiteral: endValue,
      onOpen: (kind) => {
        const mode = childMode();
        // It's much faster to parse the text of a whole value with
        // JSON.parse than to build (or just validate) it from the tokens.
        // Values that are too large to capture are still handled as tokens
        // below.
        if (mode === "build" || mode === "skip") {
          return true;
        }
        stack.push({
          kind,
          mode,
          key: undefined,
          value:
            onExtraData && mode !== "target"
              ? kind === OPEN_OBJECT
                ? {}
                : []
              : undefined,
        });
        return false;
      },
      onClose: () => {
        // The tokenizer has checked this matches the open bracket
        endValue(stack.pop()!.value);
      },
      onPunctuation: () => {
        // ignore all other punctuation
      },
    });

    super({
      start(c) {
        controller = c;
      },
      transform(chunk) {
        tokenizer.write(chunk);
      },
      flush() {
        // Throws if the document is incomplete
        tokenizer.end();
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
