export interface JsonEncoderStreamOptions {
  /**
   * The path to the location in the output where the streaming data should appear
   */
  readonly path?: readonly string[];
  /**
   * Defaults to "array". To stream an object, set this to `"object"` and
   * ensure the input to the stream is tuples of `[string, value]`
   */
  readonly mode?: "array" | "object";
  /**
   * Other properties to include with the output - must not have any value
   * at the `path`.
   */
  readonly extraData?: unknown;
}

/**
 * Encode a stream of values as a single JSON document, where the values are
 * the elements of an array (or, with `mode: "object"`, the properties of an
 * object). This is the opposite of `JsonDecoderStream`.
 *
 * Values are encoded the same way `JSON.stringify` encodes them inside an
 * array or object: in "array" mode, values that can't be represented in JSON
 * (`undefined`, functions and symbols) are encoded as `null`, and in "object"
 * mode, properties with those values are left out. Values that `JSON.stringify`
 * throws for (e.g. `BigInt` values or circular structures) error the stream.
 *
 * By default, the array/object is the root of the document. To nest it inside
 * other objects, pass the keys that lead to it as `path`, and any other
 * properties to include as `extraData`. The other properties are output
 * before the streamed array/object, so they are available to the reader
 * straight away.
 *
 * The start of the document is output immediately, each value is output as a
 * separate chunk as soon as it is written, and the end of the document is
 * output when the input closes.
 *
 * Example:
 *
 * ReadableStream.from(["a", "b"]).pipeThrough(new JsonEncoderStream())
 * // => `[`, `"a"`, `,"b"`, `]`
 *
 * ReadableStream.from([["a", 1], ["b", 2]]).pipeThrough(new JsonEncoderStream({ mode: "object" }))
 * // => `{`, `"a":1`, `,"b":2`, `}`
 *
 * ReadableStream.from(["a", "b"]).pipeThrough(new JsonEncoderStream({ path: ["results"], extraData: { total: 2 } }))
 * // => `{"total":2,"results":[`, `"a"`, `,"b"`, `]}`
 */
export default class JsonEncoderStream extends TransformStream<
  unknown,
  string
> {
  constructor({
    path = [],
    mode = "array",
    extraData = undefined,
  }: JsonEncoderStreamOptions = {}) {
    const prefix = encodePath(path, extraData) + (mode === "array" ? "[" : "{");
    const suffix = (mode === "array" ? "]" : "}") + "}".repeat(path.length);
    let isFirst = true;

    super({
      start(controller) {
        controller.enqueue(prefix);
      },
      transform(chunk, controller) {
        let json: string;
        if (mode === "object") {
          if (
            !Array.isArray(chunk) ||
            chunk.length !== 2 ||
            typeof chunk[0] !== "string"
          ) {
            throw new TypeError(
              `JsonEncoderStream: In "object" mode, each chunk must be a [key, value] tuple`,
            );
          }
          const value: string | undefined = JSON.stringify(chunk[1]);
          // Like JSON.stringify, leave out properties that have no JSON value
          if (value === undefined) return;
          json = `${JSON.stringify(chunk[0])}:${value}`;
        } else {
          // Like JSON.stringify, use null for elements that have no JSON value
          json = JSON.stringify(chunk) ?? "null";
        }
        controller.enqueue(isFirst ? json : `,${json}`);
        isFirst = false;
      },
      flush(controller) {
        controller.enqueue(suffix);
      },
    });
  }
}

/**
 * Encode the start of the document, up to the point where the streamed
 * array/object begins, including any `extraData`.
 */
function encodePath(path: readonly string[], extraData: unknown): string {
  let result = "";
  let level = extraData;
  for (let i = 0; i < path.length; i++) {
    const key = path[i]!;
    if (level !== undefined && !isPlainObject(level)) {
      throw new TypeError(
        `JsonEncoderStream: extraData at ${describePath(path.slice(0, i))} must be a plain object`,
      );
    }
    result += "{";
    for (const [k, v] of Object.entries(level ?? {})) {
      if (k === key) continue;
      const json: string | undefined = JSON.stringify(v);
      // Like JSON.stringify, leave out properties that have no JSON value
      if (json === undefined) continue;
      result += `${JSON.stringify(k)}:${json},`;
    }
    result += `${JSON.stringify(key)}:`;
    level =
      level !== undefined && Object.hasOwn(level, key) ? level[key] : undefined;
  }
  if (level !== undefined) {
    throw new TypeError(
      path.length
        ? `JsonEncoderStream: extraData must not have a value at ${describePath(path)}`
        : `JsonEncoderStream: extraData can only be used with a path`,
    );
  }
  return result;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (typeof value !== "object" || value === null) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function describePath(path: readonly string[]) {
  return path.length ? `path ${JSON.stringify(path)}` : "the root";
}
