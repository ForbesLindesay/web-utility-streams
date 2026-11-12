// Equivalent to the utilities in `node:stream/consumers`, but for web streams
// https://nodejs.org/api/webstreams.html#utility-consumers

/**
 * A chunk that can be consumed. Strings are treated as text, and everything
 * else is treated as bytes (UTF-8 encoded text, when consuming as text).
 */
export type ConsumableChunk = string | ArrayBuffer | ArrayBufferView;

/**
 * A stream that can be consumed. Either a web `ReadableStream`, or any async
 * iterable (e.g. a Node.js `Readable` stream).
 */
export type ConsumableStream =
  ReadableStream<ConsumableChunk> | AsyncIterable<ConsumableChunk>;

/**
 * Read the entire stream, and decode its contents as UTF-8 text. A leading
 * byte order mark is removed. Rejects if the stream errors.
 */
export async function textFromStream(
  stream: ConsumableStream,
): Promise<string> {
  const decoder = new TextDecoder();
  let result = "";
  for await (const chunk of stream) {
    if (typeof chunk === "string") {
      result += chunk;
    } else {
      result += decoder.decode(toUint8Array(chunk), { stream: true });
    }
  }
  return result + decoder.decode();
}

/**
 * Read the entire stream, decode its contents as UTF-8 text, and parse it as
 * JSON. Rejects if the stream errors or the text is not valid JSON.
 */
export async function jsonFromStream(
  stream: ConsumableStream,
): Promise<unknown> {
  return JSON.parse(await textFromStream(stream));
}

/**
 * Read the entire stream into a single `Uint8Array`. String chunks are UTF-8
 * encoded. Rejects if the stream errors.
 */
export async function bytesFromStream(
  stream: ConsumableStream,
): Promise<Uint8Array> {
  const encoder = new TextEncoder();
  const chunks: Uint8Array[] = [];
  let length = 0;
  for await (const chunk of stream) {
    const bytes =
      typeof chunk === "string" ? encoder.encode(chunk) : toUint8Array(chunk);
    chunks.push(bytes);
    length += bytes.byteLength;
  }
  const result = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    result.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return result;
}

function toUint8Array(chunk: ArrayBuffer | ArrayBufferView): Uint8Array {
  if (chunk instanceof ArrayBuffer) {
    return new Uint8Array(chunk);
  }
  if (ArrayBuffer.isView(chunk)) {
    return new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.byteLength);
  }
  throw new TypeError(
    `Expected each chunk to be a string, ArrayBuffer or ArrayBufferView, but got ${
      chunk === null ? "null" : typeof chunk
    }`,
  );
}
