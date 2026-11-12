// Equivalent to the utilities in `node:stream/consumers`, but for web streams
// https://nodejs.org/api/webstreams.html#utility-consumers

const encoder = new TextEncoder();

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
  const chunks = stream[Symbol.asyncIterator]();
  const firstChunk = await chunks.next();
  if (firstChunk.done) return "";

  try {
    if (typeof firstChunk.value === "string")
      return await textFromStreamOfStrings(chunks, firstChunk.value);
    return await textFromStreamOfBytes(chunks, toUint8Array(firstChunk.value));
  } catch (ex) {
    await chunks.return?.();
    throw ex;
  }
}

async function textFromStreamOfStrings(
  stream: AsyncIterator<ConsumableChunk>,
  firstChunk: string,
): Promise<string> {
  let decoder;
  let result = firstChunk;
  let next = await stream.next();
  while (!next.done) {
    if (typeof next.value === "string") {
      result += next.value;
    } else {
      decoder ??= new TextDecoder();
      result += decoder.decode(toUint8Array(next.value), { stream: true });
    }
    next = await stream.next();
  }
  if (!decoder) return result;
  return result + decoder.decode();
}

async function textFromStreamOfBytes(
  stream: AsyncIterator<ConsumableChunk>,
  firstChunk: Uint8Array,
): Promise<string> {
  const result = [firstChunk];
  let length = firstChunk.byteLength;
  let next = await stream.next();
  while (!next.done) {
    const bytes =
      typeof next.value === "string"
        ? encoder.encode(next.value)
        : toUint8Array(next.value);
    result.push(bytes);
    length += bytes.byteLength;
    next = await stream.next();
  }
  return new TextDecoder().decode(concatChunks(length, result));
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
  const chunks: Uint8Array[] = [];
  let length = 0;
  for await (const chunk of stream) {
    const bytes =
      typeof chunk === "string" ? encoder.encode(chunk) : toUint8Array(chunk);
    chunks.push(bytes);
    length += bytes.byteLength;
  }
  return concatChunks(length, chunks);
}
function concatChunks(length: number, chunks: readonly Uint8Array[]) {
  const result = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    result.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return result;
}

function toUint8Array(chunk: ArrayBuffer | ArrayBufferView): Uint8Array {
  if (chunk instanceof Uint8Array) return chunk;
  if (chunk instanceof ArrayBuffer) return new Uint8Array(chunk);
  if (ArrayBuffer.isView(chunk))
    return new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.byteLength);
  throw new TypeError(
    `Expected each chunk to be a string, ArrayBuffer or ArrayBufferView, but got ${
      chunk === null ? "null" : typeof chunk
    }`,
  );
}
