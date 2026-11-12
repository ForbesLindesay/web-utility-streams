import { test, expect } from "vitest";
import {
  bytesFromStream,
  jsonFromStream,
  textFromStream,
} from "./consumers.js";
import { readableStream, trackedSource } from "./internal/StreamTestUtils.js";

const encode = (value: string) => new TextEncoder().encode(value);

test("textFromStream - joins string chunks", async () => {
  expect(await textFromStream(readableStream(["hello ", "world"]))).toBe(
    "hello world",
  );
});

test("textFromStream - decodes bytes as UTF-8, including characters split across chunks", async () => {
  const encoded = encode("héllo 👋");
  // Split in the middle of the multi-byte "👋" character
  const splitAt = encoded.length - 2;
  expect(
    await textFromStream(
      readableStream([encoded.slice(0, splitAt), encoded.slice(splitAt)]),
    ),
  ).toBe("héllo 👋");
});

test("textFromStream - supports ArrayBuffer and other ArrayBufferView chunks", async () => {
  const buffer = encode("abcdef").buffer;
  expect(
    await textFromStream(
      readableStream<ArrayBuffer | ArrayBufferView>([
        buffer.slice(0, 2),
        new DataView(buffer, 2, 2),
        new Uint8Array(buffer, 4, 2),
      ]),
    ),
  ).toBe("abcdef");
});

test("textFromStream - supports a mix of strings and bytes", async () => {
  expect(await textFromStream(readableStream(["a", encode("b"), "c"]))).toBe(
    "abc",
  );
});

test("textFromStream - removes a leading byte order mark from bytes", async () => {
  expect(
    await textFromStream(
      readableStream([new Uint8Array([0xef, 0xbb, 0xbf]), "x"]),
    ),
  ).toBe("x");
});

test("textFromStream - returns an empty string for an empty stream", async () => {
  expect(await textFromStream(readableStream<string>([]))).toBe("");
});

test("textFromStream - supports async iterables", async () => {
  async function* generate() {
    yield "a";
    yield encode("b");
  }
  expect(await textFromStream(generate())).toBe("ab");
});

test("jsonFromStream - parses the contents of the stream as JSON", async () => {
  expect(
    await jsonFromStream(
      readableStream(['{"hello":', encode(' "world", "n": [1, 2] }')]),
    ),
  ).toEqual({ hello: "world", n: [1, 2] });
});

test("jsonFromStream - rejects if the contents are not valid JSON", async () => {
  await expect(jsonFromStream(readableStream(["{not json"]))).rejects.toThrow(
    SyntaxError,
  );
});

test("bytesFromStream - concatenates all chunks into a single Uint8Array", async () => {
  const result = await bytesFromStream(
    readableStream<string | ArrayBuffer | ArrayBufferView>([
      new Uint8Array([1, 2]),
      new Uint8Array([3]).buffer,
      new DataView(new Uint8Array([0, 4, 5, 0]).buffer, 1, 2),
      "é",
    ]),
  );
  expect(result).toBeInstanceOf(Uint8Array);
  expect([...result]).toEqual([1, 2, 3, 4, 5, 0xc3, 0xa9]);
});

test("bytesFromStream - returns an empty Uint8Array for an empty stream", async () => {
  const result = await bytesFromStream(readableStream<Uint8Array>([]));
  expect(result).toBeInstanceOf(Uint8Array);
  expect(result.length).toBe(0);
});

for (const [name, consume] of [
  ["textFromStream", textFromStream],
  ["jsonFromStream", jsonFromStream],
  ["bytesFromStream", bytesFromStream],
] as const) {
  test(`${name} - rejects if the stream errors`, async () => {
    await expect(
      consume(readableStream(['"a', 'b"'], { err: true })),
    ).rejects.toThrow("Example error");
  });

  test(`${name} - rejects and cancels the stream if a chunk has an invalid type`, async () => {
    const source = trackedSource<unknown>(['"a', 42, 'b"']);
    await expect(
      consume(source.stream as ReadableStream<string>),
    ).rejects.toThrow(TypeError);
    expect(source.state.cancelled).toBe(true);
  });
}
