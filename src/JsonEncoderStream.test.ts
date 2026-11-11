import { test, expect } from "vitest";
import JsonEncoderStream, {
  type JsonEncoderStreamOptions,
} from "./JsonEncoderStream.js";
import JsonDecoderStream from "./JsonDecoderStream.js";
import {
  readableStream,
  readableStreamResults,
  testTransformRobustness,
} from "./internal/StreamTestUtils.js";

/**
 * Enqueue all values at once, so tests are fast. The transform is still
 * called once per value.
 */
function values(chunks: unknown[]): ReadableStream<unknown> {
  return new ReadableStream({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(chunk);
      controller.close();
    },
  });
}

async function encodeChunks(
  chunks: unknown[],
  options?: JsonEncoderStreamOptions,
) {
  const results = await readableStreamResults(
    values(chunks).pipeThrough(new JsonEncoderStream(options)),
  );
  return results.map((result) => (result.ok ? result.value : "<error>"));
}

async function encode(chunks: unknown[], options?: JsonEncoderStreamOptions) {
  return (await encodeChunks(chunks, options)).join("");
}

test("JsonEncoderStream - encodes values as the elements of an array", async () => {
  expect(await encodeChunks(["a", 1, { b: [true, null] }])).toEqual([
    "[",
    `"a"`,
    ",1",
    `,{"b":[true,null]}`,
    "]",
  ]);
});

test("JsonEncoderStream - encodes an empty stream as an empty array or object", async () => {
  expect(await encode([])).toBe("[]");
  expect(await encode([], { mode: "object" })).toBe("{}");
  expect(await encode([], { path: ["a", "b"] })).toBe(`{"a":{"b":[]}}`);
});

test("JsonEncoderStream - encodes [key, value] tuples as the properties of an object", async () => {
  expect(
    await encodeChunks(
      [
        ["a", 1],
        ["b", { c: "x" }],
      ],
      { mode: "object" },
    ),
  ).toEqual(["{", `"a":1`, `,"b":{"c":"x"}`, "}"]);
});

test("JsonEncoderStream - outputs the start of the document before any values are written", async () => {
  const stream = new JsonEncoderStream({
    path: ["results"],
    extraData: { total: 2 },
  });
  const reader = stream.readable.getReader();
  expect(await reader.read()).toEqual({
    done: false,
    value: `{"total":2,"results":[`,
  });
  const writer = stream.writable.getWriter();
  void writer.write("a");
  expect(await reader.read()).toEqual({ done: false, value: `"a"` });
  void writer.close();
  expect(await reader.read()).toEqual({ done: false, value: `]}` });
  expect(await reader.read()).toEqual({ done: true, value: undefined });
});

test("JsonEncoderStream - nests the array/object at the path, after the extraData", async () => {
  expect(
    await encode([1, 2], {
      path: ["data", "results"],
      extraData: { version: 1, data: { total: 2, page: { size: 10 } } },
    }),
  ).toBe(`{"version":1,"data":{"total":2,"page":{"size":10},"results":[1,2]}}`);
  expect(
    await encode([["a", 1]], {
      mode: "object",
      path: ["users"],
      extraData: { count: 1 },
    }),
  ).toBe(`{"count":1,"users":{"a":1}}`);
});

test("JsonEncoderStream - encodes extraData like JSON.stringify", async () => {
  const extraData = {
    'key with "quotes"': "value\n",
    skipped: undefined,
    fn: () => {},
    date: new Date(0),
    nested: { list: [undefined, 1] },
  };
  const text = await encode([1], { path: ["results"], extraData });
  expect(JSON.parse(text)).toEqual(
    JSON.parse(JSON.stringify({ ...extraData, results: [1] })),
  );
});

test("JsonEncoderStream - encodes values like JSON.stringify", async () => {
  // In arrays, values with no JSON representation become null
  expect(await encode([undefined, () => {}, Symbol("x"), new Date(0)])).toBe(
    `[null,null,null,"1970-01-01T00:00:00.000Z"]`,
  );
  // In objects, properties with no JSON representation are left out
  expect(
    await encode(
      [
        ["a", undefined],
        ["b", 1],
        ["c", () => {}],
        ["d", 2],
      ],
      { mode: "object" },
    ),
  ).toBe(`{"b":1,"d":2}`);
  // Including at the start
  expect(
    await encode(
      [
        ["a", undefined],
        ["b", 1],
      ],
      { mode: "object" },
    ),
  ).toBe(`{"b":1}`);
});

test("JsonEncoderStream - the output can be parsed with JSON.parse", async () => {
  const elements = [
    'string with "quotes", \\ backslashes \n and 👋',
    0,
    -1.5e-7,
    true,
    null,
    [],
    {},
    [1, [2, { a: [3] }]],
    { "": "empty key", __proto__: null },
  ];
  expect(JSON.parse(await encode(elements))).toEqual(
    JSON.parse(JSON.stringify(elements)),
  );
});

test("JsonEncoderStream - round trips with JsonDecoderStream", async () => {
  const elements = [{ id: 1, tags: ["a"] }, "two", [3], null];
  const entries: [string, unknown][] = [
    ["first", 1],
    ["second", { nested: [true] }],
    ["__proto__", "ordinary key"],
  ];
  for (const options of [
    {},
    { path: ["results"], extraData: { total: 4 } },
    { path: ["a", "b", "c"] },
  ]) {
    expect(
      await readableStreamResults(
        values(elements)
          .pipeThrough(new JsonEncoderStream(options))
          .pipeThrough(new JsonDecoderStream({ path: options.path })),
      ),
    ).toEqual(elements.map((value) => ({ ok: true, value })));
    expect(
      await readableStreamResults(
        values(entries)
          .pipeThrough(new JsonEncoderStream({ ...options, mode: "object" }))
          .pipeThrough(new JsonDecoderStream({ path: options.path })),
      ),
    ).toEqual(entries.map((value) => ({ ok: true, value })));
  }
});

test("JsonEncoderStream - errors the stream for values that can't be encoded", async () => {
  const circular: any = {};
  circular.self = circular;
  for (const invalid of [10n, circular]) {
    expect(await encodeChunks([1, invalid, 2])).toEqual(["[", "1", "<error>"]);
    expect(
      await encodeChunks(
        [
          ["a", 1],
          ["b", invalid],
        ],
        { mode: "object" },
      ),
    ).toEqual(["{", `"a":1`, "<error>"]);
  }
});

test("JsonEncoderStream - in object mode, errors the stream for chunks that aren't [key, value] tuples", async () => {
  for (const invalid of ["a", { a: 1 }, ["a"], ["a", 1, 2], [1, "a"], null]) {
    expect(
      await encodeChunks([["ok", 1], invalid], { mode: "object" }),
    ).toEqual(["{", `"ok":1`, "<error>"]);
  }
});

test("JsonEncoderStream - throws for invalid options", () => {
  expect(() => new JsonEncoderStream({ extraData: { a: 1 } })).toThrow(
    "JsonEncoderStream: extraData can only be used with a path",
  );
  expect(
    () =>
      new JsonEncoderStream({
        path: ["results"],
        extraData: { results: [1] },
      }),
  ).toThrow(
    'JsonEncoderStream: extraData must not have a value at path ["results"]',
  );
  expect(
    () =>
      new JsonEncoderStream({
        path: ["data", "results"],
        extraData: { data: { results: {} } },
      }),
  ).toThrow(
    'JsonEncoderStream: extraData must not have a value at path ["data","results"]',
  );
  expect(
    () => new JsonEncoderStream({ path: ["results"], extraData: [1] }),
  ).toThrow("JsonEncoderStream: extraData at the root must be a plain object");
  expect(
    () =>
      new JsonEncoderStream({
        path: ["data", "results"],
        extraData: { data: "x" },
      }),
  ).toThrow(
    'JsonEncoderStream: extraData at path ["data"] must be a plain object',
  );
});

test("JsonEncoderStream - a value of undefined at the path in extraData is allowed", async () => {
  expect(
    await encode([1], { path: ["results"], extraData: { results: undefined } }),
  ).toBe(`{"results":[1]}`);
});

test("JsonEncoderStream - works with values delivered over time", async () => {
  expect(
    (
      await readableStreamResults(
        readableStream([1, 2, 3]).pipeThrough(new JsonEncoderStream()),
      )
    )
      .map((result) => (result.ok ? result.value : "<error>"))
      .join(""),
  ).toBe("[1,2,3]");
});

testTransformRobustness({
  name: "JsonEncoderStream",
  create: () => new JsonEncoderStream(),
  validInput: [1, "two", { three: 3 }],
  invalidInput: 10n,
});

testTransformRobustness({
  name: "JsonEncoderStream (object)",
  create: () => new JsonEncoderStream({ mode: "object" }),
  validInput: [
    ["a", 1],
    ["b", 2],
    ["c", 3],
  ],
  invalidInput: "not a tuple",
});
