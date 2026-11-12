import { test, expect } from "vitest";
import JsonDecoderStream from "./JsonDecoderStream.js";
import JsonEncoderStream from "./JsonEncoderStream.js";
import {
  readableStream,
  readableStreamResults,
  testTransformRobustness,
} from "./internal/StreamTestUtils.js";

async function decode(
  chunks: string[],
  options?: ConstructorParameters<typeof JsonDecoderStream>[0],
) {
  return await readableStreamResults(
    readableStream(chunks).pipeThrough(new JsonDecoderStream(options)),
  );
}

async function decodeError(
  chunks: string[],
  options?: ConstructorParameters<typeof JsonDecoderStream>[0],
): Promise<unknown> {
  const reader = readableStream(chunks)
    .pipeThrough(new JsonDecoderStream(options))
    .getReader();
  try {
    while (!(await reader.read()).done);
  } catch (ex) {
    return ex;
  }
  throw new Error("Expected the stream to error");
}

const ok = (value: unknown) => ({ ok: true, value });

test("JsonDecoderStream - decodes the elements of a root array", async () => {
  expect(await decode([`["a", "b"`, `, "c"]`])).toEqual([
    ok("a"),
    ok("b"),
    ok("c"),
  ]);
});

test("JsonDecoderStream - decodes the elements of an array at a path", async () => {
  expect(
    await decode([`{"results": `, `["a", "b"`, `, "c"]`, `}`], {
      path: ["results"],
    }),
  ).toEqual([ok("a"), ok("b"), ok("c")]);
  expect(
    await decode(
      [
        `{"skipped": {"results": [0]}, "data": {"total": 2, "results": [1, 2]}, "after": [3]}`,
      ],
      { path: ["data", "results"] },
    ),
  ).toEqual([ok(1), ok(2)]);
});
test("JsonDecoderStream - ignores other paths", async () => {
  expect(
    await decode(
      [
        `{"results": `,
        `["a", "b"`,
        `, "c"]`,
        `,"other": 42,"other": ["x"],"other": {"x": 42}`,
        `}`,
      ],
      {
        path: ["results"],
      },
    ),
  ).toEqual([ok("a"), ok("b"), ok("c")]);
  expect(
    await decode(
      [
        `{"skipped": {"results": [0]}, "data": {"total": 2, "results": [1, 2]}, "after": [3]}`,
      ],
      { path: ["data", "results"] },
    ),
  ).toEqual([ok(1), ok(2)]);
});

test("JsonDecoderStream - supports all JSON values as elements", async () => {
  const values = [
    'string with "escapes" \\ \n é 👋',
    0,
    -1.5e3,
    1e-7,
    true,
    false,
    null,
    [],
    {},
    [1, [2, [3]], { a: [{ b: null }] }],
    { nested: { deeply: { value: ["x"] } }, "": "empty key" },
  ];
  expect(await decode([JSON.stringify(values, null, 2)])).toEqual(
    values.map(ok),
  );
});

test("JsonDecoderStream - handles the input being split at every possible point", async () => {
  const document = {
    meta: { count: 3, tags: ["a", "b"], ok: true, nothing: null },
    results: [
      { id: 1, name: 'one \\ "quoted"', score: -12.5e-1 },
      "two 👋",
      [false, null, 3e10],
    ],
  };
  const text = JSON.stringify(document, null, 2);
  // Enqueue all the chunks at once, rather than using `readableStream`, which
  // waits between chunks. The transform is still called once per chunk.
  const chunks = new ReadableStream<string>({
    start(controller) {
      for (const char of text.split("")) controller.enqueue(char);
      controller.close();
    },
  });
  expect(
    await readableStreamResults(
      chunks.pipeThrough(new JsonDecoderStream({ path: ["results"] })),
    ),
  ).toEqual(document.results.map(ok));
});

test("JsonDecoderStream - emits elements as soon as they are complete", async () => {
  const stream = new JsonDecoderStream();
  const writer = stream.writable.getWriter();
  const reader = stream.readable.getReader();
  void writer.write(`[{"a": 1}, `);
  expect(await reader.read()).toEqual({ done: false, value: { a: 1 } });
  // A number may continue in the next chunk, so it isn't emitted until we
  // see the character after it.
  void writer.write(`2`);
  void writer.write(`3, `);
  expect(await reader.read()).toEqual({ done: false, value: 23 });
  void writer.write(`"x"]`);
  expect(await reader.read()).toEqual({ done: false, value: "x" });
  void writer.close();
  expect(await reader.read()).toEqual({ done: true, value: undefined });
});

test("JsonDecoderStream - handles empty arrays", async () => {
  expect(await decode([`[]`])).toEqual([]);
  expect(
    await decode([` { "results" : [ ] } `], { path: ["results"] }),
  ).toEqual([]);
});

test("JsonDecoderStream - emits elements from every matching array if a key is repeated", async () => {
  expect(
    await decode([`{"results": [1], "results": [2]}`], { path: ["results"] }),
  ).toEqual([ok(1), ok(2)]);
});

test("JsonDecoderStream - a __proto__ key creates a property rather than changing the prototype", async () => {
  const [result] = await decode([`[{"__proto__": {"polluted": true}}]`]);
  const value = (result as { value: any }).value;
  expect(Object.getPrototypeOf(value)).toBe(Object.prototype);
  expect(value.polluted).toBeUndefined();
  expect(Object.keys(value)).toEqual(["__proto__"]);
  expect(value).toEqual(JSON.parse(`{"__proto__": {"polluted": true}}`));
});

test("JsonDecoderStream - errors if there is no array or object at the path", async () => {
  const atRoot = "JsonDecoderStream: Expected an array or object at the root";
  const atPath = (path: string[]) =>
    `JsonDecoderStream: Expected an array or object at path ${JSON.stringify(path)}`;
  expect(await decodeError([`"a"`])).toMatchObject({ message: atRoot });
  expect(await decodeError([`42`])).toMatchObject({ message: atRoot });
  expect(await decodeError([`null`])).toMatchObject({ message: atRoot });
  expect(await decodeError([`[1]`], { path: ["results"] })).toMatchObject({
    message: atPath(["results"]),
  });
  expect(
    await decodeError([`{"results": 1}`], { path: ["results"] }),
  ).toMatchObject({ message: atPath(["results"]) });
  expect(
    await decodeError([`{"results": "a"}`], { path: ["results"] }),
  ).toMatchObject({ message: atPath(["results"]) });
  expect(
    await decodeError([`{"data": [1]}`], { path: ["data", "results"] }),
  ).toMatchObject({ message: atPath(["data", "results"]) });
  expect(
    await decodeError([`{"other": [1]}`], { path: ["results"] }),
  ).toMatchObject({ message: atPath(["results"]) });
});

test("JsonDecoderStream - decodes the properties of a root object as [key, value] tuples", async () => {
  expect(
    await decode([`{"a": 1, "b"`, `: [2, {"c": null}], `, `"d": "x"}`]),
  ).toEqual([ok(["a", 1]), ok(["b", [2, { c: null }]]), ok(["d", "x"])]);
});

test("JsonDecoderStream - decodes the properties of an object at a path", async () => {
  expect(
    await decode(
      [
        `{"skipped": {"results": {"x": 0}}, "data": {"total": 2, "results": {"a": 1, "b": 2}}, "after": {"c": 3}}`,
      ],
      { path: ["data", "results"] },
    ),
  ).toEqual([ok(["a", 1]), ok(["b", 2])]);
});

test("JsonDecoderStream - handles empty objects", async () => {
  expect(await decode([`{}`])).toEqual([]);
  expect(await decode([`{"results": {}}`], { path: ["results"] })).toEqual([]);
});

test("JsonDecoderStream - emits a tuple for each property of an object, even if keys are repeated", async () => {
  expect(await decode([`{"a": 1, "a": 2}`])).toEqual([
    ok(["a", 1]),
    ok(["a", 2]),
  ]);
});

test("JsonDecoderStream - __proto__ is emitted as an ordinary key", async () => {
  const results = await decode([`{"__proto__": {"polluted": true}}`]);
  expect(results).toEqual([ok(["__proto__", { polluted: true }])]);
  const [[, value]] = results.map((r) => (r as { value: any }).value);
  expect(Object.getPrototypeOf(value)).toBe(Object.prototype);
});

test("JsonDecoderStream - emits object properties as soon as they are complete", async () => {
  const stream = new JsonDecoderStream();
  const writer = stream.writable.getWriter();
  const reader = stream.readable.getReader();
  void writer.write(`{"a": {"b": 1}, `);
  expect(await reader.read()).toEqual({ done: false, value: ["a", { b: 1 }] });
  void writer.write(`"c": "x"`);
  expect(await reader.read()).toEqual({ done: false, value: ["c", "x"] });
  void writer.write(`, "d": 4`);
  void writer.write(`2}`);
  expect(await reader.read()).toEqual({ done: false, value: ["d", 42] });
  void writer.close();
  expect(await reader.read()).toEqual({ done: true, value: undefined });
});

test("JsonDecoderStream - handles an object split at every possible point", async () => {
  const document = {
    meta: { count: 3 },
    results: {
      first: { id: 1, name: 'one \\ "quoted"' },
      "second \ud83d\udc4b": [false, null, 3e10],
      third: "x",
    },
  };
  const text = JSON.stringify(document, null, 2);
  const chunks = new ReadableStream<string>({
    start(controller) {
      for (const char of text.split("")) controller.enqueue(char);
      controller.close();
    },
  });
  expect(
    await readableStreamResults(
      chunks.pipeThrough(new JsonDecoderStream({ path: ["results"] })),
    ),
  ).toEqual(Object.entries(document.results).map(ok));
});

test("JsonDecoderStream - errors on invalid JSON", async () => {
  const invalidDocuments = [
    ``,
    `   `,
    `[`,
    `[1`,
    `[1,`,
    `["abc`,
    `[1,]`,
    `[,1]`,
    `[1 2]`,
    `[1]]`,
    `[1] [2]`,
    `[1] x`,
    `[}`,
    `[[1]`,
    `[{"a" 1}]`,
    `[{"a": 1,}]`,
    `[{1: 1}]`,
    `[{"a"}]`,
    `[{"a": 1 "b": 2}]`,
    `["a": 1]`,
    `[{"a": 1, "b"}]`,
    `[{"a": "b": 1}]`,
    `[01]`,
    `[1.]`,
    `[-]`,
    `[+1]`,
    `[.5]`,
    `[1e]`,
    `[tru]`,
    `[True]`,
    `[nulls]`,
    `[undefined]`,
    `[NaN]`,
    `['single quotes']`,
    `["bad escape \\x"]`,
    `["control \u0001 character"]`,
    `[1] // comment`,
  ];
  for (const document of invalidDocuments) {
    const error = await decodeError([document]);
    expect(error, document).toBeInstanceOf(SyntaxError);
  }
});

test("JsonDecoderStream - errors on invalid JSON outside the target array", async () => {
  expect(
    await decode([`{"results": [1, 2], `, `"other": [1,]}`], {
      path: ["results"],
    }),
  ).toEqual([ok(1), ok(2), { ok: false }]);
});

test("JsonDecoderStream - errors on problems with the structure of the document", async () => {
  const cases: [string, string, string[]?][] = [
    // Between elements
    [`[{"a": 1} {"b": 2}]`, "token { at position 10"],
    [`[[1]}`, "token } at position 4"],
    // Inside an element
    [`[{"a": 1]]`, "token ] at position 8"],
    // Outside the target
    [`{"results": [1], "x" 1}`, "number 1 at position 21", ["results"]],
    [`{"results": [1]} x`, "token x at position 17", ["results"]],
    [`{"results": [1]}]`, "token ] at position 16", ["results"]],
  ];
  for (const [text, message, path] of cases) {
    expect(await decodeError([text], { path }), text).toMatchObject({
      name: "SyntaxError",
      message: `JsonDecoderStream: Unexpected ${message}`,
    });
  }
});

test("JsonDecoderStream - error messages include the position", async () => {
  expect(await decodeError([`[1, `, `2, ]`])).toMatchObject({
    message: "JsonDecoderStream: Unexpected token ] at position 7",
  });
});

/**
 * Decode `chunks`, enqueuing them all at once, rather than using
 * `readableStream`, which waits between chunks.
 */
async function decodeChunks(
  chunks: string[],
  options?: ConstructorParameters<typeof JsonDecoderStream>[0],
) {
  const input = new ReadableStream<string>({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(chunk);
      controller.close();
    },
  });
  return await readableStreamResults(
    input.pipeThrough(new JsonDecoderStream(options)),
  );
}

const splitEvery = (text: string, size: number) =>
  Array.from({ length: Math.ceil(text.length / size) }, (_, i) =>
    text.slice(i * size, (i + 1) * size),
  );

test("JsonDecoderStream - handles strings with brackets, quotes and backslashes in elements, split at every point", async () => {
  const results = [
    { a: "}]", b: "{[", c: 'quote " here', d: "ends with \\", e: '\\"' },
    ["\\", "]", "\\\\", '"]}', { "}": "{" }],
    { nested: [{ deep: ["x\\"] }] },
  ];
  const text = JSON.stringify({ skipped: results, results });
  for (const size of [1, 2, 3, 4, 7]) {
    expect(
      await decodeChunks(splitEvery(text, size), { path: ["results"] }),
    ).toEqual(results.map(ok));
  }
});

test("JsonDecoderStream - errors inside elements report the position of the problem", async () => {
  const cases = [
    // Inside an element
    [`[1, {"a": [1, 2 x]}]`, "token x at position 16"],
    [`[{"a": "\\x"}]`, "string at position 7"],
    // Inside a nested element
    [`[[{"a": 1}, {"b": 2,}]]`, "token } at position 20"],
  ];
  for (const [text, message] of cases) {
    for (const size of [1, 5, text!.length]) {
      expect(await decodeChunks(splitEvery(text!, size)), text).toEqual([
        ...(text!.startsWith("[1,") ? [ok(1)] : []),
        { ok: false },
      ]);
      expect(await decodeError(splitEvery(text!, size)), text).toMatchObject({
        message: `JsonDecoderStream: Unexpected ${message}`,
      });
    }
  }
  // Inside a value that is being skipped
  expect(
    await decodeError([`{"other": {"a": [1, 2,]}, "results": [1]}`], {
      path: ["results"],
    }),
  ).toMatchObject({
    message: "JsonDecoderStream: Unexpected token ] at position 22",
  });
});

test("JsonDecoderStream - errors if the input ends part way through an element", async () => {
  for (const document of [`[{"a": [1, 2`, `[{"a": "b`, `[{"a": "b\\`, `[[[`]) {
    expect(await decodeError([document]), document).toMatchObject({
      message: "JsonDecoderStream: Unexpected end of JSON input",
    });
  }
});

test("JsonDecoderStream - handles elements larger than 1MB", async () => {
  const large = {
    text: "x".repeat(600_000),
    items: Array.from({ length: 30_000 }, (_, i) => ({ i, s: `"${i}"\\` })),
  };
  const text = JSON.stringify({ skipped: large, results: [1, large, 2] });
  expect(
    await decodeChunks(splitEvery(text, 16384), { path: ["results"] }),
  ).toEqual([ok(1), ok(large), ok(2)]);

  // Errors are still found, with the right position
  const invalid = text.replace('{"i":29000,', '{"i":29000 x,');
  expect(
    await decodeError(splitEvery(invalid, 16384), { path: ["results"] }),
  ).toMatchObject({
    message: `JsonDecoderStream: Unexpected token x at position ${invalid.indexOf(" x,") + 1}`,
  });
});

test("JsonDecoderStream - only emits elements that are valid", async () => {
  expect(await decode([`[1, {"a": [2, }]`])).toEqual([ok(1), { ok: false }]);
});

/**
 * Decode `chunks`, returning the emitted values and every value passed to
 * `onExtraData`
 */
async function decodeWithExtraData(chunks: string[], path?: string[]) {
  const extraData: unknown[] = [];
  const results = await readableStreamResults(
    readableStream(chunks).pipeThrough(
      new JsonDecoderStream({
        path,
        onExtraData: (data) => extraData.push(data),
      }),
    ),
  );
  return { results, extraData };
}

test("JsonDecoderStream - onExtraData is called with the rest of the document", async () => {
  const { results, extraData } = await decodeWithExtraData(
    [`{"total": 2, "results": [1, 2], "next": {"cursor": "abc"}}`],
    ["results"],
  );
  expect(results).toEqual([ok(1), ok(2)]);
  expect(extraData).toEqual([{ total: 2, next: { cursor: "abc" } }]);
});

test("JsonDecoderStream - onExtraData includes the key at the path, with a value of undefined", async () => {
  const {
    extraData: [data],
  } = await decodeWithExtraData(
    [`{"before": 1, "results": [], "after": 2}`],
    ["results"],
  );
  expect(Object.keys(data as object)).toEqual(["before", "results", "after"]);
  expect((data as Record<string, unknown>).results).toBeUndefined();
});

test("JsonDecoderStream - onExtraData works with nested paths", async () => {
  const { results, extraData } = await decodeWithExtraData(
    [
      `{"version": 1, "data": {"page": {"size": 10}, "results": [1], "more": false}, "other": {"results": [9]}}`,
    ],
    ["data", "results"],
  );
  expect(results).toEqual([ok(1)]);
  expect(extraData).toEqual([
    {
      version: 1,
      data: { page: { size: 10 }, results: undefined, more: false },
      // Data at other paths is included, even if it has the same keys
      other: { results: [9] },
    },
  ]);
});

test("JsonDecoderStream - onExtraData works when streaming an object", async () => {
  const { results, extraData } = await decodeWithExtraData(
    [`{"count": 1, "users": {"alice": {"age": 30}}}`],
    ["users"],
  );
  expect(results).toEqual([ok(["alice", { age: 30 }])]);
  expect(extraData).toEqual([{ count: 1, users: undefined }]);
});

test("JsonDecoderStream - onExtraData is called with undefined when streaming the root", async () => {
  for (const document of [`[1, 2]`, `{"a": 1}`]) {
    const { extraData } = await decodeWithExtraData([document]);
    expect(extraData).toEqual([undefined]);
  }
});

test("JsonDecoderStream - onExtraData is called once the whole document has been parsed", async () => {
  const calls: unknown[] = [];
  const stream = new JsonDecoderStream({
    path: ["results"],
    onExtraData: (data) => calls.push(data),
  });
  const writer = stream.writable.getWriter();
  const reader = stream.readable.getReader();
  void writer.write(`{"total": 2, "results": [1, `);
  expect(await reader.read()).toEqual({ done: false, value: 1 });
  void writer.write(`2], "next": "abc"`);
  expect(await reader.read()).toEqual({ done: false, value: 2 });
  // The root object hasn't been closed yet, so there may be more data
  expect(calls).toEqual([]);
  void writer.write(`}`);
  void writer.close();
  expect(await reader.read()).toEqual({ done: true, value: undefined });
  expect(calls).toEqual([{ total: 2, results: undefined, next: "abc" }]);
});

test("JsonDecoderStream - onExtraData creates __proto__ as an ordinary property", async () => {
  const {
    extraData: [data],
  } = await decodeWithExtraData(
    [`{"__proto__": {"polluted": true}, "results": []}`],
    ["results"],
  );
  expect(Object.getPrototypeOf(data)).toBe(Object.prototype);
  expect(Object.keys(data as object)).toEqual(["__proto__", "results"]);
  expect(({} as any).polluted).toBeUndefined();
});

test("JsonDecoderStream - onExtraData is not called if the document is invalid before the end", async () => {
  const { results, extraData } = await decodeWithExtraData(
    [`{"total": 2, "results": [1`, `, }`],
    ["results"],
  );
  expect(results).toEqual([ok(1), { ok: false }]);
  expect(extraData).toEqual([]);
});

test("JsonDecoderStream - errors the stream if onExtraData throws", async () => {
  const results = await readableStreamResults(
    readableStream([`{"results": [1], "a": 2}`]).pipeThrough(
      new JsonDecoderStream({
        path: ["results"],
        onExtraData: () => {
          throw new Error("onExtraData failed");
        },
      }),
    ),
  );
  // Elements are emitted before the end of the document is reached
  expect(results).toEqual([ok(1), { ok: false }]);
});

test("JsonDecoderStream - onExtraData round trips the extraData from JsonEncoderStream", async () => {
  const extraData = {
    version: 1,
    data: { page: { size: 10, tags: ["a", "b"] }, more: true },
  };
  for (const mode of ["array", "object"] as const) {
    const values: unknown[] =
      mode === "array"
        ? [1, { two: 2 }]
        : [
            ["one", 1],
            ["two", { two: 2 }],
          ];
    const received: unknown[] = [];
    const results = await readableStreamResults(
      readableStream(values)
        .pipeThrough(
          new JsonEncoderStream({
            mode,
            path: ["data", "results"],
            extraData,
          }),
        )
        .pipeThrough(
          new JsonDecoderStream({
            path: ["data", "results"],
            onExtraData: (data) => received.push(data),
          }),
        ),
    );
    expect(results).toEqual(values.map(ok));
    expect(received).toEqual([extraData]);
  }
});

testTransformRobustness({
  name: "JsonDecoderStream",
  create: () => new JsonDecoderStream(),
  validInput: [`[{"a"`, `: 1}, "b"`, `, 2]`],
  longInput: [
    `[{"a"`,
    `: 1}, "b", `,
    `2, 3, `,
    `4, 5, `,
    `6, 7, `,
    `8, 9`,
    `]`,
  ],
  invalidInput: `}`,
});

testTransformRobustness({
  name: "JsonDecoderStream (object)",
  create: () => new JsonDecoderStream(),
  validInput: [`{"a"`, `: 1, "b": "x"`, `, "c": 2}`],
  longInput: [
    `{"a"`,
    `: 1, "b": "x", `,
    `"c": 2, "d": 3, `,
    `"e": 4, "f": 5, `,
    `"g": 6}`,
  ],
  invalidInput: `]`,
});

test("JsonDecoderStream - matches JSON.parse for random documents, split into random chunks", async () => {
  let seed = 1;
  const random = () => {
    seed = (seed * 1103515245 + 12345) & 0x7fffffff;
    return seed / 0x7fffffff;
  };
  const pick = <T>(items: T[]) => items[Math.floor(random() * items.length)]!;
  const randomValue = (depth: number): unknown => {
    const r = random();
    if (depth > 3 || r < 0.4) {
      return pick<unknown>([
        0,
        -1.5e3,
        true,
        null,
        "",
        'quote " and \\ backslash \\\\',
        "\\",
        "brackets {[]} and , : inside",
        "unicode é 東京 👋",
        "\n\t\u0001",
      ]);
    }
    if (r < 0.7) {
      return Array.from({ length: Math.floor(random() * 4) }, () =>
        randomValue(depth + 1),
      );
    }
    return Object.fromEntries(
      Array.from({ length: Math.floor(random() * 4) }, (_, i) => [
        pick(["a", "results", "__proto__", 'k"\\', "{"]) + i,
        randomValue(depth + 1),
      ]),
    );
  };
  const randomChunks = (text: string) => {
    const chunks: string[] = [];
    for (let i = 0; i < text.length;) {
      const size = pick([1, 2, 7, 50, 1000]);
      chunks.push(text.slice(i, i + size));
      i += size;
    }
    return chunks;
  };

  for (let n = 0; n < 1000; n++) {
    const document = {
      skipped: randomValue(1),
      results: Array.from({ length: 1 + Math.floor(random() * 4) }, () =>
        randomValue(1),
      ),
    };
    let text = JSON.stringify(document, null, random() < 0.5 ? 2 : undefined);
    const isValid = random() < 0.5;
    if (!isValid) {
      // Delete or insert a character, which usually makes it invalid
      const i = Math.floor(random() * text.length);
      text =
        random() < 0.5
          ? text.slice(0, i) + text.slice(i + 1)
          : text.slice(0, i) +
            pick([",", "]", "}", '"', "\\", "x"]) +
            text.slice(i);
    }
    const results = await decodeChunks(randomChunks(text), {
      path: ["results"],
    });
    if (isValid) {
      expect(results, text).toEqual(document.results.map(ok));
    } else {
      let parsed: unknown;
      try {
        parsed = JSON.parse(text);
      } catch {
        expect(results.at(-1), text).toEqual({ ok: false });
        continue;
      }
      if (Array.isArray((parsed as { results?: unknown }).results)) {
        expect(results, text).toEqual(
          (parsed as { results: unknown[] }).results.map(ok),
        );
      }
    }
  }
});

test("errors if an element is longer than maxValueLength", async () => {
  const reader = new ReadableStream<string>({
    start(controller) {
      for (const chunk of ['[{"a":', '"xxxxxxxx', 'xxxxxxx"}]']) {
        controller.enqueue(chunk);
      }
      controller.close();
    },
  })
    .pipeThrough(new JsonDecoderStream({ maxValueLength: 10 }))
    .getReader();
  await expect(
    (async () => {
      while (!(await reader.read()).done);
    })(),
  ).rejects.toThrow(
    /JsonDecoderStream: Value at position 1 exceeds the configured maximum value length of 10/,
  );
});
