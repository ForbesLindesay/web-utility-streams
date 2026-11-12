import { test, expect } from "vitest";
import JsonTokenDecoderStream, {
  type JsonToken,
} from "./JsonTokenDecoderStream.js";
import {
  readableStreamResults,
  testTransformRobustness,
} from "./internal/StreamTestUtils.js";

/**
 * Enqueue all chunks at once, so tests with many chunks are fast. The
 * transform is still called once per chunk.
 */
function chunks(values: string[]): ReadableStream<string> {
  return new ReadableStream({
    start(controller) {
      for (const value of values) controller.enqueue(value);
      controller.close();
    },
  });
}

async function tokenize(values: string[]) {
  const results = await readableStreamResults(
    chunks(values).pipeThrough(new JsonTokenDecoderStream()),
  );
  const tokens: JsonToken[] = [];
  for (const result of results) {
    if (!result.ok) throw new Error("Stream errored");
    tokens.push(result.value);
  }
  return tokens;
}

async function tokenizeError(values: string[]): Promise<unknown> {
  const reader = chunks(values)
    .pipeThrough(new JsonTokenDecoderStream())
    .getReader();
  try {
    while (!(await reader.read()).done);
  } catch (ex) {
    return ex;
  }
  throw new Error("Expected the stream to error");
}

test("JsonTokenDecoderStream - emits tokens with their positions", async () => {
  expect(await tokenize([`{"a": [1, -2.5e3, true, false, null]}`])).toEqual([
    { type: "PUNCTUATION", value: "{", position: 0 },
    { type: "KEY", value: "a", position: 1 },
    { type: "PUNCTUATION", value: ":", position: 4 },
    { type: "PUNCTUATION", value: "[", position: 6 },
    { type: "LITERAL", value: 1, position: 7 },
    { type: "PUNCTUATION", value: ",", position: 8 },
    { type: "LITERAL", value: -2500, position: 10 },
    { type: "PUNCTUATION", value: ",", position: 16 },
    { type: "LITERAL", value: true, position: 18 },
    { type: "PUNCTUATION", value: ",", position: 22 },
    { type: "LITERAL", value: false, position: 24 },
    { type: "PUNCTUATION", value: ",", position: 29 },
    { type: "LITERAL", value: null, position: 31 },
    { type: "PUNCTUATION", value: "]", position: 35 },
    { type: "PUNCTUATION", value: "}", position: 36 },
  ]);
});

test("JsonTokenDecoderStream - strings directly inside an object, after { or a comma, are keys", async () => {
  const describe = (token: JsonToken) =>
    token.type === "PUNCTUATION" ? token.value : `${token.type}:${token.value}`;
  expect(
    (
      await tokenize([
        `{"a": "x", "b": ["y", {"c": "z"}, "w"], "d": {"e": []}, "f": "v"}`,
      ])
    ).map(describe),
  ).toEqual([
    "{",
    "KEY:a",
    ":",
    "LITERAL:x",
    ",",
    "KEY:b",
    ":",
    "[",
    "LITERAL:y",
    ",",
    "{",
    "KEY:c",
    ":",
    "LITERAL:z",
    "}",
    ",",
    "LITERAL:w",
    "]",
    ",",
    "KEY:d",
    ":",
    "{",
    "KEY:e",
    ":",
    "[",
    "]",
    "}",
    ",",
    "KEY:f",
    ":",
    "LITERAL:v",
    "}",
  ]);
  // Strings at the root, or in arrays, are never keys
  expect((await tokenize([`"a"`])).map(describe)).toEqual(["LITERAL:a"]);
  expect((await tokenize([`["a", "b"]`])).map(describe)).toEqual([
    "[",
    "LITERAL:a",
    ",",
    "LITERAL:b",
    "]",
  ]);
});

test("JsonTokenDecoderStream - decodes string escapes", async () => {
  const value = 'quote " backslash \\ newline \n unicode é 👋';
  expect(await tokenize([JSON.stringify(value)])).toEqual([
    { type: "LITERAL", value, position: 0 },
  ]);
});

test("JsonTokenDecoderStream - gives the same tokens however the input is split", async () => {
  const text = JSON.stringify(
    {
      list: [1, -0.5, 12e-3, 'a \\"b\\" 👋', true, false, null],
      nested: { empty: {}, arr: [] },
    },
    null,
    2,
  );
  const expected = await tokenize([text]);
  expect(expected.length).toBeGreaterThan(20);
  // Split into single UTF-16 code units, including through the emoji
  expect(await tokenize(text.split(""))).toEqual(expected);
  // Split at every possible point into two chunks
  for (let i = 0; i <= text.length; i++) {
    expect(await tokenize([text.slice(0, i), text.slice(i)])).toEqual(expected);
  }
});

test("JsonTokenDecoderStream - errors on tokens in the wrong place", async () => {
  const invalid: [string, string][] = [
    [`[1 2]`, "Unexpected number 2 at position 3"],
    [`[1,]`, "Unexpected token ] at position 3"],
    [`[,1]`, "Unexpected token , at position 1"],
    [`[1}`, "Unexpected token } at position 2"],
    [`{"a": 1]`, "Unexpected token ] at position 7"],
    [`[1]]`, "Unexpected token ] at position 3"],
    [`}`, "Unexpected token } at position 0"],
    [`:`, "Unexpected token : at position 0"],
    [`{"a" 1}`, "Unexpected number 1 at position 5"],
    [`{"a": 1,}`, "Unexpected token } at position 8"],
    [`{"a"}`, "Unexpected token } at position 4"],
    [`{1: 2}`, "Unexpected number 1 at position 1"],
    [`{true: 2}`, "Unexpected token true at position 1"],
    [`{"a": 1 "b": 2}`, "Unexpected string at position 8"],
    [`{"a": "b": 1}`, "Unexpected token : at position 9"],
    [`{"a":: 1}`, "Unexpected token : at position 5"],
    [`{{}}`, "Unexpected token { at position 1"],
    [`["a": 1]`, "Unexpected token : at position 4"],
    // Only a single value is allowed
    [`[1] [2]`, "Unexpected token [ at position 4"],
    [`1 2`, "Unexpected number 2 at position 2"],
    [`"a" "b"`, "Unexpected string at position 4"],
    [`{} null`, "Unexpected token null at position 3"],
    // Incomplete documents
    [``, "Unexpected end of JSON input"],
    [` \t\r\n `, "Unexpected end of JSON input"],
    [`[1`, "Unexpected end of JSON input"],
    [`[1,`, "Unexpected end of JSON input"],
    [`{"a"`, "Unexpected end of JSON input"],
    [`{"a":`, "Unexpected end of JSON input"],
    [`[[{}]`, "Unexpected end of JSON input"],
  ];
  for (const [text, message] of invalid) {
    for (const values of [[text], text.split("")]) {
      expect(await tokenizeError(values), text).toMatchObject({
        name: "SyntaxError",
        message: `JsonTokenDecoderStream: ${message}`,
      });
    }
  }
});

test("JsonTokenDecoderStream - emits the tokens before a problem", async () => {
  const results = await readableStreamResults(
    chunks([`[1, `, `2] 3`]).pipeThrough(new JsonTokenDecoderStream()),
  );
  expect(results.map((result) => result.ok && result.value)).toEqual([
    { type: "PUNCTUATION", value: "[", position: 0 },
    { type: "LITERAL", value: 1, position: 1 },
    { type: "PUNCTUATION", value: ",", position: 2 },
    { type: "LITERAL", value: 2, position: 4 },
    { type: "PUNCTUATION", value: "]", position: 5 },
    false,
  ]);
});

test("JsonTokenDecoderStream - allows any value at the root, and whitespace around it", async () => {
  expect(await tokenize([` \n[]\n `])).toEqual([
    { type: "PUNCTUATION", value: "[", position: 2 },
    { type: "PUNCTUATION", value: "]", position: 3 },
  ]);
  expect(await tokenize([`{}`])).toHaveLength(2);
  expect(await tokenize([` null `])).toEqual([
    { type: "LITERAL", value: null, position: 1 },
  ]);
  expect(await tokenize([`"a"`])).toEqual([
    { type: "LITERAL", value: "a", position: 0 },
  ]);
});

test("JsonTokenDecoderStream - a number or literal at the end of the input is emitted", async () => {
  expect(await tokenize(["4", "2"])).toEqual([
    { type: "LITERAL", value: 42, position: 0 },
  ]);
  expect(await tokenize(["tr", "ue"])).toEqual([
    { type: "LITERAL", value: true, position: 0 },
  ]);
});

test("JsonTokenDecoderStream - errors on invalid tokens", async () => {
  const invalid: [string, string][] = [
    [`01`, "Unexpected number 01 at position 0"],
    [`[1.]`, "Unexpected number 1. at position 1"],
    [`[-]`, "Unexpected number - at position 1"],
    [`[1e]`, "Unexpected number 1e at position 1"],
    [`[1-2]`, "Unexpected number 1-2 at position 1"],
    [`[+1]`, `Unexpected character "+" at position 1`],
    [`[.5]`, `Unexpected character "." at position 1`],
    [`[tru]`, "Unexpected token tru at position 1"],
    [`[True]`, "Unexpected token True at position 1"],
    [`[nulls]`, "Unexpected token nulls at position 1"],
    [`[NaN]`, "Unexpected token NaN at position 1"],
    [`['a']`, `Unexpected character "'" at position 1`],
    [`["bad \\x"]`, "Unexpected string at position 1"],
    [`["control \u0001"]`, "Unexpected string at position 1"],
    [`"unterminated`, "Unexpected end of JSON input"],
    [`[1] // comment`, `Unexpected character "/" at position 4`],
  ];
  for (const [text, message] of invalid) {
    expect(await tokenizeError([text]), text).toMatchObject({
      name: "SyntaxError",
      message: `JsonTokenDecoderStream: ${message}`,
    });
  }
});

testTransformRobustness({
  name: "JsonTokenDecoderStream",
  create: () => new JsonTokenDecoderStream(),
  validInput: [`[{"a"`, `: 1}, "b"`, `, 2]`],
  invalidInput: `'`,
});
