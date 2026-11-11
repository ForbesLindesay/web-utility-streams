import { test, expect } from "vitest";
import NewlineDelimitedJsonDecoderStream from "./NewlineDelimitedJsonDecoderStream.js";
import {
  delay,
  readableStream,
  readableStreamResults,
  testTransformRobustness,
} from "./internal/StreamTestUtils.js";

test("NewlineDelimitedJsonDecoderStream - handles newlines in the middle of chunks and doesn't need a trailing newline", async () => {
  expect(
    await readableStreamResults(
      readableStream([`{"hello"`, `:"world"}\n{"foo":"bar"}`]).pipeThrough(
        new NewlineDelimitedJsonDecoderStream(),
      ),
    ),
  ).toEqual([
    { ok: true, value: { hello: "world" } },
    { ok: true, value: { foo: "bar" } },
  ]);
});

test("NewlineDelimitedJsonDecoderStream - handles trailing newlines", async () => {
  expect(
    await readableStreamResults(
      readableStream([`{"hello"`, `:"world"}\n{"foo":"bar"}\n`]).pipeThrough(
        new NewlineDelimitedJsonDecoderStream(),
      ),
    ),
  ).toEqual([
    { ok: true, value: { hello: "world" } },
    { ok: true, value: { foo: "bar" } },
  ]);
});

test("NewlineDelimitedJsonDecoderStream - any line ending is fine", async () => {
  expect(
    await readableStreamResults(
      readableStream([
        `{"hello"`,
        `:"world"}\r{"foo":"bar"}\r\n{"whatever"`,
        `:42}`,
      ]).pipeThrough(new NewlineDelimitedJsonDecoderStream()),
    ),
  ).toEqual([
    { ok: true, value: { hello: "world" } },
    { ok: true, value: { foo: "bar" } },
    { ok: true, value: { whatever: 42 } },
  ]);
});

test("NewlineDelimitedJsonDecoderStream - Blank lines are ignored", async () => {
  expect(
    await readableStreamResults(
      readableStream([`{"hello"`, `:"world"}\n\n\n{"foo":"bar"}`]).pipeThrough(
        new NewlineDelimitedJsonDecoderStream(),
      ),
    ),
  ).toEqual([
    { ok: true, value: { hello: "world" } },
    { ok: true, value: { foo: "bar" } },
  ]);
});

test("NewlineDelimitedJsonDecoderStream - Invalid lines can optionally be handled", async () => {
  expect(
    await readableStreamResults(
      readableStream([
        `{"hello"`,
        `:"world"}\n{"not valid": "json"\n\n{"foo":"bar"}`,
      ]).pipeThrough(
        new NewlineDelimitedJsonDecoderStream({
          onError: (invalidLine, _err, controller) =>
            controller.enqueue({ invalidLine }),
        }),
      ),
    ),
  ).toEqual([
    { ok: true, value: { hello: "world" } },
    { ok: true, value: { invalidLine: `{"not valid": "json"` } },
    { ok: true, value: { foo: "bar" } },
  ]);
});

test("NewlineDelimitedJsonDecoderStream - Invalid JSON at the end of the stream emits an error that can be handled using try/catch", async () => {
  expect(
    await readableStreamResults(
      readableStream([
        `{"hello"`,
        `:"world"}\n{"foo":"bar"}`,
        `\nnot JSON`,
      ]).pipeThrough(new NewlineDelimitedJsonDecoderStream()),
    ),
  ).toEqual([
    { ok: true, value: { hello: "world" } },
    { ok: true, value: { foo: "bar" } },
    { ok: false },
  ]);
});

test("NewlineDelimitedJsonDecoderStream - Invalid JSON followed by a newline an error that can be handled using try/catch", async () => {
  expect(
    await readableStreamResults(
      readableStream([
        `{"hello"`,
        `:"world"}\n{"foo":"bar"}`,
        `\nnot JSON\n`,
      ]).pipeThrough(new NewlineDelimitedJsonDecoderStream()),
    ),
  ).toEqual([
    { ok: true, value: { hello: "world" } },
    { ok: true, value: { foo: "bar" } },
    { ok: false },
  ]);
  expect(
    await readableStreamResults(
      readableStream([
        `{"hello"`,
        `:"world"}\n{"foo":"bar"}`,
        `\nnot JSON\n`,
        `{"hello":"world"}`,
      ]).pipeThrough(new NewlineDelimitedJsonDecoderStream()),
    ),
  ).toEqual([
    { ok: true, value: { hello: "world" } },
    { ok: true, value: { foo: "bar" } },
    { ok: false },
  ]);
});

test("NewlineDelimitedJsonDecoderStream - An error from the readable stream is forwarded so it can be handled", async () => {
  expect(
    await readableStreamResults(
      readableStream([`{"hello"`, `:"world"}\n`, `{"foo":"bar"}\n`], {
        err: true,
      }).pipeThrough(new NewlineDelimitedJsonDecoderStream()),
    ),
  ).toEqual([
    { ok: true, value: { hello: "world" } },
    { ok: true, value: { foo: "bar" } },
    { ok: false },
  ]);

  // Wait to check no uncaught exceptions are thrown after the stream is closed.
  await delay(200);
});

testTransformRobustness({
  name: "NewlineDelimitedJsonDecoderStream",
  create: () => new NewlineDelimitedJsonDecoderStream(),
  validInput: ['{"a":', "1}\n[1,2]\n", "3\n"],
  invalidInput: "not json\n",
});
