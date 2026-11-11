import { test, expect } from "vitest";
import NewlineDelimitedJsonEncoderStream from "./NewlineDelimitedJsonEncoderStream.js";
import NewlineDelimitedJsonDecoderStream from "./NewlineDelimitedJsonDecoderStream.js";
import {
  readableStream,
  readableStreamResults,
  testTransformRobustness,
} from "./StreamTestUtils.js";

test("NewlineDelimitedJsonEncoderStream - encodes each value as a line of JSON", async () => {
  expect(
    await readableStreamResults(
      readableStream<unknown>([
        { hello: "world" },
        [1, 2],
        "a\nb",
        null,
      ]).pipeThrough(new NewlineDelimitedJsonEncoderStream()),
    ),
  ).toEqual([
    { ok: true, value: `{"hello":"world"}\n` },
    { ok: true, value: `[1,2]\n` },
    { ok: true, value: `"a\\nb"\n` },
    { ok: true, value: `null\n` },
  ]);
});

test("NewlineDelimitedJsonEncoderStream - supports custom line endings", async () => {
  expect(
    await readableStreamResults(
      readableStream([1, 2]).pipeThrough(
        new NewlineDelimitedJsonEncoderStream({ lineEnding: "\r\n" }),
      ),
    ),
  ).toEqual([
    { ok: true, value: `1\r\n` },
    { ok: true, value: `2\r\n` },
  ]);
});

test("NewlineDelimitedJsonEncoderStream - values that cannot be encoded error the stream", async () => {
  const circular: any = {};
  circular.self = circular;
  for (const invalid of [undefined, () => {}, Symbol("x"), 10n, circular]) {
    expect(
      await readableStreamResults(
        readableStream<unknown>([1, invalid, 2]).pipeThrough(
          new NewlineDelimitedJsonEncoderStream(),
        ),
      ),
    ).toEqual([{ ok: true, value: `1\n` }, { ok: false }]);
  }
});

test("NewlineDelimitedJsonEncoderStream - round trips with NewlineDelimitedJsonDecoderStream", async () => {
  const values = [{ a: 1 }, "two\nlines", [null, true], 42];
  expect(
    await readableStreamResults(
      readableStream<unknown>(values)
        .pipeThrough(
          new NewlineDelimitedJsonEncoderStream({ lineEnding: "\r" }),
        )
        .pipeThrough(new NewlineDelimitedJsonDecoderStream()),
    ),
  ).toEqual(values.map((value) => ({ ok: true, value })));
});

testTransformRobustness({
  name: "NewlineDelimitedJsonEncoderStream",
  create: () => new NewlineDelimitedJsonEncoderStream(),
  validInput: [{ a: 1 }, [1, 2], 3],
  invalidInput: undefined,
});
