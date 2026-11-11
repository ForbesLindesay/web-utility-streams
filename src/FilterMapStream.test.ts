import { test, expect } from "vitest";
import FilterMapStream from "./FilterMapStream.js";
import {
  delay,
  readableStream,
  readableStreamResults,
  testTransformRobustness,
} from "./StreamTestUtils.js";

test("FilterMapStream - maps chunks and drops undefined results", async () => {
  expect(
    await readableStreamResults(
      readableStream([1, 2, 3, 4]).pipeThrough(
        new FilterMapStream(async (n: number) => {
          await delay(5);
          return n % 2 === 0 ? `even ${n}` : undefined;
        }),
      ) satisfies ReadableStream<string>,
    ),
  ).toEqual([
    { ok: true, value: "even 2" },
    { ok: true, value: "even 4" },
  ]);
});

test("FilterMapStream - supports synchronous functions and keeps null", async () => {
  expect(
    await readableStreamResults(
      readableStream([1, 2, 3]).pipeThrough(
        new FilterMapStream((n: number) =>
          n === 1 ? null : n === 2 ? undefined : n,
        ),
      ),
    ),
  ).toEqual([
    { ok: true, value: null },
    { ok: true, value: 3 },
  ]);
});

test("FilterMapStream - errors the stream if the function throws", async () => {
  expect(
    await readableStreamResults(
      readableStream([1, 2, 3]).pipeThrough(
        new FilterMapStream(async (n: number) => {
          if (n === 2) throw new Error("Bad value");
          return n;
        }),
      ),
    ),
  ).toEqual([{ ok: true, value: 1 }, { ok: false }]);
});

testTransformRobustness({
  name: "FilterMapStream",
  create: () =>
    new FilterMapStream(async (value) => {
      if (value === "bad") throw new Error("Bad value");
      await delay(5);
      return value;
    }),
  validInput: [1, 2, 3],
  invalidInput: "bad",
});
