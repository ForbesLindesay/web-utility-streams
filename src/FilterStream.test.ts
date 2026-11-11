import { test, expect } from "vitest";
import FilterStream from "./FilterStream.js";
import {
  readableStream,
  readableStreamResults,
  testTransformRobustness,
} from "./StreamTestUtils.js";

test("FilterStream - only keeps chunks that match the filter", async () => {
  expect(
    await readableStreamResults(
      readableStream([1, 2, 3, 4, 5]).pipeThrough(
        new FilterStream((n: number) => n % 2 === 1),
      ),
    ),
  ).toEqual([
    { ok: true, value: 1 },
    { ok: true, value: 3 },
    { ok: true, value: 5 },
  ]);
});

test("FilterStream - supports type guards", async () => {
  const stream = readableStream<string | number>([1, "a", 2, "b"]).pipeThrough(
    new FilterStream((v): v is string => typeof v === "string"),
  ) satisfies ReadableStream<string>;
  expect(await readableStreamResults(stream)).toEqual([
    { ok: true, value: "a" },
    { ok: true, value: "b" },
  ]);
});

test("FilterStream - errors the stream if the filter throws", async () => {
  expect(
    await readableStreamResults(
      readableStream([1, 2, 3]).pipeThrough(
        new FilterStream((n: number) => {
          if (n === 2) throw new Error("Bad value");
          return true;
        }),
      ),
    ),
  ).toEqual([{ ok: true, value: 1 }, { ok: false }]);
});

testTransformRobustness({
  name: "FilterStream",
  create: () =>
    new FilterStream((value: unknown) => {
      if (value === "bad") throw new Error("Bad value");
      return true;
    }),
  validInput: [1, 2, 3],
  invalidInput: "bad",
});
