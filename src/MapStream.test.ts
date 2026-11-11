import { test, expect } from "vitest";
import MapStream from "./MapStream.js";
import {
  delay,
  readableStream,
  readableStreamResults,
  testTransformRobustness,
} from "./internal/StreamTestUtils.js";

test("MapStream - maps each chunk using an async function, preserving order", async () => {
  expect(
    await readableStreamResults(
      readableStream([3, 1, 2]).pipeThrough(
        new MapStream(async (n: number) => {
          await delay(n * 10);
          return `n=${n}`;
        }),
      ) satisfies ReadableStream<string>,
    ),
  ).toEqual([
    { ok: true, value: "n=3" },
    { ok: true, value: "n=1" },
    { ok: true, value: "n=2" },
  ]);
});

test("MapStream - supports synchronous functions", async () => {
  expect(
    await readableStreamResults(
      readableStream([1, 2]).pipeThrough(new MapStream((n: number) => n * 2)),
    ),
  ).toEqual([
    { ok: true, value: 2 },
    { ok: true, value: 4 },
  ]);
});

test("MapStream - undefined is a valid output", async () => {
  expect(
    await readableStreamResults(
      readableStream([1]).pipeThrough(new MapStream(() => undefined)),
    ),
  ).toEqual([{ ok: true, value: undefined }]);
});

test("MapStream - errors the stream if the function throws", async () => {
  let cancelled = false;
  expect(
    await readableStreamResults(
      readableStream([1, 2, 3], {
        onCancel: () => (cancelled = true),
      }).pipeThrough(
        new MapStream(async (n: number) => {
          if (n === 2) throw new Error("Bad value");
          return n;
        }),
      ),
    ),
  ).toEqual([{ ok: true, value: 1 }, { ok: false }]);
  expect(cancelled).toBe(true);
});

testTransformRobustness({
  name: "MapStream",
  create: () =>
    new MapStream(async (value) => {
      if (value === "bad") throw new Error("Bad value");
      await delay(5);
      return value;
    }),
  validInput: [1, 2, 3],
  invalidInput: "bad",
});
