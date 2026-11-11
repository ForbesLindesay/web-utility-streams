import { test, expect, vi } from "vitest";
import DebugStream from "./DebugStream.js";
import {
  delay,
  readableStream,
  readableStreamResults,
  testTransformRobustness,
  trackedSource,
} from "./StreamTestUtils.js";

test("DebugStream - Logs values and passes them through unchanged", async () => {
  const logged: number[] = [];
  expect(
    await readableStreamResults(
      readableStream([1, 2, 3, 4]).pipeThrough(
        new DebugStream((value) => logged.push(value)),
      ) satisfies ReadableStream<number>,
    ),
  ).toEqual([
    { ok: true, value: 1 },
    { ok: true, value: 2 },
    { ok: true, value: 3 },
    { ok: true, value: 4 },
  ]);
  expect(logged).toEqual([1, 2, 3, 4]);
});

test("DebugStream - Logs to console.log by default", async () => {
  const log = vi.spyOn(console, "log").mockImplementation(() => {});
  try {
    expect(
      await readableStreamResults(
        readableStream(["a", "b"]).pipeThrough(new DebugStream()),
      ),
    ).toEqual([
      { ok: true, value: "a" },
      { ok: true, value: "b" },
    ]);
    expect(log.mock.calls).toEqual([["a"], ["b"]]);
  } finally {
    log.mockRestore();
  }
});

test("DebugStream - Forwards errors", async () => {
  const logged: number[] = [];
  expect(
    await readableStreamResults(
      readableStream([2, 4], { err: true }).pipeThrough(
        new DebugStream((value) => logged.push(value)),
      ),
    ),
  ).toEqual([{ ok: true, value: 2 }, { ok: true, value: 4 }, { ok: false }]);
  expect(logged).toEqual([2, 4]);
});

test("DebugStream - Errors the stream if the logging function throws", async () => {
  const source = trackedSource([1, 2, 3]);
  expect(
    await readableStreamResults(
      source.stream.pipeThrough(
        new DebugStream((value) => {
          if (value === 2) throw new Error("Logging failed");
        }),
      ),
    ),
  ).toEqual([{ ok: true, value: 1 }, { ok: false }]);
  // Cancellation propagates back through the pipe asynchronously
  await delay(20);
  expect(source.state.cancelled).toBe(true);
});

testTransformRobustness({
  name: "DebugStream",
  create: () =>
    new DebugStream((value) => {
      if (value === "bad") throw new Error("Logging failed");
    }),
  validInput: [1, 2, 3],
  invalidInput: "bad",
});
