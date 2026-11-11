import { test, expect } from "vitest";
import DeferredStream from "./DeferredStream.js";
import {
  delay,
  Delay,
  readableStream,
  readableStreamResults,
  testTransformRobustness,
  trackedSource,
} from "./internal/StreamTestUtils.js";

test("DeferredStream - emits the chunks from the resolved stream", async () => {
  expect(
    await readableStreamResults(
      new DeferredStream(Promise.resolve(readableStream([1, 2, 3]))),
    ),
  ).toEqual([
    { ok: true, value: 1 },
    { ok: true, value: 2 },
    { ok: true, value: 3 },
  ]);
});

test("DeferredStream - waits for the promise to resolve", async () => {
  const stream = new DeferredStream(
    delay(50).then(() => readableStream(["a", "b"])),
  );
  expect(await readableStreamResults(stream)).toEqual([
    { ok: true, value: "a" },
    { ok: true, value: "b" },
  ]);
});

test("DeferredStream - errors if the promise rejects", async () => {
  const stream = new DeferredStream<number>(
    delay(10).then(() => {
      throw new Error("Failed to get stream");
    }),
  );
  const reader = stream.getReader();
  await expect(reader.read()).rejects.toThrow("Failed to get stream");
});

test("DeferredStream - errors if the resolved stream is locked", async () => {
  const inner = readableStream([1]);
  inner.getReader();
  expect(
    await readableStreamResults(new DeferredStream(Promise.resolve(inner))),
  ).toEqual([{ ok: false }]);
});

test("DeferredStream - a rejected promise does not cause an unhandled rejection if the stream is never read", async () => {
  new DeferredStream(Promise.reject(new Error("Never read")));
  // The unhandled rejection check in testSetup.ts runs after this test
  await delay(10);
});

test("DeferredStream - forwards errors from the resolved stream", async () => {
  expect(
    await readableStreamResults(
      new DeferredStream(
        Promise.resolve(readableStream([1, 2], { err: true })),
      ),
    ),
  ).toEqual([{ ok: true, value: 1 }, { ok: true, value: 2 }, { ok: false }]);
});

test("DeferredStream - does not read from the resolved stream until it is read", async () => {
  let pulls = 0;
  const inner = new ReadableStream<number>(
    {
      pull(controller) {
        controller.enqueue(pulls++);
      },
    },
    { highWaterMark: 0 },
  );
  const stream = new DeferredStream(Promise.resolve(inner));
  await delay(20);
  expect(pulls).toBe(0);
  const reader = stream.getReader();
  expect(await reader.read()).toEqual({ done: false, value: 0 });
  await delay(20);
  expect(pulls).toBe(1);
});

test("DeferredStream - cancelling before the promise resolves cancels the stream once it resolves", async () => {
  const inner = trackedSource([1, 2, 3]);
  const cancelReasons: unknown[] = [];
  const { promise, resolve } = Promise.withResolvers<ReadableStream<number>>();
  const stream = new DeferredStream(promise);
  let cancelled = false;
  const cancelPromise = stream.cancel("Some reason").then(() => {
    cancelled = true;
  });
  await delay(20);
  expect(cancelled).toBe(false);
  resolve(
    new ReadableStream({
      cancel: (reason) => {
        cancelReasons.push(reason);
        return inner.stream.cancel(reason);
      },
    }),
  );
  await cancelPromise;
  expect(cancelReasons).toEqual(["Some reason"]);
  expect(inner.state.cancelled).toBe(true);
});

test("DeferredStream - cancelling while reading cancels the resolved stream", async () => {
  const inner = trackedSource([1, new Delay(200), 2]);
  const stream = new DeferredStream(Promise.resolve(inner.stream));
  const reader = stream.getReader();
  expect(await reader.read()).toEqual({ done: false, value: 1 });
  const pendingRead = reader.read();
  await reader.cancel();
  expect(await pendingRead).toEqual({ done: true, value: undefined });
  expect(inner.state.cancelled).toBe(true);
});

test("DeferredStream - cancelling when the promise rejected succeeds", async () => {
  const stream = new DeferredStream(Promise.reject(new Error("No stream")));
  await stream.cancel();
});

// Test that errors and cancellation are propagated in both directions by
// wrapping a TransformStream's readable side in a DeferredStream.
testTransformRobustness({
  name: "DeferredStream",
  create: () => {
    const { readable, writable } = new TransformStream();
    return {
      writable,
      readable: new DeferredStream(delay(5).then(() => readable)),
    };
  },
  validInput: [1, 2, 3],
});
