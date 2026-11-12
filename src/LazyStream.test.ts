import { test, expect } from "vitest";
import LazyStream from "./LazyStream.js";
import {
  countingSource,
  delay,
  Delay,
  readableStream,
  readableStreamResults,
  testTransformRobustness,
  trackedSource,
} from "./internal/StreamTestUtils.js";

test("LazyStream - emits the chunks from the stream returned by the function", async () => {
  expect(
    await readableStreamResults(
      new LazyStream(() => readableStream([1, 2, 3])),
    ),
  ).toEqual([
    { ok: true, value: 1 },
    { ok: true, value: 2 },
    { ok: true, value: 3 },
  ]);
});

test("LazyStream - supports a function that returns a promise", async () => {
  const stream = new LazyStream(async () => {
    await delay(20);
    return readableStream(["a", "b"]);
  });
  expect(await readableStreamResults(stream)).toEqual([
    { ok: true, value: "a" },
    { ok: true, value: "b" },
  ]);
});

test("LazyStream - does not call the function until the stream is read", async () => {
  let calls = 0;
  const stream = new LazyStream(() => {
    calls++;
    return readableStream([1, 2]);
  });
  await delay(20);
  expect(calls).toBe(0);
  const reader = stream.getReader();
  await delay(20);
  expect(calls).toBe(0);
  expect(await reader.read()).toEqual({ done: false, value: 1 });
  expect(await reader.read()).toEqual({ done: false, value: 2 });
  expect(await reader.read()).toEqual({ done: true, value: undefined });
  // The function is only called once
  expect(calls).toBe(1);
});

test("LazyStream - does not read ahead from the underlying stream", async () => {
  const source = countingSource([1, 2, 3]);
  const reader = new LazyStream(() => source.stream).getReader();
  expect(await reader.read()).toEqual({ done: false, value: 1 });
  await delay(20);
  expect(source.pulled).toEqual([1]);
});

test("LazyStream - errors if the function throws", async () => {
  const stream = new LazyStream<number>(() => {
    throw new Error("Failed to get stream");
  });
  await expect(stream.getReader().read()).rejects.toThrow(
    "Failed to get stream",
  );
});

test("LazyStream - errors if the promise rejects", async () => {
  const stream = new LazyStream<number>(async () => {
    await delay(10);
    throw new Error("Failed to get stream");
  });
  await expect(stream.getReader().read()).rejects.toThrow(
    "Failed to get stream",
  );
});

test("LazyStream - errors if the returned stream is locked", async () => {
  const inner = readableStream([1]);
  inner.getReader();
  expect(await readableStreamResults(new LazyStream(() => inner))).toEqual([
    { ok: false },
  ]);
});

test("LazyStream - forwards errors from the returned stream", async () => {
  expect(
    await readableStreamResults(
      new LazyStream(() => readableStream([1, 2], { err: true })),
    ),
  ).toEqual([{ ok: true, value: 1 }, { ok: true, value: 2 }, { ok: false }]);
});

test("LazyStream - cancelling before it is read does not call the function", async () => {
  let calls = 0;
  const stream = new LazyStream(() => {
    calls++;
    return readableStream([1]);
  });
  await stream.cancel("Some reason");
  expect(calls).toBe(0);
  await delay(20);
  expect(calls).toBe(0);
});

test("LazyStream - cancelling while waiting for the promise cancels the stream once it resolves", async () => {
  const cancelReasons: unknown[] = [];
  const { promise, resolve } = Promise.withResolvers<ReadableStream<number>>();
  let calls = 0;
  const reader = new LazyStream(() => {
    calls++;
    return promise;
  }).getReader();
  const pendingRead = reader.read();
  // Wait for the first pull to call the function
  await delay(10);
  expect(calls).toBe(1);
  let cancelled = false;
  const cancelPromise = reader.cancel("Some reason").then(() => {
    cancelled = true;
  });
  await delay(20);
  expect(cancelled).toBe(false);
  resolve(
    new ReadableStream({ cancel: (reason) => void cancelReasons.push(reason) }),
  );
  await cancelPromise;
  expect(await pendingRead).toEqual({ done: true, value: undefined });
  expect(cancelReasons).toEqual(["Some reason"]);
});

test("LazyStream - cancelling while reading cancels the returned stream", async () => {
  const inner = trackedSource([1, new Delay(200), 2]);
  const reader = new LazyStream(() => inner.stream).getReader();
  expect(await reader.read()).toEqual({ done: false, value: 1 });
  const pendingRead = reader.read();
  await reader.cancel();
  expect(await pendingRead).toEqual({ done: true, value: undefined });
  expect(inner.state.cancelled).toBe(true);
});

test("LazyStream - cancelling while waiting for a promise that rejects succeeds", async () => {
  const { promise, reject } = Promise.withResolvers<ReadableStream<number>>();
  let calls = 0;
  const reader = new LazyStream(() => {
    calls++;
    return promise;
  }).getReader();
  const pendingRead = reader.read();
  // Wait for the first pull to call the function
  await delay(10);
  expect(calls).toBe(1);
  const cancelPromise = reader.cancel();
  reject(new Error("No stream"));
  await cancelPromise;
  expect(await pendingRead).toEqual({ done: true, value: undefined });
});

// Test that errors and cancellation are propagated in both directions by
// wrapping a TransformStream's readable side in a LazyStream.
testTransformRobustness({
  name: "LazyStream",
  create: () => {
    const { readable, writable } = new TransformStream();
    return {
      writable,
      readable: new LazyStream(() => delay(5).then(() => readable)),
    };
  },
  validInput: [1, 2, 3],
});
