import { test, expect } from "vitest";
import ConcatStream from "./ConcatStream.js";
import EmptyStream from "./EmptyStream.js";
import {
  countingSource,
  delay,
  Delay,
  destination,
  readableStream,
  readableStreamResults,
  runPipeline,
  testTransformRobustness,
  trackedSource,
} from "./internal/StreamTestUtils.js";

test("ConcatStream - emits all events from each stream in order", async () => {
  expect(
    await readableStreamResults(
      new ConcatStream([
        readableStream([1, new Delay(100), 2]),
        readableStream(["a", "b"]),
        readableStream([3]),
      ]) satisfies ReadableStream<number | string>,
    ),
  ).toEqual([
    { ok: true, value: 1 },
    { ok: true, value: 2 },
    { ok: true, value: "a" },
    { ok: true, value: "b" },
    { ok: true, value: 3 },
  ]);
});

test("ConcatStream - closes immediately when there are no source streams", async () => {
  expect(await readableStreamResults(new ConcatStream([]))).toEqual([]);
});

test("ConcatStream - skips empty streams", async () => {
  expect(
    await readableStreamResults(
      new ConcatStream([
        new EmptyStream(),
        readableStream([1]),
        new EmptyStream(),
        new EmptyStream(),
        readableStream([2]),
        new EmptyStream(),
      ]),
    ),
  ).toEqual([
    { ok: true, value: 1 },
    { ok: true, value: 2 },
  ]);
});

test("ConcatStream - locks all source streams immediately", async () => {
  const a = readableStream([1]);
  const b = readableStream([2]);
  new ConcatStream([a, b]);
  expect(a.locked).toBe(true);
  expect(b.locked).toBe(true);
});

test("ConcatStream - does not read from a stream until the previous streams have closed", async () => {
  let pullsB = 0;
  const a = readableStream([1, new Delay(50), 2]);
  const b = new ReadableStream<number>(
    {
      pull(controller) {
        pullsB++;
        if (pullsB > 2) controller.close();
        else controller.enqueue(100 + pullsB);
      },
    },
    { highWaterMark: 0 },
  );
  const reader = new ConcatStream([a, b]).getReader();
  expect(await reader.read()).toEqual({ done: false, value: 1 });
  await delay(20);
  expect(pullsB).toBe(0);
  expect(await reader.read()).toEqual({ done: false, value: 2 });
  expect(pullsB).toBe(0);
  expect(await reader.read()).toEqual({ done: false, value: 101 });
  expect(await reader.read()).toEqual({ done: false, value: 102 });
  expect(await reader.read()).toEqual({ done: true, value: undefined });
});

test("ConcatStream - an error in the current stream errors the output and cancels the other streams", async () => {
  const a = trackedSource([1]);
  const b = trackedSource([2], { err: true });
  const c = trackedSource([3]);
  expect(
    await readableStreamResults(
      new ConcatStream([a.stream, b.stream, c.stream]),
    ),
  ).toEqual([{ ok: true, value: 1 }, { ok: true, value: 2 }, { ok: false }]);
  await delay(20);
  expect(c.state.cancelled).toBe(true);
});

test("ConcatStream - cancelling the output cancels all source streams with the reason", async () => {
  const reasons: unknown[] = [];
  const source = () =>
    new ReadableStream({ cancel: (reason) => void reasons.push(reason) });
  await new ConcatStream([source(), source(), source()]).cancel("Some reason");
  expect(reasons).toEqual(["Some reason", "Some reason", "Some reason"]);
});

test("ConcatStream - cancelling while reading cancels the current and remaining streams", async () => {
  const a = trackedSource([1, new Delay(200), 2]);
  const b = trackedSource([3]);
  const reader = new ConcatStream([a.stream, b.stream]).getReader();
  expect(await reader.read()).toEqual({ done: false, value: 1 });
  const pendingRead = reader.read();
  await reader.cancel();
  expect(await pendingRead).toEqual({ done: true, value: undefined });
  expect(a.state.cancelled).toBe(true);
  expect(b.state.cancelled).toBe(true);
});

test("ConcatStream - destination errors in a pipeline", async () => {
  const a = trackedSource([1, 2]);
  const b = trackedSource([3, 4]);
  const dest = destination({ failOnWrite: 1 });
  const result = await runPipeline(
    new ConcatStream([a.stream, b.stream]).pipeTo(dest.stream),
  );
  expect(result).toMatchObject({
    ok: false,
    error: { message: "Destination failed" },
  });
  await delay(50);
  expect(a.state.cancelled).toBe(true);
  expect(b.state.cancelled).toBe(true);
});

// Test that errors and cancellation are propagated in both directions by
// putting a TransformStream's readable side between two other streams.
testTransformRobustness({
  name: "ConcatStream",
  create: () => {
    const { readable, writable } = new TransformStream();
    return {
      writable,
      readable: new ConcatStream([
        readableStream(["first"]),
        readable,
        new EmptyStream(),
      ]),
    };
  },
  validInput: [1, 2, 3],
});

test("ConcatStream - by default, reads one chunk ahead of the reader", async () => {
  const a = countingSource([1, 2]);
  const b = countingSource([3, 4]);
  const reader = new ConcatStream([a.stream, b.stream]).getReader();
  await delay(10);
  expect([...a.pulled, ...b.pulled]).toEqual([1]);
  expect(await reader.read()).toEqual({ done: false, value: 1 });
  await delay(10);
  expect([...a.pulled, ...b.pulled]).toEqual([1, 2]);
});

test("ConcatStream - with a highWaterMark of 0, only reads when the output is read", async () => {
  const a = countingSource([1, 2]);
  const b = countingSource([3, 4]);
  const reader = new ConcatStream([a.stream, b.stream], {
    highWaterMark: 0,
  }).getReader();
  await delay(10);
  expect([...a.pulled, ...b.pulled]).toEqual([]);
  expect(await reader.read()).toEqual({ done: false, value: 1 });
  await delay(10);
  expect([...a.pulled, ...b.pulled]).toEqual([1]);
  expect(await reader.read()).toEqual({ done: false, value: 2 });
  // Moving on to the next stream also waits for the output to be read
  expect(await reader.read()).toEqual({ done: false, value: 3 });
  await delay(10);
  expect([...a.pulled, ...b.pulled]).toEqual([1, 2, 3]);
});

test("ConcatStream - with a larger highWaterMark, reads further ahead, across the source streams", async () => {
  const a = countingSource([1, 2]);
  const b = countingSource([3, 4, 5, 6]);
  const stream = new ConcatStream([a.stream, b.stream], { highWaterMark: 3 });
  await delay(10);
  expect([...a.pulled, ...b.pulled]).toEqual([1, 2, 3]);
  const reader = stream.getReader();
  expect(await reader.read()).toEqual({ done: false, value: 1 });
  await delay(10);
  expect([...a.pulled, ...b.pulled]).toEqual([1, 2, 3, 4]);
  reader.releaseLock();
  expect(await readableStreamResults(stream)).toEqual(
    [2, 3, 4, 5, 6].map((value) => ({ ok: true, value })),
  );
});

test("ConcatStream - uses the size function of the queuing strategy", async () => {
  const a = countingSource(["a", "bb"]);
  const b = countingSource(["ccc", "dddd"]);
  new ConcatStream([a.stream, b.stream], {
    highWaterMark: 5,
    size: (chunk) => chunk.length,
  });
  await delay(10);
  // Reads until the total size of the queued chunks reaches 5
  expect([...a.pulled, ...b.pulled]).toEqual(["a", "bb", "ccc"]);
});
